package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

var (
	gatewayB = v1alpha1.NamespacedObjectReference{Name: "gw-b", Namespace: "gateway-b"}
	gatewayC = v1alpha1.NamespacedObjectReference{Name: "gw-c", Namespace: "gateway-c"}
)

func attachmentProvider(namespace string) (*v1alpha1.ExternalProvider, *corev1.Secret) {
	provider := &v1alpha1.ExternalProvider{
		ObjectMeta: metav1.ObjectMeta{Name: "openai", Namespace: namespace, UID: types.UID("provider-" + namespace)},
		Spec: v1alpha1.ExternalProviderSpec{
			Provider: "openai", Endpoint: "api.openai.com",
			Auth: v1alpha1.AuthConfig{Type: "apikey", SecretRef: v1alpha1.NameReference{Name: "openai-key"}},
		},
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "openai-key", Namespace: namespace}, Data: map[string][]byte{"api-key": []byte("sk-" + namespace)}}
	return provider, secret
}

func attachmentModel(name, namespace, modelName string, created time.Time, gateways ...v1alpha1.NamespacedObjectReference) *v1alpha1.ExternalModel {
	return &v1alpha1.ExternalModel{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, UID: types.UID("uid-" + namespace + "-" + name), CreationTimestamp: metav1.NewTime(created)},
		Spec: v1alpha1.ExternalModelSpec{
			ModelName: modelName,
			ExternalProviderRefs: []v1alpha1.ExternalProviderRef{{
				Ref: v1alpha1.ExternalProviderReference{Name: "openai"}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions",
			}},
			GatewayRefs: gateways,
		},
	}
}

// attachmentTenantObjects returns a steady Praxis tenant owning gateway.
func attachmentTenantObjects(name, tenantNamespace string, gateway v1alpha1.NamespacedObjectReference, selector string) []client.Object {
	return []client.Object{
		controllerAITenant(name, "models-as-a-service", tenantNamespace, gateway.Name, gateway.Namespace),
		controllerMaasTenantConfig(tenantNamespace, name, "models-as-a-service", selector),
	}
}

func attachmentReconciler(t *testing.T, objects ...client.Object) *Reconciler {
	t.Helper()
	r := controllerTestClient(t, objects...)
	r.Network = "external-model"
	for _, object := range objects {
		provider, ok := object.(*v1alpha1.ExternalProvider)
		if !ok {
			continue
		}
		if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
			t.Fatalf("reconcile provider %s/%s: %v", provider.Namespace, provider.Name, err)
		}
	}
	return r
}

func reconcileModel(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err != nil {
		t.Fatalf("reconcile %s/%s: %v", model.Namespace, model.Name, err)
	}
}

func getModel(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) *v1alpha1.ExternalModel {
	t.Helper()
	var got v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &got); err != nil {
		t.Fatal(err)
	}
	return &got
}

func readyReason(model *v1alpha1.ExternalModel) string {
	if condition := apiMeta.FindStatusCondition(model.Status.Conditions, conditionReady); condition != nil {
		return condition.Reason
	}
	return ""
}

func getUnstructured(r *Reconciler, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	err := r.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: name}, obj)
	return obj, err
}

var destinationRuleGVK = schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"}

// gatewayResult returns the model's status.gateways result for gateway.
func gatewayResult(t *testing.T, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) v1alpha1.ExternalModelGatewayStatus {
	t.Helper()
	for _, result := range model.Status.Gateways {
		if result.NamespacedObjectReference == gateway {
			return result
		}
	}
	t.Fatalf("no status.gateways result for %s: %#v", gatewayKey(gateway), model.Status.Gateways)
	return v1alpha1.ExternalModelGatewayStatus{}
}

// assertTransportCollected checks that the namespace keeps no provider
// transport or selection sink of the given models.
func assertTransportCollected(t *testing.T, r *Reconciler, namespace string, models ...string) {
	t.Helper()
	if _, err := getUnstructured(r, schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntry"}, namespace, "provider-openai"); !apierrors.IsNotFound(err) {
		t.Fatalf("ServiceEntry %s/provider-openai remains: %v", namespace, err)
	}
	services := []string{"provider-openai"}
	for _, model := range models {
		services = append(services, providerSelectionSinkName(model))
	}
	for _, name := range services {
		if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, namespace, name); !apierrors.IsNotFound(err) {
			t.Fatalf("Service %s/%s remains: %v", namespace, name, err)
		}
	}
}

// setRouteParentStatus writes the Gateway's parent status on an attachment
// HTTPRoute, as the Gateway controller would.
func setRouteParentStatus(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference, status metav1.ConditionStatus, reason string) {
	t.Helper()
	route, err := getUnstructured(r, httpRouteGVK, model.Namespace, attachmentRouteName(model, gateway))
	if err != nil {
		t.Fatalf("attachment HTTPRoute: %v", err)
	}
	parents := []any{map[string]any{
		"parentRef":      map[string]any{"name": gateway.Name, "namespace": gateway.Namespace},
		"controllerName": "istio.io/gateway-controller",
		"conditions":     []any{map[string]any{"type": "Accepted", "status": string(status), "reason": reason}},
	}}
	if err := unstructured.SetNestedSlice(route.Object, parents, "status", "parents"); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(context.Background(), route); err != nil {
		t.Fatal(err)
	}
}

func overlayEnvelope(t *testing.T, r *Reconciler, namespace string) envelope.Envelope {
	t.Helper()
	var cm corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: "routing-overlay"}, &cm); err != nil {
		t.Fatalf("overlay %s: %v", namespace, err)
	}
	var env envelope.Envelope
	if err := json.Unmarshal([]byte(cm.Data["routing-overlay.json"]), &env); err != nil {
		t.Fatal(err)
	}
	return env
}

func routeFilterNames(t *testing.T, r *Reconciler, tenantID, gatewayNamespace string) []string {
	t.Helper()
	filter, err := getUnstructured(r, schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"},
		gatewayNamespace, tenant.PayloadProcessingExternalModelEnvoyFilterName(tenantID))
	if err != nil {
		t.Fatalf("route EnvoyFilter: %v", err)
	}
	var names []string
	patches := nestedSlice(t, filter.Object, "spec", "configPatches")
	for i := range patches {
		names = append(names, nestedString(t, nestedMapAt(t, patches, i), "match", "routeConfiguration", "vhost", "route", "name"))
	}
	return names
}

func TestUserNamespaceModelServedOnlyAfterRouteAcceptance(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)

	reconcileModel(t, r, model)
	routeName := attachmentRouteName(model, gatewayB)
	if !strings.HasPrefix(routeName, "external-model-chat-") || len(routeName) != len("external-model-chat-")+12 {
		t.Fatalf("attachment route name = %q", routeName)
	}
	route, err := getUnstructured(r, httpRouteGVK, "team", routeName)
	if err != nil {
		t.Fatalf("attachment HTTPRoute was not created first: %v", err)
	}
	parent := nestedMapAt(t, nestedSlice(t, route.Object, "spec", "parentRefs"), 0)
	if parent["name"] != "gw-b" || parent["namespace"] != "gateway-b" {
		t.Fatalf("parentRef = %#v, want gateway-b/gw-b", parent)
	}
	if route.GetLabels()[tenant.LabelAttachmentTenant] != "tenant-b" || route.GetLabels()["inference.opendatahub.io/external-model"] != "" {
		t.Fatalf("attachment route labels = %#v", route.GetLabels())
	}
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "team", "provider-openai"); err != nil {
		t.Fatalf("model-namespace provider Service: %v", err)
	}
	ruleName := attachmentDestinationRuleName(gatewayB, "team", "openai")
	copyName := credentialCopyName("tenant-b", "team", "openai-key", "api-key")
	assertAttachmentUnpublished := func() {
		t.Helper()
		if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName); !apierrors.IsNotFound(err) {
			t.Fatalf("DestinationRule published before acceptance: %v", err)
		}
		var copied corev1.Secret
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: copyName}, &copied); !apierrors.IsNotFound(err) {
			t.Fatalf("credential copy published before acceptance: %v", err)
		}
		var overlay corev1.ConfigMap
		if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
			t.Fatalf("overlay published before acceptance: %v", err)
		}
	}
	assertAttachmentUnpublished()
	if got := getModel(t, r, model); got.Status.Phase != modelPhasePending || readyReason(got) != reasonAttachmentPending {
		t.Fatalf("pending status = %q/%q", got.Status.Phase, readyReason(got))
	}

	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionFalse, "NotAllowedByListeners")
	reconcileModel(t, r, model)
	assertAttachmentUnpublished()
	if got := getModel(t, r, model); got.Status.Phase != modelPhaseFailed || readyReason(got) != "NotAllowedByListeners" {
		t.Fatalf("rejected status = %q/%q, want the Gateway's reason", got.Status.Phase, readyReason(got))
	}

	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	rule, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName)
	if err != nil {
		t.Fatalf("attachment DestinationRule: %v", err)
	}
	if len(ruleName) > 63 || rule.GetLabels()[tenant.LabelAttachmentTenant] != "tenant-b" || rule.GetLabels()[externalGatewayNameLabel] != "gw-b" {
		t.Fatalf("attachment DestinationRule %q labels = %#v", ruleName, rule.GetLabels())
	}
	// The provider's own reconciler selects DestinationRules by the
	// provider-namespace tenant label; the attachment rule must not carry it.
	if _, ok := rule.GetLabels()[externalTenantLabel]; ok {
		t.Fatalf("attachment DestinationRule carries %s: %#v", externalTenantLabel, rule.GetLabels())
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: copyName}, &copied); err != nil {
		t.Fatalf("credential copy: %v", err)
	}
	if string(copied.Data["api-key"]) != "sk-team" || len(copied.Data) != 1 || copied.Labels[tenant.LabelCredentialCopy] != "true" {
		t.Fatalf("credential copy = %#v", copied)
	}
	env := overlayEnvelope(t, r, "tenant-b")
	if len(env.Overlay.Candidates) != 1 {
		t.Fatalf("overlay candidates = %#v", env.Overlay.Candidates)
	}
	candidate := env.Overlay.Candidates[0]
	if candidate.StableID != "team/chat/team/openai" || candidate.Name != "chat-model" || candidate.Cluster != "provider-openai" {
		t.Fatalf("attached candidate = %#v", candidate)
	}
	if candidate.Credential == nil || candidate.Credential.SecretRef != (envelope.SecretRef{Name: copyName, Namespace: "tenant-b", Key: "api-key"}) {
		t.Fatalf("attached candidate credential = %#v, want the tenant-namespace copy", candidate.Credential)
	}
	if env.Scope.Namespace != "tenant-b" || env.Scope.Gateway != "gw-b" {
		t.Fatalf("overlay scope = %#v", env.Scope)
	}
	route, err = getUnstructured(r, httpRouteGVK, "team", routeName)
	if err != nil {
		t.Fatal(err)
	}
	headers := nestedSlice(t, nestedMapAt(t, nestedSlice(t, nestedMapAt(t, nestedSlice(t, route.Object, "spec", "rules"), 0), "matches"), 0), "headers")
	if nestedMapAt(t, headers, 0)["value"] != candidate.StableID {
		t.Fatalf("route candidate header = %#v, want %q", headers, candidate.StableID)
	}
	names := routeFilterNames(t, r, "tenant-b", "gateway-b")
	if len(names) != 4 || names[0] != "team."+routeName+".0" {
		t.Fatalf("route filter patches = %v", names)
	}
	got := getModel(t, r, model)
	if got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != routeName {
		t.Fatalf("served status = %#v", got.Status)
	}
}

func TestSharedModelAttachesToTenantsInIsolation(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB, gatewayC)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	setRouteParentStatus(t, r, model, gatewayC, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	routeB, routeC := attachmentRouteName(model, gatewayB), attachmentRouteName(model, gatewayC)
	if routeB == routeC {
		t.Fatalf("attachments share HTTPRoute name %q", routeB)
	}
	for _, tc := range []struct {
		tenantNamespace string
		gateway         v1alpha1.NamespacedObjectReference
		route           string
	}{{"tenant-b", gatewayB, routeB}, {"tenant-c", gatewayC, routeC}} {
		route, err := getUnstructured(r, httpRouteGVK, "team", tc.route)
		if err != nil {
			t.Fatal(err)
		}
		if parent := nestedMapAt(t, nestedSlice(t, route.Object, "spec", "parentRefs"), 0); parent["name"] != tc.gateway.Name || parent["namespace"] != tc.gateway.Namespace {
			t.Fatalf("route %s parentRef = %#v", tc.route, parent)
		}
		env := overlayEnvelope(t, r, tc.tenantNamespace)
		if len(env.Overlay.Candidates) != 1 || env.Overlay.Candidates[0].Credential.SecretRef.Namespace != tc.tenantNamespace {
			t.Fatalf("%s overlay = %#v", tc.tenantNamespace, env.Overlay.Candidates)
		}
		for _, name := range routeFilterNames(t, r, tc.tenantNamespace, tc.gateway.Namespace) {
			if !strings.HasPrefix(name, "team."+tc.route+".") {
				t.Fatalf("%s route filter leaks route %q", tc.tenantNamespace, name)
			}
		}
	}
	got := getModel(t, r, model)
	if got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != "" {
		t.Fatalf("multi-attachment status = %q httpRouteName=%q, want Ready and empty", got.Status.Phase, got.Status.HTTPRouteName)
	}
}

func TestUnsupportedAttachmentDoesNotBlockOtherAttachments(t *testing.T) {
	provider, secret := attachmentProvider("team")
	unmanaged := v1alpha1.NamespacedObjectReference{Name: "unmanaged", Namespace: "elsewhere"}
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayC, unmanaged, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "ipp")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("the Praxis tenant attachment must be served")
	}
	for _, gateway := range []v1alpha1.NamespacedObjectReference{gatewayC, unmanaged} {
		if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gateway)); !apierrors.IsNotFound(err) {
			t.Fatalf("unsupported attachment %s received a route: %v", gatewayKey(gateway), err)
		}
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-c", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("non-Praxis tenant received an overlay: %v", err)
	}
	if got := getModel(t, r, model); readyReason(got) != reasonTenantNotPraxis {
		t.Fatalf("status reason = %q, want the first failing attachment %q", readyReason(got), reasonTenantNotPraxis)
	}

	unmanagedOnly := attachmentModel("other", "team", "other-model", time.Unix(100, 0), unmanaged)
	if err := r.Create(context.Background(), unmanagedOnly); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, unmanagedOnly)
	if got := getModel(t, r, unmanagedOnly); readyReason(got) != reasonGatewayNotManaged {
		t.Fatalf("status reason = %q, want %q", readyReason(got), reasonGatewayNotManaged)
	}
}

func TestModelNameConflictKeepsTenantModelThenOldest(t *testing.T) {
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	ownModel := attachmentModel("own", "tenant-b", "chat-model", time.Unix(300, 0))
	providerOne, secretOne := attachmentProvider("team-one")
	providerTwo, secretTwo := attachmentProvider("team-two")
	conflicting := attachmentModel("chat", "team-one", "chat-model", time.Unix(100, 0), gatewayB)
	older := attachmentModel("older", "team-one", "shared-name", time.Unix(100, 0), gatewayB)
	newer := attachmentModel("newer", "team-two", "shared-name", time.Unix(200, 0), gatewayB)
	objects := []client.Object{tenantProvider, tenantSecret, ownModel, providerOne, secretOne, providerTwo, secretTwo, conflicting, older, newer}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, ownModel)
	for _, model := range []*v1alpha1.ExternalModel{conflicting, older, newer} {
		reconcileModel(t, r, model)
	}
	setRouteParentStatus(t, r, older, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, older)

	for _, loser := range []*v1alpha1.ExternalModel{conflicting, newer} {
		got := getModel(t, r, loser)
		if readyReason(got) != reasonModelNameConflict {
			t.Fatalf("%s reason = %q, want %q", loser.Name, readyReason(got), reasonModelNameConflict)
		}
		if _, err := getUnstructured(r, httpRouteGVK, loser.Namespace, attachmentRouteName(loser, gatewayB)); !apierrors.IsNotFound(err) {
			t.Fatalf("conflict loser %s received a route: %v", loser.Name, err)
		}
	}
	conflict := apiMeta.FindStatusCondition(gatewayResult(t, getModel(t, r, conflicting), gatewayB).Conditions, conditionReady)
	if conflict == nil || conflict.Reason != reasonModelNameConflict || !strings.Contains(conflict.Message, "tenant-b/own") {
		t.Fatalf("conflict result %#v does not name the winner", conflict)
	}
	env := overlayEnvelope(t, r, "tenant-b")
	var ids []string
	for _, candidate := range env.Overlay.Candidates {
		ids = append(ids, candidate.StableID)
	}
	if strings.Join(ids, ",") != "own/provider-openai,team-one/older/team-one/openai" {
		t.Fatalf("overlay candidates = %v, want the tenant's own model first and only the oldest attached winner", ids)
	}
}

// renameModel changes the model's client-facing name and returns the requests
// the model-name watch enqueues for that update.
func renameModel(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel, modelName string) []reconcile.Request {
	t.Helper()
	old := getModel(t, r, model)
	renamed := old.DeepCopy()
	renamed.Spec.ModelName = modelName
	if err := r.Update(context.Background(), renamed); err != nil {
		t.Fatal(err)
	}
	q := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	defer q.ShutDown()
	handler.EnqueueRequestsFromMapFunc(r.sameNameModels).Update(context.Background(), event.UpdateEvent{ObjectOld: old, ObjectNew: renamed}, q)
	var requests []reconcile.Request
	for q.Len() > 0 {
		request, _ := q.Get()
		requests = append(requests, request)
		q.Done(request)
	}
	return requests
}

func attachmentRouteExists(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) bool {
	t.Helper()
	_, err := getUnstructured(r, httpRouteGVK, model.Namespace, attachmentRouteName(model, gatewayB))
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

func TestRenamedWinnerReleasesItsNameToTheWaitingModel(t *testing.T) {
	providerOne, secretOne := attachmentProvider("team-one")
	providerTwo, secretTwo := attachmentProvider("team-two")
	winner := attachmentModel("a", "team-one", "foo", time.Unix(100, 0), gatewayB)
	waiting := attachmentModel("b", "team-two", "foo", time.Unix(200, 0), gatewayB)
	objects := []client.Object{providerOne, secretOne, providerTwo, secretTwo, winner, waiting}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, winner)
	reconcileModel(t, r, waiting)
	if reason := readyReason(getModel(t, r, waiting)); reason != reasonModelNameConflict || attachmentRouteExists(t, r, waiting) {
		t.Fatalf("waiting model reason = %q, route = %t; want %q without a route", reason, attachmentRouteExists(t, r, waiting), reasonModelNameConflict)
	}

	requests := renameModel(t, r, winner, "bar")
	if !slices.Contains(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(waiting)}) {
		t.Fatalf("renaming %s enqueued %v, want the model waiting on its previous name", winner.Name, requests)
	}
	reconcileModel(t, r, waiting)
	if reason := readyReason(getModel(t, r, waiting)); reason != reasonAttachmentPending || !attachmentRouteExists(t, r, waiting) {
		t.Fatalf("after the rename: reason = %q, route = %t; want %q with a route", reason, attachmentRouteExists(t, r, waiting), reasonAttachmentPending)
	}
}

// namedProvider returns another provider and its credential in namespace.
func namedProvider(namespace, name string) (*v1alpha1.ExternalProvider, *corev1.Secret) {
	provider, secret := attachmentProvider(namespace)
	provider.Name, provider.UID = name, types.UID("provider-"+namespace+"-"+name)
	secret.Name = name + "-key"
	provider.Spec.Auth.SecretRef.Name = secret.Name
	return provider, secret
}

func TestSameNamespaceModelsShareAnAttachedName(t *testing.T) {
	providerOne, secretOne := attachmentProvider("team-one")
	providerAlt, secretAlt := namedProvider("team-one", "alt")
	providerTwo, secretTwo := attachmentProvider("team-two")
	first := attachmentModel("first", "team-one", "foo", time.Unix(100, 0), gatewayB)
	other := attachmentModel("other", "team-two", "foo", time.Unix(200, 0), gatewayB)
	// Same-namespace models sharing a name are one selection group, so the
	// sibling uses another provider, as on the tenant-local path.
	sibling := attachmentModel("sibling", "team-one", "foo", time.Unix(300, 0), gatewayB)
	sibling.Spec.ExternalProviderRefs[0].Ref.Name = providerAlt.Name
	objects := []client.Object{providerOne, secretOne, providerAlt, secretAlt, providerTwo, secretTwo, first, other, sibling}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{first, other, sibling} {
		reconcileModel(t, r, model)
	}
	want := func(model *v1alpha1.ExternalModel, reason string, route bool) {
		t.Helper()
		if got := readyReason(getModel(t, r, model)); got != reason || attachmentRouteExists(t, r, model) != route {
			t.Fatalf("%s/%s: reason = %q, route = %t; want %q, route = %t", model.Namespace, model.Name, got, attachmentRouteExists(t, r, model), reason, route)
		}
	}
	// Conflicts apply across namespaces only: same-namespace models share the name.
	want(first, reasonAttachmentPending, true)
	want(sibling, reasonAttachmentPending, true)
	want(other, reasonModelNameConflict, false)

	// Once the oldest model releases the name, the other namespace holds it,
	// so the same-namespace sibling must be re-decided too.
	requests := renameModel(t, r, first, "bar")
	for _, model := range []*v1alpha1.ExternalModel{other, sibling} {
		if !slices.Contains(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}) {
			t.Fatalf("renaming %s enqueued %v, want %s/%s", first.Name, requests, model.Namespace, model.Name)
		}
		reconcileModel(t, r, model)
	}
	want(other, reasonAttachmentPending, true)
	want(sibling, reasonModelNameConflict, false)
}

func TestSameNamespaceAliasesRepeatingAProviderExcludeOnlyThemselves(t *testing.T) {
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	ownModel := attachmentModel("own", "tenant-b", "own-model", time.Unix(300, 0))
	provider, secret := attachmentProvider("team")
	first := attachmentModel("first", "team", "foo", time.Unix(100, 0), gatewayB)
	second := attachmentModel("second", "team", "foo", time.Unix(200, 0), gatewayB)
	objects := []client.Object{tenantProvider, tenantSecret, ownModel, provider, secret, first, second}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{first, second, ownModel} {
		reconcileModel(t, r, model)
	}
	for _, model := range []*v1alpha1.ExternalModel{first, second} {
		if got := getModel(t, r, model); readyReason(got) != reasonReconcileFailed || attachmentRouteExists(t, r, model) {
			t.Fatalf("%s: reason = %q, route = %t; want %q without a route", model.Name, readyReason(got), attachmentRouteExists(t, r, model), reasonReconcileFailed)
		}
	}
	// The rejected group never reaches the overlay or blocks the tenant's own model.
	if env := overlayEnvelope(t, r, "tenant-b"); len(env.Overlay.Candidates) != 1 || env.Overlay.Candidates[0].Name != "own-model" {
		t.Fatalf("overlay candidates = %#v, want only the tenant's own model", env.Overlay.Candidates)
	}
}

func TestDeletingProviderExcludesAttachedModel(t *testing.T) {
	provider, secret := attachmentProvider("team")
	provider.Finalizers = []string{"example.com/hold"}
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if err := r.Delete(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if reason := readyReason(getModel(t, r, model)); reason != reasonProviderNotReady {
		t.Fatalf("reason = %q, want %q", reason, reasonProviderNotReady)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("a model whose provider is being deleted kept its route: %v", err)
	}
}

func TestOwnTenantGatewayRefChangesNoResources(t *testing.T) {
	render := func(gateways ...v1alpha1.NamespacedObjectReference) (*Reconciler, *v1alpha1.ExternalModel) {
		provider, secret := attachmentProvider("tenant-b")
		model := attachmentModel("chat", "tenant-b", "chat-model", time.Unix(100, 0), gateways...)
		r := attachmentReconciler(t, append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)...)
		r.PublishOverlay = nil
		reconcileModel(t, r, model)
		return r, model
	}
	legacy, _ := render()
	attached, attachedModel := render(gatewayB)
	for _, object := range []struct {
		gvk             schema.GroupVersionKind
		namespace, name string
	}{
		{httpRouteGVK, "tenant-b", "external-model-chat"},
		{schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "tenant-b", "provider-openai"},
		{schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntry"}, "tenant-b", "provider-openai"},
		{destinationRuleGVK, "gateway-b", "provider-openai-tenant-b"},
		{schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"}, "gateway-b", tenant.PayloadProcessingExternalModelEnvoyFilterName("tenant-b")},
	} {
		want, err := getUnstructured(legacy, object.gvk, object.namespace, object.name)
		if err != nil {
			t.Fatalf("legacy %s: %v", object.gvk.Kind, err)
		}
		got, err := getUnstructured(attached, object.gvk, object.namespace, object.name)
		if err != nil {
			t.Fatalf("own-gateway %s: %v", object.gvk.Kind, err)
		}
		wantSpec, gotSpec := mustJSON(t, want.Object["spec"]), mustJSON(t, got.Object["spec"])
		if wantSpec != gotSpec || !maps.Equal(want.GetLabels(), got.GetLabels()) {
			t.Fatalf("%s differs:\nlegacy %s %v\nown-gateway %s %v", object.gvk.Kind, wantSpec, want.GetLabels(), gotSpec, got.GetLabels())
		}
	}
	if legacyEnv, gotEnv := overlayEnvelope(t, legacy, "tenant-b"), overlayEnvelope(t, attached, "tenant-b"); legacyEnv.Revision != gotEnv.Revision {
		t.Fatalf("overlay digest differs: %s vs %s", legacyEnv.Revision.Value, gotEnv.Revision.Value)
	}
	got := getModel(t, attached, attachedModel)
	if got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != "external-model-chat" {
		t.Fatalf("own-gateway status = %#v", got.Status)
	}
	// Only the home attachment's result is added; its deletion stays the
	// existing home path.
	result := gatewayResult(t, got, gatewayB)
	if result.HTTPRouteRef == nil || *result.HTTPRouteRef != (v1alpha1.NamespacedObjectReference{Name: "external-model-chat", Namespace: "tenant-b"}) ||
		!apiMeta.IsStatusConditionTrue(result.Conditions, conditionReady) || !slices.Equal(got.Finalizers, []string{externalModelFinalizer}) {
		t.Fatalf("own-gateway result = %#v, finalizers %v", result, got.Finalizers)
	}
}

func TestGatewayStatusReportsEachAttachment(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	setRefs := func(refs ...v1alpha1.NamespacedObjectReference) *v1alpha1.ExternalModel {
		t.Helper()
		got := getModel(t, r, model)
		got.Spec.GatewayRefs = refs
		if err := r.Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		reconcileModel(t, r, model)
		return getModel(t, r, model)
	}
	routeB := &v1alpha1.NamespacedObjectReference{Name: attachmentRouteName(model, gatewayB), Namespace: "team"}
	assertResult := func(got *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference, reason string, route *v1alpha1.NamespacedObjectReference) {
		t.Helper()
		result := gatewayResult(t, got, gateway)
		ready := apiMeta.FindStatusCondition(result.Conditions, conditionReady)
		routeMatches := (route == nil) == (result.HTTPRouteRef == nil) && (route == nil || *route == *result.HTTPRouteRef)
		if ready == nil || ready.Reason != reason || ready.ObservedGeneration != got.Generation ||
			(ready.Status == metav1.ConditionTrue) != (route != nil) || !routeMatches {
			t.Fatalf("%s result = %#v, want reason %q and route %v", gatewayKey(gateway), result, reason, route)
		}
	}

	got := getModel(t, r, model)
	assertResult(got, gatewayB, reasonReconciled, routeB)
	if got.Status.HTTPRouteName != routeB.Name {
		t.Fatalf("single attachment httpRouteName = %q, want %q", got.Status.HTTPRouteName, routeB.Name)
	}

	// A second attachment: the served one keeps its result while the new one
	// is pending, and the single-attachment alias is cleared.
	got = setRefs(gatewayB, gatewayC)
	if len(got.Status.Gateways) != 2 || got.Status.HTTPRouteName != "" || readyReason(got) != reasonAttachmentPending {
		t.Fatalf("status = %#v, want two results, no httpRouteName and the pending attachment reported", got.Status)
	}
	assertResult(got, gatewayB, reasonReconciled, routeB)
	assertResult(got, gatewayC, reasonAttachmentPending, nil)

	// Partial failure: only the rejected attachment fails.
	setRouteParentStatus(t, r, model, gatewayC, metav1.ConditionFalse, "NotAllowedByListeners")
	reconcileModel(t, r, model)
	got = getModel(t, r, model)
	assertResult(got, gatewayB, reasonReconciled, routeB)
	assertResult(got, gatewayC, "NotAllowedByListeners", nil)

	// Removing an attachment removes its result.
	got = setRefs(gatewayC)
	if len(got.Status.Gateways) != 1 {
		t.Fatalf("status.gateways = %#v, want only gw-c", got.Status.Gateways)
	}
	assertResult(got, gatewayC, "NotAllowedByListeners", nil)

	got = setRefs()
	if len(got.Status.Gateways) != 0 || len(got.Finalizers) != 0 {
		t.Fatalf("detached model keeps results %#v or finalizers %v", got.Status.Gateways, got.Finalizers)
	}
	assertTransportCollected(t, r, "team", "chat")
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestModelWithoutTenantOrGatewayRefsIsTenantNotResolved(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
	r := attachmentReconciler(t, provider, secret, model)
	// A route name another writer (legacy IPP) published is left alone.
	written := getModel(t, r, model)
	written.Status.HTTPRouteName = "chat"
	if err := r.Status().Update(context.Background(), written); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	got := getModel(t, r, model)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if got.Status.Phase != modelPhasePending || condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != reasonTenantNotResolved || !strings.Contains(condition.Message, "spec.gatewayRefs") || got.Status.HTTPRouteName != "chat" {
		t.Fatalf("status = %#v", got.Status)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("serving state created without a tenant: %v", err)
	}
}

func TestTenantModelBoundElsewhereIsNotServedByItsOwnTenant(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("a model bound to another Gateway fell back to its own tenant: %v", err)
	}
	if got := getModel(t, r, model); readyReason(got) != reasonGatewayNotManaged {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonGatewayNotManaged)
	}
}

func TestTooManyProvidersExcludesOnlyThatModel(t *testing.T) {
	objects := attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")
	model := attachmentModel("wide", "team", "wide-model", time.Unix(100, 0), gatewayB)
	model.Spec.ExternalProviderRefs = nil
	for _, name := range []string{"p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"} {
		provider, secret := attachmentProvider("team")
		provider.Name, provider.UID = name, types.UID("uid-"+name)
		objects = append(objects, provider)
		if name == "p1" {
			objects = append(objects, secret)
		}
		model.Spec.ExternalProviderRefs = append(model.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
			Ref: v1alpha1.ExternalProviderReference{Name: name}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions",
		})
	}
	r := attachmentReconciler(t, append(objects, model)...)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); readyReason(got) != reasonTooManyProviders {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonTooManyProviders)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("over-budget model received a route: %v", err)
	}
}

func TestUnservedHomeAttachmentBlocksAggregateReady(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "ipp")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("the Praxis attachment must still be served")
	}
	if got := getModel(t, r, model); got.Status.Phase == resolver.PhaseReady || readyReason(got) != reasonTenantNotPraxis {
		t.Fatalf("status = %q/%q, want the unserved home attachment reported", got.Status.Phase, readyReason(got))
	}
	// The provider's IPP cleanup keeps the transport the attachment route uses.
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatal(err)
	}
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "tenant-a", "provider-openai"); err != nil {
		t.Fatalf("provider Service removed while the attachment route uses it: %v", err)
	}

	// Only attachments create transport in a non-Praxis tenant namespace, so
	// removing the last one collects it.
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayA}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	assertTransportCollected(t, r, "tenant-a", "chat")
}

// failingTenantReader fails live reads of one tenant's MaasTenantConfig, as
// an unavailable API server would.
type failingTenantReader struct {
	client.Reader
	namespace string
}

func (f failingTenantReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == tenant.MaasTenantConfigGVK.Kind && key.Namespace == f.namespace {
		return apierrors.NewServiceUnavailable("etcd is unavailable")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

func TestHomeAttachmentReportsOnlyItsOwnFailure(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "ipp")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reader := r.APIReader
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}

	// Another attachment's failure does not relabel the home attachment.
	r.APIReader = failingTenantReader{Reader: reader, namespace: "tenant-b"}
	if _, err := r.Reconcile(context.Background(), request); err == nil {
		t.Fatal("a failed tenant read must be retried")
	}
	if got := getModel(t, r, model); readyReason(got) != reasonTenantNotPraxis {
		t.Fatalf("reason = %q, want the home attachment's own %q", readyReason(got), reasonTenantNotPraxis)
	}

	// The home path's own failure is reported as a failure, not a tenant state.
	r.APIReader = failingTenantReader{Reader: reader, namespace: "tenant-a"}
	if _, err := r.Reconcile(context.Background(), request); err == nil {
		t.Fatal("a failed tenant read must be retried")
	}
	got := getModel(t, r, model)
	if readyReason(got) != reasonReconcileFailed || got.Status.Phase != modelPhaseFailed {
		t.Fatalf("status = %q/%q, want %q", got.Status.Phase, readyReason(got), reasonReconcileFailed)
	}
}

func TestHomeAttachmentReportsItsTenantNotReady(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	tenantA := attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")
	// The AITenant's Ready stamp is for an older generation: not DeployReady.
	tenantA[0].SetGeneration(2)
	objects := append([]client.Object{provider, secret, model}, tenantA...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if ready := apiMeta.FindStatusCondition(gatewayResult(t, getModel(t, r, model), gatewayA).Conditions, conditionReady); ready == nil || ready.Reason != reasonTenantNotReady {
		t.Fatalf("home result = %#v, want %q", ready, reasonTenantNotReady)
	}
}

func TestRebindingAwayFromOwnTenantRemovesHomeRoute(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0))
	sibling := attachmentModel("other", "tenant-a", "other-model", time.Unix(100, 0))
	objects := []client.Object{provider, secret, model, sibling}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	reconcileModel(t, r, sibling)
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); err != nil {
		t.Fatalf("model is not served through its own tenant first: %v", err)
	}

	got := getModel(t, r, model)
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)

	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("the old home route survived rebinding to another Gateway: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-other"); err != nil {
		t.Fatalf("a sibling's route was removed: %v", err)
	}
	if candidates := overlayEnvelope(t, r, "tenant-a").Overlay.Candidates; len(candidates) != 1 || candidates[0].Name != "other-model" {
		t.Fatalf("own tenant overlay = %#v, want only the sibling", candidates)
	}
	if got := getModel(t, r, model); readyReason(got) != reasonAttachmentPending {
		t.Fatalf("reason = %q, want the new attachment pending", readyReason(got))
	}

	// The last model leaving removes the tenant's own serving state.
	got = getModel(t, r, sibling)
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, sibling)
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-other"); !apierrors.IsNotFound(err) {
		t.Fatalf("the last model's home route survived: %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("own tenant overlay survived its last model leaving: %v", err)
	}
}

func TestRebindingAwayFromDeselectedTenantCleansHomeRoute(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0))
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); err != nil {
		t.Fatalf("model is not served through its own tenant first: %v", err)
	}

	// The tenant switches to IPP while the model is rebound elsewhere.
	mtc := tenant.NewMaasTenantConfig()
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: tenant.MaasTenantConfigInstanceName}, mtc); err != nil {
		t.Fatal(err)
	}
	annotations := mtc.GetAnnotations()
	annotations[tenant.AnnotationPayloadProcessingType] = tenant.PayloadProcessingBackendIPP
	mtc.SetAnnotations(annotations)
	if err := r.Update(context.Background(), mtc); err != nil {
		t.Fatal(err)
	}
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)

	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("the home route survived an explicit non-Praxis selection: %v", err)
	}
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-a", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("the deselected tenant's overlay survived: %v", err)
	}
}

func TestAttachmentRouteFallbackNeverMatchesAHomeRoute(t *testing.T) {
	model := attachmentModel(strings.Repeat("m", 240), "team", "chat-model", time.Unix(100, 0), gatewayB)
	// ExternalModel names start with an alphanumeric, so no home route
	// (modelRoutePrefix + name) starts with modelRoutePrefix + "-".
	if name := attachmentRouteName(model, gatewayB); len(name) > 253 || !strings.HasPrefix(name, modelRoutePrefix+"-") {
		t.Fatalf("fallback route name = %q, want %q followed by the hash", name, modelRoutePrefix+"-")
	}
}

func TestRemovedGatewayRefRemovesOnlyItsAttachmentRoute(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB, gatewayC)
	other := attachmentModel("other", "team", "other-model", time.Unix(100, 0), gatewayC)
	objects := []client.Object{provider, secret, model, other}
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-c", "tenant-c", gatewayC, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	reconcileModel(t, r, other)
	setRefs := func(refs ...v1alpha1.NamespacedObjectReference) {
		t.Helper()
		got := getModel(t, r, model)
		got.Spec.GatewayRefs = refs
		if err := r.Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		reconcileModel(t, r, model)
	}
	routeExists := func(name string) bool {
		t.Helper()
		_, err := getUnstructured(r, httpRouteGVK, "team", name)
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatal(err)
		}
		return err == nil
	}
	routeB, routeC, otherC := attachmentRouteName(model, gatewayB), attachmentRouteName(model, gatewayC), attachmentRouteName(other, gatewayC)

	setRefs(gatewayB)
	if routeExists(routeC) || !routeExists(routeB) || !routeExists(otherC) {
		t.Fatalf("after removing gw-c: routes b=%v c=%v other=%v, want only the removed attachment gone",
			routeExists(routeB), routeExists(routeC), routeExists(otherC))
	}
	setRefs()
	if routeExists(routeB) || !routeExists(otherC) {
		t.Fatalf("after removing every gatewayRef: routes b=%v other=%v, want the model's last attachment gone",
			routeExists(routeB), routeExists(otherC))
	}
}

func TestAttachmentPreservesTenantOwnOverlayCandidates(t *testing.T) {
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	ownModel := attachmentModel("own", "tenant-b", "own-model", time.Unix(300, 0))
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{tenantProvider, tenantSecret, ownModel, provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, ownModel)
	before := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates

	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	after := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates
	if len(after) != 2 || mustJSON(t, after[0]) != mustJSON(t, before[0]) {
		t.Fatalf("own candidate changed: before %#v after %#v", before, after)
	}
	names := routeFilterNames(t, r, "tenant-b", "gateway-b")
	if len(names) != 8 || names[0] != "tenant-b.external-model-own.0" || names[4] != "team."+attachmentRouteName(model, gatewayB)+".0" {
		t.Fatalf("route filter patches = %v, want own patches then the attachment", names)
	}

	// The tenant's own reconcile keeps the admitted attachment.
	reconcileModel(t, r, ownModel)
	if again := overlayEnvelope(t, r, "tenant-b").Overlay.Candidates; len(again) != 2 || again[1].StableID != "team/chat/team/openai" {
		t.Fatalf("own reconcile dropped the attachment: %#v", again)
	}
}

func TestSameNamedProvidersFromNamespacesStayDistinct(t *testing.T) {
	providerOne, secretOne := attachmentProvider("team-one")
	providerTwo, secretTwo := attachmentProvider("team-two")
	modelOne := attachmentModel("chat", "team-one", "chat-one", time.Unix(100, 0), gatewayB)
	modelTwo := attachmentModel("chat", "team-two", "chat-two", time.Unix(200, 0), gatewayB)
	objects := append([]client.Object{providerOne, secretOne, providerTwo, secretTwo, modelOne, modelTwo}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{modelOne, modelTwo} {
		reconcileModel(t, r, model)
		setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	}
	for _, model := range []*v1alpha1.ExternalModel{modelOne, modelTwo} {
		reconcileModel(t, r, model)
	}
	ruleOne, ruleTwo := attachmentDestinationRuleName(gatewayB, "team-one", "openai"), attachmentDestinationRuleName(gatewayB, "team-two", "openai")
	if ruleOne == ruleTwo {
		t.Fatalf("DestinationRules collide: %s", ruleOne)
	}
	for _, name := range []string{ruleOne, ruleTwo} {
		if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", name); err != nil {
			t.Fatalf("DestinationRule %s: %v", name, err)
		}
	}
	env := overlayEnvelope(t, r, "tenant-b")
	if len(env.Overlay.Candidates) != 2 || env.Overlay.Candidates[0].StableID != "team-one/chat/team-one/openai" ||
		env.Overlay.Candidates[1].StableID != "team-two/chat/team-two/openai" {
		t.Fatalf("overlay candidates = %#v", env.Overlay.Candidates)
	}
	if env.Overlay.Candidates[0].Credential.SecretRef.Name == env.Overlay.Candidates[1].Credential.SecretRef.Name {
		t.Fatal("credential copies of different sources collide")
	}
}

func servedAttachmentFixture(t *testing.T) (*Reconciler, *v1alpha1.ExternalModel) {
	t.Helper()
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("fixture attachment is not served")
	}
	return r, model
}

func assertAttachmentWithdrawn(t *testing.T, r *Reconciler) {
	t.Helper()
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", attachmentDestinationRuleName(gatewayB, "team", "openai")); !apierrors.IsNotFound(err) {
		t.Fatalf("attachment DestinationRule remains: %v", err)
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "openai-key", "api-key")}, &copied); !apierrors.IsNotFound(err) {
		t.Fatalf("credential copy remains: %v", err)
	}
}

func assertNoTenantOverlay(t *testing.T, r *Reconciler) {
	t.Helper()
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("overlay entry remains: %v", err)
	}
}

func TestRevokedAcceptanceWithdrawsAttachment(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionFalse, "NotAllowedByListeners")
	reconcileModel(t, r, model)
	assertAttachmentWithdrawn(t, r)
	assertNoTenantOverlay(t, r)
	if got := getModel(t, r, model); readyReason(got) != "NotAllowedByListeners" || got.Status.HTTPRouteName != "" {
		t.Fatalf("reason = %q, httpRouteName = %q; want the Gateway's reason and no route", readyReason(got), got.Status.HTTPRouteName)
	}
}

func TestRemovedGatewayRefCollectsAttachmentState(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = nil
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if attachmentRouteExists(t, r, model) {
		t.Fatal("detached model keeps its attachment route")
	}
	assertAttachmentWithdrawn(t, r)
	if got := getModel(t, r, model); got.Status.HTTPRouteName != "" {
		t.Fatalf("httpRouteName = %q names the deleted attachment route", got.Status.HTTPRouteName)
	}
}

func TestFailedDetachCleanupIsRetried(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	failed := false
	fakeClient, ok := r.Client.(client.WithWatch)
	if !ok {
		t.Fatal("the test client must support watches")
	}
	r.Client = interceptor.NewClient(fakeClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == "DestinationRule" && !failed {
				failed = true
				return apierrors.NewServiceUnavailable("etcd is unavailable")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = nil
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err == nil {
		t.Fatal("a failed cleanup must be retried")
	}
	// The route is withdrawn before its backends.
	if attachmentRouteExists(t, r, model) {
		t.Fatal("the attachment route outlives a failed backend cleanup")
	}
	// The attachment finalizer stays until the cleanup succeeds, so the retry
	// collects what the deleted route used.
	reconcileModel(t, r, model)
	assertAttachmentWithdrawn(t, r)
}

func TestDeletingModelCollectsStateOfItsAlreadyDeletedRoute(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	// Foreground deletion removes the model-owned route and selection sink
	// before the model's finalizer runs; the provider transport is not
	// model-owned.
	route, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB))
	if err != nil {
		t.Fatal(err)
	}
	sink, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "team", providerSelectionSinkName("chat"))
	if err != nil {
		t.Fatal(err)
	}
	for _, owned := range []client.Object{route, sink} {
		if err := r.Delete(context.Background(), owned); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Delete(context.Background(), getModel(t, r, model)); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	assertAttachmentWithdrawn(t, r)
	assertTransportCollected(t, r, "team", "chat")
}

func TestDeletedCredentialSourceWithdrawsAttachment(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	_, secret := attachmentProvider("team")
	if err := r.Delete(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); readyReason(got) != reasonProviderNotReady {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonProviderNotReady)
	}
	// The copy must not keep serving a credential its source no longer holds.
	assertAttachmentWithdrawn(t, r)
	assertNoTenantOverlay(t, r)
	if attachmentRouteExists(t, r, model) {
		t.Fatal("excluded model keeps its route")
	}

	_, secret = attachmentProvider("team")
	if err := r.Create(context.Background(), secret); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("the attachment is not served again after its source Secret was restored")
	}
}

// failingSecretReader fails Secret reads as an unavailable API server would.
type failingSecretReader struct{ client.Reader }

func (f failingSecretReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Secret); ok {
		return apierrors.NewServiceUnavailable("etcd is unavailable")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

func TestTransientCredentialReadKeepsServedAttachment(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	r.APIReader = failingSecretReader{Reader: r.APIReader}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err == nil {
		t.Fatal("a failed Secret read must be retried")
	}
	if len(overlayEnvelope(t, r, "tenant-b").Overlay.Candidates) != 1 {
		t.Fatal("a failed Secret read withdrew the served attachment")
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "openai-key", "api-key")}, &copied); err != nil {
		t.Fatalf("a failed Secret read deleted the credential copy: %v", err)
	}
	if got := getModel(t, r, model); readyReason(got) != reasonReconcileFailed {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonReconcileFailed)
	}
}

// publishOwnCandidates publishes count tenant-local candidates in the tenant
// overlay, as the tenant-local path does.
func publishOwnCandidates(t *testing.T, r *Reconciler, namespace string, gateway v1alpha1.NamespacedObjectReference, count int) {
	t.Helper()
	set := &resolver.ResolvedRouteSet{}
	for i := range count {
		model := fmt.Sprintf("own-%d", i)
		set.Models = append(set.Models, resolver.ModelRoutes{ModelRef: namespace + "/" + model, Routes: []resolver.Route{{
			Model: model, ClientName: model, Namespace: namespace, Provider: "local", Cluster: "provider-local", Weight: 1,
		}}})
	}
	if _, err := r.publishTenantOverlay(context.Background(), namespace, gateway.Name, set, nil, nil, "tenant-source"); err != nil {
		t.Fatal(err)
	}
}

func TestAttachmentOverOverlayCapacityReceivesNoTenantState(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	publishOwnCandidates(t, r, "tenant-b", gatewayB, maxOverlayCandidates)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)

	if got := getModel(t, r, model); readyReason(got) != reasonOverlayCapacityExceeded {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonOverlayCapacityExceeded)
	}
	assertAttachmentWithdrawn(t, r)
	for _, candidate := range overlayEnvelope(t, r, "tenant-b").Overlay.Candidates {
		if envelope.IsScopedStableID(candidate.StableID) {
			t.Fatalf("over-capacity model entered the overlay: %#v", candidate)
		}
	}
	// It keeps its accepted route, and so its place in admission order.
	if !attachmentRouteExists(t, r, model) {
		t.Fatal("over-capacity model lost its accepted route")
	}
}

func TestAttachmentPushedOverCapacityIsWithdrawn(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	publishOwnCandidates(t, r, "tenant-b", gatewayB, maxOverlayCandidates)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); readyReason(got) != reasonOverlayCapacityExceeded {
		t.Fatalf("reason = %q, want %q", readyReason(got), reasonOverlayCapacityExceeded)
	}
	assertAttachmentWithdrawn(t, r)
}

func TestProviderReconcileKeepsAttachmentDestinationRules(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	objects := []client.Object{provider, secret, model}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	ruleName := attachmentDestinationRuleName(gatewayB, "tenant-a", "openai")
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName); err != nil {
		t.Fatalf("attachment DestinationRule: %v", err)
	}
	// The provider's own reconciler keeps only the rule for its tenant's
	// Gateway; the attachment's rule on another tenant's Gateway is not its.
	if _, err := r.ReconcileProvider(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(provider)}); err != nil {
		t.Fatal(err)
	}
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", ruleName); err != nil {
		t.Fatalf("provider reconcile removed the attachment DestinationRule: %v", err)
	}
}

func TestTenantKeepsAttachmentsWhenItsOwnModelsServeNothing(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	// The tenant's own model references a provider that does not exist, so
	// the tenant itself serves nothing.
	own := attachmentModel("own", "tenant-b", "own-model", time.Unix(300, 0))
	if err := r.Create(context.Background(), own); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(own)}); !errors.Is(err, resolver.ErrNoRoutes) {
		t.Fatalf("own reconcile = %v, want %v", err, resolver.ErrNoRoutes)
	}
	assertServed := func(when string) {
		t.Helper()
		if env := overlayEnvelope(t, r, "tenant-b"); len(env.Overlay.Candidates) != 1 || env.Overlay.Candidates[0].StableID != "team/chat/team/openai" {
			t.Fatalf("%s: overlay candidates = %#v, want the attachment", when, env.Overlay.Candidates)
		}
		if names := routeFilterNames(t, r, "tenant-b", "gateway-b"); len(names) != 4 || names[0] != "team."+attachmentRouteName(model, gatewayB)+".0" {
			t.Fatalf("%s: route filter patches = %v", when, names)
		}
	}
	assertServed("own model without routes")

	if err := r.Delete(context.Background(), getModel(t, r, own)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(own)}); err != nil {
		t.Fatalf("own deletion: %v", err)
	}
	assertServed("last own model deleted")
}

func TestAttachmentKeepsTenantSelectionPolicy(t *testing.T) {
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	tenantAlt, tenantAltSecret := namedProvider("tenant-b", "alt")
	// The tenant's own model spreads traffic over two equal-weight providers.
	ownModel := attachmentModel("own", "tenant-b", "own-model", time.Unix(300, 0))
	ownModel.Spec.ExternalProviderRefs = append(ownModel.Spec.ExternalProviderRefs, v1alpha1.ExternalProviderRef{
		Ref: v1alpha1.ExternalProviderReference{Name: tenantAlt.Name}, TargetModel: "gpt-4o", APIFormat: "openai-chat", Path: "/v1/chat/completions",
	})
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{tenantProvider, tenantSecret, tenantAlt, tenantAltSecret, ownModel, provider, secret, model},
		attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, ownModel)
	before := overlayEnvelope(t, r, "tenant-b").Overlay
	if mustJSON(t, before.SelectionPolicy) != `{"mode":"random"}` || len(before.Candidates) != 2 {
		t.Fatalf("own overlay = %#v, want two grouped candidates with the random policy", before)
	}

	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	after := overlayEnvelope(t, r, "tenant-b").Overlay
	if len(after.Candidates) != 3 || mustJSON(t, after.Candidates[:2]) != mustJSON(t, before.Candidates) || mustJSON(t, after.SelectionPolicy) != `{"mode":"random"}` {
		t.Fatalf("attachment publish changed the tenant's selection: before %#v after %#v", before, after)
	}
}

func TestSharedProviderStateStaysWhileAnAttachmentUsesIt(t *testing.T) {
	provider, secret := attachmentProvider("team")
	chat := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	other := attachmentModel("other", "team", "other-model", time.Unix(200, 0), gatewayB)
	objects := append([]client.Object{provider, secret, chat, other}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	for _, model := range []*v1alpha1.ExternalModel{chat, other} {
		reconcileModel(t, r, model)
		setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	}
	for _, model := range []*v1alpha1.ExternalModel{chat, other} {
		reconcileModel(t, r, model)
	}
	detach := func(model *v1alpha1.ExternalModel) {
		t.Helper()
		got := getModel(t, r, model)
		got.Spec.GatewayRefs = nil
		if err := r.Update(context.Background(), got); err != nil {
			t.Fatal(err)
		}
		reconcileModel(t, r, model)
	}
	// Both models use the same provider and credential: one DestinationRule
	// and one copy, kept while either accepted route references them.
	detach(chat)
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", attachmentDestinationRuleName(gatewayB, "team", "openai")); err != nil {
		t.Fatalf("shared DestinationRule removed while another attachment uses it: %v", err)
	}
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "team", "provider-openai"); err != nil {
		t.Fatalf("shared provider Service removed while another attachment uses it: %v", err)
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "openai-key", "api-key")}, &copied); err != nil {
		t.Fatalf("shared credential copy removed while another attachment uses it: %v", err)
	}
	detach(other)
	assertAttachmentWithdrawn(t, r)
	assertTransportCollected(t, r, "team", "chat", "other")
}

func TestDeletingAttachedOnlyModelCompletesWithoutItsTenant(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	got := getModel(t, r, model)
	if !slices.Equal(got.Finalizers, []string{attachmentFinalizer}) {
		t.Fatalf("finalizers = %v, want only %s", got.Finalizers, attachmentFinalizer)
	}
	// The attached tenant is torn down first.
	for _, object := range attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis") {
		if err := r.Delete(context.Background(), object); err != nil {
			t.Fatal(err)
		}
	}
	if err := r.Delete(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if attachmentRouteExists(t, r, model) {
		t.Fatal("deleted model keeps its attachment route")
	}
	assertAttachmentWithdrawn(t, r)
	assertTransportCollected(t, r, "team", "chat")
	var deleted v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &deleted); !apierrors.IsNotFound(err) {
		t.Fatalf("model deletion did not complete: %v", err)
	}
}

func TestDeletingModelRemovesExtraAttachmentsWhileItsHomeWaits(t *testing.T) {
	gatewayA := v1alpha1.NamespacedObjectReference{Name: "gw-a", Namespace: "gateway-a"}
	provider, secret := attachmentProvider("tenant-a")
	model := attachmentModel("chat", "tenant-a", "chat-model", time.Unix(100, 0), gatewayA, gatewayB)
	sibling := attachmentModel("other", "tenant-a", "other-model", time.Unix(100, 0))
	objects := []client.Object{provider, secret, model, sibling}
	objects = append(objects, attachmentTenantObjects("tenant-a", "tenant-a", gatewayA, "praxis")...)
	objects = append(objects, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, sibling)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	got := getModel(t, r, model)
	if !controllerutil.ContainsFinalizer(got, externalModelFinalizer) || !controllerutil.ContainsFinalizer(got, attachmentFinalizer) {
		t.Fatalf("finalizers = %v, want the home and attachment finalizers", got.Finalizers)
	}

	// Without its MaasTenantConfig the home path keeps its existing
	// conservative wait while a sibling remains; the extra attachment does not.
	mtc := tenant.NewMaasTenantConfig()
	mtc.SetNamespace("tenant-a")
	mtc.SetName(tenant.MaasTenantConfigInstanceName)
	if err := r.Delete(context.Background(), mtc); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}); err == nil {
		t.Fatal("the home path must wait for a steady tenant while a sibling remains")
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("extra attachment route remains: %v", err)
	}
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", attachmentDestinationRuleName(gatewayB, "tenant-a", "openai")); !apierrors.IsNotFound(err) {
		t.Fatalf("extra attachment DestinationRule remains: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "tenant-a", "external-model-chat"); err != nil {
		t.Fatalf("home route removed while the home path waits: %v", err)
	}
	if got := getModel(t, r, model); !slices.Equal(got.Finalizers, []string{externalModelFinalizer}) {
		t.Fatalf("finalizers = %v, want only the home finalizer", got.Finalizers)
	}
}
