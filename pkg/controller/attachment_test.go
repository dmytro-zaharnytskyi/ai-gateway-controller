package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
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
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	if len(got.Status.Gateways) != 0 {
		t.Fatalf("status.gateways belongs to RHOAIENG-90802, got %#v", got.Status.Gateways)
	}
}

func TestAttachmentDeletionRemovesOnlyItsOwnObjects(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, model)
	if !strings.Contains(strings.Join(getModel(t, r, model).Finalizers, ","), externalModelFinalizer) {
		t.Fatal("served attachment must hold the cleanup finalizer")
	}

	if err := r.Delete(context.Background(), getModel(t, r, model)); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("attachment HTTPRoute survived deletion: %v", err)
	}
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-b", attachmentDestinationRuleName(gatewayB, "team", "openai")); !apierrors.IsNotFound(err) {
		t.Fatalf("attachment DestinationRule survived deletion: %v", err)
	}
	var copied corev1.Secret
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: credentialCopyName("tenant-b", "team", "openai-key", "api-key")}, &copied); !apierrors.IsNotFound(err) {
		t.Fatalf("credential copy survived deletion: %v", err)
	}
	if _, err := getUnstructured(r, schema.GroupVersionKind{Version: "v1", Kind: "Service"}, "team", "provider-openai"); !apierrors.IsNotFound(err) {
		t.Fatalf("unreferenced user-namespace provider Service survived deletion: %v", err)
	}
	var gone v1alpha1.ExternalModel
	if err := r.Get(context.Background(), client.ObjectKeyFromObject(model), &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("model finalizer was not released: %v", err)
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

	// Removing one gatewayRef deletes only that attachment.
	got.Spec.GatewayRefs = []v1alpha1.NamespacedObjectReference{gatewayB}
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, httpRouteGVK, "team", routeC); !apierrors.IsNotFound(err) {
		t.Fatalf("detached route survived: %v", err)
	}
	if _, err := getUnstructured(r, destinationRuleGVK, "gateway-c", attachmentDestinationRuleName(gatewayC, "team", "openai")); !apierrors.IsNotFound(err) {
		t.Fatalf("detached DestinationRule survived: %v", err)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", routeB); err != nil {
		t.Fatalf("remaining attachment route removed: %v", err)
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
	if msg := apiMeta.FindStatusCondition(getModel(t, r, conflicting).Status.Conditions, conditionReady).Message; !strings.Contains(msg, "tenant-b/own") {
		t.Fatalf("conflict message %q does not name the winner", msg)
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
	if got := getModel(t, attached, attachedModel); got.Status.Phase != resolver.PhaseReady || got.Status.HTTPRouteName != "external-model-chat" {
		t.Fatalf("own-gateway status = %#v", got.Status)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
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

func TestModelWithoutTenantOrGatewayRefsIsTenantNotResolved(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
	r := attachmentReconciler(t, provider, secret, model)
	reconcileModel(t, r, model)
	got := getModel(t, r, model)
	condition := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if got.Status.Phase != modelPhasePending || condition == nil || condition.Status != metav1.ConditionFalse ||
		condition.Reason != reasonTenantNotResolved || !strings.Contains(condition.Message, "spec.gatewayRefs") {
		t.Fatalf("status = %#v", got.Status)
	}
	if _, err := getUnstructured(r, httpRouteGVK, "team", "external-model-chat"); !apierrors.IsNotFound(err) {
		t.Fatalf("serving state created without a tenant: %v", err)
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

func TestRemovingAllGatewayRefsReleasesAttachments(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	got := getModel(t, r, model)
	got.Spec.GatewayRefs = nil
	if err := r.Update(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	reconcileModel(t, r, model)
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("attachment route remains after removing gatewayRefs: %v", err)
	}
	assertAttachmentWithdrawn(t, r)
	reconcileModel(t, r, model)
	got = getModel(t, r, model)
	if len(got.Finalizers) != 0 || readyReason(got) != reasonTenantNotResolved {
		t.Fatalf("finalizers=%v reason=%q, want released and %q", got.Finalizers, readyReason(got), reasonTenantNotResolved)
	}
}

func TestRevokedAcceptanceWithdrawsAttachment(t *testing.T) {
	r, model := servedAttachmentFixture(t)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionFalse, "NotAllowedByListeners")
	reconcileModel(t, r, model)
	assertAttachmentWithdrawn(t, r)
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("overlay entry remains after revocation: %v", err)
	}
	if got := getModel(t, r, model); readyReason(got) != "NotAllowedByListeners" {
		t.Fatalf("reason = %q", readyReason(got))
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
}

func assertNoTenantOverlay(t *testing.T, r *Reconciler) {
	t.Helper()
	var overlay corev1.ConfigMap
	if err := r.Get(context.Background(), client.ObjectKey{Namespace: "tenant-b", Name: "routing-overlay"}, &overlay); !apierrors.IsNotFound(err) {
		t.Fatalf("overlay entry remains: %v", err)
	}
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
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); !apierrors.IsNotFound(err) {
		t.Fatalf("excluded model keeps its route: %v", err)
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
	if _, err := getUnstructured(r, httpRouteGVK, "team", attachmentRouteName(model, gatewayB)); err != nil {
		t.Fatalf("attachment route: %v", err)
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
