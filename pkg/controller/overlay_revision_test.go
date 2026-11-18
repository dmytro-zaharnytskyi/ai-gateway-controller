package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

// staleOverlayRevision is a revision no test overlay publishes.
var staleOverlayRevision = strings.Repeat("a", 64)

func servingRevisionLogs(revision string) func(context.Context, string, string, string) (string, error) {
	return func(context.Context, string, string, string) (string, error) {
		return `accepted_revision="` + revision + `" serving_revision="` + revision + `"`, nil
	}
}

// pendingHomeModel publishes the fixture's model while its tenant ExtProc
// starts, then makes that ExtProc available with ready pods that report
// serving revision.
func pendingHomeModel(t *testing.T, revision string) (*Reconciler, *v1alpha1.ExternalModel) {
	t.Helper()
	r, model := reconcilerFixture(t)
	setExtProcAvailable(t, r, "tenant-a", tenant.ID("tenant"), false)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); got.Status.Phase != modelPhasePending || readyReason(got) != reasonExtProcNotReady {
		t.Fatalf("fixture model before its ExtProc is available: %#v", got.Status)
	}
	seedSafeExternalModelRuntime(t, r, "tenant", "tenant-a", []*v1alpha1.ExternalModel{getModel(t, r, model)})
	r.readPodLogs = servingRevisionLogs(revision)
	return r, model
}

func reconcileForResult(t *testing.T, r *Reconciler, model *v1alpha1.ExternalModel) reconcile.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)})
	if err != nil {
		t.Fatalf("reconcile %s/%s: %v", model.Namespace, model.Name, err)
	}
	return result
}

// RHOAIENG-103283: a new model is Ready only once the tenant's ExtProc pods
// serve the overlay that contains it; until the kubelet syncs it, requests
// get empty 404s.
func TestNewHomeModelWaitsForPublishedOverlayRevision(t *testing.T) {
	r, model := pendingHomeModel(t, staleOverlayRevision)
	result := reconcileForResult(t, r, model)
	published := overlayEnvelope(t, r, "tenant-a").Revision.Value
	got := getModel(t, r, model)
	ready := apiMeta.FindStatusCondition(got.Status.Conditions, conditionReady)
	if got.Status.Phase != modelPhasePending || ready == nil || ready.Reason != reasonExtProcNotReady ||
		!strings.Contains(ready.Message, published) || result.RequeueAfter != attachmentRequeueInterval {
		t.Fatalf("model whose ExtProc serves another revision: result %#v, status %#v; want Pending %s naming %s",
			result, got.Status, reasonExtProcNotReady, published)
	}

	r.readPodLogs = servingRevisionLogs(published)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady || readyReason(got) != reasonReconciled {
		t.Fatalf("model whose ExtProc serves the published revision = %#v", got.Status)
	}
}

// Without positive evidence of another revision, readiness is reported as
// before the gate existed.
func TestUndeterminedServingRevisionKeepsReadinessAsBefore(t *testing.T) {
	for _, tt := range []struct {
		name  string
		setup func(t *testing.T, r *Reconciler)
	}{
		{name: "log read error", setup: func(_ *testing.T, r *Reconciler) {
			r.readPodLogs = func(context.Context, string, string, string) (string, error) {
				return "", errors.New("injected log read failure")
			}
		}},
		{name: "no revision line in the tail", setup: func(_ *testing.T, r *Reconciler) {
			r.readPodLogs = func(context.Context, string, string, string) (string, error) { return "starting\n", nil }
		}},
		{name: "pod not ready", setup: func(t *testing.T, r *Reconciler) {
			t.Helper()
			pod := &corev1.Pod{}
			key := client.ObjectKey{Namespace: "tenant-a", Name: tenant.PayloadProcessingExternalModelDeploymentName("tenant") + "-new-0"}
			if err := r.Get(context.Background(), key, pod); err != nil {
				t.Fatal(err)
			}
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}}
			if err := r.Status().Update(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, model := pendingHomeModel(t, staleOverlayRevision)
			tt.setup(t, r)
			reconcileModel(t, r, model)
			if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady {
				t.Fatalf("model with an undetermined serving revision = %#v, want Ready as before", got.Status)
			}
		})
	}
}

// Only a model not Ready at the gateway waits, and only while the published
// overlay was rendered less than overlayRevisionWaitLimit ago, so the wait
// never withdraws Ready and is bounded, however long the model was not Ready
// before. A model reports Ready per gateway: a gateway that gatewayRefs gained
// has no result yet, so a model Ready elsewhere waits there, while a model
// that names no gateway, as its own tenant alone serves it, stands for it with
// its Ready condition.
func TestAwaitsOverlayRevisionPerGateway(t *testing.T) {
	now := time.Now()
	renderedAgo := func(age time.Duration) string { return now.Add(-age).UTC().Format(time.RFC3339) }
	fresh, lapsed := renderedAgo(time.Minute), renderedAgo(overlayRevisionWaitLimit+time.Second)
	condition := func(status metav1.ConditionStatus, since time.Duration) []metav1.Condition {
		return []metav1.Condition{{Type: conditionReady, Status: status, LastTransitionTime: metav1.NewTime(now.Add(-since))}}
	}
	home := func(conditions []metav1.Condition) *v1alpha1.ExternalModel {
		model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0))
		model.Status.Conditions = conditions
		return model
	}
	readyOnB := func(gateways ...v1alpha1.NamespacedObjectReference) *v1alpha1.ExternalModel {
		model := home(condition(metav1.ConditionTrue, time.Minute))
		model.Spec.GatewayRefs = gateways
		model.Status.Gateways = []v1alpha1.ExternalModelGatewayStatus{{NamespacedObjectReference: gatewayB, Conditions: condition(metav1.ConditionTrue, time.Minute)}}
		return model
	}
	notReady := home(condition(metav1.ConditionFalse, time.Minute))
	longNotReady := home(condition(metav1.ConditionFalse, 10*time.Minute))
	for _, tc := range []struct {
		name       string
		model      *v1alpha1.ExternalModel
		gateway    v1alpha1.NamespacedObjectReference
		renderedAt string
		want       bool
	}{
		{name: "new model", model: home(nil), gateway: defaultGateway, renderedAt: fresh, want: true},
		{name: "model not Ready yet", model: notReady, gateway: defaultGateway, renderedAt: fresh, want: true},
		{name: "model long not Ready, overlay fresh", model: longNotReady, gateway: defaultGateway, renderedAt: fresh, want: true},
		{name: "overlay rendered past the wait limit", model: notReady, gateway: defaultGateway, renderedAt: lapsed},
		{name: "overlay rendered past the wait limit ahead of this clock", model: notReady, gateway: defaultGateway, renderedAt: renderedAgo(-overlayRevisionWaitLimit - time.Second)},
		{name: "overlay without rendered_at", model: notReady, gateway: defaultGateway},
		{name: "invalid rendered_at timestamp", model: notReady, gateway: defaultGateway, renderedAt: "later"},
		{name: "Ready model", model: home(condition(metav1.ConditionTrue, time.Minute)), gateway: defaultGateway, renderedAt: fresh},
		{name: "Ready attachment", model: readyOnB(gatewayB), gateway: gatewayB, renderedAt: fresh},
		{name: "gateway the Ready model gained", model: readyOnB(gatewayB, gatewayC), gateway: gatewayC, renderedAt: fresh, want: true},
		{name: "gained gateway past the wait limit", model: readyOnB(gatewayB, gatewayC), gateway: gatewayC, renderedAt: lapsed},
	} {
		if got := awaitsOverlayRevision(tc.model, tc.gateway, tc.renderedAt, now); got != tc.want {
			t.Errorf("%s: awaitsOverlayRevision(%s) = %t, want %t", tc.name, gatewayKey(tc.gateway), got, tc.want)
		}
	}
}

func TestAttachmentWaitsForPublishedOverlayRevision(t *testing.T) {
	provider, secret := attachmentProvider("team")
	model := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	objects := append([]client.Object{provider, secret, model}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, model)
	setRouteParentStatus(t, r, model, gatewayB, metav1.ConditionTrue, "Accepted")
	setExtProcAvailable(t, r, "tenant-b", tenant.ID("tenant-b"), false)
	reconcileModel(t, r, model)
	seedSafeExternalModelRuntime(t, r, "tenant-b", "tenant-b", nil)
	published := overlayEnvelope(t, r, "tenant-b").Revision.Value

	r.readPodLogs = servingRevisionLogs(staleOverlayRevision)
	result := reconcileForResult(t, r, model)
	got := getModel(t, r, model)
	ready := apiMeta.FindStatusCondition(gatewayResult(t, got, gatewayB).Conditions, conditionReady)
	if got.Status.Phase != modelPhasePending || ready == nil || ready.Reason != reasonExtProcNotReady ||
		!strings.Contains(ready.Message, published) || result.RequeueAfter != attachmentRequeueInterval {
		t.Fatalf("attachment whose tenant ExtProc serves another revision: result %#v, status %#v", result, got.Status)
	}

	r.readPodLogs = servingRevisionLogs(published)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("attachment whose tenant ExtProc serves the published revision = %#v", got.Status)
	}

	// Once Ready, the attachment stays Ready while the pods lag another revision.
	r.readPodLogs = servingRevisionLogs(staleOverlayRevision)
	reconcileModel(t, r, model)
	if got := getModel(t, r, model); got.Status.Phase != resolver.PhaseReady {
		t.Fatalf("Ready attachment while its tenant ExtProc lags = %#v, want it to stay Ready", got.Status)
	}
}
