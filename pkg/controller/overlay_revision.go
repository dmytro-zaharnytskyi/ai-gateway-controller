package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
)

// overlayRevisionWaitLimit bounds how long a model that is not Ready waits for
// the tenant's ExtProc pods to serve the published overlay. The kubelet syncs
// a mounted ConfigMap within about 90 seconds; past the limit something else
// is wrong and the model is reported as before.
const overlayRevisionWaitLimit = 2 * time.Minute

// overlayRevisionPending reports whether a model that is not Ready yet must
// keep waiting because a ready pod of the tenant's available ExtProc
// Deployment reports serving another overlay revision than the published one:
// until the kubelet syncs the mounted overlay, requests for a new model get
// empty 404s (RHOAIENG-103283). It only ever delays a model becoming Ready,
// never withdraws Ready: the shared overlay is republished for other models
// all the time. Anything it cannot determine leaves the model as it was
// before.
func (r *Reconciler) overlayRevisionPending(ctx context.Context, model *v1alpha1.ExternalModel, t attachmentTenant, extProc *unstructured.Unstructured) (bool, string) {
	now := time.Now()
	if !awaitsOverlayRevision(model, t.gateway, now) || r.readPodLogs == nil {
		return false, ""
	}
	env, err := r.readPublishedEnvelope(ctx, t.namespace)
	if err != nil || env == nil || env.Revision.Value == "" {
		return false, ""
	}
	deployment := &appsv1.Deployment{}
	if runtime.DefaultUnstructuredConverter.FromUnstructured(extProc.Object, deployment) != nil || !managedExtProcDeployment(deployment) {
		return false, ""
	}
	pod := r.extProcPodOnOtherRevision(ctx, t.namespace, deployment, env.Revision.Value)
	if pod == "" {
		return false, ""
	}
	// The cache can lag a Ready status written moments ago.
	live := &v1alpha1.ExternalModel{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(model), live); err != nil || !awaitsOverlayRevision(live, t.gateway, now) {
		return false, ""
	}
	return true, fmt.Sprintf("waiting for the tenant ExtProc to serve routing overlay revision %s: pod %s/%s does not serve it yet",
		env.Revision.Value, t.namespace, pod)
}

// awaitsOverlayRevision reports whether the model may wait for the overlay
// revision at the gateway: it is not Ready there, and it turned not Ready less
// than overlayRevisionWaitLimit ago. The wait never starts before that
// transition, so the existing condition bounds it without new state. A model
// without a Ready condition there has not reported one yet, so its wait starts
// now.
func awaitsOverlayRevision(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference, now time.Time) bool {
	ready := readyConditionFor(model, gateway)
	if ready == nil {
		return true
	}
	return ready.Status != metav1.ConditionTrue && now.Sub(ready.LastTransitionTime.Time) < overlayRevisionWaitLimit
}

// readyConditionFor returns the Ready condition the model reports for the
// gateway: its status.gateways result for it. A model that names no gateway
// has none, as only its own tenant serves it, so its Ready condition stands
// for that tenant's gateway. A gateway that gatewayRefs names but that has no
// result yet was attached since, and has reported nothing.
func readyConditionFor(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) *metav1.Condition {
	for i := range model.Status.Gateways {
		if model.Status.Gateways[i].NamespacedObjectReference == gateway {
			return apiMeta.FindStatusCondition(model.Status.Gateways[i].Conditions, conditionReady)
		}
	}
	if slices.Contains(model.Spec.GatewayRefs, gateway) {
		return nil
	}
	return apiMeta.FindStatusCondition(model.Status.Conditions, conditionReady)
}

// extProcPodOnOtherRevision returns a running, ready pod of the tenant's
// ExtProc Deployment that reports serving an overlay revision other than
// revision, or "" when there is none or it cannot be told.
func (r *Reconciler) extProcPodOnOtherRevision(ctx context.Context, namespace string, deployment *appsv1.Deployment, revision string) string {
	replicaSets, err := r.extProcReplicaSets(ctx, namespace, deployment)
	if err != nil {
		return ""
	}
	pods, err := r.extProcPods(ctx, namespace, deployment, replicaSets)
	if err != nil {
		return ""
	}
	for _, pod := range pods {
		if pod.replicaSet == nil || pod.Status.Phase != corev1.PodRunning || !pod.DeletionTimestamp.IsZero() || !podReady(pod.Pod) {
			continue
		}
		// An unreadable log or one without a revision line proves nothing.
		if serves, err := r.podServesOverlayRevision(ctx, pod.Pod, revision); err == nil && !serves {
			return pod.Name
		}
	}
	return ""
}
