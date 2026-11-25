package controller

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
)

// overlayRevisionWaitLimit bounds how long after the published overlay was
// rendered a model that is not Ready waits for the tenant's ExtProc pods to
// serve it. The kubelet syncs a mounted ConfigMap within about 90 seconds;
// past the limit something else is wrong and the model is reported as before.
const overlayRevisionWaitLimit = 2 * time.Minute

// overlayRevisionCheckReuse is how long a tenant's serving-revision check is
// reused. The answer is the same for every model of the tenant that waits for
// one overlay revision, so models created together would otherwise each list
// the pods and read their logs on every recheck. It is shorter than
// attachmentRequeueInterval, so a model's timed recheck never reuses its own
// previous answer.
const overlayRevisionCheckReuse = 2 * time.Second

// revisionCheck is what extProcPodOnOtherRevision found for a tenant at a
// time: the pod that serves another revision than revision, or "" when every
// ready pod serves it.
type revisionCheck struct {
	revision string
	pod      string
	at       time.Time
}

// revisionChecks holds the latest revisionCheck of each tenant namespace.
type revisionChecks struct {
	mu     sync.Mutex
	latest map[string]revisionCheck
}

// lookup returns the pod of a check of namespace for revision that was made
// less than overlayRevisionCheckReuse before now.
func (c *revisionChecks) lookup(namespace, revision string, now time.Time) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	check, found := c.latest[namespace]
	if !found || check.revision != revision || now.Before(check.at) || now.Sub(check.at) >= overlayRevisionCheckReuse {
		return "", false
	}
	return check.pod, true
}

// record stores the check of namespace and drops the checks that can no
// longer be reused, so only tenants checked moments ago are held.
func (c *revisionChecks) record(namespace string, check revisionCheck) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.latest == nil {
		c.latest = map[string]revisionCheck{}
	}
	for other, old := range c.latest {
		if check.at.Sub(old.at) >= overlayRevisionCheckReuse {
			delete(c.latest, other)
		}
	}
	c.latest[namespace] = check
}

// overlayRevisionPending reports whether a model that is not Ready yet must
// keep waiting because a ready pod of the tenant's available ExtProc
// Deployment reports serving another overlay revision than the published one:
// until the kubelet syncs the mounted overlay, requests for a new model get
// empty 404s (RHOAIENG-103283). It only ever delays a model becoming Ready,
// never withdraws Ready: the shared overlay is republished for other models
// all the time. Anything it cannot determine leaves the model as it was
// before.
func (r *Reconciler) overlayRevisionPending(ctx context.Context, model *v1alpha1.ExternalModel, t attachmentTenant, extProc *unstructured.Unstructured) (bool, string) {
	if r.readPodLogs == nil || readyAt(model, t.gateway) {
		return false, ""
	}
	env, err := r.readPublishedEnvelope(ctx, t.namespace)
	now := time.Now()
	if err != nil || env == nil || env.Revision.Value == "" || !awaitsOverlayRevision(model, t.gateway, env.Provenance.RenderedAt, now) {
		return false, ""
	}
	deployment := &appsv1.Deployment{}
	if runtime.DefaultUnstructuredConverter.FromUnstructured(extProc.Object, deployment) != nil || !managedExtProcDeployment(deployment) {
		return false, ""
	}
	pod := r.extProcPodOnOtherRevision(ctx, t.namespace, deployment, env.Revision.Value, now)
	if pod == "" {
		return false, ""
	}
	// The cache can lag a Ready status written moments ago.
	live := &v1alpha1.ExternalModel{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(model), live); err != nil || !awaitsOverlayRevision(live, t.gateway, env.Provenance.RenderedAt, now) {
		return false, ""
	}
	return true, fmt.Sprintf("waiting for the tenant ExtProc to serve routing overlay revision %s: pod %s/%s does not serve it yet",
		env.Revision.Value, t.namespace, pod)
}

// awaitsOverlayRevision reports whether the model may wait at the gateway for
// the published overlay rendered at renderedAt (RFC3339): it is not Ready
// there, and that overlay was rendered less than overlayRevisionWaitLimit ago.
// The overlay's age bounds the wait, not the Ready condition's transition
// time, which stays put while the model remains not Ready for another reason,
// such as a credential Secret created late, until the overlay that finally
// adds it. A missing or invalid timestamp starts no wait.
func awaitsOverlayRevision(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference, renderedAt string, now time.Time) bool {
	rendered, err := time.Parse(time.RFC3339, renderedAt)
	if err != nil || readyAt(model, gateway) {
		return false
	}
	// A time ahead of this clock cannot extend the wait past the limit either.
	age := now.Sub(rendered)
	return age < overlayRevisionWaitLimit && age > -overlayRevisionWaitLimit
}

// readyAt reports whether the model is Ready at the gateway: its
// status.gateways result for it. A model that names no gateway has none, as
// only its own tenant serves it, so its Ready condition stands for that
// tenant's gateway. A gateway that gatewayRefs names but that has no result
// yet was attached since, and has reported nothing.
func readyAt(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) bool {
	conditions := model.Status.Conditions
	if i := slices.IndexFunc(model.Status.Gateways, func(g v1alpha1.ExternalModelGatewayStatus) bool {
		return g.NamespacedObjectReference == gateway
	}); i >= 0 {
		conditions = model.Status.Gateways[i].Conditions
	} else if slices.Contains(model.Spec.GatewayRefs, gateway) {
		return false
	}
	return apiMeta.IsStatusConditionTrue(conditions, conditionReady)
}

// extProcPodOnOtherRevision returns a running, ready pod of the tenant's
// ExtProc Deployment that reports serving an overlay revision other than
// revision, or "" when there is none or it cannot be told. A check made less
// than overlayRevisionCheckReuse before now is reused.
func (r *Reconciler) extProcPodOnOtherRevision(ctx context.Context, namespace string, deployment *appsv1.Deployment, revision string, now time.Time) string {
	if pod, found := r.revisionChecks.lookup(namespace, revision, now); found {
		return pod
	}
	replicaSets, err := r.extProcReplicaSets(ctx, namespace, deployment)
	if err != nil {
		return ""
	}
	pods, err := r.extProcPods(ctx, namespace, deployment, replicaSets)
	if err != nil {
		return ""
	}
	behind, undetermined := "", false
	for _, pod := range pods {
		if pod.replicaSet == nil || pod.Status.Phase != corev1.PodRunning || !pod.DeletionTimestamp.IsZero() || !podReady(pod.Pod) {
			continue
		}
		// An unreadable log or one without a revision line proves nothing.
		serves, err := r.podServesOverlayRevision(ctx, pod.Pod, revision)
		if err != nil {
			undetermined = true
			continue
		}
		if !serves {
			behind = pod.Name
			break
		}
	}
	// Only an answer that was determined is reused: a pod that serves another
	// revision, or every ready pod serving this one.
	if behind != "" || !undetermined {
		r.revisionChecks.record(namespace, revisionCheck{revision: revision, pod: behind, at: now})
	}
	return behind
}
