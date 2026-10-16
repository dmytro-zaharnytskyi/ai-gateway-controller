package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

// Gateway attachments (spec.gatewayRefs). The home attachment is the one
// whose tenant's MaasTenantConfig is in the model namespace; it keeps the
// existing tenant-local path unchanged. Every other attachment is resolved to
// the tenant that owns its Gateway and gated by that tenant's backend
// handshake on its own.
const (
	reasonTenantNotResolved = "TenantNotResolved"
	reasonGatewayNotManaged = "GatewayNotManaged"
	reasonAmbiguousTenant   = "AmbiguousTenant"
	reasonTenantNotPraxis   = "TenantNotPraxis"
	reasonTenantNotSteady   = "TenantNotSteady"
	reasonAttachmentPending = "AttachmentPending"

	modelPhasePending = "Pending"
	modelPhaseFailed  = "Failed"

	attachmentRequeueInterval = 5 * time.Second
)

var httpRouteGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}

// attachmentTenant is the Praxis tenant that owns an attached Gateway.
type attachmentTenant struct {
	gateway   v1alpha1.NamespacedObjectReference
	namespace string // MaasTenantConfig namespace
	tenantID  string
}

// attachmentResult is the outcome of one gateway attachment.
type attachmentResult struct {
	gateway   v1alpha1.NamespacedObjectReference
	ready     bool
	pending   bool
	reason    string
	message   string
	routeName string
	// env is the overlay the home attachment distributed.
	env *envelope.Envelope
}

func gatewayKey(gateway v1alpha1.NamespacedObjectReference) string {
	return gateway.Namespace + "/" + gateway.Name
}

func hasGatewayRef(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) bool {
	return slices.Contains(model.Spec.GatewayRefs, gateway)
}

func newHTTPRoute() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(httpRouteGVK)
	return u
}

// gatewayAttachedModels enqueues models attached to the AITenant's Gateway.
func (r *Reconciler) gatewayAttachedModels(ctx context.Context, ait *unstructured.Unstructured) []reconcile.Request {
	name, namespace, ok := tenant.GatewayRef(ait)
	if !ok {
		return nil
	}
	gateway := v1alpha1.NamespacedObjectReference{Name: name, Namespace: namespace}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		r.Log.Error(err, "list models for Gateway attachment event", "gateway", gatewayKey(gateway))
		return nil
	}
	var requests []reconcile.Request
	for i := range models.Items {
		if r.namespaceAllowed(models.Items[i].Namespace) && hasGatewayRef(&models.Items[i], gateway) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&models.Items[i])})
		}
	}
	return requests
}

// resolveGatewayTenant resolves a gatewayRef to its owning AITenant through
// status.gatewayRef and returns that tenant's namespace. An empty namespace
// means the attachment is unsupported; result says why.
func (r *Reconciler) resolveGatewayTenant(ctx context.Context, gateway v1alpha1.NamespacedObjectReference) (*unstructured.Unstructured, string, attachmentResult, error) {
	result := attachmentResult{gateway: gateway}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(tenant.AITenantGVK.GroupVersion().WithKind("AITenantList"))
	if err := r.List(ctx, list); err != nil {
		return nil, "", result, fmt.Errorf("list AITenants for Gateway %s: %w", gatewayKey(gateway), err)
	}
	var owners []*unstructured.Unstructured
	for i := range list.Items {
		if name, namespace, ok := tenant.GatewayRef(&list.Items[i]); ok && name == gateway.Name && namespace == gateway.Namespace {
			owners = append(owners, &list.Items[i])
		}
	}
	switch {
	case len(owners) == 0:
		result.reason, result.message = reasonGatewayNotManaged, "no AITenant manages this Gateway"
		return nil, "", result, nil
	case len(owners) > 1:
		result.reason, result.message = reasonAmbiguousTenant, fmt.Sprintf("%d AITenants reference this Gateway", len(owners))
		return nil, "", result, nil
	}
	tenantNamespace, ok := tenant.ConfigNamespace(owners[0])
	if !ok {
		result.reason, result.message = reasonTenantNotSteady, "the owning AITenant has no tenant namespace yet"
		return nil, "", result, nil
	}
	if !r.namespaceAllowed(tenantNamespace) {
		result.reason, result.message = reasonGatewayNotManaged, fmt.Sprintf("tenant namespace %s is outside this controller's namespace scope", tenantNamespace)
		return nil, "", result, nil
	}
	return owners[0], tenantNamespace, result, nil
}

// gateAttachmentTenant applies the existing backend handshake gate to the
// attached tenant: it must select Praxis and be steady. cleanup reports an
// explicit non-Praxis selection, the existing cleanup command.
func (r *Reconciler) gateAttachmentTenant(ctx context.Context, ait *unstructured.Unstructured, tenantNamespace string, result attachmentResult) (attachmentTenant, attachmentResult, bool, error) {
	t := attachmentTenant{gateway: result.gateway, namespace: tenantNamespace, tenantID: tenant.ID(ait.GetName())}
	live, err := r.liveTenantResolution(ctx, tenantNamespace)
	if err != nil {
		return t, result, false, err
	}
	switch {
	case live.missingConfig:
		result.reason, result.message = reasonTenantNotSteady, "the tenant's MaasTenantConfig is missing; retaining existing serving state"
	case !live.selected:
		result.reason, result.message = reasonTenantNotPraxis, "the tenant does not select Praxis"
		return t, result, live.mtc.GetDeletionTimestamp().IsZero(), nil
	case live.aitenant == nil || live.aitenant.GetUID() != ait.GetUID() ||
		!live.mtc.GetDeletionTimestamp().IsZero() || !live.aitenant.GetDeletionTimestamp().IsZero():
		result.reason, result.message = reasonTenantNotSteady, "the tenant's owning AITenant is changing"
	case live.status != tenant.PayloadProcessingStatusSteady:
		result.reason, result.message = reasonTenantNotSteady, "the tenant's payload-processing-status is not steady"
	case !tenant.DeployReady(live.aitenant):
		result.reason, result.message = reasonTenantNotSteady, "the tenant's AITenant is not DeployReady"
	default:
		if name, namespace, ok := tenant.GatewayRef(live.aitenant); !ok || name != t.gateway.Name || namespace != t.gateway.Namespace {
			result.reason, result.message = reasonTenantNotSteady, "the tenant's Gateway reference changed"
			break
		}
		result.ready = true
	}
	return t, result, false, nil
}

// reconcileAttachedModel reconciles a model with explicit gatewayRefs: the
// home attachment through the existing path, every other attachment on its
// own, then one aggregated status write.
func (r *Reconciler) reconcileAttachedModel(ctx context.Context, model *v1alpha1.ExternalModel) (reconcile.Result, error) {
	type target struct {
		ait             *unstructured.Unstructured
		tenantNamespace string
		result          attachmentResult
	}
	targets := make([]target, 0, len(model.Spec.GatewayRefs))
	home := -1
	for _, gateway := range model.Spec.GatewayRefs {
		ait, tenantNamespace, result, err := r.resolveGatewayTenant(ctx, gateway)
		if err != nil {
			return reconcile.Result{}, err
		}
		if tenantNamespace == model.Namespace {
			home = len(targets)
		}
		targets = append(targets, target{ait: ait, tenantNamespace: tenantNamespace, result: result})
	}

	var result reconcile.Result
	var errs []error
	var homeErr error
	results := make([]attachmentResult, len(targets))
	homeReported := false
	if home >= 0 {
		result, homeErr = r.reconcileHomeModel(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(model)}, model, func(ready bool, reason, message string, env *envelope.Envelope) error {
			homeReported = true
			results[home] = attachmentResult{gateway: targets[home].result.gateway, ready: ready, reason: reason, message: message, routeName: modelRouteName(model.Name), env: env}
			return nil
		})
		if homeErr != nil {
			errs = append(errs, homeErr)
		}
	} else if err := r.leaveHomeTenant(ctx, model); err != nil {
		errs = append(errs, err)
	}

	for i, target := range targets {
		if i == home {
			continue
		}
		// Unsupported attachments retain existing state and never fall back
		// to another Gateway.
		results[i] = target.result
		if target.tenantNamespace == "" {
			continue
		}
		_, gated, _, err := r.gateAttachmentTenant(ctx, target.ait, target.tenantNamespace, target.result)
		results[i] = gated
		switch {
		case err != nil:
			errs = append(errs, err)
			results[i] = failedAttachment(gated, err)
		case gated.ready:
			// Serving through another tenant's Gateway follows; until then
			// the attachment is reported and never served.
			results[i].ready, results[i].reason = false, reasonAttachmentPending
			results[i].message = "serving through another tenant's Gateway is not implemented yet"
		}
	}

	if home >= 0 && !homeReported {
		// The home path stopped without a status: an explicit non-Praxis
		// selection (cleanup), a requeue for a changing tenant, or an error.
		results[home] = attachmentResult{gateway: targets[home].result.gateway, reason: reasonTenantNotPraxis,
			message: "the model's own tenant does not select Praxis"}
		switch {
		case homeErr != nil:
			// Only the home path's own error; other attachments report theirs.
			results[home] = failedAttachment(attachmentResult{gateway: targets[home].result.gateway}, homeErr)
		case result.Requeue || result.RequeueAfter > 0: //nolint:staticcheck // the home path's existing requeue signal
			results[home].pending, results[home].reason = true, reasonTenantNotSteady
			results[home].message = "the model's own tenant is not steady"
		}
	}
	if len(results) > 0 {
		if err := r.writeAttachedStatus(ctx, model, results); err != nil {
			errs = append(errs, err)
		}
	}
	for _, res := range results {
		if res.pending {
			// Route acceptance and credential copies are observed through
			// the cache; recheck instead of waiting for an unrelated event.
			result.RequeueAfter = attachmentRequeueInterval
		}
	}
	return result, errors.Join(errs...)
}

// failedAttachment reports an attachment whose reconcile returned an error and
// is retried. Status conditions require a reason.
func failedAttachment(result attachmentResult, err error) attachmentResult {
	if result.reason == "" {
		result.ready, result.reason, result.message = false, reasonReconcileFailed, err.Error()
	}
	return result
}

// writeAttachedStatus aggregates the attachment results into one status:
// Ready requires every attachment, otherwise the first failing attachment in
// gatewayRefs order is reported.
func (r *Reconciler) writeAttachedStatus(ctx context.Context, model *v1alpha1.ExternalModel, results []attachmentResult) error {
	for _, res := range results {
		if res.ready {
			continue
		}
		phase := modelPhaseFailed
		if res.pending {
			phase = modelPhasePending
		}
		message := fmt.Sprintf("Gateway %s: %s", gatewayKey(res.gateway), res.message)
		return r.setModelStatus(ctx, model, phase, false, res.reason, message, "")
	}
	for _, res := range results {
		if res.env != nil {
			// The home attachment keeps its existing overlay attestation.
			return r.updateModelStatus(ctx, model, true, reasonReconciled, res.message, res.env)
		}
	}
	routeName := ""
	if len(results) == 1 {
		routeName = results[0].routeName
	}
	return r.setModelStatus(ctx, model, resolver.PhaseReady, true, reasonReconciled,
		fmt.Sprintf("served through %d Gateway attachment(s)", len(results)), routeName)
}

// leaveHomeTenant republishes the model's own tenant without it when its
// gatewayRefs no longer name that tenant's Gateway. As on the home path, an
// explicit non-Praxis selection cleans up; otherwise the home state is kept
// until the tenant is steady.
func (r *Reconciler) leaveHomeTenant(ctx context.Context, model *v1alpha1.ExternalModel) error {
	if hasHome, err := r.hasHomeRoute(ctx, model); err != nil || !hasHome {
		return err
	}
	live, err := r.liveTenantResolution(ctx, model.Namespace)
	if err != nil {
		return err
	}
	if live.missingConfig {
		return nil
	}
	if !live.selected {
		// An explicit non-Praxis selection is the home path's cleanup
		// command; it does not wait for a steady tenant.
		return r.cleanupUnselectedModel(ctx, model)
	}
	if live.aitenant == nil || live.status != tenant.PayloadProcessingStatusSteady ||
		!live.mtc.GetDeletionTimestamp().IsZero() || !live.aitenant.GetDeletionTimestamp().IsZero() ||
		!tenant.DeployReady(live.aitenant) {
		return nil
	}
	gatewayName, gatewayNamespace, ok := tenant.GatewayRef(live.aitenant)
	if !ok || tenant.ServedByTenantGateway(model, gatewayName, gatewayNamespace) {
		// Still bound to its own tenant; the Gateway lookup was unresolved.
		return nil
	}
	return r.republishHomeWithout(ctx, model, tenant.ID(live.aitenant.GetName()), gatewayName, gatewayNamespace)
}

// hasHomeRoute reports whether the model has its tenant-local HTTPRoute.
func (r *Reconciler) hasHomeRoute(ctx context.Context, model *v1alpha1.ExternalModel) (bool, error) {
	route := newHTTPRoute()
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}, route); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return route.GetLabels()["inference.opendatahub.io/external-model"] == model.Name, nil
}

// setModelStatus writes the model status with an explicit phase. The route
// name is recorded only when ready, matching updateModelStatus.
func (r *Reconciler) setModelStatus(ctx context.Context, m *v1alpha1.ExternalModel, phase string, ready bool, reason, message, routeName string) error {
	old := m.DeepCopy()
	m.Status.ObservedGeneration = m.Generation
	m.Status.Phase = phase
	if ready {
		m.Status.HTTPRouteName = routeName
	}
	setCondition(&m.Status.Conditions, conditionOverlayDistributed, boolStatus(ready), reason, message)
	setCondition(&m.Status.Conditions, conditionReady, boolStatus(ready), reason, message)
	if equality.Semantic.DeepEqual(old.Status, m.Status) {
		return nil
	}
	if err := r.Status().Patch(ctx, m, client.MergeFrom(old)); err != nil {
		if !apierrors.IsConflict(err) {
			r.Log.Error(err, "update model status", "model", m.Name)
		}
		return fmt.Errorf("update model status %s/%s: %w", m.Namespace, m.Name, err)
	}
	return nil
}
