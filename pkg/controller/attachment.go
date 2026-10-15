package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

// Gateway attachments (spec.gatewayRefs). The home attachment is the one
// whose tenant's MaasTenantConfig is in the model namespace; it keeps the
// existing tenant-local path unchanged. Every other attachment contributes the
// model to a tenant from another namespace: an HTTPRoute in the model
// namespace, and, only after the Gateway accepts that route, a hashed
// DestinationRule in the Gateway namespace, credential copies in the tenant
// namespace and the tenant's overlay entries.
const (
	reasonTenantNotResolved = "TenantNotResolved"
	reasonGatewayNotManaged = "GatewayNotManaged"
	reasonAmbiguousTenant   = "AmbiguousTenant"
	reasonTenantNotPraxis   = "TenantNotPraxis"
	reasonTenantNotSteady   = "TenantNotSteady"
	reasonModelNameConflict = "ModelNameConflict"
	reasonTooManyProviders  = "TooManyProviders"
	reasonAttachmentPending = "AttachmentPending"

	modelPhasePending = "Pending"
	modelPhaseFailed  = "Failed"

	// maxAttachedProviders keeps an attached model's HTTPRoute within the
	// Gateway API limit of 16 rules: two per provider plus two sink rules.
	maxAttachedProviders = 7

	attachmentRequeueInterval = 5 * time.Second

	annotationAttachmentModel   = "inference.opendatahub.io/attachment-model"
	annotationAttachmentGateway = "inference.opendatahub.io/attachment-gateway"
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

// attachedModel is one model from another namespace attached to a tenant.
// A non-empty reason excludes it from the tenant.
type attachedModel struct {
	model     *v1alpha1.ExternalModel
	routeName string
	routes    []resolver.Route // stable IDs rewritten for the tenant
	reason    string
	message   string
}

func gatewayKey(gateway v1alpha1.NamespacedObjectReference) string {
	return gateway.Namespace + "/" + gateway.Name
}

func hasGatewayRef(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) bool {
	return slices.Contains(model.Spec.GatewayRefs, gateway)
}

func clientModelName(model *v1alpha1.ExternalModel) string {
	if model.Spec.ModelName != "" {
		return model.Spec.ModelName
	}
	return model.Name
}

func shortHash(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "/")))
	return hex.EncodeToString(sum[:])[:12]
}

// attachmentRouteName names an extra attachment's HTTPRoute in the model
// namespace. The home attachment keeps modelRouteName.
func attachmentRouteName(model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) string {
	hash := shortHash(model.Namespace, model.Name, gateway.Namespace, gateway.Name)
	if name := modelRouteName(model.Name) + "-" + hash; len(name) <= 253 {
		return name
	}
	// ExternalModel names start with an alphanumeric, so the double dash
	// never matches a home route.
	return modelRoutePrefix + "-" + hash
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

// attachmentRouteModels maps an attachment HTTPRoute to its model.
func (r *Reconciler) attachmentRouteModels(_ context.Context, route *unstructured.Unstructured) []reconcile.Request {
	if _, ok := route.GetLabels()[tenant.LabelAttachmentTenant]; !ok {
		return nil
	}
	model := route.GetAnnotations()[annotationAttachmentModel]
	if model == "" || !r.namespaceAllowed(route.GetNamespace()) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: route.GetNamespace(), Name: model}}}
}

// sameNameModels enqueues attached models in other namespaces with the same
// client-facing name, so a model-name conflict is re-decided for them.
func (r *Reconciler) sameNameModels(ctx context.Context, obj client.Object) []reconcile.Request {
	changed, ok := obj.(*v1alpha1.ExternalModel)
	if !ok {
		return nil
	}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		r.Log.Error(err, "list models for model-name conflict", "model", changed.Name)
		return nil
	}
	var requests []reconcile.Request
	for i := range models.Items {
		m := &models.Items[i]
		if m.Namespace != changed.Namespace && len(m.Spec.GatewayRefs) > 0 && r.namespaceAllowed(m.Namespace) && clientModelName(m) == clientModelName(changed) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(m)})
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
	case !tenant.IsActive(live.aitenant) || !tenant.StatusIsCurrent(live.aitenant):
		result.reason, result.message = reasonTenantNotSteady, "the tenant's AITenant is not Active"
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
		t, gated, cleanup, err := r.gateAttachmentTenant(ctx, target.ait, target.tenantNamespace, target.result)
		results[i] = gated
		switch {
		case err != nil:
			errs = append(errs, err)
			results[i] = failedAttachment(gated, err)
		case cleanup:
			// An explicit non-Praxis selection is the existing cleanup
			// command: the attachment's route is removed.
			if err := r.removeAttachmentRoute(ctx, model, gated.gateway); err != nil {
				errs = append(errs, err)
			}
		case gated.ready:
			if results[i], err = r.reconcileAttachment(ctx, model, t); err != nil {
				errs = append(errs, err)
				results[i] = failedAttachment(results[i], err)
			}
		}
	}
	if err := r.removeDetachedRoutes(ctx, model); err != nil {
		errs = append(errs, err)
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
		!tenant.IsActive(live.aitenant) || !tenant.StatusIsCurrent(live.aitenant) {
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

// reconcileAttachment serves the model through a steady Praxis tenant of
// another namespace.
func (r *Reconciler) reconcileAttachment(ctx context.Context, model *v1alpha1.ExternalModel, t attachmentTenant) (attachmentResult, error) {
	result := attachmentResult{gateway: t.gateway, routeName: attachmentRouteName(model, t.gateway)}
	candidates, err := r.attachmentCandidates(ctx, t)
	if err != nil {
		return result, err
	}
	idx := slices.IndexFunc(candidates, func(c attachedModel) bool { return c.model.UID == model.UID })
	if idx < 0 {
		result.pending, result.reason, result.message = true, reasonAttachmentPending, "waiting for the model to be observed"
		return result, nil
	}
	self := candidates[idx]
	if self.reason != "" {
		// A model-name conflict, a resolution or credential failure (such
		// as a deleted source Secret) and too many providers all exclude the
		// model: no route, overlay entry or transport on this Gateway.
		result.reason, result.message = self.reason, self.message
		return result, r.removeAttachmentRoute(ctx, model, t.gateway)
	}
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(model.Namespace)); err != nil {
		return result, fmt.Errorf("list ExternalProviders in %s: %w", model.Namespace, err)
	}
	providerOwners := make(map[string]*v1alpha1.ExternalProvider, len(providers.Items))
	for i := range providers.Items {
		providerOwners[providers.Items[i].Name] = &providers.Items[i]
	}
	if err := r.applyAttachmentRoute(ctx, self, t, providerOwners); err != nil {
		return result, err
	}

	route, err := r.getAttachmentRoute(ctx, model.Namespace, self.routeName)
	if err != nil {
		return result, err
	}
	if accepted, reason, message := routeAccepted(route, t.gateway); !accepted {
		result.reason, result.message = reason, message
		result.pending = reason == reasonAttachmentPending
		return result, nil
	}
	// Publishing into the tenant (DestinationRule, credential copies and
	// overlay entry) follows; until then the route carries no traffic.
	result.reason, result.message = reasonAttachmentPending, "the Gateway accepted the attachment HTTPRoute; publishing into the tenant is not implemented yet"
	return result, nil
}

// ownServedModels lists the models the tenant serves from its own namespace.
func (r *Reconciler) ownServedModels(ctx context.Context, t attachmentTenant) ([]*v1alpha1.ExternalModel, error) {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(t.namespace)); err != nil {
		return nil, fmt.Errorf("list tenant ExternalModels in %s: %w", t.namespace, err)
	}
	own := make([]*v1alpha1.ExternalModel, 0, len(models.Items))
	for i := range models.Items {
		if tenant.ServedByTenantGateway(&models.Items[i], t.gateway.Name, t.gateway.Namespace) {
			own = append(own, &models.Items[i])
		}
	}
	return own, nil
}

// attachmentCandidates returns every model from another namespace attached
// to the tenant's Gateway, oldest first, with model-name conflicts and
// per-model resolution decided. Failures exclude only that model.
func (r *Reconciler) attachmentCandidates(ctx context.Context, t attachmentTenant) ([]attachedModel, error) {
	if r.Namespace != "" {
		// A single-namespace controller serves no other namespaces.
		return nil, nil
	}
	own, err := r.ownServedModels(ctx, t)
	if err != nil {
		return nil, err
	}
	winners := map[string]string{}
	for _, m := range own {
		if m.DeletionTimestamp.IsZero() {
			// The tenant's own models always keep their model name.
			winners[clientModelName(m)] = m.Namespace + "/" + m.Name
		}
	}
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		return nil, fmt.Errorf("list ExternalModels attached to Gateway %s: %w", gatewayKey(t.gateway), err)
	}
	attached := make([]*v1alpha1.ExternalModel, 0)
	for i := range models.Items {
		m := &models.Items[i]
		if m.Namespace != t.namespace && m.DeletionTimestamp.IsZero() && hasGatewayRef(m, t.gateway) {
			attached = append(attached, m)
		}
	}
	sort.SliceStable(attached, func(i, j int) bool {
		if !attached[i].CreationTimestamp.Equal(&attached[j].CreationTimestamp) {
			return attached[i].CreationTimestamp.Before(&attached[j].CreationTimestamp)
		}
		return attached[i].Namespace+"/"+attached[i].Name < attached[j].Namespace+"/"+attached[j].Name
	})
	candidates := make([]attachedModel, 0, len(attached))
	for _, m := range attached {
		candidate := attachedModel{model: m, routeName: attachmentRouteName(m, t.gateway)}
		name := clientModelName(m)
		if winner, taken := winners[name]; taken {
			candidate.reason = reasonModelNameConflict
			candidate.message = fmt.Sprintf("model name %q is served on this Gateway by ExternalModel %s", name, winner)
		} else {
			winners[name] = m.Namespace + "/" + m.Name
			if err := r.resolveAttachedModel(ctx, &candidate); err != nil {
				return nil, err
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

// resolveAttachedModel resolves one attached model against the providers of
// its own namespace and rewrites its routes for the tenant with scoped stable
// IDs.
func (r *Reconciler) resolveAttachedModel(ctx context.Context, candidate *attachedModel) error {
	m := candidate.model
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(m.Namespace)); err != nil {
		return fmt.Errorf("list ExternalProviders in %s: %w", m.Namespace, err)
	}
	providerPtrs := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		if !providers.Items[i].DeletionTimestamp.IsZero() || r.validateProvider(ctx, &providers.Items[i]) != nil {
			continue
		}
		ready := providers.Items[i].DeepCopy()
		ready.Status.Phase = resolver.PhaseReady
		providerPtrs = append(providerPtrs, ready)
	}
	set, err := resolver.Resolve([]*v1alpha1.ExternalModel{m}, providerPtrs)
	if err == nil {
		err = r.validateResolvedCredentials(ctx, set.Routes())
	}
	if transientReadError(err) {
		// A failed Secret read is not the model's own failure: retry
		// instead of excluding, and so withdrawing, a served attachment.
		return err
	}
	if err != nil {
		candidate.reason, candidate.message = reasonReconcileFailed, err.Error()
		if errors.Is(err, resolver.ErrNoRoutes) || errors.Is(err, errCredentialNotReady) {
			candidate.reason = reasonProviderNotReady
		}
		return nil
	}
	routes := set.Routes()
	distinct := map[string]bool{}
	for _, route := range routes {
		distinct[route.Provider] = true
	}
	if len(distinct) > maxAttachedProviders {
		candidate.reason = reasonTooManyProviders
		candidate.message = fmt.Sprintf("%d providers exceed the limit of %d for an attached model", len(distinct), maxAttachedProviders)
		return nil
	}
	if err := envelope.ValidateModel(set.Models[0], envelope.Options{KnownClusters: r.KnownClusters}); err != nil {
		candidate.reason, candidate.message = reasonReconcileFailed, err.Error()
		return nil //nolint:nilerr // excludes only this model
	}
	for i := range routes {
		route := &routes[i]
		route.StableID = envelope.ScopedStableID(m.Namespace, m.Name, route.Namespace, route.Provider)
	}
	candidate.routes = routes
	return nil
}

// transientReadError reports an API read that failed for a reason other than
// the object being absent.
func transientReadError(err error) bool {
	if err == nil || apierrors.IsNotFound(err) {
		return false
	}
	var status apierrors.APIStatus
	var urlErr *url.Error
	return errors.As(err, &status) || errors.As(err, &urlErr) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// applyAttachmentRoute applies the model-namespace transport and the
// attachment's HTTPRoute. Provider Services, ServiceEntries and the selection
// sink are shared with the model's other attachments.
func (r *Reconciler) applyAttachmentRoute(ctx context.Context, a attachedModel, t attachmentTenant, providerOwners map[string]*v1alpha1.ExternalProvider) error {
	m := a.model
	route := modelHTTPRouteSet(a.routes, m.Namespace, t.gateway.Name, t.gateway.Namespace)
	providers := make([]string, 0, len(a.routes))
	var resources []unstructured.Unstructured
	for _, provider := range a.routes {
		if slices.Contains(providers, provider.Provider) {
			continue
		}
		providers = append(providers, provider.Provider)
		for _, obj := range []unstructured.Unstructured{providerService(provider, m.Namespace), providerServiceEntry(provider, m.Namespace)} {
			if owner := providerOwners[provider.Provider]; owner != nil {
				setOwnerReference(&obj, owner)
			}
			resources = append(resources, obj)
		}
	}
	sink := providerSelectionSink(a.routes[0], m.Namespace)
	setOwnerReference(&sink, m)
	route.SetName(a.routeName)
	route.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller", tenant.LabelAttachmentTenant: t.namespace})
	route.SetAnnotations(map[string]string{
		annotationAttachmentModel:   m.Name,
		annotationAttachmentGateway: gatewayKey(t.gateway),
	})
	setOwnerReference(&route, m)
	if existing, err := r.getAttachmentRoute(ctx, m.Namespace, a.routeName); err != nil {
		return err
	} else if existing != nil && !isAttachmentRoute(existing, m, t.gateway) {
		return fmt.Errorf("HTTPRoute %s/%s exists with a different identity; refusing to adopt it", m.Namespace, a.routeName)
	}
	return r.applyResources(ctx, append(resources, sink, route))
}

func (r *Reconciler) applyResources(ctx context.Context, resources []unstructured.Unstructured) error {
	for _, resource := range resources {
		if r.ApplyResource != nil {
			if err := r.ApplyResource(ctx, r.Client, resource); err != nil {
				return err
			}
			continue
		}
		if err := render.Apply(ctx, r.Client, []unstructured.Unstructured{resource}); err != nil {
			return err
		}
	}
	return nil
}

// getAttachmentRoute returns the HTTPRoute at an attachment route name, or nil.
func (r *Reconciler) getAttachmentRoute(ctx context.Context, namespace, name string) (*unstructured.Unstructured, error) {
	route := newHTTPRoute()
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, route); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get attachment HTTPRoute %s/%s: %w", namespace, name, err)
	}
	return route, nil
}

// isAttachmentRoute verifies a hashed route name against its identity
// annotations before AGC adopts or deletes the route.
func isAttachmentRoute(route *unstructured.Unstructured, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) bool {
	annotations := route.GetAnnotations()
	return annotations[annotationAttachmentModel] == model.Name && annotations[annotationAttachmentGateway] == gatewayKey(gateway)
}

// routeAccepted reads the route's parent status for the Gateway. The Gateway
// enforces listener allowedRoutes; its Accepted=False reason is reported
// unchanged.
func routeAccepted(route *unstructured.Unstructured, gateway v1alpha1.NamespacedObjectReference) (bool, string, string) {
	if route == nil {
		return false, reasonAttachmentPending, "waiting for the attachment HTTPRoute"
	}
	namespace := route.GetNamespace()
	parents, _, _ := unstructured.NestedSlice(route.Object, "status", "parents")
	reason, message := reasonAttachmentPending, "waiting for the Gateway to accept the attachment HTTPRoute"
	for _, raw := range parents {
		parent, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		parentName, _, _ := unstructured.NestedString(parent, "parentRef", "name")
		parentNamespace, _, _ := unstructured.NestedString(parent, "parentRef", "namespace")
		if parentNamespace == "" {
			parentNamespace = namespace
		}
		if parentName != gateway.Name || parentNamespace != gateway.Namespace {
			continue
		}
		conditions, _, _ := unstructured.NestedSlice(parent, "conditions")
		for _, rawCondition := range conditions {
			condition, ok := rawCondition.(map[string]any)
			if !ok || condition["type"] != "Accepted" {
				continue
			}
			if condition["status"] == string(metav1.ConditionTrue) {
				return true, "", ""
			}
			if gatewayReason, _ := condition["reason"].(string); gatewayReason != "" {
				reason = gatewayReason
			}
			if gatewayMessage, _ := condition["message"].(string); gatewayMessage != "" {
				message = gatewayMessage
			} else {
				message = "the Gateway did not accept the attachment HTTPRoute"
			}
		}
	}
	return false, reason, message
}

// listAttachmentRoutes lists controller-owned attachment HTTPRoutes.
func (r *Reconciler) listAttachmentRoutes(ctx context.Context, opts ...client.ListOption) ([]unstructured.Unstructured, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(httpRouteGVK.GroupVersion().WithKind("HTTPRouteList"))
	opts = append(opts, client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}, client.HasLabels{tenant.LabelAttachmentTenant})
	if err := r.List(ctx, list, opts...); err != nil {
		return nil, fmt.Errorf("list attachment HTTPRoutes: %w", err)
	}
	return list.Items, nil
}

// removeAttachmentRoute deletes the model's identity-verified HTTPRoute for
// one Gateway.
func (r *Reconciler) removeAttachmentRoute(ctx context.Context, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) error {
	route, err := r.getAttachmentRoute(ctx, model.Namespace, attachmentRouteName(model, gateway))
	if err != nil || route == nil || !isAttachmentRoute(route, model, gateway) {
		return err
	}
	if err := client.IgnoreNotFound(r.Delete(ctx, route)); err != nil {
		return fmt.Errorf("delete attachment HTTPRoute %s/%s: %w", route.GetNamespace(), route.GetName(), err)
	}
	return nil
}

// removeDetachedRoutes deletes the model's attachment HTTPRoutes for Gateways
// its gatewayRefs no longer name.
func (r *Reconciler) removeDetachedRoutes(ctx context.Context, model *v1alpha1.ExternalModel) error {
	routes, err := r.listAttachmentRoutes(ctx, client.InNamespace(model.Namespace))
	if err != nil {
		return err
	}
	for i := range routes {
		gatewayNamespace, gatewayName, ok := strings.Cut(routes[i].GetAnnotations()[annotationAttachmentGateway], "/")
		gateway := v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace}
		if !ok || routes[i].GetAnnotations()[annotationAttachmentModel] != model.Name || hasGatewayRef(model, gateway) {
			continue
		}
		if err := r.removeAttachmentRoute(ctx, model, gateway); err != nil {
			return err
		}
	}
	return nil
}

// attachedModelSinks returns the models whose selection sinks the
// namespace's attachment HTTPRoutes use.
func (r *Reconciler) attachedModelSinks(ctx context.Context, namespace string) (map[string]bool, error) {
	routes, err := r.listAttachmentRoutes(ctx, client.InNamespace(namespace))
	if err != nil {
		return nil, err
	}
	models := map[string]bool{}
	for i := range routes {
		models[routes[i].GetAnnotations()[annotationAttachmentModel]] = true
	}
	return models, nil
}
