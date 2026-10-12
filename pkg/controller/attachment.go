package controller

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
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
	"github.com/opendatahub-io/ai-gateway-controller/pkg/publisher"
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
	reasonTenantNotResolved       = "TenantNotResolved"
	reasonGatewayNotManaged       = "GatewayNotManaged"
	reasonAmbiguousTenant         = "AmbiguousTenant"
	reasonTenantNotPraxis         = "TenantNotPraxis"
	reasonTenantNotSteady         = "TenantNotSteady"
	reasonModelNameConflict       = "ModelNameConflict"
	reasonTooManyProviders        = "TooManyProviders"
	reasonOverlayCapacityExceeded = "OverlayCapacityExceeded"
	reasonAttachmentPending       = "AttachmentPending"

	modelPhasePending = "Pending"
	modelPhaseFailed  = "Failed"

	// maxAttachedProviders keeps an attached model's HTTPRoute within the
	// Gateway API limit of 16 rules: two per provider plus two sink rules.
	maxAttachedProviders = 7
	// maxOverlayCandidates is the Praxis per-overlay candidate limit.
	maxOverlayCandidates = 1024

	attachmentRequeueInterval = 5 * time.Second

	credentialCopyPrefix = "agc-cred-" //nolint:gosec // Secret name prefix, not a credential

	annotationAttachmentModel       = "inference.opendatahub.io/attachment-model"
	annotationAttachmentGateway     = "inference.opendatahub.io/attachment-gateway"
	annotationAttachmentProviders   = "inference.opendatahub.io/attachment-providers"
	annotationAttachmentRules       = "inference.opendatahub.io/attachment-destination-rules"
	annotationAttachmentCredentials = "inference.opendatahub.io/attachment-credentials"
	annotationAttachmentIdentity    = "inference.opendatahub.io/attachment-identity"
	annotationCredentialSource      = "inference.opendatahub.io/credential-source"
)

var httpRouteGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1", Kind: "HTTPRoute"}

// attachmentTenant is the Praxis tenant that owns an attached Gateway.
type attachmentTenant struct {
	gateway     v1alpha1.NamespacedObjectReference
	namespace   string // MaasTenantConfig namespace
	tenantID    string
	aitenantUID types.UID
	mtcUID      types.UID
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

// credentialCopy is a credential Secret from outside the tenant namespace
// copied into it, referenced key only.
type credentialCopy struct {
	name, sourceNamespace, sourceName, key string
}

func (c credentialCopy) source() string {
	return c.sourceNamespace + "/" + c.sourceName + "/" + c.key
}

// attachedModel is one model from another namespace attached to a tenant.
// A non-empty reason excludes it from the tenant.
type attachedModel struct {
	model     *v1alpha1.ExternalModel
	routeName string
	routes    []resolver.Route // stable IDs and credentials rewritten for the tenant
	copies    []credentialCopy
	reason    string
	message   string
	// Set on admission: the accepted attachment HTTPRoute, and whether the
	// credential copies are in place.
	routeUID types.UID
	ready    bool
}

// handoff is the tenant handoff state the attachment was gated on, re-checked
// live before writing into the tenant.
func (t attachmentTenant) handoff() modelTenantContext {
	return modelTenantContext{aitenantUID: t.aitenantUID, mtcUID: t.mtcUID, gatewayName: t.gateway.Name, gatewayNamespace: t.gateway.Namespace}
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
	return modelRoutePrefix + hash
}

// attachmentDestinationRuleName names the Gateway-namespace DestinationRule
// for a provider outside the tenant namespace, within 63 characters.
func attachmentDestinationRuleName(gateway v1alpha1.NamespacedObjectReference, providerNamespace, provider string) string {
	hash := shortHash(gateway.Namespace, gateway.Name, providerNamespace, provider)
	base := providerServicePrefix + provider
	if limit := 63 - len(hash) - 1; len(base) > limit {
		base = strings.TrimRight(base[:limit], "-.")
	}
	return base + "-" + hash
}

func credentialCopyName(tenantNamespace, sourceNamespace, sourceName, key string) string {
	return credentialCopyPrefix + shortHash(tenantNamespace, sourceNamespace, sourceName, key)
}

func newHTTPRoute() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(httpRouteGVK)
	return u
}

func splitList(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func appendMissingRequests(requests, more []reconcile.Request) []reconcile.Request {
	for _, request := range more {
		if !slices.Contains(requests, request) {
			requests = append(requests, request)
		}
	}
	return requests
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
		t.aitenantUID, t.mtcUID = live.aitenant.GetUID(), live.mtc.GetUID()
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
	results := make([]attachmentResult, len(targets))
	homeReported := false
	if home >= 0 {
		var err error
		result, err = r.reconcileHomeModel(ctx, model, func(ready bool, reason, message string, env *envelope.Envelope) error {
			homeReported = true
			results[home] = attachmentResult{gateway: targets[home].result.gateway, ready: ready, reason: reason, message: message, routeName: modelRouteName(model.Name), env: env}
			return nil
		})
		if err != nil {
			errs = append(errs, err)
		}
	} else if err := r.leaveHomeTenant(ctx, model); err != nil {
		errs = append(errs, err)
	}

	keep := map[string]bool{}
	for i, target := range targets {
		if i == home {
			continue
		}
		results[i] = target.result
		if target.tenantNamespace == "" {
			// Unsupported attachments retain existing state and never fall
			// back to another Gateway.
			keep[gatewayKey(target.result.gateway)] = true
			continue
		}
		t, gated, cleanup, err := r.gateAttachmentTenant(ctx, target.ait, target.tenantNamespace, target.result)
		results[i] = gated
		if err != nil {
			errs = append(errs, err)
			results[i] = failedAttachment(gated, err)
			keep[gatewayKey(gated.gateway)] = true
			continue
		}
		if cleanup {
			// An explicit non-Praxis selection removes this attachment
			// below; the tenant reconciler's switch-away cleanup removes
			// the tenant's ExtProc, route filter and copies.
			continue
		}
		keep[gatewayKey(gated.gateway)] = true
		if !gated.ready {
			continue
		}
		results[i], err = r.reconcileAttachment(ctx, model, t)
		if err != nil {
			errs = append(errs, err)
			results[i] = failedAttachment(results[i], err)
		}
	}
	if deleted, err := r.detachAttachments(ctx, model, func(key string) bool { return keep[key] }); err != nil {
		errs = append(errs, err)
	} else if err := r.cleanupAttachmentModelTransport(ctx, model.Namespace, deleted); err != nil {
		errs = append(errs, err)
	}

	if home >= 0 && !homeReported {
		// The home path stopped without a status: an explicit non-Praxis
		// selection (cleanup), a requeue for a changing tenant, or an error.
		results[home] = attachmentResult{gateway: targets[home].result.gateway, reason: reasonTenantNotPraxis,
			message: "the model's own tenant does not select Praxis"}
		if result.Requeue || result.RequeueAfter > 0 || len(errs) > 0 { //nolint:staticcheck // the home path's existing requeue signal
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

// leaveHomeTenant republishes the model's own tenant without it when its
// gatewayRefs no longer name that tenant's Gateway. The home state is kept
// until the tenant is steady, as for any other home republish.
func (r *Reconciler) leaveHomeTenant(ctx context.Context, model *v1alpha1.ExternalModel) error {
	if hasHome, err := r.hasHomeRoute(ctx, model); err != nil || !hasHome {
		return err
	}
	live, err := r.liveTenantResolution(ctx, model.Namespace)
	if err != nil {
		return err
	}
	if live.missingConfig || !live.selected || live.aitenant == nil || live.status != tenant.PayloadProcessingStatusSteady ||
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
		return result, r.excludeAttachment(ctx, model, t)
	}
	if err := r.addExternalModelFinalizer(ctx, model); err != nil {
		return result, err
	}
	providers, _, validProviders, err := r.providerInputs(ctx, model.Namespace)
	if err != nil {
		return result, err
	}
	providerOwners := make(map[string]*v1alpha1.ExternalProvider, len(providers.Items))
	for i := range providers.Items {
		providerOwners[providers.Items[i].Name] = &providers.Items[i]
	}
	if err := r.applyAttachmentRoute(ctx, self, t, providerOwners); err != nil {
		return result, err
	}
	tenantNamespace, err := r.isTenantNamespace(ctx, model.Namespace)
	if err != nil {
		return result, err
	}
	for _, p := range validProviders {
		// In a tenant namespace the tenant-local path writes provider status.
		if !tenantNamespace && slices.ContainsFunc(self.routes, func(route resolver.Route) bool { return route.Provider == p.Name }) {
			if err := r.updateProviderStatus(ctx, p, true, reasonReconciled, "provider Secret reference and transport resources are ready"); err != nil {
				return result, err
			}
		}
	}

	accepted, reason, message, err := r.attachmentAccepted(ctx, model.Namespace, self.routeName, t.gateway)
	if err != nil {
		return result, err
	}
	if !accepted {
		result.reason, result.message = reason, message
		result.pending = reason == reasonAttachmentPending
		// A revoked acceptance withdraws the overlay entry, copies and
		// DestinationRules; publishing excludes routes not accepted.
		return result, r.withdrawAttachment(ctx, model, t)
	}
	// Before writing into the tenant, re-check the handoff live, as the
	// tenant-local path does before applying its transport, and decide the
	// overlay capacity: a model over it receives no DestinationRule or copy.
	if valid, err := r.livePraxisBeforeWrite(ctx, t.namespace, t.handoff()); err != nil || !valid {
		if err == nil {
			result.pending, result.reason, result.message = true, reasonTenantNotSteady, "the tenant's handoff changed; retrying"
		}
		return result, err
	}
	own, err := r.ownOverlayCandidates(ctx, t.namespace)
	if err != nil {
		return result, err
	}
	admission, err := r.admitAttachments(ctx, t, candidates, len(own))
	if err != nil {
		return result, err
	}
	if i := slices.IndexFunc(admission, func(c attachedModel) bool { return c.model.UID == model.UID }); i >= 0 && admission[i].reason != "" {
		result.reason, result.message = admission[i].reason, admission[i].message
		return result, r.withdrawAttachment(ctx, model, t)
	}
	if err := r.applyAttachmentDestinationRules(ctx, self, t); err != nil {
		return result, err
	}
	if err := r.applyCredentialCopies(ctx, self, t); err != nil {
		return result, err
	}
	admitted, err := r.publishAttachmentTenant(ctx, t, model)
	if err != nil {
		return result, err
	}
	if err := r.gcAttachmentState(ctx, t.namespace, excludedRoutes(admitted)); err != nil {
		return result, err
	}
	idx = slices.IndexFunc(admitted, func(c attachedModel) bool { return c.model.UID == model.UID })
	switch {
	case idx >= 0 && admitted[idx].reason != "":
		result.reason, result.message = admitted[idx].reason, admitted[idx].message
	case idx < 0 || !admitted[idx].ready:
		result.pending, result.reason, result.message = true, reasonAttachmentPending, "the tenant has not admitted this attachment yet"
	default:
		result.ready, result.message = true, "attached and distributed to the tenant overlay"
	}
	return result, nil
}

// excludeAttachment removes an excluded model from the tenant: its HTTPRoute,
// the DestinationRules and credential copies no other attachment references,
// and its overlay entry and route filter patches.
func (r *Reconciler) excludeAttachment(ctx context.Context, model *v1alpha1.ExternalModel, t attachmentTenant) error {
	if err := r.removeAttachmentRoute(ctx, model, t.gateway); err != nil {
		return err
	}
	_, err := r.publishAttachmentTenant(ctx, t, model)
	return err
}

// withdrawAttachment stops publishing an attachment that keeps its HTTPRoute
// (not accepted by the Gateway, or over the overlay capacity): the tenant
// republishes without it and its DestinationRules and credential copies are
// collected.
func (r *Reconciler) withdrawAttachment(ctx context.Context, model *v1alpha1.ExternalModel, t attachmentTenant) error {
	admitted, err := r.publishAttachmentTenant(ctx, t, model)
	if err != nil {
		return err
	}
	return r.gcAttachmentState(ctx, t.namespace, excludedRoutes(admitted))
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
			if err := r.resolveAttachedModel(ctx, t, &candidate); err != nil {
				return nil, err
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates, nil
}

// resolveAttachedModel resolves one attached model against the providers of
// its own namespace and rewrites its routes for the tenant: scoped stable IDs
// and credential copies in the tenant namespace.
func (r *Reconciler) resolveAttachedModel(ctx context.Context, t attachmentTenant, candidate *attachedModel) error {
	m := candidate.model
	var providers v1alpha1.ExternalProviderList
	if err := r.List(ctx, &providers, client.InNamespace(m.Namespace)); err != nil {
		return fmt.Errorf("list ExternalProviders in %s: %w", m.Namespace, err)
	}
	providerPtrs := make([]*v1alpha1.ExternalProvider, 0, len(providers.Items))
	for i := range providers.Items {
		if r.validateProvider(ctx, &providers.Items[i]) != nil {
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
		if route.AuthType == "" {
			continue
		}
		copied := credentialCopy{
			name:            credentialCopyName(t.namespace, route.Namespace, route.SecretName, route.SecretKey),
			sourceNamespace: route.Namespace, sourceName: route.SecretName, key: route.SecretKey,
		}
		if !slices.Contains(candidate.copies, copied) {
			candidate.copies = append(candidate.copies, copied)
		}
		route.SecretName, route.SecretNamespace = copied.name, t.namespace
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

// admittedAttachments returns the tenant's attachments in admission order;
// see admitAttachments.
func (r *Reconciler) admittedAttachments(ctx context.Context, t attachmentTenant, ownCandidates int) ([]attachedModel, error) {
	candidates, err := r.attachmentCandidates(ctx, t)
	if err != nil {
		return nil, err
	}
	return r.admitAttachments(ctx, t, candidates, ownCandidates)
}

// admitAttachments admits, oldest first, the candidates whose route the
// Gateway accepted, within the overlay capacity left after the tenant's own
// candidates; those over it carry OverlayCapacityExceeded. Capacity is
// reserved whether or not the credential copies exist yet, so it is decided
// before anything is written into the tenant. Only ready attachments, with
// their copies in place, are published.
func (r *Reconciler) admitAttachments(ctx context.Context, t attachmentTenant, candidates []attachedModel, ownCandidates int) ([]attachedModel, error) {
	admitted := make([]attachedModel, 0, len(candidates))
	capacity := maxOverlayCandidates - ownCandidates
	for _, candidate := range candidates {
		if candidate.reason != "" {
			continue
		}
		route := newHTTPRoute()
		if err := r.Get(ctx, client.ObjectKey{Namespace: candidate.model.Namespace, Name: candidate.routeName}, route); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get attachment HTTPRoute %s/%s: %w", candidate.model.Namespace, candidate.routeName, err)
		}
		if accepted, _, _ := routeAccepted(route, t.gateway); !accepted {
			continue
		}
		candidate.routeUID = route.GetUID()
		if len(candidate.routes) > capacity {
			candidate.reason = reasonOverlayCapacityExceeded
			candidate.message = fmt.Sprintf("the tenant overlay has no capacity for %d more candidates (limit %d)", len(candidate.routes), maxOverlayCandidates)
			admitted = append(admitted, candidate)
			continue
		}
		capacity -= len(candidate.routes)
		candidate.ready = true
		for _, copied := range candidate.copies {
			ready, err := r.credentialCopyReady(ctx, t.namespace, copied)
			if err != nil {
				return nil, err
			}
			candidate.ready = candidate.ready && ready
		}
		admitted = append(admitted, candidate)
	}
	return admitted, nil
}

// servedAttachments drops attachments excluded by capacity or still waiting
// for their credential copies.
func servedAttachments(attached []attachedModel) []attachedModel {
	served := make([]attachedModel, 0, len(attached))
	for _, a := range attached {
		if a.reason == "" && a.ready {
			served = append(served, a)
		}
	}
	return served
}

// excludedRoutes returns the routes of attachments excluded by the overlay
// capacity. They keep their routes, so their DestinationRules and credential
// copies are collected as if the routes were gone.
func excludedRoutes(attached []attachedModel) map[types.UID]bool {
	routes := map[types.UID]bool{}
	for _, a := range attached {
		if a.reason != "" {
			routes[a.routeUID] = true
		}
	}
	return routes
}

func attachedRoutePatches(attached []attachedModel) []any {
	var patches []any
	for _, a := range servedAttachments(attached) {
		patches = append(patches, routeFilterPatches(a.model.Namespace, a.routeName, len(a.routes)*2+2)...)
	}
	return patches
}

// publishTenantOverlay publishes the tenant's own route set followed by its
// served attachments, or, with preserved candidates, the already-published
// own candidates followed by the attachments.
func (r *Reconciler) publishTenantOverlay(ctx context.Context, namespace, gatewayName string, own *resolver.ResolvedRouteSet,
	attached []attachedModel, preserved []envelope.Candidate, sourceUID string) (publisher.Result, error) {
	set := own
	if served := servedAttachments(attached); len(served) > 0 || own == nil {
		set = &resolver.ResolvedRouteSet{}
		if own != nil {
			set.Models = append(set.Models, own.Models...)
		}
		for _, a := range served {
			set.Models = append(set.Models, resolver.ModelRoutes{ModelRef: a.model.Namespace + "/" + a.model.Name, Routes: a.routes})
		}
	}
	pub, err := publisher.New(r.Client, publisher.Config{Namespace: namespace, Name: r.ConfigMap})
	if err != nil {
		return publisher.Result{}, err
	}
	publish := r.PublishOverlay
	if publish == nil {
		publish = pub.Publish
	}
	return publish(ctx, set, envelope.Scope{
		Network: r.Network, Gateway: gatewayName, Namespace: namespace, LocalSite: r.localSite(),
	}, envelope.Options{
		KnownClusters: r.KnownClusters, SourceUID: sourceUID, ProducerVersion: r.ProducerVersion,
		PreservedCandidates: preserved,
	})
}

// publishAttachmentTenant republishes the tenant's attachments. The tenant's
// own overlay candidates and route patches are preserved verbatim from what
// its own path last published, so attachments never change them. This
// read-modify-write is safe only because ExternalModel reconciles run one at
// a time (the default single worker): the tenant-local path and attachments
// from every namespace write the same overlay and route filter.
func (r *Reconciler) publishAttachmentTenant(ctx context.Context, t attachmentTenant, subject *v1alpha1.ExternalModel) ([]attachedModel, error) {
	if valid, err := r.livePraxisBeforeWrite(ctx, t.namespace, t.handoff()); err != nil || !valid {
		return nil, err
	}
	preserved, err := r.ownOverlayCandidates(ctx, t.namespace)
	if err != nil {
		return nil, err
	}
	ownPatches, err := r.ownRoutePatches(ctx, t)
	if err != nil {
		return nil, err
	}
	admitted, err := r.admittedAttachments(ctx, t, len(preserved))
	if err != nil {
		return nil, err
	}
	if err := r.writeExternalModelRouteFilter(ctx, t.tenantID, t.namespace, t.gateway.Name, t.gateway.Namespace, append(ownPatches, attachedRoutePatches(admitted)...)); err != nil {
		return nil, err
	}
	if len(preserved) == 0 && len(servedAttachments(admitted)) == 0 {
		return admitted, r.cleanupOverlay(ctx, t.namespace)
	}
	if _, err := r.publishTenantOverlay(ctx, t.namespace, t.gateway.Name, nil, admitted, preserved, string(subject.UID)); err != nil {
		return nil, err
	}
	return admitted, nil
}

// ownOverlayCandidates returns the tenant's own published candidates: those
// with the existing tenant-local stable_id form.
func (r *Reconciler) ownOverlayCandidates(ctx context.Context, namespace string) ([]envelope.Candidate, error) {
	var cm corev1.ConfigMap
	// Live read: the tenant-local path may have published in this same
	// event batch, and preserving a stale copy would drop its candidates.
	if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: r.configMapName()}, &cm); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	raw, ok := cm.Data[publisher.DefaultDataKey]
	if !ok {
		return nil, nil
	}
	var env envelope.Envelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return nil, fmt.Errorf("%w: %s/%s: %w", publisher.ErrEnvelopeTampered, namespace, cm.Name, err)
	}
	var own []envelope.Candidate
	for _, candidate := range env.Overlay.Candidates {
		if !envelope.IsScopedStableID(candidate.StableID) {
			own = append(own, candidate)
		}
	}
	return own, nil
}

// ownRoutePatches returns the route filter patches for HTTPRoutes in the
// tenant namespace. Istio route names start with the route namespace, and
// attached routes always live in another namespace.
func (r *Reconciler) ownRoutePatches(ctx context.Context, t attachmentTenant) ([]any, error) {
	filter := &unstructured.Unstructured{}
	filter.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1alpha3", Kind: "EnvoyFilter"})
	key := client.ObjectKey{Namespace: t.gateway.Namespace, Name: tenant.PayloadProcessingExternalModelEnvoyFilterName(t.tenantID)}
	if err := r.APIReader.Get(ctx, key, filter); err != nil {
		return nil, client.IgnoreNotFound(err)
	}
	if filter.GetLabels()[externalTenantLabel] != t.namespace {
		return nil, nil
	}
	patches, _, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
	if err != nil {
		return nil, fmt.Errorf("read ExternalModel route EnvoyFilter %s: %w", key, err)
	}
	own := make([]any, 0, len(patches))
	for _, raw := range patches {
		patch, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(patch, "match", "routeConfiguration", "vhost", "route", "name")
		if strings.HasPrefix(name, t.namespace+".") {
			own = append(own, patch)
		}
	}
	return own, nil
}

// applyAttachmentRoute applies the model-namespace transport and the
// attachment's HTTPRoute. Provider Services, ServiceEntries and the selection
// sink are shared with the model's other attachments.
func (r *Reconciler) applyAttachmentRoute(ctx context.Context, a attachedModel, t attachmentTenant, providerOwners map[string]*v1alpha1.ExternalProvider) error {
	m := a.model
	route := modelHTTPRouteSet(a.routes, m.Namespace, t.gateway.Name, t.gateway.Namespace)
	providers := make([]string, 0, len(a.routes))
	rules := make([]string, 0, len(a.routes))
	var resources []unstructured.Unstructured
	for _, provider := range a.routes {
		if slices.Contains(providers, provider.Provider) {
			continue
		}
		providers = append(providers, provider.Provider)
		rules = append(rules, attachmentDestinationRuleName(t.gateway, provider.Namespace, provider.Provider))
		for _, obj := range []unstructured.Unstructured{providerService(provider, m.Namespace), providerServiceEntry(provider, m.Namespace)} {
			if owner := providerOwners[provider.Provider]; owner != nil {
				setOwnerReference(&obj, owner)
			}
			resources = append(resources, obj)
		}
	}
	copies := make([]string, 0, len(a.copies))
	for _, copied := range a.copies {
		copies = append(copies, copied.name)
	}
	sink := providerSelectionSink(a.routes[0], m.Namespace)
	setOwnerReference(&sink, m)
	route.SetName(a.routeName)
	route.SetLabels(map[string]string{"app.kubernetes.io/managed-by": "ai-gateway-controller", tenant.LabelAttachmentTenant: t.namespace})
	route.SetAnnotations(map[string]string{
		annotationAttachmentModel:       m.Name,
		annotationAttachmentGateway:     gatewayKey(t.gateway),
		annotationAttachmentProviders:   strings.Join(providers, ","),
		annotationAttachmentRules:       strings.Join(rules, ","),
		annotationAttachmentCredentials: strings.Join(copies, ","),
	})
	setOwnerReference(&route, m)
	if err := r.verifyAttachmentRoute(ctx, m, a.routeName, t.gateway); err != nil {
		return err
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

// verifyAttachmentRoute refuses to adopt an existing HTTPRoute at a hashed
// name unless its identity annotations name this model and Gateway.
func (r *Reconciler) verifyAttachmentRoute(ctx context.Context, model *v1alpha1.ExternalModel, name string, gateway v1alpha1.NamespacedObjectReference) error {
	existing := newHTTPRoute()
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: name}, existing); err != nil {
		return client.IgnoreNotFound(err)
	}
	annotations := existing.GetAnnotations()
	if annotations[annotationAttachmentModel] != model.Name || annotations[annotationAttachmentGateway] != gatewayKey(gateway) {
		return fmt.Errorf("HTTPRoute %s/%s exists with a different identity; refusing to adopt it", model.Namespace, name)
	}
	return nil
}

// attachmentAccepted reads the route's parent status for the Gateway. The
// Gateway enforces listener allowedRoutes; its Accepted=False reason is
// reported unchanged.
func (r *Reconciler) attachmentAccepted(ctx context.Context, namespace, name string, gateway v1alpha1.NamespacedObjectReference) (bool, string, string, error) {
	route := newHTTPRoute()
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, route); err != nil {
		if apierrors.IsNotFound(err) {
			return false, reasonAttachmentPending, "waiting for the attachment HTTPRoute", nil
		}
		return false, reasonAttachmentPending, "", fmt.Errorf("get attachment HTTPRoute %s/%s: %w", namespace, name, err)
	}
	accepted, reason, message := routeAccepted(route, gateway)
	return accepted, reason, message, nil
}

// routeAccepted reads the route's parent status for the Gateway.
func routeAccepted(route *unstructured.Unstructured, gateway v1alpha1.NamespacedObjectReference) (bool, string, string) {
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

// applyAttachmentDestinationRules applies the Gateway-namespace provider TLS
// policy under hashed, identity-verified names.
func (r *Reconciler) applyAttachmentDestinationRules(ctx context.Context, a attachedModel, t attachmentTenant) error {
	seen := map[string]bool{}
	var rules []unstructured.Unstructured
	for _, route := range a.routes {
		name := attachmentDestinationRuleName(t.gateway, route.Namespace, route.Provider)
		if seen[name] {
			continue
		}
		seen[name] = true
		identity := gatewayKey(t.gateway) + "/" + route.Namespace + "/" + route.Provider
		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRule"})
		if err := r.Get(ctx, client.ObjectKey{Namespace: t.gateway.Namespace, Name: name}, existing); err == nil {
			if existing.GetAnnotations()[annotationAttachmentIdentity] != identity {
				return fmt.Errorf("DestinationRule %s/%s exists with a different identity; refusing to adopt it", t.gateway.Namespace, name)
			}
		} else if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get DestinationRule %s/%s: %w", t.gateway.Namespace, name, err)
		}
		rule := providerDestinationRuleAtGateway(route, t.gateway.Name, t.gateway.Namespace, t.tenantID)
		rule.SetName(name)
		labels := rule.GetLabels()
		labels[tenant.LabelAttachmentTenant] = t.namespace
		rule.SetLabels(labels)
		rule.SetAnnotations(map[string]string{annotationAttachmentIdentity: identity})
		rules = append(rules, rule)
	}
	return r.applyResources(ctx, rules)
}

// applyCredentialCopies copies the referenced key of each credential Secret
// from outside the tenant namespace into a controller-owned Secret there.
func (r *Reconciler) applyCredentialCopies(ctx context.Context, a attachedModel, t attachmentTenant) error {
	if r.APIReader == nil {
		return errors.New("APIReader is required for credential Secret reads")
	}
	copies := make([]unstructured.Unstructured, 0, len(a.copies))
	for _, copied := range a.copies {
		if err := r.verifyCredentialCopy(ctx, t.namespace, copied); err != nil {
			return err
		}
		var source corev1.Secret
		if err := r.APIReader.Get(ctx, client.ObjectKey{Namespace: copied.sourceNamespace, Name: copied.sourceName}, &source); err != nil {
			return fmt.Errorf("%w: Secret %s/%s: %w", errCredentialNotReady, copied.sourceNamespace, copied.sourceName, err)
		}
		value, ok := source.Data[copied.key]
		if !ok {
			return fmt.Errorf("%w: Secret %s/%s is missing key %s", errCredentialNotReady, copied.sourceNamespace, copied.sourceName, copied.key)
		}
		copies = append(copies, unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Secret",
			"metadata": map[string]any{
				"name": copied.name, "namespace": t.namespace,
				"labels": map[string]any{
					"app.kubernetes.io/managed-by": "ai-gateway-controller",
					tenant.LabelCredentialCopy:     "true",
				},
				"annotations": map[string]any{annotationCredentialSource: copied.source()},
			},
			"type": string(corev1.SecretTypeOpaque),
			"data": map[string]any{copied.key: base64.StdEncoding.EncodeToString(value)},
		}})
	}
	return r.applyResources(ctx, copies)
}

func credentialCopyMetadata() *metav1.PartialObjectMetadata {
	m := &metav1.PartialObjectMetadata{}
	m.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	return m
}

// verifyCredentialCopy refuses to adopt an existing Secret at a copy name
// unless it is the controller-owned copy of the same source.
func (r *Reconciler) verifyCredentialCopy(ctx context.Context, namespace string, copied credentialCopy) error {
	existing := credentialCopyMetadata()
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: copied.name}, existing); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !isCredentialCopyOf(existing, copied) {
		return fmt.Errorf("credential copy Secret %s/%s exists with a different identity; refusing to adopt it", namespace, copied.name)
	}
	return nil
}

func isCredentialCopyOf(secret *metav1.PartialObjectMetadata, copied credentialCopy) bool {
	return secret.GetLabels()[tenant.LabelCredentialCopy] == "true" &&
		secret.GetAnnotations()[annotationCredentialSource] == copied.source()
}

// credentialCopyReady reports whether the controller-owned copy exists.
// Secret metadata is read from the metadata-only cache.
func (r *Reconciler) credentialCopyReady(ctx context.Context, namespace string, copied credentialCopy) (bool, error) {
	existing := credentialCopyMetadata()
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: copied.name}, existing); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get credential copy %s/%s: %w", namespace, copied.name, err)
	}
	return isCredentialCopyOf(existing, copied), nil
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
// one Gateway and the tenant state no remaining attachment references.
func (r *Reconciler) removeAttachmentRoute(ctx context.Context, model *v1alpha1.ExternalModel, gateway v1alpha1.NamespacedObjectReference) error {
	_, err := r.detachAttachments(ctx, model, func(key string) bool { return key != gatewayKey(gateway) })
	return err
}

// detachAttachments deletes the model's attachment HTTPRoutes whose Gateway
// is not kept (nil keeps none), then removes the credential copies and
// DestinationRules no remaining attachment of those tenants references. The
// tenant overlay is not rebuilt: its next steady publish drops the inert
// entries, which no route reaches.
// It returns the deleted routes, which the informer cache may still list.
func (r *Reconciler) detachAttachments(ctx context.Context, model *v1alpha1.ExternalModel, keep func(gatewayKey string) bool) (map[types.UID]bool, error) {
	routes, err := r.listAttachmentRoutes(ctx, client.InNamespace(model.Namespace))
	if err != nil {
		return nil, err
	}
	deleted := map[types.UID]bool{}
	tenants := map[string]bool{}
	for i := range routes {
		route := &routes[i]
		annotations := route.GetAnnotations()
		if annotations[annotationAttachmentModel] != model.Name || (keep != nil && keep(annotations[annotationAttachmentGateway])) {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, route)); err != nil {
			return nil, fmt.Errorf("delete attachment HTTPRoute %s/%s: %w", route.GetNamespace(), route.GetName(), err)
		}
		deleted[route.GetUID()] = true
		tenants[route.GetLabels()[tenant.LabelAttachmentTenant]] = true
	}
	for tenantNamespace := range tenants {
		if err := r.gcAttachmentState(ctx, tenantNamespace, deleted); err != nil {
			return nil, err
		}
	}
	return deleted, nil
}

// gcAttachmentState deletes the tenant's attachment DestinationRules and
// credential copies that no accepted attachment HTTPRoute references, other
// than the given routes: just deleted, or excluded by the overlay capacity.
func (r *Reconciler) gcAttachmentState(ctx context.Context, tenantNamespace string, deleted map[types.UID]bool) error {
	routes, err := r.listAttachmentRoutes(ctx, client.MatchingLabels{tenant.LabelAttachmentTenant: tenantNamespace})
	if err != nil {
		return err
	}
	keepRules, keepCopies := map[string]bool{}, map[string]bool{}
	for i := range routes {
		annotations := routes[i].GetAnnotations()
		gatewayNamespace, gatewayName, _ := strings.Cut(annotations[annotationAttachmentGateway], "/")
		accepted, _, _ := routeAccepted(&routes[i], v1alpha1.NamespacedObjectReference{Name: gatewayName, Namespace: gatewayNamespace})
		if deleted[routes[i].GetUID()] || !accepted {
			continue
		}
		for _, name := range splitList(annotations[annotationAttachmentRules]) {
			keepRules[name] = true
		}
		for _, name := range splitList(annotations[annotationAttachmentCredentials]) {
			keepCopies[name] = true
		}
	}
	owned := client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}
	rules := &unstructured.UnstructuredList{}
	rules.SetGroupVersionKind(schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "DestinationRuleList"})
	if err := r.List(ctx, rules, owned, client.MatchingLabels{tenant.LabelAttachmentTenant: tenantNamespace}); err != nil {
		return fmt.Errorf("list attachment DestinationRules: %w", err)
	}
	for i := range rules.Items {
		rule := &rules.Items[i]
		if keepRules[rule.GetName()] || rule.GetAnnotations()[annotationAttachmentIdentity] == "" {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, rule)); err != nil {
			return fmt.Errorf("delete attachment DestinationRule %s/%s: %w", rule.GetNamespace(), rule.GetName(), err)
		}
	}
	copies := &metav1.PartialObjectMetadataList{}
	copies.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("SecretList"))
	if err := r.List(ctx, copies, client.InNamespace(tenantNamespace), owned, client.MatchingLabels{tenant.LabelCredentialCopy: "true"}); err != nil {
		return fmt.Errorf("list credential copies: %w", err)
	}
	for i := range copies.Items {
		copied := &copies.Items[i]
		if keepCopies[copied.GetName()] || copied.GetAnnotations()[annotationCredentialSource] == "" {
			continue
		}
		if err := client.IgnoreNotFound(r.Delete(ctx, copied)); err != nil {
			return fmt.Errorf("delete credential copy %s/%s: %w", tenantNamespace, copied.GetName(), err)
		}
	}
	return nil
}

// attachmentTransportKeep returns the providers and models whose
// model-namespace transport the namespace's attachment HTTPRoutes use.
func (r *Reconciler) attachmentTransportKeep(ctx context.Context, namespace string, deleted map[types.UID]bool) (map[string]bool, map[string]bool, error) {
	routes, err := r.listAttachmentRoutes(ctx, client.InNamespace(namespace))
	if err != nil {
		return nil, nil, err
	}
	providers, models := map[string]bool{}, map[string]bool{}
	for i := range routes {
		if deleted[routes[i].GetUID()] {
			continue
		}
		annotations := routes[i].GetAnnotations()
		models[annotations[annotationAttachmentModel]] = true
		for _, provider := range splitList(annotations[annotationAttachmentProviders]) {
			providers[provider] = true
		}
	}
	return providers, models, nil
}

// cleanupAttachmentModelTransport removes model-namespace transport that no
// attachment uses. In a tenant namespace, or one still holding tenant-local
// routes, the existing path owns that transport.
func (r *Reconciler) cleanupAttachmentModelTransport(ctx context.Context, namespace string, deleted map[types.UID]bool) error {
	if tenantNamespace, err := r.isTenantNamespace(ctx, namespace); err != nil || tenantNamespace {
		return err
	}
	homeRoutes := &unstructured.UnstructuredList{}
	homeRoutes.SetGroupVersionKind(httpRouteGVK.GroupVersion().WithKind("HTTPRouteList"))
	if err := r.List(ctx, homeRoutes, client.InNamespace(namespace), client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"},
		client.HasLabels{"inference.opendatahub.io/external-model"}); err != nil {
		return fmt.Errorf("list tenant-local HTTPRoutes in %s: %w", namespace, err)
	}
	if len(homeRoutes.Items) > 0 {
		return nil
	}
	providers, models, err := r.attachmentTransportKeep(ctx, namespace, deleted)
	if err != nil {
		return err
	}
	for _, resource := range []struct {
		gvk   schema.GroupVersionKind
		label string
		keep  map[string]bool
	}{
		{schema.GroupVersionKind{Version: "v1", Kind: "ServiceList"}, "inference.opendatahub.io/external-provider", providers},
		{schema.GroupVersionKind{Version: "v1", Kind: "ServiceList"}, "inference.opendatahub.io/external-model", models},
		{schema.GroupVersionKind{Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntryList"}, "inference.opendatahub.io/external-provider", providers},
	} {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(resource.gvk)
		if err := r.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"},
			client.HasLabels{resource.label}); err != nil {
			return fmt.Errorf("list attachment transport in %s: %w", namespace, err)
		}
		for i := range list.Items {
			if resource.keep[list.Items[i].GetLabels()[resource.label]] {
				continue
			}
			if err := client.IgnoreNotFound(r.Delete(ctx, &list.Items[i])); err != nil {
				return fmt.Errorf("delete attachment transport %s/%s: %w", namespace, list.Items[i].GetName(), err)
			}
		}
	}
	return nil
}

// reconcileDeletingAttachedModel deletes the model's extra attachments, then
// runs the existing deletion path when the model still has home state.
func (r *Reconciler) reconcileDeletingAttachedModel(ctx context.Context, model *v1alpha1.ExternalModel) error {
	if err := r.releaseAttachments(ctx, model); err != nil {
		return err
	}
	hasHome, err := r.hasHomeRoute(ctx, model)
	if err != nil {
		return err
	}
	if hasHome {
		return r.reconcileDeletingModel(ctx, model)
	}
	return r.removeExternalModelFinalizer(ctx, model)
}

// releaseAttachments deletes every extra attachment of the model and the
// model-namespace transport no remaining attachment uses.
func (r *Reconciler) releaseAttachments(ctx context.Context, model *v1alpha1.ExternalModel) error {
	deleted, err := r.detachAttachments(ctx, model, nil)
	if err != nil {
		return err
	}
	return r.cleanupAttachmentModelTransport(ctx, model.Namespace, deleted)
}

// releaseRemovedAttachments handles a model whose gatewayRefs were removed
// entirely: its extra attachments are deleted, and outside a tenant namespace
// the finalizer that only guarded them is released.
func (r *Reconciler) releaseRemovedAttachments(ctx context.Context, model *v1alpha1.ExternalModel) error {
	if hasAttachments, err := r.hasAttachmentRoutes(ctx, model); err != nil || !hasAttachments {
		return err
	}
	if err := r.releaseAttachments(ctx, model); err != nil {
		return err
	}
	tenantNamespace, err := r.isTenantNamespace(ctx, model.Namespace)
	if err != nil || tenantNamespace {
		return err
	}
	if hasHome, err := r.hasHomeRoute(ctx, model); err != nil || hasHome {
		return err
	}
	return r.removeExternalModelFinalizer(ctx, model)
}

// hasAttachmentRoutes reports whether the model has extra attachment routes.
func (r *Reconciler) hasAttachmentRoutes(ctx context.Context, model *v1alpha1.ExternalModel) (bool, error) {
	routes, err := r.listAttachmentRoutes(ctx, client.InNamespace(model.Namespace))
	if err != nil {
		return false, err
	}
	return slices.ContainsFunc(routes, func(route unstructured.Unstructured) bool {
		return route.GetAnnotations()[annotationAttachmentModel] == model.Name
	}), nil
}

func (r *Reconciler) hasHomeRoute(ctx context.Context, model *v1alpha1.ExternalModel) (bool, error) {
	route := newHTTPRoute()
	if err := r.Get(ctx, client.ObjectKey{Namespace: model.Namespace, Name: modelRouteName(model.Name)}, route); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return route.GetLabels()["inference.opendatahub.io/external-model"] == model.Name, nil
}

func (r *Reconciler) isTenantNamespace(ctx context.Context, namespace string) (bool, error) {
	mtc := tenant.NewMaasTenantConfig()
	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: tenant.MaasTenantConfigInstanceName}, mtc); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get MaasTenantConfig %s: %w", namespace, err)
	}
	return true, nil
}
