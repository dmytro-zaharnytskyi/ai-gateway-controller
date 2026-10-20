package controller

import (
	"context"
	"fmt"
	"slices"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

// Cross-namespace ExternalProvider references. A reference to a provider in
// another namespace is served only when a ReferenceGrant in the provider
// namespace permits ExternalModels of the model namespace to reference it.
// The provider then gets hashed transport in the model namespace and is
// otherwise served like an attachment's provider: a hashed Gateway
// DestinationRule and copies of credential Secrets outside the tenant
// namespace.
const (
	reasonRefNotPermitted = "RefNotPermitted"

	// labelProviderNamespace marks model-namespace transport for a provider
	// in another namespace. It replaces the local provider label, so the
	// reconciler of a same-named local provider never collects it.
	labelProviderNamespace = "inference.opendatahub.io/external-provider-namespace"
)

// ReferenceGrant v1beta1 is the storage version and is served by every
// standard Gateway API install.
var referenceGrantGVK = schema.GroupVersionKind{Group: "gateway.networking.k8s.io", Version: "v1beta1", Kind: "ReferenceGrant"}

func newReferenceGrant() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(referenceGrantGVK)
	return u
}

// referencePermitted reports whether a ReferenceGrant in the provider
// namespace permits ExternalModels of modelNamespace to reference provider.
func (r *Reconciler) referencePermitted(ctx context.Context, modelNamespace string, provider types.NamespacedName) (bool, error) {
	grants := &unstructured.UnstructuredList{}
	grants.SetGroupVersionKind(referenceGrantGVK.GroupVersion().WithKind("ReferenceGrantList"))
	if err := r.List(ctx, grants, client.InNamespace(provider.Namespace)); err != nil {
		return false, fmt.Errorf("list ReferenceGrants in %s: %w", provider.Namespace, err)
	}
	group := v1alpha1.GroupVersion.Group
	for i := range grants.Items {
		from, _, _ := unstructured.NestedSlice(grants.Items[i].Object, "spec", "from")
		to, _, _ := unstructured.NestedSlice(grants.Items[i].Object, "spec", "to")
		if slices.ContainsFunc(from, func(raw any) bool {
			f, _ := raw.(map[string]any)
			return f["group"] == group && f["kind"] == "ExternalModel" && f["namespace"] == modelNamespace
		}) && slices.ContainsFunc(to, func(raw any) bool {
			t, _ := raw.(map[string]any)
			name, _ := t["name"].(string)
			return t["group"] == group && t["kind"] == "ExternalProvider" && (name == "" || name == provider.Name)
		}) {
			return true, nil
		}
	}
	return false, nil
}

// foreignProviders returns the providers in other namespaces that the models
// reference and a ReferenceGrant permits the model namespace to reference.
// Like an attachment's providers they are validated here, not through their
// own namespace's tenant. A missing or deleting provider is omitted like an
// unpermitted one, so a reference never discloses whether a provider exists.
func (r *Reconciler) foreignProviders(ctx context.Context, modelNamespace string, models []*v1alpha1.ExternalModel) ([]*v1alpha1.ExternalProvider, error) {
	if r.Namespace != "" {
		// A single-namespace controller reads no other namespace.
		return nil, nil
	}
	refs := map[types.NamespacedName]bool{}
	for _, m := range models {
		for _, ref := range m.Spec.ExternalProviderRefs {
			if ref.Ref.Namespace != "" && ref.Ref.Namespace != modelNamespace {
				refs[types.NamespacedName{Namespace: ref.Ref.Namespace, Name: ref.Ref.Name}] = true
			}
		}
	}
	providers := make([]*v1alpha1.ExternalProvider, 0, len(refs))
	for key := range refs {
		if permitted, err := r.referencePermitted(ctx, modelNamespace, key); err != nil || !permitted {
			if err != nil {
				return nil, err
			}
			continue
		}
		var provider v1alpha1.ExternalProvider
		if err := r.Get(ctx, key, &provider); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, fmt.Errorf("get ExternalProvider %s: %w", key, err)
		}
		if !provider.DeletionTimestamp.IsZero() {
			continue
		}
		err := r.validateProvider(ctx, &provider)
		if err == nil {
			err = r.validateProviderCredential(ctx, &provider)
		}
		if transientReadError(err) {
			return nil, err
		}
		provider.Status.Phase = resolver.PhaseReady
		if err != nil {
			provider.Status.Phase = providerPhaseFailed
		}
		providers = append(providers, &provider)
	}
	return providers, nil
}

// deniedReferences maps each model with a provider reference that is not
// permitted to that reference's message.
func deniedReferences(set *resolver.ResolvedRouteSet) map[string]string {
	denied := map[string]string{}
	if set == nil {
		return denied
	}
	for _, model := range set.Models {
		for _, skip := range model.Skips {
			if _, seen := denied[model.ModelRef]; !seen && skip.Reason == resolver.SkipRefNotPermitted {
				denied[model.ModelRef] = skip.Message
			}
		}
	}
	return denied
}

// referenceGrantModels enqueues the models that reference a provider in the
// grant's namespace, so a granted reference recovers and a revoked one stops
// serving.
func (r *Reconciler) referenceGrantModels(ctx context.Context, grant *unstructured.Unstructured) []reconcile.Request {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models); err != nil {
		r.Log.Error(err, "list models for ReferenceGrant event", "namespace", grant.GetNamespace())
		return nil
	}
	var requests []reconcile.Request
	for i := range models.Items {
		m := &models.Items[i]
		if m.Namespace != grant.GetNamespace() && r.namespaceAllowed(m.Namespace) &&
			slices.ContainsFunc(m.Spec.ExternalProviderRefs, func(ref v1alpha1.ExternalProviderRef) bool { return ref.Ref.Namespace == grant.GetNamespace() }) {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(m)})
		}
	}
	return requests
}

// providerIdentity identifies the provider of hashed model-namespace transport.
func providerIdentity(route resolver.Route) string {
	return route.ProviderNamespace + "/" + route.Provider
}

// verifyProviderTransport refuses to adopt an object at a hashed provider
// transport name that was created for another provider.
func (r *Reconciler) verifyProviderTransport(ctx context.Context, resources []unstructured.Unstructured) error {
	for i := range resources {
		identity := resources[i].GetAnnotations()[annotationAttachmentIdentity]
		if identity == "" {
			continue
		}
		existing := &unstructured.Unstructured{}
		existing.SetGroupVersionKind(resources[i].GroupVersionKind())
		if err := r.Get(ctx, client.ObjectKeyFromObject(&resources[i]), existing); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("get %s %s/%s: %w", resources[i].GetKind(), resources[i].GetNamespace(), resources[i].GetName(), err)
		}
		if existing.GetAnnotations()[annotationAttachmentIdentity] != identity {
			return fmt.Errorf("%s %s/%s exists with a different identity; refusing to adopt it", resources[i].GetKind(), resources[i].GetNamespace(), resources[i].GetName())
		}
	}
	return nil
}

// collectProviderTransport deletes the namespace's transport for providers in
// other namespaces that no ExternalModel there references any more. A revoked
// grant keeps it until then: without route rules, overlay entries,
// DestinationRules and credentials it serves nothing.
func (r *Reconciler) collectProviderTransport(ctx context.Context, namespace string) error {
	var models v1alpha1.ExternalModelList
	if err := r.List(ctx, &models, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("list ExternalModels in %s: %w", namespace, err)
	}
	referenced := map[string]bool{}
	for i := range models.Items {
		for _, ref := range models.Items[i].Spec.ExternalProviderRefs {
			referenced[ref.Ref.Namespace+"/"+ref.Ref.Name] = true
		}
	}
	for _, gvk := range []schema.GroupVersionKind{
		{Version: "v1", Kind: "ServiceList"},
		{Group: "networking.istio.io", Version: "v1", Kind: "ServiceEntryList"},
	} {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk)
		if err := r.List(ctx, list, client.InNamespace(namespace),
			client.MatchingLabels{"app.kubernetes.io/managed-by": "ai-gateway-controller"}, client.HasLabels{labelProviderNamespace}); err != nil {
			return fmt.Errorf("list provider transport in %s: %w", namespace, err)
		}
		for i := range list.Items {
			identity := list.Items[i].GetAnnotations()[annotationAttachmentIdentity]
			if identity == "" || referenced[identity] {
				continue
			}
			if err := client.IgnoreNotFound(r.Delete(ctx, &list.Items[i])); err != nil {
				return fmt.Errorf("delete provider transport %s/%s: %w", namespace, list.Items[i].GetName(), err)
			}
		}
	}
	return nil
}
