package controller

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
)

// RHOAIENG-103311: a tenant-namespace provider is not in use by a same-named
// provider of another namespace: neither by an attachment served from there,
// whose overlay cluster provider-<name> names the provider Service in its own
// namespace, nor by a tenant model that reaches a granted provider through its
// hashed transport. Its deletion must complete.
func TestDeletingTenantProviderIsNotHeldBySameNamedProviders(t *testing.T) {
	ctx := context.Background()
	tenantProvider, tenantSecret := attachmentProvider("tenant-b")
	teamProvider, teamSecret := attachmentProvider("team")
	attached := attachmentModel("chat", "team", "chat-model", time.Unix(100, 0), gatewayB)
	sharedProvider, sharedSecret := attachmentProvider("shared")
	foreign := referenceProvider(attachmentModel("own", "tenant-b", "own-model", time.Unix(50, 0)), "shared")
	objects := append([]client.Object{tenantProvider, tenantSecret, teamProvider, teamSecret, attached, sharedProvider, sharedSecret, foreign,
		referenceGrant("shared", "tenant-b", "openai")}, attachmentTenantObjects("tenant-b", "tenant-b", gatewayB, "praxis")...)
	r := attachmentReconciler(t, objects...)
	reconcileModel(t, r, foreign)
	reconcileModel(t, r, attached)
	setRouteParentStatus(t, r, attached, gatewayB, metav1.ConditionTrue, "Accepted")
	reconcileModel(t, r, attached)
	clusters := map[string]string{}
	for _, candidate := range overlayEnvelope(t, r, "tenant-b").Overlay.Candidates {
		clusters[candidate.StableID] = candidate.Cluster
	}
	if len(clusters) != 2 || clusters["team/chat/team/openai"] != providerServicePrefix+tenantProvider.Name || clusters["tenant-b/own/shared/openai"] == "" {
		t.Fatalf("overlay candidate clusters = %v, want the attachment under the tenant provider's cluster name and the foreign route", clusters)
	}

	var stored v1alpha1.ExternalProvider
	if err := r.Get(ctx, client.ObjectKeyFromObject(tenantProvider), &stored); err != nil || !controllerutil.ContainsFinalizer(&stored, externalProviderFinalizer) {
		t.Fatalf("tenant provider = %#v (%v), want it to hold the deletion finalizer", stored.ObjectMeta, err)
	}
	if err := r.Delete(ctx, &stored); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReconcileProvider(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(tenantProvider)}); err != nil {
		t.Fatalf("reconcile deleting provider: %v", err)
	}
	if err := r.Get(ctx, client.ObjectKeyFromObject(tenantProvider), &stored); !apierrors.IsNotFound(err) {
		t.Fatalf("tenant provider deletion is held by same-named providers of other namespaces: %v", err)
	}
}
