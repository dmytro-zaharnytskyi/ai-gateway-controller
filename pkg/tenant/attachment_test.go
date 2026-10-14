package tenant

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/inference/v1alpha1"
)

func TestTenantProvidersExcludeModelsBoundToOtherGateways(t *testing.T) {
	ref := func(name string) []v1alpha1.ExternalProviderRef {
		return []v1alpha1.ExternalProviderRef{{Ref: v1alpha1.ExternalProviderReference{Name: name}}}
	}
	local := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "local", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{ExternalProviderRefs: ref("local")}}
	own := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "own", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{
		ExternalProviderRefs: ref("own"), GatewayRefs: []v1alpha1.NamespacedObjectReference{{Name: "gw-a", Namespace: "gateway-a"}},
	}}
	elsewhere := &v1alpha1.ExternalModel{ObjectMeta: metav1.ObjectMeta{Name: "elsewhere", Namespace: "tenant-a"}, Spec: v1alpha1.ExternalModelSpec{
		ExternalProviderRefs: ref("elsewhere"), GatewayRefs: []v1alpha1.NamespacedObjectReference{{Name: "gw-b", Namespace: "gateway-b"}},
	}}
	r := &Reconciler{Client: fake.NewClientBuilder().WithScheme(mtcSchemeForTests()).WithObjects(local, own, elsewhere).Build()}
	_, required, err := r.providersForTenant(context.Background(), "tenant-a", "gw-a", "gateway-a")
	if err != nil {
		t.Fatal(err)
	}
	if !required["local"] || !required["own"] || required["elsewhere"] || len(required) != 2 {
		t.Fatalf("required providers = %v, want only models the tenant Gateway serves", required)
	}
}
