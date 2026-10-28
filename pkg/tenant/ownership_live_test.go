package tenant

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestWaitForForeignOwnershipUsesLiveReads(t *testing.T) {
	scheme := mtcSchemeForTests()
	desired := &unstructured.Unstructured{}
	desired.SetGroupVersionKind(gvkDeployment)
	desired.SetName("payload-processing")
	desired.SetNamespace("gateway-system")

	foreign := desired.DeepCopy()
	foreign.SetUID("live-uid")
	foreign.SetLabels(map[string]string{managedByLabel: "another-controller"})
	cacheClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()

	r := &Reconciler{Client: cacheClient, APIReader: liveClient}
	err := r.waitForForeignOwnership(context.Background(), []unstructured.Unstructured{*desired})
	if err == nil {
		t.Fatal("expected foreign ownership error from live client")
	}
	if !strings.Contains(err.Error(), "another-controller") {
		t.Fatalf("waitForForeignOwnership error = %v, want another-controller ownership error", err)
	}
}

func TestDeleteResourceIfOwnedUsesUIDPrecondition(t *testing.T) {
	scheme := mtcSchemeForTests()
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(gvkDeployment)
	live.SetName("payload-processing")
	live.SetNamespace("gateway-system")
	live.SetUID("live-uid")
	live.SetLabels(map[string]string{LabelManagedBy: ManagedByAIGatewayController})

	var preconditions *metav1.Preconditions
	cacheClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, opts ...client.DeleteOption) error {
			for _, opt := range opts {
				if deleteOptions, ok := opt.(*client.DeleteOptions); ok && deleteOptions.Preconditions != nil {
					preconditions = deleteOptions.Preconditions
				}
			}
			return nil
		},
	}).Build()
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(live).Build()

	r := &Reconciler{Client: cacheClient, APIReader: liveClient}
	if err := r.deleteResourceIfOwned(context.Background(), gvkDeployment, live.GetName(), live.GetNamespace()); err != nil {
		t.Fatalf("deleteResourceIfOwned: %v", err)
	}
	if preconditions == nil || preconditions.UID == nil || *preconditions.UID != live.GetUID() {
		t.Fatalf("delete preconditions = %#v, want UID %s", preconditions, live.GetUID())
	}
}
