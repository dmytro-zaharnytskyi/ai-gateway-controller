/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tenant

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestEnsurePluginsConfigMapMigrationAllowed(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	mtc := &unstructured.Unstructured{}
	mtc.SetGroupVersionKind(MaasTenantConfigGVK)
	mtc.SetName(MaasTenantConfigInstanceName)
	mtc.SetNamespace("rhoai")

	t.Run("absent allows", func(t *testing.T) {
		t.Parallel()
		c := fake.NewClientBuilder().WithScheme(scheme).Build()
		r := &Reconciler{Client: c, APIReader: c}
		if err := r.ensurePluginsConfigMapMigrationAllowed(context.Background(), mtc, "openshift-ingress", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("praxis-shaped allows", func(t *testing.T) {
		t.Parallel()
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: PayloadProcessingPluginsConfigMapName, Namespace: "openshift-ingress"},
			Data:       map[string]string{"extproc.yaml": "server: {}"},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		r := &Reconciler{Client: c, APIReader: c}
		if err := r.ensurePluginsConfigMapMigrationAllowed(context.Background(), mtc, "openshift-ingress", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ipp-shaped blocks without force", func(t *testing.T) {
		t.Parallel()
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: PayloadProcessingPluginsConfigMapName, Namespace: "openshift-ingress"},
			Data: map[string]string{
				ippProcessingConfigKey: "kind: PayloadProcessorConfig\n",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		r := &Reconciler{Client: c, APIReader: c}
		err := r.ensurePluginsConfigMapMigrationAllowed(context.Background(), mtc, "openshift-ingress", "")
		if err == nil {
			t.Fatal("expected error for IPP-shaped ConfigMap without force annotation")
		}
	})

	t.Run("ipp-shaped allows with force", func(t *testing.T) {
		t.Parallel()
		forced := mtc.DeepCopy()
		forced.SetAnnotations(map[string]string{AnnotationForcePayloadProcessingMigration: "true"})
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: PayloadProcessingPluginsConfigMapName, Namespace: "openshift-ingress"},
			Data: map[string]string{
				ippProcessingConfigKey: "kind: PayloadProcessorConfig\n",
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
		r := &Reconciler{Client: c, APIReader: c}
		if err := r.ensurePluginsConfigMapMigrationAllowed(context.Background(), forced, "openshift-ingress", ""); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	// Cache lag: Client sees NotFound / Praxis-shaped; live still has IPP keys.
	// The guard must use APIReader so ForceOwnership cannot overwrite IPP config.
	t.Run("blocks on live IPP when cache is stale NotFound", func(t *testing.T) {
		t.Parallel()
		liveIPP := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: PayloadProcessingPluginsConfigMapName, Namespace: "openshift-ingress"},
			Data: map[string]string{
				ippProcessingConfigKey: "kind: PayloadProcessorConfig\n",
			},
		}
		cacheClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveIPP).Build()
		r := &Reconciler{Client: cacheClient, APIReader: liveClient}
		err := r.ensurePluginsConfigMapMigrationAllowed(context.Background(), mtc, "openshift-ingress", "")
		if err == nil {
			t.Fatal("expected error when live ConfigMap is IPP-shaped even if cache returns NotFound")
		}
	})
}
