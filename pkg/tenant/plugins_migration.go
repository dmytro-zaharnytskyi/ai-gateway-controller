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
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// AnnotationForcePayloadProcessingMigration mirrors maas-controller's
	// tenantreconcile.AnnotationForcePayloadProcessingMigration (RHOAIENG-98846).
	// When "true" on MaasTenantConfig, this controller may overwrite a leftover
	// IPP-shaped plugins ConfigMap during Praxis apply.
	AnnotationForcePayloadProcessingMigration = "maas.opendatahub.io/force-payload-processing-migration"

	ippPreProcessingConfigKey = "custom-pre-processing-ipp-config.yaml"
	ippProcessingConfigKey    = "custom-ipp-config.yaml"
)

// ensurePluginsConfigMapMigrationAllowed is defense-in-depth for RHOAIENG-98846.
// maas-controller owns the primary gate (fingerprint known-good baselines before
// IPP cleanup). If an IPP-shaped plugins ConfigMap is somehow still present when
// this controller is about to apply Praxis, refuse the overwrite unless the
// operator set the force annotation — so custom plugin edits are not silently
// replaced after a partial or raced handoff.
func (r *Reconciler) ensurePluginsConfigMapMigrationAllowed(
	ctx context.Context,
	mtc *unstructured.Unstructured,
	gatewayNamespace, tenantID string,
) error {
	if forcePayloadProcessingMigration(mtc) {
		return nil
	}

	cm := &corev1.ConfigMap{}
	key := client.ObjectKey{
		Namespace: gatewayNamespace,
		Name:      PayloadProcessingPluginsConfigMapForTenant(tenantID),
	}
	// Read live from the API server, not the informer cache: a cache-backed
	// Get can lag and return stale NotFound or Praxis-shaped data while the
	// live ConfigMap still holds IPP keys. That would let this guard pass and
	// render.Apply ForceOwnership-overwrite the IPP config. APIReader must be
	// wired (see Reconciler.APIReader); reads fail rather than silently degrade.
	if err := r.APIReader.Get(ctx, key, cm); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get plugins ConfigMap %s/%s: %w", key.Namespace, key.Name, err)
	}
	if !isIPPPluginsConfigMapData(cm.Data) {
		return nil
	}
	return fmt.Errorf(
		"live IPP-shaped plugins ConfigMap %s/%s still present; refusing Praxis overwrite without %s=true (maas-controller should gate/cleanup first)",
		cm.Namespace, cm.Name, AnnotationForcePayloadProcessingMigration,
	)
}

func forcePayloadProcessingMigration(mtc *unstructured.Unstructured) bool {
	if mtc == nil {
		return false
	}
	ann := mtc.GetAnnotations()
	return ann != nil && ann[AnnotationForcePayloadProcessingMigration] == "true"
}

func isIPPPluginsConfigMapData(data map[string]string) bool {
	if data == nil {
		return false
	}
	_, hasPre := data[ippPreProcessingConfigKey]
	_, hasPost := data[ippProcessingConfigKey]
	return hasPre || hasPost
}
