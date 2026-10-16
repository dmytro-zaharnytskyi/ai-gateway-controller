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

import "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

// NewAITenant returns an empty unstructured object with the AITenant GVK
// set, ready for Get. This controller only ever Gets a specific AITenant by
// name/namespace (to resolve status.gatewayRef / deploy readiness for the
// MaasTenantConfig it primarily watches).
func NewAITenant() *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(AITenantGVK)
	return u
}

// GatewayRef reads status.gatewayRef.{name,namespace}. ok is false until
// the AITenant reconciler (maas-controller) has resolved and published the
// Gateway reference; callers should requeue rather than treat that as an
// error.
func GatewayRef(aitenant *unstructured.Unstructured) (name, namespace string, ok bool) {
	name, _, _ = unstructured.NestedString(aitenant.Object, "status", "gatewayRef", "name")
	namespace, _, _ = unstructured.NestedString(aitenant.Object, "status", "gatewayRef", "namespace")
	return name, namespace, name != "" && namespace != ""
}

// Phase reads status.phase.
func Phase(aitenant *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(aitenant.Object, "status", "phase")
	return phase
}

// IsFailed reports status.phase == Failed. maas writes status.gatewayRef
// before the exclusive gateway claim; GatewayClaimFailed (and other Failed
// reasons) leave gatewayRef pointing at a gateway this tenant does not own.
func IsFailed(aitenant *unstructured.Unstructured) bool {
	return Phase(aitenant) == AITenantPhaseFailed
}

// StatusIsCurrent reports whether the AITenant's Ready condition was computed
// for the object's current spec generation — i.e. status.phase and
// status.gatewayRef reflect the live spec, not an in-flight older one that
// maas-controller has not reconciled yet. It looks for the
// AITenantConditionReady condition and requires its observedGeneration to
// equal metadata.generation; maas-controller stamps that on every phase
// transition (setAITenantPhase), including Pending and Failed.
func StatusIsCurrent(aitenant *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(aitenant.Object, "status", "conditions")
	for _, entry := range conditions {
		condition, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		condType, _, _ := unstructured.NestedString(condition, "type")
		if condType != AITenantConditionReady {
			continue
		}
		observed, ok := nestedInt64(condition, "observedGeneration")
		return ok && observed == aitenant.GetGeneration()
	}
	return false
}

// DeployReady reports whether praxis-extproc apply / ExternalModel publish may
// target this AITenant. It requires a Ready condition stamped for the current
// generation (Pending is fine — that is what breaks the Active↔MTC Ready
// deadlock) and rejects Failed so a GatewayClaimFailed re-home does not place
// EnvoyFilters or routes next to another tenant's claim. Callers still require
// a populated gatewayRef (and handshake / Gateway existence as appropriate).
func DeployReady(aitenant *unstructured.Unstructured) bool {
	return StatusIsCurrent(aitenant) && !IsFailed(aitenant)
}

// nestedInt64 reads an integer field that may have been decoded as int64
// (runtime.DefaultUnstructuredConverter) or float64 (plain encoding/json).
func nestedInt64(m map[string]any, key string) (int64, bool) {
	switch v := m[key].(type) {
	case int64:
		return v, true
	case float64:
		return int64(v), true
	}
	return 0, false
}
