/*
Copyright 2026 The opendatahub.io Authors.

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

package guardrail

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// ClientPolicyReader reads AIGuardrail policies with a controller-runtime
// client.
type ClientPolicyReader struct {
	client client.Reader
}

// NewPolicyReader returns a ClientPolicyReader backed by client. Reads use the
// supplied reader as-is; callers requiring API-server freshness should pass an
// uncached reader.
func NewPolicyReader(client client.Reader) *ClientPolicyReader {
	return &ClientPolicyReader{client: client}
}

// GetPolicy reads an AIGuardrail and projects the fields needed for resolution.
func (r *ClientPolicyReader) GetPolicy(ctx context.Context, namespace, name string) (*Policy, error) {
	var guardrail aigatewayv1alpha1.AIGuardrail
	key := types.NamespacedName{Namespace: namespace, Name: name}
	if err := r.client.Get(ctx, key, &guardrail); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("%s/%s: %w", namespace, name, ErrPolicyNotFound)
		}
		return nil, fmt.Errorf("%s/%s: %w: %w", namespace, name, ErrReadFailure, err)
	}
	return policyFromAIGuardrail(&guardrail), nil
}

func policyFromAIGuardrail(guardrail *aigatewayv1alpha1.AIGuardrail) *Policy {
	checks := make([]Check, 0, len(guardrail.Spec.Checks))
	for i, check := range guardrail.Spec.Checks {
		phases := make([]aigatewayv1alpha1.GuardrailPhase, len(check.Phases))
		copy(phases, check.Phases)
		checks = append(checks, Check{
			Name:      check.Name,
			Phases:    phases,
			SpecIndex: i,
		})
	}

	conditions := make([]PolicyCondition, 0, len(guardrail.Status.Conditions))
	for _, condition := range guardrail.Status.Conditions {
		conditions = append(conditions, PolicyCondition{
			Type:               condition.Type,
			Status:             condition.Status == metav1.ConditionTrue,
			ObservedGeneration: condition.ObservedGeneration,
		})
	}

	return &Policy{
		Namespace:       guardrail.Namespace,
		Name:            guardrail.Name,
		UID:             guardrail.UID,
		Generation:      guardrail.Generation,
		BindingRevision: guardrail.Status.BindingRevision,
		Conditions:      conditions,
		Checks:          checks,
	}
}
