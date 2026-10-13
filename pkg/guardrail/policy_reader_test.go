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
	"errors"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

func guardrailScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := aigatewayv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("AddToScheme() error = %v", err)
	}
	return scheme
}

func testAIGuardrail() *aigatewayv1alpha1.AIGuardrail {
	return &aigatewayv1alpha1.AIGuardrail{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  testNamespace,
			Name:       "safety",
			UID:        types.UID("uid-1"),
			Generation: 3,
		},
		Spec: aigatewayv1alpha1.AIGuardrailSpec{
			Checks: []aigatewayv1alpha1.AIGuardrailCheck{
				{Name: "toxicity", ConfigID: "cfg-tox", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}},
				{Name: "pii", ConfigID: "cfg-pii", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput, aigatewayv1alpha1.GuardrailPhaseOutput}},
			},
		},
		Status: aigatewayv1alpha1.AIGuardrailStatus{
			ObservedGeneration: 3,
			BindingRevision:    "binding-revision-1",
			Conditions: []metav1.Condition{{
				Type:               ConditionAccepted,
				Status:             metav1.ConditionTrue,
				ObservedGeneration: 3,
				Reason:             "PolicyAccepted",
			}},
		},
	}
}

func TestClientPolicyReaderProjectsAIGuardrail(t *testing.T) {
	client := fake.NewClientBuilder().WithScheme(guardrailScheme(t)).WithObjects(testAIGuardrail()).Build()
	reader := NewPolicyReader(client)

	policy, err := reader.GetPolicy(context.Background(), testNamespace, "safety")
	if err != nil {
		t.Fatalf("GetPolicy() error = %v", err)
	}

	want := &Policy{
		Namespace:       testNamespace,
		Name:            "safety",
		UID:             types.UID("uid-1"),
		Generation:      3,
		BindingRevision: "binding-revision-1",
		Conditions: []PolicyCondition{{
			Type:               ConditionAccepted,
			Status:             true,
			ObservedGeneration: 3,
		}},
		Checks: []Check{
			{Name: "toxicity", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}, SpecIndex: 0},
			{Name: "pii", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput, aigatewayv1alpha1.GuardrailPhaseOutput}, SpecIndex: 1},
		},
	}
	if !reflect.DeepEqual(policy, want) {
		t.Errorf("policy = %+v, want %+v", policy, want)
	}
}

func TestClientPolicyReaderDoesNotFallBackAcrossNamespaces(t *testing.T) {
	guardrail := testAIGuardrail()
	guardrail.Namespace = "other-ns"
	client := fake.NewClientBuilder().WithScheme(guardrailScheme(t)).WithObjects(guardrail).Build()
	reader := NewPolicyReader(client)

	policy, err := reader.GetPolicy(context.Background(), testNamespace, "safety")
	if !errors.Is(err, ErrPolicyNotFound) {
		t.Fatalf("error = %v, want errors.Is(%v)", err, ErrPolicyNotFound)
	}
	if policy != nil {
		t.Errorf("policy = %+v, want nil", policy)
	}
}

func TestClientPolicyReaderClassifiesReadFailure(t *testing.T) {
	client := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build()
	reader := NewPolicyReader(client)

	policy, err := reader.GetPolicy(context.Background(), testNamespace, "safety")
	if !errors.Is(err, ErrReadFailure) {
		t.Fatalf("error = %v, want errors.Is(%v)", err, ErrReadFailure)
	}
	if policy != nil {
		t.Errorf("policy = %+v, want nil", policy)
	}
}

func TestClientPolicyReaderWithResolverRejectsUnknownCondition(t *testing.T) {
	guardrail := testAIGuardrail()
	guardrail.Status.Conditions[0].Status = metav1.ConditionUnknown
	client := fake.NewClientBuilder().WithScheme(guardrailScheme(t)).WithObjects(guardrail).Build()
	resolver := NewAttachmentResolver(NewPolicyReader(client))

	resolved, err := resolver.Resolve(context.Background(), testNamespace, attachment("safety"))
	if !errors.Is(err, ErrNotAccepted) {
		t.Fatalf("error = %v, want errors.Is(%v)", err, ErrNotAccepted)
	}
	if resolved != nil {
		t.Errorf("result = %+v, want nil", resolved)
	}
}

func TestPolicyProjectionCopiesPhases(t *testing.T) {
	source := testAIGuardrail()
	policy := policyFromAIGuardrail(source)

	policy.Checks[0].Phases[0] = aigatewayv1alpha1.GuardrailPhaseOutput
	if source.Spec.Checks[0].Phases[0] != aigatewayv1alpha1.GuardrailPhaseInput {
		t.Errorf("mutating projected phases changed the source AIGuardrail")
	}
}
