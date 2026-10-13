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

	"k8s.io/apimachinery/pkg/types"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

const testNamespace = "tenant-ns"

type fakePolicyReader struct {
	policy *Policy
	err    error

	namespace string
	name      string
}

func (f *fakePolicyReader) GetPolicy(_ context.Context, namespace, name string) (*Policy, error) {
	f.namespace = namespace
	f.name = name
	if f.err != nil {
		return nil, f.err
	}
	return f.policy, nil
}

func acceptedPolicy() *Policy {
	return &Policy{
		Namespace:       testNamespace,
		Name:            "safety",
		UID:             types.UID("uid-1"),
		Generation:      2,
		BindingRevision: "binding-revision-1",
		Conditions: []PolicyCondition{{
			Type:               ConditionAccepted,
			Status:             true,
			ObservedGeneration: 2,
		}},
		Checks: []Check{
			{Name: "toxicity", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput}, SpecIndex: 0},
			{Name: "pii", Phases: []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput, aigatewayv1alpha1.GuardrailPhaseOutput}, SpecIndex: 1},
		},
	}
}

func attachment(name string, checks ...string) Attachment {
	return Attachment{Name: name, Checks: checks}
}

func TestAttachmentResolverResolve(t *testing.T) {
	tests := []struct {
		name       string
		policy     *Policy
		readerErr  error
		attachment Attachment
		wantChecks []Check
		wantErr    error
	}{
		{
			name:       "explicit subset preserves check metadata",
			policy:     acceptedPolicy(),
			attachment: attachment("safety", "pii"),
			wantChecks: []Check{acceptedPolicy().Checks[1]},
		},
		{
			name:       "explicit selection uses policy order",
			policy:     acceptedPolicy(),
			attachment: attachment("safety", "pii", "toxicity"),
			wantChecks: acceptedPolicy().Checks,
		},
		{
			name:       "omitted selection includes every check",
			policy:     acceptedPolicy(),
			attachment: Attachment{Name: "safety"},
			wantChecks: acceptedPolicy().Checks,
		},
		{
			name:       "empty selection includes every check",
			policy:     acceptedPolicy(),
			attachment: Attachment{Name: "safety", Checks: []string{}},
			wantChecks: acceptedPolicy().Checks,
		},
		{
			name:       "missing policy",
			readerErr:  ErrPolicyNotFound,
			attachment: attachment("safety"),
			wantErr:    ErrPolicyNotFound,
		},
		{
			name:       "read failure",
			readerErr:  ErrReadFailure,
			attachment: attachment("safety"),
			wantErr:    ErrReadFailure,
		},
		{
			name:       "reader returns no policy",
			attachment: attachment("safety"),
			wantErr:    ErrReadFailure,
		},
		{
			name:       "unknown check",
			policy:     acceptedPolicy(),
			attachment: attachment("safety", "unknown"),
			wantErr:    ErrUnknownCheck,
		},
		{
			name:       "duplicate check",
			policy:     acceptedPolicy(),
			attachment: attachment("safety", "pii", "pii"),
			wantErr:    ErrDuplicateCheck,
		},
		{
			name: "missing accepted condition",
			policy: func() *Policy {
				policy := acceptedPolicy()
				policy.Conditions = nil
				return policy
			}(),
			attachment: attachment("safety"),
			wantErr:    ErrNotAccepted,
		},
		{
			name: "rejected policy",
			policy: func() *Policy {
				policy := acceptedPolicy()
				policy.Conditions[0].Status = false
				return policy
			}(),
			attachment: attachment("safety"),
			wantErr:    ErrNotAccepted,
		},
		{
			name: "rejected policy precedes missing binding revision",
			policy: func() *Policy {
				policy := acceptedPolicy()
				policy.Conditions[0].Status = false
				policy.BindingRevision = ""
				return policy
			}(),
			attachment: attachment("safety"),
			wantErr:    ErrNotAccepted,
		},
		{
			name: "stale accepted condition",
			policy: func() *Policy {
				policy := acceptedPolicy()
				policy.Conditions[0].ObservedGeneration--
				return policy
			}(),
			attachment: attachment("safety"),
			wantErr:    ErrStaleGeneration,
		},
		{
			name: "stale condition precedes rejection and missing binding revision",
			policy: func() *Policy {
				policy := acceptedPolicy()
				policy.Conditions[0].ObservedGeneration--
				policy.Conditions[0].Status = false
				policy.BindingRevision = ""
				return policy
			}(),
			attachment: attachment("safety"),
			wantErr:    ErrStaleGeneration,
		},
		{
			name: "missing binding revision",
			policy: func() *Policy {
				policy := acceptedPolicy()
				policy.BindingRevision = ""
				return policy
			}(),
			attachment: attachment("safety"),
			wantErr:    ErrBindingUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &fakePolicyReader{policy: test.policy, err: test.readerErr}
			resolver := NewAttachmentResolver(reader)

			resolved, err := resolver.Resolve(context.Background(), testNamespace, test.attachment)

			if reader.namespace != testNamespace || reader.name != test.attachment.Name {
				t.Errorf("lookup = %s/%s, want %s/%s", reader.namespace, reader.name, testNamespace, test.attachment.Name)
			}
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("error = %v, want errors.Is(%v)", err, test.wantErr)
				}
				if resolved != nil {
					t.Fatalf("result = %+v, want nil", resolved)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if resolved.Namespace != test.policy.Namespace || resolved.Name != test.policy.Name ||
				resolved.UID != test.policy.UID || resolved.Generation != test.policy.Generation ||
				resolved.BindingRevision != test.policy.BindingRevision {
				t.Errorf("policy identity = %+v, want %+v", resolved, test.policy)
			}
			if !reflect.DeepEqual(resolved.Checks, test.wantChecks) {
				t.Errorf("checks = %+v, want %+v", resolved.Checks, test.wantChecks)
			}
		})
	}
}

func TestAttachmentResolverReturnsIndependentChecks(t *testing.T) {
	policy := acceptedPolicy()
	resolver := NewAttachmentResolver(&fakePolicyReader{policy: policy})

	resolved, err := resolver.Resolve(context.Background(), testNamespace, attachment("safety", "pii"))
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}

	resolved.Checks[0].Phases[0] = aigatewayv1alpha1.GuardrailPhaseOutput
	if policy.Checks[1].Phases[0] != aigatewayv1alpha1.GuardrailPhaseInput {
		t.Errorf("mutating resolved phases changed the source policy")
	}
}

type policyMapReader map[string]*Policy

func (r policyMapReader) GetPolicy(_ context.Context, _, name string) (*Policy, error) {
	policy, found := r[name]
	if !found {
		return nil, ErrPolicyNotFound
	}
	return policy, nil
}

func TestAttachmentResolverResolveAll(t *testing.T) {
	safety := acceptedPolicy()
	privacy := acceptedPolicy()
	privacy.Name = "privacy"
	privacy.UID = types.UID("uid-2")
	privacy.BindingRevision = "binding-revision-2"
	resolver := NewAttachmentResolver(policyMapReader{
		"safety":  safety,
		"privacy": privacy,
	})

	resolved, err := resolver.ResolveAll(context.Background(), testNamespace, []Attachment{
		attachment("safety", "pii"),
		attachment("privacy"),
	})
	if err != nil {
		t.Fatalf("ResolveAll() error = %v", err)
	}
	if len(resolved) != 2 {
		t.Fatalf("resolved policy count = %d, want 2", len(resolved))
	}
	if len(resolved[0].Checks) != 1 || len(resolved[1].Checks) != 2 {
		t.Errorf("resolved policies = %+v", resolved)
	}
	if resolved[0].Name != "safety" || resolved[1].Name != "privacy" {
		t.Errorf("policy order = [%q, %q], want [safety, privacy]", resolved[0].Name, resolved[1].Name)
	}
}

func TestAttachmentResolverResolveAllReturnsNoPartialResult(t *testing.T) {
	resolver := NewAttachmentResolver(policyMapReader{"safety": acceptedPolicy()})

	resolved, err := resolver.ResolveAll(context.Background(), testNamespace, []Attachment{
		attachment("safety"),
		attachment("missing"),
	})
	if !errors.Is(err, ErrPolicyNotFound) {
		t.Fatalf("error = %v, want errors.Is(%v)", err, ErrPolicyNotFound)
	}
	if resolved != nil {
		t.Errorf("result = %+v, want nil", resolved)
	}
}
