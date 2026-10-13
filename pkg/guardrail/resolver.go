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

// Package guardrail resolves check selections against accepted AIGuardrail
// policies.
package guardrail

import (
	"context"
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/types"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// ConditionAccepted is authoritative only when its ObservedGeneration matches
// the AIGuardrail generation.
const ConditionAccepted = "Accepted"

var (
	// ErrPolicyNotFound means the named policy does not exist in the target namespace.
	ErrPolicyNotFound = errors.New("guardrail policy not found")
	// ErrReadFailure means the policy lookup failed for a reason other than absence.
	ErrReadFailure = errors.New("guardrail policy read failed")
	// ErrNotAccepted means the current policy is not accepted.
	ErrNotAccepted = errors.New("guardrail policy not accepted")
	// ErrStaleGeneration means the acceptance condition is not current.
	ErrStaleGeneration = errors.New("guardrail policy status is stale")
	// ErrBindingUnavailable means an accepted policy has no binding revision.
	ErrBindingUnavailable = errors.New("guardrail policy binding unavailable")
	// ErrUnknownCheck means the selected check is not defined by the policy.
	ErrUnknownCheck = errors.New("guardrail check not found in policy")
	// ErrDuplicateCheck means a selection contains the same check more than once.
	ErrDuplicateCheck = errors.New("guardrail check requested more than once")
)

// Attachment selects checks from an AIGuardrail policy.
type Attachment struct {
	// Name is the AIGuardrail name.
	Name string
	// Checks selects policy checks by name. An empty list selects all checks.
	Checks []string
}

// Check is a policy check and its request phases.
type Check struct {
	Name   string
	Phases []aigatewayv1alpha1.GuardrailPhase
	// SpecIndex is the check's position in AIGuardrail.spec.checks.
	SpecIndex int
}

// Policy is the AIGuardrail data the resolver needs.
type Policy struct {
	Namespace       string
	Name            string
	UID             types.UID
	Generation      int64
	BindingRevision string
	Conditions      []PolicyCondition
	Checks          []Check
}

// PolicyCondition is the status condition data used during resolution.
type PolicyCondition struct {
	Type               string
	Status             bool
	ObservedGeneration int64
}

// ResolvedPolicy contains the selected checks and the policy snapshot that produced them.
type ResolvedPolicy struct {
	Namespace       string
	Name            string
	UID             types.UID
	Generation      int64
	BindingRevision string
	Checks          []Check
}

// PolicyReader reads one policy by exact namespace and name. A missing policy
// returns ErrPolicyNotFound and other read errors return ErrReadFailure. On
// success it returns a non-nil Policy.
type PolicyReader interface {
	GetPolicy(ctx context.Context, namespace, name string) (*Policy, error)
}

// AttachmentResolver resolves attachments using a PolicyReader.
type AttachmentResolver struct {
	reader PolicyReader
}

// NewAttachmentResolver returns an AttachmentResolver backed by reader.
func NewAttachmentResolver(reader PolicyReader) *AttachmentResolver {
	return &AttachmentResolver{reader: reader}
}

// Resolve resolves an attachment in targetNamespace.
func (r *AttachmentResolver) Resolve(ctx context.Context, targetNamespace string, attachment Attachment) (*ResolvedPolicy, error) {
	policy, err := r.reader.GetPolicy(ctx, targetNamespace, attachment.Name)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return nil, fmt.Errorf("policy %q reader returned no policy: %w", attachment.Name, ErrReadFailure)
	}

	if err := checkAccepted(policy); err != nil {
		return nil, err
	}

	selected, err := selectChecks(policy, attachment.Checks)
	if err != nil {
		return nil, err
	}

	return &ResolvedPolicy{
		Namespace:       policy.Namespace,
		Name:            policy.Name,
		UID:             policy.UID,
		Generation:      policy.Generation,
		BindingRevision: policy.BindingRevision,
		Checks:          selected,
	}, nil
}

// ResolveAll resolves every attachment in order and returns no partial result when one fails.
func (r *AttachmentResolver) ResolveAll(ctx context.Context, targetNamespace string, attachments []Attachment) ([]ResolvedPolicy, error) {
	resolvedPolicies := make([]ResolvedPolicy, 0, len(attachments))
	for i := range attachments {
		resolved, err := r.Resolve(ctx, targetNamespace, attachments[i])
		if err != nil {
			return nil, err
		}
		resolvedPolicies = append(resolvedPolicies, *resolved)
	}
	return resolvedPolicies, nil
}

func checkAccepted(policy *Policy) error {
	condition := findCondition(policy.Conditions, ConditionAccepted)
	if condition == nil {
		return fmt.Errorf("policy %q condition %q is not True: %w", policy.Name, ConditionAccepted, ErrNotAccepted)
	}
	if condition.ObservedGeneration != policy.Generation {
		return fmt.Errorf("policy %q condition %q observedGeneration %d does not match generation %d: %w",
			policy.Name, ConditionAccepted, condition.ObservedGeneration, policy.Generation, ErrStaleGeneration)
	}
	if !condition.Status {
		return fmt.Errorf("policy %q condition %q is not True: %w", policy.Name, ConditionAccepted, ErrNotAccepted)
	}
	if policy.BindingRevision == "" {
		return fmt.Errorf("policy %q has no accepted binding revision: %w", policy.Name, ErrBindingUnavailable)
	}

	return nil
}

func findCondition(conditions []PolicyCondition, conditionType string) *PolicyCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

func selectChecks(policy *Policy, requested []string) ([]Check, error) {
	if len(requested) == 0 {
		return copyChecks(policy.Checks), nil
	}

	available := make(map[string]struct{}, len(policy.Checks))
	for _, check := range policy.Checks {
		available[check.Name] = struct{}{}
	}

	wanted := make(map[string]struct{}, len(requested))
	for _, name := range requested {
		if _, found := available[name]; !found {
			return nil, fmt.Errorf("policy %q does not define check %q: %w", policy.Name, name, ErrUnknownCheck)
		}
		if _, duplicate := wanted[name]; duplicate {
			return nil, fmt.Errorf("policy %q check %q requested more than once: %w", policy.Name, name, ErrDuplicateCheck)
		}
		wanted[name] = struct{}{}
	}

	selected := make([]Check, 0, len(requested))
	for _, check := range policy.Checks {
		if _, found := wanted[check.Name]; found {
			selected = append(selected, cloneCheck(check))
		}
	}
	return selected, nil
}

func copyChecks(checks []Check) []Check {
	result := make([]Check, 0, len(checks))
	for _, check := range checks {
		result = append(result, cloneCheck(check))
	}
	return result
}

func cloneCheck(check Check) Check {
	phases := make([]aigatewayv1alpha1.GuardrailPhase, len(check.Phases))
	copy(phases, check.Phases)
	return Check{Name: check.Name, Phases: phases, SpecIndex: check.SpecIndex}
}
