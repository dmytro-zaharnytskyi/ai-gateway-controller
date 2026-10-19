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

package guardrail

import (
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// guardrailSelecting builds a policy whose checks select configIDs. Only
// Spec.Checks is populated; evaluateCompatible reads nothing else.
func guardrailSelecting(configIDs ...string) *aigatewayv1alpha1.AIGuardrail {
	checks := make([]aigatewayv1alpha1.AIGuardrailCheck, 0, len(configIDs))
	for i, configID := range configIDs {
		checks = append(checks, aigatewayv1alpha1.AIGuardrailCheck{
			Name:     fmt.Sprintf("check-%d", i),
			ConfigID: configID,
			Phases:   []aigatewayv1alpha1.GuardrailPhase{aigatewayv1alpha1.GuardrailPhaseInput},
		})
	}
	return &aigatewayv1alpha1.AIGuardrail{
		Spec: aigatewayv1alpha1.AIGuardrailSpec{Checks: checks},
	}
}

// nemoDeclaring builds a provider declaring names in spec.nemoConfigs.
func nemoDeclaring(names ...string) *unstructured.Unstructured {
	return withNemoConfigs(NewNemoGuardrails(), names...)
}

const (
	// providerSecret stands in for provider-owned configuration content. It is
	// planted where an apimachinery accessor error would echo it back, so a
	// test can assert it never reaches the consumer-visible message.
	providerSecret = "provider-only-do-not-publish"
	// malformedConfigsMessage is the fixed text evaluateCompatible publishes
	// for an unparsable spec.nemoConfigs.
	malformedConfigsMessage = "referenced NemoGuardrails provider has malformed configurations in spec.nemoConfigs"
)

// nemoWithRawConfigs builds a provider whose spec.nemoConfigs is set without
// going through the typed setters, so a test can plant a shape the API server
// would reject but an unstructured read still has to survive.
func nemoWithRawConfigs(configs any) *unstructured.Unstructured {
	u := NewNemoGuardrails()
	u.Object["spec"] = map[string]any{"nemoConfigs": configs}
	return u
}

func TestEvaluateCompatibleAcceptsDeclaredConfigs(t *testing.T) {
	cases := []struct {
		name      string
		guardrail *aigatewayv1alpha1.AIGuardrail
		provider  *unstructured.Unstructured
	}{
		{
			name:      "the only check selects the only configuration",
			guardrail: guardrailSelecting("jailbreak-v1"),
			provider:  nemoDeclaring("jailbreak-v1"),
		},
		{
			name:      "every check selects a declared configuration",
			guardrail: guardrailSelecting("jailbreak-v1", "pii-v2"),
			provider:  nemoDeclaring("pii-v2", "jailbreak-v1", "toxicity-v1"),
		},
		{
			name:      "two checks share one configuration",
			guardrail: guardrailSelecting("jailbreak-v1", "jailbreak-v1"),
			provider:  nemoDeclaring("jailbreak-v1"),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			verdict := evaluateCompatible(c.guardrail, c.provider)
			if !verdict.Compatible {
				t.Fatalf("evaluateCompatible = false (%s: %s), want true", verdict.Reason, verdict.Message)
			}
			if verdict.Reason != reasonChecksSatisfiable {
				t.Fatalf("reason = %q, want %q", verdict.Reason, reasonChecksSatisfiable)
			}
			// computeBindingRevision digests these instead of re-reading the
			// provider, so an accepted verdict has to carry them.
			if len(verdict.Configs) == 0 {
				t.Error("Configs is empty; the accepted path digests them")
			}
		})
	}
}

// TestEvaluateCompatibleRejectsUndeclaredConfigs covers the defect this whole
// condition exists for: a configId typo is accepted by the API server, because
// nothing enforces referential integrity between a check and the provider's
// configurations, and would otherwise surface only as a runtime failure on the
// NeMo call.
func TestEvaluateCompatibleRejectsUndeclaredConfigs(t *testing.T) {
	cases := []struct {
		name      string
		guardrail *aigatewayv1alpha1.AIGuardrail
		provider  *unstructured.Unstructured
		// wantNamed are the configIds the message must name, and the ones it
		// must not: a message that listed a satisfiable configId would send the
		// author to fix the wrong check.
		wantNamed   []string
		wantUnnamed []string
	}{
		{
			name:      "the only check selects an undeclared configuration",
			guardrail: guardrailSelecting("jailbreak-v2"),
			provider:  nemoDeclaring("jailbreak-v1"),
			wantNamed: []string{"jailbreak-v2"},
		},
		{
			name:        "one check of several is unsatisfiable",
			guardrail:   guardrailSelecting("jailbreak-v1", "pii-v9", "toxicity-v1"),
			provider:    nemoDeclaring("jailbreak-v1", "toxicity-v1"),
			wantNamed:   []string{"pii-v9"},
			wantUnnamed: []string{"jailbreak-v1", "toxicity-v1"},
		},
		{
			name:      "no check is satisfiable",
			guardrail: guardrailSelecting("a-v1", "b-v1"),
			provider:  nemoDeclaring("c-v1"),
			wantNamed: []string{"a-v1", "b-v1"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			verdict := evaluateCompatible(c.guardrail, c.provider)
			if verdict.Compatible {
				t.Fatal("evaluateCompatible = true, want false for an undeclared configId")
			}
			if verdict.Reason != reasonUnknownCheckConfig {
				t.Fatalf("reason = %q, want %q", verdict.Reason, reasonUnknownCheckConfig)
			}
			for _, configID := range c.wantNamed {
				if !strings.Contains(verdict.Message, configID) {
					t.Errorf("message %q does not name the undeclared configId %q", verdict.Message, configID)
				}
			}
			for _, configID := range c.wantUnnamed {
				if strings.Contains(verdict.Message, configID) {
					t.Errorf("message %q names %q, which the provider does declare", verdict.Message, configID)
				}
			}
		})
	}
}

// TestEvaluateCompatibleMessageIsStable pins the message byte-for-byte.
//
// Unknown configIds are collected by iterating a map, whose order Go
// randomises per run. An unstable message rewrites the condition on every
// reconcile, which bumps LastTransitionTime and re-emits a Warning event each
// time the unaccepted policy comes back round the resync loop — a slow event
// flood that only shows up in a long-running cluster.
func TestEvaluateCompatibleMessageIsStable(t *testing.T) {
	guardrail := guardrailSelecting("zeta-v1", "alpha-v1", "mu-v1", "alpha-v1")
	provider := nemoDeclaring("other-v1")
	want := "referenced NemoGuardrails provider does not declare configId(s): alpha-v1, mu-v1, zeta-v1"

	for i := range 32 {
		if message := evaluateCompatible(guardrail, provider).Message; message != want {
			t.Fatalf("message on call %d = %q, want %q", i, message, want)
		}
	}
}

// TestEvaluateCompatibleRejectsProvidersDeclaringNothing keeps a provider with
// no configurations from being read as "nothing to check, therefore fine".
func TestEvaluateCompatibleRejectsProvidersDeclaringNothing(t *testing.T) {
	cases := []struct {
		name     string
		provider *unstructured.Unstructured
	}{
		{"spec.nemoConfigs is absent", NewNemoGuardrails()},
		{"spec.nemoConfigs is an empty list", nemoDeclaring()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := evaluateCompatible(guardrailSelecting("jailbreak-v1"), c.provider)
			if result.Compatible {
				t.Fatal("evaluateCompatible = true, want false against a provider declaring no configurations")
			}
			if result.Reason != reasonProviderConfigsUnavailable {
				t.Fatalf("reason = %q, want %q", result.Reason, reasonProviderConfigsUnavailable)
			}
			if result.Message == "" {
				t.Fatal("message is empty; the condition must explain why the checks are unsatisfiable")
			}
		})
	}
}

// TestEvaluateCompatibleRejectsMalformedConfigs covers reads this controller
// cannot interpret. A shape it does not understand is not evidence that a
// configuration is declared, so it refuses rather than publishing a verdict
// drawn from a partial parse.
func TestEvaluateCompatibleRejectsMalformedConfigs(t *testing.T) {
	cases := []struct {
		name     string
		provider *unstructured.Unstructured
	}{
		{"spec.nemoConfigs is not a list", nemoWithRawConfigs(providerSecret)},
		{"an entry is not an object", nemoWithRawConfigs([]any{providerSecret})},
		{"a name is not a string", nemoWithRawConfigs([]any{map[string]any{"name": []any{providerSecret}}})},
		{
			name:     "a later entry is malformed",
			provider: nemoWithRawConfigs([]any{map[string]any{"name": "jailbreak-v1"}, providerSecret}),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := evaluateCompatible(guardrailSelecting("jailbreak-v1"), c.provider)
			if result.Compatible {
				t.Fatal("evaluateCompatible = true, want false against a provider this controller cannot parse")
			}
			if result.Reason != reasonInvalidProviderConfigs {
				t.Fatalf("reason = %q, want %q", result.Reason, reasonInvalidProviderConfigs)
			}
			// Pinned exactly. The message lands on a condition and a Warning
			// Event in the consumer's namespace, readable by anyone who can
			// read that namespace, while the provider may be owned by someone
			// else entirely. apimachinery's accessor errors embed the value
			// they rejected, so interpolating one here would republish the
			// provider's own spec.
			if result.Message != malformedConfigsMessage {
				t.Errorf("message = %q, want the fixed %q", result.Message, malformedConfigsMessage)
			}
			if strings.Contains(result.Message, providerSecret) {
				t.Errorf("message %q leaks the provider's spec.nemoConfigs content", result.Message)
			}
			// Still diagnosable: the detail has to reach the caller so it can
			// be logged, it just must not be published.
			if result.Err == nil {
				t.Fatal("Err is nil; the parse failure must still reach the caller for logging")
			}
			if !strings.Contains(result.Err.Error(), "nemoConfigs") {
				t.Errorf("error %q does not identify the field that failed to parse", result.Err)
			}
		})
	}
}
