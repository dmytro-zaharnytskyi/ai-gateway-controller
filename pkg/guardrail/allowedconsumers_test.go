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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// nemoWithNamespacesPolicy builds a NemoGuardrails fixture carrying exactly
// the spec.allowedConsumers.namespaces map given. A nil map omits
// allowedConsumers entirely, which is the shape of every NemoGuardrails that
// predates the field.
func nemoWithNamespacesPolicy(namespacesPolicy map[string]any) *unstructured.Unstructured {
	u := NewNemoGuardrails()
	u.SetName("nemo")
	u.SetNamespace("provider-ns")
	if namespacesPolicy != nil {
		if err := unstructured.SetNestedMap(u.Object, namespacesPolicy, "spec", "allowedConsumers", "namespaces"); err != nil {
			panic(err)
		}
	}
	return u
}

func TestParseAllowedConsumersAbsentPolicyDefaultsToSame(t *testing.T) {
	// TrustyAI has not shipped allowedConsumers yet, so this is the shape of
	// every provider in the field today. Defaulting it to Same rather than All
	// is what keeps a pre-field provider from being silently readable by every
	// namespace in the cluster.
	for _, c := range []struct {
		name string
		obj  *unstructured.Unstructured
	}{
		{"no spec at all", nemoWithNamespacesPolicy(nil)},
		{"empty namespaces map", nemoWithNamespacesPolicy(map[string]any{})},
		{"empty from string", nemoWithNamespacesPolicy(map[string]any{"from": ""})},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseAllowedConsumers(c.obj)
			if err != nil {
				t.Fatalf("parseAllowedConsumers: %v", err)
			}
			if got.Mode != consumerModeSame {
				t.Fatalf("mode = %q, want %q", got.Mode, consumerModeSame)
			}
			if got.Selector != nil {
				t.Fatalf("selector = %#v, want nil for Same", got.Selector)
			}
		})
	}
}

func TestParseAllowedConsumersAcceptsValidPolicies(t *testing.T) {
	cases := []struct {
		name         string
		policy       map[string]any
		wantMode     consumerMode
		wantSelector bool
	}{
		{
			name:     "explicit Same",
			policy:   map[string]any{"from": "Same"},
			wantMode: consumerModeSame,
		},
		{
			name:     "explicit All",
			policy:   map[string]any{"from": "All"},
			wantMode: consumerModeAll,
		},
		{
			name: "Selector with matchLabels",
			policy: map[string]any{
				"from":     "Selector",
				"selector": map[string]any{"matchLabels": map[string]any{"guardrails": "shared"}},
			},
			wantMode:     consumerModeSelector,
			wantSelector: true,
		},
		{
			name: "Selector with only matchExpressions",
			policy: map[string]any{
				"from": "Selector",
				"selector": map[string]any{"matchExpressions": []any{
					map[string]any{"key": "tier", "operator": "In", "values": []any{"gold"}},
				}},
			},
			wantMode:     consumerModeSelector,
			wantSelector: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseAllowedConsumers(nemoWithNamespacesPolicy(c.policy))
			if err != nil {
				t.Fatalf("parseAllowedConsumers: %v", err)
			}
			if got.Mode != c.wantMode {
				t.Fatalf("mode = %q, want %q", got.Mode, c.wantMode)
			}
			if (got.Selector != nil) != c.wantSelector {
				t.Fatalf("selector present = %v, want %v", got.Selector != nil, c.wantSelector)
			}
		})
	}
}

func TestParseAllowedConsumersRejectsInvalidPolicies(t *testing.T) {
	// Each of these is a way a policy could be read as more permissive than
	// its author intended. They are errors rather than silent defaults, and
	// the reconciler turns every error here into an unauthorized binding, so
	// a malformed policy denies instead of widening access.
	cases := []struct {
		name   string
		policy map[string]any
	}{
		{
			name:   "Selector without a selector would otherwise match everything",
			policy: map[string]any{"from": "Selector"},
		},
		{
			name:   "Selector with an empty selector would otherwise match everything",
			policy: map[string]any{"from": "Selector", "selector": map[string]any{}},
		},
		{
			name: "selector under Same suggests the author meant Selector",
			policy: map[string]any{
				"from":     "Same",
				"selector": map[string]any{"matchLabels": map[string]any{"a": "b"}},
			},
		},
		{
			name: "selector under All suggests the author meant Selector",
			policy: map[string]any{
				"from":     "All",
				"selector": map[string]any{"matchLabels": map[string]any{"a": "b"}},
			},
		},
		{
			name:   "defaulted Same with a selector",
			policy: map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"a": "b"}}},
		},
		{
			name:   "unknown mode",
			policy: map[string]any{"from": "Any"},
		},
		{
			name:   "mode is case sensitive",
			policy: map[string]any{"from": "all"},
		},
		{
			name:   "from is not a string",
			policy: map[string]any{"from": int64(7)},
		},
		{
			name:   "selector is not a map",
			policy: map[string]any{"from": "Selector", "selector": "shared"},
		},
		{
			name:   "selector has the wrong inner shape",
			policy: map[string]any{"from": "Selector", "selector": map[string]any{"matchLabels": "shared"}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parseAllowedConsumers(nemoWithNamespacesPolicy(c.policy))
			if err == nil {
				t.Fatalf("parseAllowedConsumers succeeded with %#v, want an error", got)
			}
		})
	}
}

func TestParseAllowedConsumersRejectsMalformedNamespacesNode(t *testing.T) {
	u := NewNemoGuardrails()
	u.SetName("nemo")
	u.SetNamespace("provider-ns")
	if err := unstructured.SetNestedField(u.Object, "Same", "spec", "allowedConsumers", "namespaces"); err != nil {
		t.Fatalf("SetNestedField: %v", err)
	}
	if _, err := parseAllowedConsumers(u); err == nil {
		t.Fatal("parseAllowedConsumers succeeded on a scalar namespaces node, want an error")
	}
}

func TestAllowedConsumersAllows(t *testing.T) {
	labelled := func(labels map[string]string) *corev1.Namespace {
		return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "consumer-ns", Labels: labels}}
	}
	shared := &metav1.LabelSelector{MatchLabels: map[string]string{"guardrails": "shared"}}

	cases := []struct {
		name       string
		policy     allowedConsumers
		consumer   string
		provider   string
		consumerNS *corev1.Namespace
		want       bool
		wantErr    bool
	}{
		{
			name:     "Same allows the provider's own namespace",
			policy:   allowedConsumers{Mode: consumerModeSame},
			consumer: "provider-ns", provider: "provider-ns",
			want: true,
		},
		{
			name:     "Same denies any other namespace",
			policy:   allowedConsumers{Mode: consumerModeSame},
			consumer: "consumer-ns", provider: "provider-ns",
			want: false,
		},
		{
			name:     "All allows a foreign namespace",
			policy:   allowedConsumers{Mode: consumerModeAll},
			consumer: "consumer-ns", provider: "provider-ns",
			want: true,
		},
		{
			name:     "All allows the provider's own namespace",
			policy:   allowedConsumers{Mode: consumerModeAll},
			consumer: "provider-ns", provider: "provider-ns",
			want: true,
		},
		{
			name:     "Selector matches on the Namespace object's labels",
			policy:   allowedConsumers{Mode: consumerModeSelector, Selector: shared},
			consumer: "consumer-ns", provider: "provider-ns",
			consumerNS: labelled(map[string]string{"guardrails": "shared"}),
			want:       true,
		},
		{
			name:     "Selector denies a non-matching Namespace",
			policy:   allowedConsumers{Mode: consumerModeSelector, Selector: shared},
			consumer: "consumer-ns", provider: "provider-ns",
			consumerNS: labelled(map[string]string{"guardrails": "private"}),
			want:       false,
		},
		{
			// The selector is not additive to Same: a provider that opts into
			// Selector is stating that every consumer must carry the label,
			// including one sitting in its own namespace.
			name:     "Selector is not additive to Same",
			policy:   allowedConsumers{Mode: consumerModeSelector, Selector: shared},
			consumer: "provider-ns", provider: "provider-ns",
			consumerNS: labelled(nil),
			want:       false,
		},
		{
			// A nil Namespace means missing or unreadable, which must never
			// match: failing open here would let a consumer evade the selector
			// simply by being unreadable to this controller.
			name:     "Selector denies when the Namespace could not be read",
			policy:   allowedConsumers{Mode: consumerModeSelector, Selector: shared},
			consumer: "consumer-ns", provider: "provider-ns",
			consumerNS: nil,
			want:       false,
		},
		{
			name:     "an unknown mode is an error, not an allow",
			policy:   allowedConsumers{Mode: consumerMode("Everyone")},
			consumer: "consumer-ns", provider: "provider-ns",
			wantErr: true,
		},
		{
			name: "an invalid selector is an error, not an allow",
			policy: allowedConsumers{Mode: consumerModeSelector, Selector: &metav1.LabelSelector{
				MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "tier", Operator: "Nonsense"}},
			}},
			consumer: "consumer-ns", provider: "provider-ns",
			consumerNS: labelled(map[string]string{"tier": "gold"}),
			wantErr:    true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := c.policy.allows(c.consumer, c.provider, c.consumerNS)
			if c.wantErr {
				if err == nil {
					t.Fatalf("allows = %v, nil; want an error", got)
				}
				if got {
					t.Fatal("allows returned true alongside an error, want false so the caller fails closed")
				}
				return
			}
			if err != nil {
				t.Fatalf("allows: %v", err)
			}
			if got != c.want {
				t.Fatalf("allows = %v, want %v", got, c.want)
			}
		})
	}
}
