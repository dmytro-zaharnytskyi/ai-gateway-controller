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
	"regexp"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

const nemoUID = "7b1f0c2e-5a6d-4e3f-9b8a-0c1d2e3f4a5b"

// selectorPolicy is the Selector-mode policy the label cases share.
func selectorPolicy() allowedConsumers {
	return allowedConsumers{
		Mode: consumerModeSelector,
		Selector: &metav1.LabelSelector{
			MatchLabels: map[string]string{"guardrails": "shared"},
		},
	}
}

// revisionProvider builds a provider with a stable UID, since the UID is a
// digest input and an empty one would hide a field that must participate.
func revisionProvider(names ...string) *unstructured.Unstructured {
	u := withNemoConfigs(NewNemoGuardrails(), names...)
	u.SetName(nemoName)
	u.SetNamespace(providerNS)
	u.SetUID(types.UID(nemoUID))
	return u
}

// revisionEndpoint is the discovered endpoint the cases that are not about
// the endpoint hold constant. It is passed explicitly rather than derived
// from the provider: rebuilding TrustyAI's URL formula here would make the
// test agree with the code for the wrong reason, and the whole point of
// digesting the published value is that the formula is not ours.
const revisionEndpoint = "https://" + nemoName + "." + providerNS + ".svc.cluster.local"

func mustComputeRevision(
	t *testing.T,
	nemo *unstructured.Unstructured,
	endpoint string,
	policy allowedConsumers,
	consumerNS *corev1.Namespace,
) string {
	t.Helper()
	// Parsed here the way Reconcile parses once and hands the result on, so
	// the cases below keep naming configurations instead of maps.
	configs, err := parseNemoConfigs(nemo)
	if err != nil {
		t.Fatalf("parseNemoConfigs: %v", err)
	}
	revision, err := computeBindingRevision(nemo, configs, endpoint, policy, consumerNS)
	if err != nil {
		t.Fatalf("computeBindingRevision: %v", err)
	}
	return revision
}

// TestComputeBindingRevisionIsDeterministic is the property the whole design
// rests on: the digest is a pure function of the dependency state, with no
// stored counter and no read of the previous value.
//
// Both loops below iterate maps — declared configurations and selector label
// keys — whose order Go randomises per run. If either leaked into the digest,
// the revision would differ between two reconciles that observed identical
// dependencies, and every one of them would invalidate the downstream Praxis
// generation.
func TestComputeBindingRevisionIsDeterministic(t *testing.T) {
	namespace := newNamespace(consumerNS, map[string]string{"guardrails": "shared"})
	want := mustComputeRevision(t, revisionProvider("a-v1", "b-v1", "c-v1"), revisionEndpoint, selectorPolicy(), namespace)

	for i := range 32 {
		got := mustComputeRevision(t, revisionProvider("a-v1", "b-v1", "c-v1"), revisionEndpoint, selectorPolicy(), namespace)
		if got != want {
			t.Fatalf("revision on call %d = %q, want %q", i, got, want)
		}
	}
}

func TestComputeBindingRevisionIsLowercaseHexSHA256(t *testing.T) {
	revision := mustComputeRevision(t, revisionProvider(guardrailConfig), revisionEndpoint,
		allowedConsumers{Mode: consumerModeAll}, nil)
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(revision) {
		t.Fatalf("revision = %q, want 64 lowercase hex characters", revision)
	}
}

// TestComputeBindingRevisionChangesWithDependencies covers what the field
// exists for: each of these edits happens in another object, leaving this
// policy's spec and metadata.generation untouched, so the revision is the
// only signal a consumer has that its compiled catalog is out of date.
func TestComputeBindingRevisionChangesWithDependencies(t *testing.T) {
	baseProvider := func() *unstructured.Unstructured { return revisionProvider(guardrailConfig) }
	basePolicy := allowedConsumers{Mode: consumerModeSame}
	base := mustComputeRevision(t, baseProvider(), revisionEndpoint, basePolicy, nil)

	cases := []struct {
		name     string
		provider *unstructured.Unstructured
		// endpoint is the discovered endpoint for the case; empty means the
		// unchanged revisionEndpoint, so only the endpoint case sets it.
		endpoint string
		policy   allowedConsumers
		// why records the dependency change the case stands for, so a failure
		// says what went unnoticed rather than just that two hashes matched.
		why string
	}{
		{
			name: "the provider was deleted and recreated at the same name",
			provider: func() *unstructured.Unstructured {
				u := baseProvider()
				u.SetUID("00000000-0000-0000-0000-000000000000")
				return u
			}(),
			policy: basePolicy,
			why:    "a new object took the referenced name with an identical spec",
		},
		{
			name:     "the provider moved to another namespace",
			provider: func() *unstructured.Unstructured { u := baseProvider(); u.SetNamespace("elsewhere"); return u }(),
			policy:   basePolicy,
			why:      "the binding resolves to a different object",
		},
		{
			name:     "the provider declares an additional configuration",
			provider: revisionProvider(guardrailConfig, "pii-v2"),
			policy:   basePolicy,
			why:      "the provider's configuration list was edited",
		},
		{
			name:     "the provider declares a different configuration",
			provider: revisionProvider("pii-v2"),
			policy:   basePolicy,
			why:      "the configuration this policy selects is gone",
		},
		{
			name:     "the permission mode widened to All",
			provider: baseProvider(),
			policy:   allowedConsumers{Mode: consumerModeAll},
			why:      "the provider-owned permission rule changed",
		},
		{
			// The provider's name and namespace are unchanged here, so this
			// fails unless the published endpoint is digested in its own
			// right. That is the case the field exists for: TrustyAI owns the
			// URL formula, and a future workload-namespace relocation would
			// move the server without moving anything else recorded above.
			name:     "the provider's published endpoint moved",
			provider: baseProvider(),
			endpoint: "https://" + nemoName + ".relocated.svc.cluster.local",
			policy:   basePolicy,
			why:      "the server answering checks is at a different address",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			endpoint := c.endpoint
			if endpoint == "" {
				endpoint = revisionEndpoint
			}
			got := mustComputeRevision(t, c.provider, endpoint, c.policy, nil)
			if got == base {
				t.Fatalf("revision is unchanged at %q, but %s; the dependency change is invisible to consumers", got, c.why)
			}
		})
	}
}

// TestComputeBindingRevisionTracksOnlySelectedLabels pins both halves of the
// label rule. Under-reacting is a correctness bug: a label edit can flip
// whether the binding is authorized at all. Over-reacting is expensive:
// namespaces carry labels no selector mentions, and churning the revision on
// those rolls every downstream Praxis generation for nothing.
func TestComputeBindingRevisionTracksOnlySelectedLabels(t *testing.T) {
	policy := selectorPolicy()
	matching := newNamespace(consumerNS, map[string]string{"guardrails": "shared"})
	base := mustComputeRevision(t, revisionProvider(guardrailConfig), revisionEndpoint, policy, matching)

	changes := []struct {
		name       string
		namespace  *corev1.Namespace
		wantChange bool
	}{
		{
			name:       "a selected label changes value",
			namespace:  newNamespace(consumerNS, map[string]string{"guardrails": "private"}),
			wantChange: true,
		},
		{
			name:       "a selected label is removed",
			namespace:  newNamespace(consumerNS, map[string]string{"unrelated": "x"}),
			wantChange: true,
		},
		{
			name:       "a selected label is emptied rather than removed",
			namespace:  newNamespace(consumerNS, map[string]string{"guardrails": ""}),
			wantChange: true,
		},
		{
			name:       "the Namespace became unreadable",
			namespace:  nil,
			wantChange: true,
		},
		{
			name:       "an unselected label is added",
			namespace:  newNamespace(consumerNS, map[string]string{"guardrails": "shared", "istio-injection": "enabled"}),
			wantChange: false,
		},
		{
			name:       "the Namespace was renamed but keeps the selected labels",
			namespace:  newNamespace("some-other-name", map[string]string{"guardrails": "shared"}),
			wantChange: false,
		},
	}
	for _, c := range changes {
		t.Run(c.name, func(t *testing.T) {
			got := mustComputeRevision(t, revisionProvider(guardrailConfig), revisionEndpoint, policy, c.namespace)
			if c.wantChange && got == base {
				t.Fatalf("revision is unchanged at %q, but a label the selector reads changed", got)
			}
			if !c.wantChange && got != base {
				t.Fatalf("revision changed to %q from %q on an edit the selector cannot see; "+
					"this rolls every downstream generation for nothing", got, base)
			}
		})
	}
}

// TestComputeBindingRevisionIgnoresLabelsOutsideSelectorMode guards the nil
// consumerNS contract: Reconcile reads the Namespace only in Selector mode,
// so no other mode may depend on labels it never looked at.
func TestComputeBindingRevisionIgnoresLabelsOutsideSelectorMode(t *testing.T) {
	for _, mode := range []consumerMode{consumerModeSame, consumerModeAll} {
		t.Run(string(mode), func(t *testing.T) {
			policy := allowedConsumers{Mode: mode}
			withoutLabels := mustComputeRevision(t, revisionProvider(guardrailConfig), revisionEndpoint, policy, nil)
			withLabels := mustComputeRevision(t, revisionProvider(guardrailConfig), revisionEndpoint, policy,
				newNamespace(consumerNS, map[string]string{"guardrails": "shared"}))
			if withoutLabels != withLabels {
				t.Fatalf("revision depends on Namespace labels in %s mode, which never reads them", mode)
			}
		})
	}
}

// TestSelectorLabelsCoversMatchExpressionKeys pins that matchExpressions keys
// are tracked too. Only matchLabels is exercised above, and a selector that
// read its keys exclusively from matchExpressions would otherwise contribute
// no labels at all — silently making the revision blind to the edits that
// decide authorization.
func TestSelectorLabelsCoversMatchExpressionKeys(t *testing.T) {
	selector := &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "tier", Operator: metav1.LabelSelectorOpExists},
		},
	}
	namespace := newNamespace(consumerNS, map[string]string{"tier": "gold", "unrelated": "x"})

	got := selectorLabels(selector, namespace)
	if len(got) != 1 {
		t.Fatalf("selectorLabels = %#v, want exactly the one key the selector references", got)
	}
	value, ok := got["tier"]
	if !ok {
		t.Fatalf("selectorLabels = %#v, want it to track the matchExpressions key %q", got, "tier")
	}
	if value == nil || *value != "gold" {
		t.Fatalf("selectorLabels[\"tier\"] = %v, want the Namespace's value %q", value, "gold")
	}
}

// TestSelectorLabelsDistinguishesAbsentFromEmpty pins the *string contract. A
// matchExpressions Exists requirement tells these two states apart, so a map
// that collapsed them would let an authorization-relevant edit through
// without changing the revision.
func TestSelectorLabelsDistinguishesAbsentFromEmpty(t *testing.T) {
	selector := &metav1.LabelSelector{MatchLabels: map[string]string{"guardrails": "shared"}}

	absent := selectorLabels(selector, newNamespace(consumerNS, nil))
	if value, ok := absent["guardrails"]; !ok || value != nil {
		t.Fatalf("absent label recorded as %v, want a tracked nil", value)
	}

	empty := selectorLabels(selector, newNamespace(consumerNS, map[string]string{"guardrails": ""}))
	if value, ok := empty["guardrails"]; !ok || value == nil || *value != "" {
		t.Fatalf("empty label recorded as %v, want a tracked empty string", value)
	}
}
