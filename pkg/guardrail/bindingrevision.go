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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/gowebpki/jcs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// bindingInputs is the digest preimage: the dependency state an accepted
// binding was computed from.
//
// Every field here can change while this policy's metadata.generation stays
// constant, which is the whole reason the revision exists — generation already
// covers the policy's own spec, so anything derivable from it would only add
// noise. The provider's namespace and name are the exception, carried so the
// preimage identifies its own binding rather than being a bare hash.
//
// The JSON names are part of the digest. Renaming one changes every published
// revision and invalidates every downstream generation, so treat this struct
// as a wire contract.
type bindingInputs struct {
	// ProviderUID distinguishes a provider from a different object that later
	// took its name. Nothing else here would notice that substitution: a
	// recreated provider can carry byte-identical spec and an unchanged
	// reference, while being a new object an admin may have built under
	// different review.
	ProviderUID       string `json:"providerUID"`
	ProviderNamespace string `json:"providerNamespace"`
	ProviderName      string `json:"providerName"`

	// ProviderEndpoint is the discovered checks endpoint the binding was
	// accepted against, recorded as the provider published it.
	//
	// Today it is derivable from the two fields above, because TrustyAI
	// formats it as https://<name>.<namespace>.svc.cluster.local — so this
	// adds no discrimination yet. It is carried because that formula is
	// upstream's to change and the proposal requires discovered endpoints to
	// be part of the binding contract rather than re-derived from naming
	// (02-guardrails-low-level-details.md:78). A server relocated by a future
	// workload-namespace field would then invalidate its bindings instead of
	// silently keeping them against an address that moved.
	ProviderEndpoint string `json:"providerEndpoint"`

	// ConsumerMode and Selector are the provider-owned permission rule.
	ConsumerMode consumerMode          `json:"consumerMode"`
	Selector     *metav1.LabelSelector `json:"selector,omitempty"`

	// ConsumerLabels are the consumer Namespace labels the selector reads.
	ConsumerLabels map[string]*string `json:"consumerLabels,omitempty"`

	// DeclaredConfigs is every configuration name the provider declares,
	// sorted. The full set is recorded, not just the ones this policy's checks
	// select: a provider whose configuration list was edited is a changed
	// dependency, and over-invalidating is the safe direction.
	DeclaredConfigs []string `json:"declaredConfigs"`
}

// selectorLabels records the consumer Namespace labels the selector reads.
//
// Only the referenced keys are recorded, never the whole label map.
// Namespaces carry labels no selector mentions — istio-injection,
// pod-security, cost centre — and folding those in would churn the revision,
// and every downstream Praxis generation with it, on edits that cannot change
// whether this binding is authorized.
//
// A nil value means the label is absent, which is distinct from one set to
// the empty string: a matchExpressions Exists requirement tells those apart,
// so the digest has to as well.
func selectorLabels(selector *metav1.LabelSelector, consumerNS *corev1.Namespace) map[string]*string {
	if selector == nil {
		return nil
	}

	keys := make(map[string]bool, len(selector.MatchLabels)+len(selector.MatchExpressions))
	for key := range selector.MatchLabels {
		keys[key] = true
	}
	for _, requirement := range selector.MatchExpressions {
		keys[requirement.Key] = true
	}
	if len(keys) == 0 {
		return nil
	}

	var namespaceLabels map[string]string
	if consumerNS != nil {
		namespaceLabels = consumerNS.Labels
	}

	recorded := make(map[string]*string, len(keys))
	for key := range keys {
		if value, ok := namespaceLabels[key]; ok {
			recorded[key] = &value
			continue
		}
		recorded[key] = nil
	}
	return recorded
}

// computeBindingRevision digests the dependency state behind an accepted
// binding, as lowercase hex SHA-256 over the RFC 8785 canonicalization of
// bindingInputs — the same construction pkg/envelope uses for content
// addressing.
//
// configs are the provider's declared configuration names, already parsed by
// evaluateCompatible. endpoint is the provider's discovered checks endpoint,
// as returned by evaluateNemoReady. consumerNS is the consumer namespace's
// Namespace object, or nil outside Selector mode, where no labels are read.
//
// This does not detect every remote change, and must not be documented as if
// it did. The content of a ConfigMap behind a declared configuration name is
// not observable from either CR, so editing one changes effective safety
// while this digest, and every generation in sight, stays constant. Version
// configuration resources immutably where that distinction matters.
func computeBindingRevision(
	nemo *unstructured.Unstructured,
	configs map[string]bool,
	endpoint string,
	policy allowedConsumers,
	consumerNS *corev1.Namespace,
) (string, error) {
	declared := make([]string, 0, len(configs))
	for name := range configs {
		declared = append(declared, name)
	}
	sort.Strings(declared)

	payload, err := json.Marshal(bindingInputs{
		ProviderUID:       string(nemo.GetUID()),
		ProviderNamespace: nemo.GetNamespace(),
		ProviderName:      nemo.GetName(),
		ProviderEndpoint:  endpoint,
		ConsumerMode:      policy.Mode,
		Selector:          policy.Selector,
		ConsumerLabels:    selectorLabels(policy.Selector, consumerNS),
		DeclaredConfigs:   declared,
	})
	if err != nil {
		return "", fmt.Errorf("marshalling binding inputs: %w", err)
	}
	canonical, err := jcs.Transform(payload)
	if err != nil {
		return "", fmt.Errorf("canonicalizing binding inputs: %w", err)
	}

	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}
