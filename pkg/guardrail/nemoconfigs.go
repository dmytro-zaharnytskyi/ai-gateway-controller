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
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	aigatewayv1alpha1 "github.com/opendatahub-io/ai-gateway-controller/api/aigateway/v1alpha1"
)

// parseNemoConfigs reads the configuration names a provider declares in
// spec.nemoConfigs[].name. These are the IDs a check selects through
// spec.checks[].configId, and the strings Praxis later sends as
// guardrails.config_ids.
//
// An absent spec.nemoConfigs yields an empty set rather than an error. The
// caller reports that as a provider declaring no configurations, which is a
// more useful verdict than a parse failure.
func parseNemoConfigs(nemo *unstructured.Unstructured) (map[string]bool, error) {
	entries, found, err := unstructured.NestedSlice(nemo.Object, "spec", "nemoConfigs")
	if err != nil {
		return nil, fmt.Errorf("spec.nemoConfigs is malformed: %w", err)
	}
	if !found {
		return nil, nil
	}

	configs := make(map[string]bool, len(entries))
	for i, entry := range entries {
		config, ok := entry.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("spec.nemoConfigs[%d] is not an object", i)
		}
		name, _, err := unstructured.NestedString(config, "name")
		if err != nil {
			return nil, fmt.Errorf("spec.nemoConfigs[%d].name is malformed: %w", i, err)
		}
		configs[name] = true
	}
	return configs, nil
}

// compatibility is what comparing an AIGuardrail's checks against its
// provider's declared configurations answers: the Compatible condition's
// status, reason and message, plus the configurations themselves so the
// accepted path does not parse the same provider twice.
type compatibility struct {
	Compatible bool
	Reason     string
	Message    string
	// Configs are the names the provider declares, set whenever the parse
	// succeeded. computeBindingRevision digests them.
	Configs map[string]bool
	// Err is the parse failure behind reasonInvalidProviderConfigs, for the
	// caller to log. It must not reach Message: apimachinery accessor errors
	// embed the value they rejected, which is the provider's own spec.
	Err error
}

// evaluateCompatible reports whether every check selects a configuration the
// provider declares, as the Compatible condition status, reason and message.
//
// Compatible=True means the configuration is selectable, not that the running
// server loaded it. Load success is not observable from the CR and needs the
// same endpoint-discovery contract as evaluateNemoReady.
//
// The verdict does not depend on the provider being ready. It compares two
// specs, both readable while the server is down, so a configId typo is
// reported as one instead of being masked by an unready provider.
func evaluateCompatible(guardrail *aigatewayv1alpha1.AIGuardrail, nemo *unstructured.Unstructured) compatibility {
	configs, err := parseNemoConfigs(nemo)
	if err != nil {
		return compatibility{
			Reason:  reasonInvalidProviderConfigs,
			Message: "referenced NemoGuardrails provider has malformed configurations in spec.nemoConfigs",
			Err:     err,
		}
	}
	if len(configs) == 0 {
		return compatibility{
			Reason:  reasonProviderConfigsUnavailable,
			Message: "referenced NemoGuardrails provider declares no configurations in spec.nemoConfigs",
		}
	}

	var unknown []string
	reported := make(map[string]bool)
	for _, check := range guardrail.Spec.Checks {
		if configs[check.ConfigID] || reported[check.ConfigID] {
			continue
		}
		reported[check.ConfigID] = true
		unknown = append(unknown, check.ConfigID)
	}

	if len(unknown) == 0 {
		return compatibility{
			Compatible: true,
			Reason:     reasonChecksSatisfiable,
			Message:    "every check selects a configuration declared by the provider",
			Configs:    configs,
		}
	}

	// Sorted so the message is stable across reconciles. An unstable message
	// rewrites the condition, bumping LastTransitionTime and re-emitting an
	// event on every pass through the requeue loop.
	sort.Strings(unknown)
	return compatibility{
		Reason:  reasonUnknownCheckConfig,
		Message: fmt.Sprintf("referenced NemoGuardrails provider does not declare configId(s): %s", strings.Join(unknown, ", ")),
		Configs: configs,
	}
}
