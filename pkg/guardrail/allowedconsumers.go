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
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

// allowedConsumers stores a copy of the NemoGuardrails.spec.allowedConsumers
// field which defines the types of namespaces that are allowed to attach an
// AIGuardrail consumer to a NeMo Guardrails server.
type allowedConsumers struct {
	Mode     consumerMode
	Selector *metav1.LabelSelector
}

// parseAllowedConsumers reads NemoGuardrails.spec.allowedConsumers.namespaces.from.
// If spec.allowedConsumers is omitted, from defaults to Same, meaning only
// AIGuardrails in the same namespace as the NeMo Guardrails server can attach
// to it.
//
// Validation is deliberately strict and fail-closed: a selector is rejected
// under Same and All including when from defaults to Same, a missing or empty
// selector under Selector is rejected rather than read as allow-all, and an
// unknown from value is an error. Callers surface any error as an unauthorized
// binding.
func parseAllowedConsumers(nemo *unstructured.Unstructured) (allowedConsumers, error) {
	namespaces, found, err := unstructured.NestedMap(nemo.Object, "spec", "allowedConsumers", "namespaces")
	if err != nil {
		return allowedConsumers{}, fmt.Errorf("spec.allowedConsumers.namespaces is malformed: %w", err)
	}
	if !found {
		return allowedConsumers{Mode: consumerModeSame}, nil
	}

	from, _, err := unstructured.NestedString(namespaces, "from")
	if err != nil {
		return allowedConsumers{}, fmt.Errorf("spec.allowedConsumers.namespaces.from is malformed: %w", err)
	}
	if from == "" {
		from = string(consumerModeSame)
	}

	selectorRaw, hasSelector, err := unstructured.NestedMap(namespaces, "selector")
	if err != nil {
		return allowedConsumers{}, fmt.Errorf("spec.allowedConsumers.namespaces.selector is malformed: %w", err)
	}

	switch mode := consumerMode(from); mode {
	case consumerModeSame, consumerModeAll:
		if hasSelector {
			return allowedConsumers{}, fmt.Errorf("spec.allowedConsumers.namespaces.selector is forbidden with from %q", from)
		}
		return allowedConsumers{Mode: mode}, nil

	case consumerModeSelector:
		if !hasSelector {
			// Denied, never defaulted to All: an absent selector is
			// indistinguishable from a dropped or mistyped one, and Kubernetes'
			// own empty-LabelSelector semantics would read it as match-all —
			// exactly the silent widening this field exists to prevent.
			// Granting cross-namespace access stays an explicit All.
			return allowedConsumers{}, fmt.Errorf("spec.allowedConsumers.namespaces.from %q requires a selector", from)
		}
		var selector metav1.LabelSelector
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(selectorRaw, &selector); err != nil {
			return allowedConsumers{}, fmt.Errorf("spec.allowedConsumers.namespaces.selector is malformed: %w", err)
		}
		if len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0 {
			return allowedConsumers{}, errors.New("spec.allowedConsumers.namespaces.selector requires at least one matchLabels entry or matchExpressions requirement")
		}
		return allowedConsumers{Mode: consumerModeSelector, Selector: &selector}, nil

	default:
		return allowedConsumers{}, fmt.Errorf("unknown spec.allowedConsumers.namespaces.from value %q", from)
	}
}

// allows reports whether an AIGuardrail in consumerNamespace may reference a
// NemoGuardrails in providerNamespace.
//
// consumerNS must be the consumer namespace's Namespace object, and nil when
// it is missing or could not be read: selectors match the labels on that
// Namespace object — never labels on the AIGuardrail — and a missing or
// unreadable Namespace never matches.
func (a allowedConsumers) allows(consumerNamespace, providerNamespace string, consumerNS *corev1.Namespace) (bool, error) {
	switch a.Mode {
	case consumerModeAll:
		return true, nil

	case consumerModeSame:
		return consumerNamespace == providerNamespace, nil

	case consumerModeSelector:
		if consumerNS == nil {
			return false, nil
		}
		selector, err := metav1.LabelSelectorAsSelector(a.Selector)
		if err != nil {
			return false, fmt.Errorf("spec.allowedConsumers.namespaces.selector is invalid: %w", err)
		}
		return selector.Matches(labels.Set(consumerNS.Labels)), nil

	default:
		return false, fmt.Errorf("unknown allowed-consumers mode %q", a.Mode)
	}
}
