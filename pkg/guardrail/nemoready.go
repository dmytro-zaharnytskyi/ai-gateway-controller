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
	"net/url"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// nemoReadiness is what reading a NemoGuardrails provider's status answers:
// whether a checks endpoint was discovered, which one, and the reason and
// message to publish on the ProviderReady condition.
//
// This is NeMo-specific, like the file it lives in. ProviderReady itself is
// not — if a second guardrails provider is ever supported, it gets its own
// evaluator returning this same shape, and Reconcile keeps publishing one
// condition.
type nemoReadiness struct {
	Ready    bool
	Endpoint string
	Reason   string
	Message  string
}

// evaluateNemoReady resolves the referenced NeMo server's checks endpoint,
// returning it alongside the condition reason and message to publish.
//
// What this establishes is endpoint *discovery*, not liveness. TrustyAI
// publishes status.endpoint as the in-cluster Service URL
// (https://<name>.<namespace>.svc.cluster.local) and writes it from both its
// ready and its not-ready status paths, so a published endpoint proves the
// server is addressable and authenticated — not that its pods are up. That is
// precisely the gate the guardrails proposal specifies: "Missing endpoint
// discovery reports ProviderReady=False and prevents activation"
// (02-guardrails-low-level-details.md:1339). The success message says what was
// actually established, so ProviderReady=True is not mistaken for a probe.
//
// status.phase and the DeploymentReady condition are still deliberately NOT
// consulted; TrustyAI PR #977 added the endpoint without touching either.
// Upstream's controllers/utils/deployment.go CheckDeploymentReady passes a nil
// *appsv1.Deployment to client.Get so nothing is ever populated, returns an
// error from its retry closure on every path including success (discarding
// CheckPodsReady's verdict), and then ends in an unconditional
// "return true, nil" that never inspects the error — so it always reports
// ready. Its caller, controllers/nemo_guardrails/status.go, ANDs that with
// routeReady to set phase, meaning a NemoGuardrails with spec.exposeRoute
// unset or false reports phase Ready unconditionally, whatever its pods are
// doing. That is RHAI-4448 / RHOAIENG-96107, and it is why those two fields
// cannot gate activation even now that discovery exists.
//
// The OpenShift Route was evaluated as an endpoint source and rejected; the
// merged contract agrees, publishing the Service URL and leaving the Route out
// of it. The Route exists only when the optional spec.exposeRoute is true, it
// is an external ingress path with edge or reencrypt TLS while ExtProc calls
// the server in-cluster, and its condition is set to False with reason
// RouteDisabled when not exposed — which a naive True check would read as
// failure.
//
// An absent endpoint is reported uniformly, whatever the cause. The common one
// is authentication being switched off: upstream writes the field only when
// utils.RequiresAuth holds, so security.opendatahub.io/enable-auth: "false"
// clears it. Re-reading that annotation here to word a more specific message
// would put a second interpretation of an upstream predicate in this package,
// and the proposal asks for authenticated internal connectivity regardless
// (:1338) — an unauthenticated server is not one a binding should activate
// against.
//
// The value is parsed rather than trusted. status.endpoint is owned by another
// controller, and anything that is not a plain absolute https base URL — no
// userinfo, query or fragment — is a target ExtProc cannot call; rejecting it
// here fails closed instead of handing a malformed, plaintext or
// credential-bearing address downstream.
func evaluateNemoReady(nemo *unstructured.Unstructured) nemoReadiness {
	undiscovered := nemoReadiness{
		Reason: reasonEndpointDiscoveryUnavailable,
		Message: "the referenced NemoGuardrails provider publishes no status.endpoint, so no supported " +
			"checks endpoint could be discovered",
	}

	if nemo == nil {
		return undiscovered
	}

	discovered, found, err := unstructured.NestedString(nemo.Object, "status", "endpoint")
	if err != nil || !found || discovered == "" {
		return undiscovered
	}

	// Only the scheme and host are echoed back. The rejected value is an
	// arbitrary string from another controller's status, so quoting it whole
	// would copy a userinfo password into a condition message and the Warning
	// Event refuse emits.
	parsed, err := url.Parse(discovered)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		shown := "unparsable"
		if err == nil {
			shown = (&url.URL{Scheme: parsed.Scheme, Host: parsed.Host}).String()
		}
		return nemoReadiness{
			Reason: reasonEndpointDiscoveryUnavailable,
			Message: fmt.Sprintf(
				"the referenced NemoGuardrails provider publishes a status.endpoint that is not a plain absolute "+
					"https base URL (%s), so no supported checks endpoint could be discovered", shown),
		}
	}

	return nemoReadiness{
		Ready:    true,
		Endpoint: discovered,
		Reason:   reasonProviderAvailable,
		Message: "referenced NemoGuardrails provider publishes an authenticated in-cluster checks endpoint; " +
			"this confirms discovery, not that the server is serving",
	}
}
