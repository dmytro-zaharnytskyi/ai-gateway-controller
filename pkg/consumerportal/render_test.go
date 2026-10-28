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

package consumerportal

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/render"
	"github.com/opendatahub-io/ai-gateway-controller/pkg/tenant"
)

func renderTestResources(t *testing.T) map[string]unstructured.Unstructured {
	t.Helper()
	built, err := render.Build(testManifestPath)
	if err != nil {
		t.Fatalf("render.Build: %v", err)
	}
	resources := Render(built, Params{
		Namespace:          testInfraNamespace,
		Image:              "quay.io/example/kube-auth-proxy:test",
		OIDCIssuerURL:      testIssuer,
		Hostname:           testHostname,
		GatewayName:        testGatewayName,
		GatewayNamespace:   testGatewayNamespace,
		AuthorinoNamespace: DefaultAuthorinoNamespace,
	})
	byKind := map[string]unstructured.Unstructured{}
	for _, u := range resources {
		byKind[u.GetKind()] = u
	}
	return byKind
}

func TestRenderSubstitutesTenantValues(t *testing.T) {
	resources := renderTestResources(t)
	for _, kind := range []string{"Deployment", "Service", "ServiceAccount", "NetworkPolicy"} {
		u, ok := resources[kind]
		if !ok {
			t.Fatalf("rendered resources missing %s", kind)
		}
		if u.GetNamespace() != testInfraNamespace || u.GetName() != AuthProxyName {
			t.Errorf("%s = %s/%s, want %s/%s", kind, u.GetNamespace(), u.GetName(), testInfraNamespace, AuthProxyName)
		}
		if u.GetLabels()[tenant.LabelManagedBy] != tenant.ManagedByAIGatewayController {
			t.Errorf("%s missing managed-by label: %v", kind, u.GetLabels())
		}
		raw, err := json.Marshal(u.Object)
		if err != nil {
			t.Fatalf("marshal %s: %v", kind, err)
		}
		if strings.Contains(string(raw), "CONSUMER_PORTAL_") {
			t.Errorf("%s still contains an unreplaced token: %s", kind, raw)
		}
	}

	deployment := resources["Deployment"]
	containers, _, _ := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if len(containers) != 1 {
		t.Fatalf("Deployment containers = %d, want 1", len(containers))
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		t.Fatalf("container is not an object: %#v", containers[0])
	}
	if container["image"] != "quay.io/example/kube-auth-proxy:test" {
		t.Errorf("image = %v", container["image"])
	}
	args, _, _ := unstructured.NestedStringSlice(container, "args")
	for _, want := range []string{
		"--provider=oidc",
		"--oidc-issuer-url=" + testIssuer,
		"--redirect-url=https://" + testHostname + "/oauth2/callback",
		"--cookie-domain=" + testHostname,
		"--https-address=0.0.0.0:4180",
	} {
		if !slices.Contains(args, want) {
			t.Errorf("auth proxy args missing %q: %v", want, args)
		}
	}

	policy := resources["NetworkPolicy"]
	raw, err := json.Marshal(policy.Object)
	if err != nil {
		t.Fatalf("marshal NetworkPolicy: %v", err)
	}
	for _, want := range []string{testGatewayNamespace, testGatewayName, DefaultAuthorinoNamespace} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("NetworkPolicy does not reference %q: %s", want, raw)
		}
	}
}

func TestListenerHostname(t *testing.T) {
	cases := map[string]struct {
		listeners []map[string]any
		want      string
	}{
		"https listener": {listeners: []map[string]any{httpsListener(testHostname)}, want: testHostname},
		"skips wildcard and http": {listeners: []map[string]any{
			httpsListener("*.apps.example.com"),
			{"name": "http", "protocol": "HTTP", "hostname": "plain.example.com"},
			httpsListener(testHostname),
		}, want: testHostname},
		"no hostname": {listeners: []map[string]any{{"name": "https", "protocol": "HTTPS"}}, want: ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := ListenerHostname(newGateway(tc.listeners...)); got != tc.want {
				t.Errorf("ListenerHostname = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestPortalEnabledDefaultsTrue(t *testing.T) {
	if !PortalEnabled(newAITenant(aitenantOpts{})) {
		t.Error("consumer portal should default to enabled")
	}
	if PortalEnabled(newAITenant(aitenantOpts{portalDisabled: true})) {
		t.Error("consumerPortal.enabled=false was ignored")
	}
}

func TestRenderAddsProviderCAFilesPerKey(t *testing.T) {
	built, err := render.Build(testManifestPath)
	if err != nil {
		t.Fatalf("render.Build: %v", err)
	}
	argsFor := func(keys []string) []string {
		for _, u := range Render(built, Params{Namespace: testInfraNamespace, ProviderCAKeys: keys}) {
			if u.GetKind() != "Deployment" {
				continue
			}
			containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
			container, ok := containers[0].(map[string]any)
			if !ok {
				t.Fatalf("container is not an object: %#v", containers[0])
			}
			args, _, _ := unstructured.NestedStringSlice(container, "args")
			return args
		}
		t.Fatal("no Deployment rendered")
		return nil
	}

	if args := argsFor([]string{"ca-bundle.crt"}); !slices.Contains(args, "--provider-ca-file=/etc/pki/odh-trusted-ca/ca-bundle.crt") ||
		slices.Contains(args, "--provider-ca-file=/etc/pki/odh-trusted-ca/odh-ca-bundle.crt") {
		t.Errorf("args for [ca-bundle.crt] = %v", args)
	}
	for _, arg := range argsFor(nil) {
		if strings.HasPrefix(arg, "--provider-ca-file=") {
			t.Errorf("unexpected %q without CA keys", arg)
		}
	}
}

func TestRenderSetsCABundleHashAnnotation(t *testing.T) {
	built, err := render.Build(testManifestPath)
	if err != nil {
		t.Fatalf("render.Build: %v", err)
	}
	params := Params{
		Namespace:    testInfraNamespace,
		CABundleHash: "ca-bundle-hash",
	}
	deployment := unstructured.Unstructured{}
	for _, u := range Render(built, params) {
		if u.GetKind() == "Deployment" {
			deployment = u
			break
		}
	}
	hash, _, _ := unstructured.NestedString(deployment.Object,
		"spec", "template", "metadata", "annotations", CABundleHashAnnotation)
	if hash != "ca-bundle-hash" {
		t.Errorf("CA bundle hash annotation = %q, want ca-bundle-hash", hash)
	}

	params.CABundleHash = ""
	for _, u := range Render(built, params) {
		if u.GetKind() != "Deployment" {
			continue
		}
		hash, found, _ := unstructured.NestedString(u.Object,
			"spec", "template", "metadata", "annotations", CABundleHashAnnotation)
		if found || hash != "" {
			t.Errorf("empty CA bundle hash rendered annotation %q", hash)
		}
	}
}
