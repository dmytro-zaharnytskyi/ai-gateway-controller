package envelope

import (
	"strings"
	"testing"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

func TestScopedStableIDFormsAreDisjointFromExistingForm(t *testing.T) {
	existing := CandidateStableID("model", "openai")
	scoped := ScopedStableID("team", "model", "team", "openai")
	if scoped != "team/model/team/openai" {
		t.Fatalf("scoped stable_id = %q", scoped)
	}
	long := strings.Repeat("m", 253)
	hashed := ScopedStableID(strings.Repeat("n", 63), long, strings.Repeat("p", 63), long)
	if !strings.HasPrefix(hashed, "sha256:") || len(hashed) != len("sha256:")+64 || len(hashed) > maxStableIDBytes {
		t.Fatalf("over-limit stable_id = %q", hashed)
	}
	if strings.Count(existing, "/") != 1 || strings.Count(scoped, "/") != 3 || strings.Contains(hashed, "/") {
		t.Fatalf("forms overlap: existing=%q scoped=%q hashed=%q", existing, scoped, hashed)
	}
	// Length prefixes keep field boundaries in the hash input.
	if ScopedStableID("a", long+"b", "c", long) == ScopedStableID("a", long, "bc", long) {
		t.Fatal("hash form must not depend on field concatenation only")
	}
}

func TestRenderUsesRouteStableIDOverride(t *testing.T) {
	scoped := route("model", "openai", 1)
	scoped.StableID = ScopedStableID("team", "model", "team", "openai")
	existing := route("other", "openai", 1)
	env, err := Render(routeSet(
		resolver.ModelRoutes{ModelRef: "team/model", Routes: []resolver.Route{scoped}},
		resolver.ModelRoutes{ModelRef: "team/other", Routes: []resolver.Route{existing}},
	), scope(), Revision{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	candidates := env.Overlay.Candidates
	if len(candidates) != 2 || candidates[0].StableID != scoped.StableID || candidates[1].StableID != CandidateStableID("other", "openai") {
		t.Fatalf("candidates = %#v, want the scoped override and the existing form", candidates)
	}
}

func TestValidateModelMatchesRenderRejections(t *testing.T) {
	unqualified := route("model", "openai", 1)
	unqualified.APIFormat = "messages"
	for name, m := range map[string]resolver.ModelRoutes{
		"weights":    {ModelRef: "team/model", Routes: []resolver.Route{route("model", "a", 1), route("model", "b", 2)}},
		"credential": {ModelRef: "team/model", Routes: []resolver.Route{unqualified}},
		"duplicate":  {ModelRef: "team/model", Routes: []resolver.Route{route("model", "a", 1), route("model", "a", 1)}},
	} {
		if err := ValidateModel(m, Options{}); err == nil {
			t.Fatalf("%s: ValidateModel accepted a model Render rejects", name)
		}
		if _, err := Render(routeSet(m), scope(), Revision{}, Options{}); err == nil {
			t.Fatalf("%s: Render accepted the model", name)
		}
	}
	if err := ValidateModel(resolver.ModelRoutes{Routes: []resolver.Route{route("model", "a", 1)}}, Options{KnownClusters: []string{"provider-b"}}); err == nil {
		t.Fatal("ValidateModel accepted an unknown cluster")
	}
}
