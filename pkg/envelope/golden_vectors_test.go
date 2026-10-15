package envelope

// M1 cross-language golden vectors (port plan R3): fixtures vendored from
// praxis-proxy/ai tests/fixtures/overlay-contract/v1 @ 1ef8a53e (see
// test/testdata/overlay-contract/v1/manifest.json for the manifest and the
// NOTICE entry for provenance). The manifest encodes what the Rust consumer
// (compute_semantic_digest + validation in overlay.rs) does with each
// document; these tests assert ComputeDigestFromWire reaches the same
// verdict — pinned digests for accepts, and the structural predicates
// behind each reject. This is the byte-compatibility gate the interface
// freeze depends on: no write-path PR merges unless it stays green.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/resolver"
)

const digest75 = "75b057d750d9db77030ecd5a073c235c56b2b0460d3d517340b3e44020e83056"

func fixturePath(parts ...string) string {
	return filepath.Join(append([]string{"..", "..", "test", "testdata", "overlay-contract", "v1"}, parts...)...)
}

type fixtureExpect struct {
	Expected      string `json:"expected"` // accept | accept_legacy | reject
	Reason        string `json:"reason"`
	Revision      string `json:"revision"`
	ContentDigest string `json:"content_digest"`
}

func loadManifest(t *testing.T) map[string]fixtureExpect {
	t.Helper()
	raw, err := os.ReadFile(fixturePath("manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Fixtures map[string]fixtureExpect `json:"fixtures"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Fixtures) == 0 {
		t.Fatal("empty manifest")
	}
	return m.Fixtures
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(fixturePath(name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func fixtureDoc(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("fixture is not a JSON object: %v", err)
	}
	return doc
}

func fixtureField(t *testing.T, raw []byte, path ...string) (map[string]json.RawMessage, bool) {
	t.Helper()
	doc := fixtureDoc(t, raw)
	for _, k := range path {
		var next map[string]json.RawMessage
		if err := json.Unmarshal(doc[k], &next); err != nil {
			return nil, false
		}
		doc = next
	}
	return doc, true
}

func digestString(t *testing.T, raw []byte, key string) string {
	t.Helper()
	obj, ok := fixtureField(t, raw, key)
	if !ok {
		t.Fatalf("%s not an object", key)
	}
	var s string
	if err := json.Unmarshal(obj["value"], &s); err != nil {
		t.Fatalf("%s.value: %v", key, err)
	}
	return s
}

func TestGoldenVectors_Manifest(t *testing.T) {
	fixtures := loadManifest(t)
	names := make([]string, 0, len(fixtures))
	for name := range fixtures {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		exp := fixtures[name]
		t.Run(name, func(t *testing.T) {
			raw := readFixture(t, name)
			switch exp.Expected {
			case "accept":
				// The headline byte-compat assertion: our raw-path digest
				// equals the revision the Rust consumer pins for this doc.
				got, err := ComputeDigestFromWire(raw)
				if err != nil {
					t.Fatalf("ComputeDigestFromWire: %v", err)
				}
				if got != exp.Revision {
					t.Errorf("digest byte-compat broken: got %s, want %s", got, exp.Revision)
				}
				if cd := digestString(t, raw, "content_digest"); cd != exp.Revision {
					t.Errorf("fixture inconsistent with manifest: content_digest %s vs revision %s", cd, exp.Revision)
				}

			case "accept_legacy":
				// Bare overlay payload (no envelope wrapper) must digest via
				// the same code path — shape detection mirrors praxis.
				if _, ok := fixtureDoc(t, raw)["schema_version"]; ok {
					t.Fatal("legacy fixture must not carry schema_version")
				}
				if _, err := ComputeDigestFromWire(raw); err != nil {
					t.Fatalf("legacy payload must digest: %v", err)
				}

			case "reject":
				switch exp.Reason {
				case "digest_mismatch":
					// Recomputation must find the declared digest false —
					// that is precisely the consumer's rejection reason.
					got, err := ComputeDigestFromWire(raw)
					if err != nil {
						t.Fatal(err)
					}
					if got != digest75 {
						t.Errorf("overlay is the multi-candidate one; recomputed %s, want %s", got, digest75)
					}
					if cd := digestString(t, raw, "content_digest"); cd == got {
						t.Error("fixture no longer demonstrates a digest mismatch")
					}
				case "revision_digest_mismatch":
					r := digestString(t, raw, "revision")
					c := digestString(t, raw, "content_digest")
					if r == c {
						t.Fatal("fixture no longer demonstrates revision/content_digest disagreement")
					}
					// The envelope's digest field is the honest one here;
					// the revision lies. CheckRevisionTransition is the
					// producer-side guard against ever shipping this.
					if got, err := ComputeDigestFromWire(raw); err != nil || got != c {
						t.Errorf("recomputed %s (err %v), want content_digest %s", got, err, c)
					}
				case "scope_mismatch":
					// Predicate that rejects scope.local_site !=
					// overlay.local_site (and network likewise). Render
					// derives overlay from Scope so agreement holds by
					// construction — this guards that invariant.
					scope, ok := fixtureField(t, raw, "scope")
					if !ok {
						t.Fatal("no scope object")
					}
					overlay, ok := fixtureField(t, raw, "overlay")
					if !ok {
						t.Fatal("no overlay object")
					}
					mismatched := false
					for _, k := range []string{"local_site", "network"} {
						var s, o string
						if json.Unmarshal(scope[k], &s) != nil || json.Unmarshal(overlay[k], &o) != nil {
							continue
						}
						if s != o {
							mismatched = true
						}
					}
					if !mismatched {
						t.Error("fixture no longer demonstrates a scope mismatch")
					}
				case "unsupported_schema_version":
					doc, ok := fixtureField(t, raw)
					if !ok {
						t.Fatal("not a JSON object")
					}
					var v string
					if err := json.Unmarshal(doc["schema_version"], &v); err != nil {
						t.Fatal(err)
					}
					if v == SchemaVersion {
						t.Errorf("fixture schema_version %q equals supported %q", v, SchemaVersion)
					}
				case "malformed_structure":
					// Typed parse — our own structs reject what the
					// consumer rejects (revision is a string, not object).
					var env Envelope
					if err := json.Unmarshal(raw, &env); err == nil {
						t.Error("typed unmarshal must fail on malformed structure")
					}
				case "missing_schema_version":
					// Hybrid: envelope-shaped but no schema_version. Praxis
					// rejects it outright; a naive legacy detector instead
					// digests the top-level object, finds none of the four
					// digest keys (they live under overlay), and returns a
					// VALID digest of the empty set — no error, wrong
					// answer. Pin that: shape detection must key on
					// schema_version, never on digest success.
					doc := fixtureDoc(t, raw)
					if _, ok := doc["schema_version"]; ok {
						t.Fatal("hybrid fixture must lack schema_version")
					}
					if _, ok := doc["overlay"]; !ok {
						t.Fatal("hybrid fixture must be envelope-shaped")
					}
					got, err := ComputeDigestFromWire(raw)
					if err != nil {
						t.Fatalf("misclassification must be silent (no error), got %v", err)
					}
					if got == digest75 {
						t.Error("legacy-path digest unexpectedly correct — fixture shape changed")
					}
				default:
					t.Fatalf("unhandled reject reason %q", exp.Reason)
				}
			default:
				t.Fatalf("unhandled expectation %q", exp.Expected)
			}
		})
	}
}

// TestGoldenVectors_AllFixturesManifested guards against silent rot: a
// fixture added upstream (or locally) without a manifest entry must fail
// here, not be quietly skipped by the manifest-driven loop.
func TestGoldenVectors_AllFixturesManifested(t *testing.T) {
	entries, err := os.ReadDir(fixturePath())
	if err != nil {
		t.Fatal(err)
	}
	fixtures := loadManifest(t)
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" || e.Name() == "manifest.json" {
			continue
		}
		if _, ok := fixtures[e.Name()]; !ok {
			t.Errorf("fixture %s has no manifest entry", e.Name())
		}
	}
}

// TestRender_DigestPathsAgree pins the producer-side invariant: for
// envelopes we render, the struct-path digest (what we stamp) and the
// raw-wire-path digest (what praxis recomputes from our bytes) must be
// identical — i.e. we never emit fields our own digest would drop.
func TestRender_DigestPathsAgree(t *testing.T) {
	set := routeSet(resolver.ModelRoutes{
		ModelRef: "ns1/m1",
		Routes:   []resolver.Route{route("m1", "p1", 1), route("m1", "p2", 1)},
	})
	env, err := Render(set, scope(), Revision{}, Options{
		KnownClusters: []string{"provider-p1", "provider-p2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ComputeDigestFromWire(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got != env.Revision.Value {
		t.Errorf("wire-path digest %s != stamped digest %s", got, env.Revision.Value)
	}
}
