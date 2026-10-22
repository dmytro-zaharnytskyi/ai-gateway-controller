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

package tenant

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// mtcFixture builds an unstructured MaasTenantConfig. payloadProcessingType
// set to "" omits AnnotationPayloadProcessingType entirely (matching a real
// MaasTenantConfig that never set it). aitenantName/aitenantNamespace set to
// "" omit the owning-AITenant annotations. tenantName set to "" omits the
// AITenant-managed labels entirely (matching an unlabeled/legacy config).
func mtcFixture(payloadProcessingType, aitenantName, aitenantNamespace, tenantName string) *unstructured.Unstructured {
	u := NewMaasTenantConfig()
	annotations := map[string]string{}
	if payloadProcessingType != "" {
		annotations[AnnotationPayloadProcessingType] = payloadProcessingType
	}
	if aitenantName != "" {
		annotations[AnnotationAITenantName] = aitenantName
	}
	if aitenantNamespace != "" {
		annotations[AnnotationAITenantNamespace] = aitenantNamespace
	}
	if len(annotations) > 0 {
		u.SetAnnotations(annotations)
	}
	if tenantName != "" {
		u.SetLabels(map[string]string{
			LabelManagedByAITenant: "true",
			LabelTenantName:        tenantName,
		})
	}
	return u
}

func TestNewMaasTenantConfigSetsGVK(t *testing.T) {
	u := NewMaasTenantConfig()
	if got := u.GroupVersionKind(); got != MaasTenantConfigGVK {
		t.Fatalf("GVK = %v, want %v", got, MaasTenantConfigGVK)
	}
}

func TestPayloadProcessingTypeAbsentIsEmpty(t *testing.T) {
	u := mtcFixture("", "", "", "")
	if got := PayloadProcessingType(u); got != "" {
		t.Fatalf("PayloadProcessingType = %q, want empty", got)
	}
}

func TestPayloadProcessingTypeReadsAnnotation(t *testing.T) {
	u := mtcFixture("praxis", "", "", "")
	if got := PayloadProcessingType(u); got != "praxis" {
		t.Fatalf("PayloadProcessingType = %q, want %q", got, "praxis")
	}
}

func TestUsesPraxis(t *testing.T) {
	cases := []struct {
		name string
		typ  string
		want bool
	}{
		{"absent defaults to praxis", "", true},
		{"praxis", "praxis", true},
		{"ipp opts into legacy", "ipp", false},
		{"unrecognized defaults to praxis", "unknown", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := UsesPraxis(mtcFixture(c.typ, "", "", "")); got != c.want {
				t.Errorf("UsesPraxis(type=%q) = %v, want %v", c.typ, got, c.want)
			}
		})
	}
}

func TestAPITranslationEnabled(t *testing.T) {
	cases := []struct {
		name    string
		value   any
		present bool
		want    bool
		wantErr bool
	}{
		{name: "absent defaults off"},
		{name: "empty settings defaults off", present: true, value: map[string]any{}},
		{name: "explicit false", present: true, value: map[string]any{"enabled": false}},
		{name: "explicit true", present: true, value: map[string]any{"enabled": true}, want: true},
		{name: "null enabled is malformed", present: true, value: map[string]any{"enabled": nil}, wantErr: true},
		{name: "string enabled is malformed", present: true, value: map[string]any{"enabled": "true"}, wantErr: true},
		{name: "non-object settings are malformed", present: true, value: "true", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mtc := mtcFixture("praxis", "", "", "")
			if tc.present {
				mtc.Object["spec"] = map[string]any{"apiTranslation": tc.value}
			}
			got, err := APITranslationEnabled(mtc)
			if (err != nil) != tc.wantErr {
				t.Fatalf("APITranslationEnabled() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("APITranslationEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestIdentifierFor(t *testing.T) {
	cases := []struct {
		name       string
		tenantName string
		want       string
	}{
		{"unlabeled config returns empty (legacy/default)", "", ""},
		{"default AITenant name returns empty", DefaultAITenantName, ""},
		{"non-default tenant returns its name", "redteam", "redteam"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := IdentifierFor(mtcFixture("praxis", "", "", c.tenantName)); got != c.want {
				t.Errorf("IdentifierFor(tenantName=%q) = %q, want %q", c.tenantName, got, c.want)
			}
		})
	}
}

func TestOwningAITenantRef(t *testing.T) {
	t.Run("absent", func(t *testing.T) {
		if _, _, ok := OwningAITenantRef(mtcFixture("praxis", "", "", "")); ok {
			t.Fatal("ok = true, want false when aitenant-name/namespace annotations are unset")
		}
	})

	t.Run("partially set", func(t *testing.T) {
		if _, _, ok := OwningAITenantRef(mtcFixture("praxis", "redteam", "", "")); ok {
			t.Fatal("ok = true, want false when only aitenant-name is set")
		}
	})

	t.Run("present", func(t *testing.T) {
		name, namespace, ok := OwningAITenantRef(mtcFixture("praxis", "redteam", "ai-tenants", ""))
		if !ok {
			t.Fatal("ok = false, want true")
		}
		if name != "redteam" || namespace != "ai-tenants" {
			t.Fatalf("OwningAITenantRef = (%q, %q), want (%q, %q)", name, namespace, "redteam", "ai-tenants")
		}
	})
}
