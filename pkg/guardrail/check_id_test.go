/*
Copyright 2026 The opendatahub.io Authors.

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
	"strings"
	"testing"
)

func TestCheckID(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		guardrailName string
		checkName     string
		want          string
	}{
		{
			name:          "base identity",
			namespace:     "tenant-ns",
			guardrailName: "safety",
			checkName:     "toxicity",
			want:          "e4640883205f190be3fe78a451c15dd46406d662242bf2fa7cda1830961b5922",
		},
		{
			name:          "different namespace",
			namespace:     "tenant-alt",
			guardrailName: "safety",
			checkName:     "toxicity",
			want:          "478ee9fd7845e674c16227596a9c498cc9aafd4a4e0432848f8408241e853303",
		},
		{
			name:          "different guardrail name",
			namespace:     "tenant-ns",
			guardrailName: "content-safety",
			checkName:     "toxicity",
			want:          "f1caf8fa1415bfee8c75b9474471e3916ef05ace96e368d9c8c79a3ea0f8954b",
		},
		{
			name:          "different check name",
			namespace:     "tenant-ns",
			guardrailName: "safety",
			checkName:     "pii",
			want:          "caade2fe1d0fb62b4ad99c92893b3126a5af7151ba160b393f1471305b16da74",
		},
		{
			name:          "special characters",
			namespace:     "tenant\"ns",
			guardrailName: "guard\\rail",
			checkName:     "check\n<>&",
			want:          "78e85b4229b9389aaa1512026b80c84ac8fa21b9b8d86d8906b22af3ba44b57d",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			first := CheckID(tt.namespace, tt.guardrailName, tt.checkName)
			if first != tt.want {
				t.Fatalf("CheckID() = %q, want %q", first, tt.want)
			}

			if second := CheckID(tt.namespace, tt.guardrailName, tt.checkName); second != first {
				t.Errorf("repeated CheckID() = %q, want %q", second, first)
			}
		})
	}
}

func TestCheckHeaderName(t *testing.T) {
	const wantID = "e4640883205f190be3fe78a451c15dd46406d662242bf2fa7cda1830961b5922"

	header := CheckHeaderName("tenant-ns", "safety", "toxicity")
	if want := "x-aigateway-guardrail-" + wantID; header != want {
		t.Fatalf("CheckHeaderName() = %q, want %q", header, want)
	}

	id := strings.TrimPrefix(header, checkHeaderPrefix)
	if len(id) != sha256.Size*2 {
		t.Errorf("CheckID length = %d, want %d", len(id), sha256.Size*2)
	}
	if _, err := hex.DecodeString(id); err != nil {
		t.Errorf("CheckID %q is not hexadecimal: %v", id, err)
	}
	if id != strings.ToLower(id) {
		t.Errorf("CheckID %q is not lowercase", id)
	}
}
