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
)

const checkHeaderPrefix = "x-aigateway-guardrail-"

// CheckID returns the stable identity for an AIGuardrail check.
func CheckID(namespace, guardrailName, checkName string) string {
	digest := sha256.Sum256([]byte(namespace + ":" + guardrailName + ":" + checkName))
	return hex.EncodeToString(digest[:])
}

// CheckHeaderName returns the request header name for an AIGuardrail check.
func CheckHeaderName(namespace, guardrailName, checkName string) string {
	return checkHeaderPrefix + CheckID(namespace, guardrailName, checkName)
}
