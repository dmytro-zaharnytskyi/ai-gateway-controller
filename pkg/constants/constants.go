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

// Package constants holds values shared across reconciler packages so they
// stay in agreement (requeue cadences) instead of being redefined per package.
package constants

import "time"

// NotReadyRequeueInterval is the short "come back shortly" requeue wait a
// reconciler uses when a dependency is not yet ready or resolvable — distinct
// from a steady-state resync. Shared so all reconcilers agree on the cadence.
const NotReadyRequeueInterval = 10 * time.Second
