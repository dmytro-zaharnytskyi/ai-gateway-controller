# Control-plane walking skeleton

Run `make test-envtest` from the repository root. It fetches the pinned
`setup-envtest` tool and Kubernetes 1.35.0 binaries, reusing local downloads.
The six dependency CRDs are vendored in `testdata/crds/`; tests never download
them. No cluster or container runtime is required.

Pinned sources and manual refresh instructions are documented beside the
[vendored CRDs](testdata/crds/README.md). This repo's own CRDs remain under
`config/crd/bases` and are tested directly. Vendored YAML is excluded from the
production-line cap and marked as vendored in GitHub diffs.

`make test` includes this suite. CI runs it separately from `make test-unit`.
The `envtest` build tag participates in linting. Missing assets are errors,
never skips. A supplied `KUBEBUILDER_ASSETS` overrides the binary directory.

For a focused run:

```sh
make test-envtest ENVTEST_FLAGS='-race -count=1 -timeout=3m -v -run TestControlPlane/adding_an_external_model'
```

The suite starts both controllers through their production `SetupWithManager`
methods, with real watches, cache and workqueues. `suite_test.go` contains the
controller wiring; `environment_test.go` owns startup, permissions and cleanup.
Their identity uses the ServiceAccount, ClusterRole and binding rendered from
`config/self/default`,
so reconciliation exercises the shipped RBAC. Like OpenShift, the API server
enforces owner-reference permissions. The manager stops before the
API server, including on failure. The current kubeconfig is never used.

## Contracts covered

- Given an existing Praxis tenant and a configured provider, creating an
  ExternalModel publishes the requested model/provider mapping and an HTTPRoute
  attached to the tenant's gateway, reports the current generation Ready, and
  configures its ExtProc workload. Deleting that model removes its route,
  overlay and workload, then releases its finalizer.
  Both paths require the shipped permission to patch model finalizers.
- The shipped controller identity may create, patch and delete Secrets in a
  tenant namespace for its credential copies, and read ReferenceGrants in a
  provider namespace and across all namespaces. It can read Gateways in the
  gateway namespace but cannot read Namespaces. Each permission is checked with a
  SubjectAccessReview that must evaluate without error, so it is verified
  before any controller uses it.
- Guardrail admission accepts positive Go durations and rejects invalid or
  non-positive values. Accepted values remain readable by typed Get/List.
  A typed fixture is converted to unstructured input and only its timeout is
  replaced with a raw string, so validation happens on the server.

Tests use Go's `testing` package and testify. The scenario states its
preconditions, user action and expected outcome. Small typed builders describe
user resources; assertion helpers read observable outputs. They do not call
`Reconcile` or use production rendering to calculate expected results.

`maas_test.go` supplies external tenant state: namespace, active AITenant
status and a released IPP handoff. MaaS itself does not run here. The existing
Praxis tenant explicitly includes the legacy AITenant selector still required
by the model controller. This precondition does **not** prove MTC-only opt-in.
That selection mismatch must be fixed before adding an opt-in specification.

This first slice covers API-server admission and permission gaps. Model
updates/deletion, tenant deletion, readiness changes, and backend migration
are follow-up scenarios. They should use the same user resource operations
and explicit MaaS events, with one behavioral promise per scenario.

Workload controllers also do not run in envtest: deployment configuration is
asserted, not running pods or serving traffic. The existing integration
environment remains responsible for the full MaaS and data-plane workflow.
