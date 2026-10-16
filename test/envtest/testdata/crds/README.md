# Vendored test CRDs

These upstream CRDs are checked in so envtest can load them without downloads.
Gateway API and Istio match the kind environment; MaaS matches `test/maas-e2e.lock`.

| CRDs | Pinned source |
| --- | --- |
| Gateway, HTTPRoute | [Gateway API v1.5.1](https://raw.githubusercontent.com/kubernetes-sigs/gateway-api/v1.5.1/config/crd/standard/) (`gateway.networking.k8s.io_gateways.yaml`, `gateway.networking.k8s.io_httproutes.yaml`) |
| DestinationRule, EnvoyFilter, ServiceEntry | [Istio 1.27.3 CRD bundle](https://raw.githubusercontent.com/istio/istio/1.27.3/manifests/charts/base/files/crd-all.gen.yaml) |
| AITenant | [MaaS at 26e3116f](https://raw.githubusercontent.com/opendatahub-io/models-as-a-service/26e3116f6ddbe50a73fe9c31b61fbd6d3c817411/deployment/base/maas-controller/crd/bases/maas.opendatahub.io_aitenants.yaml) |
| MaasTenantConfig | [MaaS at 26e3116f](https://raw.githubusercontent.com/opendatahub-io/models-as-a-service/26e3116f6ddbe50a73fe9c31b61fbd6d3c817411/deployment/base/maas-controller/crd/bases/maas.opendatahub.io_maastenantconfigs.yaml) |

To refresh a dependency, download its CRDs at the desired tag or commit and
replace the matching YAML files here. For Istio, copy only the three complete
CRD documents listed above from the bundle, one per file. Preserve the upstream
schemas, update the source links, regenerate the checksums CI verifies with
`sha256sum *.yaml > SHA256SUMS`, and run `make test-envtest` from the repo root.
Commit the YAML changes together with the updated source links and checksums.
