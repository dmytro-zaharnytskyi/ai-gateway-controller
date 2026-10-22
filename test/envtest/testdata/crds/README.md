# Vendored test CRDs

These upstream CRDs are checked in so envtest can load them without downloads.
Gateway API and Istio match the kind environment. The MaaS fixtures are based on
the pinned sources below, with the local addition noted after the table.

| CRDs | Pinned source |
| --- | --- |
| Gateway, HTTPRoute, ReferenceGrant | [Gateway API v1.5.1](https://raw.githubusercontent.com/kubernetes-sigs/gateway-api/v1.5.1/config/crd/standard/) (`gateway.networking.k8s.io_gateways.yaml`, `gateway.networking.k8s.io_httproutes.yaml`, `gateway.networking.k8s.io_referencegrants.yaml`) |
| DestinationRule, EnvoyFilter, ServiceEntry | [Istio 1.27.3 CRD bundle](https://raw.githubusercontent.com/istio/istio/1.27.3/manifests/charts/base/files/crd-all.gen.yaml) |
| AITenant | [MaaS at 26e3116f](https://raw.githubusercontent.com/opendatahub-io/models-as-a-service/26e3116f6ddbe50a73fe9c31b61fbd6d3c817411/deployment/base/maas-controller/crd/bases/maas.opendatahub.io_aitenants.yaml) |
| MaasTenantConfig | [MaaS at 26e3116f](https://raw.githubusercontent.com/opendatahub-io/models-as-a-service/26e3116f6ddbe50a73fe9c31b61fbd6d3c817411/deployment/base/maas-controller/crd/bases/maas.opendatahub.io_maastenantconfigs.yaml) |

The MaasTenantConfig fixture also includes `spec.apiTranslation.enabled` from
the in-flight MaaS translation change. The pinned MaaS source above predates
that field; this local addition lets envtest verify that the API server retains
the tenant setting until the MaaS pin is updated.

To refresh a dependency, download its CRDs at the desired tag or commit and
replace the matching YAML files here. For Istio, copy only the three complete
CRD documents listed above from the bundle, one per file. Preserve the upstream
schemas, update the source links, regenerate the checksums CI verifies with
`sha256sum *.yaml > SHA256SUMS`, and run `make test-envtest` from the repo root.
Commit the YAML changes together with the updated source links and checksums.
