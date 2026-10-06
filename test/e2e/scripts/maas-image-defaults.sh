#!/usr/bin/env bash
# Default container images for ai-gateway-controller e2e (no local maas-api/maas-controller trees).
#
# MaaS main-branch Konflux pushes publish :latest and a tag for each commit SHA (see
# models-as-a-service .tekton/odh-maas-api-push.yaml and odh-maas-controller-push.yaml).
# Override with MAAS_API_IMAGE / MAAS_CONTROLLER_IMAGE when testing specific builds.

set -euo pipefail

# Deploy images from the MaaS commit pinned in test/maas-e2e.lock, so local runs match
# the fetched tests. Update this tag whenever the pin changes.
MAAS_IMAGE_TAG="${MAAS_IMAGE_TAG:-090cc5d1a52a82f75b81a0a104f0d244c9a0ab9f}"

export MAAS_API_IMAGE="${MAAS_API_IMAGE:-quay.io/opendatahub/maas-api:${MAAS_IMAGE_TAG}}"
export MAAS_CONTROLLER_IMAGE="${MAAS_CONTROLLER_IMAGE:-quay.io/opendatahub/maas-controller:${MAAS_IMAGE_TAG}}"

# ai-gateway-controller is always supplied by the caller (Konflux snapshot or local override).
if [[ -z "${AI_GATEWAY_CONTROLLER_IMAGE:-}" ]]; then
  echo "ERROR: AI_GATEWAY_CONTROLLER_IMAGE must be set (PR under test)" >&2
  exit 1
fi

# Konflux group-test may export PRAXIS_EXTPROC_IMAGE from odh-praxis-extproc-ci snapshot.
export PRAXIS_EXTPROC_IMAGE="${PRAXIS_EXTPROC_IMAGE:-quay.io/opendatahub/odh-praxis-extproc:odh-stable}"

echo "MaaS platform images:"
echo "  MAAS_API_IMAGE=${MAAS_API_IMAGE}"
echo "  MAAS_CONTROLLER_IMAGE=${MAAS_CONTROLLER_IMAGE}"
echo "  AI_GATEWAY_CONTROLLER_IMAGE=${AI_GATEWAY_CONTROLLER_IMAGE}"
echo "  PRAXIS_EXTPROC_IMAGE=${PRAXIS_EXTPROC_IMAGE}"
