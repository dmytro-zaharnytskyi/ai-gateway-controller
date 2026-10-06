#!/usr/bin/env bash
# Default container images for ai-gateway-controller e2e (no local maas-api/maas-controller trees).
#
# MaaS main-branch Konflux pushes publish :latest and a tag for each commit SHA (see
# models-as-a-service .tekton/odh-maas-api-push.yaml and odh-maas-controller-push.yaml).
# Override with MAAS_API_IMAGE / MAAS_CONTROLLER_IMAGE when testing specific builds.

set -euo pipefail

# Deploy images built from the MaaS commit pinned in test/maas-e2e.lock, so the deployed
# MaaS matches the fetched tests. Set MAAS_IMAGE_TAG as well when overriding MAAS_COMMIT.
maas_lock_file="${MAAS_LOCK_FILE:-$(dirname "${BASH_SOURCE[0]}")/../../maas-e2e.lock}"
if [[ -z "${MAAS_IMAGE_TAG:-}" ]]; then
  MAAS_IMAGE_TAG="$(sed -n 's/^maas_commit=//p' "${maas_lock_file}" 2>/dev/null | tail -1)" || true
fi
if [[ -z "${MAAS_IMAGE_TAG}" ]]; then
  echo "ERROR: no maas_commit in ${maas_lock_file}; set MAAS_IMAGE_TAG" >&2
  exit 1
fi

# The shared Konflux group-test pipeline exports maas-*:latest, which drifts from the pinned
# tests whenever MaaS main changes. Use the pinned images instead unless
# MAAS_ALLOW_LATEST_IMAGES=true.
if [[ "${MAAS_ALLOW_LATEST_IMAGES:-false}" != "true" ]]; then
  if [[ "${MAAS_API_IMAGE:-}" == *:latest ]]; then
    unset MAAS_API_IMAGE
  fi
  if [[ "${MAAS_CONTROLLER_IMAGE:-}" == *:latest ]]; then
    unset MAAS_CONTROLLER_IMAGE
  fi
fi

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
