#!/usr/bin/env bash
set -Eeuo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
SCRIPT="$ROOT/test/e2e/scripts/deploy-ai-gateway-controller.sh"

wait_body=$(sed -n '/^_wait_for_praxis_extproc()/,/^_protect_praxis_from_maas_reconcile()/p' "$SCRIPT")
image_body=$(sed -n '/^assert_praxis_extproc_image()/,/^echo "Deploying ai-gateway-controller image:/p' "$SCRIPT")
gateway_payload_pattern="payload-processing -n \"\${GATEWAY_NAMESPACE}\""
gateway_pre_pattern="payload-pre-processing -n \"\${GATEWAY_NAMESPACE}\""
tenant_payload_pattern="payload-processing -n \"\${tenant_namespace}\""

grep -Fq "$gateway_payload_pattern" <<<"$wait_body"
grep -Fq "$gateway_pre_pattern" <<<"$wait_body"
grep -Fq 'shared praxis-extproc ready' <<<"$wait_body"
grep -Fq '/etc/praxis/extproc.yaml' <<<"$wait_body"
grep -Fq '/etc/praxis/pre-extproc.yaml' <<<"$wait_body"

if grep -Fq "$tenant_payload_pattern" <<<"$wait_body"; then
  echo 'FAIL: shared payload-processing wait still targets tenant_namespace' >&2
  exit 1
fi
if grep -Fq 'payload-processing-external-model' <<<"$wait_body"; then
  echo 'FAIL: shared readiness helper requires the optional ExternalModel workload' >&2
  exit 1
fi

grep -Fq "$gateway_payload_pattern" <<<"$image_body"
if grep -Fq "$tenant_payload_pattern" <<<"$image_body"; then
  echo 'FAIL: shared image assertion still targets tenant_namespace' >&2
  exit 1
fi

# Deploy-only path: no legacy IPP swap / annotation handoff.
main_tail=$(sed -n '/^echo "Deploying ai-gateway-controller image:/,$p' "$SCRIPT")
for needle in \
  '_apply_ai_gateway_controller' \
  '_wait_for_praxis_extproc' \
  '_protect_praxis_from_maas_reconcile' \
  'assert_praxis_extproc_image'
do
  grep -Fq "$needle" <<<"$main_tail" || {
    echo "FAIL: deploy sequence missing ${needle}" >&2
    exit 1
  }
done
for gone in \
  'REMOVE_MAAS_IPP' \
  '_select_praxis_on_default_tenant' \
  '_delete_legacy_ipp_in_gateway_namespace' \
  '_clear_praxis_cleanup_on_default_tenant' \
  '_pause_ai_gateway_controller' \
  '_pause_maas_controller'
do
  if grep -Fq "$gone" "$SCRIPT"; then
    echo "FAIL: legacy handoff helper ${gone} should be removed" >&2
    exit 1
  fi
done

echo 'deploy-ai-gateway-controller shared ExtProc wait: PASS'
