#!/usr/bin/env python3
"""Exercise the MaaS IPP->Praxis translation gate against a live Kind cluster."""

import datetime
import json
import os
import pathlib
import re
import subprocess
import sys
import time


CLUSTER = os.environ.get("LOCAL_ENV_CLUSTER", "external-model-two-plane")
NAMESPACE = os.environ.get("LOCAL_ENV_MIGRATION_NAMESPACE", "models-as-a-service")
GATEWAY_NAMESPACE = "maas-system"
EVIDENCE = pathlib.Path(os.environ.get("LOCAL_ENV_MIGRATION_EVIDENCE") or (
    pathlib.Path(__file__).resolve().parents[2] / "evidence" /
    (datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-ipp-translation-migration")
))
EVIDENCE.mkdir(parents=True, exist_ok=True)
RESULTS = EVIDENCE / "results.json"
CONTEXT = f"kind-{CLUSTER}"
MTC_NAME = "default-tenant"
MODEL_NAME = "demo-model"
CONFIGMAP_NAME = "payload-processing-plugins"
ANTHROPIC_PROVIDER_NAME = "ipp-phase1-migration-anthropic"
MIGRATION_ANNOTATION = "maas.opendatahub.io/payload-processing-migration"
STATUS_ANNOTATION = "maas.opendatahub.io/payload-processing-status"
TRANSLATION_RISK_ANNOTATION = "maas.opendatahub.io/api-translation-migration-risk"
RETRY_ANNOTATION = "maas.opendatahub.io/api-translation-migration-retry"
MANAGED_ANNOTATION = "opendatahub.io/managed"
BLOCKED = "blocked"
STEADY = "steady"
CLEANUP_COMPLETE = "cleanup-complete"

PREPROCESSING = """apiVersion: llm-d.ai/v1alpha1
kind: PayloadProcessorConfig
plugins:
- type: body-field-to-header
  name: model-extractor
  parameters:
    fieldName: model
    headerName: X-Gateway-Model-Name
- type: model-provider-resolver
profiles:
- name: default
  plugins:
    request:
    - pluginRef: model-extractor
    - pluginRef: model-provider-resolver
"""

PROCESSING_DEFAULT = """apiVersion: llm-d.ai/v1alpha1
kind: PayloadProcessorConfig
plugins:
- type: maas-headers-guard
  name: maas-headers-guard
- type: stream-usage-enforcer
- type: model-provider-resolver
- type: api-translation
- type: apikey-injection
profiles:
- name: default
  plugins:
    request:
    - pluginRef: maas-headers-guard
    - pluginRef: stream-usage-enforcer
    - pluginRef: model-provider-resolver
    - pluginRef: api-translation
    - pluginRef: apikey-injection
    response: []
"""

PROCESSING_RESPONSE_TRANSLATION = """apiVersion: llm-d.ai/v1alpha1
kind: PayloadProcessorConfig
plugins:
- type: maas-headers-guard
  name: maas-headers-guard
- type: stream-usage-enforcer
- type: model-provider-resolver
- type: api-translation
- type: apikey-injection
profiles:
- name: default
  plugins:
    request:
    - pluginRef: maas-headers-guard
    - pluginRef: stream-usage-enforcer
    - pluginRef: model-provider-resolver
    - pluginRef: api-translation
    - pluginRef: apikey-injection
    response:
    - pluginRef: api-translation
"""

RESULT = {"status": "RUNNING", "assertions": [], "events": []}
MTC_ORIGINAL = None
MODEL_ORIGINAL = None
CONFIGMAP_ORIGINAL = None
GATEWAY_CONTROLLER_REPLICAS = None
PRAXIS_DEPLOYMENTS_ORIGINAL = {}
PROVIDER_CREATED = False
CONTROLLER_SCALED_DOWN = False


def run(args, *, input_text=None, check=True, timeout=90):
    proc = subprocess.run(args, input=input_text, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, timeout=timeout)
    if check and proc.returncode:
        raise RuntimeError(f"command failed rc={proc.returncode}: {' '.join(args)}: {proc.stderr[-1600:]}")
    return proc


def kubectl(*args, **kwargs):
    kwargs.setdefault("timeout", 120)
    return run(["kubectl", "--context", CONTEXT, *args], **kwargs)


def save_json(path, value):
    pathlib.Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def record(name, passed, details):
    item = {"name": name, "result": "PASS" if passed else "FAIL", "details": details}
    RESULT["assertions"].append(item)
    save_json(RESULTS, RESULT)
    print(f"{item['result']} {name}: {details}", flush=True)
    if not passed:
        raise RuntimeError(f"assertion failed: {name}")


def get_json(namespace, resource, name):
    return json.loads(kubectl("-n", namespace, "get", resource, name, "-o", "json").stdout)


def get_configmap(check=True):
    args = ["-n", GATEWAY_NAMESPACE, "get", "configmap", CONFIGMAP_NAME, "-o", "json"]
    if not check:
        args.append("--ignore-not-found")
    result = kubectl(*args, check=check)
    if result.returncode:
        raise RuntimeError(f"ConfigMap read failed rc={result.returncode}: {result.stderr[-1600:]}")
    return json.loads(result.stdout) if result.stdout.strip() else None


def annotations(mtc):
    return mtc.get("metadata", {}).get("annotations", {})


def status_value(mtc, key):
    return annotations(mtc).get(key, "")


def redact_status_message(message):
    message = re.sub(
        r"(?i)(\b(?:authorization|x-api-key|api[_-]?key|token|secret)\b\s*[:=]\s*)\S+",
        r"\1[REDACTED]",
        message,
    )
    return re.sub(r"\bsk-[A-Za-z0-9_-]{8,}\b", "[REDACTED]", message)


def set_translation(value):
    if value is None:
        mtc = get_json(NAMESPACE, "maastenantconfig", MTC_NAME)
        if "apiTranslation" in mtc.get("spec", {}):
            kubectl("-n", NAMESPACE, "patch", "maastenantconfig", MTC_NAME, "--type=json", "-p",
                    '[{"op":"remove","path":"/spec/apiTranslation"}]')
        return
    kubectl("-n", NAMESPACE, "patch", "maastenantconfig", MTC_NAME, "--type=merge", "-p",
            json.dumps({"spec": {"apiTranslation": {"enabled": value}}}))


def patch_migration_annotations(*, migration=None, status=None):
    values = {
        MIGRATION_ANNOTATION: migration,
        STATUS_ANNOTATION: status,
    }
    kubectl("-n", NAMESPACE, "patch", "maastenantconfig", MTC_NAME, "--type=merge", "-p",
            json.dumps({"metadata": {"annotations": values}}))


def wait_until(predicate, label, timeout=180):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = get_json(NAMESPACE, "maastenantconfig", MTC_NAME)
            if predicate(last):
                return last
        except (RuntimeError, json.JSONDecodeError):
            pass
        time.sleep(1)
    if last is not None:
        save_json(EVIDENCE / f"timeout-{label}.json", last)
    raise RuntimeError(f"timed out waiting for {label}")


def normalized_provider_refs(model):
    refs = model.get("spec", {}).get("externalProviderRefs", [])
    normalized = []
    for ref in refs:
        provider = ref.get("ref") or {}
        normalized.append((
            provider.get("namespace") or NAMESPACE,
            provider.get("name"),
            ref.get("apiFormat"),
            ref.get("path"),
            ref.get("targetModel"),
            ref.get("weight", 1),
        ))
    return sorted(normalized)


def wait_for_model_refs(expected_refs, label, timeout=60):
    expected_model = {"spec": {"externalProviderRefs": expected_refs}}
    expected = normalized_provider_refs(expected_model)
    deadline = time.monotonic() + timeout
    stable_reads = 0
    previous_state = None
    observations = []
    while time.monotonic() < deadline:
        try:
            model = get_json(NAMESPACE, "externalmodel", MODEL_NAME)
            generation = model.get("metadata", {}).get("generation")
            resource_version = model.get("metadata", {}).get("resourceVersion")
            actual = normalized_provider_refs(model)
            state = (generation, actual)
            observations.append({
                "observedAtUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "generation": generation,
                "resourceVersion": resource_version,
                "providerRefs": actual,
                "matchesExpected": actual == expected,
            })
            if actual == expected:
                stable_reads = stable_reads + 1 if state == previous_state else 1
                if stable_reads >= 2:
                    evidence = {
                        "label": label,
                        "readSource": "kubectl GET from the API server",
                        "stableReadsRequired": 2,
                        "stableReadsObserved": stable_reads,
                        "expectedProviderRefs": expected,
                        "observations": observations,
                    }
                    save_json(EVIDENCE / f"{label}-restored-externalmodel-ref-barrier.json", evidence)
                    RESULT["events"].append({
                        "name": "restored_externalmodel_refs_observed_before_retry",
                        "label": label,
                        "generation": generation,
                        "resourceVersion": resource_version,
                        "stableReads": stable_reads,
                        "providerRefs": actual,
                    })
                    save_json(RESULTS, RESULT)
                    return model
            else:
                stable_reads = 0
            previous_state = state
        except (RuntimeError, json.JSONDecodeError):
            stable_reads = 0
            previous_state = None
        time.sleep(1)

    save_json(EVIDENCE / f"{label}-restored-externalmodel-ref-barrier.json", {
        "label": label,
        "readSource": "kubectl GET from the API server",
        "stableReadsRequired": 2,
        "expectedProviderRefs": expected,
        "observations": observations,
        "result": "timeout",
    })
    raise RuntimeError(f"timed out waiting for restored ExternalModel provider refs for {label}")


def cleanup_handoff_observation(mtc, model, configmap_present):
    conditions = []
    for condition in mtc.get("status", {}).get("conditions", []):
        if condition.get("type") not in ("Ready", "DeploymentsAvailable", "Degraded"):
            continue
        item = {key: condition.get(key) for key in ("type", "status", "reason", "observedGeneration")}
        message = condition.get("message", "")
        if "active ExternalModel provider bindings" in message:
            item["providerCompatibilityMessage"] = redact_status_message(message)
        conditions.append(item)
    metadata = mtc.get("metadata", {})
    return {
        "observedAtUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "maasTenantConfig": {
            "generation": metadata.get("generation"),
            "resourceVersion": metadata.get("resourceVersion"),
            "migration": status_value(mtc, MIGRATION_ANNOTATION),
            "payloadProcessingStatus": status_value(mtc, STATUS_ANNOTATION),
            "apiTranslationEnabled": (
                (mtc.get("spec", {}).get("apiTranslation") or {}).get("enabled")
            ),
            "conditions": conditions,
        },
        "externalModel": {
            "generation": model.get("metadata", {}).get("generation"),
            "resourceVersion": model.get("metadata", {}).get("resourceVersion"),
            "providerRefs": normalized_provider_refs(model),
        },
        "legacyConfigMapPresent": configmap_present,
    }


def wait_for_cleanup_handoff(label, timeout=360):
    deadline = time.monotonic() + timeout
    observations = []
    last_sample_key = None
    last_external_model_sample = 0.0
    last_mtc = None
    while time.monotonic() < deadline:
        try:
            mtc = get_json(NAMESPACE, "maastenantconfig", MTC_NAME)
            last_mtc = mtc
            status = status_value(mtc, STATUS_ANNOTATION)
            now = time.monotonic()
            if not observations or now - last_external_model_sample >= 10 or status in (CLEANUP_COMPLETE, STEADY):
                model = get_json(NAMESPACE, "externalmodel", MODEL_NAME)
                configmap_present = get_configmap(check=False) is not None
                observation = cleanup_handoff_observation(mtc, model, configmap_present)
                sample_key = json.dumps(observation["maasTenantConfig"], sort_keys=True)
                if sample_key != last_sample_key or now - last_external_model_sample >= 10 or status in (CLEANUP_COMPLETE, STEADY):
                    observations.append(observation)
                    last_sample_key = sample_key
                    last_external_model_sample = now
            if status in (CLEANUP_COMPLETE, STEADY):
                save_json(EVIDENCE / f"{label}-cleanup-handoff-observations.json", {
                    "label": label,
                    "pollIntervalSeconds": 1,
                    "externalModelSampleIntervalSeconds": 10,
                    "result": "handoff observed",
                    "observations": observations,
                })
                return mtc
        except (RuntimeError, json.JSONDecodeError):
            pass
        time.sleep(1)

    if last_mtc is not None:
        save_json(EVIDENCE / f"timeout-{label}-cleanup-handoff.json", last_mtc)
    save_json(EVIDENCE / f"{label}-cleanup-handoff-observations.json", {
        "label": label,
        "pollIntervalSeconds": 1,
        "externalModelSampleIntervalSeconds": 10,
        "result": "timeout",
        "observations": observations,
    })
    raise RuntimeError(f"timed out waiting for {label}-cleanup-handoff")


def wait_provider_ready():
    for _ in range(120):
        provider = kubectl("-n", NAMESPACE, "get", "externalprovider", ANTHROPIC_PROVIDER_NAME,
                           "-o", "json", check=False)
        if provider.returncode == 0:
            obj = json.loads(provider.stdout)
            if obj.get("status", {}).get("phase") == "Ready" and any(
                    condition.get("type") == "Ready" and condition.get("status") == "True"
                    for condition in obj.get("status", {}).get("conditions", [])):
                save_json(EVIDENCE / "anthropic-provider-ready.json", obj)
                return
        time.sleep(1)
    raise RuntimeError("Anthropic ExternalProvider did not become Ready")


def add_anthropic_binding():
    global PROVIDER_CREATED
    provider = {
        "apiVersion": "inference.opendatahub.io/v1alpha1",
        "kind": "ExternalProvider",
        "metadata": {"name": ANTHROPIC_PROVIDER_NAME, "namespace": NAMESPACE,
                     "labels": {"app.kubernetes.io/managed-by": "ipp-translation-migration-kind-e2e"}},
        "spec": {
            "provider": "anthropic",
            "endpoint": "provider-a-ext.maas-system.svc.cluster.local",
            "auth": {"type": "apikey", "secretRef": {"name": "provider-credentials"}},
            "config": {"tls.caCertificates": "/etc/external-model-e2e/provider-ca/ca.crt"},
        },
    }
    run(["kubectl", "--context", CONTEXT, "apply", "-f", "-"], input_text=json.dumps(provider))
    PROVIDER_CREATED = True
    wait_provider_ready()
    original_refs = MODEL_ORIGINAL.get("spec", {}).get("externalProviderRefs", [])
    openai_ref = next((ref for ref in original_refs
                       if ref.get("apiFormat") == "openai-chat" and ref.get("weight", 1) != 0), None)
    if openai_ref is None:
        raise RuntimeError("demo-model lacks an active OpenAI Chat Completions binding")
    refs = [json.loads(json.dumps(openai_ref)), {
        "ref": {"name": ANTHROPIC_PROVIDER_NAME},
        "targetModel": "claude-migration-test",
        "apiFormat": "messages",
        "path": "/v1/messages",
        "weight": 1,
    }]
    refs[0]["weight"] = 1
    kubectl("-n", NAMESPACE, "patch", "externalmodel", MODEL_NAME, "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": refs}}))
    patched_model = get_json(NAMESPACE, "externalmodel", MODEL_NAME)
    save_json(EVIDENCE / "active-mixed-externalmodel.json", patched_model)
    active_formats = [ref.get("apiFormat") for ref in patched_model.get("spec", {}).get("externalProviderRefs", [])
                      if ref.get("weight", 1) > 0]
    if "openai-chat" not in active_formats or "messages" not in active_formats:
        raise RuntimeError(f"live ExternalModel does not contain both active provider formats: {active_formats}")
    RESULT["events"].append({"name": "active_mixed_provider_fixture", "api_formats": [
        ref.get("apiFormat") for ref in patched_model.get("spec", {}).get("externalProviderRefs", [])
        if ref.get("weight", 1) > 0], "provider_ready": True,
        "model_generation": patched_model.get("metadata", {}).get("generation")})
    save_json(RESULTS, RESULT)


def add_openai_wire_azure_binding():
    """Prove apiFormat alone cannot classify an Azure provider as safe."""
    if not PROVIDER_CREATED:
        raise RuntimeError("temporary provider must exist before changing its provider family")
    kubectl("-n", NAMESPACE, "patch", "externalprovider", ANTHROPIC_PROVIDER_NAME,
            "--type=merge", "-p", json.dumps({"spec": {"provider": "azure"}}))
    original_refs = MODEL_ORIGINAL.get("spec", {}).get("externalProviderRefs", [])
    openai_ref = next((ref for ref in original_refs
                       if ref.get("apiFormat") == "openai-chat" and ref.get("weight", 1) != 0), None)
    if openai_ref is None:
        raise RuntimeError("demo-model lacks an active OpenAI Chat Completions binding")
    ref = json.loads(json.dumps(openai_ref))
    ref["ref"] = {"name": ANTHROPIC_PROVIDER_NAME}
    ref["weight"] = 1
    replace_model_refs([ref])
    provider = get_json(NAMESPACE, "externalprovider", ANTHROPIC_PROVIDER_NAME)
    model = get_json(NAMESPACE, "externalmodel", MODEL_NAME)
    save_json(EVIDENCE / "azure-openai-wire-provider.json", provider)
    save_json(EVIDENCE / "azure-openai-wire-externalmodel.json", model)
    if provider.get("spec", {}).get("provider") != "azure" or model.get("spec", {}).get(
            "externalProviderRefs", [{}])[0].get("apiFormat") != "openai-chat":
        raise RuntimeError("Azure OpenAI-wire compatibility fixture was not installed exactly")
    RESULT["events"].append({"name": "azure_openai_wire_fixture",
                             "provider_type": "azure",
                             "api_format": "openai-chat",
                             "model_generation": model.get("metadata", {}).get("generation")})
    save_json(RESULTS, RESULT)


def set_ipp_configmap(processing):
    current = get_configmap(check=False)
    if current is not None:
        kubectl("-n", GATEWAY_NAMESPACE, "delete", "configmap", CONFIGMAP_NAME, "--wait=true")
    manifest = {
        "apiVersion": "v1", "kind": "ConfigMap",
        "metadata": {"name": CONFIGMAP_NAME, "namespace": GATEWAY_NAMESPACE,
                     "annotations": {MANAGED_ANNOTATION: "false"}},
        "data": {"custom-pre-processing-ipp-config.yaml": PREPROCESSING,
                 "custom-ipp-config.yaml": processing},
    }
    run(["kubectl", "--context", CONTEXT, "apply", "-f", "-"], input_text=json.dumps(manifest))
    actual = get_configmap()
    expected_data = {"custom-pre-processing-ipp-config.yaml": PREPROCESSING,
                     "custom-ipp-config.yaml": processing}
    if actual.get("data") != expected_data:
        raise RuntimeError("live IPP fixture did not retain the exact two-key known-good ConfigMap")
    return actual


def block_and_verify(label, expected_enabled, expected_binding):
    mtc = wait_until(
        lambda obj: status_value(obj, MIGRATION_ANNOTATION) == BLOCKED
        and (obj.get("spec", {}).get("apiTranslation", {}).get("enabled") if obj.get("spec", {}).get("apiTranslation") else None) == expected_enabled,
        f"{label}-compatibility-block")
    # The migration marker is patched while the platform reconcile is still
    # running. The Ready condition is written after RunPlatform returns, so
    # wait for the detailed blocked status before asserting its reason.
    mtc = wait_until(
        lambda obj: status_value(obj, MIGRATION_ANNOTATION) == BLOCKED
        and (obj.get("spec", {}).get("apiTranslation", {}).get("enabled") if obj.get("spec", {}).get("apiTranslation") else None) == expected_enabled
        and any(condition.get("type") == "Ready"
                and condition.get("status") == "False"
                and condition.get("reason") == "DeploymentsNotReady"
                and "active ExternalModel provider bindings" in condition.get("message", "")
                and "cannot preserve OpenAI Chat Completions" in condition.get("message", "")
                and expected_binding in condition.get("message", "")
                for condition in obj.get("status", {}).get("conditions", [])),
        f"{label}-compatibility-status-reason")
    configmap = get_configmap()
    save_json(EVIDENCE / f"{label}-blocked-mtc.json", mtc)
    save_json(EVIDENCE / f"{label}-blocked-configmap.json", configmap)
    expected_processing = (PROCESSING_RESPONSE_TRANSLATION if label == "response-enabled"
                           else PROCESSING_DEFAULT)
    preserved_fingerprint = configmap.get("data") == {
        "custom-pre-processing-ipp-config.yaml": PREPROCESSING,
        "custom-ipp-config.yaml": expected_processing,
    }
    conditions = mtc.get("status", {}).get("conditions", [])
    reason = " ".join(condition.get("message", "") for condition in conditions)
    reason_present = ("active ExternalModel provider bindings" in reason
                      and "cannot preserve OpenAI Chat Completions" in reason
                      and expected_binding in reason)
    record(f"{label}_blocks_unsafe_handoff", preserved_fingerprint and reason_present, {
            "migration_annotation": status_value(mtc, MIGRATION_ANNOTATION),
            "api_translation_enabled": expected_enabled,
            "expected_binding": expected_binding,
            "configmap_present": True,
            "configmap_data_keys": sorted(configmap.get("data", {})),
            "exact_fingerprint_retained": preserved_fingerprint,
            "provider_compatibility_reason_present": reason_present,
            "status_reason": reason,
        })
    return mtc, configmap


def remove_ipp_configmap_and_wait(label, expected_enabled):
    # Observe either side of the handshake. A live Praxis controller can
    # consume cleanup-complete before this polling test sees the intermediate
    # annotation and advance directly to steady.
    mtc = wait_for_cleanup_handoff(label, timeout=360)
    configmap = get_configmap(check=False)
    if configmap is not None:
        raise RuntimeError(f"{label}: migration reached steady but the legacy ConfigMap remains")
    enabled = mtc.get("spec", {}).get("apiTranslation", {}).get("enabled") if mtc.get("spec", {}).get("apiTranslation") else None
    save_json(EVIDENCE / f"{label}-after-cleanup-mtc.json", mtc)
    praxis_resources = {}
    for namespace, name in ((GATEWAY_NAMESPACE, "payload-processing"),
                             (NAMESPACE, "payload-processing-external-model")):
        result = kubectl("-n", namespace, "get", "deployment", name, "-o", "json", check=False)
        key = f"{namespace}/{name}"
        praxis_resources[key] = result.returncode == 0
        if result.returncode == 0:
            save_json(EVIDENCE / f"{label}-{namespace}-{name}-deployment.json", json.loads(result.stdout))
    resources_preserved = all(
        not existed or praxis_resources.get(key, False)
        for key, existed in PRAXIS_DEPLOYMENTS_ORIGINAL.items()
    )
    controller_replicas = deployment_replicas("opendatahub", "ai-gateway-controller")
    record(f"{label}_cleanup_decision", enabled is expected_enabled, {
        "handoff_status": status_value(mtc, STATUS_ANNOTATION),
        "praxis_controller_replicas": controller_replicas,
        "praxis_controller_paused_during_gate_test": controller_replicas == 0,
        "configmap_absent": True,
        "api_translation_enabled": enabled,
        "praxis_deployments_preserved": praxis_resources,
    })
    record(f"{label}_preserves_praxis_owned_deployments", resources_preserved, praxis_resources)
    return mtc


def clear_block_and_retry(label, expected_enabled):
    # The controller may reconcile immediately when the retry annotations
    # change. Prove its live-reader input is back to the exact saved provider
    # profile before that retry event is issued.
    wait_for_model_refs(MODEL_ORIGINAL.get("spec", {}).get("externalProviderRefs", []), label)
    patch_migration_annotations(migration=None, status=None)
    retry_token = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    kubectl("-n", NAMESPACE, "annotate", "maastenantconfig", MTC_NAME,
            f"{RETRY_ANNOTATION}={retry_token}", "--overwrite")
    RESULT["events"].append({"name": "explicit_migration_retry_trigger",
                             "annotation": RETRY_ANNOTATION,
                             "token": retry_token,
                             "label": label})
    save_json(RESULTS, RESULT)
    return remove_ipp_configmap_and_wait(label, expected_enabled)


def deployment_replicas(namespace, name):
    return get_json(namespace, "deployment", name).get("spec", {}).get("replicas", 1)


def wait_for_pods(namespace, name, expected):
    deployment = get_json(namespace, "deployment", name)
    selector = deployment.get("spec", {}).get("selector", {}).get("matchLabels", {})
    selector_arg = ",".join(f"{key}={value}" for key, value in sorted(selector.items()))
    for _ in range(120):
        pods = json.loads(kubectl("-n", namespace, "get", "pods", "-l", selector_arg, "-o", "json").stdout)
        running = [pod for pod in pods.get("items", [])
                   if pod.get("status", {}).get("phase") not in ("Succeeded", "Failed")]
        ready = [pod for pod in running if any(
            condition.get("type") == "Ready" and condition.get("status") == "True"
            for condition in pod.get("status", {}).get("conditions", []))]
        if len(running) == expected and len(ready) == expected:
            return
        time.sleep(1)
    raise RuntimeError(f"deployment {namespace}/{name} did not converge to {expected} pods")


def scale_gateway_controller(replicas):
    global CONTROLLER_SCALED_DOWN
    CONTROLLER_SCALED_DOWN = replicas == 0
    kubectl("-n", "opendatahub", "scale", "deployment/ai-gateway-controller", f"--replicas={replicas}")
    wait_for_pods("opendatahub", "ai-gateway-controller", replicas)


def replace_model_refs(refs):
    kubectl("-n", NAMESPACE, "patch", "externalmodel", MODEL_NAME, "--type=merge", "-p",
            json.dumps({"spec": {"externalProviderRefs": refs}}))


def restart_maas_controller():
    kubectl("-n", GATEWAY_NAMESPACE, "rollout", "restart", "deployment/maas-controller")
    result = kubectl("-n", GATEWAY_NAMESPACE, "rollout", "status", "deployment/maas-controller",
                     "--timeout=180s", check=False, timeout=190)
    if result.returncode:
        raise RuntimeError(f"MaaS controller restart did not complete: {result.stderr[-1000:]}")
    mtc = get_json(NAMESPACE, "maastenantconfig", MTC_NAME)
    configmap = get_configmap()
    save_json(EVIDENCE / "after-controller-restart-mtc.json", mtc)
    save_json(EVIDENCE / "after-controller-restart-configmap.json", configmap)
    enabled = mtc.get("spec", {}).get("apiTranslation", {}).get("enabled") if mtc.get("spec", {}).get("apiTranslation") else None
    record("persisted_intent_and_configmap_survive_controller_restart",
           enabled is True and status_value(mtc, MIGRATION_ANNOTATION) == BLOCKED
           and configmap.get("data", {}).get("custom-ipp-config.yaml") == PROCESSING_RESPONSE_TRANSLATION,
           {"api_translation_enabled": enabled,
            "migration_annotation": status_value(mtc, MIGRATION_ANNOTATION),
            "configmap_present": True,
            "controller_rollout": result.stdout.strip()})


def configmap_restore_patch(current, original):
    desired_data = original.get("data", {})
    current_metadata = current.get("metadata", {})
    original_metadata = original.get("metadata", {})
    patch = []
    if current.get("data", {}) != desired_data:
        # JSON merge patch recursively merges maps, leaving keys behind when
        # the snapshot does not contain them. Replace the complete data map.
        patch.append({"op": "add", "path": "/data", "value": desired_data})
    for field in ("labels", "annotations"):
        current_values = current_metadata.get(field) or {}
        desired_values = original_metadata.get(field) or {}
        if current_values == desired_values:
            continue
        path = f"/metadata/{field}"
        if desired_values:
            # Replacing the complete map also removes run-added metadata keys.
            patch.append({"op": "add", "path": path, "value": desired_values})
        elif current_values:
            patch.append({"op": "remove", "path": path})
    return patch


def restore_original_configmap():
    if not CONFIGMAP_ORIGINAL:
        return None
    current = get_configmap(check=False)
    if current is None:
        manifest = {
            "apiVersion": "v1", "kind": "ConfigMap",
            "metadata": {"name": CONFIGMAP_NAME, "namespace": GATEWAY_NAMESPACE,
                         "labels": CONFIGMAP_ORIGINAL.get("metadata", {}).get("labels", {}),
                         "annotations": CONFIGMAP_ORIGINAL.get("metadata", {}).get("annotations", {})},
            "data": CONFIGMAP_ORIGINAL.get("data", {}),
        }
        run(["kubectl", "--context", CONTEXT, "apply", "-f", "-"], input_text=json.dumps(manifest))
    else:
        patch = configmap_restore_patch(current, CONFIGMAP_ORIGINAL)
        if patch:
            # Restore the original data and metadata maps, including annotations
            # when a controller recreated the ConfigMap during test cleanup.
            # This cluster is run-owned; no concurrent qualification may mutate it.
            kubectl("-n", GATEWAY_NAMESPACE, "patch", "configmap", CONFIGMAP_NAME,
                    "--type=json", "-p", json.dumps(patch))
    restored = get_configmap(check=False)
    desired_metadata = CONFIGMAP_ORIGINAL.get("metadata", {})
    metadata = {} if restored is None else restored.get("metadata", {})
    data_matches = restored is not None and restored.get("data", {}) == CONFIGMAP_ORIGINAL.get("data", {})
    labels_match = (metadata.get("labels") or {}) == (desired_metadata.get("labels") or {})
    annotations_match = (metadata.get("annotations") or {}) == (desired_metadata.get("annotations") or {})
    if not data_matches or not labels_match or not annotations_match:
        actual_data = None if restored is None else restored.get("data", {})
        raise RuntimeError(
            "ConfigMap restore mismatch: "
            f"data keys expected {sorted(CONFIGMAP_ORIGINAL.get('data', {}))}, "
            f"found {sorted(actual_data or {})}; labels_match={labels_match}; "
            f"annotations_match={annotations_match}")
    return {
        "present": True,
        "dataMatches": data_matches,
        "labelsMatch": labels_match,
        "annotationsMatch": annotations_match,
        "uid": metadata.get("uid"),
        "uidRecreated": metadata.get("uid") != CONFIGMAP_ORIGINAL.get("metadata", {}).get("uid"),
    }


def cleanup():
    global CONTROLLER_SCALED_DOWN, PROVIDER_CREATED
    errors = []
    leave_cleanup_claim_for_praxis = False
    praxis_configmap_restoration = None
    if MODEL_ORIGINAL is not None:
        try:
            replace_model_refs(MODEL_ORIGINAL.get("spec", {}).get("externalProviderRefs", []))
        except Exception as exc:  # cleanup must continue to restore other run-owned state
            errors.append(f"restore ExternalModel refs: {exc}")
    if MTC_ORIGINAL is not None:
        try:
            original_translation = MTC_ORIGINAL.get("spec", {}).get("apiTranslation")
            set_translation(original_translation.get("enabled") if original_translation else None)
            original_annotations = annotations(MTC_ORIGINAL)
            current_mtc = get_json(NAMESPACE, "maastenantconfig", MTC_NAME)
            leave_cleanup_claim_for_praxis = (
                RESULT.get("status") == "PASS"
                and status_value(current_mtc, STATUS_ANNOTATION) == CLEANUP_COMPLETE
            )
            original_retry = annotations(MTC_ORIGINAL).get(RETRY_ANNOTATION)
            current_retry = annotations(current_mtc).get(RETRY_ANNOTATION)
            if current_retry != original_retry:
                if original_retry is None:
                    kubectl("-n", NAMESPACE, "annotate", "maastenantconfig", MTC_NAME,
                            f"{RETRY_ANNOTATION}-")
                else:
                    kubectl("-n", NAMESPACE, "annotate", "maastenantconfig", MTC_NAME,
                            f"{RETRY_ANNOTATION}={original_retry}", "--overwrite")
            original_risk = original_annotations.get(TRANSLATION_RISK_ANNOTATION)
            current_risk = annotations(current_mtc).get(TRANSLATION_RISK_ANNOTATION)
            if current_risk != original_risk:
                if original_risk is None:
                    kubectl("-n", NAMESPACE, "annotate", "maastenantconfig", MTC_NAME,
                            f"{TRANSLATION_RISK_ANNOTATION}-")
                else:
                    kubectl("-n", NAMESPACE, "annotate", "maastenantconfig", MTC_NAME,
                            f"{TRANSLATION_RISK_ANNOTATION}={original_risk}", "--overwrite")
            patch_migration_annotations(
                migration=original_annotations.get(MIGRATION_ANNOTATION),
                status=(CLEANUP_COMPLETE if leave_cleanup_claim_for_praxis
                        else original_annotations.get(STATUS_ANNOTATION)))
        except Exception as exc:
            errors.append(f"restore MaasTenantConfig translation and migration markers: {exc}")
    if CONTROLLER_SCALED_DOWN and GATEWAY_CONTROLLER_REPLICAS is not None:
        try:
            scale_gateway_controller(GATEWAY_CONTROLLER_REPLICAS)
            if MTC_ORIGINAL is not None and leave_cleanup_claim_for_praxis:
                mtc = wait_until(lambda obj: status_value(obj, STATUS_ANNOTATION) == STEADY,
                                 "praxis-claims-cleanup-complete", timeout=180)
                configmap = get_configmap(check=False)
                legacy_keys = {"custom-ipp-config.yaml", "custom-pre-processing-ipp-config.yaml"}
                remaining_legacy_keys = sorted(legacy_keys.intersection(
                    (configmap or {}).get("data", {}).keys()))
                record("praxis_claims_cleanup_complete",
                       status_value(mtc, STATUS_ANNOTATION) == STEADY and not remaining_legacy_keys, {
                    "cleanup_complete_consumed": status_value(mtc, STATUS_ANNOTATION) == STEADY,
                    "final_status": status_value(mtc, STATUS_ANNOTATION),
                    "legacy_ipp_configmap_absent": not remaining_legacy_keys,
                    "remaining_legacy_ipp_keys": remaining_legacy_keys,
                })
        except Exception as exc:
            errors.append(f"restore ai-gateway-controller replicas and complete Praxis handoff: {exc}")
    if PROVIDER_CREATED and not CONTROLLER_SCALED_DOWN:
        result = kubectl("-n", NAMESPACE, "delete", "externalprovider", ANTHROPIC_PROVIDER_NAME,
                         "--ignore-not-found", "--wait=true", check=False)
        if result.returncode:
            errors.append(f"delete temporary provider: {result.stderr[-500:]}")
        else:
            PROVIDER_CREATED = False
    elif PROVIDER_CREATED:
        errors.append("temporary ExternalProvider retained because ai-gateway-controller did not return")
    try:
        praxis_configmap_restoration = restore_original_configmap()
    except Exception as exc:
        errors.append(f"restore Praxis ConfigMap snapshot: {exc}")
    risk_restored = True
    if MTC_ORIGINAL is not None:
        try:
            restored = get_json(NAMESPACE, "maastenantconfig", MTC_NAME)
            risk_restored = (
                annotations(restored).get(TRANSLATION_RISK_ANNOTATION)
                == annotations(MTC_ORIGINAL).get(TRANSLATION_RISK_ANNOTATION)
            )
            if not risk_restored:
                errors.append("restore MaasTenantConfig API translation migration-risk marker: value differs from snapshot")
        except Exception as exc:
            risk_restored = False
            errors.append(f"verify MaasTenantConfig API translation migration-risk marker: {exc}")
    RESULT["cleanup"] = {"errors": errors, "gateway_controller_replicas_restored": not CONTROLLER_SCALED_DOWN,
                         "temporary_provider_removed": not PROVIDER_CREATED,
                         "api_translation_risk_restored": risk_restored,
                         "praxis_configmap_restoration": praxis_configmap_restoration}
    save_json(RESULTS, RESULT)
    (EVIDENCE / "cleanup.txt").write_text(json.dumps(RESULT["cleanup"], indent=2) + "\n")
    if errors:
        RESULT["status"] = "FAIL"
        RESULT["failure"] = "cleanup errors: " + "; ".join(errors)
        save_json(RESULTS, RESULT)


def main():
    global MTC_ORIGINAL, MODEL_ORIGINAL, CONFIGMAP_ORIGINAL, GATEWAY_CONTROLLER_REPLICAS
    global PRAXIS_DEPLOYMENTS_ORIGINAL
    save_json(RESULTS, RESULT)
    MTC_ORIGINAL = get_json(NAMESPACE, "maastenantconfig", MTC_NAME)
    MODEL_ORIGINAL = get_json(NAMESPACE, "externalmodel", MODEL_NAME)
    CONFIGMAP_ORIGINAL = get_configmap()
    GATEWAY_CONTROLLER_REPLICAS = deployment_replicas("opendatahub", "ai-gateway-controller")
    PRAXIS_DEPLOYMENTS_ORIGINAL = {}
    for namespace, name in ((GATEWAY_NAMESPACE, "payload-processing"),
                             (NAMESPACE, "payload-processing-external-model")):
        result = kubectl("-n", namespace, "get", "deployment", name, "-o", "json", check=False)
        key = f"{namespace}/{name}"
        PRAXIS_DEPLOYMENTS_ORIGINAL[key] = result.returncode == 0
        if result.returncode == 0:
            save_json(EVIDENCE / f"before-{namespace}-{name}-deployment.json", json.loads(result.stdout))
    save_json(EVIDENCE / "maastenantconfig-before.json", MTC_ORIGINAL)
    save_json(EVIDENCE / "externalmodel-before.json", MODEL_ORIGINAL)
    save_json(EVIDENCE / "praxis-configmap-before.json", CONFIGMAP_ORIGINAL)
    save_json(EVIDENCE / "gateway-controller-before.json",
              get_json("opendatahub", "deployment", "ai-gateway-controller"))
    baseline_data = CONFIGMAP_ORIGINAL.get("data", {})
    legacy_ipp_keys = {"custom-ipp-config.yaml", "custom-pre-processing-ipp-config.yaml"}
    stale_ipp_keys = sorted(legacy_ipp_keys.intersection(baseline_data))
    record("preconditions", baseline_data.get("extproc.yaml") is not None
           and status_value(MTC_ORIGINAL, STATUS_ANNOTATION) == STEADY
           and all(PRAXIS_DEPLOYMENTS_ORIGINAL.values())
           and not stale_ipp_keys,
           {"tenant_namespace": NAMESPACE, "gateway_namespace": GATEWAY_NAMESPACE,
            "original_cleanup_status": status_value(MTC_ORIGINAL, STATUS_ANNOTATION),
            "gateway_controller_replicas": GATEWAY_CONTROLLER_REPLICAS,
            "praxis_deployments_before": PRAXIS_DEPLOYMENTS_ORIGINAL,
            "stale_ipp_configmap_keys": stale_ipp_keys,
            "original_externalmodel_provider_formats": [ref.get("apiFormat") for ref in MODEL_ORIGINAL.get("spec", {}).get("externalProviderRefs", [])]})

    set_translation(None)
    add_anthropic_binding()
    scale_gateway_controller(0)

    # Pure product default already ran request translation in IPP. Preserve it
    # only when an affected active provider requires it; leave intent omitted.
    set_ipp_configmap(PROCESSING_DEFAULT)
    patch_migration_annotations(migration=None, status=None)
    block_and_verify("pure-default", expected_enabled=None,
                     expected_binding="apiFormat=messages, provider=anthropic")
    replace_model_refs(MODEL_ORIGINAL.get("spec", {}).get("externalProviderRefs", []))
    clear_block_and_retry("pure-default-safe-openai", expected_enabled=None)

    # An OpenAI-shaped wire format is not sufficient evidence of safe
    # passthrough: provider-family path/response transforms remain unqualified.
    add_openai_wire_azure_binding()
    set_ipp_configmap(PROCESSING_DEFAULT)
    patch_migration_annotations(migration=None, status=None)
    block_and_verify("pure-default-azure-openai-wire", expected_enabled=None,
                     expected_binding="apiFormat=openai-chat, provider=azure")
    replace_model_refs(MODEL_ORIGINAL.get("spec", {}).get("externalProviderRefs", []))
    clear_block_and_retry("pure-default-safe-openai-after-azure", expected_enabled=None)

    # Response-enabled baseline: persist true, then stop before cleanup while
    # the mixed-provider model demonstrates the missing legacy runtime path.
    add_anthropic_binding()
    set_translation(None)
    set_ipp_configmap(PROCESSING_RESPONSE_TRANSLATION)
    patch_migration_annotations(migration=None, status=None)
    block_and_verify("response-enabled", expected_enabled=True,
                     expected_binding="apiFormat=messages, provider=anthropic")
    restart_maas_controller()
    replace_model_refs(MODEL_ORIGINAL.get("spec", {}).get("externalProviderRefs", []))
    clear_block_and_retry("response-enabled-safe-openai", expected_enabled=True)

    # Explicit false is a deliberate compatibility-risk override. The gate
    # must honor it and surface the accepted loss while cleaning up.
    add_anthropic_binding()
    set_translation(False)
    set_ipp_configmap(PROCESSING_RESPONSE_TRANSLATION)
    patch_migration_annotations(migration=None, status=None)
    remove_ipp_configmap_and_wait("explicit-false-override", expected_enabled=False)
    overridden = wait_until(
        lambda obj: status_value(obj, STATUS_ANNOTATION) in (CLEANUP_COMPLETE, STEADY)
        and obj.get("spec", {}).get("apiTranslation", {}).get("enabled") is False
        and any(condition.get("type") == "Degraded"
                and condition.get("status") == "True"
                and "explicit spec.apiTranslation.enabled=false accepts this compatibility loss"
                in condition.get("message", "")
                for condition in obj.get("status", {}).get("conditions", [])),
        "explicit-false-override-warning")
    conditions = overridden.get("status", {}).get("conditions", [])
    warning = " ".join(condition.get("message", "") for condition in conditions)
    save_json(EVIDENCE / "explicit-false-status.json", overridden)
    record("explicit_false_override_is_preserved_and_warned",
           any(condition.get("status") == "True" and "accepts this compatibility loss" in condition.get("message", "")
               for condition in conditions),
           {"api_translation_enabled": False,
            "accepted_risk_marker": status_value(overridden, TRANSLATION_RISK_ANNOTATION),
            "warning_message": warning,
            "configmap_absent": get_configmap(check=False) is None})

    # The one-time cleanup reconcile is not enough: a later steady reconcile
    # used to clear this warning after the IPP ConfigMap was gone. Give the
    # controller's periodic reconcile time to run, then assert the risk remains
    # visible while the unsupported Anthropic provider binding is active.
    scale_gateway_controller(GATEWAY_CONTROLLER_REPLICAS)
    wait_until(lambda obj: status_value(obj, STATUS_ANNOTATION) == STEADY,
               "praxis-claims-explicit-false-cleanup", timeout=180)
    time.sleep(23)
    steady = get_json(NAMESPACE, "maastenantconfig", MTC_NAME)
    steady_conditions = steady.get("status", {}).get("conditions", [])
    steady_warning = " ".join(condition.get("message", "") for condition in steady_conditions)
    save_json(EVIDENCE / "explicit-false-steady-status.json", steady)
    record("explicit_false_steady_warning_survives_reconcile",
           status_value(steady, STATUS_ANNOTATION) == STEADY
           and status_value(steady, TRANSLATION_RISK_ANNOTATION) == "explicit-false"
           and any(condition.get("type") == "Degraded" and condition.get("status") == "True"
                   and "accepts this compatibility loss" in condition.get("message", "")
                   for condition in steady_conditions),
           {"api_translation_enabled": steady.get("spec", {}).get("apiTranslation", {}).get("enabled"),
            "warning_message": steady_warning,
            "configmap_absent": get_configmap(check=False) is None})
    RESULT["status"] = "PASS"
    save_json(RESULTS, RESULT)


def main_wrapper():
    try:
        main()
    except Exception as exc:
        safe_failure = redact_status_message(str(exc))
        RESULT["status"] = "FAIL"
        RESULT["failure"] = safe_failure
        save_json(RESULTS, RESULT)
        (EVIDENCE / "failure.txt").write_text(safe_failure + "\n")
        # Preserve runtime state for diagnosis without persisting raw controller logs,
        # which could contain credentials or private-key material.
        deployment_result = kubectl(
            "-n", GATEWAY_NAMESPACE, "get", "deployment", "maas-controller", "-o", "json",
            check=False,
        )
        observation = {
            "observedAtUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "rawLogsCaptured": False,
            "reason": "credential and private-key data must not be persisted",
        }
        if deployment_result.returncode == 0:
            deployment = json.loads(deployment_result.stdout)
            template = deployment["spec"]["template"]
            observation["deployment"] = {
                "uid": deployment["metadata"].get("uid"),
                "generation": deployment["metadata"].get("generation"),
                "image": template["spec"]["containers"][0].get("image"),
                "replicas": deployment["status"].get("replicas"),
                "readyReplicas": deployment["status"].get("readyReplicas"),
            }
            pods_result = kubectl(
                "-n", GATEWAY_NAMESPACE, "get", "pods", "-o", "json", check=False,
            )
            if pods_result.returncode == 0:
                pods = json.loads(pods_result.stdout)["items"]
                observation["pods"] = [
                    {
                        "name": pod["metadata"]["name"],
                        "uid": pod["metadata"].get("uid"),
                        "phase": pod.get("status", {}).get("phase"),
                        "containers": [
                            {
                                "name": container.get("name"),
                                "image": container.get("image"),
                                "imageID": container.get("imageID"),
                                "ready": container.get("ready"),
                                "restartCount": container.get("restartCount"),
                            }
                            for container in pod.get("status", {}).get("containerStatuses", [])
                        ],
                    }
                    for pod in pods
                    if pod["metadata"]["name"].startswith("maas-controller-")
                ]
        else:
            observation["deploymentReadError"] = redact_status_message(
                deployment_result.stderr[-1000:]
            )
        save_json(EVIDENCE / "maas-controller-on-failure.json", observation)
        raise
    finally:
        cleanup()
    if RESULT.get("status") != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main_wrapper()
