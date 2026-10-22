#!/usr/bin/env bash

# MaaS API Deployments share base selector labels; scope pod checks with the
# tenant-instance label from each Deployment's pod template as well.
maas_api_deployment_pod_selector() {
  jq -er '
    .spec.template.metadata.labels["maas.opendatahub.io/tenant-instance"] as $instance |
    if ($instance | type) != "string" or $instance == "" then
      error("MaaS API Deployment has no tenant-instance pod label")
    else
      .spec.selector.matchLabels + {"maas.opendatahub.io/tenant-instance": $instance}
      | to_entries | map(.key + "=" + .value) | join(",")
    end
  ' <<<"$1"
}

# Require a non-terminating, Running, Ready maas-api pod that requested this
# run's registry tag and is serving the exact pushed image digest. Containerd
# can report an older tag alias in status.image when the same image content was
# already present; status.imageID is the content identity to compare instead.
maas_api_pods_have_exact_ready_image() {
  local pods_json=$1 expected_image=$2 expected_image_digest=$3
  jq -e --arg image "$expected_image" --arg image_digest "$expected_image_digest" '
    any(.items[]?;
      .metadata.deletionTimestamp == null and .status.phase == "Running" and
      any(.spec.containers[]?;
        .name == "maas-api" and .image == $image
      ) and
      any(.status.containerStatuses[]?;
        .name == "maas-api" and .ready == true and
        ((.imageID // "" | sub("^containerd://"; "")) == $image_digest)
      )
    )
  ' <<<"$pods_json" >/dev/null
}
