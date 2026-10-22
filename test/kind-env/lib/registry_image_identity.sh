#!/usr/bin/env bash

# Match a pushed image by its immutable registry digest and local runtime tag.
# Containerd may list both an OCI index digest and the resolved platform
# manifest digest, so callers must not assume either digest is first.
containerd_image_matches_pushed_ref() {
  local runtime_info=$1 expected_repo_digest=$2 expected_tag=$3
  [[ -n "$expected_repo_digest" && -n "$expected_tag" ]] || return 1
  jq -e --arg digest "$expected_repo_digest" --arg tag "$expected_tag" '
    ((.status.repoDigests // []) | index($digest)) != null and
    ((.status.repoTags // []) | index($tag)) != null
  ' <<<"$runtime_info" >/dev/null
}
