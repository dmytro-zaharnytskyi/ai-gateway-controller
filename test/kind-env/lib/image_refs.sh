#!/usr/bin/env bash

# Resolve the registry pull reference separately from the local tag loaded into
# Kind. A digest is an immutable pull reference, never a Docker tag target.
resolve_katan_image_refs() {
  KATAN_DEFAULT_IMAGE=ghcr.io/nerdalert/llm-katan@sha256:11379a1ec2fd69dc121eada6c544eb423a7c074414507dc1d474f4abba9df75a
  KATAN_DEFAULT_AMD64_IMAGE=ghcr.io/nerdalert/llm-katan@sha256:a8bf18109e2db641ef4a63efe65f69d4d6554f1128a174053de89a8b81b4284d

  if [[ "${BUILD_KATAN:-false}" == true ]]; then
    if [[ "${KATAN_IMAGE:-}" == *@* ]]; then
      printf '%s\n' "KATAN_IMAGE must be a tag when BUILD_KATAN=true; a digest cannot name a Docker build target" >&2
      return 2
    fi
    KATAN_EFFECTIVE_IMAGE=${KATAN_IMAGE:-llm-katan:e2e}
    KATAN_KIND_IMAGE=${KATAN_KIND_IMAGE:-$KATAN_EFFECTIVE_IMAGE}
  else
    KATAN_EFFECTIVE_IMAGE=${KATAN_IMAGE:-$KATAN_DEFAULT_IMAGE}
    KATAN_KIND_IMAGE=${KATAN_KIND_IMAGE:-llm-katan:external-model-two-plane}
  fi

  KATAN_PULL_IMAGE=${KATAN_IMAGE:-$KATAN_DEFAULT_AMD64_IMAGE}
  if [[ "$KATAN_KIND_IMAGE" == *@* ]]; then
    printf '%s\n' "KATAN_KIND_IMAGE must be a plain Docker tag, not a digest reference" >&2
    return 2
  fi
}
