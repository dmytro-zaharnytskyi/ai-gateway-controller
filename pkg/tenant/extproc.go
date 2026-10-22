/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package tenant

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/opendatahub-io/ai-gateway-controller/pkg/envelope"
)

const extprocCredentialDir = "/etc/praxis/credentials" //nolint:gosec // fixed non-secret mount path

type extprocCredential struct {
	name      string
	namespace string
	key       string
	strategy  string
	file      string
}

const extprocConfigHashAnnotation = "ai-gateway-controller.opendatahub.io/extproc-config-sha256"

// configureSharedExtProc renders the shared post-auth chain from the
// authoritative MaasTenantConfig permission. The translation filters are
// restricted to Anthropic Messages requests and do not change the dedicated
// ExternalModel processor.
func configureSharedExtProc(resources []unstructured.Unstructured, tenantID, gatewayNamespace string, apiTranslation bool) error {
	wantCM := PayloadProcessingPluginsConfigMapForTenant(tenantID)
	wantDeploy := PayloadProcessingDeploymentName(tenantID)
	var configMap *unstructured.Unstructured
	var deployment *unstructured.Unstructured
	for i := range resources {
		resource := &resources[i]
		if resource.GetNamespace() != gatewayNamespace {
			continue
		}
		if configMap == nil && resource.GetKind() == "ConfigMap" && resource.GetName() == wantCM {
			configMap = resource
		}
		if deployment == nil && resource.GetKind() == "Deployment" && resource.GetName() == wantDeploy {
			deployment = resource
		}
	}
	if configMap == nil || deployment == nil {
		return errors.New("shared ExtProc resources are missing the plugin ConfigMap or Deployment")
	}
	if err := configureSharedExtProcResponseMode(resources, tenantID, apiTranslation); err != nil {
		return err
	}
	data, found, err := unstructured.NestedStringMap(configMap.Object, "data")
	if err != nil {
		return fmt.Errorf("read shared ExtProc ConfigMap data: %w", err)
	}
	if !found {
		return errors.New("shared ExtProc ConfigMap data is missing")
	}
	config := sharedExtProcConfig(apiTranslation)
	data["extproc.yaml"] = config
	if err := unstructured.SetNestedStringMap(configMap.Object, data, "data"); err != nil {
		return fmt.Errorf("write shared ExtProc ConfigMap data: %w", err)
	}
	configHash := sha256.Sum256([]byte(config))
	if err := unstructured.SetNestedField(deployment.Object, hex.EncodeToString(configHash[:]), "spec", "template", "metadata", "annotations", extprocConfigHashAnnotation); err != nil {
		return fmt.Errorf("write shared ExtProc config hash: %w", err)
	}
	return nil
}

// configureSharedExtProcResponseMode keeps streaming scoped to tenants that
// explicitly enable API translation. The shared inference HTTPRoutes are not
// owned here and do not have a stable controller-provided route identity, so a
// route-level override cannot be safely attached by this reconciler.
func configureSharedExtProcResponseMode(resources []unstructured.Unstructured, tenantID string, apiTranslation bool) error {
	want := "BUFFERED"
	var routeCacheAction string
	if apiTranslation {
		want = "STREAMED"
		routeCacheAction = "RETAIN"
	}
	filterName := PayloadProcessingEnvoyFilterName(tenantID)
	filterFound := false
	patched := 0
	for i := range resources {
		filter := &resources[i]
		if filter.GetKind() != "EnvoyFilter" || filter.GetName() != filterName {
			continue
		}
		filterFound = true
		patches, found, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
		if err != nil {
			return fmt.Errorf("read shared ExtProc EnvoyFilter %q configPatches: %w", filterName, err)
		}
		if !found {
			return fmt.Errorf("shared ExtProc EnvoyFilter %q configPatches are missing", filterName)
		}
		for patchIndex, raw := range patches {
			patch, ok := raw.(map[string]any)
			if !ok || patch["applyTo"] != "HTTP_FILTER" {
				continue
			}
			name, found, err := unstructured.NestedString(patch, "patch", "value", "name")
			if err != nil {
				return fmt.Errorf("read shared ExtProc HTTP_FILTER patch %d name: %w", patchIndex, err)
			}
			if !found || name != "envoy.filters.http.ext_proc.ipp" {
				continue
			}
			if err := unstructured.SetNestedField(patch, want, "patch", "value", "typed_config", "processing_mode", "response_body_mode"); err != nil {
				return fmt.Errorf("set shared ipp response_body_mode in patch %d: %w", patchIndex, err)
			}
			if routeCacheAction != "" {
				if err := unstructured.SetNestedField(patch, routeCacheAction, "patch", "value", "typed_config", "route_cache_action"); err != nil {
					return fmt.Errorf("set shared ipp route_cache_action in patch %d: %w", patchIndex, err)
				}
			} else {
				unstructured.RemoveNestedField(patch, "patch", "value", "typed_config", "route_cache_action")
			}
			patched++
		}
		if err := unstructured.SetNestedSlice(filter.Object, patches, "spec", "configPatches"); err != nil {
			return fmt.Errorf("write shared ExtProc EnvoyFilter %q configPatches: %w", filterName, err)
		}
	}
	if !filterFound || patched == 0 {
		return fmt.Errorf("shared ExtProc EnvoyFilter %q has no ipp HTTP_FILTER patches", filterName)
	}
	return nil
}

// sharedExtProcConfig adds the registered Anthropic-client → Chat
// Completions-backend filters only when the tenant permits translation.
// The ordered chain validates the Messages envelope, translates buffered JSON,
// converts SSE responses, strips the client Anthropic key, then rewrites the
// exact Messages path. The tenant's permission also selects the shared ipp
// response mode: BUFFERED by default and STREAMED when translation is enabled
// so the Messages SSE filter can emit progressive events. Retaining the route
// cache is also scoped to translation-enabled tenants because only that shared
// path rewrites Messages requests. The separate ipp-pre mode and ExternalModel
// processor are unchanged.
func sharedExtProcConfig(apiTranslation bool) string {
	var b strings.Builder
	b.WriteString("server:\n")
	b.WriteString("  grpc_address: \"0.0.0.0:9004\"\n")
	b.WriteString("  health_address: \"0.0.0.0:50052\"\n")
	b.WriteString("  metrics_address: \"0.0.0.0:9090\"\n")
	b.WriteString("  tls:\n")
	b.WriteString("    mode: self_signed\n\n")
	b.WriteString("filter_chains:\n")
	b.WriteString("  - name: ipp\n")
	b.WriteString("    filters:\n")
	b.WriteString("      - filter: request_id\n")
	if apiTranslation {
		b.WriteString("      - filter: anthropic_messages_request\n")
		b.WriteString("        on_invalid: reject\n")
		b.WriteString("        conditions:\n")
		b.WriteString("          - when:\n")
		b.WriteString("              path: \"/v1/messages\"\n")
		b.WriteString("      - filter: anthropic_messages_to_chat_completions\n")
		b.WriteString("        max_body_bytes: 1048576\n")
		b.WriteString("        conditions:\n")
		b.WriteString("          - when:\n")
		b.WriteString("              path: \"/v1/messages\"\n")
		b.WriteString("      - filter: anthropic_messages_to_chat_completions_stream\n")
		b.WriteString("        max_partial_event_bytes: 10485760\n")
		b.WriteString("        max_tool_blocks: 10000\n")
		b.WriteString("        conditions:\n")
		b.WriteString("          - when:\n")
		b.WriteString("              path: \"/v1/messages\"\n")
		b.WriteString("        response_conditions:\n")
		b.WriteString("          - when:\n")
		b.WriteString("              headers:\n")
		b.WriteString("                content-type: \"text/event-stream\"\n")
		b.WriteString("      - filter: headers\n")
		b.WriteString("        request_remove:\n")
		b.WriteString("          - x-api-key\n")
		b.WriteString("        conditions:\n")
		b.WriteString("          - when:\n")
		b.WriteString("              path: \"/v1/messages\"\n")
		b.WriteString("      - filter: path_rewrite\n")
		b.WriteString("        replace:\n")
		b.WriteString("          pattern: \"^/v1/messages$\"\n")
		b.WriteString("          replacement: \"/v1/chat/completions\"\n")
		b.WriteString("        conditions:\n")
		b.WriteString("          - when:\n")
		b.WriteString("              path: \"/v1/messages\"\n")
	}
	b.WriteString("\ninsecure_options:\n")
	b.WriteString("  allow_unbounded_body: true\n")
	return b.String()
}

// configureExternalModelExtProc completes the dedicated ExternalModel ExtProc
// render with tenant-specific routing and credential references. The upstream
// MaaS/KServe post-auth workload is intentionally not modified. Secret values
// are never read: kubelet projects the referenced Secret into this workload and
// credential_inject reads the mounted file at request time.
func configureExternalModelExtProc(resources []unstructured.Unstructured, tenantID, namespace string, candidates []envelope.Candidate, apiTranslationEnabled bool) error {
	credentials, err := extprocCredentials(namespace, candidates)
	if err != nil {
		return err
	}
	config := extprocPostAuthConfig(candidates, credentials, apiTranslationEnabled)

	var configMap *unstructured.Unstructured
	var deployment *unstructured.Unstructured
	for i := range resources {
		resource := &resources[i]
		if configMap == nil && resource.GetNamespace() == namespace && resource.GetKind() == "ConfigMap" && strings.HasPrefix(resource.GetName(), PayloadProcessingExternalModelName+"-plugins") {
			configMap = resource
		}
		if deployment == nil && resource.GetNamespace() == namespace && resource.GetKind() == "Deployment" && strings.HasPrefix(resource.GetName(), PayloadProcessingExternalModelName) {
			deployment = resource
		}
	}
	if configMap == nil || deployment == nil {
		return errors.New("post-auth ExtProc resources are missing the plugin ConfigMap or Deployment")
	}
	data, found, err := unstructured.NestedStringMap(configMap.Object, "data")
	if err != nil {
		return fmt.Errorf("read post-auth ExtProc ConfigMap data: %w", err)
	}
	if !found {
		return errors.New("post-auth ExtProc ConfigMap data is missing")
	}
	data["extproc.yaml"] = config
	if err := unstructured.SetNestedStringMap(configMap.Object, data, "data"); err != nil {
		return fmt.Errorf("write post-auth ExtProc ConfigMap data: %w", err)
	}
	if err := configureExternalModelRequestOrdering(resources, tenantID, apiTranslationEnabled); err != nil {
		return err
	}
	configHash := sha256.Sum256([]byte(config))
	if err := unstructured.SetNestedField(deployment.Object, hex.EncodeToString(configHash[:]), "spec", "template", "metadata", "annotations", extprocConfigHashAnnotation); err != nil {
		return fmt.Errorf("write post-auth ExtProc config hash: %w", err)
	}

	if len(credentials) == 0 {
		return nil
	}
	volumes, _, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "volumes")
	if err != nil {
		return fmt.Errorf("read post-auth ExtProc volumes: %w", err)
	}
	projectedSources := make([]any, 0, len(credentials))
	projectedFiles := map[string]bool{}
	for _, credential := range credentials {
		if projectedFiles[credential.file] {
			continue
		}
		projectedFiles[credential.file] = true
		projectedSources = append(projectedSources, map[string]any{
			"secret": map[string]any{
				"name":  credential.name,
				"items": []any{map[string]any{"key": credential.key, "path": strings.TrimPrefix(credential.file, extprocCredentialDir+"/")}},
			},
		})
	}
	volumes = appendOrReplaceNamedVolume(volumes, "provider-credentials", map[string]any{
		"name":      "provider-credentials",
		"projected": map[string]any{"sources": projectedSources},
	})
	if err := unstructured.SetNestedSlice(deployment.Object, volumes, "spec", "template", "spec", "volumes"); err != nil {
		return fmt.Errorf("write post-auth ExtProc volumes: %w", err)
	}
	containers, found, err := unstructured.NestedSlice(deployment.Object, "spec", "template", "spec", "containers")
	if err != nil || !found || len(containers) == 0 {
		if err == nil {
			err = errors.New("containers are missing")
		}
		return fmt.Errorf("read post-auth ExtProc containers: %w", err)
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return errors.New("post-auth ExtProc first container is malformed")
	}
	mounts, _, err := unstructured.NestedSlice(container, "volumeMounts")
	if err != nil {
		return fmt.Errorf("read post-auth ExtProc volume mounts: %w", err)
	}
	mounts = appendOrReplaceNamedVolume(mounts, "provider-credentials", map[string]any{
		"name": "provider-credentials", "mountPath": extprocCredentialDir, "readOnly": true,
	})
	if err := unstructured.SetNestedSlice(container, mounts, "volumeMounts"); err != nil {
		return fmt.Errorf("write post-auth ExtProc volume mounts: %w", err)
	}
	containers[0] = container
	if err := unstructured.SetNestedSlice(deployment.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		return fmt.Errorf("write post-auth ExtProc containers: %w", err)
	}
	return nil
}

// configureExternalModelRequestOrdering enables early body delivery only for
// translation-enabled tenants. Flag-off tenants retain the main Envoy request
// ordering, including during a translation on-to-off reconciliation.
func configureExternalModelRequestOrdering(resources []unstructured.Unstructured, tenantID string, apiTranslationEnabled bool) error {
	wantFilter := PayloadProcessingExternalModelFilterNameForTenant(tenantID)
	filterFound := false
	patched := 0
	for i := range resources {
		filter := &resources[i]
		if filter.GetKind() != "EnvoyFilter" || filter.GetName() != wantFilter {
			continue
		}
		filterFound = true
		patches, found, err := unstructured.NestedSlice(filter.Object, "spec", "configPatches")
		if err != nil {
			return fmt.Errorf("read ExternalModel EnvoyFilter %q configPatches: %w", wantFilter, err)
		}
		if !found {
			return fmt.Errorf("ExternalModel EnvoyFilter %q configPatches are missing", wantFilter)
		}
		for patchIndex, raw := range patches {
			patch, ok := raw.(map[string]any)
			if !ok || patch["applyTo"] != "HTTP_FILTER" {
				continue
			}
			name, found, err := unstructured.NestedString(patch, "patch", "value", "name")
			if err != nil {
				return fmt.Errorf("read ExternalModel HTTP_FILTER patch %d name: %w", patchIndex, err)
			}
			if !found || name != "envoy.filters.http.ext_proc.external-model" {
				continue
			}
			if apiTranslationEnabled {
				if err := unstructured.SetNestedField(patch, true, "patch", "value", "typed_config", "send_body_without_waiting_for_header_response"); err != nil {
					return fmt.Errorf("enable early ExternalModel body delivery in patch %d: %w", patchIndex, err)
				}
			} else {
				unstructured.RemoveNestedField(patch, "patch", "value", "typed_config", "send_body_without_waiting_for_header_response")
			}
			patched++
		}
		if err := unstructured.SetNestedSlice(filter.Object, patches, "spec", "configPatches"); err != nil {
			return fmt.Errorf("write ExternalModel EnvoyFilter %q configPatches: %w", wantFilter, err)
		}
	}
	if !filterFound || patched == 0 {
		return fmt.Errorf("ExternalModel EnvoyFilter %q has no post-auth HTTP_FILTER patches", wantFilter)
	}
	return nil
}

// extprocCredentials returns deterministic, reference-only file mappings.
func extprocCredentials(namespace string, candidates []envelope.Candidate) ([]extprocCredential, error) {
	seen := map[string]bool{}
	credentials := make([]extprocCredential, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Credential == nil {
			continue
		}
		ref := candidate.Credential.SecretRef
		if ref.Namespace != namespace {
			return nil, fmt.Errorf("candidate %s references cross-namespace Secret %s/%s", candidate.StableID, ref.Namespace, ref.Name)
		}
		if candidate.Credential.Strategy != "bearer_token" && candidate.Credential.Strategy != "apikey" {
			return nil, fmt.Errorf("candidate %s uses unsupported ExtProc credential strategy %q", candidate.StableID, candidate.Credential.Strategy)
		}
		if ref.Name == "" || ref.Key == "" {
			return nil, fmt.Errorf("candidate %s has an incomplete credential reference", candidate.StableID)
		}
		identity := ref.Namespace + "/" + ref.Name + "/" + ref.Key + "/" + candidate.Credential.Strategy
		if seen[identity] {
			continue
		}
		seen[identity] = true
		hash := sha256.Sum256([]byte(ref.Namespace + "/" + ref.Name + "/" + ref.Key))
		path := fmt.Sprintf("%s/%s-%s/%s", extprocCredentialDir, ref.Name, hex.EncodeToString(hash[:])[:12], ref.Key)
		credentials = append(credentials, extprocCredential{name: ref.Name, namespace: ref.Namespace, key: ref.Key, strategy: candidate.Credential.Strategy, file: path})
	}
	sort.Slice(credentials, func(i, j int) bool {
		return credentials[i].name+credentials[i].key+credentials[i].strategy < credentials[j].name+credentials[j].key+credentials[j].strategy
	})
	return credentials, nil
}

func extprocPostAuthConfig(candidates []envelope.Candidate, credentials []extprocCredential, apiTranslationEnabled bool) string {
	clusterSet := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		clusterSet[candidate.Cluster] = true
	}
	clusters := make([]string, 0, len(clusterSet))
	for cluster := range clusterSet {
		clusters = append(clusters, cluster)
	}
	sort.Strings(clusters)
	var b strings.Builder
	b.WriteString("server:\n  grpc_address: \"0.0.0.0:9004\"\n  health_address: \"0.0.0.0:50052\"\n")
	b.WriteString("  metrics_address: \"0.0.0.0:9090\"\n")
	// Bound buffered translation work only when the tenant opts in. With
	// translation disabled, omit the translation and provider-profile filters
	// and retain main's unbounded-body setting.
	if apiTranslationEnabled {
		b.WriteString("  max_body_bytes: 10485760\n")
	}
	b.WriteString("  tls:\n    mode: self_signed\n\n")
	b.WriteString("filter_chains:\n  - name: post-auth\n    filters:\n      - filter: intelligent_route\n")
	b.WriteString("        overlay_file: /etc/praxis/routing/routing-overlay.json\n")
	b.WriteString("        model_header: X-Gateway-Model-Name\n        provider_hop_clusters: [")
	for i, cluster := range clusters {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%q", cluster)
	}
	b.WriteString("]\n        reload: {enabled: true, debounce_ms: 500}\n")
	if apiTranslationEnabled {
		b.WriteString("      - filter: openai_chat_completions_to_anthropic_stream\n")
		b.WriteString("        max_partial_event_bytes: 1048576\n        max_tool_blocks: 256\n")
		b.WriteString("      - filter: openai_chat_completions_to_anthropic\n")
		b.WriteString("        api_translation_enabled: true\n")
		b.WriteString("        max_body_bytes: 1048576\n")
	}
	if len(credentials) > 0 {
		b.WriteString("      - filter: credential_inject\n        credentials:\n")
		for _, credential := range credentials {
			fmt.Fprintf(&b, "          - name: %s\n            namespace: %s\n", credential.name, credential.namespace)
			fmt.Fprintf(&b, "            key: %s\n            strategy: %s\n            file: %s\n", credential.key, credential.strategy, credential.file)
		}
	}
	if !apiTranslationEnabled {
		b.WriteString("\ninsecure_options:\n  allow_unbounded_body: true\n")
	}
	return b.String()
}

func appendOrReplaceNamedVolume(items []any, name string, value map[string]any) []any {
	for i, item := range items {
		m, ok := item.(map[string]any)
		if ok && m["name"] == name {
			items[i] = value
			return items
		}
	}
	return append(items, value)
}
