/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tenant

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	defaultMaxReplicas  int32 = 10
	defaultTargetCPU    int32 = 70
	defaultTargetMemory int32 = 80
)

// ippWorkloadScaling is the resolved scaling config for one IPP Deployment
// (payload-processing or payload-pre-processing), mirrored from
// MaasTenantConfig.spec.payloadProcessing / payloadPreProcessing.
type ippWorkloadScaling struct {
	autoscaling  bool
	replicas     *int32
	maxReplicas  int32
	targetCPU    int32
	targetMemory int32
	resources    *corev1.ResourceRequirements
}

// ippScaling holds resolved scaling for both gateway-scoped IPP workloads.
type ippScaling struct {
	processing    ippWorkloadScaling
	preProcessing ippWorkloadScaling
}

func resolveIPPScaling(mtc *unstructured.Unstructured, log logr.Logger) ippScaling {
	return ippScaling{
		processing:    resolveIPPWorkloadScaling(mtc, "payloadProcessing", "payload-processing", log),
		preProcessing: resolveIPPWorkloadScaling(mtc, "payloadPreProcessing", "payload-pre-processing", log),
	}
}

func resolveIPPWorkloadScaling(mtc *unstructured.Unstructured, field, logLabel string, log logr.Logger) ippWorkloadScaling {
	cfg := ippWorkloadScaling{
		maxReplicas:  defaultMaxReplicas,
		targetCPU:    defaultTargetCPU,
		targetMemory: defaultTargetMemory,
	}
	spec, found, err := unstructured.NestedMap(mtc.Object, "spec", field)
	if err != nil || !found || spec == nil {
		return cfg
	}

	if v, ok := nestedInt32(spec, "replicas"); ok {
		if v >= 1 {
			cfg.replicas = &v
		} else {
			log.Info("Ignoring "+field+".replicas; must be >= 1", "value", v)
		}
	}

	autoscaling, asFound, asErr := unstructured.NestedMap(spec, "autoscaling")
	if asErr == nil && asFound && autoscaling != nil {
		cfg.autoscaling = true
		log.Info(logLabel + " autoscaling enabled")
		if v, ok := nestedInt32(autoscaling, "maxReplicas"); ok {
			if v >= 1 {
				cfg.maxReplicas = v
			} else {
				log.Info("Ignoring "+field+".autoscaling.maxReplicas; must be >= 1", "value", v)
			}
		}
		if v, ok := nestedInt32(autoscaling, "targetCPUUtilization"); ok {
			if v >= 1 {
				cfg.targetCPU = v
			} else {
				log.Info("Ignoring "+field+".autoscaling.targetCPUUtilization; must be >= 1", "value", v)
			}
		}
		if v, ok := nestedInt32(autoscaling, "targetMemoryUtilization"); ok {
			if v >= 1 {
				cfg.targetMemory = v
			} else {
				log.Info("Ignoring "+field+".autoscaling.targetMemoryUtilization; must be >= 1", "value", v)
			}
		}
	}

	if cfg.autoscaling && cfg.replicas != nil && *cfg.replicas > cfg.maxReplicas {
		log.Info("Clamped "+field+".autoscaling.maxReplicas to match replicas",
			"replicas", *cfg.replicas, "maxReplicas", cfg.maxReplicas)
		cfg.maxReplicas = *cfg.replicas
	}

	resourcesMap, resFound, resErr := unstructured.NestedMap(spec, "resources")
	if resErr == nil && resFound && resourcesMap != nil {
		res, warn := parseWorkloadResources(resourcesMap, cfg.autoscaling, "spec."+field)
		if warn != "" {
			log.Info("Ignoring "+field+".resources override", "reason", warn)
		} else if res != nil {
			cfg.resources = res
			log.Info(logLabel + " resource overrides configured")
		}
	}

	return cfg
}

func nestedInt32(m map[string]any, key string) (int32, bool) {
	v, ok := nestedInt64(m, key)
	if !ok {
		return 0, false
	}
	if v < math.MinInt32 || v > math.MaxInt32 {
		return 0, false
	}
	return int32(v), true
}

func parseWorkloadResources(resourcesMap map[string]any, autoscaling bool, fieldPath string) (*corev1.ResourceRequirements, string) {
	out := &corev1.ResourceRequirements{}
	if requests, ok := resourcesMap["requests"].(map[string]any); ok {
		parsed, err := parseResourceList(requests)
		if err != nil {
			return nil, fieldPath + ".resources.requests: " + err.Error()
		}
		out.Requests = parsed
	}
	if limits, ok := resourcesMap["limits"].(map[string]any); ok {
		parsed, err := parseResourceList(limits)
		if err != nil {
			return nil, fieldPath + ".resources.limits: " + err.Error()
		}
		out.Limits = parsed
	}
	if len(out.Requests) == 0 && len(out.Limits) == 0 {
		return nil, ""
	}
	if autoscaling {
		if out.Requests == nil {
			return nil, fieldPath + ".resources.requests is required when autoscaling is enabled"
		}
		if _, ok := out.Requests[corev1.ResourceCPU]; !ok {
			return nil, fieldPath + ".resources.requests.cpu is required when autoscaling is enabled"
		}
		if _, ok := out.Requests[corev1.ResourceMemory]; !ok {
			return nil, fieldPath + ".resources.requests.memory is required when autoscaling is enabled"
		}
	}
	return out, ""
}

func parseResourceList(m map[string]any) (corev1.ResourceList, error) {
	out := corev1.ResourceList{}
	for k, raw := range m {
		s, ok := raw.(string)
		if !ok {
			switch n := raw.(type) {
			case int64:
				s = strconv.FormatInt(n, 10)
			case float64:
				s = strconv.FormatFloat(n, 'g', -1, 64)
			default:
				return nil, fmt.Errorf("quantity for %q must be a string", k)
			}
		}
		q, err := resource.ParseQuantity(s)
		if err != nil {
			return nil, fmt.Errorf("invalid quantity for %q: %w", k, err)
		}
		out[corev1.ResourceName(k)] = q
	}
	return out, nil
}

// configureIPPScaling patches gateway IPP Deployments and appends HPAs when
// MaasTenantConfig autoscaling is enabled (parity with maas-controller
// tenantreconcile configurePayloadProcessingHPA).
func configureIPPScaling(log logr.Logger, resources *[]unstructured.Unstructured, tenantID, gatewayNamespace string, scaling ippScaling) error {
	if err := patchIPPDeploymentScaling(log, resources, PayloadProcessingDeploymentName(tenantID), PayloadProcessingName, scaling.processing); err != nil {
		return err
	}
	if err := patchIPPDeploymentScaling(log, resources, PayloadPreProcessingDeploymentName(tenantID), PayloadPreProcessingName, scaling.preProcessing); err != nil {
		return err
	}
	if scaling.processing.autoscaling {
		if err := appendIPPWorkloadHPA(log, resources, ippHPAConfig{
			name:         PayloadProcessingHPAName(tenantID),
			deployment:   PayloadProcessingDeploymentName(tenantID),
			namespace:    gatewayNamespace,
			minReplicas:  scaling.processing.replicas,
			maxReplicas:  scaling.processing.maxReplicas,
			targetCPU:    scaling.processing.targetCPU,
			targetMemory: scaling.processing.targetMemory,
			logLabel:     "payload-processing",
		}); err != nil {
			return err
		}
	}
	if scaling.preProcessing.autoscaling {
		if err := appendIPPWorkloadHPA(log, resources, ippHPAConfig{
			name:         PayloadPreProcessingHPAName(tenantID),
			deployment:   PayloadPreProcessingDeploymentName(tenantID),
			namespace:    gatewayNamespace,
			minReplicas:  scaling.preProcessing.replicas,
			maxReplicas:  scaling.preProcessing.maxReplicas,
			targetCPU:    scaling.preProcessing.targetCPU,
			targetMemory: scaling.preProcessing.targetMemory,
			logLabel:     "payload-pre-processing",
		}); err != nil {
			return err
		}
	}
	return nil
}

func patchIPPDeploymentScaling(log logr.Logger, resources *[]unstructured.Unstructured, deploymentName, containerName string, cfg ippWorkloadScaling) error {
	for i := range *resources {
		r := &(*resources)[i]
		if r.GetKind() != "Deployment" || r.GetName() != deploymentName {
			continue
		}
		if cfg.autoscaling {
			unstructured.RemoveNestedField(r.Object, "spec", "replicas")
			log.V(4).Info("Removed spec.replicas (HPA manages replicas)", "deployment", deploymentName)
		} else if cfg.replicas != nil {
			if err := unstructured.SetNestedField(r.Object, int64(*cfg.replicas), "spec", "replicas"); err != nil {
				return fmt.Errorf("set %s replicas: %w", deploymentName, err)
			}
			log.V(4).Info("Patched deployment replicas", "deployment", deploymentName, "replicas", *cfg.replicas)
		}
		if cfg.resources != nil {
			if err := setContainerResources(r, containerName, cfg.resources); err != nil {
				return fmt.Errorf("set %s resources: %w", deploymentName, err)
			}
			log.V(4).Info("Patched deployment resources", "deployment", deploymentName)
		}
		return nil
	}
	return nil
}

type ippHPAConfig struct {
	name         string
	deployment   string
	namespace    string
	minReplicas  *int32
	maxReplicas  int32
	targetCPU    int32
	targetMemory int32
	logLabel     string
}

func appendIPPWorkloadHPA(log logr.Logger, resources *[]unstructured.Unstructured, cfg ippHPAConfig) error {
	minReplicas := int64(1)
	if cfg.minReplicas != nil {
		minReplicas = int64(*cfg.minReplicas)
	}

	hpa := unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "autoscaling/v2",
			"kind":       "HorizontalPodAutoscaler",
			"metadata": map[string]any{
				"name":      cfg.name,
				"namespace": cfg.namespace,
			},
			"spec": map[string]any{
				"scaleTargetRef": map[string]any{
					"apiVersion": "apps/v1",
					"kind":       "Deployment",
					"name":       cfg.deployment,
				},
				"minReplicas": minReplicas,
				"maxReplicas": int64(cfg.maxReplicas),
				"metrics": []any{
					map[string]any{
						"type": "Resource",
						"resource": map[string]any{
							"name": "cpu",
							"target": map[string]any{
								"type":               "Utilization",
								"averageUtilization": int64(cfg.targetCPU),
							},
						},
					},
					map[string]any{
						"type": "Resource",
						"resource": map[string]any{
							"name": "memory",
							"target": map[string]any{
								"type":               "Utilization",
								"averageUtilization": int64(cfg.targetMemory),
							},
						},
					},
				},
				"behavior": map[string]any{
					"scaleDown": map[string]any{
						"stabilizationWindowSeconds": int64(300),
						"policies": []any{
							map[string]any{
								"type":          "Percent",
								"value":         int64(25),
								"periodSeconds": int64(60),
							},
						},
					},
					"scaleUp": map[string]any{
						"stabilizationWindowSeconds": int64(0),
						"policies": []any{
							map[string]any{
								"type":          "Percent",
								"value":         int64(100),
								"periodSeconds": int64(15),
							},
							map[string]any{
								"type":          "Pods",
								"value":         int64(4),
								"periodSeconds": int64(15),
							},
						},
						"selectPolicy": "Max",
					},
				},
			},
		},
	}
	hpa.SetGroupVersionKind(gvkHPA)

	log.V(2).Info("Appending "+cfg.logLabel+" HPA",
		"name", cfg.name,
		"namespace", cfg.namespace,
		"scaleTarget", cfg.deployment,
		"minReplicas", minReplicas,
		"maxReplicas", cfg.maxReplicas,
		"targetCPU", cfg.targetCPU,
		"targetMemory", cfg.targetMemory)
	*resources = append(*resources, hpa)
	return nil
}

func setContainerResources(r *unstructured.Unstructured, containerName string, res *corev1.ResourceRequirements) error {
	containers, found, err := unstructured.NestedSlice(r.Object, "spec", "template", "spec", "containers")
	if err != nil || !found {
		return errors.New("containers not found")
	}
	for i, c := range containers {
		cm, ok := c.(map[string]any)
		if !ok || cm["name"] != containerName {
			continue
		}
		resMap := map[string]any{}
		if res.Requests != nil {
			req := map[string]any{}
			for k, v := range res.Requests {
				req[string(k)] = v.String()
			}
			resMap["requests"] = req
		}
		if res.Limits != nil {
			lim := map[string]any{}
			for k, v := range res.Limits {
				lim[string(k)] = v.String()
			}
			resMap["limits"] = lim
		}
		cm["resources"] = resMap
		containers[i] = cm
		return unstructured.SetNestedSlice(r.Object, containers, "spec", "template", "spec", "containers")
	}
	return fmt.Errorf("container %q not found", containerName)
}

// cleanupOrphanedIPPHPAs deletes HPAs that are no longer desired because
// autoscaling was disabled. SSA never removes absent resources.
func (r *Reconciler) cleanupOrphanedIPPHPAs(ctx context.Context, tenantID, gatewayNamespace string, scaling ippScaling) error {
	if !scaling.processing.autoscaling {
		if err := r.deleteResourceIfOwned(ctx, gvkHPA, PayloadProcessingHPAName(tenantID), gatewayNamespace); err != nil {
			return fmt.Errorf("cleanup payload-processing HPA: %w", err)
		}
	}
	if !scaling.preProcessing.autoscaling {
		if err := r.deleteResourceIfOwned(ctx, gvkHPA, PayloadPreProcessingHPAName(tenantID), gatewayNamespace); err != nil {
			return fmt.Errorf("cleanup payload-pre-processing HPA: %w", err)
		}
	}
	return nil
}
