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
	"testing"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestResolveIPPScalingDefaults(t *testing.T) {
	mtc := NewMaasTenantConfig()
	got := resolveIPPScaling(mtc, logr.Discard())
	if got.processing.autoscaling || got.preProcessing.autoscaling {
		t.Fatalf("autoscaling = true, want false when spec is empty")
	}
	if got.processing.maxReplicas != defaultMaxReplicas || got.processing.targetCPU != defaultTargetCPU {
		t.Fatalf("unexpected defaults: %+v", got.processing)
	}
}

func TestResolveIPPScalingCustomValues(t *testing.T) {
	mtc := NewMaasTenantConfig()
	mtc.Object["spec"] = map[string]any{
		"payloadProcessing": map[string]any{
			"replicas": int64(2),
			"autoscaling": map[string]any{
				"maxReplicas":             int64(5),
				"targetCPUUtilization":    int64(60),
				"targetMemoryUtilization": int64(75),
			},
			"resources": map[string]any{
				"requests": map[string]any{
					"cpu":    "200m",
					"memory": "256Mi",
				},
			},
		},
		"payloadPreProcessing": map[string]any{
			"autoscaling": map[string]any{},
		},
	}
	got := resolveIPPScaling(mtc, logr.Discard())
	if !got.processing.autoscaling || !got.preProcessing.autoscaling {
		t.Fatalf("expected both workloads to enable autoscaling")
	}
	if got.processing.replicas == nil || *got.processing.replicas != 2 {
		t.Fatalf("replicas = %v, want 2", got.processing.replicas)
	}
	if got.processing.maxReplicas != 5 || got.processing.targetCPU != 60 || got.processing.targetMemory != 75 {
		t.Fatalf("unexpected processing scaling: %+v", got.processing)
	}
	if got.processing.resources == nil {
		t.Fatal("expected resources override")
	}
	if got.preProcessing.maxReplicas != defaultMaxReplicas || got.preProcessing.targetCPU != defaultTargetCPU {
		t.Fatalf("pre-processing defaults: %+v", got.preProcessing)
	}
}

func TestResolveIPPScalingClampsMaxReplicas(t *testing.T) {
	mtc := NewMaasTenantConfig()
	mtc.Object["spec"] = map[string]any{
		"payloadProcessing": map[string]any{
			"replicas": int64(12),
			"autoscaling": map[string]any{
				"maxReplicas": int64(4),
			},
		},
	}
	got := resolveIPPScaling(mtc, logr.Discard())
	if got.processing.maxReplicas != 12 {
		t.Fatalf("maxReplicas = %d, want 12 after clamp", got.processing.maxReplicas)
	}
}

func TestResolveIPPScalingIgnoresNonPositiveHPABounds(t *testing.T) {
	mtc := NewMaasTenantConfig()
	mtc.Object["spec"] = map[string]any{
		"payloadProcessing": map[string]any{
			"replicas": int64(0),
			"autoscaling": map[string]any{
				"maxReplicas":             int64(0),
				"targetCPUUtilization":    int64(-1),
				"targetMemoryUtilization": int64(0),
			},
		},
	}
	got := resolveIPPScaling(mtc, logr.Discard())
	if !got.processing.autoscaling {
		t.Fatal("expected autoscaling enabled when autoscaling map is present")
	}
	if got.processing.replicas != nil {
		t.Fatalf("replicas = %v, want nil after rejecting < 1", got.processing.replicas)
	}
	if got.processing.maxReplicas != defaultMaxReplicas {
		t.Fatalf("maxReplicas = %d, want default %d", got.processing.maxReplicas, defaultMaxReplicas)
	}
	if got.processing.targetCPU != defaultTargetCPU || got.processing.targetMemory != defaultTargetMemory {
		t.Fatalf("targets = %d/%d, want defaults %d/%d",
			got.processing.targetCPU, got.processing.targetMemory, defaultTargetCPU, defaultTargetMemory)
	}
}

func TestConfigureIPPScalingAppendsHPAAndStripsReplicas(t *testing.T) {
	resources := []unstructured.Unstructured{
		{
			Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "payload-processing", "namespace": "openshift-ingress"},
				"spec":       map[string]any{"replicas": int64(1)},
			},
		},
		{
			Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "payload-pre-processing", "namespace": "openshift-ingress"},
				"spec":       map[string]any{"replicas": int64(1)},
			},
		},
	}
	replicas := int32(2)
	scaling := ippScaling{
		processing: ippWorkloadScaling{
			autoscaling:  true,
			replicas:     &replicas,
			maxReplicas:  8,
			targetCPU:    65,
			targetMemory: 85,
		},
		preProcessing: ippWorkloadScaling{autoscaling: true, maxReplicas: 10, targetCPU: 70, targetMemory: 80},
	}
	if err := configureIPPScaling(logr.Discard(), &resources, "", "openshift-ingress", scaling); err != nil {
		t.Fatalf("configureIPPScaling: %v", err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(resources[0].Object, "spec", "replicas"); found {
		t.Fatal("payload-processing still has spec.replicas")
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(resources[1].Object, "spec", "replicas"); found {
		t.Fatal("payload-pre-processing still has spec.replicas")
	}
	if len(resources) != 4 {
		t.Fatalf("resources len = %d, want 4 (2 deployments + 2 HPAs)", len(resources))
	}
	hpa := resources[2]
	if hpa.GetKind() != "HorizontalPodAutoscaler" || hpa.GetName() != "payload-processing" {
		t.Fatalf("unexpected HPA: kind=%s name=%s", hpa.GetKind(), hpa.GetName())
	}
	minR, _, _ := unstructured.NestedInt64(hpa.Object, "spec", "minReplicas")
	maxR, _, _ := unstructured.NestedInt64(hpa.Object, "spec", "maxReplicas")
	if minR != 2 || maxR != 8 {
		t.Fatalf("min/max = %d/%d, want 2/8", minR, maxR)
	}
	target, _, _ := unstructured.NestedString(hpa.Object, "spec", "scaleTargetRef", "name")
	if target != "payload-processing" {
		t.Fatalf("scaleTargetRef.name = %q", target)
	}
}

func TestConfigureIPPScalingSetsStaticReplicasWhenAutoscalingDisabled(t *testing.T) {
	resources := []unstructured.Unstructured{
		{
			Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "payload-processing-redteam"},
				"spec":       map[string]any{"replicas": int64(1)},
			},
		},
	}
	replicas := int32(3)
	scaling := ippScaling{
		processing: ippWorkloadScaling{replicas: &replicas, maxReplicas: 10, targetCPU: 70, targetMemory: 80},
	}
	if err := configureIPPScaling(logr.Discard(), &resources, "redteam", "gw", scaling); err != nil {
		t.Fatalf("configureIPPScaling: %v", err)
	}
	got, _, _ := unstructured.NestedInt64(resources[0].Object, "spec", "replicas")
	if got != 3 {
		t.Fatalf("replicas = %d, want 3", got)
	}
	if len(resources) != 1 {
		t.Fatalf("resources len = %d, want 1 (no HPA)", len(resources))
	}
}

func TestConfigureIPPScalingTenantSuffixedNames(t *testing.T) {
	resources := []unstructured.Unstructured{
		{
			Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata":   map[string]any{"name": "payload-processing-redteam"},
				"spec":       map[string]any{},
			},
		},
	}
	scaling := ippScaling{
		processing: ippWorkloadScaling{autoscaling: true, maxReplicas: 10, targetCPU: 70, targetMemory: 80},
	}
	if err := configureIPPScaling(logr.Discard(), &resources, "redteam", "gw", scaling); err != nil {
		t.Fatalf("configureIPPScaling: %v", err)
	}
	hpa := resources[len(resources)-1]
	if hpa.GetName() != "payload-processing-redteam" {
		t.Fatalf("HPA name = %q", hpa.GetName())
	}
	target, _, _ := unstructured.NestedString(hpa.Object, "spec", "scaleTargetRef", "name")
	if target != "payload-processing-redteam" {
		t.Fatalf("scaleTargetRef.name = %q", target)
	}
}
