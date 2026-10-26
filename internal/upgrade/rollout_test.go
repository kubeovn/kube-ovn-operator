package upgrade

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestPartitionKeepsEveryRenderedObjectInOnePhase(t *testing.T) {
	objects := []*unstructured.Unstructured{
		{Object: map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": "installations.operator.kubeovn.io"}}},
		{Object: map[string]any{"apiVersion": "apps/v1", "kind": "Deployment", "metadata": map[string]any{"name": "ovn-central"}}},
		{Object: map[string]any{"apiVersion": "apps/v1", "kind": "DaemonSet", "metadata": map[string]any{"name": "ovs-ovn"}}},
		{Object: map[string]any{"apiVersion": "v1", "kind": "Service", "metadata": map[string]any{"name": "ovn-central"}}},
	}

	set, err := Partition(objects)
	if err != nil {
		t.Fatal(err)
	}
	if len(set.All) != 4 || len(set.CRDs) != 1 || len(set.Central) != 2 || len(set.DataPlane) != 1 {
		t.Fatalf("unexpected partition: all=%d crds=%d central=%d data=%d", len(set.All), len(set.CRDs), len(set.Central), len(set.DataPlane))
	}
	if got := InitialPhase(set); got != RolloutCRDs {
		t.Fatalf("initial phase = %q, want %q", got, RolloutCRDs)
	}
	if got := NextPhase(RolloutCRDs, set); got != RolloutCentral {
		t.Fatalf("after CRDs = %q, want %q", got, RolloutCentral)
	}
	if got := NextPhase(RolloutCentral, set); got != RolloutDataPlane {
		t.Fatalf("after central = %q, want %q", got, RolloutDataPlane)
	}
	if got := NextPhase(RolloutDataPlane, set); got != RolloutVerify {
		t.Fatalf("after data plane = %q, want %q", got, RolloutVerify)
	}
}

func TestParsePhaseDoesNotSkipUnknownPersistedStep(t *testing.T) {
	set := ObjectSet{Central: []*unstructured.Unstructured{{Object: map[string]any{"kind": "Deployment"}}}, DataPlane: []*unstructured.Unstructured{{Object: map[string]any{"kind": "DaemonSet"}}}}
	if got := ParsePhase("RollCentralBeforeDataPlane", set); got != RolloutCentral {
		t.Fatalf("unknown rollout step = %q, want safe initial central phase", got)
	}
	if got := ParsePhase(string(RolloutDataPlane), set); got != RolloutDataPlane {
		t.Fatalf("persisted data-plane phase = %q", got)
	}
}

func TestPartitionRejectsNilObject(t *testing.T) {
	if _, err := Partition([]*unstructured.Unstructured{nil}); err == nil {
		t.Fatal("nil rendered object was accepted")
	}
}
