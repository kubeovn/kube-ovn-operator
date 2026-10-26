package upgrade

import (
	"errors"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// RolloutPhase is the durable order used for a version-only workload update.
// The controller persists the string form in UpgradeStatus.Step so a restart
// cannot skip the central readiness gate or repeat a completed data-plane
// transition as a fresh upgrade.
type RolloutPhase string

const (
	RolloutCRDs      RolloutPhase = "CRDMigration"
	RolloutCentral   RolloutPhase = "CentralRollout"
	RolloutDataPlane RolloutPhase = "DataPlaneRollout"
	RolloutVerify    RolloutPhase = "Verification"
)

// ObjectSet is the immutable partition of one rendered chart. CRDs are
// applied before namespaced workloads, and every DaemonSet is treated as a
// data-plane workload so node agents cannot be updated before central is
// healthy. All other resources are part of the central/control-plane phase.
type ObjectSet struct {
	All       []*unstructured.Unstructured
	CRDs      []*unstructured.Unstructured
	Central   []*unstructured.Unstructured
	DataPlane []*unstructured.Unstructured
}

// Partition groups rendered resources for the upgrade rollout. It rejects a
// nil resource because silently dropping one object would make the later
// readiness observation cover a different manifest from the apply checkpoint.
func Partition(objects []*unstructured.Unstructured) (ObjectSet, error) {
	set := ObjectSet{All: objects}
	for _, object := range objects {
		if object == nil {
			return ObjectSet{}, errors.New("upgrade rollout contains a nil rendered object")
		}
		switch object.GetKind() {
		case "CustomResourceDefinition":
			set.CRDs = append(set.CRDs, object)
		case "DaemonSet":
			set.DataPlane = append(set.DataPlane, object)
		default:
			set.Central = append(set.Central, object)
		}
	}
	return set, nil
}

// InitialPhase returns the first phase that has work. Empty CRD or data-plane
// sets are skipped while preserving the central-before-data-plane ordering.
func InitialPhase(set ObjectSet) RolloutPhase {
	if len(set.CRDs) > 0 {
		return RolloutCRDs
	}
	if len(set.Central) > 0 {
		return RolloutCentral
	}
	if len(set.DataPlane) > 0 {
		return RolloutDataPlane
	}
	return RolloutVerify
}

// NextPhase advances after a phase has passed its readiness gate. The final
// phase is verification, which intentionally has no apply side effect.
func NextPhase(current RolloutPhase, set ObjectSet) RolloutPhase {
	switch current {
	case RolloutCRDs:
		if len(set.Central) > 0 {
			return RolloutCentral
		}
		if len(set.DataPlane) > 0 {
			return RolloutDataPlane
		}
	case RolloutCentral:
		if len(set.DataPlane) > 0 {
			return RolloutDataPlane
		}
	case RolloutDataPlane:
		return RolloutVerify
	case RolloutVerify:
		return RolloutVerify
	}
	return RolloutVerify
}

// ParsePhase recovers a persisted phase. Unknown or empty values are treated
// as a new rollout and therefore start at InitialPhase; they never authorize a
// later phase implicitly.
func ParsePhase(step string, set ObjectSet) RolloutPhase {
	switch RolloutPhase(step) {
	case RolloutCRDs:
		return RolloutCRDs
	case RolloutCentral:
		return RolloutCentral
	case RolloutDataPlane:
		return RolloutDataPlane
	case RolloutVerify:
		return RolloutVerify
	default:
		return InitialPhase(set)
	}
}
