// Package membership contains the safety-critical, side-effect-free part of
// OVNDB membership reconciliation. The admin adapter is the only component
// allowed to execute the returned operation.
package membership

import "fmt"

type Member struct {
	ServerID string
	Address  string
	Healthy  bool
	Leader   bool
}

type Snapshot struct {
	ClusterID     string
	Members       []Member
	CommittedData bool
}

type Operation string

const (
	OperationJoin  Operation = "join"
	OperationLeave Operation = "leave"
	OperationKick  Operation = "kick"
)

type Change struct {
	Operation Operation
	Member    Member
	Reason    string
}

type Plan struct {
	Changes         []Change
	ScaleWorkloadTo int
}

// BuildPlan enforces the ordering required by clustered OVN databases. It
// refuses to produce a destructive change when quorum or data ownership is not
// proven. A caller must execute the first change and re-read Snapshot before
// applying the next one.
func BuildPlan(snapshot Snapshot, desiredReplicas int, replacement Member, removeServerID string, permanentLoss bool) (Plan, error) {
	if snapshot.ClusterID == "" {
		return Plan{}, fmt.Errorf("cluster ID is unknown")
	}
	if desiredReplicas != 1 && desiredReplicas != 3 && desiredReplicas != 5 {
		return Plan{}, fmt.Errorf("desired replicas must be 1, 3, or 5")
	}
	if len(snapshot.Members) == 0 {
		return Plan{}, fmt.Errorf("membership snapshot is empty")
	}
	healthy := 0
	for _, member := range snapshot.Members {
		if member.Healthy {
			healthy++
		}
	}
	majority := len(snapshot.Members)/2 + 1
	if healthy < majority {
		return Plan{}, fmt.Errorf("quorum is not proven: %d healthy members, need %d", healthy, majority)
	}
	if !snapshot.CommittedData {
		return Plan{}, fmt.Errorf("committed data is not proven")
	}
	plan := Plan{ScaleWorkloadTo: desiredReplicas}
	if desiredReplicas > len(snapshot.Members) {
		if replacement.ServerID == "" || replacement.Address == "" {
			return Plan{}, fmt.Errorf("replacement server ID and address are required before joining")
		}
		plan.Changes = []Change{{Operation: OperationJoin, Member: replacement, Reason: "join replacement and wait for committed data before scaling workload"}}
		return plan, nil
	}
	if desiredReplicas < len(snapshot.Members) {
		if removeServerID == "" {
			return Plan{}, fmt.Errorf("server ID is required before removing a member")
		}
		var target *Member
		for index := range snapshot.Members {
			if snapshot.Members[index].ServerID == removeServerID {
				target = &snapshot.Members[index]
				break
			}
		}
		if target == nil {
			return Plan{}, fmt.Errorf("server ID %q is not present", removeServerID)
		}
		operation := OperationLeave
		reason := "leave healthy member before scaling workload"
		if !target.Healthy {
			if !permanentLoss {
				return Plan{}, fmt.Errorf("unhealthy member %q requires explicit permanent-loss approval", removeServerID)
			}
			operation = OperationKick
			reason = "kick confirmed permanently lost member with quorum"
		}
		plan.Changes = []Change{{Operation: operation, Member: *target, Reason: reason}}
	}
	return plan, nil
}
