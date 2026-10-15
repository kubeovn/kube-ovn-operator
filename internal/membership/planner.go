// Package membership contains the safety-critical, side-effect-free part of
// OVNDB membership reconciliation. The admin adapter is the only component
// allowed to execute the returned operation.
package membership

import (
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"uuid"
)

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
	Operation             Operation
	ClusterID             string
	Member                Member
	Reason                string
	BackupRef             string
	PermanentLossApproved bool
	OperationID           string
	ApprovalRef           ApprovalReference
}

// ApprovalReference pins a durable approval's UID, not only its reusable name.
type ApprovalReference struct {
	Name string
	UID  string
}

type Plan struct {
	Changes         []Change
	ScaleWorkloadTo int
	ReadyToScale    bool
}

// BuildPlan enforces the ordering required by clustered OVN databases. It
// refuses to produce a destructive change when quorum or data ownership is not
// proven. A caller must execute the first change and re-read Snapshot before
// applying the next one.
func BuildPlan(snapshot Snapshot, desiredReplicas int, replacement Member, removeServerID string, permanentLoss bool) (Plan, error) {
	if !ValidID(snapshot.ClusterID) {
		return Plan{}, fmt.Errorf("a full cluster UUID is required")
	}
	if desiredReplicas != 1 && desiredReplicas != 3 && desiredReplicas != 5 {
		return Plan{}, fmt.Errorf("desired replicas must be 1, 3, or 5")
	}
	if err := ValidateMembers(snapshot.Members); err != nil {
		return Plan{}, err
	}
	healthy := 0
	leaders := 0
	for _, member := range snapshot.Members {
		if member.Healthy {
			healthy++
		}
		if member.Leader {
			leaders++
			if !member.Healthy {
				return Plan{}, fmt.Errorf("leader is not healthy")
			}
		}
	}
	majority := len(snapshot.Members)/2 + 1
	if healthy < majority || leaders != 1 {
		return Plan{}, fmt.Errorf("quorum is not proven: %d healthy members, need %d", healthy, majority)
	}
	if !snapshot.CommittedData {
		return Plan{}, fmt.Errorf("committed data is not proven")
	}
	plan := Plan{}
	var joined *Member
	if replacement.ServerID != "" || replacement.Address != "" {
		if err := ValidateMembers([]Member{replacement}); err != nil {
			return Plan{}, fmt.Errorf("replacement: %w", err)
		}
		if replacement.ServerID == removeServerID {
			return Plan{}, fmt.Errorf("replacement must have a new server identity")
		}
		for i, member := range snapshot.Members {
			if member.ServerID == replacement.ServerID {
				if member.Address != replacement.Address {
					return Plan{}, fmt.Errorf("replacement address differs from live member")
				}
				joined = &snapshot.Members[i]
			} else if member.Address == replacement.Address {
				return Plan{}, fmt.Errorf("replacement address belongs to another member")
			}
		}
	}
	if joined == nil && (desiredReplicas > len(snapshot.Members) || (replacement.ServerID != "" && removeServerID != "")) {
		if replacement.ServerID == "" {
			return Plan{}, fmt.Errorf("replacement server ID and address are required before joining")
		}
		if removeServerID != "" && !containsMember(snapshot.Members, removeServerID) {
			return Plan{}, fmt.Errorf("replacement target is not present")
		}
		plan.Changes = []Change{{Operation: OperationJoin, ClusterID: snapshot.ClusterID, Member: replacement, Reason: "join replacement and wait for committed data before scaling workload"}}
		return plan, nil
	}
	if replacement.ServerID != "" && joined == nil {
		return Plan{}, fmt.Errorf("replacement requires a removal target or a larger desired size")
	}
	if joined != nil && !joined.Healthy {
		return Plan{}, fmt.Errorf("replacement has not caught up")
	}
	if containsMember(snapshot.Members, removeServerID) && len(snapshot.Members) <= desiredReplicas {
		return Plan{}, fmt.Errorf("removal requires a replacement above the desired member count")
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
		remainingHealthy := healthy
		if target.Healthy {
			remainingHealthy--
		}
		if len(snapshot.Members) < 2 || remainingHealthy < (len(snapshot.Members)-1)/2+1 {
			return Plan{}, fmt.Errorf("removal would leave insufficient healthy members")
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
		plan.Changes = []Change{{Operation: operation, ClusterID: snapshot.ClusterID, Member: *target, Reason: reason, PermanentLossApproved: operation == OperationKick && permanentLoss}}
		return plan, nil
	}
	if len(snapshot.Members) != desiredReplicas || healthy != desiredReplicas {
		return Plan{}, fmt.Errorf("membership has not converged to the healthy desired size")
	}
	plan.ScaleWorkloadTo, plan.ReadyToScale = desiredReplicas, true
	return plan, nil
}

func containsMember(members []Member, id string) bool {
	for _, member := range members {
		if member.ServerID == id {
			return true
		}
	}
	return false
}

// ValidID rejects shortened IDs, alternate spellings and the zero UUID.
func ValidID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != (uuid.UUID{}) && id.String() == value
}

// ValidateMembers requires exact identities and unique literal-IP RAFT addresses. Health
// is evaluated separately, against live observations and a committed-data fence.
func ValidateMembers(members []Member) error {
	if len(members) == 0 {
		return fmt.Errorf("membership snapshot is empty")
	}
	ids, addresses := map[string]bool{}, map[string]bool{}
	for _, member := range members {
		protocol, address, ok := strings.Cut(member.Address, ":")
		host, port, err := net.SplitHostPort(address)
		number, portErr := strconv.ParseUint(port, 10, 16)
		ip, ipErr := netip.ParseAddr(host)
		if !ValidID(member.ServerID) || !ok || (protocol != "tcp" && protocol != "ssl") || err != nil || ipErr != nil || ip.IsUnspecified() || ip.IsMulticast() || ip.Is4In6() || ip.String() != host || portErr != nil || number == 0 || strconv.FormatUint(number, 10) != port {
			return fmt.Errorf("member requires a full server UUID and a valid RAFT address")
		}
		if ids[member.ServerID] || addresses[member.Address] {
			return fmt.Errorf("duplicate server identity or RAFT address")
		}
		ids[member.ServerID], addresses[member.Address] = true, true
	}
	return nil
}
