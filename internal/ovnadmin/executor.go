// Package ovnadmin defines the narrow side-effect boundary for OVNDB
// membership operations. A concrete transport can use libovsdb or an
// authenticated admin sidecar; the executor keeps safety checks in one place.
package ovnadmin

import (
	"context"
	"fmt"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

type Snapshot struct {
	ClusterID      string
	Database       string
	LeaderServerID string
	Members        []membership.Member
	CommitIndex    int64
	AppliedIndex   int64
}

type Client interface {
	Status(ctx context.Context, database, endpoint string) (Snapshot, error)
	Join(ctx context.Context, database, endpoint string, member membership.Member) error
	Leave(ctx context.Context, database, endpoint, serverID string) error
	Kick(ctx context.Context, database, endpoint, serverID string) error
}

type Executor struct {
	Client   Client
	Database string
}

func (e Executor) Execute(ctx context.Context, endpoint string, change membership.Change) error {
	if e.Client == nil {
		return fmt.Errorf("OVNDB admin client is not configured")
	}
	database := e.Database
	if database == "" {
		database = "northbound"
	}
	snapshot, err := e.Client.Status(ctx, database, endpoint)
	if err != nil {
		return fmt.Errorf("read OVNDB status: %w", err)
	}
	if snapshot.ClusterID == "" {
		return fmt.Errorf("refuse membership change without cluster ID")
	}
	if snapshot.LeaderServerID == "" {
		return fmt.Errorf("refuse membership change without a visible leader")
	}
	if snapshot.CommitIndex < snapshot.AppliedIndex {
		return fmt.Errorf("refuse membership change with applied index %d ahead of commit index %d", snapshot.AppliedIndex, snapshot.CommitIndex)
	}
	healthy := 0
	leaderHealthy := false
	for _, member := range snapshot.Members {
		if member.Healthy {
			healthy++
		}
		if member.ServerID == snapshot.LeaderServerID && member.Healthy {
			leaderHealthy = true
		}
	}
	if healthy < len(snapshot.Members)/2+1 {
		return fmt.Errorf("refuse membership change without quorum: %d of %d healthy", healthy, len(snapshot.Members))
	}
	if !leaderHealthy {
		return fmt.Errorf("refuse membership change with unhealthy or unknown leader %q", snapshot.LeaderServerID)
	}
	switch change.Operation {
	case membership.OperationJoin:
		if change.Member.ServerID == "" || change.Member.Address == "" {
			return fmt.Errorf("join requires server ID and address")
		}
		for _, member := range snapshot.Members {
			if member.ServerID == change.Member.ServerID || member.Address == change.Member.Address {
				return fmt.Errorf("join would duplicate existing member identity %q/%q", change.Member.ServerID, change.Member.Address)
			}
		}
		return e.Client.Join(ctx, database, endpoint, change.Member)
	case membership.OperationLeave:
		if change.Member.ServerID == "" {
			return fmt.Errorf("leave requires server ID")
		}
		if err := validateRemoval(snapshot, change.Member.ServerID); err != nil {
			return err
		}
		return e.Client.Leave(ctx, database, endpoint, change.Member.ServerID)
	case membership.OperationKick:
		if change.Member.ServerID == "" {
			return fmt.Errorf("kick requires server ID")
		}
		if change.Reason == "" {
			return fmt.Errorf("kick requires an auditable reason")
		}
		if err := validateRemoval(snapshot, change.Member.ServerID); err != nil {
			return err
		}
		return e.Client.Kick(ctx, database, endpoint, change.Member.ServerID)
	default:
		return fmt.Errorf("unsupported membership operation %q", change.Operation)
	}
}

func validateRemoval(snapshot Snapshot, serverID string) error {
	remaining := len(snapshot.Members) - 1
	if remaining < 1 {
		return fmt.Errorf("refuse removing the last OVNDB member")
	}
	removedHealthy := false
	found := false
	for _, member := range snapshot.Members {
		if member.ServerID == serverID {
			found = true
			removedHealthy = member.Healthy
			break
		}
	}
	if !found {
		return fmt.Errorf("server ID %q is not present", serverID)
	}
	healthy := 0
	for _, member := range snapshot.Members {
		if member.Healthy {
			healthy++
		}
	}
	if removedHealthy {
		healthy--
	}
	if healthy < remaining/2+1 {
		return fmt.Errorf("refuse removal: remaining members would lose quorum (%d healthy of %d)", healthy, remaining)
	}
	return nil
}
