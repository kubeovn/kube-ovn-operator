// Package ovnadmin owns the side-effect boundary for OVNDB membership changes.
package ovnadmin

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

type Snapshot struct {
	ClusterID      string
	Database       string
	LeaderServerID string
	Members        []membership.Member
	// CommitIndex is the leader's committed fence at the start of sampling.
	// Healthy members must have applied at least that index in the same term.
	Term         int64
	CommitIndex  int64
	AppliedIndex int64
}

// Client keeps mutation private to this package so callers go through Executor.
// Transport implementations revalidate the snapshot immediately before dispatch.
type Client interface {
	Status(context.Context, string, string) (Snapshot, error)
	mutate(context.Context, string, Snapshot, membership.Change, func(context.Context, Snapshot) error) error
}

// BackupVerifier must read and verify an actual backup for the observed database
// identity and fence. A nonempty reference alone is never sufficient evidence.
type BackupVerifier interface {
	Verify(context.Context, Snapshot, string) error
}

// LossVerifier checks a durable, explicit approval bound to this exact cluster,
// member UUID and address. An arbitrary reason or unhealthy Pod is not approval.
type LossVerifier interface {
	VerifyPermanentLoss(context.Context, Snapshot, membership.Change) error
}

type Executor struct {
	Client        Client
	Database      string
	Backups       BackupVerifier
	PermanentLoss LossVerifier
}

func (e Executor) Execute(ctx context.Context, endpoint string, change membership.Change) error {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if e.Client == nil {
		return fmt.Errorf("OVNDB admin client is not configured")
	}
	database := e.Database
	if database == "" {
		database = "OVN_Northbound"
	}
	if _, err := clusterCommand(database, "cluster/status"); err != nil {
		return err
	}
	if err := lease.Check(ctx, lease.Central); err != nil {
		return fmt.Errorf("guard membership observation: %w", err)
	}
	snapshot, err := e.Client.Status(ctx, database, endpoint)
	if err != nil {
		return fmt.Errorf("read OVNDB status: %w", err)
	}
	if snapshot.Database != database {
		return fmt.Errorf("observed database differs from requested database")
	}
	if err := validateChange(snapshot, change); err != nil {
		return err
	}
	return e.Client.mutate(ctx, endpoint, snapshot, change, func(ctx context.Context, current Snapshot) error {
		// Verify providers against the transport's final observation, not the
		// earlier planning fence, which may have advanced while collecting peers.
		if err := validateChange(current, change); err != nil {
			return err
		}
		if e.Backups == nil || strings.TrimSpace(change.BackupRef) == "" {
			return fmt.Errorf("membership change requires a verified backup")
		}
		if err := e.Backups.Verify(ctx, current, change.BackupRef); err != nil {
			return fmt.Errorf("verify membership backup: %w", err)
		}
		if change.Operation == membership.OperationKick {
			if e.PermanentLoss == nil {
				return fmt.Errorf("kick requires a permanent-loss approval verifier")
			}
			if err := e.PermanentLoss.VerifyPermanentLoss(ctx, current, change); err != nil {
				return fmt.Errorf("verify permanent loss: %w", err)
			}
		}
		if err := lease.Check(ctx, lease.Central); err != nil {
			return fmt.Errorf("guard membership change: %w", err)
		}
		return nil
	})
}

func validateChange(snapshot Snapshot, change membership.Change) error {
	if !membership.ValidID(snapshot.ClusterID) || snapshot.ClusterID != change.ClusterID {
		return fmt.Errorf("membership change is not bound to the observed cluster UUID")
	}
	if err := membership.ValidateMembers(snapshot.Members); err != nil {
		return err
	}
	if snapshot.Term < 1 || snapshot.CommitIndex < 1 || snapshot.AppliedIndex != snapshot.CommitIndex {
		return fmt.Errorf("leader has not applied a consistent positive committed fence")
	}
	healthy, leaders := 0, 0
	for _, member := range snapshot.Members {
		if member.Healthy {
			healthy++
		}
		if member.Leader {
			leaders++
			if !member.Healthy || member.ServerID != snapshot.LeaderServerID {
				return fmt.Errorf("leader identity or health is inconsistent")
			}
		}
	}
	if leaders != 1 || healthy < len(snapshot.Members)/2+1 {
		return fmt.Errorf("membership change requires a healthy leader and quorum")
	}
	if err := membership.ValidateMembers([]membership.Member{change.Member}); err != nil {
		return fmt.Errorf("change target: %w", err)
	}
	if strings.TrimSpace(change.Reason) == "" {
		return fmt.Errorf("membership change requires an auditable reason")
	}
	if change.Operation == membership.OperationJoin {
		for _, member := range snapshot.Members {
			if member.ServerID == change.Member.ServerID || member.Address == change.Member.Address {
				return fmt.Errorf("join would duplicate a member identity or address")
			}
		}
		return nil
	}
	if change.Operation != membership.OperationLeave && change.Operation != membership.OperationKick {
		return fmt.Errorf("unsupported membership operation %q", change.Operation)
	}
	var target *membership.Member
	for i := range snapshot.Members {
		if snapshot.Members[i].ServerID == change.Member.ServerID {
			target = &snapshot.Members[i]
			break
		}
	}
	if target == nil || target.Address != change.Member.Address {
		return fmt.Errorf("removal target UUID and address do not match live membership")
	}
	if change.Operation == membership.OperationLeave && !target.Healthy {
		return fmt.Errorf("graceful leave requires a healthy target")
	}
	if change.Operation == membership.OperationKick && (target.Healthy || !change.PermanentLossApproved) {
		return fmt.Errorf("kick requires an unhealthy member and explicit permanent-loss approval")
	}
	remaining := len(snapshot.Members) - 1
	if target.Healthy {
		healthy--
	}
	if remaining < 1 || healthy < remaining/2+1 {
		return fmt.Errorf("removal would lose quorum or remove the last member")
	}
	return nil
}
