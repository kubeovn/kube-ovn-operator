package ovnadmin

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

// Runner resolves an inventory endpoint to a pinned process identity. Pod
// transports must validate ownership, Pod UID and container identity on each call.
type Runner interface {
	Run(context.Context, string, []string) ([]byte, error)
}

var ErrTargetIdentity = errors.New("member process identity changed")

type unavailableError struct{ error }

type Endpoint struct {
	Name     string
	ServerID string
	Address  string
}

// CommandClient executes fixed unixctl commands against pinned full identities.
// Endpoint inventory must come from an observed and persisted topology, including
// unreachable members. Never derive a full server ID from a four-character prefix.
type CommandClient struct {
	Runner    Runner
	ClusterID string
	Endpoints []Endpoint
}

var _ Client = (*CommandClient)(nil)

func (c *CommandClient) Status(ctx context.Context, database, endpoint string) (Snapshot, error) {
	if c.Runner == nil || !membership.ValidID(c.ClusterID) {
		return Snapshot{}, fmt.Errorf("membership transport requires a runner and pinned cluster UUID")
	}
	var members []membership.Member
	names := map[string]bool{}
	selected := -1
	for i, target := range c.Endpoints {
		if target.Name == "" || names[target.Name] {
			return Snapshot{}, fmt.Errorf("endpoint names must be unique and nonempty")
		}
		names[target.Name] = true
		members = append(members, membership.Member{ServerID: target.ServerID, Address: target.Address})
		if target.Name == endpoint {
			selected = i
		}
	}
	if err := membership.ValidateMembers(members); err != nil {
		return Snapshot{}, err
	}
	if selected < 0 {
		return Snapshot{}, fmt.Errorf("requested endpoint is not pinned")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	leader, err := c.read(ctx, database, c.Endpoints[selected])
	if err != nil {
		return Snapshot{}, err
	}
	if leader.State != "cluster member" || leader.Role != "leader" || leader.Leader != "self" || leader.CommitIndex < 1 || leader.AppliedIndex < leader.CommitIndex {
		return Snapshot{}, fmt.Errorf("requested endpoint is not a caught-up cluster leader")
	}
	configured, err := c.resolve(leader)
	if err != nil {
		return Snapshot{}, err
	}
	result := Snapshot{Database: database, ClusterID: c.ClusterID, LeaderServerID: leader.ServerID, Term: leader.Term, CommitIndex: leader.CommitIndex, AppliedIndex: leader.AppliedIndex}
	leaderName := nickname(leader, leader.ServerID, leader.Address)
	for _, target := range configured {
		member := membership.Member{ServerID: target.ServerID, Address: target.Address, Leader: target.ServerID == leader.ServerID}
		if member.Leader {
			member.Healthy = true
		} else {
			peer, err := c.read(ctx, database, target)
			if err != nil {
				if _, unavailable := errors.AsType[unavailableError](err); !unavailable {
					return Snapshot{}, err
				}
			}
			if err == nil {
				if peer.Term != leader.Term || peer.Role == "leader" || peer.Role == "candidate" {
					return Snapshot{}, fmt.Errorf("observed conflicting terms or unsettled leadership")
				}
				peerMembers, resolveErr := c.resolve(peer)
				if resolveErr != nil {
					return Snapshot{}, resolveErr
				}
				if !sameEndpoints(configured, peerMembers) {
					return Snapshot{}, fmt.Errorf("member configurations disagree")
				}
				peerName := nickname(leader, target.ServerID, target.Address)
				peerLeaderName := nickname(peer, leader.ServerID, leader.Address)
				member.Healthy = peer.State == "cluster member" && peer.Role == "follower" && peer.Term == leader.Term && peer.Leader == peerLeaderName && peer.AppliedIndex >= leader.CommitIndex && leader.Connections[peerName] && peer.Connections[peerLeaderName]
			}
		}
		result.Members = append(result.Members, member)
	}
	// Recheck the leader after sampling. A leadership or membership change makes
	// the entire observation unusable, even if individual samples looked healthy.
	after, err := c.read(ctx, database, c.Endpoints[selected])
	if err != nil {
		return Snapshot{}, err
	}
	afterMembers, err := c.resolve(after)
	if err != nil {
		return Snapshot{}, err
	}
	if after.Role != "leader" || after.Leader != "self" || after.State != "cluster member" || after.Term != leader.Term || after.CommitIndex < leader.CommitIndex || after.AppliedIndex < leader.CommitIndex || !sameEndpoints(configured, afterMembers) || nickname(after, leader.ServerID, leader.Address) != leaderName {
		return Snapshot{}, fmt.Errorf("leader or membership changed during observation")
	}
	for i := range result.Members {
		member := &result.Members[i]
		if !member.Leader && !after.Connections[nickname(after, member.ServerID, member.Address)] {
			member.Healthy = false
		}
	}
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	return result, nil
}

func (c *CommandClient) read(ctx context.Context, database string, target Endpoint) (ClusterStatus, error) {
	command, err := clusterCommand(database, "cluster/status")
	if err != nil {
		return ClusterStatus{}, err
	}
	data, err := c.Runner.Run(ctx, target.Name, command)
	if err != nil {
		if ctx.Err() != nil {
			return ClusterStatus{}, ctx.Err()
		}
		if errors.Is(err, ErrTargetIdentity) {
			return ClusterStatus{}, err
		}
		return ClusterStatus{}, unavailableError{fmt.Errorf("read member endpoint %s: %w", target.Name, err)}
	}
	status, err := ParseClusterStatus(data, database)
	if err != nil {
		return ClusterStatus{}, err
	}
	if status.ClusterID != c.ClusterID || status.ServerID != target.ServerID || status.Address != target.Address {
		return ClusterStatus{}, fmt.Errorf("member endpoint identity differs from pinned topology")
	}
	return status, nil
}

func (c *CommandClient) resolve(status ClusterStatus) ([]Endpoint, error) {
	result := make([]Endpoint, 0, len(status.Servers))
	for _, server := range status.Servers {
		matches := 0
		for _, target := range c.Endpoints {
			if target.ServerID[:4] == server.ShortID && target.Address == server.Address {
				result = append(result, target)
				matches++
			}
		}
		if matches != 1 {
			return nil, fmt.Errorf("member configuration cannot be resolved to exactly one pinned full identity")
		}
	}
	slices.SortFunc(result, func(a, b Endpoint) int { return cmp.Compare(a.ServerID, b.ServerID) })
	return result, nil
}

func sameEndpoints(a, b []Endpoint) bool { return slices.Equal(a, b) }

func nickname(status ClusterStatus, serverID, address string) string {
	for _, server := range status.Servers {
		if server.ShortID == serverID[:4] && server.Address == address {
			return server.Nickname
		}
	}
	return ""
}

func (c *CommandClient) mutate(ctx context.Context, endpoint string, expected Snapshot, change membership.Change, authorize func(context.Context, Snapshot) (authorization, error)) error {
	if change.Operation == membership.OperationJoin {
		return fmt.Errorf("join requires a provisioned new database and workload transition; unixctl cannot join a running server")
	}
	current, err := c.Status(ctx, expected.Database, endpoint)
	if err != nil {
		return err
	}
	if current.ClusterID != expected.ClusterID || current.Term != expected.Term || current.LeaderServerID != expected.LeaderServerID || current.CommitIndex < expected.CommitIndex || !sameMembership(current.Members, expected.Members) {
		return fmt.Errorf("membership precondition changed before dispatch")
	}
	if err := validateChange(current, change); err != nil {
		return err
	}
	command, err := clusterCommand(current.Database, "cluster/"+string(change.Operation))
	if err != nil {
		return err
	}
	targetEndpoint := endpoint
	if change.Operation == membership.OperationLeave {
		targetEndpoint = ""
		for _, target := range c.Endpoints {
			if target.ServerID == change.Member.ServerID && target.Address == change.Member.Address {
				targetEndpoint = target.Name
				break
			}
		}
		if targetEndpoint == "" {
			return fmt.Errorf("leave endpoint is not pinned")
		}
		for _, target := range c.Endpoints {
			if target.Name != targetEndpoint {
				continue
			}
			local, err := c.read(ctx, current.Database, target)
			if err != nil {
				return err
			}
			configured, err := c.resolve(local)
			if err != nil {
				return err
			}
			if local.State != "cluster member" || local.Term != current.Term || local.AppliedIndex < current.CommitIndex || len(configured) != len(current.Members) {
				return fmt.Errorf("leave target changed before dispatch")
			}
			for i, member := range current.Members {
				if configured[i].ServerID != member.ServerID || configured[i].Address != member.Address {
					return fmt.Errorf("leave target membership changed before dispatch")
				}
			}
		}
	} else {
		command = append(command, change.Member.ServerID)
	}
	if authorize == nil {
		return fmt.Errorf("membership dispatch requires final authorization")
	}
	grant, err := authorize(ctx, current)
	if err != nil {
		return err
	}
	if change.Operation == membership.OperationKick {
		if grant.recheck == nil || !time.Now().Before(grant.validUntil) {
			return fmt.Errorf("kick requires a live, recheckable approval")
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, grant.validUntil)
		defer cancel()
	}
	// Reading and verifying a persisted backup can take time. Invalidate its
	// authorization if the database advanced, changed leaders, or the removal
	// target changed health while the provider was reading storage.
	final, err := c.Status(ctx, current.Database, endpoint)
	if err != nil {
		return err
	}
	if final.ClusterID != current.ClusterID || final.Term != current.Term || final.LeaderServerID != current.LeaderServerID || final.CommitIndex != current.CommitIndex || !sameMembership(final.Members, current.Members) {
		return fmt.Errorf("database changed while verifying membership authorization")
	}
	if err := validateChange(final, change); err != nil {
		return err
	}
	if grant.recheck != nil {
		if err := grant.recheck(ctx, final); err != nil {
			return fmt.Errorf("recheck membership authorization before dispatch: %w", err)
		}
	}
	if err := lease.Check(ctx, lease.Central); err != nil {
		return fmt.Errorf("guard unixctl dispatch: %w", err)
	}
	_, err = c.Runner.Run(ctx, targetEndpoint, command)
	if err != nil {
		return fmt.Errorf("membership dispatch outcome is uncertain; reobserve before retrying: %w", err)
	}
	// unixctl acknowledges an asynchronous request, not convergence. A lifecycle
	// executor must reobserve NB/SB before changing workload replicas.
	return nil
}

func sameMembership(a, b []membership.Member) bool {
	return slices.EqualFunc(a, b, func(x, y membership.Member) bool { return x.ServerID == y.ServerID && x.Address == y.Address })
}

func clusterCommand(database, operation string) ([]string, error) {
	var socket string
	switch database {
	case "OVN_Northbound":
		socket = "/var/run/ovn/ovnnb_db.ctl"
	case "OVN_Southbound":
		socket = "/var/run/ovn/ovnsb_db.ctl"
	default:
		return nil, fmt.Errorf("unsupported OVN database %q", database)
	}
	switch operation {
	case "cluster/status", "cluster/leave", "cluster/kick":
	default:
		return nil, fmt.Errorf("unsupported membership command")
	}
	return []string{"ovs-appctl", "--timeout=5", "-t", socket, operation, database}, nil
}
