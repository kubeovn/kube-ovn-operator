package lifecycle

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/central"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Database adapters bind all methods to the same installation and inventory.
// Remove must reobserve and verify backup/approval immediately before dispatch.
type Database interface {
	Observe(context.Context) (ovnadmin.Snapshot, error)
	Capture(context.Context) (string, error)
	Verify(context.Context, ovnadmin.Snapshot, string) error
	Remove(context.Context, membership.Change, func(context.Context, ovnadmin.Snapshot) error) error
}

type Members interface {
	Prepare(context.Context, workload.Request) (workload.Preparation, error)
	Start(context.Context, workload.Reference, central.Request) (workload.Reference, error)
}

type Approvals interface {
	Reserve(context.Context, membership.ApprovalReference, string) error
	Complete(context.Context, membership.ApprovalReference, string, []ovnadmin.Snapshot) error
}

type Dependencies struct {
	Database  func(context.Context, Inventory) (Database, error)
	Members   Members
	Approvals Approvals
}

// Executor uses an uncached Kubernetes client. Both installation-bound Leases
// must be held by the caller for Begin and every Step. No process-local state
// is required between calls; errors leave persisted intent available to retry.
type Executor struct {
	kube         client.Client
	installation *api.KubeOVNInstallation
	dependencies Dependencies
}

func New(kube client.Client, installation *api.KubeOVNInstallation, dependencies Dependencies) (*Executor, error) {
	if kube == nil || installation == nil || installation.UID == "" || installation.Name == "" || installation.Namespace == "" || installation.Generation < 1 || installation.Spec.Central.Mode != api.CentralModeRaft || installation.Spec.Mode == api.InstallationModeDataPlaneOnly || installation.Spec.HCP.Enabled || dependencies.Database == nil {
		return nil, errors.New("joint lifecycle requires an uncached client, local RAFT installation and database factory")
	}
	return &Executor{kube: kube, installation: installation.DeepCopy(), dependencies: dependencies}, nil
}

// Begin admits at most one active operation for an installation. Repeating the
// exact request recovers an uncertain response. A completed operation is never
// replayed after a later operation has claimed the installation journal.
func (e *Executor) Begin(ctx context.Context, request Request) (Reference, error) {
	request, digest, err := canonical(request)
	if err != nil {
		return Reference{}, err
	}
	if request.InstallationUID != string(e.installation.UID) || request.InstallationGeneration != e.installation.Generation || request.Add != nil && e.dependencies.Members == nil {
		return Reference{}, errors.New("operation does not match installation or member provider is missing")
	}
	if err := e.guard(ctx); err != nil {
		return Reference{}, err
	}
	var previous *corev1.ConfigMap
	active := new(corev1.ConfigMap)
	err = e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: e.activeName()}, active)
	if err == nil {
		if err := e.checkRecord(active, false); err != nil {
			return Reference{}, err
		}
		var old journal
		if err := json.Unmarshal([]byte(active.Data["record.json"]), &old, json.RejectUnknownMembers(true)); err != nil {
			return Reference{}, err
		}
		ref := Reference{Journal: workload.Reference{Name: active.Name, UID: active.UID}, Request: old.Request}
		_, _, old, err = e.load(ctx, ref, false)
		if err != nil {
			return Reference{}, err
		}
		if old.Digest == digest {
			return ref, nil
		}
		if old.State.Phase != Complete {
			return Reference{}, errors.New("another joint operation still owns the installation journal")
		}
		previous = active
	} else if !apierrors.IsNotFound(err) {
		return Reference{}, err
	}
	name := "ovn-operation-" + request.OperationID
	finished := new(corev1.ConfigMap)
	if err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: name + "-complete"}, finished); err == nil {
		return Reference{}, errors.New("completed operation UUID cannot be reused")
	} else if !apierrors.IsNotFound(err) {
		return Reference{}, err
	}
	state := State{Phase: Removing}
	if request.Add != nil {
		state.Phase = Preparing
	}
	dbs, err := e.open(ctx, request, state)
	if err != nil {
		return Reference{}, err
	}
	if _, err := observe(ctx, request, state, dbs, false); err != nil {
		return Reference{}, fmt.Errorf("admit joint operation: %w", err)
	}
	if request.Retirement != nil {
		set, err := e.retirementTarget(ctx, *request.Retirement)
		if err != nil {
			return Reference{}, err
		}
		if *set.Spec.Replicas != 1 {
			return Reference{}, errors.New("retirement workload was scaled before operation admission")
		}
	}
	record, err := e.record(ctx, name, request, true)
	if err != nil {
		return Reference{}, err
	}
	saved := journal{Request: workload.Reference{Name: record.Name, UID: record.UID}, Digest: digest, State: state}
	if previous != nil {
		if err := e.save(ctx, previous, saved); err != nil {
			return Reference{}, err
		}
		active = previous
	} else {
		active, err = e.record(ctx, e.activeName(), saved, false)
		if err != nil {
			return Reference{}, err
		}
	}
	return Reference{Journal: workload.Reference{Name: active.Name, UID: active.UID}, Request: saved.Request}, nil
}

// Step advances one durable phase, preparation attempt or removal dispatch.
// A successful dispatch remains Removing until fresh observations prove both
// configurations. Complete includes workload retirement, not just a unixctl ACK.
func (e *Executor) Step(ctx context.Context, ref Reference) (State, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	object, request, saved, err := e.load(ctx, ref, true)
	if err != nil {
		return State{}, err
	}
	state := &saved.State
	if state.Phase == Complete {
		return *state, nil
	}
	if request.Add != nil && e.dependencies.Members == nil {
		return *state, errors.New("persisted operation requires its member provider")
	}
	if request.Retirement != nil && state.Phase != Retiring {
		set, err := e.retirementTarget(ctx, *request.Retirement)
		if err != nil {
			return *state, err
		}
		if *set.Spec.Replicas != 1 {
			return *state, errors.New("retirement workload was scaled before joint membership convergence")
		}
	}
	dbs, err := e.open(ctx, request, *state)
	if err != nil {
		return *state, err
	}
	caught := state.Phase == CatchingUp || state.Phase == Removing || state.Phase == Retiring
	snapshots, err := observe(ctx, request, *state, dbs, caught)
	if err != nil {
		return *state, err
	}
	switch state.Phase {
	case Preparing:
		prepared, err := e.dependencies.Members.Prepare(ctx, request.Add.Workload)
		if err != nil {
			return *state, err
		}
		if prepared.Record.Name == "" || prepared.Record.UID == "" || state.MemberRecord.Name != "" && state.MemberRecord != prepared.Record {
			return *state, errors.New("preparation record identity changed")
		}
		state.MemberRecord = prepared.Record
		if prepared.Result != nil {
			if err := prepared.Result.Validate(request.Add.Workload.Preparation); err != nil {
				return *state, err
			}
			state.Prepared, state.Phase = prepared.Result, Starting
		}
	case Starting:
		// Starting is persisted before any runtime can exist. After an uncertain
		// Start, the inventory already contains both generated server identities.
		if err := e.backup(ctx, object, request, &saved, dbs, false); err != nil {
			return *state, err
		}
		if err := e.guard(ctx); err != nil {
			return *state, err
		}
		running, err := e.dependencies.Members.Start(ctx, state.MemberRecord, request.launch(*state.Prepared))
		if err != nil {
			return *state, err
		}
		if running.Name != state.MemberRecord.Name || running.UID == "" {
			return *state, errors.New("started workload does not match prepared member")
		}
		state.Running, state.Phase = running, CatchingUp
	case CatchingUp:
		state.Phase = Removing
	case Removing:
		for i, snapshot := range snapshots {
			old := peerNamed(request.Databases[i], request.RemoveEndpoint)
			if old == nil {
				continue
			}
			index := slices.IndexFunc(snapshot.Members, func(m membership.Member) bool { return m.ServerID == old.ServerID })
			if index < 0 {
				continue
			}
			target := snapshot.Members[index]
			change := membership.Change{Operation: membership.OperationLeave, ClusterID: snapshot.ClusterID, Member: target,
				OperationID: request.OperationID, Reason: "joint NB/SB lifecycle: retire the pinned old member after both databases converge"}
			if !target.Healthy {
				change.Operation, change.PermanentLossApproved, change.ApprovalRef = membership.OperationKick, true, request.Databases[i].Approval
				if e.dependencies.Approvals == nil || change.ApprovalRef.UID == "" {
					return *state, errors.New("lost old member requires an explicit durable approval")
				}
				if err := e.dependencies.Approvals.Reserve(ctx, change.ApprovalRef, request.OperationID); err != nil {
					return *state, err
				}
				state.Databases[i].ApprovalReserved = true
			}
			if err := e.backup(ctx, object, request, &saved, dbs, true); err != nil {
				return *state, err
			}
			change.BackupRef = state.Databases[i].BackupRef
			state.Databases[i].Intent = &change
			if err := e.save(ctx, object, saved); err != nil {
				return *state, err
			}
			// Recheck both databases after persisting intent. The admin adapter
			// additionally rechecks the target database at actual dispatch.
			if _, err := verifyPair(ctx, request, *state, dbs, true); err != nil {
				return *state, err
			}
			if err := e.guard(ctx); err != nil {
				return *state, err
			}
			return *state, dbs[i].Remove(ctx, change, func(ctx context.Context, final ovnadmin.Snapshot) error {
				joint, err := verifyPair(ctx, request, *state, dbs, true)
				if err != nil {
					return err
				}
				if !sameFence(joint[i], final) {
					return errors.New("target database changed during joint dispatch authorization")
				}
				return e.guard(ctx)
			})
		}
		state.Phase = Retiring
	case Retiring:
		if err := e.completeApprovals(ctx, request, *state, snapshots); err != nil {
			return *state, err
		}
		// Approval audit writes may take time; they are not workload permission.
		snapshots, err = observe(ctx, request, *state, dbs, true)
		if err != nil {
			return *state, err
		}
		if request.Retirement != nil {
			done, err := e.retire(ctx, *request.Retirement)
			if err != nil || !done {
				return *state, err
			}
		}
		state.Phase, state.Final = Complete, snapshots
		if err := e.finish(ctx, &saved); err != nil {
			return *state, err
		}
	}
	if err := validateState(request, *state); err != nil {
		return *state, err
	}
	return *state, e.save(ctx, object, saved)
}

func (e *Executor) open(ctx context.Context, request Request, state State) ([2]Database, error) {
	var result [2]Database
	for i, inventory := range request.inventories(state) {
		db, err := e.dependencies.Database(ctx, inventory)
		if err != nil {
			return result, err
		}
		if db == nil {
			return result, errors.New("database factory returned no adapter")
		}
		result[i] = db
	}
	return result, nil
}

func observe(ctx context.Context, request Request, state State, dbs [2]Database, caught bool) ([]ovnadmin.Snapshot, error) {
	result := make([]ovnadmin.Snapshot, 0, 2)
	for i, db := range dbs {
		snapshot, err := db.Observe(ctx)
		if err != nil {
			return nil, fmt.Errorf("observe %s: %w", databaseNames[i], err)
		}
		if err := snapshot.Validate(); err != nil {
			return nil, err
		}
		original := request.Databases[i]
		if snapshot.Database != original.Database || snapshot.ClusterID != original.ClusterID {
			return nil, errors.New("live database differs from immutable operation identity")
		}
		expected := map[string]Peer{}
		for _, peer := range original.Peers {
			expected[peer.ServerID] = peer
		}
		var added string
		if state.Prepared != nil {
			prepared := state.Prepared.Databases[i]
			added = prepared.ServerID
			if _, exists := expected[added]; exists {
				return nil, errors.New("prepared member reuses an existing identity")
			}
			for _, peer := range expected {
				if peer.Address == prepared.Address || peer.Name == state.MemberRecord.Name+"-0" {
					return nil, errors.New("prepared member conflicts with the original inventory")
				}
			}
			expected[added] = Peer{Name: state.MemberRecord.Name + "-0", ServerID: added, Address: prepared.Address}
		}
		seen := map[string]bool{}
		for _, member := range snapshot.Members {
			peer, exists := expected[member.ServerID]
			if !exists || peer.Address != member.Address {
				return nil, errors.New("live membership contains an unplanned identity or address")
			}
			seen[member.ServerID] = true
			if caught && (request.RemoveEndpoint == "" || peer.Name != request.RemoveEndpoint) && !member.Healthy {
				return nil, errors.New("both databases must retain healthy, caught-up members before removal or retirement")
			}
		}
		for id, peer := range expected {
			if seen[id] {
				continue
			}
			if id == added && !caught {
				continue
			}
			if peer.Name == request.RemoveEndpoint && state.Databases[i].Intent != nil {
				continue
			}
			return nil, errors.New("expected member is missing or replacement has not joined both databases")
		}
		if state.Phase == Retiring && request.RemoveEndpoint != "" {
			old := peerNamed(original, request.RemoveEndpoint)
			if seen[old.ServerID] {
				return nil, errors.New("old member is still configured; workload retirement is forbidden")
			}
		}
		result = append(result, snapshot)
	}
	return result, nil
}

func (e *Executor) backup(ctx context.Context, object *corev1.ConfigMap, request Request, saved *journal, dbs [2]Database, caught bool) error {
	for i, db := range dbs {
		ref, err := db.Capture(ctx)
		if err != nil {
			return err
		}
		if ref == "" {
			return errors.New("backup provider returned an empty reference")
		}
		saved.State.Databases[i].BackupRef = ref
	}
	if err := e.save(ctx, object, *saved); err != nil {
		return err
	}
	_, err := verifyPair(ctx, request, saved.State, dbs, caught)
	return err
}

func verifyPair(ctx context.Context, request Request, state State, dbs [2]Database, caught bool) ([]ovnadmin.Snapshot, error) {
	snapshots, err := observe(ctx, request, state, dbs, caught)
	if err != nil {
		return nil, err
	}
	for i, db := range dbs {
		if err := db.Verify(ctx, snapshots[i], state.Databases[i].BackupRef); err != nil {
			return nil, err
		}
	}
	// Verification may read large stored files. Reobserve the pair and require
	// unchanged fences before the caller can submit its next side effect.
	current, err := observe(ctx, request, state, dbs, caught)
	if err != nil {
		return nil, err
	}
	for i := range snapshots {
		if !sameFence(snapshots[i], current[i]) {
			return nil, errors.New("NB/SB evidence changed while checking the backup pair")
		}
	}
	return current, nil
}

func sameFence(a, b ovnadmin.Snapshot) bool {
	return a.Database == b.Database && a.ClusterID == b.ClusterID && a.Term == b.Term && a.LeaderServerID == b.LeaderServerID && a.CommitIndex == b.CommitIndex && a.AppliedIndex == b.AppliedIndex && slices.Equal(a.Members, b.Members)
}

func (e *Executor) completeApprovals(ctx context.Context, request Request, state State, snapshots []ovnadmin.Snapshot) error {
	used := map[membership.ApprovalReference]bool{}
	for i, db := range state.Databases {
		if db.ApprovalReserved {
			used[request.Databases[i].Approval] = true
		}
	}
	for ref := range used {
		if e.dependencies.Approvals == nil {
			return errors.New("reserved approval provider is unavailable")
		}
		var evidence []ovnadmin.Snapshot
		for i, db := range request.Databases {
			if db.Approval == ref {
				evidence = append(evidence, snapshots[i])
			}
		}
		if err := e.dependencies.Approvals.Complete(ctx, ref, request.OperationID, evidence); err != nil {
			return err
		}
	}
	return nil
}
