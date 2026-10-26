package lifecycle

import (
	"context"
	"crypto/sha256"
	stdjson "encoding/json/v2"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager is the controller seam for resuming durable operations. StoreFactory
// is deliberately explicit: the manager never creates a backup directory or
// credentials and cannot silently fall back to an unprotected store.
type Manager struct {
	Client       client.Client
	Runner       health.BackupRunner
	StoreFactory func(*api.KubeOVNInstallation) (backup.Store, error)
	RestoreFence RestoreFence
	MaxAge       time.Duration
	MaxBytes     int64
}

// Reader exposes the direct Kubernetes reader used for immutable lifecycle
// selection. The controller uses this boundary instead of reaching into the
// concrete manager, which keeps admission tests independent of backup stores.
func (m *Manager) Reader() client.Reader {
	if m == nil {
		return nil
	}
	return m.Client
}

// SetStoreFactory configures the operator-owned backup store used by lifecycle
// capture, verification and retention operations.
func (m *Manager) SetStoreFactory(factory func(*api.KubeOVNInstallation) (backup.Store, error)) {
	if m != nil {
		m.StoreFactory = factory
	}
}

type Recovery struct {
	Reference  Reference
	Request    Request
	State      State
	Completion *Completion
}

// CleanupBackups applies an explicit retention policy to the configured
// operator-owned store. It does not create storage and refuses stores without
// the restricted inventory/delete surface.
func (m *Manager) CleanupBackups(ctx context.Context, installation *api.KubeOVNInstallation, policy backup.RetentionPolicy, protected []string, now time.Time) (backup.RetentionResult, error) {
	if m == nil || installation == nil || m.StoreFactory == nil {
		return backup.RetentionResult{}, errors.New("backup cleanup requires an installation and store factory")
	}
	store, err := m.StoreFactory(installation)
	if err != nil {
		return backup.RetentionResult{}, err
	}
	defer func() {
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}()
	retention, ok := store.(backup.RetentionStore)
	if !ok {
		return backup.RetentionResult{}, errors.New("configured backup store does not support retention cleanup")
	}
	return backup.Cleanup(ctx, retention, policy, protected, now)
}

// ProtectedBackupRefs reads the active lifecycle journal and returns every
// database reference that may still be needed by an in-flight operation.
func (m *Manager) ProtectedBackupRefs(ctx context.Context, installation *api.KubeOVNInstallation, pinned *api.CentralOperationReference) ([]string, error) {
	if pinned == nil {
		return nil, nil
	}
	operation, err := Inspect(ctx, m.Client, installation, &Reference{
		Journal: workload.Reference{Name: pinned.JournalName, UID: pinned.JournalUID},
		Request: workload.Reference{Name: pinned.RequestName, UID: pinned.RequestUID},
	})
	if err != nil {
		return nil, err
	}
	if operation == nil {
		return nil, errors.New("active lifecycle operation disappeared")
	}
	refs := make([]string, 0, len(operation.State.Databases))
	for _, database := range operation.State.Databases {
		if database.BackupRef != "" {
			refs = append(refs, database.BackupRef)
		}
		if database.Intent != nil && database.Intent.BackupRef != "" {
			refs = append(refs, database.Intent.BackupRef)
		}
	}
	return slices.Compact(slices.Sorted(slices.Values(refs))), nil
}

// BackupBinding identifies the installation state that a verified pair
// protects. The database references alone are insufficient because they can
// otherwise be replayed after a version or topology change.
type BackupBinding struct {
	Destination    string
	Version        string
	TopologyDigest string
}

// CaptureBackupPair captures and verifies both OVN database backups against
// one retained topology. A pair is returned only after both databases have
// been re-observed with unchanged fences.
func (m *Manager) CaptureBackupPair(ctx context.Context, installation *api.KubeOVNInstallation, topology health.CentralTopology, binding BackupBinding) (*api.BackupStatus, error) {
	return m.backupPair(ctx, installation, topology, binding, nil, false)
}

// VerifyBackupPair revalidates an existing pair against the current retained
// topology and binding. It performs no capture and rejects stale or foreign
// references.
func (m *Manager) VerifyBackupPair(ctx context.Context, installation *api.KubeOVNInstallation, topology health.CentralTopology, binding BackupBinding, status *api.BackupStatus) error {
	_, err := m.backupPair(ctx, installation, topology, binding, status, true)
	return err
}

// RestoreFenceReady reports whether the destructive restore transport can
// atomically admit native restore commands. It is intentionally separate from
// the ordinary Lease checks used by backup capture and lifecycle writes.
func (m *Manager) RestoreFenceReady() error {
	if m == nil || m.RestoreFence == nil {
		return errors.New("atomic restore fence is not configured")
	}
	if err := m.RestoreFence.Ready(); err != nil {
		return fmt.Errorf("atomic restore fence is not ready: %w", err)
	}
	return nil
}

// RestoreFenceReadyFor performs the optional live readiness preflight exposed
// by a remote fence service. Static capability checks remain useful for
// admission, but destructive callers should use this method before quiescing
// workloads when the fence can verify its current Lease arm state.
func (m *Manager) RestoreFenceReadyFor(ctx context.Context, installation *api.KubeOVNInstallation, topology health.CentralTopology) error {
	if err := m.RestoreFenceReady(); err != nil {
		return err
	}
	preflight, ok := m.RestoreFence.(interface {
		ReadyFor(context.Context, *api.KubeOVNInstallation, health.CentralTopology) error
	})
	if !ok {
		return nil
	}
	if err := preflight.ReadyFor(ctx, installation, topology); err != nil {
		return fmt.Errorf("atomic restore fence readiness preflight: %w", err)
	}
	return nil
}

// RestoreBackupPair restores both databases from one immutable, version-bound
// pair. The caller must hold the installation and central Leases; this method
// also checks the central fence before and after every database mutation so a
// stale or cancelled operation cannot continue with the second database.
func (m *Manager) RestoreBackupPair(ctx context.Context, installation *api.KubeOVNInstallation, topology health.CentralTopology, binding BackupBinding, status *api.BackupStatus) error {
	if m == nil || installation == nil {
		return errors.New("database restore requires an installation")
	}
	if err := m.RestoreFenceReady(); err != nil {
		return err
	}
	if binding.Version == "" || binding.TopologyDigest == "" {
		return errors.New("database restore requires a target version and topology digest")
	}
	if status == nil || status.Version != binding.Version || strings.TrimSpace(status.Destination) != strings.TrimSpace(binding.Destination) || status.TopologyDigest != binding.TopologyDigest {
		return errors.New("database restore pair is not bound to the requested target version, destination and topology")
	}
	if err := validateBackupReferences(status); err != nil {
		return err
	}
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("validate retained topology before restore: %w", err)
	}
	if installation.Status.Topology.Inventory == nil || installation.Status.Topology.Inventory.Digest != binding.TopologyDigest {
		return errors.New("database restore topology does not match the retained installation topology")
	}
	executor, closeStore, err := m.executor(installation)
	if err != nil {
		return err
	}
	defer closeStore()
	inventories := inventories(topology)
	restorers, err := preflightFencedRestoreAdapters(ctx, inventories, executor.dependencies.Database)
	if err != nil {
		return err
	}
	for index, inventory := range inventories {
		if err := lease.CheckFor(ctx, lease.Central, installation.UID); err != nil {
			return fmt.Errorf("fence %s restore: %w", inventory.Database, err)
		}
		restorer := restorers[index]
		ref := status.DatabaseRefs[inventory.Database]
		token, err := lease.Token(ctx, lease.Central)
		if err != nil {
			return fmt.Errorf("read current restore fence for %s: %w", inventory.Database, err)
		}
		if err := m.RestoreFence.Restore(ctx, installation, inventory.Database, func(restoreCtx context.Context) error {
			return restorer.RestoreFenced(restoreCtx, ref, token)
		}); err != nil {
			return fmt.Errorf("restore %s database: %w", inventory.Database, err)
		}
		if err := lease.CheckFor(ctx, lease.Central, installation.UID); err != nil {
			return fmt.Errorf("fence after %s restore: %w", inventory.Database, err)
		}
	}
	return nil
}

// preflightFencedRestoreAdapters opens and validates every database adapter
// before the caller is allowed to invoke any restore operation. A missing
// capability in the second database must therefore fail before the first
// database can receive restore input.
func preflightFencedRestoreAdapters(ctx context.Context, inventories []Inventory, factory func(context.Context, Inventory) (Database, error)) ([2]FencedDatabaseRestorer, error) {
	var restorers [2]FencedDatabaseRestorer
	if len(inventories) != len(restorers) || factory == nil {
		return restorers, errors.New("restore requires both database adapters")
	}
	for index, inventory := range inventories {
		database, err := factory(ctx, inventory)
		if err != nil {
			return restorers, fmt.Errorf("open %s restore adapter: %w", inventory.Database, err)
		}
		if database == nil {
			return restorers, fmt.Errorf("open %s restore adapter: factory returned nil", inventory.Database)
		}
		restorer, ok := database.(FencedDatabaseRestorer)
		if !ok {
			return restorers, fmt.Errorf("%s restore adapter does not expose atomic fencing", inventory.Database)
		}
		restorers[index] = restorer
	}
	return restorers, nil
}

func (m *Manager) backupPair(ctx context.Context, installation *api.KubeOVNInstallation, topology health.CentralTopology, binding BackupBinding, existing *api.BackupStatus, verifyOnly bool) (*api.BackupStatus, error) {
	if m == nil || installation == nil {
		return nil, errors.New("backup pair requires an installation")
	}
	if binding.Version == "" || binding.TopologyDigest == "" {
		return nil, errors.New("backup pair requires a version and topology digest")
	}
	binding.Destination = strings.TrimSpace(binding.Destination)
	if installation.Status.ObservedVersion != binding.Version || installation.Status.Topology.Inventory == nil || installation.Status.Topology.Inventory.Digest != binding.TopologyDigest {
		return nil, errors.New("backup pair binding does not match the installation's observed version and retained topology")
	}
	if err := topology.Validate(); err != nil {
		return nil, fmt.Errorf("validate retained topology for backup pair: %w", err)
	}
	if existing != nil {
		if strings.TrimSpace(existing.Destination) != binding.Destination || existing.Version != binding.Version || existing.TopologyDigest != binding.TopologyDigest {
			return nil, errors.New("backup pair binding does not match the requested destination, version and topology")
		}
		if err := validateBackupReferences(existing); err != nil {
			return nil, err
		}
	}
	executor, closeStore, err := m.executor(installation)
	if err != nil {
		return nil, err
	}
	defer closeStore()
	inventories := inventories(topology)
	var databases [2]Database
	for index, inventory := range inventories {
		database, err := executor.dependencies.Database(ctx, inventory)
		if err != nil {
			return nil, fmt.Errorf("open %s backup adapter: %w", inventory.Database, err)
		}
		if database == nil {
			return nil, fmt.Errorf("open %s backup adapter: factory returned nil", inventory.Database)
		}
		databases[index] = database
	}
	var before [2]ovnadmin.Snapshot
	for index, database := range databases {
		before[index], err = database.Observe(ctx)
		if err != nil {
			return nil, fmt.Errorf("observe %s before backup: %w", inventories[index].Database, err)
		}
		if err := before[index].Validate(); err != nil {
			return nil, fmt.Errorf("validate %s before backup: %w", inventories[index].Database, err)
		}
	}
	status := existing
	if !verifyOnly {
		refs := make(map[string]string, len(databases))
		for index, database := range databases {
			ref, err := database.Capture(ctx)
			if err != nil {
				return nil, fmt.Errorf("capture %s backup: %w", inventories[index].Database, err)
			}
			if ref == "" {
				return nil, fmt.Errorf("capture %s backup returned an empty reference", inventories[index].Database)
			}
			refs[inventories[index].Database] = ref
		}
		status = &api.BackupStatus{DatabaseRefs: refs, Destination: binding.Destination, Version: binding.Version, TopologyDigest: binding.TopologyDigest, CompletedAt: metav1.NewTime(time.Now().UTC())}
		status.ID = backupPairID(refs)
	}
	if err := validateBackupReferences(status); err != nil {
		return nil, err
	}
	for index, database := range databases {
		if err := database.Verify(ctx, before[index], status.DatabaseRefs[inventories[index].Database]); err != nil {
			return nil, fmt.Errorf("verify %s backup pair member: %w", inventories[index].Database, err)
		}
	}
	var after [2]ovnadmin.Snapshot
	for index, database := range databases {
		after[index], err = database.Observe(ctx)
		if err != nil {
			return nil, fmt.Errorf("observe %s after backup verification: %w", inventories[index].Database, err)
		}
		if err := after[index].Validate(); err != nil {
			return nil, fmt.Errorf("validate %s after backup verification: %w", inventories[index].Database, err)
		}
		if !sameFence(before[index], after[index]) {
			return nil, fmt.Errorf("%s database fence changed while verifying the backup pair", inventories[index].Database)
		}
	}
	result := *status
	result.Destination = binding.Destination
	result.DatabaseRefs = maps.Clone(status.DatabaseRefs)
	return &result, nil
}

func validateBackupReferences(status *api.BackupStatus) error {
	if status == nil || status.ID == "" || len(status.DatabaseRefs) != 2 || status.DatabaseRefs["OVN_Northbound"] == "" || status.DatabaseRefs["OVN_Southbound"] == "" || status.CompletedAt.IsZero() {
		return errors.New("backup pair is incomplete")
	}
	if expected := backupPairID(status.DatabaseRefs); expected != status.ID {
		return errors.New("backup pair ID does not match its database references")
	}
	return nil
}

func backupPairID(refs map[string]string) string {
	data, _ := stdjson.Marshal(refs, stdjson.Deterministic(true))
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

// Begin admits a controller-selected operation into the same durable executor
// used by Resume. The journal is the source of truth before this method
// returns, so a lost status update cannot cause a different selection on retry.
func (m *Manager) Begin(ctx context.Context, installation *api.KubeOVNInstallation, request Request) (Reference, error) {
	executor, closeStore, err := m.executor(installation)
	if err != nil {
		return Reference{}, err
	}
	defer closeStore()
	return executor.Begin(ctx, request)
}

// Resume discovers the active installation operation, advances one durable
// step when possible, and re-reads the journal. A complete result is accepted
// only after the immutable receipt has been validated. The caller must hold
// both installation and central Leases for the entire call.
func (m *Manager) Resume(ctx context.Context, installation *api.KubeOVNInstallation, pinned *api.CentralOperationReference) (*Recovery, error) {
	if m == nil || m.Client == nil || m.Runner == nil || installation == nil {
		return nil, errors.New("lifecycle manager requires a direct client, Pod runner and installation")
	}
	var pin *Reference
	if pinned != nil {
		if pinned.JournalName == "" || pinned.JournalUID == "" || pinned.RequestName == "" || pinned.RequestUID == "" {
			return nil, errors.New("pinned lifecycle operation is incomplete")
		}
		if installation.Status.Topology.Inventory == nil {
			return nil, errors.New("lifecycle operation has no retained topology")
		}
		matchesOriginal := pinned.Inventory == *installation.Status.Topology.Inventory
		matchesHandoff := pinned.HandoffInventory != nil && *pinned.HandoffInventory == *installation.Status.Topology.Inventory
		if !matchesOriginal && !matchesHandoff {
			return nil, errors.New("lifecycle operation inventory no longer matches the retained topology")
		}
		pin = new(Reference)
		pin.Journal = workload.Reference{Name: pinned.JournalName, UID: pinned.JournalUID}
		pin.Request = workload.Reference{Name: pinned.RequestName, UID: pinned.RequestUID}
	}
	operation, err := Inspect(ctx, m.Client, installation, pin)
	if err != nil || operation == nil {
		return nil, err
	}
	if operation.State.Phase != Complete {
		executor, closeStore, err := m.executor(installation)
		if err != nil {
			return nil, err
		}
		defer closeStore()
		state, err := executor.Step(ctx, operation.Reference)
		if err != nil {
			return nil, err
		}
		operation, err = Inspect(ctx, m.Client, installation, &operation.Reference)
		if err != nil {
			return nil, err
		}
		operation.State = state
	}
	if pinned != nil && pinned.CompletionDigest != "" {
		if operation.Completion == nil || operation.Completion.Digest != pinned.CompletionDigest {
			return nil, errors.New("lifecycle completion receipt digest changed")
		}
	}
	return &Recovery{Reference: operation.Reference, Request: operation.Request, State: operation.State, Completion: operation.Completion}, nil
}

func (m *Manager) executor(installation *api.KubeOVNInstallation) (*Executor, func(), error) {
	if m == nil || m.Client == nil || m.Runner == nil || installation == nil {
		return nil, func() {}, errors.New("lifecycle manager requires a direct client, Pod runner and installation")
	}
	if m.StoreFactory == nil || m.MaxAge <= 0 || m.MaxBytes <= 0 {
		return nil, func() {}, errors.New("active lifecycle operation requires an explicit backup store policy")
	}
	store, err := m.StoreFactory(installation)
	if err != nil {
		return nil, func() {}, err
	}
	closeStore := func() {}
	if closer, ok := store.(interface{ Close() error }); ok {
		closeStore = func() { _ = closer.Close() }
	}
	executor, err := NewKubernetes(m.Client, installation, m.Runner, store, m.MaxAge, m.MaxBytes)
	if err != nil {
		closeStore()
		return nil, func() {}, err
	}
	return executor, closeStore, nil
}
