package lifecycle

import (
	"context"
	"errors"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Manager is the controller seam for resuming durable operations. StoreFactory
// is deliberately explicit: the manager never creates a backup directory or
// credentials and cannot silently fall back to an unprotected store.
type Manager struct {
	Client       client.Client
	Runner       health.BackupRunner
	StoreFactory func(*api.KubeOVNInstallation) (backup.Store, error)
	MaxAge       time.Duration
	MaxBytes     int64
}

type Recovery struct {
	Reference  Reference
	Request    Request
	State      State
	Completion *Completion
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
		if m.StoreFactory == nil || m.MaxAge <= 0 || m.MaxBytes <= 0 {
			return nil, errors.New("active lifecycle operation requires an explicit backup store policy")
		}
		store, err := m.StoreFactory(installation)
		if err != nil {
			return nil, err
		}
		if closer, ok := store.(interface{ Close() error }); ok {
			defer func() { _ = closer.Close() }()
		}
		executor, err := NewKubernetes(m.Client, installation, m.Runner, store, m.MaxAge, m.MaxBytes)
		if err != nil {
			return nil, err
		}
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
