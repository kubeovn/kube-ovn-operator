package lifecycle

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type restoreFenceFake struct {
	readyErr error
}

func (f restoreFenceFake) Ready() error { return f.readyErr }

func (restoreFenceFake) Restore(context.Context, *api.KubeOVNInstallation, string, func(context.Context) error) error {
	return nil
}

type restoreFencePreflightFake struct {
	restoreFenceFake
	preflightErr error
	calls        int
}

func (f *restoreFencePreflightFake) ReadyFor(context.Context, *api.KubeOVNInstallation, health.CentralTopology) error {
	f.calls++
	return f.preflightErr
}

func TestRestoreFenceReadyRequiresAtomicTransport(t *testing.T) {
	manager := new(Manager)
	if err := manager.RestoreFenceReady(); err == nil || !strings.Contains(err.Error(), "atomic restore fence") {
		t.Fatalf("missing restore fence was accepted: %v", err)
	}
	manager.RestoreFence = restoreFenceFake{readyErr: errors.New("remote service unavailable")}
	if err := manager.RestoreFenceReady(); err == nil || !strings.Contains(err.Error(), "remote service unavailable") {
		t.Fatalf("unready restore fence was accepted: %v", err)
	}
	manager.RestoreFence = restoreFenceFake{}
	if err := manager.RestoreFenceReady(); err != nil {
		t.Fatalf("ready restore fence was rejected: %v", err)
	}
}

func TestRestoreFenceReadyForRunsLivePreflight(t *testing.T) {
	fence := &restoreFencePreflightFake{preflightErr: errors.New("sidecar is not armed")}
	manager := &Manager{RestoreFence: fence}
	err := manager.RestoreFenceReadyFor(t.Context(), &api.KubeOVNInstallation{}, health.CentralTopology{})
	if err == nil || !strings.Contains(err.Error(), "sidecar is not armed") {
		t.Fatalf("live restore fence failure was accepted: %v", err)
	}
	if fence.calls != 1 {
		t.Fatalf("live restore fence preflight calls = %d, want 1", fence.calls)
	}
}

func TestBackupPairReferencesAreCompleteAndContentAddressed(t *testing.T) {
	refs := map[string]string{
		"OVN_Northbound": "sha256:" + strings.Repeat("a", 64),
		"OVN_Southbound": "sha256:" + strings.Repeat("b", 64),
	}
	status := &api.BackupStatus{ID: backupPairID(refs), DatabaseRefs: refs, Version: "v1.17.0", TopologyDigest: "sha256:" + strings.Repeat("c", 64), CompletedAt: metav1.NewTime(time.Unix(10, 0))}
	if err := validateBackupReferences(status); err != nil {
		t.Fatalf("valid backup pair rejected: %v", err)
	}

	status.ID = "sha256:" + strings.Repeat("d", 64)
	if err := validateBackupReferences(status); err == nil {
		t.Fatal("backup pair with a mismatched content ID was accepted")
	}
	status.ID = backupPairID(refs)
	delete(status.DatabaseRefs, "OVN_Southbound")
	if err := validateBackupReferences(status); err == nil {
		t.Fatal("partial backup pair was accepted")
	}
}

type restoreDatabaseAdapter struct{}

func (restoreDatabaseAdapter) Observe(context.Context) (ovnadmin.Snapshot, error) {
	return ovnadmin.Snapshot{}, nil
}

func (restoreDatabaseAdapter) Capture(context.Context) (string, error) { return "", nil }

func (restoreDatabaseAdapter) Verify(context.Context, ovnadmin.Snapshot, string) error { return nil }

func (restoreDatabaseAdapter) Remove(context.Context, membership.Change, func(context.Context, ovnadmin.Snapshot) error) error {
	return nil
}

type fencedRestoreDatabaseAdapter struct {
	restoreDatabaseAdapter
	restoreCalls int
}

func (a *fencedRestoreDatabaseAdapter) RestoreFenced(context.Context, string, lease.FenceToken) error {
	a.restoreCalls++
	return nil
}

func TestPreflightFencedRestoreAdaptersRejectsPartialCapabilityBeforeRestore(t *testing.T) {
	first := new(fencedRestoreDatabaseAdapter)
	second := restoreDatabaseAdapter{}
	opened := make([]string, 0, 2)
	inventories := []Inventory{{Database: "OVN_Northbound"}, {Database: "OVN_Southbound"}}

	_, err := preflightFencedRestoreAdapters(t.Context(), inventories, func(_ context.Context, inventory Inventory) (Database, error) {
		opened = append(opened, inventory.Database)
		if inventory.Database == "OVN_Northbound" {
			return first, nil
		}
		return second, nil
	})
	if err == nil || !strings.Contains(err.Error(), "OVN_Southbound restore adapter does not expose atomic fencing") {
		t.Fatalf("partial fenced restore capability was accepted: %v", err)
	}
	if !slices.Equal(opened, []string{"OVN_Northbound", "OVN_Southbound"}) {
		t.Fatalf("preflight did not inspect both adapters: %v", opened)
	}
	if first.restoreCalls != 0 {
		t.Fatalf("first database received restore input before second adapter was validated: %d calls", first.restoreCalls)
	}
}
