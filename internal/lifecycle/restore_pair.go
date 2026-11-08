package lifecycle

import (
	"context"
	"errors"
	"fmt"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

// stageRestorePair requires both authenticated native snapshots before any
// database can receive restore input. The handles outlive the store and carry
// the exact verified bytes through the later online topology/fence checks.
func (m *Manager) stageRestorePair(ctx context.Context, installation *api.KubeOVNInstallation, inventories []Inventory, status *api.BackupStatus) ([2]*backup.StagedSnapshot, error) {
	var staged [2]*backup.StagedSnapshot
	if m == nil || installation == nil || m.StoreFactory == nil {
		return staged, errors.New("restore staging requires an installation and store factory")
	}
	if err := validateBackupReferences(status); err != nil {
		return staged, err
	}
	if len(inventories) != 2 || inventories[0].Database != databaseNames[0] || inventories[1].Database != databaseNames[1] {
		return staged, errors.New("restore staging requires the retained NB/SB inventories")
	}
	if err := lease.CheckFor(ctx, lease.Central, installation.UID); err != nil {
		return staged, err
	}
	store, err := m.StoreFactory(installation)
	if err != nil {
		return staged, err
	}
	defer func() {
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}()
	success := false
	defer func() {
		if !success {
			for _, snapshot := range staged {
				if snapshot != nil {
					_ = snapshot.Close()
				}
			}
		}
	}()
	provider := &backup.Provider{Store: store, InstallationUID: string(installation.UID), MaxAge: m.MaxAge, MaxBytes: m.MaxBytes}
	for index, database := range databaseNames {
		staged[index], err = provider.Stage(ctx, database, status.DatabaseRefs[database])
		if err != nil {
			return [2]*backup.StagedSnapshot{}, fmt.Errorf("stage %s restore input: %w", database, err)
		}
		record := staged[index].Record()
		inventory := inventories[index]
		if record.Snapshot.ClusterID != inventory.ClusterID || len(record.Snapshot.Members) != len(inventory.Peers) {
			return [2]*backup.StagedSnapshot{}, fmt.Errorf("staged %s backup does not match retained membership", database)
		}
		members := make(map[string]string, len(record.Snapshot.Members))
		for _, member := range record.Snapshot.Members {
			members[member.ServerID] = member.Address
		}
		for _, peer := range inventory.Peers {
			if members[peer.ServerID] != peer.Address {
				return [2]*backup.StagedSnapshot{}, fmt.Errorf("staged %s backup does not match a retained server identity or address", database)
			}
		}
	}
	success = true
	return staged, nil
}
