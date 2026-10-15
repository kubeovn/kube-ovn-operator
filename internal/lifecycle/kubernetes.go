package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/approval"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewKubernetes connects the durable executor to the actual pod transport,
// native backup store, approval provider and preparation/runtime workloads.
// The store must already be provisioned; this function creates no credentials,
// storage, permissions or installations. The caller owns its lifetime.
func NewKubernetes(kube client.Client, installation *api.KubeOVNInstallation, runner health.BackupRunner, store backup.Store, maxAge time.Duration, maxBytes int64) (*Executor, error) {
	if runner == nil || store == nil || maxAge <= 0 || maxBytes < 1 || maxBytes > 1<<30 {
		return nil, errors.New("joint lifecycle requires a pod runner and explicit backup store policy")
	}
	members, err := workload.New(kube, installation)
	if err != nil {
		return nil, err
	}
	approvals, err := approval.New(kube, installation)
	if err != nil {
		return nil, err
	}
	return New(kube, installation, Dependencies{Members: members, Approvals: approvals,
		Database: func(ctx context.Context, inventory Inventory) (Database, error) {
			transport, err := health.NewMembershipClient(ctx, kube, runner, installation, inventory.ClusterID, inventory.endpoints())
			if err != nil {
				return nil, err
			}
			source, err := health.NewBackupSource(ctx, kube, runner, installation, inventory.ClusterID, inventory.endpoints())
			if err != nil {
				return nil, err
			}
			return &kubernetesDatabase{inventory: inventory, transport: transport, approvals: approvals,
				backups: &backup.Provider{Source: source, Store: store, InstallationUID: string(installation.UID), MaxAge: maxAge, MaxBytes: maxBytes}}, nil
		}})
}

type kubernetesDatabase struct {
	inventory Inventory
	transport *ovnadmin.CommandClient
	backups   *backup.Provider
	approvals *approval.Provider
}

func (d *kubernetesDatabase) Observe(ctx context.Context) (ovnadmin.Snapshot, error) {
	var failures []error
	for _, peer := range d.inventory.Peers {
		snapshot, err := d.transport.Status(ctx, d.inventory.Database, peer.Name)
		if err == nil {
			err = snapshot.Validate()
		}
		if err == nil {
			return snapshot, nil
		}
		if ctx.Err() != nil || errors.Is(err, ovnadmin.ErrTargetIdentity) {
			return ovnadmin.Snapshot{}, err
		}
		failures = append(failures, err)
	}
	return ovnadmin.Snapshot{}, fmt.Errorf("no consistent healthy leader for %s: %w", d.inventory.Database, errors.Join(failures...))
}

func (d *kubernetesDatabase) leader(ctx context.Context) (string, error) {
	snapshot, err := d.Observe(ctx)
	if err != nil {
		return "", err
	}
	for _, peer := range d.inventory.Peers {
		if peer.ServerID == snapshot.LeaderServerID {
			return peer.Name, nil
		}
	}
	return "", errors.New("leader is absent from the durable inventory")
}

func (d *kubernetesDatabase) Capture(ctx context.Context) (string, error) {
	leader, err := d.leader(ctx)
	if err != nil {
		return "", err
	}
	return d.backups.Capture(ctx, d.inventory.Database, leader)
}

func (d *kubernetesDatabase) Verify(ctx context.Context, snapshot ovnadmin.Snapshot, ref string) error {
	return d.backups.Verify(ctx, snapshot, ref)
}

func (d *kubernetesDatabase) Remove(ctx context.Context, change membership.Change, guard func(context.Context, ovnadmin.Snapshot) error) error {
	leader, err := d.leader(ctx)
	if err != nil {
		return err
	}
	executor := ovnadmin.Executor{Client: d.transport, Database: d.inventory.Database, Backups: d.backups, PermanentLoss: d.approvals, BeforeDispatch: guard}
	return executor.Execute(ctx, leader, change)
}
