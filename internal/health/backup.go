package health

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type BackupRunner interface {
	CommandRunner
	StreamingCommandRunner
}

// NewBackupSource uses the same fixed inventory and owned process for both
// cluster/status and native backup. It does not discover or adopt databases.
func NewBackupSource(ctx context.Context, reader client.Reader, runner BackupRunner, installation *operatorv1alpha1.KubeOVNInstallation, clusterID string, endpoints []ovnadmin.Endpoint) (backup.Source, error) {
	bound, err := newMembershipRunner(ctx, reader, runner, installation)
	if err != nil {
		return nil, err
	}
	return &backupSource{bound: bound, runner: runner, transport: &ovnadmin.CommandClient{Runner: bound, ClusterID: clusterID, Endpoints: slices.Clone(endpoints)}}, nil
}

type backupSource struct {
	bound     *membershipRunner
	runner    StreamingCommandRunner
	transport *ovnadmin.CommandClient
}

func (s *backupSource) Identity(ctx context.Context, endpoint string) (backup.Identity, error) {
	if err := s.bound.validate(ctx, endpoint); err != nil {
		return backup.Identity{}, err
	}
	if !slices.ContainsFunc(s.transport.Endpoints, func(e ovnadmin.Endpoint) bool { return e.Name == endpoint }) {
		return backup.Identity{}, errors.New("backup endpoint is not in the pinned database inventory")
	}
	pod := s.bound.pods[endpoint]
	for _, container := range pod.Status.ContainerStatuses {
		if container.Name == "ovn-central" {
			return backup.Identity{InstallationUID: string(s.bound.installation.UID), PodUID: string(pod.UID), ContainerID: container.ContainerID, RestartCount: container.RestartCount}, nil
		}
	}
	return backup.Identity{}, errors.New("backup process identity is unavailable")
}

func (s *backupSource) Status(ctx context.Context, database, endpoint string) (ovnadmin.Snapshot, error) {
	return s.transport.Status(ctx, database, endpoint)
}

func (s *backupSource) Stream(ctx context.Context, database, endpoint string, destination io.Writer) error {
	socket := ""
	switch database {
	case "OVN_Northbound":
		socket = "unix:/var/run/ovn/ovnnb_db.sock"
	case "OVN_Southbound":
		socket = "unix:/var/run/ovn/ovnsb_db.sock"
	default:
		return errors.New("unsupported backup database")
	}
	if _, err := s.Identity(ctx, endpoint); err != nil {
		return err
	}
	if err := lease.CheckFor(ctx, lease.Central, s.bound.installation.UID); err != nil {
		return err
	}
	command := []string{"ovsdb-client", "--timeout=60", "backup", socket, database}
	if err := s.runner.Stream(ctx, s.bound.installation.Namespace, endpoint, "ovn-central", command, destination); err != nil {
		return err
	}
	_, err := s.Identity(ctx, endpoint)
	return err
}

func (s *backupSource) RestoreFenced(ctx context.Context, database, endpoint string, source io.Reader, token lease.FenceToken) error {
	if source == nil {
		return errors.New("restore source is required")
	}
	if _, err := s.Identity(ctx, endpoint); err != nil {
		return err
	}
	restorer, ok := s.runner.(FencedInputStreamingCommandRunner)
	if !ok {
		return errors.New("pod transport does not support atomic fenced restore input")
	}
	switch database {
	case "OVN_Northbound":
	case "OVN_Southbound":
	default:
		return errors.New("unsupported restore database")
	}
	if err := lease.CheckFor(ctx, lease.Central, s.bound.installation.UID); err != nil {
		return err
	}
	if err := restorer.StreamInputFenced(ctx, s.bound.installation.Namespace, endpoint, "ovn-central", database, source, token); err != nil {
		return fmt.Errorf("fenced restore input: %w", err)
	}
	if _, err := s.Identity(ctx, endpoint); err != nil {
		return err
	}
	return lease.CheckFor(ctx, lease.Central, s.bound.installation.UID)
}
