package health

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

// RemoteRestoreFence is the operator-side readiness adapter for the remote
// fence agent. The lifecycle manager supplies the callback only after it has
// asserted a FencedDatabaseRestorer; that adapter sends the backup stream and
// token to the agent, where comparison and native restore admission are one
// critical section.
type RemoteRestoreFence struct {
	Remote *FenceHTTPClient
}

func (f *RemoteRestoreFence) Ready() error {
	if f == nil || f.Remote == nil {
		return errors.New("remote restore fence client is not configured")
	}
	return nil
}

// ReadyFor verifies every retained central sidecar before the caller changes
// workload rollout state. A configured HTTP client alone is insufficient:
// the remote Lease watcher may be disconnected or observing a stale epoch.
func (f *RemoteRestoreFence) ReadyFor(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, topology CentralTopology) error {
	if err := f.Ready(); err != nil {
		return err
	}
	if installation == nil || installation.Namespace == "" {
		return errors.New("restore fence preflight requires an installation namespace")
	}
	if err := topology.Validate(); err != nil {
		return fmt.Errorf("restore fence preflight topology: %w", err)
	}
	for _, member := range topology.Members {
		if err := f.Remote.ProbeReadyAt(ctx, installation.Namespace, member.Pod.Name, retainedFenceHost(member), "ovn-central"); err != nil {
			return fmt.Errorf("restore fence for central Pod %s is not ready: %w", member.Pod.Name, err)
		}
	}
	return nil
}

func retainedFenceHost(member CentralIdentity) string {
	for _, database := range member.Databases {
		address := strings.TrimSpace(database.Address)
		protocol, endpoint, ok := strings.Cut(address, ":")
		if !ok || protocol != "tcp" && protocol != "ssl" {
			continue
		}
		host, _, err := net.SplitHostPort(endpoint)
		if err == nil && strings.TrimSpace(host) != "" {
			return strings.Trim(host, "[]")
		}
	}
	return ""
}

func (f *RemoteRestoreFence) Restore(ctx context.Context, _ *operatorv1alpha1.KubeOVNInstallation, _ string, callback func(context.Context) error) error {
	if err := f.Ready(); err != nil {
		return err
	}
	if callback == nil {
		return errors.New("restore fence callback is required")
	}
	return callback(ctx)
}
