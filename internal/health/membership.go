package health

import (
	"context"
	"fmt"
	"slices"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NewMembershipClient binds a pinned database inventory to the currently owned
// central Pod/container identities. It does not discover or approve a topology
// transition. Callers must persist full database identities before losing a Pod.
func NewMembershipClient(ctx context.Context, reader client.Reader, runner CommandRunner, installation *operatorv1alpha1.KubeOVNInstallation, clusterID string, endpoints []ovnadmin.Endpoint) (*ovnadmin.CommandClient, error) {
	bound, err := newMembershipRunner(ctx, reader, runner, installation)
	if err != nil {
		return nil, err
	}
	return &ovnadmin.CommandClient{Runner: bound, ClusterID: clusterID, Endpoints: slices.Clone(endpoints)}, nil
}

func newMembershipRunner(ctx context.Context, reader client.Reader, runner CommandRunner, installation *operatorv1alpha1.KubeOVNInstallation) (*membershipRunner, error) {
	if reader == nil || runner == nil || installation == nil || installation.UID == "" || installation.Name == "" || installation.Namespace == "" || installation.Spec.Central.Mode != operatorv1alpha1.CentralModeRaft || installation.Spec.Mode == operatorv1alpha1.InstallationModeDataPlaneOnly || installation.Spec.HCP.Enabled {
		return nil, fmt.Errorf("membership transport requires a local managed RAFT installation")
	}
	pods, err := ownedPods(ctx, reader, installation, "ovn-central")
	if err != nil {
		return nil, err
	}
	bound := &membershipRunner{reader: reader, runner: runner, installation: installation.DeepCopy(), pods: map[string]corev1.Pod{}}
	for _, pod := range pods {
		bound.pods[pod.Name] = *pod.DeepCopy()
	}
	return bound, nil
}

type membershipRunner struct {
	reader       client.Reader
	runner       CommandRunner
	installation *operatorv1alpha1.KubeOVNInstallation
	pods         map[string]corev1.Pod
}

func (r *membershipRunner) Run(ctx context.Context, endpoint string, command []string) ([]byte, error) {
	if err := r.validate(ctx, endpoint); err != nil {
		return nil, err
	}
	if len(command) < 6 || command[0] != "ovs-appctl" {
		return nil, fmt.Errorf("unsupported membership command")
	}
	if command[4] != "cluster/status" {
		if err := lease.CheckFor(ctx, lease.Central, r.installation.UID); err != nil {
			return nil, err
		}
	}
	data, err := r.runner.Run(ctx, r.installation.Namespace, endpoint, "ovn-central", command)
	if err != nil {
		return nil, err
	}
	if err := r.validate(ctx, endpoint); err != nil {
		return nil, err
	}
	return data, nil
}

func (r *membershipRunner) validate(ctx context.Context, endpoint string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	pods, err := ownedPods(ctx, r.reader, r.installation, "ovn-central")
	if err != nil {
		return err
	}
	for _, current := range pods {
		if current.Name != endpoint {
			continue
		}
		previous, exists := r.pods[endpoint]
		if !exists || current.UID == "" || !samePods([]corev1.Pod{previous}, []corev1.Pod{current}) {
			return fmt.Errorf("%w: %s", ovnadmin.ErrTargetIdentity, endpoint)
		}
		for _, status := range current.Status.ContainerStatuses {
			if status.Name == "ovn-central" && status.ContainerID != "" && status.Ready && status.State.Running != nil {
				return nil
			}
		}
		return fmt.Errorf("central process identity is unavailable for %s", endpoint)
	}
	current := new(corev1.Pod)
	if err := r.reader.Get(ctx, types.NamespacedName{Namespace: r.installation.Namespace, Name: endpoint}, current); err == nil {
		return fmt.Errorf("%w: endpoint %s no longer has the expected ownership", ovnadmin.ErrTargetIdentity, endpoint)
	} else if !apierrors.IsNotFound(err) {
		return err
	}
	return fmt.Errorf("owned central endpoint %s is unavailable", endpoint)
}
