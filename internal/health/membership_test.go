package health

import (
	"context"
	"errors"
	"testing"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestMembershipPodRunnerPinsOwnerPodAndContainer(t *testing.T) {
	for _, scenario := range []string{"stable", "readiness changed", "readiness during exec", "pod replaced", "container restarted", "restart count changed", "container stopped", "node changed", "owner changed", "replacement during exec", "write without lease"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube, objects := healthFixture(t)
			pod := objects[2].(*corev1.Pod)
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "containerd://original", Ready: true, State: corev1.ContainerState{Running: new(corev1.ContainerStateRunning)}}}
			if err := kube.Status().Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			calls := 0
			transport, err := NewMembershipClient(t.Context(), kube, runnerFunc(func(ctx context.Context, namespace, name, container string, command []string) ([]byte, error) {
				calls++
				if namespace != installation.Namespace || name != pod.Name || container != "ovn-central" {
					t.Fatal("unexpected exec target")
				}
				if scenario == "replacement during exec" {
					pod.UID = "replacement"
					if err := kube.Update(ctx, pod); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "readiness during exec" {
					pod.Status.Conditions[0].Status = corev1.ConditionFalse
					pod.Status.ContainerStatuses[0].Ready = false
					if err := kube.Status().Update(ctx, pod); err != nil {
						t.Fatal(err)
					}
				}
				return []byte("observed"), nil
			}), installation, clusterID, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := kube.Get(t.Context(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, pod); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "pod replaced":
				pod.UID = "replacement"
				if err := kube.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			case "container restarted":
				pod.Status.ContainerStatuses[0].ContainerID = "containerd://replacement"
				if err := kube.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			case "readiness changed", "restart count changed", "container stopped":
				switch scenario {
				case "readiness changed":
					pod.Status.Conditions[0].Status = corev1.ConditionFalse
					pod.Status.ContainerStatuses[0].Ready = false
				case "restart count changed":
					pod.Status.ContainerStatuses[0].RestartCount++
				case "container stopped":
					pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: new(corev1.ContainerStateTerminated)}
				}
				if err := kube.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			case "node changed":
				pod.Spec.NodeName = "another-node"
				if err := kube.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			case "owner changed":
				pod.OwnerReferences[0].UID = "other-owner"
				if err := kube.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			operation := "cluster/status"
			if scenario == "write without lease" {
				operation = "cluster/leave"
			}
			_, err = transport.Runner.Run(t.Context(), pod.Name, []string{"ovs-appctl", "--timeout=5", "-t", "/var/run/ovn/ovnnb_db.ctl", operation, "OVN_Northbound"})
			if scenario == "stable" || scenario == "readiness changed" || scenario == "readiness during exec" {
				if err != nil || calls != 1 {
					t.Fatalf("calls=%d err=%v", calls, err)
				}
			} else if err == nil {
				t.Fatal("invalid identity or missing lease accepted")
			} else if scenario == "replacement during exec" {
				if calls != 1 || !errors.Is(err, ovnadmin.ErrTargetIdentity) {
					t.Fatalf("replacement result: calls=%d err=%v", calls, err)
				}
			} else if calls != 0 {
				t.Fatalf("queried an invalid target: %d", calls)
			}
		})
	}
}

// The two databases share one container readiness probe. NB leave may drop
// Ready before SB leave, while both commands still require the exact live
// container and a current server-side fence token.
func TestMembershipFenceSurvivesReadinessChangeButRejectsStaleToken(t *testing.T) {
	installation, kube, objects := healthFixture(t)
	pod := objects[2].(*corev1.Pod)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "containerd://original", Ready: true, State: corev1.ContainerState{Running: new(corev1.ContainerStateRunning)}}}
	if err := kube.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	if err := coordinationv1.AddToScheme(kube.Scheme()); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []lease.Scope{lease.Installation, lease.Central} {
		object := &coordinationv1.Lease{Name: lease.Name(scope, installation.Name), Namespace: installation.Namespace, UID: types.UID(string(scope) + "-lease"), OwnerReferences: []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: new(true)}}}
		if err := kube.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	runner := membershipPodFenceRunner{runnerFunc(func(ctx context.Context, _, _, _ string, command []string) ([]byte, error) {
		calls++
		if calls == 1 {
			pod.Status.Conditions[0].Status = corev1.ConditionFalse
			pod.Status.ContainerStatuses[0].Ready = false
			if err := kube.Status().Update(ctx, pod); err != nil {
				return nil, err
			}
		}
		return []byte("accepted"), nil
	})}
	transport, err := NewMembershipClient(t.Context(), kube, runner, installation, clusterID, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager := lease.New(kube)
	err = manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
			token, err := lease.Token(ctx, lease.Central)
			if err != nil {
				return err
			}
			fenced := transport.Runner.(ovnadmin.FencedRunner)
			for _, database := range []string{"OVN_Northbound", "OVN_Southbound"} {
				command := []string{"ovs-appctl", "--timeout=5", "-t", "/var/run/ovn/" + database + ".ctl", "cluster/leave", database}
				if _, err := fenced.RunFenced(ctx, pod.Name, command, token); err != nil {
					return err
				}
			}
			stale := token
			stale.Epoch++
			_, err = fenced.RunFenced(ctx, pod.Name, []string{"ovs-appctl", "--timeout=5", "-t", "/var/run/ovn/ovnsb_db.ctl", "cluster/leave", "OVN_Southbound"}, stale)
			if err == nil {
				t.Fatal("unready Pod accepted a stale fence")
			}
			return nil
		})
	})
	if err != nil || calls != 2 {
		t.Fatalf("joint leave across readiness change: calls=%d err=%v", calls, err)
	}
}

type membershipPodFenceRunner struct{ runnerFunc }

func (r membershipPodFenceRunner) RunFenced(ctx context.Context, namespace, pod, container string, command []string, _ lease.FenceToken) ([]byte, error) {
	return r.Run(ctx, namespace, pod, container, command)
}
