package health

import (
	"context"
	"errors"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

func TestMembershipPodRunnerPinsOwnerPodAndContainer(t *testing.T) {
	for _, scenario := range []string{"stable", "pod replaced", "container restarted", "owner changed", "replacement during exec", "write without lease"} {
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
			if scenario == "stable" {
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
