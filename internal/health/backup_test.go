package health

import (
	"bytes"
	"context"
	"io"
	"slices"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
)

type backupRunnerFunc struct {
	CommandRunner
	stream func(context.Context, string, string, string, []string, io.Writer) error
}

func (f backupRunnerFunc) Stream(ctx context.Context, namespace, pod, container string, command []string, destination io.Writer) error {
	return f.stream(ctx, namespace, pod, container, command, destination)
}

func TestBackupSourcePinsProcessAndNativeCommand(t *testing.T) {
	for _, scenario := range []string{"northbound", "southbound", "foreign pod", "restart during stream", "no lease", "unsupported database", "unlisted endpoint"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube, objects := healthFixture(t)
			pod := objects[2].(*corev1.Pod)
			pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "containerd://original", Ready: true, State: corev1.ContainerState{Running: new(corev1.ContainerStateRunning)}}}
			if err := kube.Status().Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
			calls := 0
			runner := backupRunnerFunc{stream: func(ctx context.Context, namespace, name, container string, command []string, destination io.Writer) error {
				calls++
				database, socket := "OVN_Northbound", "unix:/var/run/ovn/ovnnb_db.sock"
				if scenario == "southbound" {
					database, socket = "OVN_Southbound", "unix:/var/run/ovn/ovnsb_db.sock"
				}
				if namespace != installation.Namespace || name != pod.Name || container != "ovn-central" || !slices.Equal(command, []string{"ovsdb-client", "--timeout=60", "backup", socket, database}) {
					t.Fatal("unexpected backup command or target")
				}
				if scenario == "restart during stream" {
					pod.Status.ContainerStatuses[0].RestartCount++
					if err := kube.Status().Update(ctx, pod); err != nil {
						return err
					}
				}
				_, err := destination.Write([]byte("streamed"))
				return err
			}}
			endpoints := []ovnadmin.Endpoint{{Name: pod.Name, ServerID: "11111111-2222-3333-4444-555555555555", Address: "tcp:127.0.0.1:6643"}}
			if scenario == "unlisted endpoint" {
				endpoints = nil
			}
			source, err := NewBackupSource(t.Context(), kube, runner, installation, clusterID, endpoints)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "foreign pod" {
				pod.OwnerReferences[0].UID = "foreign"
				if err := kube.Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			}
			database := "OVN_Northbound"
			switch scenario {
			case "southbound":
				database = "OVN_Southbound"
			case "unsupported database":
				database = "OVN_Northbound; unsafe"
			}
			var output bytes.Buffer
			capture := func(ctx context.Context) error { return source.Stream(ctx, database, pod.Name, &output) }
			if scenario == "no lease" {
				err = capture(t.Context())
			} else {
				if err := coordinationv1.AddToScheme(kube.Scheme()); err != nil {
					t.Fatal(err)
				}
				manager := lease.New(kube)
				err = manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
					return manager.With(ctx, installation, lease.Central, capture)
				})
			}
			if scenario == "northbound" || scenario == "southbound" {
				if err != nil || calls != 1 || output.String() != "streamed" {
					t.Fatalf("capture: calls=%d err=%v", calls, err)
				}
			} else if err == nil {
				t.Fatal("accepted invalid source")
			} else if scenario != "restart during stream" && calls != 0 {
				t.Fatal("streamed from an invalid source")
			}
		})
	}
}
