package topology

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"uuid"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type commandFunc func(context.Context, string, string, string, []string) ([]byte, error)

func (f commandFunc) Run(ctx context.Context, namespace, pod, container string, command []string) ([]byte, error) {
	return f(ctx, namespace, pod, container, command)
}

type fixture struct {
	kube         client.Client
	installation *api.KubeOVNInstallation
	provider     *Kubernetes
	failCreate   bool
	failQuery    bool
	serverID     string
	index        int
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := api.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	f := &fixture{serverID: "aaaa0000-1111-2222-3333-444444444444", index: 21}
	f.installation = &api.KubeOVNInstallation{Name: "network", Namespace: "default", UID: "installation", Generation: 1, Spec: api.KubeOVNInstallationSpec{Mode: api.InstallationModeControlPlaneOnly, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 1}}}
	workload := &appsv1.StatefulSet{Name: "central", Namespace: "default", UID: "workload", Labels: labels(f.installation), OwnerReferences: []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: "network", UID: "installation", Controller: new(true)}}}
	pod := &corev1.Pod{Name: "central-0", Namespace: "default", UID: "pod", Labels: map[string]string{"app": "ovn-central"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "central", UID: "workload", Controller: new(true)}}, Spec: corev1.PodSpec{NodeName: "node", HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", VolumeMounts: []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/ovn-member"}}}}, Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}, ContainerStatuses: []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "containerd://original", Ready: true, State: corev1.ContainerState{Running: new(corev1.ContainerStateRunning)}}}}}
	node := &corev1.Node{Name: "node", UID: "node", Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}, Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.1"}}}}
	claim := &corev1.PersistentVolumeClaim{Name: "data", Namespace: "default", UID: "claim", Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "data"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	volume := &corev1.PersistentVolume{Name: "data", UID: "volume", Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: "data", Namespace: "default", UID: "claim"}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	f.kube = fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.installation, workload, pod, node, claim, volume).WithStatusSubresource(f.installation).WithInterceptorFuncs(interceptor.Funcs{
		Create: func(ctx context.Context, kube client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			if object.GetUID() == "" {
				object.SetUID(types.UID(uuid.New().String()))
			}
			if err := kube.Create(ctx, object, opts...); err != nil {
				return err
			}
			if _, ok := object.(*corev1.ConfigMap); ok && f.failCreate {
				f.failCreate = false
				return errors.New("accepted create response lost")
			}
			return nil
		},
	}).Build()
	f.provider = &Kubernetes{Client: f.kube, Runner: commandFunc(func(_ context.Context, _, _, _ string, command []string) ([]byte, error) {
		if f.failQuery {
			return nil, errors.New("unavailable")
		}
		database, port := "OVN_Northbound", 6643
		if strings.Contains(command[3], "ovnsb") {
			database, port = "OVN_Southbound", 6644
		}
		if command[4] == "ovsdb-server/list-remotes" {
			return fmt.Appendf(nil, "ptcp:%d:0.0.0.0", port-2), nil
		}
		if command[4] != "cluster/status" {
			return nil, errors.New("unexpected native mutation")
		}
		return fmt.Appendf(nil, "Name: %s\nCluster ID: dddd (dddd0000-1111-2222-3333-444444444444)\nServer ID: %s (%s)\nAddress: tcp:192.0.2.1:%d\nStatus: cluster member\nRole: leader\nTerm: 7\nLeader: self\nLog: [1, %d]\nEntries not yet committed: 0\nEntries not yet applied: 0\nConnections:\nServers:\nmember (%s at tcp:192.0.2.1:%d) (self)\n", database, f.serverID[:4], f.serverID, port, f.index, f.serverID[:4], port), nil
	})}
	return f
}

func (f *fixture) ensure(t *testing.T) (*api.CentralTopologyReference, error) {
	t.Helper()
	var ref *api.CentralTopologyReference
	leases := lease.New(f.kube)
	err := leases.With(t.Context(), f.installation, lease.Installation, func(ctx context.Context) error {
		return leases.With(ctx, f.installation, lease.Central, func(ctx context.Context) error {
			var err error
			ref, err = f.provider.Ensure(ctx, f.installation)
			return err
		})
	})
	return ref, err
}

func TestDurableTopologySurvivesRestartAndRefusesIdentityChanges(t *testing.T) {
	for _, scenario := range []string{"stable", "commit advanced", "pod restarted", "pod lost", "database replaced", "node replaced", "volume replaced", "record replaced", "record missing", "record corrupted", "foreign owner", "missing status and query failure"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			ref, err := f.ensure(t)
			if err != nil || ref == nil {
				t.Fatalf("initial inventory: %+v %v", ref, err)
			}
			if scenario != "missing status and query failure" {
				f.installation.Status.Topology.Inventory = ref
				if err := f.kube.Status().Update(t.Context(), f.installation); err != nil {
					t.Fatal(err)
				}
			}
			object := new(corev1.ConfigMap)
			if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: ref.Name}, object); err != nil {
				t.Fatal(err)
			}
			original := object.Data["topology.json"]
			switch scenario {
			case "commit advanced":
				f.index += 100
			case "database replaced":
				f.serverID = "aaaa1111-1111-2222-3333-444444444444"
			case "pod restarted", "pod lost":
				pod := new(corev1.Pod)
				if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "default", Name: "central-0"}, pod); err != nil {
					t.Fatal(err)
				}
				if scenario == "pod lost" {
					if err := f.kube.Delete(t.Context(), pod); err != nil {
						t.Fatal(err)
					}
				} else {
					pod.UID = "new-pod"
					if err := f.kube.Update(t.Context(), pod); err != nil {
						t.Fatal(err)
					}
					pod.Status.ContainerStatuses[0].ContainerID = "containerd://restarted"
					if err := f.kube.Status().Update(t.Context(), pod); err != nil {
						t.Fatal(err)
					}
				}
			case "node replaced", "volume replaced":
				var target client.Object = &corev1.Node{Name: "node"}
				if scenario == "volume replaced" {
					target = &corev1.PersistentVolume{Name: "data"}
				}
				if err := f.kube.Get(t.Context(), client.ObjectKeyFromObject(target), target); err != nil {
					t.Fatal(err)
				}
				target.SetUID("replacement")
				if err := f.kube.Update(t.Context(), target); err != nil {
					t.Fatal(err)
				}
			case "record replaced":
				object.UID = "replacement"
			case "record corrupted":
				object.Data["topology.json"] = "{}"
			case "foreign owner":
				object.OwnerReferences = []metav1.OwnerReference{{UID: "owner"}}
			case "record missing":
				if err := f.kube.Delete(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			case "missing status and query failure":
				f.failQuery = true
			}
			if scenario == "record replaced" || scenario == "record corrupted" || scenario == "foreign owner" {
				if err := f.kube.Update(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
			// Construct a new provider just as a restarted operator would.
			f.provider = &Kubernetes{Client: f.kube, Runner: f.provider.Runner}
			got, err := f.ensure(t)
			succeeds := scenario == "stable" || scenario == "commit advanced" || scenario == "pod restarted"
			if succeeds && (err != nil || got == nil || *got != *ref) {
				t.Fatalf("stable identity not recovered: %+v %v", got, err)
			}
			if !succeeds && err == nil {
				t.Fatal("unverified identity accepted")
			}
			if scenario == "pod lost" || scenario == "missing status and query failure" {
				if got == nil || *got != *ref {
					t.Fatal("lost retained identity on observation failure")
				}
			}
			records := new(corev1.ConfigMapList)
			if err := f.kube.List(t.Context(), records); err != nil {
				t.Fatal(err)
			}
			if scenario == "record missing" {
				if len(records.Items) != 0 {
					t.Fatal("recreated missing identity")
				}
			} else if len(records.Items) != 1 {
				t.Fatal("created extra topology records")
			} else if scenario != "record corrupted" && records.Items[0].Data["topology.json"] != original {
				t.Fatal("rewrote retained topology")
			}
		})
	}
}

func TestHandoffCreatesNewImmutableBaselineAfterIdentityChange(t *testing.T) {
	f := newFixture(t)
	old, err := f.ensure(t)
	if err != nil {
		t.Fatal(err)
	}
	f.installation.Status.Topology.Inventory = old
	if err := f.kube.Status().Update(t.Context(), f.installation); err != nil {
		t.Fatal(err)
	}
	f.serverID = "aaaa1111-1111-2222-3333-444444444444"
	var next *api.CentralTopologyReference
	leases := lease.New(f.kube)
	err = leases.With(t.Context(), f.installation, lease.Installation, func(ctx context.Context) error {
		return leases.With(ctx, f.installation, lease.Central, func(ctx context.Context) error {
			var err error
			next, err = f.provider.Handoff(ctx, f.installation, old)
			return err
		})
	})
	if err != nil || next == nil {
		t.Fatalf("handoff failed: %+v %v", next, err)
	}
	if *next == *old {
		t.Fatal("handoff reused the old immutable baseline")
	}
	f.installation.Status.Topology.Inventory = next
	if err := f.kube.Status().Update(t.Context(), f.installation); err != nil {
		t.Fatal(err)
	}
	if _, err := f.provider.VerifyRetained(t.Context(), f.installation); err == nil {
		t.Fatal("verification unexpectedly ran without the central lease")
	}
	err = leases.With(t.Context(), f.installation, lease.Installation, func(ctx context.Context) error {
		return leases.With(ctx, f.installation, lease.Central, func(ctx context.Context) error {
			got, err := f.provider.VerifyRetained(ctx, f.installation)
			if err != nil || got == nil || *got != *next {
				return fmt.Errorf("new baseline was not retained: %+v %w", got, err)
			}
			return nil
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	records := new(corev1.ConfigMapList)
	if err := f.kube.List(t.Context(), records); err != nil || len(records.Items) != 2 {
		t.Fatalf("handoff did not retain both immutable records: %d %v", len(records.Items), err)
	}
}

func TestTopologyRecoversAcceptedCreateAndRequiresLeases(t *testing.T) {
	f := newFixture(t)
	if _, err := f.provider.Ensure(t.Context(), f.installation); err == nil {
		t.Fatal("recorded without operation Leases")
	}
	f.failCreate = true
	if _, err := f.ensure(t); err == nil {
		t.Fatal("lost response was not surfaced")
	}
	ref, err := f.ensure(t)
	if err != nil || ref == nil {
		t.Fatalf("accepted create not recovered: %+v %v", ref, err)
	}
}

func TestRetainedVerificationDoesNotInitializeBeforeFirstRollout(t *testing.T) {
	f := newFixture(t)
	f.failQuery = true
	leases := lease.New(f.kube)
	verify := func() (*api.CentralTopologyReference, error) {
		var ref *api.CentralTopologyReference
		err := leases.With(t.Context(), f.installation, lease.Installation, func(ctx context.Context) error {
			return leases.With(ctx, f.installation, lease.Central, func(ctx context.Context) error {
				var err error
				ref, err = f.provider.VerifyRetained(ctx, f.installation)
				return err
			})
		})
		return ref, err
	}
	if ref, err := verify(); err != nil || ref != nil {
		t.Fatalf("first rollout blocked: %+v %v", ref, err)
	}
	records := new(corev1.ConfigMapList)
	if err := f.kube.List(t.Context(), records); err != nil || len(records.Items) != 0 {
		t.Fatalf("initialized before discovery: %+v %v", records, err)
	}
	f.failQuery = false
	ref, err := f.ensure(t)
	if err != nil {
		t.Fatal(err)
	}
	f.failQuery = true
	got, err := verify()
	if err == nil || got == nil || *got != *ref {
		t.Fatalf("lost status bypassed retained verification: %+v %v", got, err)
	}
}
