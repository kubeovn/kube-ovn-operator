// This exercises native member provisioning in kind's existing cluster. It
// does not install Kube-OVN or claim to verify node replacement/network traffic.
package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"time"
	"uuid"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/lifecycle"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Durable joint NB/SB replacement, native preparation/startup, backup verification, retirement and preserved data passed")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}
	if err := api.AddToScheme(scheme); err != nil {
		return err
	}
	config, err := ctrl.GetConfig()
	if err != nil {
		return err
	}
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	exec, err := health.NewPodExecutor(config)
	if err != nil {
		return err
	}
	installation := &api.KubeOVNInstallation{Name: "member-smoke", Namespace: "kube-ovn-operator-system", Spec: api.KubeOVNInstallationSpec{Version: "v1.16.13", Mode: api.InstallationModeControlPlaneOnly, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 3}}}
	if err := kube.Create(ctx, installation); err != nil {
		return err
	}
	node := new(corev1.Node)
	if err := kube.Get(ctx, client.ObjectKey{Name: os.Getenv("MEMBER_TEST_NODE")}, node); err != nil {
		return err
	}
	var ip string
	for _, address := range node.Status.Addresses {
		if address.Type == corev1.NodeInternalIP && !strings.Contains(address.Address, ":") {
			ip = address.Address
			break
		}
	}
	if parsed, err := netip.ParseAddr(ip); err != nil || !parsed.Is4() {
		return errors.New("test requires a kind node InternalIPv4 address")
	}
	image := os.Getenv("MEMBER_TEST_IMAGE")
	if image == "" {
		return errors.New("MEMBER_TEST_IMAGE is required")
	}
	seed := &corev1.Pod{Name: "member-seed-0", Namespace: installation.Namespace, Spec: corev1.PodSpec{
		NodeName: node.Name, HostNetwork: true, DNSPolicy: corev1.DNSClusterFirstWithHostNet, AutomountServiceAccountToken: new(false),
		SecurityContext: &corev1.PodSecurityContext{RunAsUser: new(int64(65532)), RunAsGroup: new(int64(65532)), RunAsNonRoot: new(true)},
		Containers: []corev1.Container{{Name: "ovn-central", Image: image, ImagePullPolicy: corev1.PullIfNotPresent,
			Command:         []string{"/bin/sh", "-ec", seedCommand(ip)},
			SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: new(false), Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
			VolumeMounts:    []corev1.VolumeMount{{Name: "work", MountPath: "/work"}, {Name: "run", MountPath: "/var/run/ovn"}},
		}}, Volumes: []corev1.Volume{{Name: "work", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}, {Name: "run", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}},
	}}
	seedLabels := map[string]string{"app": "ovn-central", "operator.kubeovn.io/test": "seed"}
	seedSet := &appsv1.StatefulSet{Name: "member-seed", Namespace: installation.Namespace,
		Labels:          map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: installation.Name, apply.InstallationNamespaceLabel: installation.Namespace, apply.InstallationUIDLabel: string(installation.UID)},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: new(true)}},
		Spec:            appsv1.StatefulSetSpec{Replicas: new(int32(1)), ServiceName: "member-seed", Selector: &metav1.LabelSelector{MatchLabels: seedLabels}, UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}, Template: corev1.PodTemplateSpec{Labels: seedLabels, Spec: seed.Spec}}}
	if err := kube.Create(ctx, seedSet); err != nil {
		return err
	}
	var nbCID, sbCID string
	if err := wait(ctx, func() (bool, error) {
		out, err := exec.Run(ctx, seed.Namespace, seed.Name, "ovn-central", []string{"ovsdb-tool", "db-cid", "/work/db/nb"})
		if err != nil {
			return false, nil
		}
		nbCID = strings.TrimSpace(string(out))
		out, err = exec.Run(ctx, seed.Namespace, seed.Name, "ovn-central", []string{"ovsdb-tool", "db-cid", "/work/db/sb"})
		if err != nil {
			return false, nil
		}
		sbCID = strings.TrimSpace(string(out))
		return true, nil
	}); err != nil {
		return fmt.Errorf("seed readiness: %w", err)
	}
	// Preserve an actual business row and its UUID across the Kubernetes handoff.
	transaction := `[{"op":"insert","table":"Logical_Switch","row":{"name":"provisioner-smoke","external_ids":["map",[["owner","member-smoke"]]]}}]`
	insert := `["OVN_Northbound",` + transaction[1:]
	var original string
	if err := wait(ctx, func() (bool, error) {
		out, err := exec.Run(ctx, seed.Namespace, seed.Name, "ovn-central", []string{"ovsdb-client", "transact", "tcp:" + ip + ":16641", insert})
		if err != nil {
			return false, nil
		}
		var rows []struct {
			UUID  []string `json:"uuid"`
			Error string   `json:"error"`
		}
		if err := json.Unmarshal(out, &rows); err != nil {
			return false, err
		}
		if len(rows) != 1 || len(rows[0].UUID) != 2 {
			return false, fmt.Errorf("seed insert failed: %s", out)
		}
		original = rows[0].UUID[1]
		return true, nil
	}); err != nil {
		return err
	}
	claim := &corev1.PersistentVolumeClaim{Name: "member-data", Namespace: installation.Namespace, Spec: corev1.PersistentVolumeClaimSpec{
		VolumeName: "member-smoke-volume", StorageClassName: new(""), AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		Resources: corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
	}}
	if err := kube.Create(ctx, claim); err != nil {
		return err
	}
	volume := &corev1.PersistentVolume{Name: claim.Spec.VolumeName, Spec: corev1.PersistentVolumeSpec{
		PersistentVolumeSource: corev1.PersistentVolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/tmp/operator-member-volume", Type: new(corev1.HostPathDirectory)}},
		Capacity:               corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}, AccessModes: claim.Spec.AccessModes, PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimRetain,
		ClaimRef:     &corev1.ObjectReference{Name: claim.Name, Namespace: claim.Namespace, UID: claim.UID},
		NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchFields: []corev1.NodeSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{node.Name}}}}}}},
	}}
	if err := kube.Create(ctx, volume); err != nil {
		return err
	}
	if err := wait(ctx, func() (bool, error) {
		if err := kube.Get(ctx, client.ObjectKeyFromObject(claim), claim); err != nil {
			return false, err
		}
		if err := kube.Get(ctx, client.ObjectKeyFromObject(volume), volume); err != nil {
			return false, err
		}
		return claim.Status.Phase == corev1.ClaimBound && volume.Status.Phase == corev1.VolumeBound, nil
	}); err != nil {
		return err
	}
	request := workload.Request{InstallationGeneration: installation.Generation, Node: workload.Reference{Name: node.Name, UID: node.UID}, Claim: workload.Reference{Name: claim.Name, UID: claim.UID}, Volume: workload.Reference{Name: volume.Name, UID: volume.UID}, Image: image,
		Preparation: prepare.Plan{FormatVersion: 1, InstallationUID: string(installation.UID), OperationID: uuid.New().String(), MemberName: "member-smoke", Databases: []prepare.Database{
			{Name: "OVN_Northbound", ClusterID: nbCID, LocalAddress: "tcp:" + ip + ":6643", RemoteAddresses: []string{"tcp:" + ip + ":16643"}},
			{Name: "OVN_Southbound", ClusterID: sbCID, LocalAddress: "tcp:" + ip + ":6644", RemoteAddresses: []string{"tcp:" + ip + ":16644"}},
		}}}
	retirement, err := lifecycle.PinRetirement(ctx, kube, installation, workload.Reference{Name: seedSet.Name, UID: seedSet.UID})
	if err != nil {
		return err
	}
	operation := lifecycle.Request{FormatVersion: 1, InstallationUID: string(installation.UID), InstallationGeneration: installation.Generation,
		OperationID: request.Preparation.OperationID, RemoveEndpoint: seed.Name, Retirement: &retirement,
		Add: &lifecycle.Addition{Workload: request, NorthboundAddress: "tcp:" + ip + ":6641", SouthboundAddress: "tcp:" + ip + ":6642"}}
	for i, db := range request.Preparation.Databases {
		filename := []string{"nb", "sb"}[i]
		out, err := exec.Run(ctx, seed.Namespace, seed.Name, "ovn-central", []string{"ovsdb-tool", "db-sid", "/work/db/" + filename})
		if err != nil {
			return err
		}
		operation.Databases = append(operation.Databases, lifecycle.Inventory{Database: db.Name, ClusterID: db.ClusterID, Peers: []lifecycle.Peer{{Name: seed.Name, ServerID: strings.TrimSpace(string(out)), Address: db.RemoteAddresses[0], ClientAddress: fmt.Sprintf("tcp:%s:%d", ip, 16641+i)}}})
	}
	backupRoot, err := os.MkdirTemp("", "operator-lifecycle-backup-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(backupRoot) }()
	store, err := backup.OpenFileStore(backupRoot)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	newExecutor := func() (*lifecycle.Executor, error) {
		return lifecycle.NewKubernetes(kube, installation, exec, store, time.Hour, 16<<20)
	}
	m := lease.New(kube)
	guarded := func(fn func(context.Context) error) error {
		return m.With(ctx, installation, lease.Installation, func(ctx context.Context) error { return m.With(ctx, installation, lease.Central, fn) })
	}
	var ref lifecycle.Reference
	if err := wait(ctx, func() (bool, error) {
		err := guarded(func(ctx context.Context) error {
			e, err := newExecutor()
			if err != nil {
				return err
			}
			ref, err = e.Begin(ctx, operation)
			return err
		})
		if err != nil {
			fmt.Printf("Awaiting joint operation admission: %v\n", err)
		}
		return err == nil, nil
	}); err != nil {
		return err
	}
	var state lifecycle.State
	if err := wait(ctx, func() (bool, error) {
		// A new executor and fresh pod transports on every step model restarts.
		err := guarded(func(ctx context.Context) error {
			e, err := newExecutor()
			if err != nil {
				return err
			}
			state, err = e.Step(ctx, ref)
			return err
		})
		if err != nil {
			fmt.Printf("Joint lifecycle %s awaiting convergence: %v\n", state.Phase, err)
		}
		if err == nil {
			fmt.Printf("Joint lifecycle phase: %s\n", state.Phase)
		}
		return err == nil && state.Phase == lifecycle.Complete, nil
	}); err != nil {
		return fmt.Errorf("joint lifecycle: %w", err)
	}
	running := state.Running
	if len(state.Final) != 2 {
		return errors.New("joint completion lacks both database observations")
	}
	for _, snapshot := range state.Final {
		if len(snapshot.Members) != 1 || !snapshot.Members[0].Healthy {
			return errors.New("old database member remains or replacement is not healthy")
		}
	}
	if err := kube.Get(ctx, client.ObjectKeyFromObject(seedSet), seedSet); err != nil {
		return err
	}
	if seedSet.Spec.Replicas == nil || *seedSet.Spec.Replicas != 0 {
		return errors.New("old member workload was not retired")
	}
	if err := kube.Get(ctx, client.ObjectKey{Namespace: seed.Namespace, Name: seed.Name}, new(corev1.Pod)); err == nil || client.IgnoreNotFound(err) != nil {
		return errors.New("old member pod remains after joint completion")
	}
	prepared := workload.Preparation{Record: state.MemberRecord, Result: state.Prepared}
	set := new(appsv1.StatefulSet)
	podName := running.Name + "-0"
	query := `["OVN_Northbound",{"op":"select","table":"Logical_Switch","where":[["name","==","provisioner-smoke"]],"columns":["_uuid","external_ids"]}]`
	if err := wait(ctx, func() (bool, error) {
		pod := new(corev1.Pod)
		if err := kube.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: podName}, pod); err != nil {
			return false, client.IgnoreNotFound(err)
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.RestartCount != 0 || status.State.Terminated != nil {
				return false, errors.New("native member exited during initial startup; inspect container logs")
			}
		}
		for i, db := range []struct{ socket, name string }{{"ovnnb_db", "OVN_Northbound"}, {"ovnsb_db", "OVN_Southbound"}} {
			out, err := exec.Run(ctx, installation.Namespace, podName, "ovn-central", []string{"ovs-appctl", "-t", "/var/run/ovn/" + db.socket + ".ctl", "cluster/status", db.name})
			if err != nil {
				return false, nil
			}
			member, err := ovnadmin.ParseClusterStatus(out, db.name)
			if err != nil || member.State != "cluster member" || len(member.Servers) != 1 || member.CommitIndex < 1 || member.AppliedIndex < member.CommitIndex {
				return false, nil
			}
			expected := prepared.Result.Databases[i]
			if member.ClusterID != expected.ClusterID || member.ServerID != expected.ServerID || member.Address != expected.Address {
				return false, errors.New("native identities differ from the recorded prepared result")
			}
		}
		out, err := exec.Run(ctx, installation.Namespace, podName, "ovn-central", []string{"ovsdb-client", "query", "unix:/var/run/ovn/ovnnb_db.sock", query})
		if err != nil {
			return false, nil
		}
		return strings.Contains(string(out), original) && strings.Contains(string(out), "member-smoke"), nil
	}); err != nil {
		return fmt.Errorf("native join/data verification: %w", err)
	}
	if err := guarded(func(ctx context.Context) error {
		e, err := newExecutor()
		if err != nil {
			return err
		}
		again, err := e.Step(ctx, ref)
		if err == nil && (again.Phase != lifecycle.Complete || again.Running != running) {
			return errors.New("completed operation retry changed the workload identity")
		}
		return err
	}); err != nil {
		return err
	}

	if err := kube.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: running.Name}, set); err != nil {
		return err
	}
	if set.UID != running.UID {
		return errors.New("workload UID changed")
	}
	fmt.Printf("Verified member record %s, Job-produced NB/SB identities, StatefulSet %s and retained row UUID %s\n", prepared.Record.Name, running.Name, original)
	return nil
}

func wait(ctx context.Context, observe func() (bool, error)) error {
	for {
		ready, err := observe()
		if err != nil || ready {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

func seedCommand(ip string) string {
	// The address is read from a kind Node and must be a literal IPv4 address.
	return fmt.Sprintf(`umask 077
mkdir -p /work/db /var/run/ovn
ovsdb-tool create-cluster /work/db/nb /usr/share/ovn/ovn-nb.ovsschema tcp:%[1]s:16643
ovsdb-tool create-cluster /work/db/sb /usr/share/ovn/ovn-sb.ovsschema tcp:%[1]s:16644
ovsdb-server /work/db/nb --remote=ptcp:16641:%[1]s --remote=punix:/var/run/ovn/ovnnb_db.sock --unixctl=/var/run/ovn/ovnnb_db.ctl --pidfile=/var/run/ovn/ovnnb_db.pid &
nb_pid=$!
ovsdb-server /work/db/sb --remote=ptcp:16642:%[1]s --remote=punix:/var/run/ovn/ovnsb_db.sock --unixctl=/var/run/ovn/ovnsb_db.ctl --pidfile=/var/run/ovn/ovnsb_db.pid &
sb_pid=$!
trap 'kill "$nb_pid" "$sb_pid"; wait' TERM INT EXIT
wait
`, ip)
}
