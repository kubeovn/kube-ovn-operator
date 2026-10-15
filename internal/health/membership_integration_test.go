package health

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type membershipRunFunc func(context.Context, string, []string) ([]byte, error)

func (f membershipRunFunc) Run(ctx context.Context, endpoint string, command []string) ([]byte, error) {
	return f(ctx, endpoint, command)
}

type membershipLossFunc func(context.Context, ovnadmin.Snapshot, membership.Change) error

func (f membershipLossFunc) VerifyPermanentLoss(ctx context.Context, snapshot ovnadmin.Snapshot, change membership.Change) error {
	return f(ctx, snapshot, change)
}

// The test provisions synthetic members explicitly. It tests the real admin
// boundary and 3->2->3 convergence, not the future Kubernetes join/workload FSM.
func TestLiveOVNMembershipReplacementAndKick(t *testing.T) {
	ovsdbBinary(t, "ovs-appctl")
	root := t.TempDir()
	schemaFile := writeTestSchema(t, root)
	dirs := map[string]string{}
	stops := map[string]func(){}
	transport := &ovnadmin.CommandClient{}
	toolOutput := func(args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, ovsdbBinary(t, "ovsdb-tool"), args...).Output()
		if err != nil {
			t.Fatalf("ovsdb-tool %v: %v", args, err)
		}
		return strings.TrimSpace(string(output))
	}
	prepare := func(index int, remote string) {
		t.Helper()
		name := fmt.Sprintf("member-%d", index)
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = listener.Close() }()
		address := "tcp:" + listener.Addr().String()
		db := filepath.Join(dir, "db")
		if index == 0 {
			runOVSDBTool(t, "--election-timer=1000", "create-cluster", db, schemaFile, address)
			transport.ClusterID = toolOutput("db-cid", db)
		} else {
			if remote == "" {
				remote = transport.Endpoints[0].Address
			}
			runOVSDBTool(t, "--cid="+transport.ClusterID, "join-cluster", db, "OVN_Northbound", address, remote)
		}
		transport.Endpoints = append(transport.Endpoints, ovnadmin.Endpoint{Name: name, ServerID: toolOutput("db-sid", db), Address: address})
		dirs[name] = dir
		_ = listener.Close()
		stops[name] = startOVSDB(t, dir, db)
	}
	for i := range 3 {
		prepare(i, "")
	}
	mutations := 0
	transport.Runner = membershipRunFunc(func(ctx context.Context, endpoint string, command []string) ([]byte, error) {
		dir, ok := dirs[endpoint]
		if !ok {
			return nil, fmt.Errorf("unknown test endpoint")
		}
		args := append([]string(nil), command...)
		args[0] = ovsdbBinary(t, "ovs-appctl")
		args[3] = filepath.Join(dir, "ctl")
		if command[4] != "cluster/status" {
			mutations++
		}
		return exec.CommandContext(ctx, args[0], args[1:]...).Output()
	})
	var leader string
	var snapshot ovnadmin.Snapshot
	observe := func(want int) error {
		for _, target := range transport.Endpoints {
			state, err := transport.Status(t.Context(), "OVN_Northbound", target.Name)
			if err != nil || len(state.Members) != want {
				continue
			}
			healthy := 0
			for _, member := range state.Members {
				if member.Healthy {
					healthy++
				}
			}
			if healthy != want {
				continue
			}
			leader, snapshot = target.Name, state
			return nil
		}
		return fmt.Errorf("waiting for %d healthy members", want)
	}
	waitOVSDB(t, func() error { return observe(3) })
	clientCommand := func(args ...string) []byte {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		output, err := exec.CommandContext(ctx, ovsdbBinary(t, "ovsdb-client"), args...).Output()
		if err != nil {
			t.Fatalf("ovsdb-client failed: %v", err)
		}
		return output
	}
	payload := "preserved-during-membership-change" + strings.Repeat("x", 72<<10)
	marker := fmt.Sprintf(`["OVN_Northbound",{"op":"insert","table":"Root","row":{"name":%q}}]`, payload)
	clientCommand("transact", "unix:"+filepath.Join(dirs[leader], "socket"), marker)
	waitOVSDB(t, func() error { return observe(3) })
	store, err := backup.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	provider := &backup.Provider{Source: &liveBackupSource{transport: transport, directories: dirs, client: ovsdbBinary(t, "ovsdb-client")}, Store: store, InstallationUID: "test-installation", MaxBytes: 1 << 20, MaxAge: time.Hour}
	executor := ovnadmin.Executor{Client: transport, Backups: provider}
	var backupRef string

	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	leases := lease.New(fake.NewClientBuilder().WithScheme(scheme).Build())
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "membership", Namespace: "test", UID: "test-installation"}
	guarded := func(fn func(context.Context) error) error {
		return leases.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
			return leases.With(ctx, installation, lease.Central, fn)
		})
	}
	execute := func(change membership.Change) error {
		change.BackupRef = backupRef
		return guarded(func(ctx context.Context) error { return executor.Execute(ctx, leader, change) })
	}
	capture := func() {
		t.Helper()
		if err := guarded(func(ctx context.Context) error {
			var err error
			backupRef, err = provider.Capture(ctx, "OVN_Northbound", leader)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}

	var removed membership.Member
	for _, member := range snapshot.Members {
		if !member.Leader {
			removed = member
			break
		}
	}
	capture()
	// Serve the published native snapshot in a fresh standalone process. This
	// proves native schema/data recovery, including row UUIDs and >64 KiB data,
	// without changing or restoring over the live RAFT cluster.
	_, contents, err := store.Open(t.Context(), backupRef)
	if err != nil {
		t.Fatal(err)
	}
	restoredDirectory := t.TempDir()
	restoredPath := filepath.Join(restoredDirectory, "restored.db")
	restoredFile, err := os.OpenFile(restoredPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(restoredFile, contents)
	closeErr := restoredFile.Close()
	_ = contents.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatalf("restore copy: %v %v", copyErr, closeErr)
	}
	startOVSDB(t, restoredDirectory, restoredPath)
	query := `["OVN_Northbound",{"op":"select","table":"Root","where":[],"columns":["_uuid","name"]}]`
	original := clientCommand("query", "unix:"+filepath.Join(dirs[leader], "socket"), query)
	waitOVSDB(t, func() error {
		_, err := os.Stat(filepath.Join(restoredDirectory, "socket"))
		return err
	})
	restored := clientCommand("query", "unix:"+filepath.Join(restoredDirectory, "socket"), query)
	var originalRows, restoredRows any
	if json.Unmarshal(original, &originalRows) != nil || json.Unmarshal(restored, &restoredRows) != nil || !reflect.DeepEqual(originalRows, restoredRows) || !strings.Contains(string(restored), payload) {
		t.Fatal("native restore did not preserve committed data and row UUIDs")
	}
	t.Log("persisted backup restored into a native OVSDB process with original row UUIDs and data")

	change := membership.Change{Operation: membership.OperationLeave, ClusterID: snapshot.ClusterID, Member: removed, Reason: "replace synthetic member"}
	if err := execute(change); err != nil {
		t.Fatal(err)
	}
	waitOVSDB(t, func() error { return observe(2) })
	t.Log("healthy member left: three members converged to two")
	// Joining requires creating a new database and starting a new process. No
	// cluster/join unixctl command exists; the transport must not invent one.
	joined := membership.Member{ServerID: "eeee0000-1111-2222-3333-444444444444", Address: "tcp:127.0.0.1:65534"}
	change = membership.Change{Operation: membership.OperationJoin, ClusterID: snapshot.ClusterID, Member: joined, Reason: "replacement"}
	capture()
	before := mutations
	if err := execute(change); err == nil || !strings.Contains(err.Error(), "provisioned new database") || mutations != before {
		t.Fatalf("unprovisioned join: mutations=%d err=%v", mutations, err)
	}
	// Use the current leader as bootstrap remote without modifying pinned entries.
	bootstrap := ""
	for _, target := range transport.Endpoints {
		if target.Name == leader {
			bootstrap = target.Address
			break
		}
	}
	prepare(3, bootstrap)
	waitOVSDB(t, func() error { return observe(3) })
	for _, member := range snapshot.Members {
		if member.ServerID == removed.ServerID {
			t.Fatal("departed member reappeared")
		}
	}
	t.Log("new server identity joined: two members converged to three")
	var lost membership.Member
	var lostEndpoint string
	for _, member := range snapshot.Members {
		if !member.Leader {
			lost = member
			break
		}
	}
	for _, target := range transport.Endpoints {
		if target.ServerID == lost.ServerID {
			lostEndpoint = target.Name
		}
	}
	stops[lostEndpoint]()
	waitOVSDB(t, func() error {
		state, err := transport.Status(t.Context(), "OVN_Northbound", leader)
		if err != nil {
			return err
		}
		for _, member := range state.Members {
			if member.ServerID == lost.ServerID && !member.Healthy {
				snapshot = state
				return nil
			}
		}
		return fmt.Errorf("stopped member still healthy")
	})
	change = membership.Change{Operation: membership.OperationKick, ClusterID: snapshot.ClusterID, Member: lost, Reason: "explicitly approved synthetic process loss", PermanentLossApproved: true}
	capture()
	before = mutations
	if err := execute(change); err == nil || mutations != before {
		t.Fatal("kick bypassed approval verification")
	}
	executor.PermanentLoss = membershipLossFunc(func(_ context.Context, state ovnadmin.Snapshot, action membership.Change) error {
		if state.ClusterID != transport.ClusterID || action.Member.ServerID != lost.ServerID || action.Member.Address != lost.Address {
			return fmt.Errorf("approval target mismatch")
		}
		return nil
	})
	if err := execute(change); err != nil {
		t.Fatal(err)
	}
	waitOVSDB(t, func() error { return observe(2) })
	t.Log("confirmed lost member removed only after approval verification")
	data := clientCommand("query", "unix:"+filepath.Join(dirs[leader], "socket"), `["OVN_Northbound",{"op":"select","table":"Root","where":[],"columns":["name"]}]`)
	if !strings.Contains(string(data), "preserved-during-membership-change") {
		t.Fatal("committed data did not survive replacement and removal")
	}
	for _, member := range snapshot.Members {
		if member.Leader {
			continue
		}
		for _, target := range transport.Endpoints {
			if target.ServerID == member.ServerID {
				stops[target.Name]()
			}
		}
	}
	change = membership.Change{Operation: membership.OperationLeave, ClusterID: snapshot.ClusterID, Member: snapshot.Members[0], Reason: "must refuse without quorum"}
	before = mutations
	if err := execute(change); err == nil || mutations != before {
		t.Fatalf("quorum loss allowed mutation: count=%d err=%v", mutations, err)
	}
	t.Log("quorum loss blocked further membership writes; committed data was preserved")
}

// liveBackupSource adapts isolated test processes. Production uses the owned
// Pod/container adapter from NewBackupSource instead.
type liveBackupSource struct {
	transport   *ovnadmin.CommandClient
	directories map[string]string
	client      string
}

func (s *liveBackupSource) Status(ctx context.Context, database, endpoint string) (ovnadmin.Snapshot, error) {
	return s.transport.Status(ctx, database, endpoint)
}

func (s *liveBackupSource) Identity(_ context.Context, endpoint string) (backup.Identity, error) {
	pid, err := os.ReadFile(filepath.Join(s.directories[endpoint], "pid"))
	return backup.Identity{InstallationUID: "test-installation", PodUID: endpoint, ContainerID: strings.TrimSpace(string(pid))}, err
}

func (s *liveBackupSource) Stream(ctx context.Context, database, endpoint string, destination io.Writer) error {
	command := exec.CommandContext(ctx, s.client, "--timeout=10", "backup", "unix:"+filepath.Join(s.directories[endpoint], "socket"), database)
	command.Stdout = destination
	return command.Run()
}
