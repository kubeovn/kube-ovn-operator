package health

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestLiveOVNDatabaseBackedClientListener(t *testing.T) {
	ovsdbBinary(t, "ovs-appctl")
	schemaDir := os.Getenv("OVN_TEST_SCHEMA_DIR")
	if schemaDir == "" {
		t.Skip("set OVN_TEST_SCHEMA_DIR for the native OVN schema")
	}
	root := t.TempDir()
	db := filepath.Join(root, "nb.db")
	runOVSDBTool(t, "create", db, filepath.Join(schemaDir, "ovn-nb.ovsschema"))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	socket, ctl := filepath.Join(root, "socket"), filepath.Join(root, "ctl")
	startTestProcess(t, root, exec.Command(ovsdbBinary(t, "ovsdb-server"), "--no-chdir", "--pidfile="+filepath.Join(root, "pid"), "--unixctl="+ctl, "--remote=punix:"+socket, "--remote=db:OVN_Northbound,NB_Global,connections", db))
	transaction := fmt.Sprintf(`["OVN_Northbound",{"op":"insert","table":"Connection","uuid-name":"listener","row":{"target":"ptcp:%d:127.0.0.1"}},{"op":"insert","table":"NB_Global","row":{"connections":["set",[["named-uuid","listener"]]]}}]`, port)
	waitOVSDB(t, func() error {
		return exec.CommandContext(t.Context(), ovsdbBinary(t, "ovsdb-client"), "--timeout=5", "transact", "unix:"+socket, transaction).Run()
	})
	installation, kube, objects := healthFixture(t)
	pod := objects[2].(*corev1.Pod)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "ovn-central", ContainerID: "native-process", Ready: true, State: corev1.ContainerState{Running: new(corev1.ContainerStateRunning)}}}
	if err := kube.Status().Update(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	bound, err := newMembershipRunner(t.Context(), kube, runnerFunc(func(ctx context.Context, _, _, _ string, command []string) ([]byte, error) {
		args := append([]string(nil), command...)
		args[0] = ovsdbBinary(t, command[0])
		if command[0] == "ovs-appctl" {
			args[3] = ctl
		} else {
			args[3] = "unix:" + socket
		}
		return exec.CommandContext(ctx, args[0], args[1:]...).Output()
	}), installation)
	if err != nil {
		t.Fatal(err)
	}
	got, err := bound.clientAddress(t.Context(), pod.Name, 0, "tcp:127.0.0.1:16643")
	want := fmt.Sprintf("tcp:127.0.0.1:%d", port)
	if err != nil || got != want {
		t.Fatalf("native database-backed listener: got %q, want %q: %v", got, want, err)
	}
	t.Log("Native list-remotes and NB_Global/Connection query resolved the actual client listener")
}
