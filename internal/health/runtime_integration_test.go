package health

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func ovnBinary(t *testing.T, name string) string {
	t.Helper()
	dir := os.Getenv("OVN_TEST_BIN")
	if dir == "" {
		t.Skip("set OVN_TEST_BIN and schema paths to run live OVN runtime tests")
	}
	path := filepath.Join(dir, name)
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	return path
}

func liveDatabase(t *testing.T, root, name, schema string) (string, func()) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(dir, "db")
	runOVSDBTool(t, "create", db, schema)
	stop := startOVSDB(t, dir, db)
	waitOVSDB(t, func() error { _, err := os.Stat(filepath.Join(dir, "socket")); return err })
	return "unix:" + filepath.Join(dir, "socket"), stop
}

func liveCommand(t *testing.T, command []string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, command[0], command[1:]...).Output()
}

func startRuntimeDaemon(t *testing.T, root, name string, args ...string) (string, func()) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	control := filepath.Join(dir, "ctl")
	args = append(args, "--no-chdir", "--unixctl="+control, "--pidfile="+filepath.Join(dir, "pid"))
	command := exec.Command(ovnBinary(t, name), args...)
	command.Env = append(os.Environ(), "OVS_RUNDIR="+dir, "OVN_RUNDIR="+dir)
	stop := startTestProcess(t, dir, command)
	waitOVSDB(t, func() error { _, err := os.Stat(control); return err })
	return control, stop
}

func TestLiveOVNNorthdActiveStandbyPauseAndDisconnect(t *testing.T) {
	ovnBinary(t, "ovn-northd")
	root := t.TempDir()
	schemas := os.Getenv("OVN_TEST_SCHEMA_DIR")
	nb, _ := liveDatabase(t, root, "nb", filepath.Join(schemas, "ovn-nb.ovsschema"))
	sb, stopSB := liveDatabase(t, root, "sb", filepath.Join(schemas, "ovn-sb.ovsschema"))
	var controls []string
	for i := range 2 {
		dir := filepath.Join(root, fmt.Sprintf("northd-%d", i))
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		control := filepath.Join(dir, "ctl")
		command := exec.Command(ovnBinary(t, "ovn-northd"), "--no-chdir", "--unixctl="+control, "--pidfile="+filepath.Join(dir, "pid"), "--ovnnb-db="+nb, "--ovnsb-db="+sb)
		command.Env = append(os.Environ(), "OVN_RUNDIR="+dir)
		startTestProcess(t, dir, command)
		controls = append(controls, control)
	}
	query := func(control, operation string) ([]byte, error) {
		return liveCommand(t, []string{ovnBinary(t, "ovn-appctl"), "--timeout=3", "-t", control, operation})
	}
	active := ""
	waitOVSDB(t, func() error {
		count := 0
		for _, control := range controls {
			data, err := query(control, "status")
			if err != nil {
				return err
			}
			state, err := parseNorthdState(data)
			if err != nil {
				return err
			}
			if state == "active" {
				count++
				active = control
			}
			for _, operation := range []string{"nb-connection-status", "sb-connection-status"} {
				data, err := query(control, operation)
				if err != nil {
					return err
				}
				if !connectionReady(data) {
					return fmt.Errorf("northd not connected")
				}
			}
		}
		if count != 1 {
			return fmt.Errorf("observed %d active northd processes", count)
		}
		return nil
	})
	if _, err := query(active, "pause"); err != nil {
		t.Fatal(err)
	}
	data, err := query(active, "status")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseNorthdState(data); err == nil {
		t.Fatal("paused northd accepted as healthy")
	}
	stopSB()
	waitOVSDB(t, func() error {
		data, err := query(controls[0], "sb-connection-status")
		if err != nil {
			return err
		}
		if connectionReady(data) {
			return fmt.Errorf("northd retained stale SB connection")
		}
		return nil
	})
}

func TestLiveOVNDataPlaneBridgeAndSBDisconnect(t *testing.T) {
	ovnBinary(t, "ovn-controller")
	root := t.TempDir()
	sb, stopSB := liveDatabase(t, root, "sb", filepath.Join(os.Getenv("OVN_TEST_SCHEMA_DIR"), "ovn-sb.ovsschema"))
	ovs, _ := liveDatabase(t, root, "ovs", os.Getenv("OVS_TEST_SCHEMA"))
	command := []string{ovnBinary(t, "ovs-vsctl"), "--timeout=5", "--no-wait", "--db=" + ovs, "init", "--", "set", "Open_vSwitch", ".", "external_ids:system-id=" + serverID, "external_ids:ovn-remote=" + sb, "external_ids:ovn-encap-type=geneve", "external_ids:ovn-encap-ip=127.0.0.1", "--", "add-br", "br-int", "--", "set", "Bridge", "br-int", "datapath_type=dummy"}
	if output, err := liveCommand(t, command); err != nil {
		t.Fatalf("initialize test OVS: %v %s", err, output)
	}
	vswitch, _ := startRuntimeDaemon(t, root, "ovs-vswitchd", "--enable-dummy", ovs)
	controller, _ := startRuntimeDaemon(t, root, "ovn-controller", ovs)
	observer := RuntimeObserver{Runner: runnerFunc(func(ctx context.Context, _, _, _ string, command []string) ([]byte, error) {
		command = slices.Clone(command)
		command[0] = ovnBinary(t, command[0])
		for i, arg := range command {
			switch arg {
			case "ovn-controller":
				command[i] = controller
			case "ovs-vswitchd":
				command[i] = vswitch
			default:
				if strings.HasPrefix(arg, "--db=") {
					command[i] = "--db=" + ovs
				}
			}
		}
		return exec.CommandContext(ctx, command[0], command[1:]...).Output()
	})}
	pod := corev1.Pod{Name: "ovs-local", Spec: corev1.PodSpec{NodeName: "node-local"}}
	waitOVSDB(t, func() error {
		state, err := observer.observeOVSPod(t.Context(), pod)
		if err != nil {
			return err
		}
		if state.ChassisID != serverID || !state.IntegrationBridgeReady || !state.SBConnected {
			return fmt.Errorf("incomplete runtime evidence")
		}
		return nil
	})
	stopSB()
	waitOVSDB(t, func() error {
		if _, err := observer.observeOVSPod(t.Context(), pod); err == nil {
			return fmt.Errorf("disconnected SB was accepted")
		}
		return nil
	})
}
