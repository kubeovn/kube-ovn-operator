package health

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

// These tests use isolated loopback processes and temporary databases. CI sets
// OVSDB_TEST_BIN to extracted distribution binaries; no system service starts.
func ovsdbBinary(t *testing.T, name string) string {
	t.Helper()
	dir := os.Getenv("OVSDB_TEST_BIN")
	if dir == "" {
		t.Skip("set OVSDB_TEST_BIN to run live OVSDB metadata tests")
	}
	path := filepath.Join(dir, name)
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
	return path
}

func runOVSDBTool(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, ovsdbBinary(t, "ovsdb-tool"), args...).CombinedOutput(); err != nil {
		t.Fatalf("ovsdb-tool failed: %v: %s", err, output)
	}
}

func startOVSDB(t *testing.T, dir, db string) func() {
	t.Helper()
	command := exec.Command(ovsdbBinary(t, "ovsdb-server"), "--no-chdir", "--pidfile="+filepath.Join(dir, "pid"), "--unixctl="+filepath.Join(dir, "ctl"), "--remote=punix:"+filepath.Join(dir, "socket"), db)
	return startTestProcess(t, dir, command)
}

func startTestProcess(t *testing.T, dir string, command *exec.Cmd) func() {
	t.Helper()
	log, err := os.Create(filepath.Join(dir, "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = log, log
	if err := command.Start(); err != nil {
		_ = log.Close()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _ = command.Wait(); _ = log.Close(); close(done) }()
	stop := func() {
		select {
		case <-done:
			return
		default:
		}
		_ = command.Process.Signal(os.Interrupt)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = command.Process.Kill()
			<-done
		}
	}
	t.Cleanup(stop)
	return stop
}

func readLiveDatabase(t *testing.T, dir string) (Database, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	command, err := DatabaseCommand("OVN_Northbound")
	if err != nil {
		return Database{}, err
	}
	command[0] = ovsdbBinary(t, "ovsdb-client")
	command[3] = "unix:" + filepath.Join(dir, "socket")
	data, err := exec.CommandContext(ctx, command[0], command[1:]...).Output()
	if err != nil {
		return Database{}, err
	}
	return ParseDatabase(data, "OVN_Northbound")
}

func waitOVSDB(t *testing.T, check func() error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := check()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("OVSDB did not converge: %v", err)
		case <-ticker.C:
		}
	}
}

func writeTestSchema(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "schema")
	if err := os.WriteFile(path, []byte(`{"name":"OVN_Northbound","version":"1.0.0","tables":{"Root":{"isRoot":true,"columns":{"name":{"type":"string"}}}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLiveOVSDBStandaloneMetadata(t *testing.T) {
	ovsdbBinary(t, "ovsdb-server")
	dir := t.TempDir()
	db := filepath.Join(dir, "db")
	runOVSDBTool(t, "create", db, writeTestSchema(t, dir))
	startOVSDB(t, dir, db)
	waitOVSDB(t, func() error {
		state, err := readLiveDatabase(t, dir)
		if err != nil {
			return err
		}
		_, err = assessDatabase("OVN_Northbound", operatorv1alpha1.CentralModeSingle, []Database{state})
		return err
	})
}

func TestLiveOVSDBClusterLeaderFailoverAndQuorumLoss(t *testing.T) {
	ovsdbBinary(t, "ovsdb-server")
	root := t.TempDir()
	schema := writeTestSchema(t, root)
	dirs := make([]string, 3)
	addresses := make([]string, 3)
	listeners := make([]net.Listener, 3)
	stops := make([]func(), 3)
	for i := range 3 {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		listeners[i] = listener
		addresses[i] = "tcp:" + listener.Addr().String()
		dirs[i] = filepath.Join(root, fmt.Sprint(i))
		if err := os.Mkdir(dirs[i], 0700); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 3 {
		db := filepath.Join(dirs[i], "db")
		if i == 0 {
			runOVSDBTool(t, "--election-timer=1000", "create-cluster", db, schema, addresses[i])
		} else {
			runOVSDBTool(t, "join-cluster", db, "OVN_Northbound", addresses[i], addresses[0])
		}
		_ = listeners[i].Close()
		stops[i] = startOVSDB(t, dirs[i], db)
	}
	read := func(indices []int) ([]Database, error) {
		var states []Database
		for _, i := range indices {
			state, err := readLiveDatabase(t, dirs[i])
			if err != nil {
				return nil, err
			}
			states = append(states, state)
		}
		_, err := assessDatabase("OVN_Northbound", operatorv1alpha1.CentralModeRaft, states)
		return states, err
	}
	indices := []int{0, 1, 2}
	var original []Database
	waitOVSDB(t, func() error { var err error; original, err = read(indices); return err })
	var remaining []int
	for i, state := range original {
		if state.Leader {
			stops[i]()
		} else {
			remaining = append(remaining, i)
		}
	}
	var after []Database
	waitOVSDB(t, func() error { var err error; after, err = read(remaining); return err })
	survivor := -1
	for i, state := range after {
		if state.ClusterID != original[0].ClusterID {
			t.Fatal("cluster identity changed during failover")
		}
		if state.Leader {
			stops[remaining[i]]()
		} else {
			survivor = remaining[i]
		}
	}
	if survivor < 0 {
		t.Fatal("no follower survived")
	}
	waitOVSDB(t, func() error {
		state, err := readLiveDatabase(t, dirs[survivor])
		if err != nil {
			return err
		}
		if state.Connected {
			return fmt.Errorf("minority follower still reports connected")
		}
		if _, err := assessDatabase("OVN_Northbound", operatorv1alpha1.CentralModeRaft, []Database{state}); err == nil {
			return fmt.Errorf("minority accepted as healthy")
		}
		return nil
	})
}
