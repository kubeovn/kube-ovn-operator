package central

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"uuid"

	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
)

type nativeRunner func(context.Context, string, []string) ([]byte, error)

func (f nativeRunner) Run(ctx context.Context, endpoint string, command []string) ([]byte, error) {
	return f(ctx, endpoint, command)
}

type launchFixture struct {
	options    Options
	request    Request
	transports map[string]*ovnadmin.CommandClient
	queries    map[string]string
	original   map[string][]byte
	toolDir    string
	ovnDir     string
}

func nativeCall(t *testing.T, binary string, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binary, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %s %v", filepath.Base(binary), args, out, err)
	}
	return out
}

func newLaunchFixture(t *testing.T) *launchFixture {
	t.Helper()
	toolDir, ovnDir, schemas := os.Getenv("OVSDB_TEST_BIN"), os.Getenv("OVN_TEST_BIN"), os.Getenv("OVN_TEST_SCHEMA_DIR")
	if toolDir == "" || ovnDir == "" || schemas == "" {
		t.Skip("set OVSDB_TEST_BIN, OVN_TEST_BIN and OVN_TEST_SCHEMA_DIR for native launch tests")
	}
	root, err := os.MkdirTemp("", "central-launch-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	privateDir := func(name string) string {
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		return dir
	}
	f := &launchFixture{options: Options{Root: privateDir("data"), RuntimeDirectory: privateDir("run"), OVSDBTool: filepath.Join(toolDir, "ovsdb-tool"), OVSDBServer: filepath.Join(toolDir, "ovsdb-server"), Northd: filepath.Join(ovnDir, "ovn-northd"), ShutdownTimeout: time.Second}, toolDir: toolDir, ovnDir: ovnDir, transports: map[string]*ovnadmin.CommandClient{}, queries: map[string]string{}, original: map[string][]byte{}}
	f.request = Request{FormatVersion: 1, Preparation: prepare.Plan{FormatVersion: 1, InstallationUID: "native-launch-installation", OperationID: uuid.New().String(), MemberName: "central-3"}}
	log, err := os.OpenFile(filepath.Join(root, "launch.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = log.Close()
		if t.Failed() {
			if data, err := os.ReadFile(log.Name()); err == nil {
				if len(data) > 16<<10 {
					data = data[len(data)-(16<<10):]
				}
				t.Logf("native launch log: %s", data)
			}
		}
	})
	f.options.Stderr = log
	for _, database := range []string{"OVN_Northbound", "OVN_Southbound"} {
		name, schema, table := "ovnnb_db", "ovn-nb.ovsschema", "Logical_Switch"
		if database == "OVN_Southbound" {
			name, schema, table = "ovnsb_db", "ovn-sb.ovsschema", "Chassis_Private"
		}
		var raft, clients []string
		var listeners []net.Listener
		for range 8 {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			listeners = append(listeners, listener)
			address := "tcp:" + listener.Addr().String()
			if len(raft) < 4 {
				raft = append(raft, address)
			} else {
				clients = append(clients, address)
			}
		}
		directories := map[string]string{"central-3": f.options.RuntimeDirectory}
		transport := &ovnadmin.CommandClient{Runner: nativeRunner(func(ctx context.Context, endpoint string, args []string) ([]byte, error) {
			command := append([]string(nil), args...)
			command[0], command[3] = filepath.Join(toolDir, "ovs-appctl"), filepath.Join(directories[endpoint], name+".ctl")
			return exec.CommandContext(ctx, command[0], command[1:]...).Output()
		})}
		for i := range 3 {
			endpoint := fmt.Sprintf("central-%d", i)
			dir := privateDir(name + "-" + strconv.Itoa(i))
			directories[endpoint] = dir
			file := filepath.Join(dir, "db")
			if i == 0 {
				nativeCall(t, f.options.OVSDBTool, "--election-timer=1000", "create-cluster", file, filepath.Join(schemas, schema), raft[i])
				transport.ClusterID = strings.TrimSpace(string(nativeCall(t, f.options.OVSDBTool, "db-cid", file)))
			} else {
				nativeCall(t, f.options.OVSDBTool, "--cid="+transport.ClusterID, "join-cluster", file, database, raft[i], raft[0])
			}
			transport.Endpoints = append(transport.Endpoints, ovnadmin.Endpoint{Name: endpoint, ServerID: strings.TrimSpace(string(nativeCall(t, f.options.OVSDBTool, "db-sid", file))), Address: raft[i]})
			_ = listeners[i].Close()
			_ = listeners[i+4].Close()
			passive, err := passiveAddress(clients[i])
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(f.options.OVSDBServer, "--no-chdir", "--pidfile="+filepath.Join(dir, "pid"), "--unixctl="+filepath.Join(dir, name+".ctl"), "--remote="+passive, "--remote=punix:"+filepath.Join(dir, "socket"), file)
			cmd.Stderr = log
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
		}
		f.transports[database] = transport
		f.waitMembers(t, database, 3)
		row := fmt.Sprintf(`[%q,{"op":"insert","table":%q,"row":{"name":"launcher-data","external_ids":["map",[["launcher","preserved"]]]}}]`, database, table)
		out := nativeCall(t, filepath.Join(toolDir, "ovsdb-client"), "--timeout=3", "transact", "unix:"+filepath.Join(directories["central-0"], "socket"), row)
		if strings.Contains(string(out), `"error"`) {
			t.Fatalf("insert failed: %s", out)
		}
		query := fmt.Sprintf(`[%q,{"op":"select","table":%q,"where":[["name","==","launcher-data"]],"columns":["_uuid","name","external_ids"]}]`, database, table)
		f.queries[database] = query
		f.original[database] = nativeCall(t, filepath.Join(toolDir, "ovsdb-client"), "--timeout=3", "query", "unix:"+filepath.Join(directories["central-0"], "socket"), query)
		if !strings.Contains(string(f.original[database]), "preserved") {
			t.Fatal("fixture data missing")
		}
		f.request.Preparation.Databases = append(f.request.Preparation.Databases, prepare.Database{Name: database, ClusterID: transport.ClusterID, LocalAddress: raft[3], RemoteAddresses: raft[:3]})
		if database == "OVN_Northbound" {
			f.request.NorthboundAddress, f.request.NorthboundRemotes = clients[3], clients
		} else {
			f.request.SouthboundAddress, f.request.SouthboundRemotes = clients[3], clients
		}
		_ = listeners[3].Close()
		_ = listeners[7].Close()
	}
	p, err := prepare.Open(f.options.Root, f.options.OVSDBTool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	f.request.Prepared, err = p.Prepare(t.Context(), f.request.Preparation)
	if err != nil {
		t.Fatal(err)
	}
	for _, db := range f.request.Prepared.Databases {
		f.transports[db.Name].Endpoints = append(f.transports[db.Name].Endpoints, ovnadmin.Endpoint{Name: "central-3", ServerID: db.ServerID, Address: db.Address})
	}
	return f
}

func (f *launchFixture) waitMembers(t *testing.T, database string, want int) {
	t.Helper()
	waitFor(t, func() error {
		for _, endpoint := range f.transports[database].Endpoints {
			snapshot, err := f.transports[database].Status(t.Context(), database, endpoint.Name)
			if err != nil || len(snapshot.Members) != want || snapshot.Validate() != nil {
				continue
			}
			healthy := 0
			for _, member := range snapshot.Members {
				if member.Healthy {
					healthy++
				}
			}
			if healthy == want {
				return nil
			}
		}
		return fmt.Errorf("%s does not have %d healthy members", database, want)
	})
}

func (f *launchFixture) verifyRunning(t *testing.T) {
	t.Helper()
	for _, db := range f.request.Prepared.Databases {
		f.waitMembers(t, db.Name, 4)
		name := "ovnnb_db"
		if db.Name == "OVN_Southbound" {
			name = "ovnsb_db"
		}
		data := nativeCall(t, filepath.Join(f.toolDir, "ovsdb-client"), "--timeout=3", "query", "unix:"+filepath.Join(f.options.RuntimeDirectory, name+".sock"), f.queries[db.Name])
		var before, after any
		if json.Unmarshal(f.original[db.Name], &before) != nil || json.Unmarshal(data, &after) != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("%s lost row UUIDs or map data: %s", db.Name, data)
		}
	}
	waitFor(t, func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		for operation, expected := range map[string]string{"status": "Status: active", "nb-connection-status": "connected", "sb-connection-status": "connected"} {
			cmd := exec.CommandContext(ctx, filepath.Join(f.ovnDir, "ovn-appctl"), "--timeout=2", "-t", "ovn-northd", operation)
			cmd.Env = append(os.Environ(), "OVN_RUNDIR="+f.options.RuntimeDirectory)
			data, err := cmd.Output()
			if err != nil || strings.TrimSpace(string(data)) != expected {
				return fmt.Errorf("northd %s: %s %v", operation, data, err)
			}
		}
		return nil
	})
	for _, name := range []string{"ovnnb_db", "ovnsb_db", "ovn-northd"} {
		pid := processID(t, f.options.RuntimeDirectory, name)
		for fd, path := range map[int]string{3: f.options.Root, 4: filepath.Join(f.options.Root, ".prepare.lock"), 5: f.options.RuntimeDirectory, 6: filepath.Join(f.options.RuntimeDirectory, ".launch.lock")} {
			original, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			inherited, err := os.Stat(fmt.Sprintf("/proc/%d/fd/%d", pid, fd))
			if err != nil || !os.SameFile(original, inherited) {
				t.Fatalf("%s did not retain storage/runtime descriptor %d: %v", name, fd, err)
			}
		}
	}
	// An actual NB logical switch must be compiled into the SB datapath.
	waitFor(t, func() error {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		query := `["OVN_Southbound",{"op":"select","table":"Datapath_Binding","where":[],"columns":["external_ids"]}]`
		data, err := exec.CommandContext(ctx, filepath.Join(f.toolDir, "ovsdb-client"), "--timeout=2", "query", "unix:"+filepath.Join(f.options.RuntimeDirectory, "ovnsb_db.sock"), query).Output()
		if err != nil || !strings.Contains(string(data), "launcher-data") {
			return fmt.Errorf("northd has not compiled the logical switch: %s %v", data, err)
		}
		return nil
	})
}

func (f *launchFixture) start(t *testing.T) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		done <- Run(ctx, f.options, f.request)
		close(finished)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Error("launcher cleanup did not finish")
		}
	})
	f.verifyRunning(t)
	return cancel, done
}

func processID(t *testing.T, dir, name string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name+".pid"))
	if err != nil {
		t.Fatal(err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 1 {
		t.Fatalf("invalid PID: %s %v", data, err)
	}
	return pid
}

func waitStopped(t *testing.T, done <-chan error, cancelled bool) {
	t.Helper()
	select {
	case err := <-done:
		if err == nil || (cancelled && !errors.Is(err, context.Canceled)) {
			t.Fatalf("unexpected stop result: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("launcher did not stop and reap its children")
	}
}

func TestLiveOVNCentralMemberLaunchRestartAndFailure(t *testing.T) {
	f := newLaunchFixture(t)
	cancel, done := f.start(t)
	if err := Run(t.Context(), f.options, f.request); err == nil || !strings.Contains(err.Error(), "another actor") {
		t.Fatalf("second launcher was not excluded: %v", err)
	}
	cancel()
	waitStopped(t, done, true)
	p, err := prepare.Open(f.options.Root, f.options.OVSDBTool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	result, err := p.Prepare(t.Context(), f.request.Preparation)
	if err != nil || !reflect.DeepEqual(result, f.request.Prepared) {
		t.Fatalf("shutdown changed prepared identities: %+v %v", result, err)
	}
	_, done = f.start(t)
	pids := map[string]int{}
	for _, name := range []string{"ovnnb_db", "ovnsb_db", "ovn-northd"} {
		pids[name] = processID(t, f.options.RuntimeDirectory, name)
	}
	if err := syscall.Kill(pids["ovnsb_db"], syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	waitStopped(t, done, false)
	for name, pid := range pids {
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("%s was not reaped: %v", name, err)
		}
	}
	result, err = p.Prepare(t.Context(), f.request.Preparation)
	if err != nil || !reflect.DeepEqual(result, f.request.Prepared) {
		t.Fatalf("child failure changed prepared identities: %+v %v", result, err)
	}
	t.Log("both databases joined as fourth members, northd compiled real data, restart preserved identities, and child failure stopped/reaped the entire member")
}

func TestLiveOVNCentralSupervisorHelper(t *testing.T) {
	path := os.Getenv("CENTRAL_LAUNCH_REQUEST")
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var request Request
	if err := json.Unmarshal(data, &request); err != nil {
		t.Fatal(err)
	}
	options := Options{Root: os.Getenv("CENTRAL_LAUNCH_ROOT"), RuntimeDirectory: os.Getenv("CENTRAL_LAUNCH_RUNTIME"), OVSDBTool: filepath.Join(os.Getenv("OVSDB_TEST_BIN"), "ovsdb-tool"), OVSDBServer: filepath.Join(os.Getenv("OVSDB_TEST_BIN"), "ovsdb-server"), Northd: filepath.Join(os.Getenv("OVN_TEST_BIN"), "ovn-northd"), Stderr: os.Stderr}
	if err := Run(t.Context(), options, request); err != nil {
		t.Fatal(err)
	}
}

func TestLiveOVNCentralSupervisorCrashStopsChildren(t *testing.T) {
	f := newLaunchFixture(t)
	requestFile := filepath.Join(t.TempDir(), "request.json")
	content, err := json.Marshal(f.request)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(requestFile, content, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestLiveOVNCentralSupervisorHelper$")
	cmd.Env = append(os.Environ(), "CENTRAL_LAUNCH_REQUEST="+requestFile, "CENTRAL_LAUNCH_ROOT="+f.options.Root, "CENTRAL_LAUNCH_RUNTIME="+f.options.RuntimeDirectory)
	cmd.Stderr = f.options.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	f.verifyRunning(t)
	var pids []int
	for _, name := range []string{"ovnnb_db", "ovnsb_db", "ovn-northd"} {
		pid := processID(t, f.options.RuntimeDirectory, name)
		pids = append(pids, pid)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	waitFor(t, func() error {
		for _, pid := range pids {
			data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			// Orphans may briefly be zombies until the host init reaps them.
			_, state, ok := strings.Cut(string(data), ") ")
			if err != nil || !ok || !strings.HasPrefix(state, "Z ") {
				return fmt.Errorf("native child %d survived the supervisor", pid)
			}
		}
		return nil
	})
	p, err := prepare.Open(f.options.Root, f.options.OVSDBTool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Close() }()
	result, err := p.Prepare(t.Context(), f.request.Preparation)
	if err != nil || !reflect.DeepEqual(result, f.request.Prepared) {
		t.Fatalf("crash left locked data or changed identities: %+v %v", result, err)
	}
	cancel, done := f.start(t)
	cancel()
	waitStopped(t, done, true)
	t.Log("supervisor SIGKILL terminated all native children; retained files restarted with the same identities and data")
}
