package health

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
)

func TestLiveOVNPreparedMemberJoinsBothDatabases(t *testing.T) {
	schemaDirectory := os.Getenv("OVN_TEST_SCHEMA_DIR")
	if schemaDirectory == "" {
		t.Skip("set OVN_TEST_SCHEMA_DIR for native NB/SB preparation integration")
	}
	tool := ovsdbBinary(t, "ovsdb-tool")
	root := t.TempDir()
	directories := map[string]string{}
	transports := map[string]*ovnadmin.CommandClient{}
	addresses := map[string][]string{}
	listeners := map[string][]net.Listener{}
	toolRead := func(command, path string) string {
		t.Helper()
		out, err := exec.CommandContext(t.Context(), tool, command, path).Output()
		if err != nil {
			t.Fatal(err)
		}
		return strings.TrimSpace(string(out))
	}
	databases := []string{"OVN_Northbound", "OVN_Southbound"}
	for i := range 4 {
		name := fmt.Sprintf("central-%d", i)
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
		directories[name] = dir
		for _, database := range databases {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			listeners[name] = append(listeners[name], listener)
			addresses[database] = append(addresses[database], "tcp:"+listener.Addr().String())
		}
	}
	for _, database := range databases {
		filename, schemaFile := "ovnnb_db.db", "ovn-nb.ovsschema"
		if database == "OVN_Southbound" {
			filename, schemaFile = "ovnsb_db.db", "ovn-sb.ovsschema"
		}
		transport := &ovnadmin.CommandClient{Runner: membershipRunFunc(func(ctx context.Context, endpoint string, command []string) ([]byte, error) {
			args := append([]string(nil), command...)
			args[0], args[3] = ovsdbBinary(t, "ovs-appctl"), filepath.Join(directories[endpoint], "ctl")
			return exec.CommandContext(ctx, args[0], args[1:]...).Output()
		})}
		for i := range 3 {
			name := fmt.Sprintf("central-%d", i)
			path := filepath.Join(directories[name], filename)
			if i == 0 {
				runOVSDBTool(t, "--election-timer=1000", "create-cluster", path, filepath.Join(schemaDirectory, schemaFile), addresses[database][i])
				transport.ClusterID = toolRead("db-cid", path)
			} else {
				runOVSDBTool(t, "--cid="+transport.ClusterID, "join-cluster", path, database, addresses[database][i], addresses[database][0])
			}
			transport.Endpoints = append(transport.Endpoints, ovnadmin.Endpoint{Name: name, ServerID: toolRead("db-sid", path), Address: addresses[database][i]})
		}
		transports[database] = transport
	}
	start := func(name string, files []string) {
		t.Helper()
		for _, listener := range listeners[name] {
			_ = listener.Close()
		}
		dir := directories[name]
		args := []string{"--no-chdir", "--pidfile=" + filepath.Join(dir, "pid"), "--unixctl=" + filepath.Join(dir, "ctl"), "--remote=punix:" + filepath.Join(dir, "socket")}
		startTestProcess(t, dir, exec.Command(ovsdbBinary(t, "ovsdb-server"), append(args, files...)...))
	}
	for i := range 3 {
		name := fmt.Sprintf("central-%d", i)
		start(name, []string{filepath.Join(directories[name], "ovnnb_db.db"), filepath.Join(directories[name], "ovnsb_db.db")})
	}
	observe := func(want int) {
		t.Helper()
		for _, database := range databases {
			waitOVSDB(t, func() error {
				transport := transports[database]
				for _, endpoint := range transport.Endpoints {
					snapshot, err := transport.Status(t.Context(), database, endpoint.Name)
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
				return fmt.Errorf("waiting for %d healthy %s members", want, database)
			})
		}
	}
	observe(3)
	clientCall := func(endpoint, method, transaction string) ([]byte, error) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancel()
		return exec.CommandContext(ctx, ovsdbBinary(t, "ovsdb-client"), "--timeout=2", method, "unix:"+filepath.Join(directories[endpoint], "socket"), transaction).Output()
	}
	original := map[string][]byte{}
	queries := map[string]string{}
	for _, database := range databases {
		table := "NB_Global"
		if database == "OVN_Southbound" {
			table = "SB_Global"
		}
		transaction := fmt.Sprintf(`[%q,{"op":"insert","table":%q,"row":{"options":["map",[["member-preparation","preserved"]]]}}]`, database, table)
		if out, err := clientCall("central-0", "transact", transaction); err != nil || strings.Contains(string(out), `"error"`) {
			t.Fatalf("insert: %s %v", out, err)
		}
		queries[database] = fmt.Sprintf(`[%q,{"op":"select","table":%q,"where":[],"columns":["_uuid","options"]}]`, database, table)
		var err error
		original[database], err = clientCall("central-0", "query", queries[database])
		if err != nil || !strings.Contains(string(original[database]), "preserved") {
			t.Fatal("initial database data missing")
		}
	}
	prepareRoot := t.TempDir()
	if err := os.Chmod(prepareRoot, 0700); err != nil {
		t.Fatal(err)
	}
	preparer, err := prepare.Open(prepareRoot, tool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = preparer.Close() })
	plan := prepare.Plan{FormatVersion: 1, InstallationUID: "test-installation", OperationID: uuid.New().String(), MemberName: "central-3"}
	for _, database := range databases {
		plan.Databases = append(plan.Databases, prepare.Database{Name: database, ClusterID: transports[database].ClusterID, LocalAddress: addresses[database][3], RemoteAddresses: addresses[database][:3]})
	}
	prepared, err := preparer.Prepare(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	observe(3)
	var files []string
	for _, db := range prepared.Databases {
		transports[db.Name].Endpoints = append(transports[db.Name].Endpoints, ovnadmin.Endpoint{Name: "central-3", ServerID: db.ServerID, Address: db.Address})
		files = append(files, filepath.Join(prepareRoot, db.Path))
	}
	start("central-3", files)
	observe(4)
	for _, database := range databases {
		waitOVSDB(t, func() error {
			data, err := clientCall("central-3", "query", queries[database])
			if err != nil {
				return err
			}
			var before, after any
			if json.Unmarshal(original[database], &before) != nil || json.Unmarshal(data, &after) != nil || !reflect.DeepEqual(before, after) {
				return fmt.Errorf("%s data or row UUIDs not yet preserved", database)
			}
			return nil
		})
	}
	// Files have now joined and advanced; preparation retries must still preserve
	// the generated server identities rather than resetting their RAFT logs.
	restarted, err := prepare.Open(prepareRoot, tool)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	again, err := restarted.Prepare(t.Context(), plan)
	if err != nil || !reflect.DeepEqual(prepared, again) {
		t.Fatalf("joined retry changed identity: %+v %v", again, err)
	}
	observe(4)
	t.Log("prepared NB/SB files joined their pinned clusters as fourth members, preserved row UUIDs/data, and retained identities on retry after joining")
}
