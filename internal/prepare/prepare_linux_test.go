package prepare

import (
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"uuid"
)

func privateTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func testPlan() Plan {
	return Plan{FormatVersion: 1, InstallationUID: "installation-uid", OperationID: "eeee0000-1111-2222-3333-444444444444", MemberName: "central-replacement", Databases: []Database{
		{Name: "OVN_Northbound", ClusterID: "aaaa0000-1111-2222-3333-444444444444", LocalAddress: "tcp:192.0.2.4:6643", RemoteAddresses: []string{"tcp:192.0.2.1:6643", "tcp:192.0.2.2:6643"}},
		{Name: "OVN_Southbound", ClusterID: "bbbb0000-1111-2222-3333-444444444444", LocalAddress: "tcp:192.0.2.4:6644", RemoteAddresses: []string{"tcp:192.0.2.1:6644", "tcp:192.0.2.2:6644"}},
	}}
}

func openTest(t *testing.T, root, tool string) *Preparer {
	t.Helper()
	p, err := Open(root, tool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func nativeTool(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("OVSDB_TEST_BIN")
	if dir == "" {
		t.Skip("set OVSDB_TEST_BIN for native member preparation tests")
	}
	tool := filepath.Join(dir, "ovsdb-tool")
	if _, err := os.Stat(tool); err != nil {
		t.Fatal(err)
	}
	return tool
}

func TestPreparationRejectsInvalidPlansBeforeWriting(t *testing.T) {
	for _, scenario := range []string{"no installation", "short operation", "path traversal", "no member", "missing SB", "duplicate DB", "zero CID", "hostname", "duplicate remote", "remote is local", "no remote", "protocol mismatch", "NB SB collision"} {
		t.Run(scenario, func(t *testing.T) {
			root := privateTempDir(t)
			p := openTest(t, root, "/unused-tool")
			plan := testPlan()
			switch scenario {
			case "no installation":
				plan.InstallationUID = ""
			case "short operation":
				plan.OperationID = "abcd"
			case "path traversal":
				plan.OperationID = "../outside"
			case "no member":
				plan.MemberName = ""
			case "missing SB":
				plan.Databases = plan.Databases[:1]
			case "duplicate DB":
				plan.Databases[1] = plan.Databases[0]
			case "zero CID":
				plan.Databases[0].ClusterID = "00000000-0000-0000-0000-000000000000"
			case "hostname":
				plan.Databases[0].LocalAddress = "tcp:node:6643"
			case "duplicate remote":
				plan.Databases[0].RemoteAddresses[1] = plan.Databases[0].RemoteAddresses[0]
			case "remote is local":
				plan.Databases[0].RemoteAddresses[0] = plan.Databases[0].LocalAddress
			case "no remote":
				plan.Databases[0].RemoteAddresses = nil
			case "protocol mismatch":
				plan.Databases[0].LocalAddress = "ssl:192.0.2.4:6643"
			case "NB SB collision":
				plan.Databases[1].LocalAddress = plan.Databases[0].LocalAddress
			}
			if _, err := p.Prepare(t.Context(), plan); err == nil {
				t.Fatal("invalid plan accepted")
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 0 {
				t.Fatalf("invalid request wrote files: %v %v", entries, err)
			}
		})
	}
}

func TestPreparationRefusesUnclaimedDataAndLinks(t *testing.T) {
	for _, scenario := range []string{"existing directory", "directory symlink", "lock symlink", "request symlink", "request hard link"} {
		t.Run(scenario, func(t *testing.T) {
			root, outside := privateTempDir(t), privateTempDir(t)
			p := openTest(t, root, "/unused-tool")
			plan := testPlan()
			path := filepath.Join(root, plan.OperationID)
			content, _ := json.Marshal(plan)
			victim := filepath.Join(outside, "request.json")
			if err := os.WriteFile(victim, content, 0600); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "existing directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "directory symlink":
				if err := os.Symlink(outside, path); err != nil {
					t.Fatal(err)
				}
			case "lock symlink":
				if err := os.Symlink(victim, filepath.Join(root, ".prepare.lock")); err != nil {
					t.Fatal(err)
				}
			case "request symlink", "request hard link":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
				link := os.Symlink
				if scenario == "request hard link" {
					link = os.Link
				}
				if err := link(victim, filepath.Join(path, "request.json")); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := p.Prepare(t.Context(), plan); err == nil {
				t.Fatal("unsafe existing path accepted")
			}
			data, err := os.ReadFile(victim)
			if err != nil || string(data) != string(content) {
				t.Fatal("existing file was modified")
			}
		})
	}
}

func TestPreparationRootOwnershipAndContention(t *testing.T) {
	root := privateTempDir(t)
	p := openTest(t, root, "/unused-tool")
	other := openTest(t, root, "/unused-tool")
	lock, err := p.lock()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lock.Close() }()
	if _, err := other.Prepare(t.Context(), testPlan()); err == nil || !strings.Contains(err.Error(), "another actor") {
		t.Fatalf("concurrent preparation was not blocked: %v", err)
	}
	if err := os.Chmod(root, 0755); err != nil {
		t.Fatal(err)
	}
	if p, err := Open(root, "/unused-tool"); err == nil {
		_ = p.Close()
		t.Fatal("nonprivate root accepted")
	}
}

func TestPreparationMetadataOutputIsBounded(t *testing.T) {
	output := new(limitedOutput)
	// Hide Reader.WriteTo so io.Copy takes its destination optimization path.
	_, err := io.Copy(output, struct{ io.Reader }{strings.NewReader(strings.Repeat("x", 16<<10))})
	if err == nil || output.buffer.Len() > 4096 {
		t.Fatal("subprocess output bypassed the memory limit")
	}
}

func TestLiveOVNPreparationPreservesPartialPairAcrossRestart(t *testing.T) {
	tool, root := nativeTool(t), privateTempDir(t)
	plan := testPlan()
	p := openTest(t, root, tool)
	actual := p.run
	p.run = func(ctx context.Context, args ...string) (string, error) {
		if len(args) > 3 && args[1] == "join-cluster" && args[3] == "OVN_Southbound" {
			return "", errors.New("simulated failure before SB preparation")
		}
		return actual(ctx, args...)
	}
	if _, err := p.Prepare(t.Context(), plan); err == nil {
		t.Fatal("partial pair was published")
	}
	if _, err := os.Stat(filepath.Join(root, plan.OperationID, "prepared.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial pair has a completion record: %v", err)
	}
	nb, err := p.inspect(t.Context(), plan.OperationID, plan.Databases[0])
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(root, nb.Path))
	if err != nil {
		t.Fatal(err)
	}
	restarted := openTest(t, root, tool)
	result, err := restarted.Prepare(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.Databases[0].ServerID != nb.ServerID {
		t.Fatal("restart replaced already generated NB identity")
	}
	after, err := os.ReadFile(filepath.Join(root, nb.Path))
	if err != nil || string(before) != string(after) {
		t.Fatal("retry rewrote NB file")
	}
	// Canonical ordering makes a reordered equivalent request an exact retry.
	slices.Reverse(plan.Databases)
	for i := range plan.Databases {
		slices.Reverse(plan.Databases[i].RemoteAddresses)
	}
	again, err := restarted.Prepare(t.Context(), plan)
	if err != nil || !reflect.DeepEqual(result, again) {
		t.Fatalf("unstable retry: %+v %v", again, err)
	}
	for _, db := range result.Databases {
		info, err := os.Stat(filepath.Join(root, db.Path))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("database is not private: %v %v", info, err)
		}
	}
	t.Log("partial NB preparation survived restart with the same SID; both native identities published together")
}

func TestLiveOVNPreparationRetainsRejectedData(t *testing.T) {
	for _, scenario := range []string{"changed installation", "changed CID", "changed local address", "changed remote", "changed member", "corrupt DB", "replaced SID", "corrupt result", "result symlink", "DB symlink", "DB hard link"} {
		t.Run(scenario, func(t *testing.T) {
			tool, root := nativeTool(t), privateTempDir(t)
			p := openTest(t, root, tool)
			plan := testPlan()
			result, err := p.Prepare(t.Context(), plan)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, result.Databases[0].Path)
			marker := filepath.Join(root, plan.OperationID, "prepared.json")
			switch scenario {
			case "changed installation":
				plan.InstallationUID = "another-installation"
			case "changed CID":
				plan.Databases[0].ClusterID = uuid.New().String()
			case "changed local address":
				plan.Databases[0].LocalAddress = "tcp:192.0.2.5:6643"
			case "changed remote":
				plan.Databases[0].RemoteAddresses[0] = "tcp:192.0.2.9:6643"
			case "changed member":
				plan.MemberName = "another-member"
			case "corrupt DB":
				if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
					t.Fatal(err)
				}
			case "replaced SID":
				other, err := p.run(t.Context(), "--cid="+plan.Databases[0].ClusterID, "join-cluster", commandPath(plan.OperationID+"/replacement.db"), "OVN_Northbound", plan.Databases[0].LocalAddress, plan.Databases[0].RemoteAddresses[0])
				if err != nil {
					t.Fatalf("%s %v", other, err)
				}
				if err := os.Rename(filepath.Join(root, plan.OperationID, "replacement.db"), path); err != nil {
					t.Fatal(err)
				}
			case "corrupt result":
				if err := os.WriteFile(marker, []byte("bad"), 0600); err != nil {
					t.Fatal(err)
				}
			case "result symlink", "DB symlink", "DB hard link":
				target := path
				if scenario == "result symlink" {
					target = marker
				}
				moved := target + ".original"
				if err := os.Rename(target, moved); err != nil {
					t.Fatal(err)
				}
				link := os.Symlink
				if scenario == "DB hard link" {
					link = os.Link
				}
				if err := link(moved, target); err != nil {
					t.Fatal(err)
				}
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Prepare(t.Context(), plan); err == nil {
				t.Fatal("unsafe preparation accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || string(before) != string(after) {
				t.Fatal("rejection changed existing data")
			}
		})
	}
}

func TestLiveOVNPreparationInterruptedAfterNativeWrite(t *testing.T) {
	for _, database := range []string{"OVN_Northbound", "OVN_Southbound"} {
		t.Run(database, func(t *testing.T) {
			root, tool := privateTempDir(t), nativeTool(t)
			plan := testPlan()
			p := openTest(t, root, tool)
			actual := p.run
			p.run = func(ctx context.Context, args ...string) (string, error) {
				out, err := actual(ctx, args...)
				if err == nil && len(args) > 3 && args[1] == "join-cluster" && args[3] == database {
					return "", context.Canceled
				}
				return out, err
			}
			if _, err := p.Prepare(t.Context(), plan); err == nil {
				t.Fatal("interruption was ignored")
			}
			path := filepath.Join(root, plan.OperationID, databaseFile(database))
			before, err := exec.CommandContext(t.Context(), tool, "db-sid", path).Output()
			if err != nil {
				t.Fatal(err)
			}
			restarted := openTest(t, root, tool)
			result, err := restarted.Prepare(t.Context(), plan)
			if err != nil {
				t.Fatal(err)
			}
			index := slices.IndexFunc(result.Databases, func(d PreparedDatabase) bool { return d.Name == database })
			if index < 0 || result.Databases[index].ServerID != strings.TrimSpace(string(before)) {
				t.Fatal("native identity was regenerated after interruption")
			}
		})
	}
}
