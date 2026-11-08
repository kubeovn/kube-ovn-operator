package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
)

type rebuildStore struct {
	records map[string]backup.Record
	data    map[string][]byte
}

func (s rebuildStore) Publish(context.Context, func(io.Writer) (backup.Record, error)) (string, error) {
	return "", fmt.Errorf("publish is not supported by the test store")
}

func (s rebuildStore) Open(_ context.Context, reference string) (backup.Record, io.ReadCloser, error) {
	record, ok := s.records[reference]
	if !ok {
		return backup.Record{}, nil, os.ErrNotExist
	}
	return record, io.NopCloser(bytes.NewReader(s.data[reference])), nil
}

type rebuildIsolation struct {
	oldCalls     int
	storageCalls int
	err          error
}

func (i *rebuildIsolation) OldProcessesIsolated(context.Context, Plan) error {
	i.oldCalls++
	return i.err
}

func (i *rebuildIsolation) TargetStorageOwned(context.Context, Member, string) error {
	i.storageCalls++
	return i.err
}

type rebuildNative struct {
	root    string
	created []string
	ids     map[string]NativeDatabase
	err     error
	failAt  int
}

func (n *rebuildNative) CreateCluster(_ context.Context, database, contents, _ string, _ string) error {
	if n.failAt > 0 && len(n.created)+1 == n.failAt {
		return fmt.Errorf("simulated native interruption")
	}
	if n.err != nil {
		return n.err
	}
	data, err := os.ReadFile(contents)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("empty contents")
	}
	if err := os.WriteFile(database, []byte("clustered:"+string(data)), 0600); err != nil {
		return err
	}
	n.created = append(n.created, database)
	return nil
}

func (n *rebuildNative) Inspect(_ context.Context, database string) (NativeDatabase, error) {
	if n.err != nil {
		return NativeDatabase{}, n.err
	}
	identity, ok := n.ids[filepath.Base(database)]
	if !ok {
		return NativeDatabase{}, os.ErrNotExist
	}
	return identity, nil
}

func executorFixture(t *testing.T) (Plan, rebuildStore, *rebuildIsolation, *rebuildNative) {
	t.Helper()
	plan := validPlan()
	data := map[string][]byte{
		plan.Databases[0].BackupRef: []byte("northbound backup"),
		plan.Databases[1].BackupRef: []byte("southbound backup"),
	}
	records := make(map[string]backup.Record, len(data))
	for _, database := range plan.Databases {
		content := data[database.BackupRef]
		records[database.BackupRef] = backup.Record{
			FormatVersion: 1,
			Identity:      backup.Identity{InstallationUID: plan.InstallationUID, PodUID: "old-pod", ContainerID: "containerd://old"},
			Endpoint:      "tcp:192.0.2.1:6641",
			CompletedAt:   time.Now().UTC(),
			Snapshot:      ovnadmin.Snapshot{Database: database.Name, ClusterID: database.SourceClusterID},
			Size:          int64(len(content)),
			SHA256:        fmt.Sprintf("%x", sha256.Sum256(content)),
		}
	}
	isolation := new(rebuildIsolation)
	native := &rebuildNative{ids: map[string]NativeDatabase{}}
	for _, database := range plan.Databases {
		native.ids[databaseFile(database.Name)] = NativeDatabase{
			ClusterID: database.TargetClusterID,
			ServerID:  "eeee0000-1111-2222-3333-" + map[string]string{"OVN_Northbound": "555555555555", "OVN_Southbound": "666666666666"}[database.Name],
			Address:   plan.TargetMembers[0].RAFTAddress(database.Name),
		}
	}
	return plan, rebuildStore{records: records, data: data}, isolation, native
}

func TestRebuildStagesBothBackupsOnlyAfterIsolation(t *testing.T) {
	plan, store, isolation, native := executorFixture(t)
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	native.root = root
	result, err := (&Executor{Root: root, Backups: store, Isolation: isolation, Native: native, MaxBackupBytes: 1 << 20}).Rebuild(t.Context(), plan, "new")
	if err != nil {
		t.Fatal(err)
	}
	if result.PlanDigest == "" || result.MemberName != "new" || len(result.Databases) != 2 {
		t.Fatalf("unexpected rebuild result: %+v", result)
	}
	if isolation.oldCalls != 1 || isolation.storageCalls != 1 || len(native.created) != 2 {
		t.Fatalf("precondition/native calls = %d/%d/%d", isolation.oldCalls, isolation.storageCalls, len(native.created))
	}
	for _, name := range []string{"ovnnb_backup.db", "ovnsb_backup.db", "ovnnb_db.db", "ovnsb_db.db"} {
		if _, err := os.Stat(filepath.Join(root, plan.OperationID, name)); err != nil {
			t.Fatalf("missing rebuild artifact %s: %v", name, err)
		}
	}
}

func TestRebuildFailsClosedBeforeWriting(t *testing.T) {
	plan, store, isolation, native := executorFixture(t)
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	isolation.err = fmt.Errorf("retained process is still running")
	if _, err := (&Executor{Root: root, Backups: store, Isolation: isolation, Native: native, MaxBackupBytes: 1 << 20}).Rebuild(t.Context(), plan, "new"); err == nil || !strings.Contains(err.Error(), "isolation") {
		t.Fatalf("isolation failure was accepted: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 || len(native.created) != 0 {
		t.Fatalf("isolation failure wrote files: entries=%v native=%v", entries, native.created)
	}
}

func TestRebuildRejectsBackupProvenanceAndNativeIdentity(t *testing.T) {
	for _, scenario := range []string{"wrong installation", "wrong source cluster", "wrong digest", "wrong target cluster", "duplicate native identity"} {
		t.Run(scenario, func(t *testing.T) {
			plan, store, isolation, native := executorFixture(t)
			root := t.TempDir()
			if err := os.Chmod(root, 0700); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "wrong installation":
				record := store.records[plan.Databases[0].BackupRef]
				record.Identity.InstallationUID = "another"
				store.records[plan.Databases[0].BackupRef] = record
			case "wrong source cluster":
				record := store.records[plan.Databases[0].BackupRef]
				record.Snapshot.ClusterID = targetNB
				store.records[plan.Databases[0].BackupRef] = record
			case "wrong digest":
				record := store.records[plan.Databases[0].BackupRef]
				record.SHA256 = strings.Repeat("f", 64)
				store.records[plan.Databases[0].BackupRef] = record
			case "wrong target cluster":
				path := databaseFile(plan.Databases[0].Name)
				native.ids[path] = NativeDatabase{ClusterID: sourceNB, ServerID: native.ids[path].ServerID, Address: native.ids[path].Address}
			case "duplicate native identity":
				path := databaseFile(plan.Databases[1].Name)
				first := native.ids[databaseFile(plan.Databases[0].Name)]
				native.ids[path] = NativeDatabase{ClusterID: targetSB, ServerID: first.ServerID, Address: native.ids[path].Address}
			}
			if _, err := (&Executor{Root: root, Backups: store, Isolation: isolation, Native: native, MaxBackupBytes: 1 << 20}).Rebuild(t.Context(), plan, "new"); err == nil {
				t.Fatal("unsafe rebuild was accepted")
			}
		})
	}
}

func TestRebuildResumesAndRejectsUnjournaledOperation(t *testing.T) {
	plan, store, isolation, native := executorFixture(t)
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, plan.OperationID), 0700); err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Root: root, Backups: store, Isolation: isolation, Native: native, MaxBackupBytes: 1 << 20}
	if _, err := executor.Rebuild(t.Context(), plan, "new"); err != nil {
		t.Fatal(err)
	}
	created := len(native.created)
	if _, err := executor.Rebuild(t.Context(), plan, "new"); err != nil {
		t.Fatalf("journaled retry failed: %v", err)
	}
	if len(native.created) != created {
		t.Fatalf("journaled retry recreated native files: before=%d after=%d", created, len(native.created))
	}
	if err := os.WriteFile(filepath.Join(root, plan.OperationID, "unexpected"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Rebuild(t.Context(), plan, "new"); err == nil {
		t.Fatal("unexpected operation entry was accepted")
	}
}

func TestRebuildResumesAfterNativeInterruption(t *testing.T) {
	plan, store, isolation, native := executorFixture(t)
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	native.failAt = 2
	executor := &Executor{Root: root, Backups: store, Isolation: isolation, Native: native, MaxBackupBytes: 1 << 20}
	if _, err := executor.Rebuild(t.Context(), plan, "new"); err == nil {
		t.Fatal("native interruption was ignored")
	}
	if len(native.created) != 1 {
		t.Fatalf("partial native writes = %d, want one", len(native.created))
	}
	native.failAt = 0
	if _, err := executor.Rebuild(t.Context(), plan, "new"); err != nil {
		t.Fatalf("resume after native interruption failed: %v", err)
	}
	if len(native.created) != 2 {
		t.Fatalf("resume recreated the completed database: native writes=%d", len(native.created))
	}
}
