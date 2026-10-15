package backup

import (
	"bytes"
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const testSchema = `{"name":"OVN_Northbound","version":"1.0.0","tables":{"Root":{"isRoot":true,"columns":{"name":{"type":"string"}}}}}`
const testRows = `{"_date":1792000000000,"_is_diff":true,"Root":{"aaaaaaaa-1111-2222-3333-444444444444":{"name":"saved"}}}`

func nativeRecord(value string) []byte {
	data := []byte(value + "\n")
	return fmt.Appendf(nil, "OVSDB JSON %d %x\n%s", len(data), sha1.Sum(data), data)
}

func nativeBackup(schema, rows string) []byte {
	return append(nativeRecord(schema), nativeRecord(rows)...)
}

func testSnapshot() ovnadmin.Snapshot {
	id := "11111111-2222-3333-4444-555555555555"
	return ovnadmin.Snapshot{Database: "OVN_Northbound", ClusterID: "22222222-2222-3333-4444-555555555555", LeaderServerID: id, Term: 2, CommitIndex: 10, AppliedIndex: 10,
		Members: []membership.Member{{ServerID: id, Address: "tcp:127.0.0.1:6643", Healthy: true, Leader: true}}}
}

type testSource struct {
	data     []byte
	identity Identity
	snapshot ovnadmin.Snapshot
	after    func()
	err      error
}

func (s *testSource) Identity(context.Context, string) (Identity, error) { return s.identity, nil }
func (s *testSource) Status(context.Context, string, string) (ovnadmin.Snapshot, error) {
	state := s.snapshot
	state.Members = slices.Clone(state.Members)
	return state, nil
}
func (s *testSource) Stream(_ context.Context, _, _ string, destination io.Writer) error {
	_, err := io.Copy(destination, bytes.NewReader(s.data))
	if s.after != nil {
		s.after()
	}
	return errors.Join(err, s.err)
}

func fixture(t *testing.T) (*Provider, *testSource, string) {
	t.Helper()
	directory := t.TempDir()
	store, err := OpenFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	source := &testSource{data: nativeBackup(testSchema, testRows), identity: Identity{InstallationUID: "installation", PodUID: "pod", ContainerID: "containerd://process"}, snapshot: testSnapshot()}
	return &Provider{Source: source, Store: store, InstallationUID: "installation", MaxBytes: 1 << 20, MaxAge: time.Hour}, source, directory
}

func withLeases(t *testing.T, fn func(context.Context) error) error {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	manager := lease.New(fake.NewClientBuilder().WithScheme(scheme).Build())
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "backup", Namespace: "test", UID: "installation"}
	return manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, fn)
	})
}

func TestCaptureSurvivesRestartAndRejectsChanges(t *testing.T) {
	provider, source, directory := fixture(t)
	err := withLeases(t, func(ctx context.Context) error {
		ref, err := provider.Capture(ctx, "OVN_Northbound", "central-0")
		if err != nil {
			return err
		}
		store, err := OpenFileStore(directory)
		if err != nil {
			return err
		}
		defer func() { _ = store.Close() }()
		restarted := &Provider{Store: store, InstallationUID: "installation", MaxBytes: provider.MaxBytes, MaxAge: time.Hour}
		if err := restarted.Verify(ctx, source.snapshot, ref); err != nil {
			return err
		}
		for _, change := range []string{"cluster", "database", "leader", "term", "commit", "address", "quorum"} {
			state := source.snapshot
			state.Members = slices.Clone(state.Members)
			switch change {
			case "cluster":
				state.ClusterID = "33333333-2222-3333-4444-555555555555"
			case "database":
				state.Database = "OVN_Southbound"
			case "leader":
				state.LeaderServerID = "33333333-2222-3333-4444-555555555555"
			case "term":
				state.Term++
			case "commit":
				state.CommitIndex++
				state.AppliedIndex++
			case "address":
				state.Members[0].Address = "tcp:127.0.0.2:6643"
			case "quorum":
				state.Members[0].Healthy = false
			}
			if err := restarted.Verify(ctx, state, ref); err == nil {
				t.Fatalf("accepted changed %s", change)
			}
		}
		restarted.InstallationUID = "other-installation"
		if err := restarted.Verify(ctx, source.snapshot, ref); err == nil {
			t.Fatal("accepted another installation's backup")
		}
		restarted.InstallationUID = "installation"
		db := filepath.Join(directory, strings.TrimPrefix(ref, "sha256:"), "snapshot.db")
		info, err := os.Stat(db)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("backup permissions: %v %v", info, err)
		}
		// Even a valid native record with recalculated SHA-1 must fail SHA-256.
		changed := nativeBackup(testSchema, strings.ReplaceAll(testRows, "saved", "other"))
		if err := os.WriteFile(db, changed, 0600); err != nil {
			return err
		}
		if err := restarted.Verify(ctx, source.snapshot, ref); err == nil {
			t.Fatal("accepted modified database bytes")
		}
		if err := os.WriteFile(db, source.data, 0600); err != nil {
			return err
		}
		metadata := filepath.Join(directory, strings.TrimPrefix(ref, "sha256:"), "record.json")
		content, err := os.ReadFile(metadata)
		if err != nil {
			return err
		}
		if err := os.WriteFile(metadata, append(content, '\n'), 0600); err != nil {
			return err
		}
		if err := restarted.Verify(ctx, source.snapshot, ref); err == nil {
			t.Fatal("accepted modified metadata")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCaptureFailuresNeverPublish(t *testing.T) {
	for _, scenario := range []string{"schema only", "truncated", "checksum", "wrong database", "extra record", "unknown table", "unknown column", "invalid UUID", "invalid JSON", "duplicate column", "object value", "oversized token", "size limit", "command failure", "identity", "term", "commit", "membership", "cancelled", "no lease", "foreign lease"} {
		t.Run(scenario, func(t *testing.T) {
			provider, source, directory := fixture(t)
			switch scenario {
			case "schema only":
				source.data = nativeRecord(testSchema)
			case "truncated":
				source.data = source.data[:len(source.data)-10]
			case "checksum":
				source.data = bytes.Replace(source.data, []byte("saved"), []byte("wrong"), 1)
			case "wrong database":
				source.data = nativeBackup(strings.ReplaceAll(testSchema, "OVN_Northbound", "OVN_Southbound"), testRows)
			case "extra record":
				source.data = append(source.data, nativeRecord(testRows)...)
			case "unknown table":
				source.data = nativeBackup(testSchema, strings.ReplaceAll(testRows, "Root", "Other"))
			case "unknown column":
				source.data = nativeBackup(testSchema, strings.ReplaceAll(testRows, "name", "other"))
			case "invalid UUID":
				source.data = nativeBackup(testSchema, strings.ReplaceAll(testRows, "aaaaaaaa-1111-2222-3333-444444444444", "invalid"))
			case "invalid JSON":
				source.data = nativeBackup(testSchema, testRows[:len(testRows)-1])
			case "duplicate column":
				source.data = nativeBackup(testSchema, strings.ReplaceAll(testRows, `"name":"saved"`, `"name":"saved","name":"other"`))
			case "object value":
				source.data = nativeBackup(testSchema, strings.ReplaceAll(testRows, `"saved"`, `{"invalid":"object"}`))
			case "oversized token":
				provider.MaxBytes = 4 << 20
				source.data = nativeBackup(testSchema, strings.ReplaceAll(testRows, "saved", strings.Repeat("x", 2<<20)))
			case "size limit":
				provider.MaxBytes = int64(len(source.data) - 1)
			case "command failure":
				source.err = errors.New("remote command exited nonzero after output")
			case "identity":
				source.after = func() { source.identity.ContainerID = "new-container" }
			case "term":
				source.after = func() { source.snapshot.Term++ }
			case "commit":
				source.after = func() { source.snapshot.CommitIndex++; source.snapshot.AppliedIndex++ }
			case "membership":
				source.after = func() { source.snapshot.Members[0].Address = "tcp:127.0.0.2:6643" }
			case "foreign lease":
				provider.InstallationUID, source.identity.InstallationUID = "other", "other"
			}
			operation := func(leaseCtx context.Context) error {
				captureCtx, captureCancel := context.WithCancel(leaseCtx)
				defer captureCancel()
				if scenario == "cancelled" {
					source.after = captureCancel
				}
				_, err := provider.Capture(captureCtx, "OVN_Northbound", "central-0")
				return err
			}
			var err error
			if scenario == "no lease" {
				err = operation(t.Context())
			} else {
				err = withLeases(t, operation)
			}
			if err == nil {
				t.Fatal("invalid capture succeeded")
			}
			files, err := os.ReadDir(directory)
			if err != nil || len(files) != 0 {
				t.Fatalf("failed capture left published or temporary files: %v %v", files, err)
			}
		})
	}
}

func TestNativeBackupStreamingBeyondMetadataLimit(t *testing.T) {
	data := nativeBackup(testSchema, strings.ReplaceAll(testRows, "saved", strings.Repeat("x", 256<<10)))
	info, err := inspect(t.Context(), bytes.NewReader(data), "OVN_Northbound", 1<<20)
	if err != nil || info.Size != int64(len(data)) {
		t.Fatalf("streamed snapshot: %+v %v", info, err)
	}
}

func TestBackupAgeAndReferencePolicy(t *testing.T) {
	provider, source, directory := fixture(t)
	err := withLeases(t, func(ctx context.Context) error {
		ref, err := provider.Capture(ctx, "OVN_Northbound", "central-0")
		if err != nil {
			return err
		}
		for _, invalid := range []string{"", "../../record.json", "sha256:../../outside", strings.ToUpper(ref)} {
			if err := provider.Verify(ctx, source.snapshot, invalid); err == nil {
				t.Fatalf("accepted reference %q", invalid)
			}
		}
		for _, when := range []time.Time{time.Now().Add(-2 * time.Hour), time.Now().Add(time.Hour), {}} {
			record, file, err := provider.Store.Open(ctx, ref)
			if err != nil {
				return err
			}
			_ = file.Close()
			record.CompletedAt = when
			other, err := provider.Store.Publish(ctx, func(w io.Writer) (Record, error) {
				_, err := w.Write(source.data)
				return record, err
			})
			if err != nil {
				return err
			}
			if err := provider.Verify(ctx, source.snapshot, other); err == nil {
				t.Fatal("accepted an expired or future backup")
			}
		}
		// File objects must not be symlinks, even if the target is in the root.
		db := filepath.Join(directory, strings.TrimPrefix(ref, "sha256:"), "snapshot.db")
		if err := os.Rename(db, db+".original"); err != nil {
			return err
		}
		if err := os.Symlink("snapshot.db.original", db); err != nil {
			return err
		}
		if err := provider.Verify(ctx, source.snapshot, ref); err == nil {
			t.Fatal("accepted a symbolic link")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestCapturePropagatesStorageWriteFailure(t *testing.T) {
	provider, _, _ := fixture(t)
	want := errors.New("storage unavailable")
	if _, err := provider.collect(t.Context(), "OVN_Northbound", "central-0", failingWriter{want}); !errors.Is(err, want) {
		t.Fatalf("lost storage error: %v", err)
	}
}
