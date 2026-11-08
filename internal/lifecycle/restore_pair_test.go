package lifecycle

import (
	"bytes"
	"context"
	"crypto/sha1" // Native OVSDB framing uses SHA-1; capture records use SHA-256.
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func stagedPairBytes(database string) []byte {
	schema := fmt.Sprintf(`{"name":%q,"version":"1.0.0","tables":{"Root":{"isRoot":true,"columns":{"name":{"type":"string"}}}}}`, database)
	rows := `{"Root":{"aaaaaaaa-1111-2222-3333-444444444444":{"name":"saved"}}}`
	var result []byte
	for _, value := range []string{schema, rows} {
		data := []byte(value + "\n")
		result = fmt.Appendf(result, "OVSDB JSON %d %x\n%s", len(data), sha1.Sum(data), data)
	}
	return result
}

func TestStageRestorePairRejectsInvalidSecondSnapshotWithoutLiveSource(t *testing.T) {
	for _, scenario := range []string{"valid", "corrupt", "foreign installation", "foreign cluster", "wrong member", "wrong address", "wrong database", "expired", "missing", "pair ID"} {
		t.Run(scenario, func(t *testing.T) {
			f := setup(t)
			directory := t.TempDir()
			temporary := t.TempDir()
			t.Setenv("TMPDIR", temporary)
			store, err := backup.OpenFileStore(directory)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			refs := map[string]string{}
			originals := map[string][]byte{}
			for index, database := range databaseNames {
				data := stagedPairBytes(database)
				record := backup.Record{FormatVersion: 1, Identity: backup.Identity{InstallationUID: string(f.installation.UID), PodUID: "original-pod", ContainerID: "containerd://original"}, Endpoint: "a-0", Snapshot: f.dbs[index].snapshot, CompletedAt: time.Now().UTC(), SchemaVersion: "1.0.0", Size: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
				if index == 1 {
					switch scenario {
					case "foreign installation":
						record.Identity.InstallationUID = "other-installation"
					case "foreign cluster":
						record.Snapshot.ClusterID = "99999999-2222-3333-4444-555555555555"
					case "wrong member":
						record.Snapshot.Members[1].ServerID = "99999999-2222-3333-4444-555555555555"
					case "wrong address":
						record.Snapshot.Members[1].Address = "tcp:192.0.2.99:6644"
					case "wrong database":
						record.Snapshot.Database = "OVN_Northbound"
					case "expired":
						record.CompletedAt = time.Now().Add(-2 * time.Hour)
					}
				}
				ref, err := store.Publish(t.Context(), func(destination io.Writer) (backup.Record, error) {
					_, err := destination.Write(data)
					return record, err
				})
				if err != nil {
					t.Fatal(err)
				}
				refs[database], originals[database] = ref, data
			}
			if scenario == "corrupt" {
				path := filepath.Join(directory, strings.TrimPrefix(refs[databaseNames[1]], "sha256:"), "snapshot.db")
				if err := os.WriteFile(path, bytes.ReplaceAll(originals[databaseNames[1]], []byte("saved"), []byte("other")), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "missing" {
				refs[databaseNames[1]] = "sha256:" + strings.Repeat("f", 64)
			}
			status := &api.BackupStatus{ID: backupPairID(refs), DatabaseRefs: refs, CompletedAt: metav1.Now()}
			if scenario == "pair ID" {
				status.ID = "sha256:" + strings.Repeat("e", 64)
			}
			manager := &Manager{StoreFactory: func(*api.KubeOVNInstallation) (backup.Store, error) { return backup.OpenFileStore(directory) }, MaxAge: time.Hour, MaxBytes: 1 << 20}
			err = f.guarded(t, func(ctx context.Context) error {
				staged, err := manager.stageRestorePair(ctx, f.installation, f.request.Databases, status)
				if scenario != "valid" {
					if err == nil || staged[0] != nil || staged[1] != nil {
						return fmt.Errorf("invalid pair exposed partial input: error=%v handles=%v", err, staged)
					}
					return nil
				}
				if err != nil {
					return err
				}
				defer func() { _ = staged[0].Close(); _ = staged[1].Close() }()
				// The store opened by stageRestorePair is already closed here.
				for index, database := range databaseNames {
					data, err := io.ReadAll(staged[index].Reader())
					if err != nil || !bytes.Equal(data, originals[database]) {
						return errors.New("pair lost its authenticated staged bytes after store close")
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(temporary)
			if err != nil || len(entries) != 0 {
				t.Fatalf("staging left named backup files: entries=%v error=%v", entries, err)
			}
			fds, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Fatal(err)
			}
			for _, fd := range fds {
				path, _ := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
				if strings.HasPrefix(path, temporary+"/") {
					t.Fatalf("staging leaked a descriptor: %s", path)
				}
			}
		})
	}
}
