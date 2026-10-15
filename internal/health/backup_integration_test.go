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
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLiveOVNBackupSchemas(t *testing.T) {
	schemaDirectory := os.Getenv("OVN_TEST_SCHEMA_DIR")
	if schemaDirectory == "" {
		t.Skip("set OVN_TEST_SCHEMA_DIR for native OVN schema backup/restore tests")
	}
	for _, database := range []string{"OVN_Northbound", "OVN_Southbound"} {
		t.Run(database, func(t *testing.T) {
			directory := t.TempDir()
			schemaName, table := "ovn-nb.ovsschema", "NB_Global"
			if database == "OVN_Southbound" {
				schemaName, table = "ovn-sb.ovsschema", "SB_Global"
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			address := "tcp:" + listener.Addr().String()
			defer func() { _ = listener.Close() }()
			db := filepath.Join(directory, "db")
			runOVSDBTool(t, "create-cluster", db, filepath.Join(schemaDirectory, schemaName), address)
			tool := func(command string) string {
				t.Helper()
				output, err := exec.CommandContext(t.Context(), ovsdbBinary(t, "ovsdb-tool"), command, db).Output()
				if err != nil {
					t.Fatal(err)
				}
				return strings.TrimSpace(string(output))
			}
			transport := &ovnadmin.CommandClient{ClusterID: tool("db-cid"), Endpoints: []ovnadmin.Endpoint{{Name: "central", ServerID: tool("db-sid"), Address: address}}}
			transport.Runner = membershipRunFunc(func(ctx context.Context, _ string, command []string) ([]byte, error) {
				command[0], command[3] = ovsdbBinary(t, "ovs-appctl"), filepath.Join(directory, "ctl")
				return exec.CommandContext(ctx, command[0], command[1:]...).Output()
			})
			_ = listener.Close()
			startOVSDB(t, directory, db)
			waitOVSDB(t, func() error { _, err := transport.Status(t.Context(), database, "central"); return err })
			client := func(dir, method, transaction string) []byte {
				t.Helper()
				output, err := exec.CommandContext(t.Context(), ovsdbBinary(t, "ovsdb-client"), "--timeout=5", method, "unix:"+filepath.Join(dir, "socket"), transaction).Output()
				if err != nil {
					t.Fatal(err)
				}
				return output
			}
			client(directory, "transact", fmt.Sprintf(`[%q,{"op":"insert","table":%q,"row":{"options":["map",[["backup-test","preserved"]]]}}]`, database, table))
			store, err := backup.OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			provider := &backup.Provider{Source: &liveBackupSource{transport: transport, directories: map[string]string{"central": directory}, client: ovsdbBinary(t, "ovsdb-client")}, Store: store, InstallationUID: "test-installation", MaxBytes: 8 << 20, MaxAge: time.Hour}
			scheme := runtime.NewScheme()
			if err := coordinationv1.AddToScheme(scheme); err != nil {
				t.Fatal(err)
			}
			manager := lease.New(fake.NewClientBuilder().WithScheme(scheme).Build())
			installation := &operatorv1alpha1.KubeOVNInstallation{Name: "backup", Namespace: "test", UID: "test-installation"}
			var reference string
			err = manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
				return manager.With(ctx, installation, lease.Central, func(ctx context.Context) error {
					var err error
					reference, err = provider.Capture(ctx, database, "central")
					return err
				})
			})
			if err != nil {
				t.Fatal(err)
			}
			_, data, err := store.Open(t.Context(), reference)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = data.Close() }()
			restoredDirectory := t.TempDir()
			restoredPath := filepath.Join(restoredDirectory, "restored.db")
			file, err := os.OpenFile(restoredPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Fatal(err)
			}
			_, copyErr := io.Copy(file, data)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				t.Fatalf("copy snapshot: %v %v", copyErr, closeErr)
			}
			startOVSDB(t, restoredDirectory, restoredPath)
			waitOVSDB(t, func() error { _, err := os.Stat(filepath.Join(restoredDirectory, "socket")); return err })
			query := fmt.Sprintf(`[%q,{"op":"select","table":%q,"where":[],"columns":["_uuid","options"]}]`, database, table)
			original, recovered := client(directory, "query", query), client(restoredDirectory, "query", query)
			var originalRows, recoveredRows any
			if json.Unmarshal(original, &originalRows) != nil || json.Unmarshal(recovered, &recoveredRows) != nil || !reflect.DeepEqual(originalRows, recoveredRows) || !strings.Contains(string(recovered), "preserved") {
				t.Fatal("native OVN schema restore did not preserve rows")
			}
			t.Logf("%s native backup and standalone restore preserved row UUIDs and map values", database)
		})
	}
}
