package main

import (
	"bytes"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"uuid"

	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
)

func TestPreparationCommandRejectsInvalidInput(t *testing.T) {
	for _, scenario := range []string{"missing path", "malformed", "oversized", "FIFO"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "request.json")
			if scenario == "FIFO" {
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			} else if scenario != "missing path" {
				data := "bad-json"
				if scenario == "oversized" {
					data = strings.Repeat(" ", 17<<10)
				}
				if err := os.WriteFile(path, []byte(data), 0600); err != nil {
					t.Fatal(err)
				}
			}
			var output bytes.Buffer
			if err := run(t.Context(), root, path, "/unused-tool", &output); err == nil || output.Len() != 0 {
				t.Fatalf("invalid input emitted a result: %s %v", output.String(), err)
			}
		})
	}
}

func TestLiveOVNPreparationCommand(t *testing.T) {
	toolDir := os.Getenv("OVSDB_TEST_BIN")
	if toolDir == "" {
		t.Skip("set OVSDB_TEST_BIN for the native preparation command test")
	}
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	plan := prepare.Plan{FormatVersion: 1, InstallationUID: "installation-uid", OperationID: uuid.New().String(), MemberName: "central-3", Databases: []prepare.Database{
		{Name: "OVN_Northbound", ClusterID: uuid.New().String(), LocalAddress: "tcp:192.0.2.4:6643", RemoteAddresses: []string{"tcp:192.0.2.1:6643"}},
		{Name: "OVN_Southbound", ClusterID: uuid.New().String(), LocalAddress: "tcp:192.0.2.4:6644", RemoteAddresses: []string{"tcp:192.0.2.1:6644"}},
	}}
	content, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "request.json")
	if err := os.WriteFile(path, content, 0600); err != nil {
		t.Fatal(err)
	}
	var first prepare.Result
	for i := range 2 {
		var output bytes.Buffer
		if err := run(t.Context(), root, path, filepath.Join(toolDir, "ovsdb-tool"), &output); err != nil {
			t.Fatal(err)
		}
		var result prepare.Result
		if err := json.Unmarshal(output.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Databases) != 2 || result.PlanDigest == "" {
			t.Fatal("incomplete command output")
		}
		if i == 0 {
			first = result
		} else if !reflect.DeepEqual(first, result) {
			t.Fatal("command retry changed prepared identities")
		}
	}
}
