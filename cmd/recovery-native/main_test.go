package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const testClusterID = "cccc0000-1111-2222-3333-444444444444"

func TestRunCreateClusterBindsCIDAndInspectsOnce(t *testing.T) {
	tool := fakeTool(t, `#!/bin/sh
set -eu
case "$1" in
  --cid=`+testClusterID+`)
    test "$2" = create-cluster
    test "$3" = /var/lib/target.db
    ;;
  db-raft-header)
    printf '%s\n' '{"server_id":"eeee0000-1111-2222-3333-555555555555","cluster_id":"`+testClusterID+`","local_address":"tcp:192.0.2.4:6643"}'
    ;;
  *) exit 1 ;;
esac
`)
	if err := run(t.Context(), []string{"--target-cluster-id=" + testClusterID, "create-cluster", "/var/lib/target.db", "/var/lib/backup.db", "tcp:192.0.2.4:6643"}, tool); err != nil {
		t.Fatal(err)
	}
}

func TestRunRejectsRandomCIDFromUnpatchedTool(t *testing.T) {
	tool := fakeTool(t, `#!/bin/sh
set -eu
if test "$1" = --cid=`+testClusterID+`; then exit 0; fi
printf '%s\n' '{"server_id":"eeee0000-1111-2222-3333-555555555555","cluster_id":"ffffffff-1111-2222-3333-555555555555","local_address":"tcp:192.0.2.4:6643"}'
`)
	err := run(t.Context(), []string{"--target-cluster-id=" + testClusterID, "create-cluster", "/var/lib/target.db", "/var/lib/backup.db", "tcp:192.0.2.4:6643"}, tool)
	if err == nil || !strings.Contains(err.Error(), "differs from target") {
		t.Fatalf("random cluster ID was accepted: %v", err)
	}
}

func TestInspectRejectsJoiningHeader(t *testing.T) {
	tool := fakeTool(t, `#!/bin/sh
printf '%s\n' '{"server_id":"eeee0000-1111-2222-3333-555555555555","cluster_id":"`+testClusterID+`","local_address":"tcp:192.0.2.4:6643","remote_addresses":["tcp:192.0.2.5:6643"]}'
`)
	if _, err := inspect(context.Background(), tool, "/var/lib/target.db"); err == nil {
		t.Fatal("joining header was accepted")
	}
}

func TestRunRejectsUnsafeTool(t *testing.T) {
	if err := run(t.Context(), []string{"inspect", "/var/lib/target.db"}, "relative/ovsdb-tool"); err == nil {
		t.Fatal("relative OVSDB_TOOL path accepted")
	}
	missing := filepath.Join(t.TempDir(), "missing-ovsdb-tool")
	if err := run(t.Context(), []string{"inspect", "/var/lib/target.db"}, missing); err == nil {
		t.Fatal("missing OVSDB_TOOL accepted")
	}
}

func fakeTool(t *testing.T, script string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ovsdb-tool")
	if err := os.WriteFile(path, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
