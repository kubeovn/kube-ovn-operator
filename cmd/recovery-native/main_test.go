package main

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
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

func TestParseWorkloadArgumentsRequiresAbsoluteBoundInputs(t *testing.T) {
	valid := []string{
		"--workload-plan=/plan/plan.json", "--isolation-proof=/proof/proof.json",
		"--target-root=/target", "--proc-root=/proc", "--backup-dir=/backups", "--member=new",
	}
	if _, err := parseWorkloadArguments(valid); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"--workload-plan=relative"},
		append(append([]string(nil), valid...), "--member=other"),
		append(append([]string(nil), valid...), "--unknown=/value"),
	} {
		if _, err := parseWorkloadArguments(args); err == nil {
			t.Fatalf("unsafe workload arguments accepted: %v", args)
		}
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

func TestWorkloadRejectsMetadataWithoutEnforcedIsolation(t *testing.T) {
	root := t.TempDir()
	refs := map[string]string{"OVN_Northbound": "sha256:" + strings.Repeat("a", 64), "OVN_Southbound": "sha256:" + strings.Repeat("b", 64)}
	pair, err := json.Marshal(refs, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	member := func(name, node string) recovery.Member {
		return recovery.Member{Name: name, NodeName: "node-" + node,
			Storage:               recovery.StorageIdentity{NodeUID: "node-" + node, WorkloadUID: "workload-" + node, PodUID: "pod-" + node, ContainerID: "container-" + node, VolumeUID: "volume-" + node},
			NorthboundRAFTAddress: "tcp:192.0.2." + node + ":6643", SouthboundRAFTAddress: "tcp:192.0.2." + node + ":6644",
			NorthboundClient: "tcp:192.0.2." + node + ":6641", SouthboundClient: "tcp:192.0.2." + node + ":6642"}
	}
	plan := recovery.Plan{FormatVersion: recovery.FormatVersion, InstallationUID: "installation", OperationID: "eeee0000-1111-2222-3333-444444444444",
		SourceVersion: "v1.16.13", TargetVersion: "v1.16.13", TopologyDigest: "sha256:" + strings.Repeat("d", 64), BackupPairID: fmt.Sprintf("sha256:%x", sha256.Sum256(pair)),
		Approval:        recovery.Approval{Name: "approval", UID: "approval-uid", DecisionDigest: "sha256:" + strings.Repeat("c", 64)},
		RetainedMembers: []recovery.Member{member("old", "1")}, TargetMembers: []recovery.Member{member("new", "2")},
		Databases: []recovery.Database{
			{Name: "OVN_Northbound", BackupRef: refs["OVN_Northbound"], SourceClusterID: "aaaa0000-1111-2222-3333-444444444444", TargetClusterID: testClusterID},
			{Name: "OVN_Southbound", BackupRef: refs["OVN_Southbound"], SourceClusterID: "bbbb0000-1111-2222-3333-444444444444", TargetClusterID: "dddd0000-1111-2222-3333-444444444444"},
		},
	}
	_, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	proof := recovery.IsolationProof{PlanDigest: digest, OperationID: plan.OperationID, RetainedMembers: []string{"old"}, VerifiedAt: time.Now()}
	write := func(name string, value any) string {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	planPath, proofPath := write("plan.json", plan), write("proof.json", proof)
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	// The retained member is remote; an empty local process table proves nothing.
	err = runWorkload(t.Context(), []string{"--workload-plan=" + planPath, "--isolation-proof=" + proofPath, "--target-root=" + target, "--proc-root=" + root, "--backup-dir=" + root, "--member=new"})
	if err == nil || !strings.Contains(err.Error(), "enforced isolation") {
		t.Fatalf("missing isolation capability: %v", err)
	}
	entries, err := os.ReadDir(target)
	if err != nil || len(entries) != 0 {
		t.Fatalf("disabled workload modified target: %v, %v", entries, err)
	}
}
