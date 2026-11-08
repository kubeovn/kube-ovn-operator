package recovery

import (
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"strings"
	"testing"
)

const (
	operationID = "eeee0000-1111-2222-3333-444444444444"
	sourceNB    = "aaaa0000-1111-2222-3333-444444444444"
	sourceSB    = "bbbb0000-1111-2222-3333-444444444444"
	targetNB    = "cccc0000-1111-2222-3333-444444444444"
	targetSB    = "dddd0000-1111-2222-3333-444444444444"
)

func validPlan() Plan {
	refs := map[string]string{
		"OVN_Northbound": "sha256:" + strings.Repeat("a", 64),
		"OVN_Southbound": "sha256:" + strings.Repeat("b", 64),
	}
	data, _ := json.Marshal(refs, json.Deterministic(true))
	return Plan{
		FormatVersion: FormatVersion, InstallationUID: "installation-uid", OperationID: operationID,
		SourceVersion: "v1.16.13", TargetVersion: "v1.16.13", BackupPairID: "sha256:" + stringHash(data),
		Approval:        Approval{Name: "lost-quorum-approval", UID: "approval-uid", DecisionDigest: "sha256:" + strings.Repeat("c", 64)},
		RetainedMembers: []Member{member("old", "1", "2", "3", "4", "5")},
		TargetMembers:   []Member{member("new", "6", "7", "8", "9", "a")},
		Databases: []Database{
			{Name: "OVN_Northbound", BackupRef: refs["OVN_Northbound"], SourceClusterID: sourceNB, TargetClusterID: targetNB},
			{Name: "OVN_Southbound", BackupRef: refs["OVN_Southbound"], SourceClusterID: sourceSB, TargetClusterID: targetSB},
		},
	}
}

func member(name, node, workload, pod, container, volume string) Member {
	return Member{
		Name: name, NodeName: "node-" + node,
		Storage:               StorageIdentity{NodeUID: "node-uid-" + node, WorkloadUID: "workload-uid-" + workload, PodUID: "pod-uid-" + pod, ContainerID: "container-" + container, VolumeUID: "volume-uid-" + volume},
		NorthboundRAFTAddress: "tcp:192.0.2." + node + ":6643", SouthboundRAFTAddress: "tcp:192.0.2." + node + ":6644",
		NorthboundClient: "tcp:192.0.2." + node + ":6641", SouthboundClient: "tcp:192.0.2." + node + ":6642",
	}
}

func stringHash(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func TestCanonicalBindsPairApprovalAndFreshStorage(t *testing.T) {
	plan := validPlan()
	canonical, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if digest == "" || canonical.Databases[0].Name != "OVN_Northbound" || canonical.TargetMembers[0].Name != "new" {
		t.Fatalf("unexpected canonical plan: %+v digest=%q", canonical, digest)
	}
	canonical.TargetMembers[0].Name = "mutated"
	if plan.TargetMembers[0].Name != "new" {
		t.Fatal("canonicalization retained mutable input")
	}
}

func TestCanonicalRejectsUnsafeRebuilds(t *testing.T) {
	cases := map[string]func(*Plan){
		"missing approval":      func(plan *Plan) { plan.Approval.UID = "" },
		"pair mismatch":         func(plan *Plan) { plan.BackupPairID = "sha256:" + strings.Repeat("f", 64) },
		"source cluster reused": func(plan *Plan) { plan.Databases[0].TargetClusterID = plan.Databases[0].SourceClusterID },
		"target storage reused": func(plan *Plan) { plan.TargetMembers[0].Storage.VolumeUID = plan.RetainedMembers[0].Storage.VolumeUID },
		"even target count": func(plan *Plan) {
			plan.TargetMembers = append(plan.TargetMembers, member("new-2", "7", "8", "9", "b", "c"))
		},
		"duplicate endpoint": func(plan *Plan) {
			plan.TargetMembers[0].SouthboundRAFTAddress = plan.TargetMembers[0].NorthboundRAFTAddress
		},
		"wrong database": func(plan *Plan) { plan.Databases[0].Name = "OVN_Unknown" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			plan := validPlan()
			mutate(&plan)
			if _, _, err := plan.Canonical(); err == nil {
				t.Fatal("unsafe rebuild plan was accepted")
			}
		})
	}
}
