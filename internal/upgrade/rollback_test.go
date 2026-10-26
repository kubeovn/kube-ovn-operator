package upgrade

import (
	"strings"
	"testing"
)

func TestRollbackContractRequiresBothRestoreCapabilitiesAndFence(t *testing.T) {
	contract := RollbackContract{
		FromVersion:     "v1.17.0",
		ToVersion:       "v1.16.0",
		FromCentralMode: "raft",
		ToCentralMode:   "raft",
		AllowRollback:   true,
		BackupID:        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		DatabaseRefs: map[string]string{
			"OVN_Northbound": "sha256:nb",
			"OVN_Southbound": "sha256:sb",
		},
		TopologyDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ChartDigest:    "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	for _, test := range []struct {
		name string
		set  func(*RollbackContract)
		want string
	}{
		{name: "database restore", set: func(*RollbackContract) {}, want: "database restore executor"},
		{name: "workload rollback", set: func(c *RollbackContract) { c.RestoreExecutorReady = true }, want: "workload rollback executor"},
		{name: "restore fence", set: func(c *RollbackContract) { c.RestoreExecutorReady, c.WorkloadRollbackReady = true, true }, want: "atomic restore fence"},
	} {
		t.Run(test.name, func(t *testing.T) {
			current := contract
			test.set(&current)
			if err := current.Validate(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate() error = %v, want %q", err, test.want)
			}
		})
	}
	contract.RestoreExecutorReady, contract.WorkloadRollbackReady, contract.FenceReady = true, true, true
	if err := contract.Validate(); err != nil {
		t.Fatalf("complete rollback contract was rejected: %v", err)
	}
}

func TestRollbackContractRequiresExplicitQuorumApproval(t *testing.T) {
	contract := RollbackContract{
		FromVersion: "v1.17.0", ToVersion: "v1.16.0", FromCentralMode: "raft", ToCentralMode: "raft", AllowRollback: true,
		BackupID: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", DatabaseRefs: map[string]string{"OVN_Northbound": "nb", "OVN_Southbound": "sb"},
		TopologyDigest:       "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ChartDigest:          "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		RestoreExecutorReady: true, WorkloadRollbackReady: true, FenceReady: true,
		QuorumApprovalRequired: true,
	}
	if err := contract.Validate(); err == nil || !strings.Contains(err.Error(), "lost-quorum approval") {
		t.Fatalf("Validate() error = %v, want lost-quorum approval", err)
	}
	contract.QuorumApprovalReady = true
	if err := contract.Validate(); err != nil {
		t.Fatalf("approved rollback contract was rejected: %v", err)
	}
}
