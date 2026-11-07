// Package upgrade validates version-aware rollback admission separately from
// the forward rollout contract. A lower target needs a verified pair, a
// version-pinned chart, and independently fenced restore/workload adapters.
package upgrade

import (
	"fmt"
	"strings"
)

// RollbackContract is the complete admission input for a destructive
// version rollback. Database references are deliberately carried as a pair:
// accepting one database without the other can leave NB and SB at different
// schema/data generations.
type RollbackContract struct {
	FromVersion            string
	ToVersion              string
	FromCentralMode        string
	ToCentralMode          string
	AllowRollback          bool
	BackupID               string
	DatabaseRefs           map[string]string
	TopologyDigest         string
	ChartDigest            string
	RestoreExecutorReady   bool
	WorkloadRollbackReady  bool
	FenceReady             bool
	QuorumApprovalRequired bool
	QuorumApprovalReady    bool
}

// Validate admits only a target that can be restored as one version-bound
// operation. The restore and workload adapters are independent capabilities so
// a database-only implementation cannot accidentally claim a complete
// rollback.
func (c RollbackContract) Validate() error {
	direction, err := CompareVersions(c.FromVersion, c.ToVersion)
	if err != nil {
		return err
	}
	if direction != VersionRollback {
		return fmt.Errorf("rollback contract requires a lower target version, got %q to %q", c.FromVersion, c.ToVersion)
	}
	if c.FromCentralMode == "" || c.FromCentralMode != c.ToCentralMode {
		return fmt.Errorf("rollback cannot change central mode from %q to %q", c.FromCentralMode, c.ToCentralMode)
	}
	if !c.AllowRollback {
		return fmt.Errorf("version rollback from %q to %q requires allowRollback", c.FromVersion, c.ToVersion)
	}
	if !validDigest(c.BackupID) {
		return fmt.Errorf("version rollback from %q to %q requires a verified backup pair", c.FromVersion, c.ToVersion)
	}
	if len(c.DatabaseRefs) != 2 || strings.TrimSpace(c.DatabaseRefs["OVN_Northbound"]) == "" || strings.TrimSpace(c.DatabaseRefs["OVN_Southbound"]) == "" {
		return fmt.Errorf("version rollback from %q to %q requires both NB and SB backup references", c.FromVersion, c.ToVersion)
	}
	if !validDigest(c.TopologyDigest) {
		return fmt.Errorf("version rollback from %q to %q requires a topology-bound backup pair", c.FromVersion, c.ToVersion)
	}
	if !validDigest(c.ChartDigest) {
		return fmt.Errorf("version rollback from %q to %q requires a digest-pinned target chart", c.FromVersion, c.ToVersion)
	}
	if !c.RestoreExecutorReady {
		return fmt.Errorf("version rollback from %q to %q requires a version-aware database restore executor", c.FromVersion, c.ToVersion)
	}
	if !c.WorkloadRollbackReady {
		return fmt.Errorf("version rollback from %q to %q requires a coordinated Kubernetes workload rollback executor", c.FromVersion, c.ToVersion)
	}
	if !c.FenceReady {
		return fmt.Errorf("version rollback from %q to %q requires an atomic restore fence", c.FromVersion, c.ToVersion)
	}
	if c.QuorumApprovalRequired && !c.QuorumApprovalReady {
		return fmt.Errorf("version rollback from %q to %q requires explicit lost-quorum approval", c.FromVersion, c.ToVersion)
	}
	return nil
}

func validDigest(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	return strings.Trim(value[len("sha256:"):], "0123456789abcdef") == ""
}
