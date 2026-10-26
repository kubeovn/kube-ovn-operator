// Package upgrade validates transitions before a workload or CRD is changed.
package upgrade

import (
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
)

type VersionDirection string

const (
	VersionUnchanged VersionDirection = "unchanged"
	VersionUpgrade   VersionDirection = "upgrade"
	VersionRollback  VersionDirection = "rollback"
)

type Contract struct {
	FromVersion     string
	ToVersion       string
	FromCentralMode string
	ToCentralMode   string
	BackupReady     bool
	CRDsReady       bool
	AllowRollback   bool
	RollbackReady   bool
}

// CompareVersions classifies a pinned version transition. The controller must
// not treat a lower target as an ordinary upgrade: database restore and
// workload rollback have a materially different safety contract.
func CompareVersions(from, to string) (VersionDirection, error) {
	fromVersion, err := semver.NewVersion(strings.TrimSpace(from))
	if err != nil || strings.TrimSpace(from) == "" {
		return "", fmt.Errorf("source version %q is not a valid semantic version", from)
	}
	toVersion, err := semver.NewVersion(strings.TrimSpace(to))
	if err != nil || strings.TrimSpace(to) == "" || strings.EqualFold(strings.TrimSpace(to), "latest") {
		return "", fmt.Errorf("target version %q is not a valid pinned semantic version", to)
	}
	switch toVersion.Compare(fromVersion) {
	case 0:
		return VersionUnchanged, nil
	case 1:
		return VersionUpgrade, nil
	default:
		return VersionRollback, nil
	}
}

func Validate(c Contract) error {
	if strings.TrimSpace(c.ToVersion) == "" || strings.EqualFold(c.ToVersion, "latest") {
		return fmt.Errorf("target version must be pinned")
	}
	if c.FromVersion == "" || c.FromVersion == c.ToVersion {
		return nil
	}
	direction, err := CompareVersions(c.FromVersion, c.ToVersion)
	if err != nil {
		return err
	}
	if c.FromCentralMode != c.ToCentralMode {
		return fmt.Errorf("central mode cannot change in place from %q to %q", c.FromCentralMode, c.ToCentralMode)
	}
	if direction == VersionRollback {
		if !c.AllowRollback {
			return fmt.Errorf("version rollback from %q to %q requires allowRollback", c.FromVersion, c.ToVersion)
		}
		if !c.RollbackReady {
			return fmt.Errorf("version rollback from %q to %q requires a version-aware restore executor", c.FromVersion, c.ToVersion)
		}
	}
	if !c.BackupReady {
		return fmt.Errorf("upgrade requires a verified backup checkpoint")
	}
	if !c.CRDsReady {
		return fmt.Errorf("upgrade requires compatible CRDs before workload rollout")
	}
	return nil
}
