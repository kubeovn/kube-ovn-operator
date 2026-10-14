// Package upgrade validates transitions before a workload or CRD is changed.
package upgrade

import (
	"fmt"
	"strings"
)

type Contract struct {
	FromVersion     string
	ToVersion       string
	FromCentralMode string
	ToCentralMode   string
	BackupReady     bool
	CRDsReady       bool
}

func Validate(c Contract) error {
	if strings.TrimSpace(c.ToVersion) == "" || strings.EqualFold(c.ToVersion, "latest") {
		return fmt.Errorf("target version must be pinned")
	}
	if c.FromVersion == "" || c.FromVersion == c.ToVersion {
		return nil
	}
	if c.FromCentralMode != c.ToCentralMode {
		return fmt.Errorf("central mode cannot change in place from %q to %q", c.FromCentralMode, c.ToCentralMode)
	}
	if !c.BackupReady {
		return fmt.Errorf("upgrade requires a verified backup checkpoint")
	}
	if !c.CRDsReady {
		return fmt.Errorf("upgrade requires compatible CRDs before workload rollout")
	}
	return nil
}
