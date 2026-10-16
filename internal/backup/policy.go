package backup

import (
	"fmt"
	"strings"
	"time"
)

const (
	// ScheduledBackupMinInterval prevents a tight loop from turning a backup
	// destination into an unbounded workload.
	ScheduledBackupMinInterval = time.Minute
	// ScheduledBackupMaxInterval keeps a configured schedule meaningful while
	// leaving retention as a separate policy.
	ScheduledBackupMaxInterval = 30 * 24 * time.Hour
)

// ParseSchedule parses the interval used by the controller's backup
// checkpoint scheduler. An empty value disables scheduling.
func ParseSchedule(raw string) (time.Duration, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("backup schedule must be a duration: %w", err)
	}
	if interval < ScheduledBackupMinInterval {
		return 0, fmt.Errorf("backup schedule must be at least %s", ScheduledBackupMinInterval)
	}
	if interval > ScheduledBackupMaxInterval {
		return 0, fmt.Errorf("backup schedule must be at most %s", ScheduledBackupMaxInterval)
	}
	return interval, nil
}

// ValidateSchedule is a small predicate for callers that only need to know
// whether scheduling is configured and valid.
func ValidateSchedule(raw string) error {
	_, err := ParseSchedule(raw)
	return err
}
