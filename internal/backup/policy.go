package backup

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	// RetentionMaxAge prevents an accidental duration from retaining an
	// effectively unbounded or immediately-expiring object set.
	RetentionMaxAge  = 365 * 24 * time.Hour
	RetentionMaxKeep = 10000
)

type RetentionPolicy struct {
	KeepLast int
	MaxAge   time.Duration
}

type RetentionResult struct {
	Scanned   int
	Protected int
	Deleted   int
}

// ParseRetention validates the optional CRD retention fields. Both zero values
// disable cleanup; at least one non-zero value is required to enable it.
func ParseRetention(keepLast int32, rawMaxAge string) (RetentionPolicy, error) {
	if keepLast < 0 || int64(keepLast) > RetentionMaxKeep {
		return RetentionPolicy{}, fmt.Errorf("backup retention keepLast must be between 0 and %d", RetentionMaxKeep)
	}
	value := strings.TrimSpace(rawMaxAge)
	if value == "" {
		return RetentionPolicy{KeepLast: int(keepLast)}, nil
	}
	maxAge, err := time.ParseDuration(value)
	if err != nil {
		return RetentionPolicy{}, fmt.Errorf("backup retention maxAge must be a duration: %w", err)
	}
	if maxAge <= 0 || maxAge > RetentionMaxAge {
		return RetentionPolicy{}, fmt.Errorf("backup retention maxAge must be greater than zero and at most %s", RetentionMaxAge)
	}
	return RetentionPolicy{KeepLast: int(keepLast), MaxAge: maxAge}, nil
}

// Cleanup deletes only complete pairs outside the keep-last and age windows.
// Protected references always win, including when they would otherwise be
// older than the configured policy. A deletion error stops the pass so the
// caller can persist the failure and retry without claiming success.
func Cleanup(ctx context.Context, store RetentionStore, policy RetentionPolicy, protected []string, now time.Time) (RetentionResult, error) {
	if store == nil {
		return RetentionResult{}, errors.New("backup retention requires a retention store")
	}
	if policy.KeepLast == 0 && policy.MaxAge == 0 {
		return RetentionResult{}, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	objects, err := store.List(ctx)
	if err != nil {
		return RetentionResult{}, err
	}
	slices.SortStableFunc(objects, func(left, right Object) int {
		return right.Record.CompletedAt.Compare(left.Record.CompletedAt)
	})
	protectedSet := make(map[string]struct{}, len(protected))
	for _, reference := range protected {
		if strings.TrimSpace(reference) != "" {
			protectedSet[reference] = struct{}{}
		}
	}
	result := RetentionResult{Scanned: len(objects)}
	for index, object := range objects {
		if _, ok := protectedSet[object.Reference]; ok {
			result.Protected++
			continue
		}
		keep := index < policy.KeepLast
		if policy.MaxAge > 0 && !object.Record.CompletedAt.Before(now.Add(-policy.MaxAge)) {
			keep = true
		}
		if keep {
			continue
		}
		if err := store.Delete(ctx, object.Reference); err != nil {
			return result, fmt.Errorf("delete backup %s: %w", object.Reference, err)
		}
		result.Deleted++
	}
	return result, nil
}

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
