package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

func TestStageSurvivesStoreMutationWithoutLiveSource(t *testing.T) {
	provider, source, directory := fixture(t)
	err := withLeases(t, func(ctx context.Context) error {
		ref, err := provider.Capture(ctx, "OVN_Northbound", "central-0")
		if err != nil {
			return err
		}
		provider.Source = nil
		staged, err := provider.Stage(ctx, "OVN_Northbound", ref)
		if err != nil {
			return err
		}
		defer func() { _ = staged.Close() }()
		path := filepath.Join(directory, strings.TrimPrefix(ref, "sha256:"), "snapshot.db")
		if err := os.WriteFile(path, []byte("damaged after staging"), 0600); err != nil {
			return err
		}
		data, err := io.ReadAll(staged.Reader())
		if err != nil || !bytes.Equal(data, source.data) {
			return errors.New("staged bytes changed with the backup store")
		}
		record := staged.Record()
		record.Snapshot.Members[0].Address = "tcp:192.0.2.99:6643"
		if staged.Record().Snapshot.Members[0].Address == record.Snapshot.Members[0].Address {
			return errors.New("staged record exposed mutable provenance")
		}
		provider.Source = source
		token, err := lease.Token(ctx, lease.Central)
		if err != nil {
			return err
		}
		if err := provider.RestoreStagedFenced(ctx, "OVN_Northbound", "central-0", staged, token); err != nil {
			return err
		}
		if !bytes.Equal(source.restored, source.data) || source.fencedToken == nil || *source.fencedToken != token {
			return errors.New("restore did not consume the exact staged bytes through fencing")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestStageAndStagedRestoreRejectInvalidInputs(t *testing.T) {
	for _, scenario := range []string{"corrupt", "expired", "oversized", "foreign installation", "wrong database", "no lease", "stale token", "closed", "expired after stage", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			provider, source, directory := fixture(t)
			err := withLeases(t, func(ctx context.Context) error {
				ref, err := provider.Capture(ctx, "OVN_Northbound", "central-0")
				if err != nil {
					return err
				}
				database := "OVN_Northbound"
				switch scenario {
				case "corrupt":
					if err := os.WriteFile(filepath.Join(directory, strings.TrimPrefix(ref, "sha256:"), "snapshot.db"), nativeBackup(testSchema, strings.ReplaceAll(testRows, "saved", "other")), 0600); err != nil {
						return err
					}
				case "expired":
					provider.MaxAge = time.Nanosecond
				case "oversized":
					provider.MaxBytes = 8
				case "foreign installation":
					provider.InstallationUID = "foreign"
				case "wrong database":
					database = "OVN_Southbound"
				case "no lease":
					ctx = t.Context()
				case "cancelled":
					cancelled, cancel := context.WithCancel(ctx)
					cancel()
					ctx = cancelled
				}
				staged, err := provider.Stage(ctx, database, ref)
				if err != nil {
					if scenario == "stale token" || scenario == "closed" || scenario == "expired after stage" {
						return err
					}
					return nil
				}
				defer func() { _ = staged.Close() }()
				token, err := lease.Token(ctx, lease.Central)
				if err != nil {
					return err
				}
				switch scenario {
				case "stale token":
					token.Epoch++
				case "closed":
					if err := staged.Close(); err != nil {
						return err
					}
				case "expired after stage":
					provider.MaxAge = time.Nanosecond
				default:
					return errors.New("invalid backup reached staging")
				}
				if err := provider.RestoreStagedFenced(ctx, database, "central-0", staged, token); err == nil {
					return errors.New("invalid staged restore was accepted")
				}
				if source.restored != nil || source.fencedToken != nil {
					return errors.New("rejected staged restore reached native input")
				}
				return nil
			})
			if err != nil && !(scenario == "cancelled" && errors.Is(err, context.Canceled)) {
				t.Fatal(err)
			}
		})
	}
}
