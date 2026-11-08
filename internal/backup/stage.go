package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"k8s.io/apimachinery/pkg/types"
)

// StagedSnapshot owns verified bytes independently of the backup store. Its
// private, unlinked file cannot be changed through the public reader or record.
// Close releases the file. Staging proves capture provenance, not current
// quorum, isolation of old processes, or permission to rebuild a cluster.
type StagedSnapshot struct {
	file   *os.File
	record Record
}

func (s *StagedSnapshot) Close() error { return s.file.Close() }

// Reader returns a new read-only view starting at the first verified byte.
// The caller must keep the snapshot open until every consumer has finished.
func (s *StagedSnapshot) Reader() io.Reader {
	return io.NewSectionReader(s.file, 0, s.record.Size)
}

func (s *StagedSnapshot) Record() Record {
	record := s.record
	record.Snapshot.Members = slices.Clone(record.Snapshot.Members)
	return record
}

// Stage verifies the authenticated store record and every native snapshot byte
// without contacting a live Source. The current installation's central Lease
// is still required. At most MaxBytes are staged on disk, never in memory.
func (p *Provider) Stage(ctx context.Context, database, ref string) (*StagedSnapshot, error) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	if err := p.configured(); err != nil {
		return nil, err
	}
	if err := lease.CheckFor(ctx, lease.Central, types.UID(p.InstallationUID)); err != nil {
		return nil, err
	}
	record, data, err := p.Store.Open(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("open staged backup: %w", err)
	}
	defer func() { _ = data.Close() }()
	if record.FormatVersion != 1 || record.Identity.InstallationUID != p.InstallationUID || record.Identity.PodUID == "" || record.Identity.ContainerID == "" || record.Identity.RestartCount < 0 || record.Endpoint == "" || record.Snapshot.Database != database || validSnapshot(record.Snapshot) != nil {
		return nil, errors.New("staged backup is not a valid installation-bound database snapshot")
	}
	if err := p.stagedPolicy(record); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp("", "kube-ovn-staged-backup-*")
	if err != nil {
		return nil, fmt.Errorf("create backup staging file: %w", err)
	}
	// Unlink before returning a handle, so cancellation and crashes leave no
	// named snapshot behind and no later pathname lookup can replace its bytes.
	if err := os.Remove(file.Name()); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("unlink backup staging file: %w", err)
	}
	success := false
	defer func() {
		if !success {
			_ = file.Close()
		}
	}()
	info, err := inspect(ctx, io.TeeReader(data, file), database, p.MaxBytes)
	if err != nil {
		return nil, fmt.Errorf("inspect staged backup: %w", err)
	}
	if info.Size != record.Size || info.SHA256 != record.SHA256 || info.SchemaVersion != record.SchemaVersion {
		return nil, errors.New("staged backup differs from its capture record")
	}
	if err := p.stagedPolicy(record); err != nil {
		return nil, err
	}
	if err := lease.CheckFor(ctx, lease.Central, types.UID(p.InstallationUID)); err != nil {
		return nil, err
	}
	record.Snapshot.Members = slices.Clone(record.Snapshot.Members)
	success = true
	return &StagedSnapshot{file: file, record: record}, nil
}

func (p *Provider) stagedPolicy(record Record) error {
	age := time.Since(record.CompletedAt)
	if record.CompletedAt.IsZero() || age < 0 || age > p.MaxAge || record.Size <= 0 || record.Size > p.MaxBytes {
		return errors.New("staged backup age or size is outside the configured policy")
	}
	return nil
}

// RestoreStagedFenced consumes the exact preverified bytes. It rechecks policy,
// current topology and the Lease token before streaming to the remote fence;
// the store is never reopened between validation and native input.
func (p *Provider) RestoreStagedFenced(ctx context.Context, database, endpoint string, staged *StagedSnapshot, token lease.FenceToken) error {
	return p.restoreStaged(ctx, database, endpoint, staged, &token)
}
