// Package backup captures immutable OVN snapshots and verifies their provenance
// and contents before a lifecycle operation can use them.
package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"k8s.io/apimachinery/pkg/types"
)

// Identity pins the Kubernetes installation and the exact process serving the
// backup. Source implementations must validate these identities on every call.
type Identity struct {
	InstallationUID string `json:"installationUID"`
	PodUID          string `json:"podUID"`
	ContainerID     string `json:"containerID"`
	RestartCount    int32  `json:"restartCount"`
}

// Source must query and export the same pinned database process. Stream emits
// native ovsdb-client backup output, returns command failures, and honors ctx.
type Source interface {
	Identity(context.Context, string) (Identity, error)
	Status(context.Context, string, string) (ovnadmin.Snapshot, error)
	Stream(context.Context, string, string, io.Writer) error
}

type Record struct {
	FormatVersion int               `json:"formatVersion"`
	Identity      Identity          `json:"identity"`
	Endpoint      string            `json:"endpoint"`
	Snapshot      ovnadmin.Snapshot `json:"snapshot"`
	CompletedAt   time.Time         `json:"completedAt"`
	SchemaVersion string            `json:"schemaVersion"`
	Size          int64             `json:"size"`
	SHA256        string            `json:"sha256"`
}

// Store publishes data and metadata together, only after produce succeeds.
// References bind the complete metadata, including the data digest. Open must
// authenticate that binding before returning a record. Stores are trusted and
// must be writable only by the operator; a digest is not an approval signature.
type Store interface {
	Publish(context.Context, func(io.Writer) (Record, error)) (string, error)
	Open(context.Context, string) (Record, io.ReadCloser, error)
}

type Provider struct {
	Source          Source
	Store           Store
	InstallationUID string
	// MaxBytes bounds a single snapshot; MaxAge is an explicit lifecycle policy.
	MaxBytes int64
	MaxAge   time.Duration
}

var _ ovnadmin.BackupVerifier = (*Provider)(nil)

func (p *Provider) configured() error {
	if p.Store == nil || p.InstallationUID == "" || p.MaxBytes <= 0 || p.MaxBytes > 1<<30 || p.MaxAge <= 0 {
		return errors.New("backup requires a store, installation UID, size limit (at most 1 GiB) and positive maximum age")
	}
	return nil
}

// Capture requires an unchanged committed fence across a native backup. Native
// backup files do not encode RAFT identities or indexes. Concurrent writes or
// elections therefore cause a retry, never an inferred newer backup fence.
func (p *Provider) Capture(ctx context.Context, database, endpoint string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	if err := p.configured(); err != nil {
		return "", err
	}
	if p.Source == nil || endpoint == "" {
		return "", errors.New("backup source and endpoint are required")
	}
	if err := lease.CheckFor(ctx, lease.Central, types.UID(p.InstallationUID)); err != nil {
		return "", err
	}
	identity, err := p.Source.Identity(ctx, endpoint)
	if err != nil {
		return "", err
	}
	if identity.InstallationUID != p.InstallationUID || identity.PodUID == "" || identity.ContainerID == "" || identity.RestartCount < 0 {
		return "", errors.New("backup source identity is incomplete or belongs to another installation")
	}
	before, err := p.Source.Status(ctx, database, endpoint)
	if err != nil {
		return "", err
	}
	if err := validSnapshot(before); err != nil {
		return "", err
	}
	if before.Database != database {
		return "", errors.New("backup source returned another database")
	}
	ref, err := p.Store.Publish(ctx, func(destination io.Writer) (Record, error) {
		info, err := p.collect(ctx, database, endpoint, destination)
		if err != nil {
			return Record{}, err
		}
		after, err := p.Source.Status(ctx, database, endpoint)
		if err != nil {
			return Record{}, err
		}
		if err := validSnapshot(after); err != nil {
			return Record{}, err
		}
		if !sameFence(before, after) {
			return Record{}, errors.New("database identity, membership or committed fence changed during backup")
		}
		current, err := p.Source.Identity(ctx, endpoint)
		if err != nil {
			return Record{}, err
		}
		if current != identity {
			return Record{}, errors.New("backup source process changed during capture")
		}
		if err := lease.CheckFor(ctx, lease.Central, types.UID(p.InstallationUID)); err != nil {
			return Record{}, err
		}
		return Record{FormatVersion: 1, Identity: identity, Endpoint: endpoint, Snapshot: before, CompletedAt: time.Now().UTC(), SchemaVersion: info.SchemaVersion, Size: info.Size, SHA256: info.SHA256}, nil
	})
	if err != nil {
		return "", fmt.Errorf("capture backup: %w", err)
	}
	// A successful upload alone is not evidence: reopen and verify stored bytes.
	if err := p.Verify(ctx, before, ref); err != nil {
		return "", fmt.Errorf("verify published backup: %w", err)
	}
	return ref, nil
}

func (p *Provider) collect(ctx context.Context, database, endpoint string, destination io.Writer) (fileInfo, error) {
	reader, writer := io.Pipe()
	type result struct {
		info fileInfo
		err  error
	}
	done := make(chan result, 1)
	stored := &captureWriter{destination: destination}
	go func() {
		info, err := inspect(ctx, io.TeeReader(reader, stored), database, p.MaxBytes)
		_ = reader.CloseWithError(err)
		done <- result{info, err}
	}()
	err := p.Source.Stream(ctx, database, endpoint, writer)
	_ = writer.CloseWithError(err)
	observed := <-done
	return observed.info, errors.Join(err, observed.err, stored.err)
}

type captureWriter struct {
	destination io.Writer
	err         error
}

func (w *captureWriter) Write(data []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	n, err := w.destination.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	w.err = err
	return n, err
}

// Verify can be used after an operator restart without a live Source. It binds
// the backup to the final membership observation, then re-reads all stored data.
func (p *Provider) Verify(ctx context.Context, snapshot ovnadmin.Snapshot, ref string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := p.configured(); err != nil {
		return err
	}
	if err := lease.CheckFor(ctx, lease.Central, types.UID(p.InstallationUID)); err != nil {
		return err
	}
	if err := validSnapshot(snapshot); err != nil {
		return err
	}
	record, data, err := p.Store.Open(ctx, ref)
	if err != nil {
		return err
	}
	defer func() { _ = data.Close() }()
	if record.FormatVersion != 1 || record.Identity.InstallationUID != p.InstallationUID || record.Identity.PodUID == "" || record.Identity.ContainerID == "" || record.Identity.RestartCount < 0 || record.Endpoint == "" || validSnapshot(record.Snapshot) != nil || !sameFence(record.Snapshot, snapshot) {
		return errors.New("backup does not cover the installation, database identity and current committed fence")
	}
	age := time.Since(record.CompletedAt)
	if record.CompletedAt.IsZero() || age < 0 || age > p.MaxAge || record.Size <= 0 || record.Size > p.MaxBytes {
		return errors.New("backup age or size is outside the configured policy")
	}
	info, err := inspect(ctx, data, snapshot.Database, p.MaxBytes)
	if err != nil {
		return err
	}
	if info.Size != record.Size || info.SHA256 != record.SHA256 || info.SchemaVersion != record.SchemaVersion {
		return errors.New("stored backup differs from its capture record")
	}
	if time.Since(record.CompletedAt) > p.MaxAge {
		return errors.New("backup expired during verification")
	}
	return lease.CheckFor(ctx, lease.Central, types.UID(p.InstallationUID))
}

func validSnapshot(s ovnadmin.Snapshot) error {
	if (s.Database != "OVN_Northbound" && s.Database != "OVN_Southbound") || !membership.ValidID(s.ClusterID) || s.Term < 1 || s.CommitIndex < 1 || s.AppliedIndex != s.CommitIndex {
		return errors.New("backup requires a valid RAFT identity and applied committed fence")
	}
	if err := membership.ValidateMembers(s.Members); err != nil {
		return err
	}
	leaders, healthy := 0, 0
	for _, member := range s.Members {
		if member.Healthy {
			healthy++
		}
		if member.Leader {
			leaders++
			if !member.Healthy || member.ServerID != s.LeaderServerID {
				return errors.New("backup leader identity is inconsistent")
			}
		}
	}
	if leaders != 1 || healthy < len(s.Members)/2+1 {
		return errors.New("backup requires a healthy leader and quorum")
	}
	return nil
}

func sameFence(a, b ovnadmin.Snapshot) bool {
	if a.Database != b.Database || a.ClusterID != b.ClusterID || a.LeaderServerID != b.LeaderServerID || a.Term != b.Term || a.CommitIndex != b.CommitIndex || a.AppliedIndex != b.AppliedIndex || len(a.Members) != len(b.Members) {
		return false
	}
	addresses := make(map[string]string, len(a.Members))
	for _, member := range a.Members {
		addresses[member.ServerID] = member.Address
	}
	for _, member := range b.Members {
		if addresses[member.ServerID] != member.Address {
			return false
		}
	}
	return true
}
