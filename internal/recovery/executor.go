package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/kubeovn/kube-ovn-operator/internal/backup"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

// Isolation is the privileged precondition boundary for an offline rebuild.
// Implementations must prove that every retained process is fenced/stopped
// before native files are created and that the target root belongs to the
// selected target member. A Kubernetes adapter may implement this interface,
// but this package never performs those mutations itself.
type Isolation interface {
	OldProcessesIsolated(context.Context, Plan) error
	TargetStorageOwned(context.Context, Member, string) error
}

// Native creates one fresh clustered database from a verified standalone
// backup and returns its on-disk identity. The target cluster ID is explicit in
// the call because an implementation that silently lets the native tool pick a
// different ID cannot be bound to an approved recovery plan.
type Native interface {
	CreateCluster(context.Context, string, string, string, string) error
	Inspect(context.Context, string) (NativeDatabase, error)
}

type NativeDatabase struct {
	ClusterID string `json:"clusterID"`
	ServerID  string `json:"serverID"`
	Address   string `json:"address"`
}

// Result is the durable native identity produced for one target member. The
// caller must persist it before starting ovsdb-server or northd.
type Result struct {
	PlanDigest string           `json:"planDigest"`
	MemberName string           `json:"memberName"`
	Databases  []NativeDatabase `json:"databases"`
}

type Executor struct {
	Root      string
	Backups   backup.Store
	Isolation Isolation
	Native    Native
	// MaxBackupBytes bounds each standalone backup while it is copied into the
	// target root. It must be positive and no larger than 1 GiB.
	MaxBackupBytes int64
}

// Rebuild validates and materializes both databases for one target member. It
// only writes below Root/operationID and never starts a process, removes a
// PVC, changes Kubernetes objects, or touches retained storage.
func (e *Executor) Rebuild(ctx context.Context, plan Plan, memberName string) (Result, error) {
	if e == nil || e.Backups == nil || e.Isolation == nil || e.Native == nil {
		return Result{}, errors.New("offline rebuild requires backups, isolation and native capabilities")
	}
	if e.MaxBackupBytes <= 0 || e.MaxBackupBytes > 1<<30 {
		return Result{}, errors.New("offline rebuild backup limit must be positive and at most 1 GiB")
	}
	canonical, digest, err := plan.Canonical()
	if err != nil {
		return Result{}, err
	}
	member, ok := targetMember(canonical.TargetMembers, memberName)
	if !ok {
		return Result{}, fmt.Errorf("offline rebuild target member %q is not in the plan", memberName)
	}
	if err := e.Isolation.OldProcessesIsolated(ctx, canonical); err != nil {
		return Result{}, fmt.Errorf("prove retained process isolation: %w", err)
	}
	if err := e.Isolation.TargetStorageOwned(ctx, member, e.Root); err != nil {
		return Result{}, fmt.Errorf("prove target storage ownership: %w", err)
	}
	root, lock, err := openRoot(e.Root)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = root.Close(); _ = lock.Close() }()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	if err := root.Mkdir(canonical.OperationID, 0700); err != nil {
		return Result{}, fmt.Errorf("create rebuild operation directory: %w", err)
	}
	operation := filepath.Join(e.Root, canonical.OperationID)
	result := Result{PlanDigest: digest, MemberName: member.Name, Databases: make([]NativeDatabase, 0, len(canonical.Databases))}
	for _, database := range canonical.Databases {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		contents, err := e.stageBackup(ctx, canonical, database)
		if err != nil {
			return Result{}, fmt.Errorf("stage %s backup: %w", database.Name, err)
		}
		dbPath := filepath.Join(operation, databaseFile(database.Name))
		if err := e.Native.CreateCluster(ctx, dbPath, contents, member.RAFTAddress(database.Name), database.TargetClusterID); err != nil {
			return Result{}, fmt.Errorf("create %s target cluster: %w", database.Name, err)
		}
		observed, err := e.Native.Inspect(ctx, dbPath)
		if err != nil {
			return Result{}, fmt.Errorf("inspect %s target cluster: %w", database.Name, err)
		}
		if err := validateNative(database, member, observed, result.Databases); err != nil {
			return Result{}, fmt.Errorf("validate %s target identity: %w", database.Name, err)
		}
		result.Databases = append(result.Databases, observed)
	}
	return result, nil
}

func (e *Executor) stageBackup(ctx context.Context, plan Plan, database Database) (string, error) {
	record, source, err := e.Backups.Open(ctx, database.BackupRef)
	if err != nil {
		return "", err
	}
	defer func() { _ = source.Close() }()
	if record.FormatVersion != 1 || record.Identity.InstallationUID != plan.InstallationUID || record.Identity.PodUID == "" || record.Identity.ContainerID == "" || record.Endpoint == "" || record.CompletedAt.IsZero() || record.Snapshot.Database != database.Name || record.Snapshot.ClusterID != database.SourceClusterID || record.Size <= 0 || record.Size > e.MaxBackupBytes || !validHexDigest(record.SHA256) {
		return "", errors.New("backup provenance does not match the recovery plan")
	}
	root, err := os.OpenRoot(filepath.Join(e.Root, plan.OperationID))
	if err != nil {
		return "", err
	}
	defer func() { _ = root.Close() }()
	name := backupFile(database.Name)
	temporary := "." + name + ".pending"
	file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", err
	}
	success := false
	defer func() {
		if !success {
			_ = root.Remove(temporary)
		}
	}()
	hash := sha256.New()
	limited := io.LimitReader(source, e.MaxBackupBytes+1)
	written, copyErr := io.Copy(file, io.TeeReader(limited, hash))
	closeErr := errors.Join(file.Sync(), file.Close())
	if err := errors.Join(copyErr, closeErr); err != nil {
		return "", err
	}
	if written != record.Size || hex.EncodeToString(hash.Sum(nil)) != record.SHA256 {
		return "", errors.New("backup bytes differ from the authenticated record")
	}
	if err := root.Rename(temporary, name); err != nil {
		return "", err
	}
	success = true
	return filepath.Join(e.Root, plan.OperationID, name), nil
}

func openRoot(directory string) (*os.Root, *os.File, error) {
	if !filepath.IsAbs(directory) {
		return nil, nil, errors.New("offline rebuild root must be absolute")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, nil, errors.New("offline rebuild root must be an existing private directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return nil, nil, errors.New("offline rebuild root belongs to another OS user")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, nil, err
	}
	lock, err := root.OpenFile(".rebuild.lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		_ = root.Close()
		return nil, nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		_ = root.Close()
		return nil, nil, errors.New("another offline rebuild holds the target root")
	}
	return root, lock, nil
}

func validateNative(database Database, member Member, observed NativeDatabase, previous []NativeDatabase) error {
	if observed.ClusterID != database.TargetClusterID || !membership.ValidID(observed.ServerID) || observed.Address != member.RAFTAddress(database.Name) {
		return errors.New("native identity differs from the authorized target")
	}
	for _, identity := range previous {
		if identity.ClusterID == observed.ClusterID || identity.ServerID == observed.ServerID || identity.Address == observed.Address {
			return errors.New("NB and SB native identities are not distinct")
		}
	}
	return nil
}

func targetMember(members []Member, name string) (Member, bool) {
	for _, member := range members {
		if member.Name == name {
			return member, true
		}
	}
	return Member{}, false
}

func databaseFile(name string) string {
	if name == "OVN_Northbound" {
		return "ovnnb_db.db"
	}
	return "ovnsb_db.db"
}

func backupFile(name string) string {
	if name == "OVN_Northbound" {
		return "ovnnb_backup.db"
	}
	return "ovnsb_backup.db"
}

func validHexDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil && strings.ToLower(value) == value
}

// RAFTAddress returns the database-specific address of a target member.
func (m Member) RAFTAddress(database string) string {
	if database == "OVN_Northbound" {
		return m.NorthboundRAFTAddress
	}
	return m.SouthboundRAFTAddress
}
