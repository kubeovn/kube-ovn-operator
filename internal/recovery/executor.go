package recovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
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
// selected target member. Isolation must remain enforced throughout the call;
// repeated checks cannot make a transient observation into a fence.
// A Kubernetes adapter may implement this interface for the checks used by a
// guard, but this package never performs those mutations itself.
type Isolation interface {
	OldProcessesIsolated(context.Context, Plan) error
	TargetStorageOwned(context.Context, Member, string) error
}

// IsolationGuard is the lifetime capability for an offline rebuild. Begin
// must establish enforced fencing and target ownership; Check must verify that
// the same enforcement is still active, and Close must release it. A sequence
// of independent observations is not a substitute for this capability.
type IsolationGuard interface {
	Check(context.Context) error
	Close(context.Context) error
}

// IsolationSession is implemented by providers that can hold fencing for the
// complete native write window. The executor refuses providers that only
// implement the legacy point-in-time Isolation checks.
type IsolationSession interface {
	Begin(context.Context, Plan, Member, string) (IsolationGuard, error)
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

type operationRequest struct {
	Plan       Plan   `json:"plan"`
	PlanDigest string `json:"planDigest"`
	MemberName string `json:"memberName"`
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

const (
	requestFile = "request.json"
	resultFile  = "result.json"
)

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
	session, ok := e.Isolation.(IsolationSession)
	if !ok {
		return Result{}, errors.New("offline rebuild requires a continuous isolation session")
	}
	guard, err := session.Begin(ctx, canonical, member, e.Root)
	if err != nil {
		return Result{}, fmt.Errorf("begin offline rebuild isolation: %w", err)
	}
	if guard == nil {
		return Result{}, errors.New("offline rebuild isolation session returned no guard")
	}
	defer func() {
		if guard != nil {
			_ = guard.Close(context.Background())
		}
	}()
	root, lock, err := openRoot(e.Root)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = root.Close(); _ = lock.Close() }()
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	operation, err := openOperation(root, canonical, digest, member.Name)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = operation.Close() }()
	if result, err := e.readResult(ctx, operation, canonical, digest, member); err != nil {
		return Result{}, err
	} else if result != nil {
		if err := e.checkCapabilities(ctx, canonical, member, guard); err != nil {
			return Result{}, err
		}
		closeErr := guard.Close(ctx)
		guard = nil
		if closeErr != nil {
			return Result{}, fmt.Errorf("close offline rebuild isolation: %w", closeErr)
		}
		return *result, nil
	}
	// Authenticate the complete pair before any native database is created.
	contents := make(map[string]string, len(canonical.Databases))
	for _, database := range canonical.Databases {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		staged, err := e.stageBackup(ctx, operation, canonical, database)
		if err != nil {
			return Result{}, fmt.Errorf("stage %s backup: %w", database.Name, err)
		}
		contents[database.Name] = staged
	}
	result := Result{PlanDigest: digest, MemberName: member.Name, Databases: make([]NativeDatabase, 0, len(canonical.Databases))}
	for _, database := range canonical.Databases {
		if err := e.checkCapabilities(ctx, canonical, member, guard); err != nil {
			return Result{}, err
		}
		dbPath := filepath.Join(operation.Name(), databaseFile(database.Name))
		if existing, err := operation.Lstat(databaseFile(database.Name)); err == nil {
			if !privateRegular(existing) {
				return Result{}, fmt.Errorf("existing %s target database is not a private regular file", database.Name)
			}
		} else if errors.Is(err, os.ErrNotExist) {
			if err := e.Native.CreateCluster(ctx, dbPath, contents[database.Name], member.RAFTAddress(database.Name), database.TargetClusterID); err != nil {
				return Result{}, fmt.Errorf("create %s target cluster: %w", database.Name, err)
			}
		} else {
			return Result{}, fmt.Errorf("inspect existing %s target database: %w", database.Name, err)
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
	if err := e.checkCapabilities(ctx, canonical, member, guard); err != nil {
		return Result{}, err
	}
	if err := writeJSON(operation, resultFile, result); err != nil {
		return Result{}, fmt.Errorf("publish offline rebuild result: %w", err)
	}
	closeErr := guard.Close(ctx)
	guard = nil
	if closeErr != nil {
		return Result{}, fmt.Errorf("close offline rebuild isolation: %w", closeErr)
	}
	return result, nil
}

func (e *Executor) checkCapabilities(ctx context.Context, plan Plan, member Member, guard IsolationGuard) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if guard != nil {
		if err := guard.Check(ctx); err != nil {
			return fmt.Errorf("verify continuous offline rebuild isolation: %w", err)
		}
		return nil
	}
	if err := e.Isolation.OldProcessesIsolated(ctx, plan); err != nil {
		return fmt.Errorf("prove retained process isolation: %w", err)
	}
	if err := e.Isolation.TargetStorageOwned(ctx, member, e.Root); err != nil {
		return fmt.Errorf("prove target storage ownership: %w", err)
	}
	return nil
}

func (e *Executor) stageBackup(ctx context.Context, operation *os.Root, plan Plan, database Database) (string, error) {
	record, source, err := e.Backups.Open(ctx, database.BackupRef)
	if err != nil {
		return "", err
	}
	defer func() { _ = source.Close() }()
	if record.FormatVersion != 1 || record.Identity.InstallationUID != plan.InstallationUID || record.Identity.PodUID == "" || record.Identity.ContainerID == "" || record.Endpoint == "" || record.CompletedAt.IsZero() || record.Snapshot.Database != database.Name || record.Snapshot.ClusterID != database.SourceClusterID || record.Size <= 0 || record.Size > e.MaxBackupBytes || !validHexDigest(record.SHA256) {
		return "", errors.New("backup provenance does not match the recovery plan")
	}
	name := backupFile(database.Name)
	temporary := "." + name + ".pending"
	file, err := operation.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return "", err
	}
	success := false
	defer func() {
		if !success {
			_ = operation.Remove(temporary)
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
	if existing, err := operation.Lstat(name); err == nil {
		if !privateRegular(existing) {
			return "", errors.New("existing staged backup is not a private regular file")
		}
		if err := verifyFile(operation, name, record.Size, record.SHA256, e.MaxBackupBytes); err != nil {
			return "", fmt.Errorf("existing staged backup differs from the authenticated record: %w", err)
		}
		_ = operation.Remove(temporary)
	} else if errors.Is(err, os.ErrNotExist) {
		if err := operation.Rename(temporary, name); err != nil {
			return "", err
		}
	} else {
		return "", err
	}
	success = true
	return filepath.Join(operation.Name(), name), nil
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

func openOperation(root *os.Root, plan Plan, digest, memberName string) (*os.Root, error) {
	info, err := root.Lstat(plan.OperationID)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := root.Mkdir(plan.OperationID, 0700); err != nil {
			return nil, fmt.Errorf("create rebuild operation directory: %w", err)
		}
	case err != nil:
		return nil, fmt.Errorf("inspect rebuild operation directory: %w", err)
	case !info.IsDir() || info.Mode().Perm() != 0700:
		return nil, errors.New("rebuild operation directory must be a private directory")
	}
	operation, err := root.OpenRoot(plan.OperationID)
	if err != nil {
		return nil, err
	}
	if err := cleanOperationEntries(operation); err != nil {
		_ = operation.Close()
		return nil, err
	}
	request := operationRequest{Plan: plan, PlanDigest: digest, MemberName: memberName}
	if info, err := operation.Lstat(requestFile); errors.Is(err, os.ErrNotExist) {
		entries, readErr := operationEntries(operation)
		if readErr != nil {
			_ = operation.Close()
			return nil, readErr
		}
		if len(entries) != 0 {
			_ = operation.Close()
			return nil, errors.New("unjournaled rebuild operation directory is not safe to reuse")
		}
		if err := writeJSON(operation, requestFile, request); err != nil {
			_ = operation.Close()
			return nil, fmt.Errorf("publish offline rebuild request: %w", err)
		}
	} else if err != nil || !privateRegular(info) {
		_ = operation.Close()
		return nil, errors.New("offline rebuild request must be a private regular file")
	} else {
		content, err := readRegular(operation, requestFile, 64<<10)
		if err != nil {
			_ = operation.Close()
			return nil, fmt.Errorf("read offline rebuild request: %w", err)
		}
		var recorded operationRequest
		if err := json.Unmarshal(content, &recorded, json.RejectUnknownMembers(true)); err != nil || recorded.PlanDigest != digest || recorded.MemberName != memberName {
			_ = operation.Close()
			return nil, errors.New("offline rebuild request differs from the authorized plan")
		}
		_, recordedDigest, err := recorded.Plan.Canonical()
		if err != nil || recordedDigest != digest {
			_ = operation.Close()
			return nil, errors.New("offline rebuild request has an invalid plan digest")
		}
	}
	return operation, nil
}

func cleanOperationEntries(operation *os.Root) error {
	entries, err := operationEntries(operation)
	if err != nil {
		return err
	}
	allowed := map[string]bool{
		requestFile: true, resultFile: true,
		"ovnnb_backup.db": true, "ovnsb_backup.db": true,
		"ovnnb_db.db": true, "ovnsb_db.db": true,
		"." + requestFile + ".pending": true, "." + resultFile + ".pending": true,
		".ovnnb_backup.db.pending": true, ".ovnsb_backup.db.pending": true,
	}
	for _, entry := range entries {
		if !allowed[entry.Name()] {
			return fmt.Errorf("unexpected file %q in offline rebuild operation", entry.Name())
		}
		if strings.HasSuffix(entry.Name(), ".pending") {
			if err := operation.Remove(entry.Name()); err != nil {
				return fmt.Errorf("remove stale offline rebuild temporary: %w", err)
			}
		}
	}
	return nil
}

func operationEntries(operation *os.Root) ([]os.DirEntry, error) {
	directory, err := operation.Open(".")
	if err != nil {
		return nil, err
	}
	entries, readErr := directory.ReadDir(-1)
	return entries, errors.Join(readErr, directory.Close())
}

func (e *Executor) readResult(ctx context.Context, operation *os.Root, plan Plan, digest string, member Member) (*Result, error) {
	info, err := operation.Lstat(resultFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil || !privateRegular(info) {
		return nil, errors.New("offline rebuild result must be a private regular file")
	}
	content, err := readRegular(operation, resultFile, 64<<10)
	if err != nil {
		return nil, fmt.Errorf("read offline rebuild result: %w", err)
	}
	var result Result
	if err := json.Unmarshal(content, &result, json.RejectUnknownMembers(true)); err != nil {
		return nil, errors.New("invalid offline rebuild result")
	}
	if result.PlanDigest != digest || result.MemberName != member.Name || len(result.Databases) != len(plan.Databases) {
		return nil, errors.New("offline rebuild result differs from the authorized plan")
	}
	for index, database := range plan.Databases {
		info, err := operation.Lstat(databaseFile(database.Name))
		if err != nil || !privateRegular(info) {
			return nil, errors.New("persisted target database is not a private regular file")
		}
		path := filepath.Join(operation.Name(), databaseFile(database.Name))
		observed, err := e.Native.Inspect(ctx, path)
		if err != nil {
			return nil, fmt.Errorf("reinspect %s target cluster: %w", database.Name, err)
		}
		if observed != result.Databases[index] {
			return nil, errors.New("offline rebuild result identity changed")
		}
		if err := validateNative(database, member, observed, result.Databases[:index]); err != nil {
			return nil, fmt.Errorf("validate persisted %s target identity: %w", database.Name, err)
		}
	}
	return &result, nil
}

func writeJSON(root *os.Root, name string, value any) error {
	content, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temporary := "." + name + ".pending"
	file, err := root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	written, writeErr := file.Write(content)
	if writeErr == nil && written != len(content) {
		writeErr = io.ErrShortWrite
	}
	closeErr := errors.Join(writeErr, file.Sync(), file.Close())
	if closeErr != nil {
		_ = root.Remove(temporary)
		return closeErr
	}
	if err := root.Rename(temporary, name); err != nil {
		_ = root.Remove(temporary)
		return err
	}
	return nil
}

func verifyFile(root *os.Root, name string, size int64, expected string, maxBytes int64) error {
	file, err := root.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	got, err := io.Copy(io.Discard, io.TeeReader(io.LimitReader(file, maxBytes+1), hash))
	if err != nil {
		return err
	}
	if got != size || hex.EncodeToString(hash.Sum(nil)) != expected {
		return errors.New("file content or size differs")
	}
	return nil
}

func readRegular(root *os.Root, name string, maxBytes int64) ([]byte, error) {
	file, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	content, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > maxBytes {
		return nil, errors.New("journal file exceeds its size limit")
	}
	return content, nil
}

func privateRegular(info os.FileInfo) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Geteuid() && stat.Nlink == 1
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
