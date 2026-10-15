package prepare

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"time"
	"uuid"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

// Preparer owns no network credentials. The root must already exist, be private
// to the current OS user and support flock, atomic rename and directory fsync.
// All preparation actors for this root must honor the same lock. Root/ancestor
// replacement by a privileged actor is outside this trusted storage contract.
type Preparer struct {
	root *os.Root
	tool string
	run  func(context.Context, ...string) (string, error)
}

func Open(directory, tool string) (*Preparer, error) {
	if !filepath.IsAbs(directory) || !filepath.IsAbs(tool) {
		return nil, errors.New("preparation root and ovsdb-tool must be absolute paths")
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("preparation root must be an existing private directory without a symlink")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return nil, errors.New("preparation root belongs to another OS user")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return nil, err
	}
	p := &Preparer{root: root, tool: tool}
	p.run = p.command
	return p, nil
}

func (p *Preparer) Close() error { return p.root.Close() }

// Prepare commits the request before creating either database, then publishes
// the pair's native identities after both validate and fsync. A retry preserves
// partial valid files; a corrupt/incompatible file stops preparation. Published
// files may have joined and advanced since a previous call, so retries validate
// identities rather than comparing a digest of mutable RAFT data.
func (p *Preparer) Prepare(ctx context.Context, requested Plan) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	plan, digest, err := canonical(requested)
	if err != nil {
		return Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return Result{}, err
	}
	lock, err := p.lock()
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = lock.Close() }()
	if err := p.request(plan); err != nil {
		return Result{}, err
	}
	resultPath := plan.OperationID + "/prepared.json"
	var existing Result
	if err := p.readJSON(resultPath, &existing); err == nil {
		if existing.PlanDigest != digest || len(existing.Databases) != 2 {
			return Result{}, errors.New("prepared result does not match the immutable request")
		}
		for i, db := range plan.Databases {
			observed, err := p.inspect(ctx, plan.OperationID, db)
			if err != nil {
				return Result{}, err
			}
			if observed != existing.Databases[i] {
				return Result{}, errors.New("prepared database identity changed")
			}
		}
		return existing, ctx.Err()
	} else if !errors.Is(err, os.ErrNotExist) {
		return Result{}, err
	}
	result := Result{PlanDigest: digest}
	for _, db := range plan.Databases {
		path := plan.OperationID + "/" + databaseFile(db.Name)
		if _, err := p.root.Lstat(path); errors.Is(err, os.ErrNotExist) {
			args := []string{"--cid=" + db.ClusterID, "join-cluster", commandPath(path), db.Name, db.LocalAddress}
			args = append(args, db.RemoteAddresses...)
			if _, err := p.run(ctx, args...); err != nil {
				return Result{}, fmt.Errorf("prepare %s (existing files retained): %w", db.Name, err)
			}
		} else if err != nil {
			return Result{}, err
		}
		observed, err := p.inspect(ctx, plan.OperationID, db)
		if err != nil {
			return Result{}, err
		}
		file, err := p.regular(path, os.O_RDWR)
		if err != nil {
			return Result{}, err
		}
		if err := errors.Join(file.Chmod(0600), file.Sync(), file.Close()); err != nil {
			return Result{}, err
		}
		if err := p.syncDirectory(plan.OperationID); err != nil {
			return Result{}, err
		}
		result.Databases = append(result.Databases, observed)
	}
	if err := errors.Join(p.syncDirectory(plan.OperationID), ctx.Err()); err != nil {
		return Result{}, err
	}
	if err := p.writeJSON(resultPath, result); err != nil {
		return Result{}, err
	}
	var committed Result
	if err := p.readJSON(resultPath, &committed); err != nil {
		return Result{}, err
	}
	if !reflect.DeepEqual(result, committed) {
		return Result{}, errors.New("prepared result readback differs")
	}
	return result, ctx.Err()
}

func (p *Preparer) request(plan Plan) error {
	name := plan.OperationID + "/request.json"
	var existing Plan
	if err := p.readJSON(name, &existing); err == nil {
		if !reflect.DeepEqual(existing, plan) {
			return errors.New("operation directory belongs to another preparation request")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := p.root.Lstat(plan.OperationID); !errors.Is(err, os.ErrNotExist) {
		return errors.New("existing operation directory has no valid preparation request")
	}
	temporary := ".request-" + uuid.New().String()
	if err := p.root.Mkdir(temporary, 0700); err != nil {
		return err
	}
	// Only this newly created, unpublished request directory may be cleaned up.
	// It cannot contain database files, which are created after publication.
	defer func() { _ = p.root.RemoveAll(temporary) }()
	if err := p.writeJSON(temporary+"/request.json", plan); err != nil {
		return err
	}
	if err := p.root.Rename(temporary, plan.OperationID); err != nil {
		return err
	}
	return p.syncDirectory(".")
}

func (p *Preparer) inspect(ctx context.Context, operation string, db Database) (PreparedDatabase, error) {
	path := operation + "/" + databaseFile(db.Name)
	file, err := p.regular(path, os.O_RDONLY)
	if err != nil {
		return PreparedDatabase{}, err
	}
	info, err := file.Stat()
	_ = file.Close()
	if err != nil || info.Size() == 0 {
		return PreparedDatabase{}, errors.New("database file is empty or unavailable; manual investigation is required")
	}
	for command, expected := range map[string]string{"db-name": db.Name, "db-cid": db.ClusterID, "db-local-address": db.LocalAddress, "db-is-clustered": ""} {
		actual, err := p.run(ctx, command, commandPath(path))
		if err != nil || actual != expected {
			return PreparedDatabase{}, fmt.Errorf("%s failed %s identity check; existing data retained", db.Name, command)
		}
	}
	sid, err := p.run(ctx, "db-sid", commandPath(path))
	if err != nil {
		return PreparedDatabase{}, err
	}
	if !membership.ValidID(sid) {
		return PreparedDatabase{}, errors.New("prepared database has an invalid server UUID")
	}
	return PreparedDatabase{Name: db.Name, ClusterID: db.ClusterID, ServerID: sid, Address: db.LocalAddress, Path: path}, nil
}

func (p *Preparer) lock() (*os.File, error) {
	file, err := p.root.OpenFile(".prepare.lock", os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if errors.Is(err, os.ErrExist) {
		file, err = p.regular(".prepare.lock", os.O_RDWR)
	}
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, errors.New("another actor holds the preparation root lock")
	}
	return file, nil
}

func (p *Preparer) regular(name string, flags int) (*os.File, error) {
	// Reject all intermediate symlinks as well as the final entry. os.Root
	// additionally prevents directory escapes when opening through its handle.
	parts := strings.Split(name, "/")
	for i := range len(parts) - 1 {
		info, err := p.root.Lstat(strings.Join(parts[:i+1], "/"))
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			return nil, errors.New("preparation path is not a real directory")
		}
	}
	info, err := p.root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("preparation object is not a regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 || int(stat.Uid) != os.Geteuid() {
		return nil, errors.New("preparation object has another owner or multiple hard links")
	}
	file, err := p.root.OpenFile(name, flags, 0)
	if err != nil {
		return nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = file.Close()
		return nil, errors.New("preparation object changed while opening")
	}
	return file, nil
}

func (p *Preparer) readJSON(path string, target any) error {
	file, err := p.regular(path, os.O_RDONLY)
	if err != nil {
		return err
	}
	content, readErr := io.ReadAll(io.LimitReader(file, (16<<10)+1))
	if err := errors.Join(readErr, file.Close()); err != nil {
		return err
	}
	if len(content) > 16<<10 {
		return errors.New("preparation record exceeds 16 KiB")
	}
	if err := json.Unmarshal(content, target, json.RejectUnknownMembers(true)); err != nil {
		return errors.New("invalid preparation record; existing data retained")
	}
	return nil
}

func (p *Preparer) writeJSON(path string, value any) error {
	content, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temporary := path + ".pending-" + uuid.New().String()
	file, err := p.root.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer func() { _ = p.root.Remove(temporary) }()
	_, writeErr := file.Write(content)
	if err := errors.Join(writeErr, file.Sync(), file.Close()); err != nil {
		return err
	}
	if err := p.root.Rename(temporary, path); err != nil {
		return err
	}
	return p.syncDirectory(filepath.Dir(path))
}

func (p *Preparer) syncDirectory(name string) error {
	file, err := p.root.Open(name)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func commandPath(path string) string { return "/proc/self/fd/3/" + path }

func (p *Preparer) command(ctx context.Context, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// Pin the root in the child; do not resolve a potentially replaced mount
	// pathname again for each command. Every file argument is under this fd.
	directory, err := p.root.Open(".")
	if err != nil {
		return "", err
	}
	defer func() { _ = directory.Close() }()
	cmd := exec.CommandContext(ctx, p.tool, args...)
	cmd.ExtraFiles = []*os.File{directory}
	cmd.WaitDelay = time.Second
	output := new(limitedOutput)
	cmd.Stdout = output
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("ovsdb-tool failed: %w", err)
	}
	return strings.TrimSpace(output.buffer.String()), ctx.Err()
}

// Do not embed bytes.Buffer: its promoted ReadFrom would let io.Copy bypass
// Write's output bound when os/exec drains the child process.
type limitedOutput struct{ buffer bytes.Buffer }

func (w *limitedOutput) Write(data []byte) (int, error) {
	if len(data) > 4096-w.buffer.Len() {
		return 0, errors.New("ovsdb-tool metadata output exceeds 4 KiB")
	}
	return w.buffer.Write(data)
}
