package fencing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

const defaultRuntimeDirectory = "/var/run/ovn"

const (
	IsolationPath = "/v1/fence/isolate"
	CheckPath     = "/v1/fence/check"
	ReleasePath   = "/v1/fence/release"
)

// ProcessController is the sidecar-local process fence. Its methods run
// synchronously while the HTTP handler holds the matching Lease token.
type ProcessController interface {
	Isolate(context.Context, lease.FenceToken) error
	Check(context.Context, lease.FenceToken) error
	Release(context.Context, lease.FenceToken) error
}

var processPidFiles = map[string]string{
	"ovnnb_db":   "ovsdb-server",
	"ovnsb_db":   "ovsdb-server",
	"ovn-northd": "ovn-northd",
}

// ProcessIsolation suspends the exact native processes owned by one central
// member. PID files and /proc command lines are checked together so a reused
// PID cannot be mistaken for the database process that the plan retained.
// The runtime directory must be shared read-only with the fence sidecar and
// the Pod must use a shared PID namespace.
type ProcessIsolation struct {
	RuntimeDirectory string
	ReadProc         func(int) (string, byte, error)
	Signal           func(int, syscall.Signal) error

	mu        sync.Mutex
	isolated  bool
	pids      map[string]int
	lastToken lease.FenceToken
}

func (p *ProcessIsolation) Isolate(ctx context.Context, token lease.FenceToken) error {
	if err := ValidateToken(token); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.isolated {
		if err := p.checkLocked(ctx); err != nil {
			return err
		}
		// Gate has already compared the request with the authoritative Lease.
		// Rebinding here lets a new Lease holder safely manage a process that
		// remained stopped after an old holder disappeared.
		p.lastToken = token
		return nil
	}
	pids, err := p.readProcesses()
	if err != nil {
		return err
	}
	stopped := make([]int, 0, len(pids))
	for _, pid := range pids {
		if err := ctx.Err(); err != nil {
			p.resume(stopped)
			return err
		}
		if err := p.signal(pid, syscall.SIGSTOP); err != nil {
			p.resume(stopped)
			return fmt.Errorf("stop retained process %d: %w", pid, err)
		}
		stopped = append(stopped, pid)
	}
	p.pids, p.lastToken, p.isolated = pids, token, true
	if err := p.checkLocked(ctx); err != nil {
		p.resume(stopped)
		p.pids, p.lastToken, p.isolated = nil, lease.FenceToken{}, false
		return err
	}
	return nil
}

func (p *ProcessIsolation) Check(ctx context.Context, token lease.FenceToken) error {
	if err := ValidateToken(token); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.isolated {
		return ErrStale
	}
	if err := p.checkLocked(ctx); err != nil {
		return err
	}
	p.lastToken = token
	return nil
}

func (p *ProcessIsolation) Release(ctx context.Context, token lease.FenceToken) error {
	if err := ValidateToken(token); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.isolated {
		return ErrStale
	}
	if err := p.checkLocked(ctx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var releaseErr error
	for _, pid := range p.pids {
		if err := p.signal(pid, syscall.SIGCONT); err != nil && !errors.Is(err, syscall.ESRCH) {
			releaseErr = errors.Join(releaseErr, fmt.Errorf("resume retained process %d: %w", pid, err))
		}
	}
	if releaseErr != nil {
		return releaseErr
	}
	p.pids, p.lastToken, p.isolated = nil, lease.FenceToken{}, false
	return nil
}

func (p *ProcessIsolation) checkLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for name, expectedPID := range p.pids {
		pid, command, state, err := p.readProcess(name)
		if err != nil {
			return err
		}
		if pid != expectedPID || state != 'T' && state != 't' {
			return fmt.Errorf("retained process %q is not continuously stopped", name)
		}
		if !validProcessCommand(command, processPidFiles[name], name+".pid") {
			return fmt.Errorf("retained process %q identity changed", name)
		}
	}
	return nil
}

func (p *ProcessIsolation) readProcesses() (map[string]int, error) {
	pids := make(map[string]int, len(processPidFiles))
	for name := range processPidFiles {
		pid, command, state, err := p.readProcess(name)
		if err != nil {
			return nil, err
		}
		if !validProcessCommand(command, processPidFiles[name], name+".pid") {
			return nil, fmt.Errorf("runtime process %q has an unexpected command", name)
		}
		if state == 'Z' || state == 'X' {
			return nil, fmt.Errorf("runtime process %q is not running", name)
		}
		pids[name] = pid
	}
	return pids, nil
}

func (p *ProcessIsolation) readProcess(name string) (int, string, byte, error) {
	directory := p.RuntimeDirectory
	if directory == "" {
		directory = defaultRuntimeDirectory
	}
	if !filepath.IsAbs(directory) {
		return 0, "", 0, errors.New("fence runtime directory must be absolute")
	}
	pidPath := filepath.Join(directory, name+".pid")
	info, err := os.Lstat(pidPath)
	if err != nil {
		return 0, "", 0, fmt.Errorf("read %s PID file: %w", name, err)
	}
	if !ownedPIDFile(info) {
		return 0, "", 0, fmt.Errorf("runtime process %q PID file is not a private regular file", name)
	}
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return 0, "", 0, fmt.Errorf("read %s PID file: %w", name, err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 1 {
		return 0, "", 0, fmt.Errorf("runtime process %q has an invalid PID", name)
	}
	command, state, err := p.proc(pid)
	if err != nil {
		return 0, "", 0, fmt.Errorf("inspect runtime process %q: %w", name, err)
	}
	return pid, command, state, nil
}

func ownedPIDFile(info os.FileInfo) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Nlink == 1 && int(stat.Uid) == os.Geteuid()
}

func (p *ProcessIsolation) proc(pid int) (string, byte, error) {
	if p.ReadProc != nil {
		return p.ReadProc(pid)
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
	if err != nil {
		return "", 0, err
	}
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "", 0, err
	}
	closeParen := strings.LastIndexByte(string(stat), ')')
	if closeParen < 0 || closeParen+2 >= len(stat) {
		return "", 0, errors.New("process stat has no state")
	}
	return strings.ReplaceAll(string(data), "\x00", " "), stat[closeParen+2], nil
}

func (p *ProcessIsolation) signal(pid int, signal syscall.Signal) error {
	if p.Signal != nil {
		return p.Signal(pid, signal)
	}
	return syscall.Kill(pid, signal)
}

func (p *ProcessIsolation) resume(pids []int) {
	for _, pid := range pids {
		_ = p.signal(pid, syscall.SIGCONT)
	}
}

func validProcessCommand(command, executable, pidFile string) bool {
	parts := strings.Fields(command)
	if len(parts) == 0 || filepath.Base(parts[0]) != executable {
		return false
	}
	return strings.Contains(command, pidFile)
}

var _ ProcessController = (*ProcessIsolation)(nil)
