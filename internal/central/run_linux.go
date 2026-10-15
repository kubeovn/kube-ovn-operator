package central

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
)

type Options struct {
	Root             string
	RuntimeDirectory string
	OVSDBTool        string
	OVSDBServer      string
	Northd           string
	ShutdownTimeout  time.Duration
	Stdout           *os.File
	Stderr           *os.File
}

// Run holds both private directories for the lifetime of all foreground children.
// Any child exit is a failure, even exit(0). Cancellation sends SIGTERM to all
// children and escalates to SIGKILL after ShutdownTimeout (default 10 seconds).
// Return follows reaping every child. No automatic restart or recovery occurs.
// Running processes are not readiness evidence: the caller must observe both
// databases and northd before advancing the installation's lifecycle.
func Run(ctx context.Context, options Options, request Request) error {
	if err := request.validate(); err != nil {
		return err
	}
	for _, binary := range []string{options.OVSDBTool, options.OVSDBServer, options.Northd} {
		if !filepath.IsAbs(binary) {
			return errors.New("native tool paths must be absolute")
		}
		info, err := os.Stat(binary)
		if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
			return errors.New("native tools must already exist and be executable")
		}
	}
	if options.ShutdownTimeout == 0 {
		options.ShutdownTimeout = 10 * time.Second
	}
	if options.ShutdownTimeout < 0 || options.ShutdownTimeout > 30*time.Second {
		return errors.New("shutdown timeout must be positive and at most 30 seconds")
	}
	preparer, err := prepare.Open(options.Root, options.OVSDBTool)
	if err != nil {
		return err
	}
	defer func() { _ = preparer.Close() }()
	files, err := preparer.Acquire(ctx, request.Preparation, request.Prepared)
	if err != nil {
		return err
	}
	defer func() { _ = files.Close() }()
	rundir, lock, err := acquireRuntime(options.RuntimeDirectory)
	if err != nil {
		return err
	}
	defer func() { _ = rundir.Close(); _ = lock.Close() }()
	var commands []*exec.Cmd
	for i, database := range []string{"OVN_Northbound", "OVN_Southbound"} {
		path, err := files.DatabasePath(database)
		if err != nil {
			return err
		}
		command := files.Command(options.OVSDBServer)
		runpath := attachRuntime(command, rundir, lock)
		name := []string{"ovnnb_db", "ovnsb_db"}[i]
		remote, err := passiveAddress([]string{request.NorthboundAddress, request.SouthboundAddress}[i])
		if err != nil {
			return err
		}
		command.Args = append(command.Args, "--no-chdir", "--pidfile="+runpath+name+".pid", "--unixctl="+runpath+name+".ctl", "--remote=punix:"+runpath+name+".sock", "--remote="+remote)
		if request.TLS != nil {
			command.Args = append(command.Args, "--private-key="+request.TLS.PrivateKey, "--certificate="+request.TLS.Certificate, "--ca-cert="+request.TLS.CA)
		}
		command.Args = append(command.Args, path)
		commands = append(commands, command)
	}
	command := files.Command(options.Northd)
	runpath := attachRuntime(command, rundir, lock)
	// Use northd's default PID-specific control socket, so ovn-appctl -t
	// ovn-northd continues to work with the standard runtime directory.
	command.Args = append(command.Args, "--no-chdir", "--pidfile="+runpath+"ovn-northd.pid")
	for _, database := range request.Preparation.Databases {
		flag, remotes := "--ovnnb-db=", request.NorthboundRemotes
		if database.Name == "OVN_Southbound" {
			flag, remotes = "--ovnsb-db=", request.SouthboundRemotes
		}
		// Northd's IDL requires the leaders, which need not run on this member
		// or even on the same host for NB/SB. Pin each cluster UUID explicitly.
		// Older native parsers remove cid through svec_del before sorting, so
		// their input must already be sorted to avoid an assertion failure.
		remotes = slices.Clone(remotes)
		slices.Sort(remotes)
		command.Args = append(command.Args, flag+"cid:"+database.ClusterID+","+strings.Join(remotes, ","))
	}
	if request.TLS != nil {
		command.Args = append(command.Args, "--private-key="+request.TLS.PrivateKey, "--certificate="+request.TLS.Certificate, "--ca-cert="+request.TLS.CA)
	}
	commands = append(commands, command)
	for _, cmd := range commands {
		cmd.Stdout, cmd.Stderr = options.Stdout, options.Stderr
		cmd.WaitDelay = time.Second
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	}
	return supervise(ctx, commands, options.ShutdownTimeout)
}

func attachRuntime(cmd *exec.Cmd, directory, lock *os.File) string {
	path := fmt.Sprintf("/proc/self/fd/%d/", 3+len(cmd.ExtraFiles))
	cmd.ExtraFiles = append(cmd.ExtraFiles, directory, lock)
	cmd.Env = append(os.Environ(), "OVN_RUNDIR="+path, "OVS_RUNDIR="+path)
	return path
}

func acquireRuntime(path string) (*os.File, *os.File, error) {
	if !filepath.IsAbs(path) {
		return nil, nil, errors.New("runtime directory must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return nil, nil, errors.New("runtime directory must already exist, be private and not a symlink")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Geteuid() {
		return nil, nil, errors.New("runtime directory belongs to another OS user")
	}
	root, err := os.OpenRoot(path)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = root.Close() }()
	lock, err := root.OpenFile(".launch.lock", os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0600)
	if err != nil {
		return nil, nil, err
	}
	success := false
	defer func() {
		if !success {
			_ = lock.Close()
		}
	}()
	lockInfo, err := lock.Stat()
	if err != nil || !ownedRegular(lockInfo) {
		return nil, nil, errors.New("runtime lock must be an owned regular file with one link")
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, nil, errors.New("another launcher holds the runtime directory")
	}
	for _, name := range []string{"ovnnb_db.pid", "ovnsb_db.pid", "ovn-northd.pid", "ovnnb_db.sock", "ovnsb_db.sock", "ovnnb_db.ctl", "ovnsb_db.ctl"} {
		entry, err := root.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || (filepath.Ext(name) == ".pid" && !ownedRegular(entry)) || (filepath.Ext(name) != ".pid" && !ownedSocket(entry)) {
			return nil, nil, errors.New("runtime entries must be owned regular files or sockets without links")
		}
	}
	directory, err := root.Open(".")
	if err != nil {
		return nil, nil, err
	}
	opened, err := directory.Stat()
	if err != nil || !os.SameFile(info, opened) {
		_ = directory.Close()
		return nil, nil, errors.New("runtime directory changed while opening")
	}
	success = true
	return directory, lock, nil
}

func ownedRegular(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular() && ownedSingleLink(info)
}

func ownedSocket(info os.FileInfo) bool {
	return info != nil && info.Mode()&os.ModeSocket != 0 && ownedSingleLink(info)
}

func ownedSingleLink(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Geteuid() && stat.Nlink == 1
}

func supervise(ctx context.Context, commands []*exec.Cmd, grace time.Duration) error {
	// Linux's parent-death signal belongs to the thread that creates a child.
	// Keep that thread alive until every child is reaped; otherwise Go retiring
	// an idle OS thread could kill an otherwise healthy database process.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	type exit struct {
		command *exec.Cmd
		err     error
	}
	exits := make(chan exit, len(commands))
	active := make(map[*exec.Cmd]bool)
	defer func() {
		for command := range active {
			_ = command.Process.Signal(syscall.SIGTERM)
		}
		timer := time.NewTimer(grace)
		defer timer.Stop()
		for len(active) > 0 {
			select {
			case stopped := <-exits:
				delete(active, stopped.command)
			case <-timer.C:
				for command := range active {
					_ = command.Process.Kill()
				}
			}
		}
	}()
	for _, command := range commands {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := command.Start(); err != nil {
			return fmt.Errorf("start %s: %w", filepath.Base(command.Path), err)
		}
		active[command] = true
		go func() { exits <- exit{command, command.Wait()} }()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case stopped := <-exits:
		delete(active, stopped.command)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("%s exited unexpectedly: %w", filepath.Base(stopped.command.Path), errors.Join(errors.New("central process stopped"), stopped.err))
	}
}
