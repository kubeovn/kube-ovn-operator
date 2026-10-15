package central

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
)

func TestCentralChildHelper(t *testing.T) {
	mode := os.Getenv("CENTRAL_CHILD_MODE")
	if mode == "" {
		return
	}
	if mode == "ignore" {
		signal.Ignore(syscall.SIGTERM)
	}
	if err := os.WriteFile(os.Getenv("CENTRAL_CHILD_READY"), []byte("ready"), 0600); err != nil {
		os.Exit(2)
	}
	for {
		if mode == "exit" {
			if _, err := os.Stat(os.Getenv("CENTRAL_CHILD_EXIT")); err == nil {
				os.Exit(0)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitFor(t *testing.T, condition func() error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	for {
		err := condition()
		if err == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestCentralSupervisionStopsAndReapsEveryChild(t *testing.T) {
	for _, scenario := range []string{"cancel", "clean child exit", "partial start"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			dir := t.TempDir()
			var commands []*exec.Cmd
			var markers []string
			for i, mode := range []string{"ignore", "exit", "ignore"} {
				marker := filepath.Join(dir, string(rune('a'+i)))
				cmd := exec.Command(os.Args[0], "-test.run=^TestCentralChildHelper$")
				cmd.Env = append(os.Environ(), "CENTRAL_CHILD_MODE="+mode, "CENTRAL_CHILD_READY="+marker, "CENTRAL_CHILD_EXIT="+filepath.Join(dir, "exit"))
				commands = append(commands, cmd)
				markers = append(markers, marker)
			}
			if scenario == "partial start" {
				commands[1] = exec.Command(filepath.Join(dir, "does-not-exist"))
			}
			done := make(chan error, 1)
			go func() { done <- supervise(ctx, commands, 100*time.Millisecond) }()
			if scenario != "partial start" {
				waitFor(t, func() error {
					for _, marker := range markers {
						if _, err := os.Stat(marker); err != nil {
							return err
						}
					}
					return nil
				})
				if scenario == "cancel" {
					cancel()
				} else if err := os.WriteFile(filepath.Join(dir, "exit"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case err := <-done:
				if err == nil || (scenario == "cancel" && !errors.Is(err, context.Canceled)) {
					t.Fatalf("unexpected supervision result: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("supervisor failed to stop children")
			}
			for _, command := range commands {
				if command.Process != nil && command.ProcessState == nil {
					t.Fatal("supervisor returned without reaping a started process")
				}
			}
		})
	}
}

func TestLaunchAddressesAndTLSFailClosed(t *testing.T) {
	request := Request{FormatVersion: 1, NorthboundAddress: "tcp:192.0.2.4:6641", SouthboundAddress: "tcp:192.0.2.4:6642", Preparation: prepare.Plan{Databases: []prepare.Database{{LocalAddress: "tcp:192.0.2.4:6643"}, {LocalAddress: "tcp:192.0.2.4:6644"}}}}
	request.NorthboundRemotes = []string{request.NorthboundAddress, "tcp:192.0.2.1:6641"}
	request.SouthboundRemotes = []string{request.SouthboundAddress, "tcp:192.0.2.1:6642"}
	if err := request.validate(); err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{"tcp:hostname:6641", "tcp:192.0.2.4:6643", "ssl:192.0.2.4:6641", "ptcp:6641:192.0.2.4", "tcp:[2001:0db8::1]:6641"} {
		changed := request
		changed.NorthboundAddress = address
		if err := changed.validate(); err == nil {
			t.Fatalf("unsafe address accepted: %s", address)
		}
	}
	for address, expected := range map[string]string{"tcp:192.0.2.4:6641": "ptcp:6641:192.0.2.4", "ssl:[2001:db8::4]:6641": "pssl:6641:[2001:db8::4]"} {
		actual, err := passiveAddress(address)
		if err != nil || actual != expected {
			t.Fatalf("passive address: %s %v", actual, err)
		}
	}
	request.TLS = &TLSFiles{PrivateKey: "/missing", Certificate: "/missing", CA: "/missing"}
	if request.validate() == nil {
		t.Fatal("TLS silently accepted for TCP-only launch")
	}
	request.NorthboundAddress = "ssl:192.0.2.4:6641"
	if request.validate() == nil {
		t.Fatal("missing TLS files accepted")
	}
}

func TestRuntimeDirectoryRejectsLinksAndConcurrentLaunchers(t *testing.T) {
	for _, name := range []string{".launch.lock", "ovnnb_db.pid", "ovnsb_db.sock"} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0700); err != nil {
				t.Fatal(err)
			}
			victim := filepath.Join(t.TempDir(), "retained")
			if err := os.WriteFile(victim, []byte("retained"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(victim, filepath.Join(dir, name)); err != nil {
				t.Fatal(err)
			}
			if directory, lock, err := acquireRuntime(dir); err == nil {
				_ = directory.Close()
				_ = lock.Close()
				t.Fatal("runtime symlink accepted")
			}
			if data, err := os.ReadFile(victim); err != nil || string(data) != "retained" {
				t.Fatal("runtime rejection changed an existing file")
			}
		})
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	directory, lock, err := acquireRuntime(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = directory.Close(); _ = lock.Close() }()
	if second, lock2, err := acquireRuntime(dir); err == nil {
		_ = second.Close()
		_ = lock2.Close()
		t.Fatal("second launcher acquired the runtime directory")
	} else if !strings.Contains(err.Error(), "another launcher") {
		t.Fatal(err)
	}
}
