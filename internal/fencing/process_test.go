package fencing

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

func TestProcessIsolationStopsAndResumesPIDFileIdentities(t *testing.T) {
	directory := t.TempDir()
	for name, pid := range map[string]int{"ovnnb_db": 11, "ovnsb_db": 12, "ovn-northd": 13} {
		if err := os.WriteFile(filepath.Join(directory, name+".pid"), []byte(strconv.Itoa(pid)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	states := map[int]byte{11: 'R', 12: 'R', 13: 'R'}
	commands := map[int]string{11: "ovsdb-server --pidfile=ovnnb_db.pid", 12: "ovsdb-server --pidfile=ovnsb_db.pid", 13: "ovn-northd --pidfile=ovn-northd.pid"}
	fence := &ProcessIsolation{RuntimeDirectory: directory,
		ReadProc: func(pid int) (string, byte, error) { return commands[pid], states[pid], nil },
		Signal: func(pid int, signal syscall.Signal) error {
			switch signal {
			case syscall.SIGSTOP:
				states[pid] = 'T'
			case syscall.SIGCONT:
				states[pid] = 'R'
			}
			return nil
		},
	}
	token := lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "holder", Epoch: 1}
	if err := fence.Isolate(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	if err := fence.Check(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	if err := fence.Release(t.Context(), token); err != nil {
		t.Fatal(err)
	}
	if states[11] != 'R' || states[12] != 'R' || states[13] != 'R' {
		t.Fatalf("process states after release = %+v", states)
	}
}

func TestProcessIsolationRejectsChangedToken(t *testing.T) {
	fence := &ProcessIsolation{RuntimeDirectory: t.TempDir()}
	oldToken := lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "old", Epoch: 1}
	newToken := oldToken
	newToken.Holder = "new"
	if err := fence.Check(context.Background(), oldToken); err == nil {
		t.Fatal("unstarted process fence accepted a check")
	}
	if err := fence.Release(context.Background(), newToken); err == nil {
		t.Fatal("unstarted process fence accepted release")
	}
}
