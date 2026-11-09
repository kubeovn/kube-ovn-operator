package recovery

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

type fenceSessionFake struct {
	actions       []string
	err           error
	isolateCalls  int
	failIsolateAt int
}

func (f *fenceSessionFake) Isolate(_ context.Context, member Member, token lease.FenceToken) error {
	f.isolateCalls++
	f.actions = append(f.actions, "isolate:"+member.Name+":"+fmt.Sprint(token.Epoch))
	if f.failIsolateAt > 0 && f.isolateCalls == f.failIsolateAt {
		return errors.New("isolation unavailable")
	}
	return f.err
}

func (f *fenceSessionFake) Check(_ context.Context, member Member, token lease.FenceToken) error {
	f.actions = append(f.actions, "check:"+member.Name+":"+fmt.Sprint(token.Epoch))
	return f.err
}

func (f *fenceSessionFake) Release(_ context.Context, member Member, token lease.FenceToken) error {
	f.actions = append(f.actions, "release:"+member.Name+":"+fmt.Sprint(token.Epoch))
	return f.err
}

type storageSessionFake struct {
	calls int
	err   error
}

func (s *storageSessionFake) Verify(context.Context, Member, string) error {
	s.calls++
	return s.err
}

func TestFencedIsolationSessionBindsLeaseAndReleasesInReverse(t *testing.T) {
	plan := validPlan()
	fence := new(fenceSessionFake)
	storage := new(storageSessionFake)
	token := lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "holder", Epoch: 4}
	session := &FencedIsolationSession{Token: func(context.Context) (lease.FenceToken, error) { return token, nil }, Fence: fence, Storage: storage}
	guard, err := session.Begin(t.Context(), plan, plan.TargetMembers[0], "/target")
	if err != nil {
		t.Fatal(err)
	}
	if err := guard.Check(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := guard.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	want := []string{"isolate:old:4", "check:old:4", "check:old:4", "release:old:4"}
	if !slices.Equal(fence.actions, want) {
		t.Fatalf("fence actions = %v, want %v", fence.actions, want)
	}
	if storage.calls != 3 {
		t.Fatalf("storage verification calls = %d, want 3", storage.calls)
	}
}

func TestFencedIsolationSessionRejectsMissingLiveCapabilities(t *testing.T) {
	plan := validPlan()
	for name, session := range map[string]*FencedIsolationSession{
		"token":   {Fence: new(fenceSessionFake), Storage: new(storageSessionFake)},
		"fence":   {Token: func(context.Context) (lease.FenceToken, error) { return lease.FenceToken{}, nil }, Storage: new(storageSessionFake)},
		"storage": {Token: func(context.Context) (lease.FenceToken, error) { return lease.FenceToken{}, nil }, Fence: new(fenceSessionFake)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := session.Begin(t.Context(), plan, plan.TargetMembers[0], "/target"); err == nil {
				t.Fatal("incomplete isolation provider was accepted")
			}
		})
	}
}

func TestFencedIsolationSessionRejectsLeaseTakeoverAndDoesNotReleaseWithOldToken(t *testing.T) {
	plan := validPlan()
	fence := new(fenceSessionFake)
	storage := new(storageSessionFake)
	oldToken := lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "old", Epoch: 1}
	newToken := oldToken
	newToken.Holder, newToken.Epoch = "new", 2
	current := oldToken
	session := &FencedIsolationSession{Token: func(context.Context) (lease.FenceToken, error) { return current, nil }, Fence: fence, Storage: storage}
	guard, err := session.Begin(t.Context(), plan, plan.TargetMembers[0], "/target")
	if err != nil {
		t.Fatal(err)
	}
	current = newToken
	if err := guard.Check(t.Context()); !errors.Is(err, ErrIsolationTokenChanged) {
		t.Fatalf("Lease takeover was not rejected: %v", err)
	}
	if err := guard.Close(t.Context()); !errors.Is(err, ErrIsolationTokenChanged) {
		t.Fatalf("old holder released after takeover: %v", err)
	}
	if slices.Contains(fence.actions, "release:old:1") {
		t.Fatalf("old token was used to release fencing: %v", fence.actions)
	}
}

func TestFencedIsolationSessionRollsBackPartialBegin(t *testing.T) {
	plan := validPlan()
	plan.RetainedMembers = append(plan.RetainedMembers, member("old-2", "2", "b", "c", "d", "e"))
	fence := new(fenceSessionFake)
	storage := new(storageSessionFake)
	fence.failIsolateAt = 2
	token := lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "holder", Epoch: 1}
	session := &FencedIsolationSession{Token: func(context.Context) (lease.FenceToken, error) { return token, nil }, Fence: fence, Storage: storage}
	if _, err := session.Begin(t.Context(), plan, plan.TargetMembers[0], "/target"); err == nil {
		t.Fatal("partial isolation failure was accepted")
	}
	if !slices.Contains(fence.actions, "release:old:1") {
		t.Fatalf("partial isolation was not rolled back: %v", fence.actions)
	}
}
