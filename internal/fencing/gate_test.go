package fencing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

func testToken(epoch int32) lease.FenceToken {
	return lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "holder", Epoch: epoch}
}

func TestGateRequiresCurrentArmedToken(t *testing.T) {
	gate := new(Gate)
	if _, err := gate.Execute(t.Context(), testToken(1), func(context.Context) ([]byte, error) { return []byte("unexpected"), nil }); !errors.Is(err, ErrNotArmed) {
		t.Fatalf("unarmed command was accepted: %v", err)
	}
	if err := gate.Arm(testToken(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Execute(t.Context(), testToken(1), func(context.Context) ([]byte, error) { return nil, nil }); !errors.Is(err, ErrStale) {
		t.Fatalf("stale command was accepted: %v", err)
	}
	output, err := gate.Execute(t.Context(), testToken(2), func(context.Context) ([]byte, error) { return []byte("accepted"), nil })
	if err != nil || string(output) != "accepted" {
		t.Fatalf("current command failed: %q %v", output, err)
	}
	gate.Clear()
	if _, armed := gate.Current(); armed {
		t.Fatal("clear retained the active fence")
	}
}

func TestTokenFromLeaseBindsCentralLeaseIdentity(t *testing.T) {
	token, err := TokenFromLease("installation", "lease", "holder", 7)
	if err != nil {
		t.Fatal(err)
	}
	if token.InstallationUID != "installation" || token.LeaseUID != "lease" || token.Scope != lease.Central || token.Holder != "holder" || token.Epoch != 7 {
		t.Fatalf("unexpected token: %+v", token)
	}
	if _, err := TokenFromLease("", "lease", "holder", 7); !errors.Is(err, ErrInvalid) {
		t.Fatalf("incomplete Lease identity was accepted: %v", err)
	}
}

func TestGateSerializesArmWithCommandAdmission(t *testing.T) {
	gate := new(Gate)
	first, second := testToken(1), testToken(2)
	if err := gate.Arm(first); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := gate.Execute(t.Context(), first, func(context.Context) ([]byte, error) {
			close(started)
			<-release
			return nil, nil
		})
		done <- err
	}()
	<-started
	armed := make(chan error, 1)
	go func() { armed <- gate.Arm(second) }()
	select {
	case err := <-armed:
		t.Fatalf("new fence replaced an admitted command before it finished: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-armed; err != nil {
		t.Fatal(err)
	}
	if _, ok := gate.Current(); !ok {
		t.Fatal("replacement fence was not retained")
	}
}

func TestHandlerUsesGateAndReturnsStaleFenceCode(t *testing.T) {
	gate := new(Gate)
	token := testToken(1)
	if err := gate.Arm(token); err != nil {
		t.Fatal(err)
	}
	handler := &Handler{Gate: gate, Execute: func(_ context.Context, command []string) ([]byte, error) {
		return []byte(command[0]), nil
	}}
	server := httptest.NewServer(handler)
	defer server.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+ExecutePath, strings.NewReader(`{"command":["ovs-appctl","--timeout=5","-t","/var/run/ovn/ovnnb_db.ctl","cluster/leave","OVN_Northbound"],"token":{"installationUID":"installation","leaseUID":"lease","scope":"central","holder":"old","epoch":1}}`))
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("unexpected status: %d", response.StatusCode)
	}
}

type processControllerFake struct{ actions []string }

func (f *processControllerFake) Isolate(context.Context, lease.FenceToken) error {
	f.actions = append(f.actions, "isolate")
	return nil
}

func (f *processControllerFake) Check(context.Context, lease.FenceToken) error {
	f.actions = append(f.actions, "check")
	return nil
}

func (f *processControllerFake) Release(context.Context, lease.FenceToken) error {
	f.actions = append(f.actions, "release")
	return nil
}

func TestHandlerProcessFenceRequiresCurrentLeaseToken(t *testing.T) {
	gate := new(Gate)
	current := testToken(2)
	if err := gate.Arm(current); err != nil {
		t.Fatal(err)
	}
	process := new(processControllerFake)
	handler := &Handler{Gate: gate, Isolation: process}
	server := httptest.NewServer(handler)
	defer server.Close()
	for _, test := range []struct {
		path   string
		token  lease.FenceToken
		status int
	}{
		{path: IsolationPath, token: current, status: http.StatusOK},
		{path: CheckPath, token: current, status: http.StatusOK},
		{path: ReleasePath, token: current, status: http.StatusOK},
		{path: IsolationPath, token: testToken(1), status: http.StatusConflict},
	} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+test.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("X-Kube-OVN-Installation-UID", string(test.token.InstallationUID))
		request.Header.Set("X-Kube-OVN-Lease-UID", string(test.token.LeaseUID))
		request.Header.Set("X-Kube-OVN-Fence-Scope", string(test.token.Scope))
		request.Header.Set("X-Kube-OVN-Fence-Holder", test.token.Holder)
		request.Header.Set("X-Kube-OVN-Fence-Epoch", fmt.Sprint(test.token.Epoch))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != test.status {
			t.Fatalf("%s status = %d, want %d", test.path, response.StatusCode, test.status)
		}
	}
	if !slices.Equal(process.actions, []string{"isolate", "check", "release"}) {
		t.Fatalf("process actions = %v", process.actions)
	}
}
