package fencing

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
