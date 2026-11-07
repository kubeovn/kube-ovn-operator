package health

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"encoding/json/v2"
	"github.com/kubeovn/kube-ovn-operator/internal/fencing"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
)

func TestFenceHTTPClientSendsExactTokenAndCommand(t *testing.T) {
	token := lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "holder", Epoch: 4}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/fence/central-0"+fencing.ExecutePath {
			t.Fatalf("unexpected request: %s %s", request.Method, request.URL.Path)
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		var payload fencing.Request
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Token == nil || *payload.Token != token {
			t.Fatalf("token mismatch: %+v", payload.Token)
		}
		if fmt.Sprint(payload.Command) != "[ovs-appctl --timeout=5 -t /var/run/ovn/ovnnb_db.ctl cluster/leave OVN_Northbound]" {
			t.Fatalf("command mismatch: %v", payload.Command)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"output":"accepted"}`))
	}))
	defer server.Close()

	client, err := NewFenceHTTPClient(server.URL+"/fence/{pod}", nil)
	if err != nil {
		t.Fatal(err)
	}
	output, err := client.RunFenced(t.Context(), "central-0", []string{"ovs-appctl", "--timeout=5", "-t", "/var/run/ovn/ovnnb_db.ctl", "cluster/leave", "OVN_Northbound"}, token)
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != "accepted" {
		t.Fatalf("unexpected output %q", output)
	}
}

func TestFenceHTTPClientMapsStaleFenceAndRejectsIncompleteToken(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		calls++
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusConflict)
		_, _ = writer.Write([]byte(`{"code":"stale_fence","message":"epoch is no longer armed"}`))
	}))
	defer server.Close()
	client, err := NewFenceHTTPClient(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	kick := []string{"ovs-appctl", "--timeout=5", "-t", "/var/run/ovn/ovnnb_db.ctl", "cluster/kick", "OVN_Northbound", "00000000-0000-0000-0000-000000000001"}
	if _, err := client.RunFenced(t.Context(), "central-0", kick, lease.FenceToken{}); err == nil {
		t.Fatal("incomplete token was accepted")
	} else if calls != 0 {
		t.Fatalf("incomplete token reached remote service: %d", calls)
	}
	token := lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "holder", Epoch: 1}
	if _, err := client.RunFenced(t.Context(), "central-0", kick, token); !errors.Is(err, ovnadmin.ErrStaleFence) {
		t.Fatalf("stale fence was not classified: %v", err)
	}
}

func TestFenceHTTPClientRequiresTrustedEndpoint(t *testing.T) {
	if _, err := NewFenceHTTPClient("http://fencer.example.test", nil); err == nil {
		t.Fatal("non-loopback HTTP endpoint was accepted")
	}
	if _, err := NewFenceHTTPClient("https://fencer.example.test/path?token=secret", nil); err == nil {
		t.Fatal("query-bearing fence endpoint was accepted")
	}
}

func TestFenceHTTPClientProbesArmedReadiness(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		if request.Method != http.MethodGet || request.URL.Path != "/fence/default/central-0/ovn-central"+fencing.ReadyPath {
			t.Fatalf("unexpected readiness request: %s %s", request.Method, request.URL.Path)
		}
		_, _ = writer.Write([]byte("ok\n"))
	}))
	defer server.Close()
	client, err := NewFenceHTTPClient(server.URL+"/fence/{namespace}/{pod}/{container}", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ProbeReady(t.Context(), "default", "central-0", "ovn-central"); err != nil {
		t.Fatal(err)
	}
	if requests != 1 {
		t.Fatalf("readiness probe count = %d, want 1", requests)
	}
}

func TestFenceHTTPClientRoutesNodePlaceholderToLiveIP(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/fence/central-0/ovn-central"+fencing.ReadyPath {
			t.Fatalf("unexpected node-routed readiness path: %s", request.URL.Path)
		}
		_, _ = writer.Write([]byte("ok\n"))
	}))
	defer server.Close()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewFenceHTTPClient("https://{node}:"+parsed.Port()+"/fence/{pod}/{container}", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ProbeReadyAt(t.Context(), "default", "central-0", "127.0.0.1", "ovn-central"); err != nil {
		t.Fatalf("node placeholder readiness failed: %v", err)
	}
}

func TestFenceHTTPClientRejectsUnarmedReadiness(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(http.StatusServiceUnavailable)
		_, _ = writer.Write([]byte(`{"code":"fence_not_armed","message":"Lease is not held"}`))
	}))
	defer server.Close()
	client, err := NewFenceHTTPClient(server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.ProbeReady(t.Context(), "default", "central-0", "ovn-central"); !errors.Is(err, ovnadmin.ErrStaleFence) {
		t.Fatalf("unarmed readiness was not classified as stale fence: %v", err)
	}
}

func TestFenceHTTPClientStreamsRestoreWithToken(t *testing.T) {
	token := lease.FenceToken{InstallationUID: "installation", LeaseUID: "lease", Scope: lease.Central, Holder: "holder", Epoch: 4}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/fence/central-0"+fencing.RestorePath || request.Header.Get("X-Kube-OVN-Database") != "OVN_Southbound" {
			t.Fatalf("unexpected restore request: %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("X-Kube-OVN-Installation-UID") != string(token.InstallationUID) || request.Header.Get("X-Kube-OVN-Lease-UID") != string(token.LeaseUID) || request.Header.Get("X-Kube-OVN-Fence-Holder") != token.Holder || request.Header.Get("X-Kube-OVN-Fence-Epoch") != "4" {
			t.Fatal("restore fence headers did not preserve the token")
		}
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "native-backup" {
			t.Fatalf("restore body mismatch: %q", body)
		}
		_, _ = writer.Write([]byte(`{}`))
	}))
	defer server.Close()
	client, err := NewFenceHTTPClient(server.URL+"/fence/{pod}", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.RestoreFenced(t.Context(), "network", "central-0", "ovn-central", "OVN_Southbound", bytes.NewBufferString("native-backup"), token); err != nil {
		t.Fatal(err)
	}
}
