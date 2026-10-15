package health

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/apimachinery/pkg/util/httpstream/spdy"
	remotecommandconsts "k8s.io/apimachinery/pkg/util/remotecommand"
	"k8s.io/client-go/rest"
)

func TestPodExecutorTargetsExplicitContainerWithoutShell(t *testing.T) {
	command, err := DatabaseCommand("OVN_Northbound")
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/namespaces/kube-system/pods/central-0/exec" || r.URL.Query().Get("container") != "ovn-central" || !slices.Equal(r.URL.Query()["command"], command) {
			t.Errorf("wrong execution target: %s %s", r.Method, r.URL)
		}
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer server.Close()
	executor, err := NewPodExecutor(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Run(t.Context(), "kube-system", "central-0", "ovn-central", command); err == nil || calls.Load() != 1 {
		t.Fatalf("expected forbidden execution, got %v after %d calls", err, calls.Load())
	}
}

func TestPodExecutorSPDYSuccessLimitAndCancellation(t *testing.T) {
	for _, scenario := range []string{"success", "oversized", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			payload := []byte(`[{"rows":[]}]`)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := httpstream.Handshake(r, w, []string{remotecommandconsts.StreamProtocolV4Name}); err != nil {
					return
				}
				streams := make(chan httpstream.Stream, 3)
				conn := spdy.NewResponseUpgrader().UpgradeResponse(w, r, func(stream httpstream.Stream, _ <-chan struct{}) error {
					streams <- stream
					return nil
				})
				if conn == nil {
					return
				}
				defer func() { _ = conn.Close() }()
				byType := make(map[string]httpstream.Stream)
				for range 3 {
					select {
					case stream := <-streams:
						byType[stream.Headers().Get(corev1.StreamType)] = stream
					case <-ctx.Done():
						return
					}
				}
				if scenario == "cancelled" {
					cancel()
					select {
					case <-conn.CloseChan():
					case <-time.After(3 * time.Second):
					}
					return
				}
				stdout := byType[corev1.StreamTypeStdout]
				_, _ = stdout.Write(payload)
				if scenario == "oversized" {
					_, _ = stdout.Write(bytes.Repeat([]byte(" "), 65536))
				}
				_ = stdout.Close()
				_ = byType[corev1.StreamTypeStderr].Close()
				_, _ = byType[corev1.StreamTypeError].Write([]byte(`{"status":"Success"}`))
				_ = byType[corev1.StreamTypeError].Close()
			}))
			defer server.Close()
			executor, err := NewPodExecutor(&rest.Config{Host: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			got, err := executor.Run(ctx, "default", "central", "ovn-central", []string{"ovsdb-client", "query"})
			switch scenario {
			case "success":
				if err != nil || !bytes.Equal(got, payload) {
					t.Fatalf("got %q, %v", got, err)
				}
			case "oversized":
				if err == nil || !strings.Contains(err.Error(), "64 KiB") {
					t.Fatalf("truncated output accepted: %v", err)
				}
			case "cancelled":
				if err == nil {
					t.Fatal("cancelled stream succeeded")
				}
			}
		})
	}
}

func TestPodOutputLimitCannotBeBypassedByIOCopy(t *testing.T) {
	output := new(boundedBuffer)
	if _, err := io.Copy(output, bytes.NewReader(make([]byte, 65537))); err == nil {
		t.Fatal("accepted unbounded output")
	}
	if len(output.Bytes()) > 65536 {
		t.Fatal("output exceeded limit")
	}
}
