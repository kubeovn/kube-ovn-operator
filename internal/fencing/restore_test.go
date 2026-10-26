package fencing

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

func writeFenceHeaders(request *http.Request, token lease.FenceToken) {
	request.Header.Set("X-Kube-OVN-Installation-UID", string(token.InstallationUID))
	request.Header.Set("X-Kube-OVN-Lease-UID", string(token.LeaseUID))
	request.Header.Set("X-Kube-OVN-Fence-Scope", string(token.Scope))
	request.Header.Set("X-Kube-OVN-Fence-Holder", token.Holder)
	request.Header.Set("X-Kube-OVN-Fence-Epoch", strconv.FormatInt(int64(token.Epoch), 10))
}

func TestHandlerRestoreRequiresCurrentFenceAndStreamsInput(t *testing.T) {
	gate := new(Gate)
	token := testToken(3)
	if err := gate.Arm(token); err != nil {
		t.Fatal(err)
	}
	var got []byte
	handler := &Handler{Gate: gate, Restore: func(_ context.Context, database string, source io.Reader) error {
		if database != "OVN_Northbound" {
			t.Fatalf("unexpected database %q", database)
		}
		var err error
		got, err = io.ReadAll(source)
		return err
	}}
	server := httptest.NewServer(handler)
	defer server.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+RestorePath, bytes.NewReader([]byte("backup")))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Kube-OVN-Database", "OVN_Northbound")
	writeFenceHeaders(request, token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || string(got) != "backup" {
		t.Fatalf("restore was not admitted: status=%d body=%q", response.StatusCode, got)
	}

	stale := token
	stale.Epoch++
	request, err = http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+RestorePath, bytes.NewReader([]byte("stale")))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Kube-OVN-Database", "OVN_Northbound")
	writeFenceHeaders(request, stale)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("stale restore was accepted: status=%d", response.StatusCode)
	}
}

func TestHandlerRestoreRejectsInvalidDatabaseBeforeExecutor(t *testing.T) {
	gate := new(Gate)
	token := testToken(1)
	if err := gate.Arm(token); err != nil {
		t.Fatal(err)
	}
	calls := 0
	handler := &Handler{Gate: gate, Restore: func(context.Context, string, io.Reader) error {
		calls++
		return errors.New("unexpected")
	}}
	server := httptest.NewServer(handler)
	defer server.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+RestorePath, bytes.NewReader([]byte("backup")))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Kube-OVN-Database", "OVN_Unsafe")
	writeFenceHeaders(request, token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest || calls != 0 {
		t.Fatalf("invalid database reached native executor: status=%d calls=%d", response.StatusCode, calls)
	}
}

func TestHandlerReadinessFollowsArmedFence(t *testing.T) {
	gate := new(Gate)
	handler := &Handler{Gate: gate, Execute: func(context.Context, []string) ([]byte, error) {
		return nil, nil
	}, Restore: func(context.Context, string, io.Reader) error {
		return nil
	}}
	server := httptest.NewServer(handler)
	defer server.Close()

	for _, path := range []string{HealthPath, ReadyPath} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		status := response.StatusCode
		_ = response.Body.Close()
		want := http.StatusOK
		if path == ReadyPath {
			want = http.StatusServiceUnavailable
		}
		if status != want {
			t.Fatalf("%s returned status %d, want %d", path, status, want)
		}
	}

	if err := gate.Arm(testToken(1)); err != nil {
		t.Fatal(err)
	}
	response, err := http.Get(server.URL + ReadyPath)
	if err != nil {
		t.Fatal(err)
	}
	status := response.StatusCode
	_ = response.Body.Close()
	if status != http.StatusOK {
		t.Fatalf("armed fence returned readiness status %d", status)
	}
}

func TestHandlerRestoreRejectsKnownOversizedBodyBeforeExecutor(t *testing.T) {
	gate := new(Gate)
	if err := gate.Arm(testToken(1)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	handler := &Handler{Gate: gate, Restore: func(context.Context, string, io.Reader) error {
		calls++
		return nil
	}}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://fence.test"+RestorePath, strings.NewReader("ignored"))
	if err != nil {
		t.Fatal(err)
	}
	request.ContentLength = maxRestoreBytes + 1
	request.Header.Set("X-Kube-OVN-Database", "OVN_Northbound")
	writeFenceHeaders(request, testToken(1))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	status := recorder.Code
	if status != http.StatusRequestEntityTooLarge || calls != 0 {
		t.Fatalf("oversized restore reached executor: status=%d calls=%d", status, calls)
	}
}

func TestHandlerRestoreMapsStreamingSizeErrorToEntityTooLarge(t *testing.T) {
	gate := new(Gate)
	if err := gate.Arm(testToken(1)); err != nil {
		t.Fatal(err)
	}
	handler := &Handler{Gate: gate, Restore: func(context.Context, string, io.Reader) error {
		return &http.MaxBytesError{Limit: maxRestoreBytes}
	}}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://fence.test"+RestorePath, strings.NewReader("backup"))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Kube-OVN-Database", "OVN_Northbound")
	writeFenceHeaders(request, testToken(1))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("streaming size error returned status %d", recorder.Code)
	}
}

func TestHandlerStagesUnknownLengthOversizedBodyBeforeExecutor(t *testing.T) {
	gate := new(Gate)
	if err := gate.Arm(testToken(1)); err != nil {
		t.Fatal(err)
	}
	calls := 0
	handler := &Handler{Gate: gate, restoreLimit: 4, Restore: func(context.Context, string, io.Reader) error {
		calls++
		return nil
	}}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://fence.test"+RestorePath, strings.NewReader("12345"))
	if err != nil {
		t.Fatal(err)
	}
	request.ContentLength = -1
	request.Header.Set("X-Kube-OVN-Database", "OVN_Northbound")
	writeFenceHeaders(request, testToken(1))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge || calls != 0 {
		t.Fatalf("unknown-length oversized restore reached executor: status=%d calls=%d", recorder.Code, calls)
	}
}
