package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
)

func TestS3StorePublishesAndOpensImmutableObjects(t *testing.T) {
	var (
		mu      sync.Mutex
		objects = map[string][]byte{}
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") == "" || !strings.HasPrefix(request.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			t.Error("S3 request was not signed")
		}
		if request.Header.Get("X-Amz-Date") != "20261016T170000Z" {
			t.Errorf("unexpected signing time %q", request.Header.Get("X-Amz-Date"))
		}
		key := request.URL.Path
		mu.Lock()
		defer mu.Unlock()
		switch request.Method {
		case http.MethodPut:
			data, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
				return
			}
			payloadHash := sha256.Sum256(data)
			if got, want := request.Header.Get("X-Amz-Content-Sha256"), fmt.Sprintf("%x", payloadHash); got != want {
				t.Errorf("payload hash = %q, want %q", got, want)
			}
			objects[key] = data
			writer.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, ok := objects[key]
			if !ok {
				http.NotFound(writer, request)
				return
			}
			_, _ = writer.Write(data)
		default:
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store, err := NewS3Store("s3://test-bucket/operator", Credentials{AccessKeyID: "access", SecretAccessKey: "secret", Region: "us-east-1", Endpoint: server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	store.clock = func() time.Time { return time.Date(2026, 10, 16, 17, 0, 0, 0, time.UTC) }
	data := []byte("native ovsdb backup")
	digest := sha256.Sum256(data)
	want := Record{FormatVersion: 1, Identity: Identity{InstallationUID: "installation", PodUID: "pod", ContainerID: "container"}, Endpoint: "central-0", Snapshot: ovnadmin.Snapshot{Database: "OVN_Northbound", ClusterID: "11111111-2222-3333-4444-555555555555", LeaderServerID: "22222222-3333-4444-5555-666666666666", Term: 3, CommitIndex: 4, AppliedIndex: 4}, CompletedAt: time.Date(2026, 10, 16, 17, 0, 0, 0, time.UTC), SchemaVersion: "v1", Size: int64(len(data)), SHA256: fmt.Sprintf("%x", digest[:])}
	ref, err := store.Publish(context.Background(), func(writer io.Writer) (Record, error) {
		if _, err := writer.Write(data); err != nil {
			return Record{}, err
		}
		return want, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(ref, "sha256:") {
		t.Fatalf("unexpected reference %q", ref)
	}
	got, reader, err := store.Open(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	readData, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	snapshotMatches := got.Snapshot.Database == want.Snapshot.Database && got.Snapshot.ClusterID == want.Snapshot.ClusterID && got.Snapshot.LeaderServerID == want.Snapshot.LeaderServerID && got.Snapshot.Term == want.Snapshot.Term && got.Snapshot.CommitIndex == want.Snapshot.CommitIndex && got.Snapshot.AppliedIndex == want.Snapshot.AppliedIndex && slices.Equal(got.Snapshot.Members, want.Snapshot.Members)
	if !bytes.Equal(readData, data) || got.SHA256 != want.SHA256 || !snapshotMatches {
		t.Fatalf("round trip changed backup: got=%+v data=%q", got, readData)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(objects) != 2 || objects[path.Join("/test-bucket/operator", strings.TrimPrefix(ref, "sha256:"), "snapshot.db")] == nil {
		t.Fatalf("unexpected object set: %v", objects)
	}
}

func TestS3StoreRejectsManifestTampering(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodGet && strings.HasSuffix(request.URL.Path, "record.json") {
			_, _ = writer.Write([]byte(`{"formatVersion":1}`))
			return
		}
		http.NotFound(writer, request)
	}))
	defer server.Close()
	store, err := NewS3Store("s3://test-bucket/operator", Credentials{AccessKeyID: "access", SecretAccessKey: "secret", Endpoint: server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Open(context.Background(), "sha256:"+strings.Repeat("a", 64))
	if err == nil || !strings.Contains(err.Error(), "manifest does not match") {
		t.Fatalf("tampered manifest was accepted: %v", err)
	}
}

func TestS3StoreRequiresCredentials(t *testing.T) {
	if _, err := NewS3Store("s3://test-bucket/operator", Credentials{}, nil); err == nil {
		t.Fatal("S3 store accepted empty credentials")
	}
}

func TestS3StoreRejectsEndpointCredentials(t *testing.T) {
	_, err := NewS3Store("s3://test-bucket/operator", Credentials{
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
		Endpoint:        "https://user:secret@example.test",
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "without credentials") {
		t.Fatalf("endpoint credentials were accepted: %v", err)
	}
}

func TestS3StoreUsesPathStyleForDottedAWSBucket(t *testing.T) {
	store, err := NewS3Store("s3://bucket.name/operator", Credentials{
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
		Region:          "us-east-1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := store.objectURL("operator/key").String(), "https://s3.amazonaws.com/bucket.name/operator/key"; got != want {
		t.Fatalf("dotted bucket URL = %q, want %q", got, want)
	}
}

func TestS3StorePreservesObjectPrefixSeparators(t *testing.T) {
	store, err := NewS3Store("s3://test-bucket/a//b", Credentials{
		AccessKeyID:     "access",
		SecretAccessKey: "secret",
		Endpoint:        "https://objects.example.test/gateway",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := store.objectURL(store.objectKey("digest", "snapshot.db")).EscapedPath(), "/gateway/test-bucket/a//b/digest/snapshot.db"; got != want {
		t.Fatalf("object path = %q, want %q", got, want)
	}
}

func TestS3StoreProviderRejectsTamperedSnapshot(t *testing.T) {
	var (
		mu      sync.Mutex
		objects = map[string][]byte{}
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		key := request.URL.Path
		mu.Lock()
		defer mu.Unlock()
		switch request.Method {
		case http.MethodPut:
			data, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(writer, "read failed", http.StatusInternalServerError)
				return
			}
			objects[key] = data
			writer.WriteHeader(http.StatusOK)
		case http.MethodGet:
			data, ok := objects[key]
			if !ok {
				http.NotFound(writer, request)
				return
			}
			_, _ = writer.Write(data)
		default:
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	store, err := NewS3Store("s3://test-bucket/operator", Credentials{AccessKeyID: "access", SecretAccessKey: "secret", Endpoint: server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	source := &testSource{
		data:     nativeBackup(testSchema, testRows),
		identity: Identity{InstallationUID: "installation", PodUID: "pod", ContainerID: "containerd://process"},
		snapshot: testSnapshot(),
	}
	provider := &Provider{Source: source, Store: store, InstallationUID: "installation", MaxBytes: 1 << 20, MaxAge: time.Hour}
	err = withLeases(t, func(ctx context.Context) error {
		ref, captureErr := provider.Capture(ctx, "OVN_Northbound", "central-0")
		if captureErr != nil {
			return captureErr
		}
		mu.Lock()
		for key := range objects {
			if strings.HasSuffix(key, "/snapshot.db") {
				objects[key] = nativeBackup(testSchema, strings.ReplaceAll(testRows, "saved", "tampered"))
			}
		}
		mu.Unlock()
		if err := provider.Verify(ctx, source.snapshot, ref); err == nil {
			return errors.New("accepted tampered S3 snapshot")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestS3StoreAllowsRetryAfterPartialPublication(t *testing.T) {
	var (
		mu       sync.Mutex
		putCount int
		failNext bool
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut {
			_, _ = writer.Write([]byte("unexpected method"))
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		_, _ = io.Copy(io.Discard, request.Body)
		mu.Lock()
		putCount++
		fail := failNext && putCount == 2
		mu.Unlock()
		if fail {
			http.Error(writer, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store, err := NewS3Store("s3://test-bucket/operator", Credentials{AccessKeyID: "access", SecretAccessKey: "secret", Endpoint: server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("retryable native backup")
	record := Record{Size: int64(len(data)), SHA256: fmt.Sprintf("%x", sha256.Sum256(data))}
	mu.Lock()
	failNext = true
	mu.Unlock()
	ref, err := store.Publish(t.Context(), func(writer io.Writer) (Record, error) {
		_, writeErr := writer.Write(data)
		return record, writeErr
	})
	if err == nil || ref != "" {
		t.Fatalf("partial publication returned ref=%q err=%v", ref, err)
	}
	mu.Lock()
	failNext = false
	mu.Unlock()
	ref, err = store.Publish(t.Context(), func(writer io.Writer) (Record, error) {
		_, writeErr := writer.Write(data)
		return record, writeErr
	})
	if err != nil || !strings.HasPrefix(ref, "sha256:") {
		t.Fatalf("retry failed: ref=%q err=%v", ref, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if putCount != 4 {
		t.Fatalf("PUT count = %d, want 4 (partial pair plus successful retry)", putCount)
	}
}

func TestS3StoreRetriesDeletionAfterPartialPairFailure(t *testing.T) {
	var (
		mu           sync.Mutex
		storage      = map[string][]byte{}
		deleteCalls  int
		failDeleteAt = 2
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch request.Method {
		case http.MethodPut:
			data, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(writer, "read failed", http.StatusInternalServerError)
				return
			}
			storage[request.URL.Path] = data
		case http.MethodGet:
			data, ok := storage[request.URL.Path]
			if !ok {
				http.NotFound(writer, request)
				return
			}
			_, _ = writer.Write(data)
		case http.MethodDelete:
			deleteCalls++
			if deleteCalls == failDeleteAt {
				http.Error(writer, "temporary failure", http.StatusServiceUnavailable)
				return
			}
			if _, ok := storage[request.URL.Path]; !ok {
				http.NotFound(writer, request)
				return
			}
			delete(storage, request.URL.Path)
		default:
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, err := NewS3Store("s3://test-bucket/operator", Credentials{AccessKeyID: "access", SecretAccessKey: "secret", Endpoint: server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	ref, err := store.Publish(t.Context(), func(writer io.Writer) (Record, error) {
		_, writeErr := writer.Write([]byte("snapshot"))
		return Record{FormatVersion: 1, CompletedAt: time.Unix(100, 0).UTC()}, writeErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(t.Context(), ref); err == nil {
		t.Fatal("partial S3 deletion unexpectedly succeeded")
	}
	mu.Lock()
	remaining := len(storage)
	mu.Unlock()
	if remaining != 1 {
		t.Fatalf("partial deletion left %d objects, want 1", remaining)
	}
	if err := store.Delete(t.Context(), ref); err != nil {
		t.Fatalf("retry after partial S3 deletion failed: %v", err)
	}
	mu.Lock()
	remaining = len(storage)
	mu.Unlock()
	if remaining != 0 {
		t.Fatalf("retry left %d objects, want 0", remaining)
	}
}

func TestS3StoreListsCompletePairsAndDeletesBothObjects(t *testing.T) {
	var (
		mu      sync.Mutex
		storage = map[string][]byte{}
	)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if request.Method == http.MethodGet && request.URL.Query().Get("list-type") == "2" {
			prefix := request.URL.Query().Get("prefix")
			var keys []string
			for key := range storage {
				key = strings.TrimPrefix(key, "/test-bucket/")
				if strings.HasPrefix(key, prefix) {
					keys = append(keys, key)
				}
			}
			slices.Sort(keys)
			writer.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(writer, "<ListBucketResult>")
			for _, key := range keys {
				_, _ = fmt.Fprintf(writer, "<Contents><Key>%s</Key></Contents>", key)
			}
			_, _ = io.WriteString(writer, "<IsTruncated>false</IsTruncated></ListBucketResult>")
			return
		}
		switch request.Method {
		case http.MethodPut:
			data, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(writer, "read failed", http.StatusInternalServerError)
				return
			}
			storage[request.URL.Path] = data
		case http.MethodGet:
			data, ok := storage[request.URL.Path]
			if !ok {
				http.NotFound(writer, request)
				return
			}
			_, _ = writer.Write(data)
		case http.MethodDelete:
			delete(storage, request.URL.Path)
		default:
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	store, err := NewS3Store("s3://test-bucket/operator", Credentials{AccessKeyID: "access", SecretAccessKey: "secret", Endpoint: server.URL}, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	for _, completed := range []time.Time{time.Date(2026, 10, 16, 17, 0, 0, 0, time.UTC), time.Date(2026, 10, 16, 16, 0, 0, 0, time.UTC)} {
		if _, err := store.Publish(t.Context(), func(writer io.Writer) (Record, error) {
			_, writeErr := writer.Write([]byte("snapshot"))
			return Record{FormatVersion: 1, CompletedAt: completed}, writeErr
		}); err != nil {
			t.Fatal(err)
		}
	}
	listed, err := store.List(t.Context())
	if err != nil || len(listed) != 2 {
		t.Fatalf("listed objects=%+v err=%v", listed, err)
	}
	if err := store.Delete(t.Context(), listed[0].Reference); err != nil {
		t.Fatal(err)
	}
	listed, err = store.List(t.Context())
	if err != nil || len(listed) != 1 {
		t.Fatalf("after deletion objects=%+v err=%v", listed, err)
	}
}
