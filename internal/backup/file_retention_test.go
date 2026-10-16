package backup

import (
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestFileStoreListsAndDeletesCompletePairs(t *testing.T) {
	directory := t.TempDir()
	store, err := OpenFileStore(directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	now := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	refs := make([]string, 0, 2)
	for _, completed := range []time.Time{now.Add(-time.Hour), now.Add(-2 * time.Hour)} {
		ref, err := store.Publish(t.Context(), func(writer io.Writer) (Record, error) {
			_, writeErr := writer.Write([]byte("snapshot"))
			return Record{FormatVersion: 1, CompletedAt: completed}, writeErr
		})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	if err := os.Mkdir(filepath.Join(directory, ".pending-manual"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(directory, "unknown"), 0700); err != nil {
		t.Fatal(err)
	}
	objects, err := store.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	listed := make([]string, 0, len(objects))
	for _, object := range objects {
		listed = append(listed, object.Reference)
	}
	slices.Sort(listed)
	slices.Sort(refs)
	if !slices.Equal(listed, refs) {
		t.Fatalf("listed references = %v, want %v", listed, refs)
	}
	if err := store.Delete(t.Context(), refs[0]); err != nil {
		t.Fatal(err)
	}
	objects, err = store.List(t.Context())
	if err != nil || len(objects) != 1 || objects[0].Reference != refs[1] {
		t.Fatalf("after deletion objects=%+v err=%v", objects, err)
	}
	if err := store.Delete(t.Context(), "sha256:"+"a"+"/../snapshot.db"); err == nil {
		t.Fatal("accepted traversal reference")
	}
}
