package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"
)

type retentionStoreFake struct {
	objects []Object
	deleted []string
	err     error
}

func (s *retentionStoreFake) Publish(_ context.Context, _ func(io.Writer) (Record, error)) (string, error) {
	panic("unused")
}
func (s *retentionStoreFake) Open(context.Context, string) (Record, io.ReadCloser, error) {
	panic("unused")
}
func (s *retentionStoreFake) List(context.Context) ([]Object, error) {
	return slices.Clone(s.objects), s.err
}
func (s *retentionStoreFake) Delete(_ context.Context, reference string) error {
	if s.err != nil {
		return s.err
	}
	s.deleted = append(s.deleted, reference)
	return nil
}

func TestParseSchedule(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{name: "disabled", raw: "", want: 0},
		{name: "valid", raw: "6h", want: 6 * time.Hour},
		{name: "too short", raw: "59s", wantErr: true},
		{name: "too long", raw: "744h", wantErr: true},
		{name: "invalid", raw: "hourly", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseSchedule(test.raw)
			if test.wantErr {
				if err == nil {
					t.Fatalf("ParseSchedule(%q) succeeded", test.raw)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("ParseSchedule(%q) = %s, %v; want %s", test.raw, got, err, test.want)
			}
		})
	}
}

func TestParseRetention(t *testing.T) {
	for _, test := range []struct {
		name    string
		keep    int32
		age     string
		want    RetentionPolicy
		wantErr bool
	}{
		{name: "disabled"},
		{name: "count", keep: 3, want: RetentionPolicy{KeepLast: 3}},
		{name: "age", age: "24h", want: RetentionPolicy{MaxAge: 24 * time.Hour}},
		{name: "negative count", keep: -1, wantErr: true},
		{name: "invalid age", age: "day", wantErr: true},
		{name: "zero age", age: "0s", wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseRetention(test.keep, test.age)
			if test.wantErr {
				if err == nil {
					t.Fatal("ParseRetention unexpectedly succeeded")
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("ParseRetention = %+v, %v; want %+v", got, err, test.want)
			}
		})
	}
}

func TestCleanupKeepsProtectedAndRecentPairs(t *testing.T) {
	now := time.Date(2026, 10, 16, 18, 0, 0, 0, time.UTC)
	store := &retentionStoreFake{objects: []Object{
		{Reference: "old", Record: Record{CompletedAt: now.Add(-72 * time.Hour)}},
		{Reference: "middle", Record: Record{CompletedAt: now.Add(-48 * time.Hour)}},
		{Reference: "recent", Record: Record{CompletedAt: now.Add(-time.Hour)}},
	}}
	result, err := Cleanup(t.Context(), store, RetentionPolicy{KeepLast: 1, MaxAge: 24 * time.Hour}, []string{"old"}, now)
	if err != nil {
		t.Fatal(err)
	}
	if result.Scanned != 3 || result.Protected != 1 || result.Deleted != 1 || !bytes.Equal([]byte(store.deleted[0]), []byte("middle")) {
		t.Fatalf("unexpected cleanup result: %+v deleted=%v", result, store.deleted)
	}
}

func TestCleanupStopsOnDeleteFailure(t *testing.T) {
	store := &retentionStoreFake{objects: []Object{{Reference: "old", Record: Record{CompletedAt: time.Unix(1, 0)}}}, err: errors.New("delete unavailable")}
	if _, err := Cleanup(t.Context(), store, RetentionPolicy{KeepLast: 1}, nil, time.Unix(100, 0)); err == nil {
		t.Fatal("cleanup unexpectedly succeeded")
	}
}
