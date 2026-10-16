package backup

import (
	"testing"
	"time"
)

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
