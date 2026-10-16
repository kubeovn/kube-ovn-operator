package backup

import "testing"

func TestFileDestination(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		want string
		err  bool
	}{
		{name: "absolute", raw: "file:///var/lib/kube-ovn/backups", want: "/var/lib/kube-ovn/backups"},
		{name: "localhost", raw: "file://localhost/var/lib/kube-ovn/backups", want: "/var/lib/kube-ovn/backups"},
		{name: "s3 unsupported", raw: "s3://bucket/kube-ovn", err: true},
		{name: "relative", raw: "file://backups", err: true},
		{name: "query", raw: "file:///var/lib/kube-ovn/backups?version=1", err: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := FileDestination(test.raw)
			if test.err {
				if err == nil {
					t.Fatalf("accepted invalid destination %q", test.raw)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("FileDestination(%q) = %q, %v; want %q", test.raw, got, err, test.want)
			}
		})
	}
}
