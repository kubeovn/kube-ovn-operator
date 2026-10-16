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

func TestParseS3Destination(t *testing.T) {
	for _, test := range []struct {
		name   string
		raw    string
		bucket string
		prefix string
		err    bool
	}{
		{name: "bucket and prefix", raw: "s3://bucket-name/kube-ovn/backups", bucket: "bucket-name", prefix: "kube-ovn/backups"},
		{name: "empty prefix", raw: "s3://bucket-name", bucket: "bucket-name"},
		{name: "uppercase bucket", raw: "s3://Bucket-name/backups", err: true},
		{name: "query", raw: "s3://bucket-name/backups?version=1", err: true},
		{name: "parent traversal", raw: "s3://bucket-name/kube-ovn/../other", err: true},
		{name: "short bucket", raw: "s3://ab/backups", err: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseS3Destination(test.raw)
			if test.err {
				if err == nil {
					t.Fatalf("accepted invalid destination %q", test.raw)
				}
				return
			}
			if err != nil || got.Bucket != test.bucket || got.Prefix != test.prefix {
				t.Fatalf("ParseS3Destination(%q) = %+v, %v; want bucket=%q prefix=%q", test.raw, got, err, test.bucket, test.prefix)
			}
		})
	}
}
