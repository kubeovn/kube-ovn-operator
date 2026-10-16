package backup

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

// S3Destination identifies a bucket and immutable object-key prefix. It does
// not contain credentials or an endpoint override; those are supplied by the
// process environment or an explicit store configuration.
type S3Destination struct {
	Bucket string
	Prefix string
}

func ParseS3Destination(raw string) (S3Destination, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return S3Destination{}, errors.New("backup destination is empty")
	}
	destination, err := url.Parse(value)
	if err != nil {
		return S3Destination{}, fmt.Errorf("parse S3 backup destination: %w", err)
	}
	if destination.Scheme != "s3" || destination.Host == "" || destination.User != nil || destination.RawQuery != "" || destination.Fragment != "" {
		return S3Destination{}, errors.New("S3 backup destination must be s3://bucket/prefix without credentials, query or fragment")
	}
	bucket := strings.ToLower(destination.Host)
	if destination.Host != bucket || !validS3Bucket(bucket) {
		return S3Destination{}, fmt.Errorf("invalid S3 bucket name %q", destination.Host)
	}
	prefix, err := url.PathUnescape(strings.TrimPrefix(destination.EscapedPath(), "/"))
	if err != nil {
		return S3Destination{}, fmt.Errorf("decode S3 object prefix: %w", err)
	}
	if len(prefix) > 1024 || strings.ContainsAny(prefix, "\\\x00\r\n") {
		return S3Destination{}, errors.New("S3 object prefix is invalid or too long")
	}
	for _, component := range strings.Split(prefix, "/") {
		if component == ".." {
			return S3Destination{}, errors.New("S3 object prefix cannot contain parent traversal")
		}
	}
	return S3Destination{Bucket: bucket, Prefix: strings.Trim(prefix, "/")}, nil
}

func validS3Bucket(bucket string) bool {
	if len(bucket) < 3 || len(bucket) > 63 || strings.HasPrefix(bucket, ".") || strings.HasSuffix(bucket, ".") || strings.HasPrefix(bucket, "-") || strings.HasSuffix(bucket, "-") || strings.Contains(bucket, "..") {
		return false
	}
	for _, character := range bucket {
		if character != '.' && character != '-' && (character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

// FileDestination resolves a local destination. The operator process still
// owns the directory configuration through --backup-dir; this parser prevents
// a CR from redirecting writes to an arbitrary host path or from treating an
// object-store URL as a local file store.
func FileDestination(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", errors.New("backup destination is empty")
	}
	destination, err := url.Parse(value)
	if err != nil {
		return "", fmt.Errorf("parse backup destination: %w", err)
	}
	if destination.Scheme != "file" {
		return "", fmt.Errorf("backup destination scheme %q is not a local file destination", destination.Scheme)
	}
	if destination.Host != "" && destination.Host != "localhost" {
		return "", errors.New("file backup destination must use a local path")
	}
	if destination.RawQuery != "" || destination.Fragment != "" || destination.Path == "" || !filepath.IsAbs(destination.Path) {
		return "", errors.New("file backup destination must be an absolute path without query or fragment")
	}
	return filepath.Clean(destination.Path), nil
}
