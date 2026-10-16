package backup

import (
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

// FileDestination resolves the only destination handled by the built-in
// store. The operator process still owns the directory configuration through
// --backup-dir; this parser prevents a CR from redirecting writes to an
// arbitrary host path or from silently treating an unsupported object-store
// URL as a local file store.
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
		return "", fmt.Errorf("backup destination scheme %q is unsupported; the built-in store accepts file:// only", destination.Scheme)
	}
	if destination.Host != "" && destination.Host != "localhost" {
		return "", errors.New("file backup destination must use a local path")
	}
	if destination.RawQuery != "" || destination.Fragment != "" || destination.Path == "" || !filepath.IsAbs(destination.Path) {
		return "", errors.New("file backup destination must be an absolute path without query or fragment")
	}
	return filepath.Clean(destination.Path), nil
}
