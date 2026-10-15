package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/central"
)

func TestLaunchCommandRejectsInvalidInput(t *testing.T) {
	for _, scenario := range []string{"missing", "malformed", "oversized", "unknown field", "FIFO"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "request.json")
			if scenario == "FIFO" {
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			} else if scenario != "missing" {
				content := "bad-json"
				switch scenario {
				case "oversized":
					content = strings.Repeat(" ", 33<<10)
				case "unknown field":
					content = `{"unknown":true}`
				}
				if err := os.WriteFile(path, []byte(content), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := run(t.Context(), central.Options{Root: root}, path); err == nil {
				t.Fatal("invalid launch input accepted")
			}
		})
	}
}
