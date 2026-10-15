// member-run supervises explicitly prepared NB/SB databases and ovn-northd.
package main

import (
	"context"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/central"
)

func main() {
	var options central.Options
	flag.StringVar(&options.Root, "root", "", "Existing private persistent preparation root")
	flag.StringVar(&options.RuntimeDirectory, "run-dir", "/var/run/ovn", "Existing private runtime directory")
	flag.StringVar(&options.OVSDBTool, "ovsdb-tool", "", "Absolute ovsdb-tool path (default: resolve from PATH)")
	flag.StringVar(&options.OVSDBServer, "ovsdb-server", "", "Absolute ovsdb-server path (default: resolve from PATH)")
	flag.StringVar(&options.Northd, "ovn-northd", "", "Absolute ovn-northd path (default: resolve from PATH)")
	flag.DurationVar(&options.ShutdownTimeout, "shutdown-timeout", 10*time.Second, "Grace period before SIGKILL, at most 30 seconds")
	request := flag.String("request", "", "Persisted launch request JSON file")
	flag.Parse()
	options.Stdout, options.Stderr = os.Stdout, os.Stderr
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, options, *request); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, options central.Options, path string) error {
	if options.Root == "" || path == "" {
		return errors.New("--root and --request are required")
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("launch request must be a regular file")
	}
	content, err := io.ReadAll(io.LimitReader(file, (32<<10)+1))
	if err != nil {
		return err
	}
	if len(content) > 32<<10 {
		return errors.New("launch request exceeds 32 KiB")
	}
	var request central.Request
	if err := json.Unmarshal(content, &request, json.RejectUnknownMembers(true)); err != nil {
		return errors.New("invalid launch request JSON")
	}
	for name, target := range map[string]*string{"ovsdb-tool": &options.OVSDBTool, "ovsdb-server": &options.OVSDBServer, "ovn-northd": &options.Northd} {
		if *target == "" {
			*target, err = exec.LookPath(name)
			if err != nil {
				return err
			}
		}
	}
	return central.Run(ctx, options, request)
}
