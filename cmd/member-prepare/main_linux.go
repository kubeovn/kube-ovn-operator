// member-prepare creates a new NB/SB member's files without starting OVN or
// changing membership. Run it with ovsdb-tool available in a trusted image.
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

	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
)

func main() {
	root := flag.String("root", "", "Existing private persistent preparation root (absolute path)")
	request := flag.String("request", "", "Preparation plan JSON file")
	tool := flag.String("ovsdb-tool", "", "Absolute ovsdb-tool path (default: resolve from PATH)")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *root, *request, *tool, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, root, path, tool string, stdout io.Writer) error {
	if root == "" || path == "" {
		return errors.New("--root and --request are required")
	}
	// O_NONBLOCK lets us reject a FIFO before waiting for an absent writer.
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("preparation request must be a regular file")
	}
	content, err := io.ReadAll(io.LimitReader(file, (16<<10)+1))
	if err != nil {
		return err
	}
	if len(content) > 16<<10 {
		return errors.New("preparation request exceeds 16 KiB")
	}
	var plan prepare.Plan
	if err := json.Unmarshal(content, &plan, json.RejectUnknownMembers(true)); err != nil {
		return errors.New("invalid preparation request JSON")
	}
	if tool == "" {
		tool, err = exec.LookPath("ovsdb-tool")
		if err != nil {
			return err
		}
	}
	preparer, err := prepare.Open(root, tool)
	if err != nil {
		return err
	}
	defer func() { _ = preparer.Close() }()
	result, err := preparer.Prepare(ctx, plan)
	if err != nil {
		return err
	}
	return json.MarshalWrite(stdout, result)
}
