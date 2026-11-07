package fencing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
)

const maxNativeOutput = 128 << 10

// NativeExecutor runs the already validated canonical membership command
// without invoking a shell. It is suitable for a fence-agent sidecar whose
// filesystem contains the OVN admin binary and control sockets.
type NativeExecutor struct{}

func (NativeExecutor) Execute(ctx context.Context, command []string) ([]byte, error) {
	if err := ValidateMembershipCommand(command); err != nil {
		return nil, err
	}
	process := exec.CommandContext(ctx, command[0], command[1:]...)
	output := &boundedOutput{limit: maxNativeOutput}
	process.Stdout = output
	process.Stderr = output
	if err := process.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("native OVN command failed: %w: %s", err, output.Bytes())
	}
	return output.Bytes(), nil
}

// Restore streams a native backup directly to ovsdb-client without buffering
// it in the fence agent. The caller has already admitted the request through
// Gate.ExecuteRestore and the database name is restricted to the two OVN DBs.
func (NativeExecutor) Restore(ctx context.Context, database string, source io.Reader) error {
	if err := ValidateRestoreDatabase(database); err != nil {
		return err
	}
	if source == nil {
		return errors.New("restore source is required")
	}
	socket := "/var/run/ovn/ovnnb_db.sock"
	if database == "OVN_Southbound" {
		socket = "/var/run/ovn/ovnsb_db.sock"
	}
	process := exec.CommandContext(ctx, "ovsdb-client", "--timeout=120", "restore", "unix:"+socket, database)
	process.Stdin = source
	output := &boundedOutput{limit: maxNativeOutput}
	process.Stdout = output
	process.Stderr = output
	if err := process.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("native OVN restore failed: %w: %s", err, output.Bytes())
	}
	return nil
}

type boundedOutput struct {
	buffer bytes.Buffer
	limit  int
}

func (o *boundedOutput) Write(data []byte) (int, error) {
	if len(data) > o.limit-o.buffer.Len() {
		return 0, errors.New("native OVN command output exceeds 128 KiB")
	}
	return o.buffer.Write(data)
}

func (o *boundedOutput) Bytes() []byte {
	return o.buffer.Bytes()
}
