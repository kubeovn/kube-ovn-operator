package recovery

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

// CommandNative adapts an audited native rebuild helper to Native. The helper
// must understand --target-cluster-id for create-cluster; passing a stock
// ovsdb-tool is expected to fail because it cannot bind that identity.
//
// Run is injectable for tests. When it is nil, Binary must be an absolute,
// private executable without file-based privilege transitions.
type CommandNative struct {
	Binary string
	Run    func(context.Context, ...string) (string, error)
}

func (n CommandNative) CreateCluster(ctx context.Context, database, contents, localAddress, targetClusterID string) error {
	if err := validateNativeInputs(database, contents, localAddress, targetClusterID); err != nil {
		return err
	}
	_, err := n.run(ctx, "--target-cluster-id="+targetClusterID, "create-cluster", database, contents, localAddress)
	return err
}

func (n CommandNative) Inspect(ctx context.Context, database string) (NativeDatabase, error) {
	if !filepath.IsAbs(database) {
		return NativeDatabase{}, errors.New("native database path must be absolute")
	}
	output, err := n.run(ctx, "inspect", database)
	if err != nil {
		return NativeDatabase{}, err
	}
	var inspection struct {
		ClusterID string `json:"clusterID"`
		ServerID  string `json:"serverID"`
		Address   string `json:"address"`
		Clustered bool   `json:"clustered"`
	}
	if err := json.Unmarshal([]byte(output), &inspection); err != nil {
		return NativeDatabase{}, fmt.Errorf("decode native inspection: %w", err)
	}
	clusterID, serverID, address := strings.TrimSpace(inspection.ClusterID), strings.TrimSpace(inspection.ServerID), strings.TrimSpace(inspection.Address)
	if !inspection.Clustered || !membership.ValidID(clusterID) || !membership.ValidID(serverID) {
		return NativeDatabase{}, errors.New("native inspection did not return a clustered database identity")
	}
	if err := membership.ValidateMembers([]membership.Member{{ServerID: serverID, Address: address}}); err != nil {
		return NativeDatabase{}, fmt.Errorf("native inspection returned an invalid address: %w", err)
	}
	return NativeDatabase{ClusterID: clusterID, ServerID: serverID, Address: address}, nil
}

func (n CommandNative) run(ctx context.Context, args ...string) (string, error) {
	if n.Run != nil {
		return n.Run(ctx, args...)
	}
	if err := validateNativeBinary(n.Binary); err != nil {
		return "", err
	}
	command := exec.CommandContext(ctx, n.Binary, args...)
	stdout, stderr := new(nativeOutput), new(nativeOutput)
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = time.Second
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("native rebuild helper failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func validateNativeInputs(database, contents, address, clusterID string) error {
	for name, path := range map[string]string{"database": database, "contents": contents} {
		if !filepath.IsAbs(path) {
			return fmt.Errorf("native %s path must be absolute", name)
		}
	}
	if !membership.ValidID(clusterID) {
		return errors.New("native target cluster ID must be a full UUID")
	}
	if err := membership.ValidateMembers([]membership.Member{{ServerID: "11111111-1111-1111-1111-111111111111", Address: address}}); err != nil {
		return fmt.Errorf("native local address is invalid: %w", err)
	}
	return nil
}

func validateNativeBinary(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("native helper path must be absolute")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return errors.New("native helper must be an executable regular file")
	}
	if info.Mode()&(os.ModeSetuid|os.ModeSetgid) != 0 {
		return errors.New("native helper must not perform file-based privilege transitions")
	}
	size, err := syscall.Getxattr(path, "security.capability", nil)
	if err != nil && !errors.Is(err, syscall.ENODATA) && !errors.Is(err, syscall.ENOTSUP) {
		return fmt.Errorf("inspect native helper capabilities: %w", err)
	}
	if size > 0 {
		return errors.New("native helper must not perform file-based privilege transitions")
	}
	return nil
}

type nativeOutput struct{ bytes.Buffer }

func (o *nativeOutput) Write(data []byte) (int, error) {
	if len(data) > 4<<10-o.Len() {
		return 0, errors.New("native helper output exceeds 4 KiB")
	}
	return o.Buffer.Write(data)
}

var _ io.Writer = (*nativeOutput)(nil)
