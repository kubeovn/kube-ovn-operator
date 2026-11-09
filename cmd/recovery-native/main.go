// Command recovery-native is the small, audited adapter used by the offline
// recovery executor. It deliberately requires an OVS tool with the Kube-OVN
// target-CID create-cluster extension; an unpatched tool is rejected after it
// creates a database with a different cluster identity.
package main

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
	"github.com/kubeovn/kube-ovn-operator/internal/recovery"
)

const maxOutput = 4 << 10

type raftHeader struct {
	ServerID        string   `json:"server_id"`
	ClusterID       string   `json:"cluster_id"`
	LocalAddress    string   `json:"local_address"`
	RemoteAddresses []string `json:"remote_addresses"`
}

type inspection struct {
	ClusterID string `json:"clusterID"`
	ServerID  string `json:"serverID"`
	Address   string `json:"address"`
	Clustered bool   `json:"clustered"`
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Getenv("OVSDB_TOOL")); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, tool string) error {
	if tool == "" {
		tool = "/opt/ovn-native/ovsdb-tool"
	}
	if err := validateTool(tool); err != nil {
		return err
	}
	if len(args) > 0 && strings.HasPrefix(args[0], "--workload-plan=") {
		return runWorkload(ctx, args)
	}
	if len(args) == 2 && args[0] == "inspect" {
		identity, err := inspect(ctx, tool, args[1])
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(identity)
		if err != nil {
			return fmt.Errorf("encode native inspection: %w", err)
		}
		_, err = os.Stdout.Write(append(encoded, '\n'))
		return err
	}
	if len(args) != 5 || !strings.HasPrefix(args[0], "--target-cluster-id=") || args[1] != "create-cluster" {
		return errors.New("usage: --target-cluster-id=<uuid> create-cluster <db> <contents> <local> | inspect <db>")
	}
	targetID := strings.TrimPrefix(args[0], "--target-cluster-id=")
	if !membership.ValidID(targetID) {
		return errors.New("target cluster ID must be a full UUID")
	}
	if err := validateDatabasePaths(args[2], args[3]); err != nil {
		return err
	}
	if err := validateAddress(args[4]); err != nil {
		return err
	}
	if _, err := invoke(ctx, tool, "--cid="+targetID, "create-cluster", args[2], args[3], args[4]); err != nil {
		return err
	}
	identity, err := inspect(ctx, tool, args[2])
	if err != nil {
		return fmt.Errorf("verify created cluster: %w", err)
	}
	if identity.ClusterID != targetID {
		return fmt.Errorf("created cluster ID %q differs from target %q", identity.ClusterID, targetID)
	}
	return nil
}

type workloadArguments struct {
	PlanPath   string
	ProofPath  string
	TargetRoot string
	ProcRoot   string
	BackupDir  string
	MemberName string
}

func runWorkload(ctx context.Context, args []string) error {
	arguments, err := parseWorkloadArguments(args)
	if err != nil {
		return err
	}
	planBytes, err := readPrivateRegular(arguments.PlanPath, 1<<20)
	if err != nil {
		return fmt.Errorf("read offline rebuild plan: %w", err)
	}
	var plan recovery.Plan
	if err := json.Unmarshal(planBytes, &plan, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("decode offline rebuild plan: %w", err)
	}
	canonical, _, err := plan.Canonical()
	if err != nil {
		return err
	}
	proofBytes, err := readPrivateRegular(arguments.ProofPath, 64<<10)
	if err != nil {
		return fmt.Errorf("read offline isolation proof: %w", err)
	}
	var proof recovery.IsolationProof
	if err := json.Unmarshal(proofBytes, &proof, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("decode offline isolation proof: %w", err)
	}
	if err := proof.Validate(canonical); err != nil {
		return err
	}
	if _, ok := targetMember(canonical.TargetMembers, arguments.MemberName); !ok {
		return fmt.Errorf("offline rebuild target member %q is not in the plan", arguments.MemberName)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// A local process scan and caller-supplied metadata cannot establish remote
	// fencing or live PVC ownership. Keep this entry point disabled until an
	// enforced isolation provider covers all retained members and target storage.
	return errors.New("offline rebuild workload requires enforced isolation and live target storage ownership; provider unavailable")
}

func parseWorkloadArguments(args []string) (workloadArguments, error) {
	var result workloadArguments
	seen := make(map[string]bool, len(args))
	for _, argument := range args {
		name, value, ok := strings.Cut(argument, "=")
		if !ok {
			return result, errors.New("offline rebuild workload arguments must use --name=value")
		}
		value = strings.TrimSpace(value)
		if value == "" || seen[name] {
			return result, errors.New("offline rebuild workload arguments are missing or duplicated")
		}
		seen[name] = true
		switch name {
		case "--workload-plan":
			result.PlanPath = value
		case "--isolation-proof":
			result.ProofPath = value
		case "--target-root":
			result.TargetRoot = value
		case "--proc-root":
			result.ProcRoot = value
		case "--backup-dir":
			result.BackupDir = value
		case "--member":
			result.MemberName = value
		default:
			return result, fmt.Errorf("unknown offline rebuild workload argument %q", name)
		}
	}
	if result.PlanPath == "" || result.ProofPath == "" || result.TargetRoot == "" || result.ProcRoot == "" || result.BackupDir == "" || result.MemberName == "" {
		return result, errors.New("offline rebuild workload requires plan, proof, target root, proc root, backup dir and member")
	}
	for name, path := range map[string]string{"plan": result.PlanPath, "proof": result.ProofPath, "target root": result.TargetRoot, "proc root": result.ProcRoot, "backup dir": result.BackupDir} {
		if !filepath.IsAbs(path) {
			return result, fmt.Errorf("offline rebuild %s must be absolute", name)
		}
	}
	return result, nil
}

func targetMember(members []recovery.Member, name string) (recovery.Member, bool) {
	for _, member := range members {
		if member.Name == name {
			return member, true
		}
	}
	return recovery.Member{}, false
}

func readPrivateRegular(path string, limit int64) ([]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, errors.New("path must be absolute")
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errors.New("path must reference a bounded regular file")
	}
	return os.ReadFile(path)
}

func inspect(ctx context.Context, tool, database string) (inspection, error) {
	if !filepath.IsAbs(database) {
		return inspection{}, errors.New("database path must be absolute")
	}
	output, err := invoke(ctx, tool, "db-raft-header", database)
	if err != nil {
		return inspection{}, err
	}
	var header raftHeader
	if err := json.Unmarshal([]byte(output), &header); err != nil {
		return inspection{}, fmt.Errorf("decode OVSDB raft header: %w", err)
	}
	header.ClusterID = strings.TrimSpace(header.ClusterID)
	header.ServerID = strings.TrimSpace(header.ServerID)
	header.LocalAddress = strings.TrimSpace(header.LocalAddress)
	if len(header.RemoteAddresses) != 0 || !membership.ValidID(header.ClusterID) || !membership.ValidID(header.ServerID) {
		return inspection{}, errors.New("OVSDB raft header is not a joined clustered database")
	}
	if err := validateAddress(header.LocalAddress); err != nil {
		return inspection{}, fmt.Errorf("validate OVSDB raft address: %w", err)
	}
	return inspection{ClusterID: header.ClusterID, ServerID: header.ServerID, Address: header.LocalAddress, Clustered: true}, nil
}

func validateDatabasePaths(database, contents string) error {
	for name, path := range map[string]string{"database": database, "contents": contents} {
		if !filepath.IsAbs(path) {
			return fmt.Errorf("%s path must be absolute", name)
		}
	}
	return nil
}

func validateTool(path string) error {
	if !filepath.IsAbs(path) {
		return errors.New("OVSDB_TOOL must be an absolute path")
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("inspect OVSDB_TOOL: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return errors.New("OVSDB_TOOL must be an executable regular file")
	}
	if info.Mode()&(os.ModeSetuid|os.ModeSetgid) != 0 || info.Mode().Perm()&022 != 0 {
		return errors.New("OVSDB_TOOL must not be writable or perform file-based privilege transitions")
	}
	size, err := syscall.Getxattr(path, "security.capability", nil)
	if err != nil && !errors.Is(err, syscall.ENODATA) && !errors.Is(err, syscall.ENOTSUP) {
		return fmt.Errorf("inspect OVSDB_TOOL capabilities: %w", err)
	}
	if size > 0 {
		return errors.New("OVSDB_TOOL must not perform file-based privilege transitions")
	}
	return nil
}

func validateAddress(address string) error {
	if err := membership.ValidateMembers([]membership.Member{{ServerID: "11111111-1111-1111-1111-111111111111", Address: address}}); err != nil {
		return err
	}
	return nil
}

func invoke(ctx context.Context, tool string, args ...string) (string, error) {
	command := exec.CommandContext(ctx, tool, args...)
	stdout, stderr := new(boundedOutput), new(boundedOutput)
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = time.Second
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("OVSDB tool failed: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

type boundedOutput struct{ bytes.Buffer }

func (o *boundedOutput) Write(data []byte) (int, error) {
	if len(data) > maxOutput-o.Len() {
		return 0, errors.New("OVSDB tool output exceeds 4 KiB")
	}
	return o.Buffer.Write(data)
}

var _ io.Writer = (*boundedOutput)(nil)
