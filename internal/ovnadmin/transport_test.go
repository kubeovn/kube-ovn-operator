package ovnadmin

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
)

type runFunc func(context.Context, string, []string) ([]byte, error)

func (f runFunc) Run(ctx context.Context, endpoint string, command []string) ([]byte, error) {
	return f(ctx, endpoint, command)
}

func testStatus(endpoint string) string {
	targets := testEndpoints()
	index := -1
	for i, target := range targets {
		if target.Name == endpoint {
			index = i
		}
	}
	if index < 0 {
		panic("unexpected test endpoint")
	}
	target := targets[index]
	role, leader := "follower", "server-0"
	if index == 0 {
		role, leader = "leader", "self"
	}
	text := fmt.Sprintf("%s\nName: OVN_Northbound\nCluster ID: dddd (%s)\nServer ID: %s (%s)\nAddress: %s\nStatus: cluster member\nRole: %s\nTerm: 7\nLeader: %s\nVote: self\nLog: [1, 21]\nEntries not yet committed: 0\nEntries not yet applied: 0\nConnections:", target.Name, testClusterID, target.ServerID[:4], target.ServerID, target.Address, role, leader)
	for _, peer := range targets {
		if peer.Name != endpoint {
			text += " ->" + peer.Name
		}
	}
	text += "\nServers:\n"
	for _, peer := range targets {
		text += fmt.Sprintf("    %s (%s at %s)", peer.Name, peer.ServerID[:4], peer.Address)
		if peer.Name == endpoint {
			text += " (self)"
		}
		text += "\n"
	}
	return text
}

func TestClusterStatusUsesExplicitCommitCounters(t *testing.T) {
	data := strings.Replace(testStatus("server-0"), "Entries not yet committed: 0", "Entries not yet committed: 2", 1)
	data = strings.Replace(data, "Entries not yet applied: 0", "Entries not yet applied: 3", 1)
	got, err := ParseClusterStatus([]byte(data), "OVN_Northbound")
	if err != nil || got.CommitIndex != 18 || got.AppliedIndex != 17 || len(got.Servers) != 3 {
		t.Fatalf("status=%+v err=%v", got, err)
	}
	for _, mutate := range []func(string) string{
		func(s string) string { return strings.Replace(s, "Entries not yet committed: 2\n", "", 1) },
		func(s string) string { return s + "unexpected member\n" },
		func(s string) string { return strings.Replace(s, "Term: 7", "Term: 7\nTerm: 7", 1) },
		func(s string) string {
			return strings.Replace(s, "Entries not yet applied: 3", "Entries not yet applied: 1", 1)
		},
		func(s string) string {
			return strings.Replace(s, "Entries not yet committed: 2", "Entries not yet committed: 21", 1)
		},
		func(s string) string {
			return strings.Replace(s, "Role: leader", "Adding server fourth (eeee at tcp:192.0.2.4:6643) (catchup)\nRole: leader", 1)
		},
		func(s string) string { return strings.Replace(s, " (self)", " (self) (removing)", 1) },
		func(s string) string { return strings.Replace(s, "Cluster ID: dddd", "Cluster ID: abcd", 1) },
		func(s string) string { return strings.Replace(s, "    server-1", "    server-0", 1) },
	} {
		if _, err := ParseClusterStatus([]byte(mutate(data)), "OVN_Northbound"); err == nil {
			t.Fatal("invalid cluster status accepted")
		}
	}
}

func TestCommandTransportRequiresStablePinnedIdentity(t *testing.T) {
	for _, scenario := range []string{"healthy", "unreachable", "lagging", "disconnected", "wrong identity", "wrong cluster", "unknown member", "term changed", "peer higher term", "second leader", "pod replaced", "leader disconnected after sampling"} {
		t.Run(scenario, func(t *testing.T) {
			leaderReads := 0
			c := &CommandClient{ClusterID: testClusterID, Endpoints: testEndpoints(), Runner: runFunc(func(_ context.Context, endpoint string, command []string) ([]byte, error) {
				if len(command) != 6 || command[4] != "cluster/status" || command[3] != "/var/run/ovn/ovnnb_db.ctl" {
					t.Fatalf("unexpected command: %v", command)
				}
				data := testStatus(endpoint)
				if endpoint == "server-0" {
					leaderReads++
				}
				if endpoint == "server-1" {
					switch scenario {
					case "unreachable":
						return nil, fmt.Errorf("connection closed")
					case "lagging":
						data = strings.Replace(data, "Entries not yet applied: 0", "Entries not yet applied: 1", 1)
					case "disconnected":
						data = strings.ReplaceAll(data, "->server-0", "(->server-0)")
					case "wrong identity":
						data = strings.ReplaceAll(data, testEndpoints()[1].ServerID, "bbbb9999-1111-2222-3333-444444444444")
					case "wrong cluster":
						data = strings.ReplaceAll(data, testClusterID, "dddd9999-1111-2222-3333-444444444444")
					case "unknown member":
						data = strings.ReplaceAll(data, "tcp:192.0.2.3:6643", "tcp:192.0.2.9:6643")
					case "pod replaced":
						return nil, ErrTargetIdentity
					case "peer higher term":
						data = strings.Replace(data, "Term: 7", "Term: 8", 1)
					case "second leader":
						data = strings.Replace(data, "Role: follower", "Role: leader", 1)
					}
				}
				if endpoint == "server-0" && leaderReads == 2 {
					switch scenario {
					case "term changed":
						data = strings.Replace(data, "Term: 7", "Term: 8", 1)
					case "leader disconnected after sampling":
						data = strings.ReplaceAll(data, "->server-1", "(->server-1)")
					}
				}
				return []byte(data), nil
			})}
			snapshot, err := c.Status(t.Context(), "OVN_Northbound", "server-0")
			wantError := scenario == "wrong identity" || scenario == "wrong cluster" || scenario == "unknown member" || scenario == "term changed" || scenario == "pod replaced" || scenario == "peer higher term" || scenario == "second leader"
			if wantError {
				if err == nil {
					t.Fatalf("unsafe snapshot: %+v", snapshot)
				}
				return
			}
			if err != nil || len(snapshot.Members) != 3 {
				t.Fatalf("snapshot=%+v err=%v", snapshot, err)
			}
			if snapshot.Members[1].Healthy != (scenario == "healthy") {
				t.Fatalf("wrong member health: %+v", snapshot)
			}
		})
	}
}

func TestCommandTransportRechecksBeforeDispatch(t *testing.T) {
	for _, scenario := range []string{"leave", "kick", "term changed", "target recovered", "no join primitive"} {
		t.Run(scenario, func(t *testing.T) {
			writes := 0
			leaderReads := 0
			var writtenEndpoint string
			var written []string
			c := &CommandClient{ClusterID: testClusterID, Endpoints: testEndpoints(), Runner: runFunc(func(_ context.Context, endpoint string, command []string) ([]byte, error) {
				if command[4] != "cluster/status" {
					writes++
					writtenEndpoint = endpoint
					written = command
					return []byte("started"), nil
				}
				if endpoint == "server-0" {
					leaderReads++
				}
				if endpoint == "server-1" && (scenario == "kick" || (scenario == "target recovered" && leaderReads < 3)) {
					return nil, fmt.Errorf("offline")
				}
				data := testStatus(endpoint)
				if scenario == "term changed" && leaderReads >= 3 {
					data = strings.Replace(data, "Term: 7", "Term: 8", 1)
				}
				return []byte(data), nil
			})}
			target := testSnapshot().Members[1]
			change := membership.Change{Operation: membership.OperationLeave, ClusterID: testClusterID, Member: target, Reason: "approved drain", BackupRef: "verified-backup"}
			if scenario == "kick" || scenario == "target recovered" {
				change.Operation = membership.OperationKick
				change.PermanentLossApproved = true
			}
			if scenario == "no join primitive" {
				change.Operation = membership.OperationJoin
				change.Member = membership.Member{ServerID: "eeee0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.4:6643"}
			}
			err := withCentralLease(t, func(ctx context.Context) error { return approvedExecutor(t, c).Execute(ctx, "server-0", change) })
			if scenario == "leave" || scenario == "kick" {
				if err != nil || writes != 1 {
					t.Fatalf("writes=%d err=%v", writes, err)
				}
				if scenario == "leave" && (writtenEndpoint != "server-1" || len(written) != 6 || written[4] != "cluster/leave") {
					t.Fatalf("wrong leave: %s %v", writtenEndpoint, written)
				}
				if scenario == "kick" && (writtenEndpoint != "server-0" || len(written) != 7 || written[6] != target.ServerID) {
					t.Fatalf("wrong kick: %s %v", writtenEndpoint, written)
				}
			} else if err == nil || writes != 0 {
				t.Fatalf("unsafe write=%d err=%v", writes, err)
			}
		})
	}
}

func TestBackupVerifierSeesFinalCommittedFence(t *testing.T) {
	leaderReads := 0
	writes := 0
	c := &CommandClient{ClusterID: testClusterID, Endpoints: testEndpoints(), Runner: runFunc(func(_ context.Context, endpoint string, command []string) ([]byte, error) {
		if command[4] != "cluster/status" {
			writes++
			return nil, nil
		}
		if endpoint == "server-0" {
			leaderReads++
		}
		data := testStatus(endpoint)
		if leaderReads >= 3 {
			data = strings.Replace(data, "Log: [1, 21]", "Log: [1, 22]", 1)
		}
		return []byte(data), nil
	})}
	executor := approvedExecutor(t, c)
	observedFence := int64(0)
	executor.Backups = backupFunc(func(_ context.Context, s Snapshot, _ string) error {
		observedFence = s.CommitIndex
		return fmt.Errorf("old backup does not cover new fence")
	})
	change := membership.Change{Operation: membership.OperationLeave, ClusterID: testClusterID, Member: testSnapshot().Members[1], Reason: "drain", BackupRef: "verified-backup"}
	err := withCentralLease(t, func(ctx context.Context) error { return executor.Execute(ctx, "server-0", change) })
	if err == nil || observedFence != 21 || writes != 0 {
		t.Fatalf("fence=%d writes=%d err=%v", observedFence, writes, err)
	}
}

func TestDispatchRejectsChangesDuringBackupReadback(t *testing.T) {
	for _, scenario := range []string{"commit", "term", "target recovered"} {
		t.Run(scenario, func(t *testing.T) {
			verified, writes := false, 0
			c := &CommandClient{ClusterID: testClusterID, Endpoints: testEndpoints(), Runner: runFunc(func(_ context.Context, endpoint string, command []string) ([]byte, error) {
				if command[4] != "cluster/status" {
					writes++
					return nil, nil
				}
				if scenario == "target recovered" && endpoint == "server-1" && !verified {
					return nil, fmt.Errorf("offline")
				}
				data := testStatus(endpoint)
				if verified {
					switch scenario {
					case "commit":
						data = strings.Replace(data, "Log: [1, 21]", "Log: [1, 22]", 1)
					case "term":
						data = strings.Replace(data, "Term: 7", "Term: 8", 1)
					}
				}
				return []byte(data), nil
			})}
			executor := approvedExecutor(t, c)
			executor.Backups = backupFunc(func(context.Context, Snapshot, string) error { verified = true; return nil })
			change := membership.Change{Operation: membership.OperationLeave, ClusterID: testClusterID, Member: testSnapshot().Members[1], Reason: "replace", BackupRef: "verified-backup"}
			if scenario == "target recovered" {
				change.Operation, change.PermanentLossApproved = membership.OperationKick, true
			}
			err := withCentralLease(t, func(ctx context.Context) error { return executor.Execute(ctx, "server-0", change) })
			if err == nil || writes != 0 || !verified {
				t.Fatalf("stale authorization: writes=%d verified=%v err=%v", writes, verified, err)
			}
		})
	}
}
