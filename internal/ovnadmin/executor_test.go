package ovnadmin

import (
	"context"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeClient struct {
	snapshot  Snapshot
	operation membership.Operation
	serverID  string
}

func (f *fakeClient) Status(context.Context, string, string) (Snapshot, error) {
	return f.snapshot, nil
}
func (f *fakeClient) Join(context.Context, string, string, membership.Member) error {
	f.operation = membership.OperationJoin
	return nil
}
func (f *fakeClient) Leave(_ context.Context, _, _, serverID string) error {
	f.operation = membership.OperationLeave
	f.serverID = serverID
	return nil
}
func (f *fakeClient) Kick(_ context.Context, _, _, serverID string) error {
	f.operation = membership.OperationKick
	f.serverID = serverID
	return nil
}

func TestExecutorRejectsMissingQuorum(t *testing.T) {
	client := &fakeClient{snapshot: Snapshot{ClusterID: "cluster-a", LeaderServerID: "a", Database: "northbound", CommitIndex: 10, AppliedIndex: 10, Members: []membership.Member{{ServerID: "a", Healthy: true}, {ServerID: "b", Healthy: false}, {ServerID: "c", Healthy: false}}}}
	err := (Executor{Client: client}).Execute(t.Context(), "nb.example", membership.Change{Operation: membership.OperationKick, Member: membership.Member{ServerID: "b"}, Reason: "permanent loss"})
	if err == nil {
		t.Fatal("expected quorum gate")
	}
	if client.operation != "" {
		t.Fatalf("unsafe operation was executed: %q", client.operation)
	}
}

func TestExecutorExecutesAuditedKickAfterChecks(t *testing.T) {
	client := &fakeClient{snapshot: Snapshot{ClusterID: "cluster-a", LeaderServerID: "a", Database: "northbound", CommitIndex: 10, AppliedIndex: 9, Members: []membership.Member{{ServerID: "a", Healthy: true}, {ServerID: "b", Healthy: false}, {ServerID: "c", Healthy: true}}}}
	err := withCentralLease(t, func(ctx context.Context) error {
		return (Executor{Client: client}).Execute(ctx, "nb.example", membership.Change{Operation: membership.OperationKick, Member: membership.Member{ServerID: "b"}, Reason: "node permanently lost; approved recovery request"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if client.operation != membership.OperationKick || client.serverID != "b" {
		t.Fatalf("unexpected operation: %q %q", client.operation, client.serverID)
	}
}

func TestExecutorRefusesRemovalThatLeavesNoQuorum(t *testing.T) {
	client := &fakeClient{snapshot: Snapshot{ClusterID: "cluster-a", LeaderServerID: "a", Database: "northbound", CommitIndex: 1, AppliedIndex: 1, Members: []membership.Member{{ServerID: "a", Address: "a", Healthy: true}}}}
	err := (Executor{Client: client}).Execute(t.Context(), "nb.example", membership.Change{Operation: membership.OperationLeave, Member: membership.Member{ServerID: "a"}})
	if err == nil {
		t.Fatal("expected last-member removal gate")
	}
	if client.operation != "" {
		t.Fatal("unsafe leave was executed")
	}
}

func TestExecutorRefusesDuplicateJoin(t *testing.T) {
	client := &fakeClient{snapshot: Snapshot{ClusterID: "cluster-a", LeaderServerID: "a", Database: "northbound", CommitIndex: 1, AppliedIndex: 1, Members: []membership.Member{{ServerID: "a", Address: "a", Healthy: true}, {ServerID: "b", Address: "b", Healthy: true}, {ServerID: "c", Address: "c", Healthy: true}}}}
	err := (Executor{Client: client}).Execute(t.Context(), "nb.example", membership.Change{Operation: membership.OperationJoin, Member: membership.Member{ServerID: "a", Address: "new"}})
	if err == nil {
		t.Fatal("expected duplicate identity gate")
	}
	if client.operation != "" {
		t.Fatal("unsafe join was executed")
	}
}

func withCentralLease(t *testing.T, operation func(context.Context) error) error {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	manager := lease.New(fake.NewClientBuilder().WithScheme(scheme).Build())
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "ovn", Namespace: "default", UID: "installation-uid"}
	return manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, lease.Central, operation)
	})
}

func TestExecutorRefusesMembershipWriteWithoutLease(t *testing.T) {
	client := &fakeClient{snapshot: Snapshot{ClusterID: "cluster-a", LeaderServerID: "a", CommitIndex: 10, AppliedIndex: 10, Members: []membership.Member{{ServerID: "a", Healthy: true}, {ServerID: "b", Healthy: true}, {ServerID: "c", Healthy: true}}}}
	err := (Executor{Client: client}).Execute(t.Context(), "nb.example", membership.Change{Operation: membership.OperationJoin, Member: membership.Member{ServerID: "d", Address: "192.0.2.4"}})
	if err == nil || client.operation != "" {
		t.Fatalf("membership mutation bypassed lease: %v %s", err, client.operation)
	}
}
