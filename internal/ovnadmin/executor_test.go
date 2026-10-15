package ovnadmin

import (
	"context"
	"fmt"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const testClusterID = "dddd0000-1111-2222-3333-444444444444"

func testEndpoints() []Endpoint {
	return []Endpoint{
		{Name: "server-0", ServerID: "aaaa0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.1:6643"},
		{Name: "server-1", ServerID: "bbbb0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.2:6643"},
		{Name: "server-2", ServerID: "cccc0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.3:6643"},
	}
}

func testSnapshot() Snapshot {
	result := Snapshot{Database: "OVN_Northbound", ClusterID: testClusterID, Term: 7, CommitIndex: 20, AppliedIndex: 20}
	for i, target := range testEndpoints() {
		result.Members = append(result.Members, membership.Member{ServerID: target.ServerID, Address: target.Address, Healthy: true, Leader: i == 0})
	}
	result.LeaderServerID = result.Members[0].ServerID
	return result
}

type fakeClient struct {
	snapshot  Snapshot
	operation membership.Operation
}

func (f *fakeClient) Status(context.Context, string, string) (Snapshot, error) {
	return f.snapshot, nil
}
func (f *fakeClient) mutate(ctx context.Context, _ string, current Snapshot, change membership.Change, authorize func(context.Context, Snapshot) (authorization, error)) error {
	if _, err := authorize(ctx, current); err != nil {
		return err
	}
	f.operation = change.Operation
	return nil
}

type backupFunc func(context.Context, Snapshot, string) error

func (f backupFunc) Verify(ctx context.Context, s Snapshot, ref string) error { return f(ctx, s, ref) }

type lossFunc func(context.Context, Snapshot, membership.Change) (time.Time, error)

func (f lossFunc) VerifyPermanentLoss(ctx context.Context, s Snapshot, c membership.Change) (time.Time, error) {
	return f(ctx, s, c)
}

func approvedExecutor(t *testing.T, c Client) Executor {
	t.Helper()
	expiry := time.Now().Add(time.Hour)
	return Executor{Client: c, Backups: backupFunc(func(_ context.Context, s Snapshot, ref string) error {
		if s.ClusterID != testClusterID || ref != "verified-backup" {
			return fmt.Errorf("backup does not match")
		}
		return nil
	}), PermanentLoss: lossFunc(func(_ context.Context, s Snapshot, c membership.Change) (time.Time, error) {
		if c.ClusterID != s.ClusterID || !c.PermanentLossApproved {
			return time.Time{}, fmt.Errorf("approval does not match")
		}
		return expiry, nil
	})}
}

func TestExecutorValidatesBoundRemoval(t *testing.T) {
	for _, operation := range []membership.Operation{membership.OperationLeave, membership.OperationKick} {
		t.Run(string(operation), func(t *testing.T) {
			c := &fakeClient{snapshot: testSnapshot()}
			c.snapshot.Members[1].Healthy = operation == membership.OperationLeave
			change := membership.Change{Operation: operation, ClusterID: testClusterID, Member: c.snapshot.Members[1], Reason: "approved replacement", BackupRef: "verified-backup", PermanentLossApproved: operation == membership.OperationKick}
			err := withCentralLease(t, func(ctx context.Context) error { return approvedExecutor(t, c).Execute(ctx, "server-0", change) })
			if err != nil || c.operation != operation {
				t.Fatalf("operation=%s err=%v", c.operation, err)
			}
		})
	}
}

func TestExecutorRejectsUnsafeOrUnboundRemoval(t *testing.T) {
	for _, scenario := range []string{"cluster", "database", "short ID", "address", "duplicate ID", "duplicate address", "no leader", "two leaders", "no quorum", "zero commit", "lagging leader", "applied ahead", "healthy kick", "unapproved kick", "missing reason", "missing backup", "missing verifier", "bad backup", "missing loss verifier", "bad loss approval", "remaining quorum"} {
		t.Run(scenario, func(t *testing.T) {
			c := &fakeClient{snapshot: testSnapshot()}
			c.snapshot.Members[1].Healthy = false
			change := membership.Change{Operation: membership.OperationKick, ClusterID: testClusterID, Member: c.snapshot.Members[1], Reason: "permanent loss", BackupRef: "verified-backup", PermanentLossApproved: true}
			executor := approvedExecutor(t, c)
			switch scenario {
			case "cluster":
				change.ClusterID = "eeee0000-1111-2222-3333-444444444444"
			case "database":
				c.snapshot.Database = "OVN_Southbound"
			case "short ID":
				change.Member.ServerID = "bbbb"
			case "address":
				change.Member.Address = "tcp:192.0.2.9:6643"
			case "duplicate ID":
				c.snapshot.Members[2].ServerID = c.snapshot.Members[0].ServerID
			case "duplicate address":
				c.snapshot.Members[2].Address = c.snapshot.Members[0].Address
			case "no leader":
				c.snapshot.Members[0].Leader = false
			case "two leaders":
				c.snapshot.Members[2].Leader = true
			case "no quorum":
				c.snapshot.Members[2].Healthy = false
			case "zero commit":
				c.snapshot.CommitIndex = 0
				c.snapshot.AppliedIndex = 0
			case "lagging leader":
				c.snapshot.AppliedIndex--
			case "applied ahead":
				c.snapshot.AppliedIndex++
			case "healthy kick":
				c.snapshot.Members[1].Healthy = true
			case "unapproved kick":
				change.PermanentLossApproved = false
			case "missing reason":
				change.Reason = ""
			case "missing backup":
				change.BackupRef = ""
			case "missing verifier":
				executor.Backups = nil
			case "bad backup":
				executor.Backups = backupFunc(func(context.Context, Snapshot, string) error { return fmt.Errorf("checksum mismatch") })
			case "missing loss verifier":
				executor.PermanentLoss = nil
			case "bad loss approval":
				executor.PermanentLoss = lossFunc(func(context.Context, Snapshot, membership.Change) (time.Time, error) {
					return time.Time{}, fmt.Errorf("expired approval")
				})
			case "remaining quorum":
				change.Operation = membership.OperationLeave
				c.snapshot.Members[1].Healthy = true
				c.snapshot.Members[2].Healthy = false
			}
			err := withCentralLease(t, func(ctx context.Context) error { return executor.Execute(ctx, "server-0", change) })
			if err == nil || c.operation != "" {
				t.Fatalf("unsafe operation=%s err=%v", c.operation, err)
			}
		})
	}
}

func TestExecutorRequiresLease(t *testing.T) {
	c := &fakeClient{snapshot: testSnapshot()}
	change := membership.Change{Operation: membership.OperationLeave, ClusterID: testClusterID, Member: c.snapshot.Members[1], Reason: "drain", BackupRef: "verified-backup"}
	if err := approvedExecutor(t, c).Execute(t.Context(), "server-0", change); err == nil || c.operation != "" {
		t.Fatalf("write without lease: %v", err)
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
	return manager.With(t.Context(), installation, lease.Installation, func(ctx context.Context) error { return manager.With(ctx, installation, lease.Central, operation) })
}

func TestExecutorRefusesLeaseLostDuringBackupVerification(t *testing.T) {
	c := &fakeClient{snapshot: testSnapshot()}
	change := membership.Change{Operation: membership.OperationLeave, ClusterID: testClusterID, Member: c.snapshot.Members[1], Reason: "drain", BackupRef: "verified-backup"}
	err := withCentralLease(t, func(ctx context.Context) error {
		operationCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		executor := approvedExecutor(t, c)
		executor.Backups = backupFunc(func(context.Context, Snapshot, string) error { cancel(); return nil })
		return executor.Execute(operationCtx, "server-0", change)
	})
	if err == nil || c.operation != "" {
		t.Fatalf("write after cancellation: %s %v", c.operation, err)
	}
}

func TestExecutorRefusesLastMemberRemoval(t *testing.T) {
	c := &fakeClient{snapshot: testSnapshot()}
	c.snapshot.Members = c.snapshot.Members[:1]
	change := membership.Change{Operation: membership.OperationLeave, ClusterID: testClusterID, Member: c.snapshot.Members[0], Reason: "drain", BackupRef: "verified-backup"}
	err := withCentralLease(t, func(ctx context.Context) error { return approvedExecutor(t, c).Execute(ctx, "server-0", change) })
	if err == nil || c.operation != "" {
		t.Fatalf("last member removed: %s %v", c.operation, err)
	}
}
