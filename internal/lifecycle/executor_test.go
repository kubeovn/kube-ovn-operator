package lifecycle

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"uuid"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/central"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type databaseFake struct {
	snapshot       ovnadmin.Snapshot
	backups        map[string]ovnadmin.Snapshot
	removals       []membership.Change
	onVerify       func()
	loseReply      bool
	beforeDispatch func()
}

func (d *databaseFake) Observe(context.Context) (ovnadmin.Snapshot, error) {
	result := d.snapshot
	result.Members = slices.Clone(result.Members)
	return result, nil
}

func (d *databaseFake) Capture(ctx context.Context) (string, error) {
	snapshot, _ := d.Observe(ctx)
	ref := uuid.New().String()
	d.backups[ref] = snapshot
	return ref, nil
}

func (d *databaseFake) Verify(_ context.Context, snapshot ovnadmin.Snapshot, ref string) error {
	if d.onVerify != nil {
		d.onVerify()
	}
	if !sameFence(snapshot, d.backups[ref]) {
		return errors.New("backup does not cover the live fence")
	}
	return nil
}

func (d *databaseFake) Remove(ctx context.Context, change membership.Change, guard func(context.Context, ovnadmin.Snapshot) error) error {
	if err := d.Verify(ctx, d.snapshot, change.BackupRef); err != nil {
		return err
	}
	if d.beforeDispatch != nil {
		d.beforeDispatch()
	}
	if err := guard(ctx, d.snapshot); err != nil {
		return err
	}
	d.removals = append(d.removals, change)
	d.snapshot.Members = slices.DeleteFunc(d.snapshot.Members, func(m membership.Member) bool { return m.ServerID == change.Member.ServerID })
	d.snapshot.CommitIndex++
	d.snapshot.AppliedIndex++
	if d.loseReply {
		return errors.New("accepted command response lost")
	}
	return nil
}

type memberFake struct {
	prepared  workload.Preparation
	starts    int
	loseReply bool
}

func (m *memberFake) Prepare(context.Context, workload.Request) (workload.Preparation, error) {
	return m.prepared, nil
}

func (m *memberFake) Start(_ context.Context, ref workload.Reference, launch central.Request) (workload.Reference, error) {
	if ref != m.prepared.Record || !reflect.DeepEqual(launch.Prepared, *m.prepared.Result) {
		return workload.Reference{}, errors.New("incorrect durable startup identities")
	}
	m.starts++
	if m.loseReply {
		return workload.Reference{}, errors.New("runtime create response lost")
	}
	return workload.Reference{Name: ref.Name, UID: "new-workload-uid"}, nil
}

type approvalFake struct {
	reserved  int
	completed [][]ovnadmin.Snapshot
}

func (a *approvalFake) Reserve(context.Context, membership.ApprovalReference, string) error {
	a.reserved++
	return nil
}

func (a *approvalFake) Complete(_ context.Context, _ membership.ApprovalReference, _ string, observations []ovnadmin.Snapshot) error {
	a.completed = append(a.completed, observations)
	return nil
}

type fixture struct {
	kube         client.Client
	installation *api.KubeOVNInstallation
	request      Request
	dbs          [2]*databaseFake
	members      memberFake
	approvals    approvalFake
	inventories  [2]Inventory
	failUpdate   func(client.Object) bool
	failCreate   func(client.Object) bool
}

func setup(t *testing.T) *fixture {
	t.Helper()
	f := new(fixture)
	f.installation = &api.KubeOVNInstallation{Name: "ovn", Namespace: "test", UID: "installation-uid", Generation: 1,
		Spec: api.KubeOVNInstallationSpec{Mode: api.InstallationModeControlPlaneOnly, Central: api.CentralSpec{Mode: api.CentralModeRaft, Replicas: 3}}}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, clientgoscheme.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	f.kube = fake.NewClientBuilder().WithScheme(scheme).WithObjects(f.installation).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
				object.SetUID(types.UID(uuid.New().String()))
				if err := c.Create(ctx, object, opts...); err != nil {
					return err
				}
				if f.failCreate != nil && f.failCreate(object) {
					return errors.New("accepted create response lost")
				}
				return nil
			},
			Update: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.UpdateOption) error {
				if f.failUpdate != nil && f.failUpdate(object) {
					return errors.New("journal write failed")
				}
				return c.Update(ctx, object, opts...)
			},
		}).Build()
	f.request = Request{FormatVersion: 1, InstallationUID: string(f.installation.UID), InstallationGeneration: 1, OperationID: uuid.New().String(), RemoveEndpoint: "old-0"}
	plan := prepare.Plan{FormatVersion: 1, InstallationUID: f.request.InstallationUID, OperationID: f.request.OperationID, MemberName: "replacement"}
	for i, name := range databaseNames {
		cid := uuid.New().String()
		inventory := Inventory{Database: name, ClusterID: cid}
		snapshot := ovnadmin.Snapshot{Database: name, ClusterID: cid, Term: 1, CommitIndex: 10, AppliedIndex: 10}
		prepared := prepare.Database{Name: name, ClusterID: cid, LocalAddress: fmt.Sprintf("tcp:192.0.2.4:%d", 6643+i)}
		for j, endpoint := range []string{"a-0", "b-0", "old-0"} {
			peer := Peer{Name: endpoint, ServerID: uuid.New().String(), Address: fmt.Sprintf("tcp:192.0.2.%d:%d", j+1, 6643+i), ClientAddress: fmt.Sprintf("tcp:192.0.2.%d:%d", j+1, 6641+i)}
			inventory.Peers = append(inventory.Peers, peer)
			prepared.RemoteAddresses = append(prepared.RemoteAddresses, peer.Address)
			snapshot.Members = append(snapshot.Members, membership.Member{ServerID: peer.ServerID, Address: peer.Address, Healthy: true, Leader: j == 0})
		}
		snapshot.LeaderServerID = snapshot.Members[0].ServerID
		f.request.Databases = append(f.request.Databases, inventory)
		f.dbs[i] = &databaseFake{snapshot: snapshot, backups: map[string]ovnadmin.Snapshot{}}
		plan.Databases = append(plan.Databases, prepared)
	}
	work := workload.Request{InstallationGeneration: 1, Preparation: plan, Node: workload.Reference{Name: "new-node", UID: "node-uid"}, Claim: workload.Reference{Name: "data", UID: "pvc-uid"}, Volume: workload.Reference{Name: "pv", UID: "pv-uid"}, Image: "example/central@sha256:" + strings.Repeat("a", 64)}
	f.request.Add = &Addition{Workload: work, NorthboundAddress: "tcp:192.0.2.4:6641", SouthboundAddress: "tcp:192.0.2.4:6642"}
	_, digest, err := plan.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	result := &prepare.Result{PlanDigest: digest}
	for i, db := range plan.Databases {
		file := []string{"ovnnb_db.db", "ovnsb_db.db"}[i]
		result.Databases = append(result.Databases, prepare.PreparedDatabase{Name: db.Name, ClusterID: db.ClusterID, Address: db.LocalAddress, ServerID: uuid.New().String(), Path: plan.OperationID + "/" + file})
	}
	f.members.prepared = workload.Preparation{Record: workload.Reference{Name: "replacement", UID: "record-uid"}, Result: result}
	set := &appsv1.StatefulSet{Name: "old", Namespace: "test", Labels: map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: "ovn", apply.InstallationUIDLabel: string(f.installation.UID), apply.InstallationNamespaceLabel: "test"},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: api.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: "ovn", UID: f.installation.UID, Controller: new(true)}},
		Spec:            appsv1.StatefulSetSpec{Replicas: new(int32(1)), UpdateStrategy: appsv1.StatefulSetUpdateStrategy{Type: appsv1.OnDeleteStatefulSetStrategyType}, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ovn-central", Image: work.Image}}}}}}
	if err := f.kube.Create(t.Context(), set); err != nil {
		t.Fatal(err)
	}
	retire, err := PinRetirement(t.Context(), f.kube, f.installation, workload.Reference{Name: set.Name, UID: set.UID})
	if err != nil {
		t.Fatal(err)
	}
	f.request.Retirement = &retire
	return f
}

func (f *fixture) executor(t *testing.T) *Executor {
	t.Helper()
	e, err := New(f.kube, f.installation, Dependencies{Members: &f.members, Approvals: &f.approvals, Database: func(_ context.Context, inventory Inventory) (Database, error) {
		i := slices.Index(databaseNames[:], inventory.Database)
		f.inventories[i] = inventory
		return f.dbs[i], nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (f *fixture) guarded(t *testing.T, fn func(context.Context) error) error {
	t.Helper()
	m := lease.New(f.kube)
	return m.With(t.Context(), f.installation, lease.Installation, func(ctx context.Context) error { return m.With(ctx, f.installation, lease.Central, fn) })
}

func (f *fixture) begin(t *testing.T) Reference {
	t.Helper()
	var ref Reference
	if err := f.guarded(t, func(ctx context.Context) error {
		var err error
		ref, err = f.executor(t).Begin(ctx, f.request)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return ref
}

func (f *fixture) step(t *testing.T, ref Reference) (State, error) {
	t.Helper()
	var state State
	err := f.guarded(t, func(ctx context.Context) error { var err error; state, err = f.executor(t).Step(ctx, ref); return err })
	return state, err
}

func (f *fixture) mustStep(t *testing.T, ref Reference, phase Phase) State {
	t.Helper()
	state, err := f.step(t, ref)
	if err != nil || state.Phase != phase {
		t.Fatalf("phase=%s, want %s: %v", state.Phase, phase, err)
	}
	return state
}

func (f *fixture) join(i int, healthy bool) {
	prepared := f.members.prepared.Result.Databases[i]
	f.dbs[i].snapshot.Members = append(f.dbs[i].snapshot.Members, membership.Member{ServerID: prepared.ServerID, Address: prepared.Address, Healthy: healthy})
}

func (f *fixture) started(t *testing.T) Reference {
	t.Helper()
	ref := f.begin(t)
	f.mustStep(t, ref, Starting)
	f.mustStep(t, ref, CatchingUp)
	return ref
}

func (f *fixture) replicas(t *testing.T) int32 {
	t.Helper()
	set := new(appsv1.StatefulSet)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "old"}, set); err != nil {
		t.Fatal(err)
	}
	return *set.Spec.Replicas
}

func TestJointReplacementResumesAfterPartialJoinAndLostRemovalReply(t *testing.T) {
	f := setup(t)
	ref := f.started(t)
	f.join(0, true)
	if _, err := f.step(t, ref); err == nil {
		t.Fatal("NB-only join authorized removal")
	}
	f.join(1, false)
	if _, err := f.step(t, ref); err == nil {
		t.Fatal("uncaught SB replacement authorized removal")
	}
	if len(f.dbs[0].removals) != 0 || f.replicas(t) != 1 {
		t.Fatal("old member changed before joint convergence")
	}
	f.dbs[1].snapshot.Members[3].Healthy = true
	f.mustStep(t, ref, Removing)
	f.dbs[0].loseReply = true
	if _, err := f.step(t, ref); err == nil {
		t.Fatal("expected uncertain NB dispatch")
	}
	if f.replicas(t) != 1 || len(f.dbs[1].removals) != 0 {
		t.Fatal("partial removal changed SB or retired workload")
	}
	f.mustStep(t, ref, Removing)
	if len(f.dbs[0].removals) != 1 || len(f.dbs[1].removals) != 1 {
		t.Fatal("restart repeated an already converged NB removal")
	}
	f.mustStep(t, ref, Retiring)
	if f.replicas(t) != 1 {
		t.Fatal("convergence alone scaled the workload")
	}
	f.mustStep(t, ref, Retiring)
	if f.replicas(t) != 0 {
		t.Fatal("exact retired workload was not scaled to zero")
	}
	state := f.mustStep(t, ref, Complete)
	if len(state.Final) != 2 || len(state.Final[0].Members) != 3 || len(state.Final[1].Members) != 3 || f.members.starts != 1 {
		t.Fatal("incomplete joint result")
	}
	for _, inventory := range f.inventories {
		if len(inventory.Peers) != 4 {
			t.Fatal("temporary full inventory was lost across reconstruction")
		}
	}
	f.mustStep(t, ref, Complete)
}

func TestUncertainStartPreservesGeneratedIdentities(t *testing.T) {
	f := setup(t)
	ref := f.begin(t)
	f.mustStep(t, ref, Starting)
	f.members.loseReply = true
	if _, err := f.step(t, ref); err == nil {
		t.Fatal("expected lost startup response")
	}
	f.join(0, true)
	f.join(1, true)
	f.members.loseReply = false
	state := f.mustStep(t, ref, CatchingUp)
	if state.MemberRecord != f.members.prepared.Record || !reflect.DeepEqual(state.Prepared, f.members.prepared.Result) {
		t.Fatal("retry changed generated member identities")
	}
}

func TestChangingPeerFenceDuringBackupBlocksStartup(t *testing.T) {
	f := setup(t)
	ref := f.begin(t)
	f.mustStep(t, ref, Starting)
	f.dbs[1].onVerify = func() { f.dbs[0].snapshot.CommitIndex++; f.dbs[0].snapshot.AppliedIndex++ }
	if _, err := f.step(t, ref); err == nil || f.members.starts != 0 {
		t.Fatal("stale NB evidence authorized startup", err)
	}
}

func TestPersistedIntentIsRequiredBeforeRemoval(t *testing.T) {
	f := setup(t)
	ref := f.started(t)
	f.join(0, true)
	f.join(1, true)
	f.mustStep(t, ref, Removing)
	f.failUpdate = func(object client.Object) bool {
		cm, ok := object.(*corev1.ConfigMap)
		return ok && strings.Contains(cm.Data["record.json"], `"intent":`)
	}
	if _, err := f.step(t, ref); err == nil || len(f.dbs[0].removals) != 0 || len(f.dbs[1].removals) != 0 {
		t.Fatal("failed journal write permitted a removal", err)
	}
}

func TestOtherDatabaseFailureAtDispatchBlocksRemoval(t *testing.T) {
	f := setup(t)
	ref := f.started(t)
	for i := range 2 {
		f.join(i, true)
	}
	f.mustStep(t, ref, Removing)
	f.dbs[0].beforeDispatch = func() {
		f.dbs[1].snapshot.Members[3].Healthy = false
	}
	if _, err := f.step(t, ref); err == nil || len(f.dbs[0].removals) != 0 {
		t.Fatal("late loss of SB replacement health authorized NB removal", err)
	}
}

func TestMembershipAndInstallationDriftFailClosed(t *testing.T) {
	for _, which := range []string{"unknown-member", "unexpected-absence", "quorum-loss", "generation", "journal-uid", "request-uid"} {
		t.Run(which, func(t *testing.T) {
			f := setup(t)
			ref := f.started(t)
			f.join(0, true)
			f.join(1, true)
			switch which {
			case "unknown-member":
				f.dbs[1].snapshot.Members[3].ServerID = uuid.New().String()
			case "unexpected-absence":
				f.dbs[0].snapshot.Members = slices.Delete(f.dbs[0].snapshot.Members, 2, 3)
			case "quorum-loss":
				f.dbs[1].snapshot.Members[1].Healthy = false
				f.dbs[1].snapshot.Members[2].Healthy = false
			case "generation":
				installation := f.installation.DeepCopy()
				if err := f.kube.Get(t.Context(), client.ObjectKeyFromObject(installation), installation); err != nil {
					t.Fatal(err)
				}
				installation.Generation++
				if err := f.kube.Update(t.Context(), installation); err != nil {
					t.Fatal(err)
				}
			case "journal-uid":
				ref.Journal.UID = "replacement-journal"
			case "request-uid":
				ref.Request.UID = "replacement-request"
			}
			if _, err := f.step(t, ref); err == nil || f.replicas(t) != 1 || len(f.dbs[0].removals) != 0 {
				t.Fatal("drift authorized lifecycle progress", err)
			}
		})
	}
}

func TestLostTargetNeedsApprovalAndCompletionCoversBothDatabases(t *testing.T) {
	f := setup(t)
	decision := membership.ApprovalReference{Name: "lost-member", UID: "approval-uid"}
	for i := range f.request.Databases {
		f.request.Databases[i].Approval = decision
	}
	ref := f.started(t)
	for i := range 2 {
		f.join(i, true)
		f.dbs[i].snapshot.Members[2].Healthy = false
	}
	f.mustStep(t, ref, Removing)
	f.mustStep(t, ref, Removing)
	if len(f.approvals.completed) != 0 {
		t.Fatal("approval completed after only NB removal")
	}
	f.mustStep(t, ref, Removing)
	f.mustStep(t, ref, Retiring)
	f.mustStep(t, ref, Retiring)
	f.mustStep(t, ref, Complete)
	if f.approvals.reserved != 2 || len(f.approvals.completed) < 1 || len(f.approvals.completed[0]) != 2 {
		t.Fatal("joint approval was not reserved and completed for both targets")
	}
	for _, db := range f.dbs {
		if len(db.removals) != 1 || db.removals[0].Operation != membership.OperationKick {
			t.Fatal("wrong lost-member command")
		}
	}
}

func TestNoApprovalCannotRemoveLostMember(t *testing.T) {
	f := setup(t)
	ref := f.started(t)
	for i := range 2 {
		f.join(i, true)
		f.dbs[i].snapshot.Members[2].Healthy = false
	}
	f.mustStep(t, ref, Removing)
	if _, err := f.step(t, ref); err == nil || len(f.dbs[0].removals) != 0 {
		t.Fatal("lost member removed without approval", err)
	}
}

func TestOneOperationOwnsTheInstallationAndCreateResponseCanBeRecovered(t *testing.T) {
	f := setup(t)
	f.failCreate = func(object client.Object) bool { return object.GetName() == f.executor(t).activeName() }
	if err := f.guarded(t, func(ctx context.Context) error { _, err := f.executor(t).Begin(ctx, f.request); return err }); err == nil {
		t.Fatal("expected lost create response")
	}
	f.failCreate = nil
	ref := f.begin(t)
	if retry := f.begin(t); retry != ref {
		t.Fatal("Begin retry replaced operation")
	}
	other := f.request
	other.OperationID = uuid.New().String()
	other.Add = nil
	if err := f.guarded(t, func(ctx context.Context) error { _, err := f.executor(t).Begin(ctx, other); return err }); err == nil {
		t.Fatal("parallel operation admitted")
	}
	if _, err := f.executor(t).Step(t.Context(), ref); err == nil {
		t.Fatal("unguarded step accepted")
	}
}

func TestCompletionWriteFailureRecoversOriginalReceipt(t *testing.T) {
	f := setup(t)
	ref := f.started(t)
	for i := range 2 {
		f.join(i, true)
	}
	for _, phase := range []Phase{Removing, Removing, Removing, Retiring, Retiring} {
		f.mustStep(t, ref, phase)
	}
	f.failUpdate = func(object client.Object) bool {
		cm, ok := object.(*corev1.ConfigMap)
		return ok && strings.Contains(cm.Data["record.json"], `"phase":"Complete"`)
	}
	if _, err := f.step(t, ref); err == nil {
		t.Fatal("expected journal completion failure")
	}
	f.failUpdate = nil
	for _, db := range f.dbs {
		db.snapshot.CommitIndex++
		db.snapshot.AppliedIndex++
	}
	completed := f.mustStep(t, ref, Complete)
	if completed.Final[0].CommitIndex != f.dbs[0].snapshot.CommitIndex-1 {
		t.Fatal("original completion audit was rewritten")
	}
}

func TestRetirementWaitsForPodsAndRejectsSpecChanges(t *testing.T) {
	f := setup(t)
	ref := f.started(t)
	for i := range 2 {
		f.join(i, true)
	}
	for _, phase := range []Phase{Removing, Removing, Removing, Retiring} {
		f.mustStep(t, ref, phase)
	}
	pod := &corev1.Pod{Name: "old-0", Namespace: "test", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "old", UID: f.request.Retirement.Workload.UID, Controller: new(true)}}}
	if err := f.kube.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	f.mustStep(t, ref, Retiring)
	f.mustStep(t, ref, Retiring)
	set := new(appsv1.StatefulSet)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "old"}, set); err != nil {
		t.Fatal(err)
	}
	set.Spec.Template.Spec.Containers[0].Image = "foreign-image"
	if err := f.kube.Update(t.Context(), set); err != nil {
		t.Fatal(err)
	}
	if _, err := f.step(t, ref); err == nil {
		t.Fatal("retirement accepted changed workload spec")
	}
}

func TestDeploymentRetirementPinsTargetAndScalesOnlyThatPod(t *testing.T) {
	f := setup(t)
	old := new(appsv1.StatefulSet)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "old"}, old); err != nil {
		t.Fatal(err)
	}
	if err := f.kube.Delete(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	controller := true
	labels := map[string]string{"app": "ovn-central"}
	owner := metav1.OwnerReference{APIVersion: api.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: f.installation.Name, UID: f.installation.UID, Controller: &controller}
	deployment := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "old", Namespace: "test", Labels: map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: f.installation.Name, apply.InstallationNamespaceLabel: f.installation.Namespace, apply.InstallationUIDLabel: string(f.installation.UID)}, OwnerReferences: []metav1.OwnerReference{owner}}, Spec: appsv1.DeploymentSpec{Replicas: new(int32(2)), Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: corev1.PodSpec{HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: f.request.Add.Workload.Image}}}}}}
	if err := f.kube.Create(t.Context(), deployment); err != nil {
		t.Fatal(err)
	}
	deploymentOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: &controller}
	replicaSet := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "old-rs", Namespace: "test", OwnerReferences: []metav1.OwnerReference{deploymentOwner}}, Spec: appsv1.ReplicaSetSpec{Replicas: new(int32(2)), Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: deployment.Spec.Template}}
	if err := f.kube.Create(t.Context(), replicaSet); err != nil {
		t.Fatal(err)
	}
	podOwner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: replicaSet.Name, UID: replicaSet.UID, Controller: &controller}
	ready := corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}
	for _, pod := range []*corev1.Pod{{ObjectMeta: metav1.ObjectMeta{Name: "old-0", Namespace: "test", OwnerReferences: []metav1.OwnerReference{podOwner}}, Spec: corev1.PodSpec{NodeName: "node-a", HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: f.request.Add.Workload.Image}}}, Status: ready}, {ObjectMeta: metav1.ObjectMeta{Name: "old-1", Namespace: "test", OwnerReferences: []metav1.OwnerReference{podOwner}}, Spec: corev1.PodSpec{NodeName: "node-b", HostNetwork: true, Containers: []corev1.Container{{Name: "ovn-central", Image: f.request.Add.Workload.Image}}}, Status: ready}} {
		if err := f.kube.Create(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
	}
	targetPod := new(corev1.Pod)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "old-0"}, targetPod); err != nil {
		t.Fatal(err)
	}
	retirement, err := PinDeploymentRetirement(t.Context(), f.kube, f.installation, workload.Reference{Name: "old", UID: deployment.UID}, workload.Reference{Name: "old-0", UID: targetPod.UID})
	if err != nil {
		t.Fatal(err)
	}
	f.request.Retirement = &retirement
	ref := f.started(t)
	for i := range 2 {
		f.join(i, true)
	}
	f.mustStep(t, ref, Removing)
	f.mustStep(t, ref, Removing)
	f.mustStep(t, ref, Removing)
	f.mustStep(t, ref, Retiring)
	f.mustStep(t, ref, Retiring)
	current := new(appsv1.Deployment)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "old"}, current); err != nil {
		t.Fatal(err)
	}
	if current.Spec.Replicas == nil || *current.Spec.Replicas != 1 {
		t.Fatalf("Deployment was not scaled by one: %v", current.Spec.Replicas)
	}
	target := new(corev1.Pod)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "old-0"}, target); err != nil {
		t.Fatal(err)
	}
	if target.Annotations[podDeletionCostAnnotation] != strconv.Itoa(minimumPodDeletionCost) {
		t.Fatalf("target Pod was not prioritized: %#v", target.Annotations)
	}
	if err := f.kube.Delete(t.Context(), target); err != nil {
		t.Fatal(err)
	}
	f.mustStep(t, ref, Complete)
}

func TestCanonicalDoesNotMutateCallerAndRejectsIncompleteInventories(t *testing.T) {
	f := setup(t)
	slices.Reverse(f.request.Databases)
	before, _ := json.Marshal(f.request)
	if _, _, err := canonical(f.request); err != nil {
		t.Fatal(err)
	}
	after, _ := json.Marshal(f.request)
	if string(before) != string(after) {
		t.Fatal("canonicalization mutated caller state")
	}
	f.request.Databases[0].Peers = f.request.Databases[0].Peers[:2]
	if _, _, err := canonical(f.request); err == nil {
		t.Fatal("incomplete joint inventory accepted")
	}
}

func TestCompletedOperationCannotDriveOrReplayItsSuccessor(t *testing.T) {
	f := setup(t)
	oldRequest, _, err := canonical(f.request)
	if err != nil {
		t.Fatal(err)
	}
	ref := f.started(t)
	for i := range 2 {
		f.join(i, true)
	}
	for _, phase := range []Phase{Removing, Removing, Removing, Retiring, Retiring, Complete} {
		f.mustStep(t, ref, phase)
	}
	next, _, err := canonical(f.request)
	if err != nil {
		t.Fatal(err)
	}
	next.OperationID = uuid.New().String()
	next.RemoveEndpoint, next.Retirement = "", nil
	next.Add.Workload.Preparation.OperationID = next.OperationID
	next.Add.Workload.Preparation.MemberName = "second-replacement"
	next.Add.NorthboundAddress, next.Add.SouthboundAddress = "tcp:192.0.2.5:6641", "tcp:192.0.2.5:6642"
	for i := range 2 {
		peers := slices.DeleteFunc(next.Databases[i].Peers, func(p Peer) bool { return p.Name == "old-0" })
		prepared := f.members.prepared.Result.Databases[i]
		peers = append(peers, Peer{Name: "replacement-0", ServerID: prepared.ServerID, Address: prepared.Address, ClientAddress: fmt.Sprintf("tcp:192.0.2.4:%d", 6641+i)})
		next.Databases[i].Peers = peers
		db := &next.Add.Workload.Preparation.Databases[i]
		db.LocalAddress = fmt.Sprintf("tcp:192.0.2.5:%d", 6643+i)
		db.RemoteAddresses = nil
		for _, peer := range peers {
			db.RemoteAddresses = append(db.RemoteAddresses, peer.Address)
		}
	}
	f.request = next
	successor := f.begin(t)
	if successor.Journal != ref.Journal || successor.Request == ref.Request {
		t.Fatal("successor did not pin a new immutable request")
	}
	if _, err := f.step(t, ref); err == nil {
		t.Fatal("old reference drove the new operation")
	}
	if err := f.guarded(t, func(ctx context.Context) error { _, err := f.executor(t).Begin(ctx, oldRequest); return err }); err == nil {
		t.Fatal("completed operation replayed")
	}
}

func TestInspectRequiresImmutableCompletionReceipt(t *testing.T) {
	f := setup(t)
	ref := f.started(t)
	for i := range 2 {
		f.join(i, true)
	}
	for _, phase := range []Phase{Removing, Removing, Removing, Retiring, Retiring, Complete} {
		f.mustStep(t, ref, phase)
	}
	var operation *Operation
	err := f.guarded(t, func(ctx context.Context) error {
		var err error
		operation, err = Inspect(ctx, f.kube, f.installation, &ref)
		return err
	})
	if err != nil || operation == nil || operation.Completion == nil {
		t.Fatalf("completed operation was not inspected with its receipt: %+v %v", operation, err)
	}
	receipt := new(corev1.ConfigMap)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: f.installation.Namespace, Name: ref.Request.Name + "-complete"}, receipt); err != nil {
		t.Fatal(err)
	}
	if err := f.kube.Delete(t.Context(), receipt); err != nil {
		t.Fatal(err)
	}
	if err := f.guarded(t, func(ctx context.Context) error {
		_, err := Inspect(ctx, f.kube, f.installation, &ref)
		return err
	}); err == nil {
		t.Fatal("completed journal was accepted without its immutable completion receipt")
	}
}

func TestAdditionOnlyRequiresBothNewMembersHealthy(t *testing.T) {
	f := setup(t)
	f.request.RemoveEndpoint, f.request.Retirement = "", nil
	ref := f.started(t)
	f.join(0, true)
	f.join(1, false)
	if _, err := f.step(t, ref); err == nil {
		t.Fatal("addition-only operation completed with an unhealthy SB member")
	}
	f.dbs[1].snapshot.Members[3].Healthy = true
	for _, phase := range []Phase{Removing, Retiring, Complete} {
		f.mustStep(t, ref, phase)
	}
	if f.replicas(t) != 1 || len(f.dbs[0].removals) != 0 {
		t.Fatal("addition-only operation retired an existing member")
	}
}

func TestRemovalOnlyKeepsIntermediateReplicaCount(t *testing.T) {
	f := setup(t)
	f.request.Add = nil
	ref := f.begin(t)
	for _, phase := range []Phase{Removing, Removing, Retiring, Retiring, Complete} {
		f.mustStep(t, ref, phase)
	}
	if len(f.dbs[0].snapshot.Members) != 2 || f.members.starts != 0 {
		t.Fatal("removal-only transition incorrectly used final spec replicas or started a member")
	}
}
