package workload

import (
	"context"
	"encoding/json/v2"
	"errors"
	"reflect"
	"strings"
	"testing"
	"uuid"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/central"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type fixture struct {
	kube         client.Client
	p            *Provisioner
	installation *api.KubeOVNInstallation
	request      Request
}

func setup(t *testing.T, intercept func(context.Context, client.WithWatch, client.Object, ...client.CreateOption) error) fixture {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, appsv1.AddToScheme, batchv1.AddToScheme, coordinationv1.AddToScheme, corev1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	installation := &api.KubeOVNInstallation{Name: "ovn", Namespace: "test", UID: "installation-uid", Generation: 1}
	node := &corev1.Node{Name: "central-4", UID: "node-uid", Status: corev1.NodeStatus{
		Addresses:  []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.4"}},
		Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
	}}
	claim := &corev1.PersistentVolumeClaim{Name: "central-data", Namespace: "test", UID: "claim-uid",
		Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "central-pv"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	volume := &corev1.PersistentVolume{Name: "central-pv", UID: "volume-uid",
		Spec:   corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: claim.Name, Namespace: claim.Namespace, UID: claim.UID}},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(installation, node, claim, volume).
		WithStatusSubresource(&batchv1.Job{}, &corev1.Pod{}, node, claim, volume).
		WithInterceptorFuncs(interceptor.Funcs{Create: func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
			object.SetUID(types.UID(uuid.New().String()))
			if intercept != nil {
				return intercept(ctx, c, object, opts...)
			}
			return c.Create(ctx, object, opts...)
		}}).Build()
	p, err := New(kube, installation)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{InstallationGeneration: installation.Generation, Node: Reference{Name: node.Name, UID: node.UID}, Claim: Reference{Name: claim.Name, UID: claim.UID}, Volume: Reference{Name: volume.Name, UID: volume.UID},
		Image: "ghcr.io/kubeovn/central@sha256:" + strings.Repeat("a", 64),
		Preparation: prepare.Plan{FormatVersion: 1, InstallationUID: string(installation.UID), OperationID: uuid.New().String(), MemberName: "central-4", Databases: []prepare.Database{
			{Name: "OVN_Northbound", ClusterID: uuid.New().String(), LocalAddress: "tcp:192.0.2.4:6643", RemoteAddresses: []string{"tcp:192.0.2.1:6643", "tcp:192.0.2.2:6643", "tcp:192.0.2.3:6643"}},
			{Name: "OVN_Southbound", ClusterID: uuid.New().String(), LocalAddress: "tcp:192.0.2.4:6644", RemoteAddresses: []string{"tcp:192.0.2.1:6644", "tcp:192.0.2.2:6644", "tcp:192.0.2.3:6644"}},
		}}}
	return fixture{kube: kube, p: p, installation: installation, request: request}
}

func (f fixture) guarded(t *testing.T, fn func(context.Context) error) error {
	t.Helper()
	m := lease.New(f.kube)
	return m.With(t.Context(), f.installation, lease.Installation, func(ctx context.Context) error {
		return m.With(ctx, f.installation, lease.Central, fn)
	})
}

func (f fixture) prepare(t *testing.T) Preparation {
	t.Helper()
	var result Preparation
	if err := f.guarded(t, func(ctx context.Context) error { var err error; result, err = f.p.Prepare(ctx, f.request); return err }); err != nil {
		t.Fatal(err)
	}
	return result
}

func (f fixture) complete(t *testing.T, ref Reference) prepare.Result {
	t.Helper()
	job := new(batchv1.Job)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: ref.Name + "-prepare"}, job); err != nil {
		t.Fatal(err)
	}
	plan, digest, err := f.request.Preparation.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	result := prepare.Result{PlanDigest: digest}
	for _, db := range plan.Databases {
		filename := "ovnnb_db.db"
		if db.Name == "OVN_Southbound" {
			filename = "ovnsb_db.db"
		}
		result.Databases = append(result.Databases, prepare.PreparedDatabase{Name: db.Name, ClusterID: db.ClusterID, Address: db.LocalAddress, ServerID: uuid.New().String(), Path: plan.OperationID + "/" + filename})
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{Name: "prepare-result", Namespace: "test", Labels: job.Spec.Template.Labels, Spec: *job.Spec.Template.Spec.DeepCopy(),
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID, Controller: new(true)}},
		Status:          corev1.PodStatus{Phase: corev1.PodSucceeded, ContainerStatuses: []corev1.ContainerStatus{{Name: "prepare", ContainerID: "containerd://prepared", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0, Message: string(encoded)}}}}}}
	if err := f.kube.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	job.Status = batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}
	if err := f.kube.Status().Update(t.Context(), job); err != nil {
		t.Fatal(err)
	}
	return result
}

func (f fixture) launch(result prepare.Result) central.Request {
	return central.Request{FormatVersion: 1, Preparation: f.request.Preparation, Prepared: result,
		NorthboundAddress: "tcp:192.0.2.4:6641", SouthboundAddress: "tcp:192.0.2.4:6642",
		NorthboundRemotes: []string{"tcp:192.0.2.1:6641", "tcp:192.0.2.2:6641", "tcp:192.0.2.3:6641", "tcp:192.0.2.4:6641"},
		SouthboundRemotes: []string{"tcp:192.0.2.1:6642", "tcp:192.0.2.2:6642", "tcp:192.0.2.3:6642", "tcp:192.0.2.4:6642"}}
}

func TestPrepareThenExplicitStartSurvivesRestart(t *testing.T) {
	f := setup(t, nil)
	first := f.prepare(t)
	if first.Result != nil {
		t.Fatal("incomplete Job returned a prepared result")
	}
	sets := new(appsv1.StatefulSetList)
	if err := f.kube.List(t.Context(), sets); err != nil || len(sets.Items) != 0 {
		t.Fatal("Prepare started a server", err)
	}
	planConfig := new(corev1.ConfigMap)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: first.Record.Name + "-plan"}, planConfig); err != nil {
		t.Fatal(err)
	}
	var nativePlan prepare.Plan
	if err := json.Unmarshal([]byte(planConfig.Data["request.json"]), &nativePlan, json.RejectUnknownMembers(true)); err != nil {
		t.Fatal("native helper cannot read projected request", err)
	}
	if !reflect.DeepEqual(nativePlan, f.request.Preparation) {
		t.Fatal("projected native plan differs")
	}
	result := f.complete(t, first.Record)
	f.p, _ = New(f.kube, f.installation)
	second := f.prepare(t)
	if second.Record != first.Record || !reflect.DeepEqual(second.Result, &result) {
		t.Fatal("restart changed prepared identity")
	}
	var started Reference
	if err := f.guarded(t, func(ctx context.Context) error {
		var err error
		started, err = f.p.Start(ctx, first.Record, f.launch(result))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	f.p, _ = New(f.kube, f.installation)
	if err := f.guarded(t, func(ctx context.Context) error {
		again, err := f.p.Start(ctx, first.Record, f.launch(result))
		if again != started {
			t.Fatal("restart replaced workload")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.kube.List(t.Context(), sets); err != nil || len(sets.Items) != 1 {
		t.Fatal("wrong workload count", err)
	}
	set := sets.Items[0]
	if set.Spec.Template.Spec.NodeName != f.request.Node.Name || *set.Spec.Replicas != 1 || set.Spec.UpdateStrategy.Type != appsv1.OnDeleteStatefulSetStrategyType {
		t.Fatal("incorrect pinned workload")
	}
	if set.Spec.Template.Spec.Containers[0].ReadinessProbe != nil {
		t.Fatal("process readiness must not impersonate committed-data readiness")
	}
}

func TestNoStartWithoutDurableResult(t *testing.T) {
	f := setup(t, nil)
	ref := f.prepare(t).Record
	result := f.complete(t, ref)
	// Completing the Job alone does not persist the controller's accepted result.
	if err := f.guarded(t, func(ctx context.Context) error { _, err := f.p.Start(ctx, ref, f.launch(result)); return err }); err == nil {
		t.Fatal("started before accepting native result")
	}
	sets := new(appsv1.StatefulSetList)
	_ = f.kube.List(t.Context(), sets)
	if len(sets.Items) != 0 {
		t.Fatal("unapproved server created")
	}
}

func TestProvisioningRejectsIdentityChanges(t *testing.T) {
	for _, which := range []string{"node", "claim", "volume", "installation", "job", "result", "request", "lease"} {
		t.Run(which, func(t *testing.T) {
			f := setup(t, nil)
			ref := f.prepare(t).Record
			result := f.complete(t, ref)
			f.prepare(t)
			switch which {
			case "node", "claim", "volume", "installation", "job":
				var object client.Object
				switch which {
				case "node":
					object = &corev1.Node{Name: f.request.Node.Name}
				case "claim":
					object = &corev1.PersistentVolumeClaim{Name: f.request.Claim.Name, Namespace: "test"}
				case "volume":
					object = &corev1.PersistentVolume{Name: f.request.Volume.Name}
				case "installation":
					object = &api.KubeOVNInstallation{Name: "ovn", Namespace: "test"}
				case "job":
					object = &batchv1.Job{Name: ref.Name + "-prepare", Namespace: "test"}
				}
				if err := f.kube.Get(t.Context(), client.ObjectKeyFromObject(object), object); err != nil {
					t.Fatal(err)
				}
				object.SetUID("replacement-uid")
				if err := f.kube.Update(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			case "result":
				result.Databases[0].ServerID = uuid.New().String()
			case "request":
				f.request.Preparation.OperationID = uuid.New().String()
			case "lease":
				if _, err := f.p.Start(t.Context(), ref, f.launch(result)); err == nil {
					t.Fatal("unguarded Start accepted")
				}
				return
			}
			if err := f.guarded(t, func(ctx context.Context) error { _, err := f.p.Start(ctx, ref, f.launch(result)); return err }); err == nil {
				t.Fatal("changed identity accepted")
			}
			sets := new(appsv1.StatefulSetList)
			_ = f.kube.List(t.Context(), sets)
			if len(sets.Items) != 0 {
				t.Fatal("server started after identity change")
			}
		})
	}
}

func TestAmbiguousCreationResponseReusesOriginalJob(t *testing.T) {
	lost := false
	f := setup(t, func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
		err := c.Create(ctx, object, opts...)
		options := new(client.CreateOptions).ApplyOptions(opts)
		if _, ok := object.(*batchv1.Job); ok && len(options.DryRun) == 0 && err == nil && !lost {
			lost = true
			return errors.New("response lost after accepted creation")
		}
		return err
	})
	if err := f.guarded(t, func(ctx context.Context) error { _, err := f.p.Prepare(ctx, f.request); return err }); err == nil || !lost {
		t.Fatal("creation failure not injected")
	}
	jobs := new(batchv1.JobList)
	_ = f.kube.List(t.Context(), jobs)
	if len(jobs.Items) != 1 {
		t.Fatal("expected one accepted Job")
	}
	uid := jobs.Items[0].UID
	f.p, _ = New(f.kube, f.installation)
	f.prepare(t)
	_ = f.kube.List(t.Context(), jobs)
	if len(jobs.Items) != 1 || jobs.Items[0].UID != uid {
		t.Fatal("retry regenerated preparation workload")
	}
}

func TestClaimReservationAndForeignConsumer(t *testing.T) {
	for _, which := range []string{"reserved", "consumer", "new-operation", "missing-job", "mutated-job", "truncated-result", "spoofed-pod", "collectable-record"} {
		t.Run(which, func(t *testing.T) {
			f := setup(t, nil)
			ref := f.prepare(t).Record
			switch which {
			case "reserved":
				f.request.Preparation.MemberName = "another-member"
			case "consumer":
				pod := &corev1.Pod{Name: "foreign", Namespace: "test", Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: f.request.Claim.Name}}}}}}
				if err := f.kube.Create(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			case "new-operation":
				f.request.Preparation.OperationID = uuid.New().String()
			case "collectable-record":
				record := new(corev1.ConfigMap)
				if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: ref.Name}, record); err != nil {
					t.Fatal(err)
				}
				record.OwnerReferences = f.p.owner()
				if err := f.kube.Update(t.Context(), record); err != nil {
					t.Fatal(err)
				}
			case "missing-job", "mutated-job":
				job := new(batchv1.Job)
				_ = f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: ref.Name + "-prepare"}, job)
				if which == "missing-job" {
					_ = f.kube.Delete(t.Context(), job)
				} else {
					job.Spec.Template.Spec.Containers[0].Command = []string{"/bin/false"}
					_ = f.kube.Update(t.Context(), job)
				}
			case "truncated-result", "spoofed-pod":
				f.complete(t, ref)
				pod := new(corev1.Pod)
				_ = f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: "prepare-result"}, pod)
				if which == "truncated-result" {
					pod.Status.ContainerStatuses[0].State.Terminated.Message = "{truncated"
					_ = f.kube.Status().Update(t.Context(), pod)
				} else {
					pod.Spec.Containers[0].Image = "foreign:latest"
					_ = f.kube.Update(t.Context(), pod)
				}
			}
			if err := f.guarded(t, func(ctx context.Context) error { _, err := f.p.Prepare(ctx, f.request); return err }); err == nil {
				t.Fatal("unsafe preparation accepted")
			}
		})
	}
}

func TestAdmissionAndLeaseLossBlockWorkloadWrite(t *testing.T) {
	for _, which := range []string{"sidecar", "host-pid", "container-root", "lifecycle", "lease"} {
		t.Run(which, func(t *testing.T) {
			f := setup(t, func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
				job, ok := object.(*batchv1.Job)
				options := new(client.CreateOptions).ApplyOptions(opts)
				if ok && len(options.DryRun) > 0 {
					switch which {
					case "sidecar":
						job.Spec.Template.Spec.Containers = append(job.Spec.Template.Spec.Containers, corev1.Container{Name: "sidecar"})
					case "host-pid":
						job.Spec.Template.Spec.HostPID = true
					case "container-root":
						job.Spec.Template.Spec.Containers[0].SecurityContext.RunAsUser = new(int64(0))
					case "lifecycle":
						job.Spec.Template.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/false"}}}}
					case "lease":
						l := new(coordinationv1.Lease)
						if err := c.Get(ctx, client.ObjectKey{Namespace: "test", Name: lease.Name(lease.Central, "ovn")}, l); err != nil {
							return err
						}
						l.Spec.HolderIdentity = new("another-operation")
						if err := c.Update(ctx, l); err != nil {
							return err
						}
					}
				}
				return c.Create(ctx, object, opts...)
			})
			if err := f.guarded(t, func(ctx context.Context) error { _, err := f.p.Prepare(ctx, f.request); return err }); err == nil {
				t.Fatal("unsafe workload write accepted")
			}
			jobs := new(batchv1.JobList)
			_ = f.kube.List(t.Context(), jobs)
			if len(jobs.Items) != 0 {
				t.Fatal("unsafe Job persisted")
			}
		})
	}
}

func TestStartRecoversAcceptedWorkloadWithLivePod(t *testing.T) {
	lost := false
	f := setup(t, func(ctx context.Context, c client.WithWatch, object client.Object, opts ...client.CreateOption) error {
		err := c.Create(ctx, object, opts...)
		options := new(client.CreateOptions).ApplyOptions(opts)
		set, ok := object.(*appsv1.StatefulSet)
		if ok && len(options.DryRun) == 0 && err == nil && !lost {
			pod := &corev1.Pod{Name: set.Name + "-0", Namespace: set.Namespace, UID: "runtime-pod", Spec: *set.Spec.Template.Spec.DeepCopy(),
				OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: set.Name, UID: set.UID, Controller: new(true)}},
				Status:          corev1.PodStatus{Phase: corev1.PodRunning}}
			if err := c.Create(ctx, pod); err != nil {
				return err
			}
			lost = true
			return errors.New("accepted runtime response lost")
		}
		return err
	})
	ref := f.prepare(t).Record
	result := f.complete(t, ref)
	f.prepare(t)
	if err := f.guarded(t, func(ctx context.Context) error { _, err := f.p.Start(ctx, ref, f.launch(result)); return err }); err == nil || !lost {
		t.Fatal("runtime response loss not injected")
	}
	set := new(appsv1.StatefulSet)
	if err := f.kube.Get(t.Context(), client.ObjectKey{Namespace: "test", Name: ref.Name}, set); err != nil {
		t.Fatal(err)
	}
	uid := set.UID
	f.p, _ = New(f.kube, f.installation)
	if err := f.guarded(t, func(ctx context.Context) error {
		again, err := f.p.Start(ctx, ref, f.launch(result))
		if err == nil && again.UID != uid {
			t.Fatal("retry replaced accepted runtime")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	set.Spec.Template.Spec.Containers[0].Image = "foreign:latest"
	if err := f.kube.Update(t.Context(), set); err != nil {
		t.Fatal(err)
	}
	if err := f.guarded(t, func(ctx context.Context) error { _, err := f.p.Start(ctx, ref, f.launch(result)); return err }); err == nil {
		t.Fatal("changed runtime spec accepted")
	}
}

func TestInstallationGenerationPinnedAcrossRestart(t *testing.T) {
	f := setup(t, nil)
	ref := f.prepare(t).Record
	result := f.complete(t, ref)
	f.prepare(t)
	current := new(api.KubeOVNInstallation)
	if err := f.kube.Get(t.Context(), client.ObjectKeyFromObject(f.installation), current); err != nil {
		t.Fatal(err)
	}
	current.Generation++
	if err := f.kube.Update(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	f.installation = current
	f.p, _ = New(f.kube, current)
	if err := f.guarded(t, func(ctx context.Context) error { _, err := f.p.Start(ctx, ref, f.launch(result)); return err }); err == nil {
		t.Fatal("new installation generation reused old launch authorization")
	}
}
