package health

import (
	"context"
	"fmt"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type runnerFunc func(context.Context, string, string, string, []string) ([]byte, error)

func (f runnerFunc) Run(ctx context.Context, ns, pod, container string, command []string) ([]byte, error) {
	return f(ctx, ns, pod, container, command)
}

func healthFixture(t *testing.T) (*operatorv1alpha1.KubeOVNInstallation, client.Client, []client.Object) {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme, operatorv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "network", Namespace: "kube-system", UID: "installation", Spec: operatorv1alpha1.KubeOVNInstallationSpec{Mode: operatorv1alpha1.InstallationModeFull, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}}}
	deployment := &appsv1.Deployment{Name: "ovn-central", Namespace: installation.Namespace, UID: "deployment", Labels: map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: installation.Name, apply.InstallationNamespaceLabel: installation.Namespace, apply.InstallationUIDLabel: string(installation.UID)}, OwnerReferences: []metav1.OwnerReference{{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: new(true)}}}
	rs := &appsv1.ReplicaSet{Name: "ovn-central-rs", Namespace: installation.Namespace, UID: "replicaset", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: deployment.Name, UID: deployment.UID, Controller: new(true)}}}
	objects := []client.Object{deployment, rs}
	for i := range 3 {
		objects = append(objects, &corev1.Pod{Name: fmt.Sprintf("ovn-central-%d", i), Namespace: installation.Namespace, UID: types.UID(fmt.Sprintf("pod-%d", i)), Labels: map[string]string{"app": "ovn-central"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: new(true)}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}})
	}
	return installation, fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build(), objects
}

func TestOVNObserverReadsBothDatabasesAndUsesOwnerUIDs(t *testing.T) {
	installation, kube, objects := healthFixture(t)
	for index := 0; index < 3; index++ {
		pod := objects[index+2].(*corev1.Pod)
		pod.Spec.NodeName = fmt.Sprintf("node-%d", index)
		if err := kube.Update(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
	}
	calls := 0
	observer := OVNObserver{Reader: kube, Runner: runnerFunc(func(_ context.Context, ns, pod, container string, command []string) ([]byte, error) {
		calls++
		if ns != "kube-system" || container != "ovn-central" {
			t.Fatal("wrong target")
		}
		name := "OVN_Northbound"
		if strings.Contains(command[4], "OVN_Southbound") {
			name = "OVN_Southbound"
		}
		id := fmt.Sprintf("1234567%s-1122-3344-5566-778899aabbcc", strings.TrimPrefix(pod, "ovn-central-"))
		return databaseResponse(t, Database{Name: name, Model: "clustered", Connected: true, Leader: pod == "ovn-central-0", ClusterID: clusterID, ServerID: id, Index: 12}), nil
	})}
	got, err := observer.Observe(t.Context(), installation)
	if err != nil || !got.Ready || calls != 6 || len(got.Members) != 6 || len(got.Databases) != 2 {
		t.Fatalf("report=%+v calls=%d err=%v", got, calls, err)
	}
	for _, member := range got.Members {
		if member.Node == "" {
			t.Fatalf("central member status did not retain its Node identity: %+v", got.Members)
		}
	}
	foreign := &corev1.Pod{Name: "foreign", Namespace: installation.Namespace, Labels: map[string]string{"app": "ovn-central"}}
	if err := kube.Create(t.Context(), foreign); err != nil {
		t.Fatal(err)
	}
	got, err = observer.Observe(t.Context(), installation)
	if err != nil || !got.Ready || calls != 12 {
		t.Fatalf("unowned pod affected result: %+v %v", got, err)
	}
	deployment := new(appsv1.Deployment)
	if err := kube.Get(t.Context(), types.NamespacedName{Name: "ovn-central", Namespace: installation.Namespace}, deployment); err != nil {
		t.Fatal(err)
	}
	deployment.UID = "replacement-deployment"
	if err := kube.Update(t.Context(), deployment); err != nil {
		t.Fatal(err)
	}
	got, err = observer.Observe(t.Context(), installation)
	if err != nil || got.Ready || got.Reason != "CentralPodsPending" || calls != 12 {
		t.Fatalf("stale owner was queried: %+v %v", got, err)
	}
}

func TestOVNObserverDoesNotTreatMissingOrUnreadyPodsAsSuccess(t *testing.T) {
	for _, scenario := range []string{"zero replicas", "unready", "wrong installation", "peer"} {
		t.Run(scenario, func(t *testing.T) {
			installation, kube, objects := healthFixture(t)
			switch scenario {
			case "zero replicas":
				installation.Spec.Central.Replicas = 0
			case "unready":
				pod := objects[2].(*corev1.Pod)
				pod.Status.Conditions[0].Status = corev1.ConditionFalse
				if err := kube.Status().Update(t.Context(), pod); err != nil {
					t.Fatal(err)
				}
			case "wrong installation":
				installation.UID = "other-installation"
			case "peer":
				installation.Spec.Mode = operatorv1alpha1.InstallationModeDataPlaneOnly
			}
			observer := OVNObserver{Reader: kube, Runner: runnerFunc(func(context.Context, string, string, string, []string) ([]byte, error) {
				t.Fatal("unexpected execution")
				return nil, nil
			})}
			got, err := observer.Observe(t.Context(), installation)
			if err != nil || scenario != "peer" && got.Ready {
				t.Fatalf("unexpected report: %+v %v", got, err)
			}
			if scenario == "peer" && (got.Reason != "NotRequired" || got.Message == "") {
				t.Fatalf("external OVN was not marked not-required: %+v", got)
			}
		})
	}
}

func TestOVNObserverFailsWhenSouthboundCannotBeRead(t *testing.T) {
	installation, kube, _ := healthFixture(t)
	observer := OVNObserver{Reader: kube, Runner: runnerFunc(func(_ context.Context, _, pod, _ string, command []string) ([]byte, error) {
		if strings.Contains(command[4], "OVN_Southbound") {
			return nil, fmt.Errorf("permission denied")
		}
		id := fmt.Sprintf("1234567%s-1122-3344-5566-778899aabbcc", strings.TrimPrefix(pod, "ovn-central-"))
		return databaseResponse(t, Database{Name: "OVN_Northbound", Model: "clustered", Connected: true, Leader: pod == "ovn-central-0", ClusterID: clusterID, ServerID: id, Index: 12}), nil
	})}
	got, err := observer.Observe(t.Context(), installation)
	if err == nil || got.Ready || len(got.Members) != 0 {
		t.Fatalf("partial read accepted: %+v %v", got, err)
	}
}

func TestOVNObserverStandaloneStatefulSetAndPodReplacement(t *testing.T) {
	installation, kube, objects := healthFixture(t)
	installation.Spec.Central.Mode = operatorv1alpha1.CentralModeSingle
	installation.Spec.Central.Replicas = 1
	deployment := objects[0].(*appsv1.Deployment)
	statefulSet := &appsv1.StatefulSet{Name: "central-single", Namespace: installation.Namespace, UID: "statefulset", Labels: deployment.Labels, OwnerReferences: deployment.OwnerReferences}
	if err := kube.Create(t.Context(), statefulSet); err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{Name: "central-single-0", Namespace: installation.Namespace, UID: "single-pod", Labels: map[string]string{"app": "ovn-central"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: statefulSet.Name, UID: statefulSet.UID, Controller: new(true)}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
	if err := kube.Create(t.Context(), pod); err != nil {
		t.Fatal(err)
	}
	// Existing RAFT pods belong to another installation after its identity changes.
	deployment.OwnerReferences[0].UID = "other-installation"
	if err := kube.Update(t.Context(), deployment); err != nil {
		t.Fatal(err)
	}
	replace := false
	observer := OVNObserver{Reader: kube, Runner: runnerFunc(func(_ context.Context, _, name, _ string, command []string) ([]byte, error) {
		if name != pod.Name {
			t.Fatalf("unexpected pod %s", name)
		}
		if replace {
			replace = false
			pod.UID = "replacement-pod"
			if err := kube.Update(t.Context(), pod); err != nil {
				t.Fatal(err)
			}
		}
		database := "OVN_Northbound"
		if strings.Contains(command[4], "OVN_Southbound") {
			database = "OVN_Southbound"
		}
		return databaseResponse(t, Database{Name: database, Model: "standalone", Connected: true, Leader: true}), nil
	})}
	got, err := observer.Observe(t.Context(), installation)
	if err != nil || !got.Ready {
		t.Fatalf("standalone observation: %+v %v", got, err)
	}
	replace = true
	got, err = observer.Observe(t.Context(), installation)
	if err != nil || got.Ready || got.Reason != "CentralPodsChanged" || len(got.Members) != 0 {
		t.Fatalf("pod replacement inherited stale health: %+v %v", got, err)
	}
}
