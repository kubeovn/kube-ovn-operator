package health

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"uuid"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"golang.org/x/sync/errgroup"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type RuntimeReport struct {
	Northd          metav1.Condition
	DataPlane       metav1.Condition
	NorthdProcesses []operatorv1alpha1.NorthdProcessStatus
	Nodes           []operatorv1alpha1.DataPlaneNodeStatus
}

type RuntimeProvider interface {
	Observe(context.Context, *operatorv1alpha1.KubeOVNInstallation) RuntimeReport
}

type RuntimeObserver struct {
	Reader client.Reader
	Runner CommandRunner
}

func (o *RuntimeObserver) Observe(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) RuntimeReport {
	ctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	report := RuntimeReport{
		Northd:    metav1.Condition{Type: "NorthdReady", Status: metav1.ConditionTrue, Reason: "NotRequired", Message: "This installation does not manage northd"},
		DataPlane: metav1.Condition{Type: "DataPlaneReady", Status: metav1.ConditionTrue, Reason: "NotRequired", Message: "This installation does not manage a data plane"},
	}
	if installation.Spec.Mode != operatorv1alpha1.InstallationModeDataPlaneOnly {
		processes, err := o.observeNorthd(ctx, installation)
		if err != nil {
			report.Northd.Status, report.Northd.Reason, report.Northd.Message = metav1.ConditionFalse, "NorthdUnhealthy", err.Error()
		} else {
			report.NorthdProcesses = processes
			report.Northd.Reason, report.Northd.Message = "NorthdHealthy", "Exactly one northd is active and all instances are connected to NB/SB"
		}
	}
	if installation.Spec.Mode != operatorv1alpha1.InstallationModeControlPlaneOnly {
		nodes, err := o.observeDataPlane(ctx, installation)
		if err != nil {
			report.DataPlane.Status, report.DataPlane.Reason, report.DataPlane.Message = metav1.ConditionFalse, "DataPlaneUnhealthy", err.Error()
		} else {
			report.Nodes = nodes
			report.DataPlane.Reason, report.DataPlane.Message = "DataPlaneConnected", "All scheduled OVS nodes have unique chassis identities, SB connectivity and an active integration bridge; network probes remain required"
		}
	}
	return report
}

func (o *RuntimeObserver) observeNorthd(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) ([]operatorv1alpha1.NorthdProcessStatus, error) {
	if o.Reader == nil || o.Runner == nil {
		return nil, fmt.Errorf("northd observation requires a reader and command runner")
	}
	pods, err := ownedPods(ctx, o.Reader, installation, "ovn-central")
	if err != nil {
		return nil, err
	}
	if len(pods) == 0 || len(pods) != int(installation.Spec.Central.Replicas) {
		return nil, fmt.Errorf("expected %d owned central pods, found %d", installation.Spec.Central.Replicas, len(pods))
	}
	var states []operatorv1alpha1.NorthdProcessStatus
	active := 0
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
			return nil, fmt.Errorf("central pod %s is not ready", pod.Name)
		}
		data, err := o.Runner.Run(ctx, pod.Namespace, pod.Name, "ovn-central", []string{"ovn-appctl", "--timeout=5", "-t", "ovn-northd", "status"})
		if err != nil {
			return nil, fmt.Errorf("read northd status on %s: %w", pod.Name, err)
		}
		state, err := parseNorthdState(data)
		if err != nil {
			return nil, fmt.Errorf("northd on %s: %w", pod.Name, err)
		}
		if state == "active" {
			active++
		}
		for _, command := range []string{"nb-connection-status", "sb-connection-status"} {
			data, err := o.Runner.Run(ctx, pod.Namespace, pod.Name, "ovn-central", []string{"ovn-appctl", "--timeout=5", "-t", "ovn-northd", command})
			if err != nil {
				return nil, fmt.Errorf("read northd %s on %s: %w", command, pod.Name, err)
			}
			if !connectionReady(data) {
				return nil, fmt.Errorf("northd %s is not connected on %s", command, pod.Name)
			}
		}
		states = append(states, operatorv1alpha1.NorthdProcessStatus{Pod: pod.Name, State: state, NBConnected: true, SBConnected: true})
	}
	if active != 1 {
		return nil, fmt.Errorf("expected one active northd, observed %d", active)
	}
	current, err := ownedPods(ctx, o.Reader, installation, "ovn-central")
	if err != nil {
		return nil, err
	}
	if !samePods(pods, current) {
		return nil, fmt.Errorf("central pods changed during northd observation")
	}
	return states, nil
}

func (o *RuntimeObserver) observeDataPlane(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) ([]operatorv1alpha1.DataPlaneNodeStatus, error) {
	if o.Reader == nil || o.Runner == nil {
		return nil, fmt.Errorf("data-plane observation requires a reader and command runner")
	}
	daemonSet, err := o.ovsDaemonSet(ctx, installation)
	if err != nil {
		return nil, err
	}
	pods, err := ownedPods(ctx, o.Reader, installation, "ovs")
	if err != nil {
		return nil, err
	}
	if int32(len(pods)) != daemonSet.Status.DesiredNumberScheduled {
		return nil, fmt.Errorf("expected %d OVS pods, observed %d", daemonSet.Status.DesiredNumberScheduled, len(pods))
	}
	nodes := make([]corev1.Node, len(pods))
	names := make(map[string]bool)
	for i, pod := range pods {
		owner := metav1.GetControllerOf(&pod)
		if owner == nil || owner.Kind != "DaemonSet" || owner.UID != daemonSet.UID || pod.Spec.NodeName == "" || names[pod.Spec.NodeName] || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
			return nil, fmt.Errorf("OVS pod ownership, node assignment or readiness is not stable")
		}
		names[pod.Spec.NodeName] = true
		if err := o.Reader.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, &nodes[i]); err != nil {
			return nil, err
		}
		if !nodeReady(nodes[i]) {
			return nil, fmt.Errorf("OVS node %s is not ready", pod.Spec.NodeName)
		}
	}
	states := make([]operatorv1alpha1.DataPlaneNodeStatus, len(pods))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for i, pod := range pods {
		group.Go(func() error {
			state, err := o.observeOVSPod(groupCtx, pod)
			if err != nil {
				return err
			}
			state.NodeUID = string(nodes[i].UID)
			states[i] = state
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	chassis := make(map[string]bool)
	for _, state := range states {
		if chassis[state.ChassisID] {
			return nil, fmt.Errorf("multiple OVS nodes report the same chassis identity")
		}
		chassis[state.ChassisID] = true
	}
	current, err := ownedPods(ctx, o.Reader, installation, "ovs")
	if err != nil {
		return nil, err
	}
	if !samePods(pods, current) {
		return nil, fmt.Errorf("OVS pods changed during data-plane observation")
	}
	currentDaemonSet, err := o.ovsDaemonSet(ctx, installation)
	if err != nil {
		return nil, err
	}
	if currentDaemonSet.UID != daemonSet.UID || currentDaemonSet.Generation != daemonSet.Generation || currentDaemonSet.Status.DesiredNumberScheduled != daemonSet.Status.DesiredNumberScheduled {
		return nil, fmt.Errorf("OVS DaemonSet changed during observation")
	}
	for _, previous := range nodes {
		current := new(corev1.Node)
		if err := o.Reader.Get(ctx, types.NamespacedName{Name: previous.Name}, current); err != nil {
			return nil, err
		}
		if current.UID != previous.UID || !nodeReady(*current) {
			return nil, fmt.Errorf("OVS node %s changed during observation", previous.Name)
		}
	}
	return states, nil
}

func (o *RuntimeObserver) ovsDaemonSet(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) (*appsv1.DaemonSet, error) {
	list := new(appsv1.DaemonSetList)
	if err := o.Reader.List(ctx, list, client.InNamespace(installation.Namespace), client.MatchingLabels{apply.InstallationUIDLabel: string(installation.UID)}); err != nil {
		return nil, err
	}
	var found *appsv1.DaemonSet
	for _, item := range list.Items {
		if item.Spec.Selector == nil || item.Spec.Selector.MatchLabels["app"] != "ovs" || !ownedWorkload(&item, installation) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("multiple owned OVS DaemonSets are ambiguous")
		}
		found = new(item)
	}
	if found == nil {
		return nil, fmt.Errorf("no owned OVS DaemonSet was observed")
	}
	s := found.Status
	if found.DeletionTimestamp != nil || s.DesiredNumberScheduled <= 0 || s.ObservedGeneration < found.Generation || s.UpdatedNumberScheduled != s.DesiredNumberScheduled || s.NumberReady != s.DesiredNumberScheduled || s.NumberAvailable != s.DesiredNumberScheduled || s.NumberMisscheduled != 0 {
		return nil, fmt.Errorf("OVS DaemonSet rollout is incomplete")
	}
	return found, nil
}

func (o *RuntimeObserver) observeOVSPod(ctx context.Context, pod corev1.Pod) (operatorv1alpha1.DataPlaneNodeStatus, error) {
	state := operatorv1alpha1.DataPlaneNodeStatus{Node: pod.Spec.NodeName, Pod: pod.Name}
	data, err := o.Runner.Run(ctx, pod.Namespace, pod.Name, "openvswitch", []string{"ovn-appctl", "--timeout=5", "-t", "ovn-controller", "connection-status"})
	if err != nil {
		return state, fmt.Errorf("read SB connection on %s: %w", pod.Spec.NodeName, err)
	}
	if !connectionReady(data) {
		return state, fmt.Errorf("ovn-controller is not connected to SB on %s", pod.Spec.NodeName)
	}
	state.SBConnected = true
	data, err = o.Runner.Run(ctx, pod.Namespace, pod.Name, "openvswitch", []string{"ovs-appctl", "--timeout=5", "-t", "ovs-vswitchd", "ofproto/list"})
	if err != nil {
		return state, fmt.Errorf("read integration bridge on %s: %w", pod.Spec.NodeName, err)
	}
	if !integrationBridgeReady(data) {
		return state, fmt.Errorf("OVS integration bridge is not active on %s", pod.Spec.NodeName)
	}
	state.IntegrationBridgeReady = true
	data, err = o.Runner.Run(ctx, pod.Namespace, pod.Name, "openvswitch", []string{"ovs-vsctl", "--timeout=5", "--db=unix:/var/run/openvswitch/db.sock", "get", "Open_vSwitch", ".", "external_ids:system-id"})
	if err != nil {
		return state, fmt.Errorf("read chassis identity on %s: %w", pod.Spec.NodeName, err)
	}
	state.ChassisID, err = parseChassisID(data)
	if err != nil {
		return state, fmt.Errorf("invalid chassis identity on %s", pod.Spec.NodeName)
	}
	return state, nil
}

func parseNorthdState(data []byte) (string, error) {
	state := ""
	for line := range strings.SplitSeq(strings.TrimSpace(string(data)), "\n") {
		if value, found := strings.CutPrefix(line, "Status: "); found {
			if state != "" {
				return "", fmt.Errorf("ambiguous northd status")
			}
			state = strings.TrimSpace(value)
		}
	}
	if state != "active" && state != "standby" {
		return "", fmt.Errorf("northd is not active or standby")
	}
	return state, nil
}

func connectionReady(data []byte) bool { return strings.TrimSpace(string(data)) == "connected" }

func integrationBridgeReady(data []byte) bool {
	count := 0
	for name := range strings.FieldsSeq(string(data)) {
		if name == "br-int" {
			count++
		}
	}
	return count == 1
}

func parseChassisID(data []byte) (string, error) {
	value := strings.TrimSpace(string(data))
	if strings.HasPrefix(value, `"`) {
		var err error
		value, err = strconv.Unquote(value)
		if err != nil {
			return "", err
		}
	}
	id, err := uuid.Parse(value)
	if err != nil || id == (uuid.UUID{}) {
		return "", fmt.Errorf("invalid chassis UUID")
	}
	return id.String(), nil
}

func nodeReady(node corev1.Node) bool {
	if node.DeletionTimestamp != nil {
		return false
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == corev1.NodeReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}
