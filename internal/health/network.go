package health

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"maps"
	"net/netip"
	"slices"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/probe"
	"golang.org/x/sync/errgroup"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type NetworkReport struct {
	Condition metav1.Condition
	Evidence  *operatorv1alpha1.NetworkStatus
}

type NetworkProvider interface {
	Observe(context.Context, *operatorv1alpha1.KubeOVNInstallation, []operatorv1alpha1.DataPlaneNodeStatus) NetworkReport
}

type NetworkObserver struct {
	Reader client.Reader
	Runner CommandRunner
}

func (o *NetworkObserver) Observe(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, nodes []operatorv1alpha1.DataPlaneNodeStatus) NetworkReport {
	report := NetworkReport{Condition: metav1.Condition{Type: "NetworkReady", Status: metav1.ConditionUnknown, Reason: "ProviderUnavailable", Message: "Configure spec.networkProbe.image to enable network verification"}}
	if installation.Spec.Mode == operatorv1alpha1.InstallationModeControlPlaneOnly {
		report.Condition.Status, report.Condition.Reason, report.Condition.Message = metav1.ConditionTrue, "NotRequired", "This installation does not manage a data plane"
		return report
	}
	if installation.Spec.NetworkProbe == nil {
		return report
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	evidence, err := o.observeNetwork(ctx, installation, nodes)
	if err != nil {
		report.Condition.Status, report.Condition.Reason, report.Condition.Message = metav1.ConditionFalse, "NetworkUnhealthy", err.Error()
		return report
	}
	report.Condition.Status, report.Condition.Reason = metav1.ConditionTrue, "NetworkProbesPassed"
	report.Condition.Message = "Every managed OVS node passed same-node Pod IP, cross-node ring, Service IP and Service DNS probes for every observed address family"
	report.Evidence = evidence
	return report
}

type networkSnapshot struct {
	clients, servers []corev1.Pod
	daemonSets       []*appsv1.DaemonSet
	service          *corev1.Service
	addresses        map[string]string
	families         []string
}

func (o *NetworkObserver) snapshot(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, nodes []operatorv1alpha1.DataPlaneNodeStatus) (networkSnapshot, error) {
	result := networkSnapshot{}
	if o.Reader == nil || o.Runner == nil || len(nodes) == 0 {
		return result, fmt.Errorf("network verification requires a reader, runner and observed OVS nodes")
	}
	expected := make(map[string]string, len(nodes))
	for _, node := range nodes {
		if node.Node == "" || node.NodeUID == "" || expected[node.Node] != "" {
			return result, fmt.Errorf("network verification requires unique observed node identities")
		}
		expected[node.Node] = node.NodeUID
		current := new(corev1.Node)
		if err := o.Reader.Get(ctx, types.NamespacedName{Name: node.Node}, current); err != nil {
			return result, err
		}
		if string(current.UID) != node.NodeUID || !nodeReady(*current) {
			return result, fmt.Errorf("network probe node identity or readiness changed")
		}
	}
	name := probe.Name(string(installation.UID))
	var families []string
	for _, role := range []struct{ name, app string }{{"client", probe.ClientApp}, {"server", probe.ServerApp}} {
		ds := new(appsv1.DaemonSet)
		if err := o.Reader.Get(ctx, types.NamespacedName{Namespace: installation.Namespace, Name: name + "-" + role.name}, ds); err != nil {
			return result, err
		}
		s := ds.Status
		if !ownedWorkload(ds, installation) || ds.DeletionTimestamp != nil || s.ObservedGeneration < ds.Generation || s.DesiredNumberScheduled != int32(len(nodes)) || s.UpdatedNumberScheduled != s.DesiredNumberScheduled || s.NumberReady != s.DesiredNumberScheduled || s.NumberAvailable != s.DesiredNumberScheduled || s.NumberMisscheduled != 0 {
			return result, fmt.Errorf("network probe %s DaemonSet is not fully owned and ready", role.name)
		}
		pods, err := ownedPods(ctx, o.Reader, installation, role.app)
		if err != nil {
			return result, err
		}
		if len(pods) != len(nodes) {
			return result, fmt.Errorf("network probe %s pods do not cover all OVS nodes", role.name)
		}
		seen := make(map[string]bool)
		for _, pod := range pods {
			owner := metav1.GetControllerOf(&pod)
			if owner == nil || owner.UID != ds.UID || owner.Kind != "DaemonSet" || expected[pod.Spec.NodeName] == "" || seen[pod.Spec.NodeName] || pod.Spec.HostNetwork || pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
				return result, fmt.Errorf("network probe pod identity, scheduling or readiness is invalid")
			}
			index := slices.IndexFunc(pod.Spec.Containers, func(container corev1.Container) bool { return container.Name == probe.Container })
			if index < 0 || pod.Spec.Containers[index].Image != installation.Spec.NetworkProbe.Image {
				return result, fmt.Errorf("network probe pod image differs from the requested image")
			}
			seen[pod.Spec.NodeName] = true
			ips, err := podAddresses(pod)
			if err != nil {
				return result, err
			}
			currentFamilies := slices.Sorted(maps.Keys(ips))
			if families == nil {
				families = currentFamilies
			}
			if !slices.Equal(families, currentFamilies) {
				return result, fmt.Errorf("network probe pods disagree on address families")
			}
		}
		result.daemonSets = append(result.daemonSets, ds)
		if role.name == "client" {
			result.clients = pods
		} else {
			result.servers = pods
		}
	}
	service := new(corev1.Service)
	if err := o.Reader.Get(ctx, types.NamespacedName{Namespace: installation.Namespace, Name: name}, service); err != nil {
		return result, err
	}
	selector := map[string]string{"app": probe.ServerApp, "operator.kubeovn.io/probe-installation": string(installation.UID)}
	if !ownedWorkload(service, installation) || service.DeletionTimestamp != nil || service.Spec.Type != corev1.ServiceTypeClusterIP || !maps.Equal(service.Spec.Selector, selector) || len(service.Spec.Ports) != 1 || service.Spec.Ports[0].Port != probe.Port || service.Spec.Ports[0].TargetPort.IntValue() != probe.Port || service.Spec.Ports[0].Protocol != corev1.ProtocolTCP {
		return result, fmt.Errorf("network probe Service identity or routing configuration is invalid")
	}
	addresses := make(map[string]string)
	for _, address := range service.Spec.ClusterIPs {
		family, err := addressFamily(address)
		if err != nil || addresses[family] != "" {
			return result, fmt.Errorf("invalid network probe Service addresses")
		}
		addresses[family] = address
	}
	if !slices.Equal(families, slices.Sorted(maps.Keys(addresses))) {
		return result, fmt.Errorf("probe Service and pods disagree on address families")
	}
	result.service, result.addresses, result.families = service, addresses, families
	return result, nil
}

type networkCheck struct {
	source      corev1.Pod
	destination *corev1.Pod
	request     probe.Request
	kind        string
}

func (o *NetworkObserver) observeNetwork(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, nodes []operatorv1alpha1.DataPlaneNodeStatus) (*operatorv1alpha1.NetworkStatus, error) {
	before, err := o.snapshot(ctx, installation, nodes)
	if err != nil {
		return nil, err
	}
	servers := make(map[string]corev1.Pod)
	for _, pod := range before.servers {
		servers[pod.Spec.NodeName] = pod
	}
	nodeNames := slices.Sorted(maps.Keys(servers))
	var checks []networkCheck
	for _, source := range before.clients {
		sameNode := servers[source.Spec.NodeName]
		nextNode := servers[nodeNames[(slices.Index(nodeNames, source.Spec.NodeName)+1)%len(nodeNames)]]
		for _, family := range before.families {
			for _, target := range []struct {
				kind string
				pod  corev1.Pod
			}{{"same-node Pod", sameNode}, {"cross-node Pod", nextNode}} {
				if target.kind == "cross-node Pod" && len(nodeNames) == 1 {
					continue
				}
				addresses, _ := podAddresses(target.pod)
				checks = append(checks, networkCheck{source: source, destination: new(target.pod), kind: target.kind, request: probe.Request{Host: addresses[family], ExpectedAddress: addresses[family], Family: family}})
			}
			for _, target := range []struct{ kind, host string }{{"Service IP", before.addresses[family]}, {"Service DNS", before.service.Name + "." + installation.Namespace + ".svc"}} {
				checks = append(checks, networkCheck{source: source, kind: target.kind, request: probe.Request{Host: target.host, ExpectedAddress: before.addresses[family], Family: family}})
			}
		}
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(8)
	for _, check := range checks {
		group.Go(func() error {
			var nonce [16]byte
			if _, err := rand.Read(nonce[:]); err != nil {
				return err
			}
			check.request.Nonce = hex.EncodeToString(nonce[:])
			if err := o.check(groupCtx, installation.Namespace, check, before.servers); err != nil {
				return fmt.Errorf("%s %s probe from node %s: %w", check.request.Family, check.kind, check.source.Spec.NodeName, err)
			}
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	after, err := o.snapshot(ctx, installation, nodes)
	if err != nil {
		return nil, err
	}
	if !samePods(before.clients, after.clients) || !samePods(before.servers, after.servers) || before.service.UID != after.service.UID || !equality.Semantic.DeepEqual(before.service.Spec, after.service.Spec) {
		return nil, fmt.Errorf("network probe resources changed during observation")
	}
	for i, ds := range before.daemonSets {
		if ds.UID != after.daemonSets[i].UID || ds.Generation != after.daemonSets[i].Generation {
			return nil, fmt.Errorf("network probe rollout changed during observation")
		}
	}
	for _, pair := range [][2][]corev1.Pod{{before.clients, after.clients}, {before.servers, after.servers}} {
		for i, pod := range pair[0] {
			if !slices.Equal(pod.Status.PodIPs, pair[1][i].Status.PodIPs) {
				return nil, fmt.Errorf("network probe addresses changed during observation")
			}
		}
	}
	return &operatorv1alpha1.NetworkStatus{ObservedNodes: int32(len(nodes)), AddressFamilies: before.families, SuccessfulProbes: int32(len(checks))}, nil
}

func (o *NetworkObserver) check(ctx context.Context, namespace string, check networkCheck, servers []corev1.Pod) error {
	request, err := json.Marshal(check.request)
	if err != nil {
		return err
	}
	data, err := o.Runner.Run(ctx, namespace, check.source.Name, probe.Container, []string{"/network-probe", "check", string(request)})
	if err != nil {
		return fmt.Errorf("execute probe: %w", err)
	}
	var response probe.Response
	if err := json.Unmarshal(data, &response, json.RejectUnknownMembers(true)); err != nil {
		return fmt.Errorf("invalid probe result")
	}
	identity := func(pod corev1.Pod) probe.Identity {
		return probe.Identity{Pod: pod.Name, PodUID: string(pod.UID), Node: pod.Spec.NodeName}
	}
	if response.Version != probe.ProtocolVersion || response.Nonce != check.request.Nonce || response.Address != check.request.ExpectedAddress || response.Source != identity(check.source) {
		return fmt.Errorf("probe source, address or nonce mismatch")
	}
	if check.destination != nil {
		if response.Destination != identity(*check.destination) {
			return fmt.Errorf("probe destination identity mismatch")
		}
	} else if !slices.ContainsFunc(servers, func(pod corev1.Pod) bool { return identity(pod) == response.Destination }) {
		return fmt.Errorf("service probe reached an unobserved backend")
	}
	return nil
}

func addressFamily(value string) (string, error) {
	address, err := netip.ParseAddr(value)
	if err != nil || address.Is4In6() || address.Zone() != "" || !address.IsGlobalUnicast() || address.IsLoopback() || address.IsLinkLocalUnicast() {
		return "", fmt.Errorf("invalid network probe address")
	}
	if address.Is4() {
		return "IPv4", nil
	}
	return "IPv6", nil
}

func podAddresses(pod corev1.Pod) (map[string]string, error) {
	result := make(map[string]string)
	for _, address := range pod.Status.PodIPs {
		family, err := addressFamily(address.IP)
		if err != nil || result[family] != "" {
			return nil, fmt.Errorf("invalid network probe pod addresses")
		}
		result[family] = address.IP
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("network probe pod has no addresses")
	}
	return result, nil
}
