package health

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/netip"
	"reflect"
	"slices"
	"strings"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CentralTopology contains complete observed identities, not health or mutation
// authorization. Runtime endpoints may change without changing database identity.
type CentralTopology struct {
	Members []CentralIdentity `json:"members"`
}
type ObjectIdentity struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}
type WorkloadIdentity struct {
	ObjectIdentity `json:",inline"`
	Kind           string `json:"kind"`
}
type StorageIdentity struct {
	MountPath string         `json:"mountPath"`
	SubPath   string         `json:"subPath,omitempty"`
	HostPath  string         `json:"hostPath,omitempty"`
	Claim     ObjectIdentity `json:"claim"`
	Volume    ObjectIdentity `json:"volume"`
}
type DatabaseIdentity struct {
	Database      string `json:"database"`
	ClusterID     string `json:"clusterID"`
	ServerID      string `json:"serverID"`
	Address       string `json:"address"`
	ClientAddress string `json:"clientAddress"`
}
type CentralIdentity struct {
	Node        ObjectIdentity      `json:"node"`
	Workload    WorkloadIdentity    `json:"workload"`
	Storage     []StorageIdentity   `json:"storage"`
	Databases   [2]DatabaseIdentity `json:"databases"`
	Pod         ObjectIdentity      `json:"pod"`
	ContainerID string              `json:"containerID"`
}

// EndpointDriftError identifies a RAFT endpoint that is no longer advertised
// by its retained Kubernetes node. A replacement request may acknowledge this
// narrow condition for its exact target; every other topology check remains
// strict so an address change cannot silently become a new baseline.
type EndpointDriftError struct {
	NodeName string
	Address  string
}

func (e *EndpointDriftError) Error() string {
	return fmt.Sprintf("RAFT address %s is not an observed InternalIP of node %s", e.Address, e.NodeName)
}

// NodeLifecycleError identifies a retained central member whose Kubernetes
// node cannot safely remain the operation target. A cordoned node is admitted
// only for its exact explicit replacement request; a deleting or not-ready
// node always blocks discovery.
type NodeLifecycleError struct {
	NodeName      string
	Unschedulable bool
	Deleting      bool
}

func (e *NodeLifecycleError) Error() string {
	switch {
	case e.Deleting:
		return fmt.Sprintf("central node %s is being deleted", e.NodeName)
	case e.Unschedulable:
		return fmt.Sprintf("central node %s is cordoned without an explicit replacement", e.NodeName)
	default:
		return fmt.Sprintf("central node %s is not ready or has no stable identity", e.NodeName)
	}
}

var topologyDatabases = [2]string{"OVN_Northbound", "OVN_Southbound"}

// SameIdentity ignores the originally sampled process endpoint. Pod replacement
// and container restarts require fresh discovery of the same native identities,
// node, workload and storage; they do not establish a new topology baseline.
func (t CentralTopology) SameIdentity(other CentralTopology) bool {
	return slices.EqualFunc(t.Members, other.Members, func(a, b CentralIdentity) bool {
		a.Pod, b.Pod = ObjectIdentity{}, ObjectIdentity{}
		a.ContainerID, b.ContainerID = "", ""
		return reflect.DeepEqual(a, b)
	})
}

// SameIdentityExceptEndpoints verifies that one exact retained member keeps
// every Kubernetes and native database identity while allowing its two RAFT
// and two client addresses to change. The target Pod identity and container
// identity remain pinned; this observation is only for a healthy Node whose
// advertised address has drifted.
func (t CentralTopology) SameIdentityExceptEndpoints(other CentralTopology, targetPod string) (changed bool, same bool) {
	if strings.TrimSpace(targetPod) == "" || len(t.Members) != len(other.Members) {
		return false, false
	}
	targets := 0
	for index := range t.Members {
		expected, observed := t.Members[index], other.Members[index]
		if expected.Pod.Name != observed.Pod.Name || expected.Pod.Name == "" {
			return false, false
		}
		if expected.Pod.Name != targetPod {
			if !reflect.DeepEqual(expected, observed) {
				return false, false
			}
			continue
		}
		targets++
		if !reflect.DeepEqual(expected.Node, observed.Node) || !reflect.DeepEqual(expected.Workload, observed.Workload) || !reflect.DeepEqual(expected.Storage, observed.Storage) || expected.Pod != observed.Pod || expected.ContainerID != observed.ContainerID {
			return false, false
		}
		for database := range expected.Databases {
			oldDB, newDB := expected.Databases[database], observed.Databases[database]
			if oldDB.Database != newDB.Database || oldDB.ClusterID != newDB.ClusterID || oldDB.ServerID != newDB.ServerID {
				return false, false
			}
			changed = changed || oldDB.Address != newDB.Address || oldDB.ClientAddress != newDB.ClientAddress
		}
	}
	return changed, targets == 1
}

// DiscoverCentralTopology requires all desired local RAFT members to be live.
// Complete full IDs come from each server's own status. CommandClient then
// verifies that every peer agrees with the leader's complete configuration.
func DiscoverCentralTopology(ctx context.Context, reader client.Reader, runner CommandRunner, installation *api.KubeOVNInstallation) (CentralTopology, error) {
	return DiscoverCentralTopologyWithExpectedReplicas(ctx, reader, runner, installation, int(installation.Spec.Central.Replicas))
}

// DiscoverCentralTopologyWithExpectedReplicas verifies a complete live
// inventory against an explicitly selected member count. Retained verification
// uses the previously admitted count while a desired replica change is waiting
// for lifecycle authorization; final handoff uses the installation's desired
// count through DiscoverCentralTopology.
func DiscoverCentralTopologyWithExpectedReplicas(ctx context.Context, reader client.Reader, runner CommandRunner, installation *api.KubeOVNInstallation, expectedReplicas int) (CentralTopology, error) {
	return discoverCentralTopology(ctx, reader, runner, installation, expectedReplicas, "")
}

// DiscoverCentralTopologyWithExpectedReplicasForReplacement observes a retained
// topology while allowing endpoint address drift for exactly one named Pod.
// Every other endpoint remains subject to the normal node InternalIP check.
// Callers must compare the result with the retained baseline before admitting
// a lifecycle operation.
func DiscoverCentralTopologyWithExpectedReplicasForReplacement(ctx context.Context, reader client.Reader, runner CommandRunner, installation *api.KubeOVNInstallation, expectedReplicas int, targetPod string) (CentralTopology, error) {
	if strings.TrimSpace(targetPod) == "" {
		return CentralTopology{}, errors.New("central replacement observation requires a target Pod")
	}
	return discoverCentralTopology(ctx, reader, runner, installation, expectedReplicas, targetPod)
}

func discoverCentralTopology(ctx context.Context, reader client.Reader, runner CommandRunner, installation *api.KubeOVNInstallation, expectedReplicas int, endpointDriftTarget string) (CentralTopology, error) {
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	bound, err := newMembershipRunner(ctx, reader, runner, installation)
	if err != nil {
		return CentralTopology{}, err
	}
	if expectedReplicas < 1 || len(bound.pods) == 0 || len(bound.pods) > 7 || len(bound.pods) != expectedReplicas {
		return CentralTopology{}, errors.New("complete desired central Pod inventory is not available")
	}
	result := CentralTopology{}
	leaders := [2]string{}
	for _, name := range slices.Sorted(maps.Keys(bound.pods)) {
		pod := bound.pods[name]
		member, err := centralKubernetesIdentity(ctx, reader, installation, pod)
		if err != nil {
			return CentralTopology{}, err
		}
		for i, database := range topologyDatabases {
			command := []string{"ovs-appctl", "--timeout=5", "-t", topologySocket(i), "cluster/status", database}
			data, err := bound.Run(ctx, pod.Name, command)
			if err != nil {
				return CentralTopology{}, err
			}
			status, err := ovnadmin.ParseClusterStatus(data, database)
			if err != nil {
				return CentralTopology{}, err
			}
			if status.Role == "leader" {
				if leaders[i] != "" {
					return CentralTopology{}, errors.New("multiple central leaders observed")
				}
				leaders[i] = pod.Name
			}
			allowDrift := endpointDriftAllowed(installation, pod.Name)
			if endpointDriftTarget != "" {
				allowDrift = pod.Name == endpointDriftTarget
			}
			if err := addressOnNode(ctx, reader, member.Node, status.Address, allowDrift); err != nil {
				return CentralTopology{}, err
			}
			address, err := bound.clientAddress(ctx, pod.Name, i, status.Address)
			if err != nil {
				return CentralTopology{}, err
			}
			member.Databases[i] = DatabaseIdentity{Database: database, ClusterID: status.ClusterID, ServerID: status.ServerID, Address: status.Address, ClientAddress: address}
		}
		result.Members = append(result.Members, member)
	}
	slices.SortFunc(result.Members, func(a, b CentralIdentity) int { return cmp.Compare(a.Databases[0].ServerID, b.Databases[0].ServerID) })
	if err := verifyCentralTopology(ctx, bound, result, leaders); err != nil {
		return CentralTopology{}, err
	}

	// Re-read all identity-bearing Kubernetes objects and both listener settings.
	// A changing observation is retried rather than published as a trusted baseline.
	after, err := ownedPods(ctx, reader, installation, "ovn-central")
	if err != nil {
		return CentralTopology{}, err
	}
	if len(after) != len(result.Members) {
		return CentralTopology{}, errors.New("central Pod set changed during discovery")
	}
	for _, member := range result.Members {
		if err := bound.validate(ctx, member.Pod.Name); err != nil {
			return CentralTopology{}, err
		}
		index := slices.IndexFunc(after, func(pod corev1.Pod) bool { return pod.Name == member.Pod.Name })
		if index < 0 {
			return CentralTopology{}, errors.New("central endpoint disappeared during discovery")
		}
		current, err := centralKubernetesIdentity(ctx, reader, installation, after[index])
		if err != nil {
			return CentralTopology{}, err
		}
		current.Databases = member.Databases
		if !reflect.DeepEqual(current, member) {
			return CentralTopology{}, errors.New("central node, workload or storage changed during discovery")
		}
		for i, db := range member.Databases {
			allowDrift := endpointDriftAllowed(installation, member.Pod.Name)
			if endpointDriftTarget != "" {
				allowDrift = member.Pod.Name == endpointDriftTarget
			}
			if err := addressOnNode(ctx, reader, member.Node, db.Address, allowDrift); err != nil {
				return CentralTopology{}, err
			}
			address, err := bound.clientAddress(ctx, member.Pod.Name, i, db.Address)
			if err != nil {
				return CentralTopology{}, err
			}
			if address != db.ClientAddress {
				return CentralTopology{}, errors.New("central client listener changed during discovery")
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return CentralTopology{}, err
	}
	if err := verifyCentralTopology(ctx, bound, result, leaders); err != nil {
		return CentralTopology{}, err
	}
	if err := result.Validate(); err != nil {
		return CentralTopology{}, err
	}
	return result, nil
}

func verifyCentralTopology(ctx context.Context, bound *membershipRunner, result CentralTopology, leaders [2]string) error {
	for i, database := range topologyDatabases {
		transport := &ovnadmin.CommandClient{Runner: bound, ClusterID: result.Members[0].Databases[i].ClusterID}
		var clients []membership.Member
		for _, member := range result.Members {
			db := member.Databases[i]
			if db.ClusterID != transport.ClusterID {
				return errors.New("central cluster identities disagree")
			}
			transport.Endpoints = append(transport.Endpoints, ovnadmin.Endpoint{Name: member.Pod.Name, ServerID: db.ServerID, Address: db.Address})
			clients = append(clients, membership.Member{ServerID: db.ServerID, Address: db.ClientAddress})
		}
		if err := membership.ValidateMembers(clients); err != nil {
			return err
		}
		snapshot, err := transport.Status(ctx, database, leaders[i])
		if err != nil {
			return err
		}
		if len(snapshot.Members) != len(result.Members) {
			return errors.New("live membership does not contain every observed central member")
		}
		for _, member := range snapshot.Members {
			if !member.Healthy {
				return errors.New("initial topology requires all members to have applied committed data")
			}
		}
	}
	return nil
}

func topologySocket(index int) string {
	return []string{"/var/run/ovn/ovnnb_db.ctl", "/var/run/ovn/ovnsb_db.ctl"}[index]
}

func centralKubernetesIdentity(ctx context.Context, reader client.Reader, installation *api.KubeOVNInstallation, pod corev1.Pod) (CentralIdentity, error) {
	result := CentralIdentity{Pod: ObjectIdentity{Name: pod.Name, UID: pod.UID}}
	workload, err := ownedPodWorkload(ctx, reader, installation, pod)
	if err != nil {
		return result, err
	}
	if workload == nil || pod.Spec.NodeName == "" || !pod.Spec.HostNetwork {
		return result, errors.New("central topology requires an owned workload on a host-network node")
	}
	result.Workload.ObjectIdentity = ObjectIdentity{Name: workload.GetName(), UID: workload.GetUID()}
	switch workload.(type) {
	case *appsv1.Deployment:
		result.Workload.Kind = "Deployment"
	case *appsv1.StatefulSet:
		result.Workload.Kind = "StatefulSet"
	case *appsv1.DaemonSet:
		result.Workload.Kind = "DaemonSet"
	}
	node := new(corev1.Node)
	if err := reader.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
		return result, err
	}
	if node.UID == "" || node.DeletionTimestamp != nil || !slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
		return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
	}) {
		return result, &NodeLifecycleError{NodeName: node.Name, Deleting: node.DeletionTimestamp != nil}
	}
	if node.Spec.Unschedulable && !replacementTargetAllowed(installation, pod.Name) {
		return result, &NodeLifecycleError{NodeName: node.Name, Unschedulable: true}
	}
	result.Node = ObjectIdentity{Name: node.Name, UID: node.UID}
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == "ovn-central" {
			result.ContainerID = status.ContainerID
		}
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != "ovn-central" {
			continue
		}
		for _, mount := range container.VolumeMounts {
			// These are the explicit database roots of the upstream Chart and the
			// operator member runtime. Other mounts do not establish data ownership.
			if mount.MountPath != "/etc/ovn" && mount.MountPath != "/var/lib/ovn-member" {
				continue
			}
			if mount.ReadOnly || mount.SubPathExpr != "" {
				return result, errors.New("database storage requires a writable, fixed mount")
			}
			storage := StorageIdentity{MountPath: mount.MountPath, SubPath: mount.SubPath}
			for _, volume := range pod.Spec.Volumes {
				if volume.Name != mount.Name {
					continue
				}
				switch {
				case volume.HostPath != nil:
					storage.HostPath = volume.HostPath.Path
					if !strings.HasPrefix(storage.HostPath, "/") {
						return result, errors.New("database host path must be absolute")
					}
				case volume.PersistentVolumeClaim != nil:
					claim := new(corev1.PersistentVolumeClaim)
					if err := reader.Get(ctx, client.ObjectKey{Namespace: pod.Namespace, Name: volume.PersistentVolumeClaim.ClaimName}, claim); err != nil {
						return result, err
					}
					if claim.UID == "" || claim.DeletionTimestamp != nil || claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName == "" || claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem || volume.PersistentVolumeClaim.ReadOnly {
						return result, errors.New("database claim is not a bound filesystem")
					}
					pv := new(corev1.PersistentVolume)
					if err := reader.Get(ctx, client.ObjectKey{Name: claim.Spec.VolumeName}, pv); err != nil {
						return result, err
					}
					ref := pv.Spec.ClaimRef
					if pv.UID == "" || pv.DeletionTimestamp != nil || pv.Status.Phase != corev1.VolumeBound || ref == nil || ref.Name != claim.Name || ref.Namespace != claim.Namespace || ref.UID != claim.UID {
						return result, errors.New("database volume no longer binds the exact claim")
					}
					storage.Claim = ObjectIdentity{Name: claim.Name, UID: claim.UID}
					storage.Volume = ObjectIdentity{Name: pv.Name, UID: pv.UID}
				default:
					return result, errors.New("database storage must be a host path or bound persistent claim")
				}
			}
			if storage.HostPath == "" && storage.Claim.UID == "" {
				return result, errors.New("database storage volume is missing")
			}
			result.Storage = append(result.Storage, storage)
		}
	}
	if len(result.Storage) != 1 {
		return result, errors.New("central member requires one unambiguous persistent database root")
	}
	return result, nil
}

func addressOnNode(ctx context.Context, reader client.Reader, identity ObjectIdentity, address string, allowDrift bool) error {
	_, endpoint, _ := strings.Cut(address, ":")
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil {
		return err
	}
	node := new(corev1.Node)
	if err := reader.Get(ctx, client.ObjectKey{Name: identity.Name}, node); err != nil {
		return err
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return err
	}
	if node.UID != identity.UID {
		return errors.New("central node identity changed")
	}
	for _, address := range node.Status.Addresses {
		if candidate, err := netip.ParseAddr(address.Address); err == nil && address.Type == corev1.NodeInternalIP && candidate == ip {
			return nil
		}
	}
	if allowDrift {
		return nil
	}
	return &EndpointDriftError{NodeName: identity.Name, Address: address}
}

// replacementTargetAllowed is deliberately tied to one explicit replacement
// request. Replica changes alone must never turn a changed Node address into a
// trusted topology observation, and multiple replacement targets are rejected
// by API validation before reconciliation.
func replacementTargetAllowed(installation *api.KubeOVNInstallation, podName string) bool {
	if installation == nil || installation.Status.Topology.Inventory == nil || !installation.Spec.Central.DynamicMembership || podName == "" {
		return false
	}
	for _, request := range installation.Spec.Central.MemberRequests {
		if request.ReplaceEndpoint == podName {
			return true
		}
	}
	return false
}

func endpointDriftAllowed(installation *api.KubeOVNInstallation, podName string) bool {
	return replacementTargetAllowed(installation, podName)
}

// Validate checks retained data before it can become an identity reference.
func (t CentralTopology) Validate() error {
	if len(t.Members) == 0 || len(t.Members) > 7 {
		return errors.New("invalid retained central member count")
	}
	pods, volumes := map[string]bool{}, map[string]bool{}
	for j, member := range t.Members {
		if member.Node.Name == "" || member.Node.UID == "" || member.Workload.Name == "" || member.Workload.UID == "" || !slices.Contains([]string{"Deployment", "StatefulSet", "DaemonSet"}, member.Workload.Kind) || member.Pod.Name == "" || member.Pod.UID == "" || member.ContainerID == "" || pods[member.Pod.Name] || len(member.Storage) != 1 {
			return errors.New("incomplete or duplicate retained Kubernetes identity")
		}
		pods[member.Pod.Name] = true
		storage := member.Storage[0]
		if storage.MountPath != "/etc/ovn" && storage.MountPath != "/var/lib/ovn-member" {
			return errors.New("unknown retained database root")
		}
		if storage.HostPath != "" {
			if !strings.HasPrefix(storage.HostPath, "/") || storage.Claim != (ObjectIdentity{}) || storage.Volume != (ObjectIdentity{}) {
				return errors.New("invalid retained host storage")
			}
		} else if storage.Claim.Name == "" || storage.Claim.UID == "" || storage.Volume.Name == "" || storage.Volume.UID == "" {
			return errors.New("incomplete retained volume identity")
		}
		storageKey := string(storage.Claim.UID) + ":" + storage.SubPath
		if storage.HostPath != "" {
			storageKey = string(member.Node.UID) + ":" + storage.HostPath + ":" + storage.SubPath
		}
		if volumes[storageKey] {
			return errors.New("central members share the same recorded database storage")
		}
		volumes[storageKey] = true
		if j > 0 && t.Members[j-1].Databases[0].ServerID >= member.Databases[0].ServerID {
			return errors.New("retained central identities are not canonically ordered")
		}
	}
	for i, name := range topologyDatabases {
		var peers, clients []membership.Member
		cid := t.Members[0].Databases[i].ClusterID
		for _, member := range t.Members {
			db := member.Databases[i]
			if db.Database != name || !membership.ValidID(cid) || db.ClusterID != cid || db.Address == db.ClientAddress {
				return errors.New("invalid retained database identity")
			}
			peers = append(peers, membership.Member{ServerID: db.ServerID, Address: db.Address})
			clients = append(clients, membership.Member{ServerID: db.ServerID, Address: db.ClientAddress})
		}
		if err := errors.Join(membership.ValidateMembers(peers), membership.ValidateMembers(clients)); err != nil {
			return err
		}
	}
	return nil
}
