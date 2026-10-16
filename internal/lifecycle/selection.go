package lifecycle

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"uuid"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SelectRemoval builds one immutable removal request from a retained complete
// topology. Selection is deliberately conservative: a transition may remove
// exactly one member, and that member must be an independently managed,
// single-member StatefulSet. The caller persists the returned request through
// Begin before any workload or database mutation.
func SelectRemoval(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, topology health.CentralTopology) (*Request, error) {
	if kube == nil || installation == nil || installation.UID == "" || installation.Generation < 1 {
		return nil, errors.New("automatic membership selection requires an identified installation and reader")
	}
	if installation.Spec.Central.Mode != api.CentralModeRaft || installation.Spec.Mode == api.InstallationModeDataPlaneOnly || installation.Spec.HCP.Enabled {
		return nil, errors.New("automatic membership selection requires a local RAFT installation")
	}
	if err := topology.Validate(); err != nil {
		return nil, fmt.Errorf("retained topology is not selectable: %w", err)
	}
	desired := int(installation.Spec.Central.Replicas)
	current := len(topology.Members)
	switch {
	case desired == current:
		return nil, nil
	case desired > current:
		return nil, fmt.Errorf("automatic central membership addition requires an explicit prepared member; current=%d desired=%d", current, desired)
	case current-desired != 1:
		return nil, fmt.Errorf("automatic central membership removal supports one member per transition; current=%d desired=%d", current, desired)
	}

	type candidate struct {
		endpoint   string
		retirement Retirement
	}
	candidates := make([]candidate, 0, len(topology.Members))
	for _, member := range topology.Members {
		if member.Workload.Kind != "StatefulSet" || member.Pod.Name != member.Workload.Name+"-0" {
			continue
		}
		retirement, err := PinRetirement(ctx, kube, installation, workload.Reference{Name: member.Workload.Name, UID: member.Workload.UID})
		if err != nil {
			return nil, fmt.Errorf("cannot pin central member %s for retirement: %w", member.Workload.Name, err)
		}
		candidates = append(candidates, candidate{endpoint: member.Pod.Name, retirement: retirement})
	}
	if len(candidates) == 0 {
		return nil, errors.New("central replica decrease is unsafe: no independently managed one-member StatefulSet can be retired; ordinary Chart multi-replica workloads are not selectable")
	}
	if len(candidates) != 1 {
		return nil, fmt.Errorf("central replica decrease is ambiguous: %d independently managed members are eligible; an explicit retirement selection is required", len(candidates))
	}

	request := Request{
		FormatVersion:          1,
		InstallationUID:        string(installation.UID),
		InstallationGeneration: installation.Generation,
		OperationID:            uuid.New().String(),
		Databases:              inventories(topology),
		RemoveEndpoint:         candidates[0].endpoint,
		Retirement:             &candidates[0].retirement,
	}
	return &request, nil
}

// SelectAddition builds one immutable addition request from a member slot that
// the user has explicitly prepared. Replica count alone is never enough to
// choose a node, volume, image or listener address.
func SelectAddition(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, topology health.CentralTopology) (*Request, error) {
	if kube == nil || installation == nil || installation.UID == "" || installation.Generation < 1 {
		return nil, errors.New("automatic membership selection requires an identified installation and reader")
	}
	if installation.Spec.Central.Mode != api.CentralModeRaft || installation.Spec.Mode == api.InstallationModeDataPlaneOnly || installation.Spec.HCP.Enabled {
		return nil, errors.New("automatic membership selection requires a local RAFT installation")
	}
	if err := topology.Validate(); err != nil {
		return nil, fmt.Errorf("retained topology is not selectable: %w", err)
	}
	desired, current := int(installation.Spec.Central.Replicas), len(topology.Members)
	switch {
	case desired == current:
		return nil, nil
	case desired < current:
		return nil, errors.New("automatic membership addition cannot reduce the retained member count")
	case !installation.Spec.Central.DynamicMembership:
		return nil, errors.New("central dynamicMembership must be enabled for automatic member addition")
	}

	existing := make(map[string]bool, len(topology.Members))
	for _, member := range topology.Members {
		existing[member.Workload.Name] = true
	}
	available := make([]api.CentralMemberSpec, 0, len(installation.Spec.Central.MemberRequests))
	for _, member := range installation.Spec.Central.MemberRequests {
		if existing[workload.RecordName(string(installation.UID), member.Name)] {
			continue
		}
		available = append(available, member)
	}
	if len(available) == 0 {
		return nil, errors.New("central membership increase requires one explicit unused memberRequest")
	}
	slices.SortFunc(available, func(a, b api.CentralMemberSpec) int { return cmp.Compare(a.Name, b.Name) })
	member := available[0]
	if err := validateMemberSpec(member); err != nil {
		return nil, err
	}

	node := new(corev1.Node)
	if err := kube.Get(ctx, client.ObjectKey{Name: member.NodeName}, node); err != nil {
		return nil, fmt.Errorf("read member node %s: %w", member.NodeName, err)
	}
	if node.UID == "" || node.DeletionTimestamp != nil || node.Spec.Unschedulable || !nodeReady(node) {
		return nil, errors.New("member node is not ready, schedulable or durably identified")
	}
	claim := new(corev1.PersistentVolumeClaim)
	if err := kube.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: member.ClaimName}, claim); err != nil {
		return nil, fmt.Errorf("read member claim %s: %w", member.ClaimName, err)
	}
	if claim.UID == "" || claim.DeletionTimestamp != nil || claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName != member.VolumeName || claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return nil, errors.New("member claim is not the requested bound filesystem")
	}
	volume := new(corev1.PersistentVolume)
	if err := kube.Get(ctx, client.ObjectKey{Name: member.VolumeName}, volume); err != nil {
		return nil, fmt.Errorf("read member volume %s: %w", member.VolumeName, err)
	}
	if volume.UID == "" || volume.DeletionTimestamp != nil || volume.Status.Phase != corev1.VolumeBound || volume.Spec.ClaimRef == nil || volume.Spec.ClaimRef.UID != claim.UID || volume.Spec.ClaimRef.Name != claim.Name || volume.Spec.ClaimRef.Namespace != claim.Namespace {
		return nil, errors.New("member volume is not durably bound to the requested claim")
	}
	for _, address := range []string{member.NorthboundAddress, member.SouthboundAddress, member.NorthboundClientAddress, member.SouthboundClientAddress} {
		if err := addressOnNode(address, node); err != nil {
			return nil, err
		}
	}

	operationID := uuid.New().String()
	plan := prepare.Plan{FormatVersion: 1, InstallationUID: string(installation.UID), OperationID: operationID, MemberName: member.Name, Databases: make([]prepare.Database, 0, 2)}
	for index, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
		peers := make([]string, 0, len(topology.Members))
		for _, retained := range topology.Members {
			peers = append(peers, retained.Databases[index].Address)
		}
		local := member.NorthboundAddress
		if index == 1 {
			local = member.SouthboundAddress
		}
		plan.Databases = append(plan.Databases, prepare.Database{Name: name, ClusterID: topology.Members[0].Databases[index].ClusterID, LocalAddress: local, RemoteAddresses: peers})
	}
	canonicalPlan, _, err := plan.Canonical()
	if err != nil {
		return nil, fmt.Errorf("validate member preparation plan: %w", err)
	}
	request := &Request{FormatVersion: 1, InstallationUID: string(installation.UID), InstallationGeneration: installation.Generation, OperationID: operationID, Databases: inventories(topology), Add: &Addition{
		Workload:          workload.Request{InstallationGeneration: installation.Generation, Preparation: canonicalPlan, Node: workload.Reference{Name: node.Name, UID: node.UID}, Claim: workload.Reference{Name: claim.Name, UID: claim.UID}, Volume: workload.Reference{Name: volume.Name, UID: volume.UID}, Image: member.Image},
		NorthboundAddress: member.NorthboundClientAddress, SouthboundAddress: member.SouthboundClientAddress,
	}}
	if err := request.launch(prepare.Result{}).Validate(); err != nil {
		return nil, fmt.Errorf("validate member launch addresses: %w", err)
	}
	return request, nil
}

func validateMemberSpec(member api.CentralMemberSpec) error {
	if len(validation.IsDNS1123Label(member.Name)) != 0 {
		return errors.New("memberRequest.name must be a DNS label")
	}
	for _, value := range []string{member.NodeName, member.ClaimName, member.VolumeName, member.Image, member.NorthboundAddress, member.SouthboundAddress, member.NorthboundClientAddress, member.SouthboundClientAddress} {
		if strings.TrimSpace(value) == "" {
			return errors.New("memberRequest requires node, storage, image and all NB/SB addresses")
		}
	}
	image, digest, found := strings.Cut(member.Image, "@sha256:")
	if !found || image == "" || len(digest) != 64 || strings.Trim(digest, "0123456789abcdef") != "" {
		return errors.New("memberRequest.image must be pinned to a sha256 digest")
	}
	return nil
}

func nodeReady(node *corev1.Node) bool {
	return slices.ContainsFunc(node.Status.Conditions, func(condition corev1.NodeCondition) bool {
		return condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue
	})
}

func addressOnNode(address string, node *corev1.Node) error {
	protocol, endpoint, ok := strings.Cut(address, ":")
	if !ok || protocol != "tcp" {
		return fmt.Errorf("member address %q must use tcp", address)
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return fmt.Errorf("member address %q is invalid: %w", address, err)
	}
	if _, err := netip.ParseAddr(host); err != nil {
		return fmt.Errorf("member address %q has an invalid IP: %w", address, err)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1024 || number > 65535 {
		return fmt.Errorf("member address %q has an invalid unprivileged port", address)
	}
	for _, candidate := range node.Status.Addresses {
		if candidate.Type == corev1.NodeInternalIP && candidate.Address == host {
			return nil
		}
	}
	return fmt.Errorf("member address %q is not a current InternalIP of node %s", address, node.Name)
}

func inventories(topology health.CentralTopology) []Inventory {
	result := make([]Inventory, 2)
	for database := range 2 {
		result[database].Database = []string{"OVN_Northbound", "OVN_Southbound"}[database]
		result[database].ClusterID = topology.Members[0].Databases[database].ClusterID
		result[database].Peers = make([]Peer, 0, len(topology.Members))
		for _, member := range topology.Members {
			identity := member.Databases[database]
			result[database].Peers = append(result[database].Peers, Peer{
				Name:          member.Pod.Name,
				ServerID:      identity.ServerID,
				Address:       identity.Address,
				ClientAddress: identity.ClientAddress,
			})
		}
		slices.SortFunc(result[database].Peers, func(a, b Peer) int {
			return cmp.Compare(a.Name, b.Name)
		})
	}
	return result
}
