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
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	retireEndpoint := ""
	for _, member := range installation.Spec.Central.MemberRequests {
		if member.RetireEndpoint == "" {
			continue
		}
		if retireEndpoint != "" && retireEndpoint != member.RetireEndpoint {
			return nil, errors.New("central member retirement requires one explicit retireEndpoint")
		}
		retireEndpoint = member.RetireEndpoint
	}
	if retireEndpoint != "" && !installation.Spec.Central.DynamicMembership {
		return nil, errors.New("explicit Deployment retirement requires central dynamicMembership=true")
	}

	type candidate struct {
		endpoint   string
		retirement Retirement
		member     *api.CentralMemberSpec
	}
	candidates := make([]candidate, 0, len(topology.Members))
	for _, member := range topology.Members {
		if retireEndpoint != "" && member.Pod.Name != retireEndpoint {
			continue
		}
		if retireEndpoint != "" && member.Workload.Kind == "StatefulSet" && member.Pod.Name != member.Workload.Name+"-0" {
			return nil, errors.New("explicit retirement must identify the ordinal Pod of an independent StatefulSet")
		}
		if retireEndpoint == "" && (member.Workload.Kind != "StatefulSet" || member.Pod.Name != member.Workload.Name+"-0") {
			continue
		}
		workloadRef := workload.Reference{Name: member.Workload.Name, UID: member.Workload.UID}
		memberRequest := memberRequestForEndpoint(installation, member.Pod.Name)
		retirement, err := pinRetirementForMember(ctx, kube, installation, topology, memberRequest, member, workloadRef, workload.Reference{Name: member.Pod.Name, UID: member.Pod.UID})
		if err != nil {
			return nil, fmt.Errorf("cannot pin central member %s for retirement: %w", member.Workload.Name, err)
		}
		candidates = append(candidates, candidate{endpoint: member.Pod.Name, retirement: retirement, member: memberRequest})
	}
	if len(candidates) == 0 {
		if retireEndpoint != "" {
			return nil, fmt.Errorf("central replica decrease cannot find the requested retained endpoint %q", retireEndpoint)
		}
		return nil, errors.New("central replica decrease is unsafe: no independently managed one-member StatefulSet can be retired; ordinary Chart multi-replica workloads require an explicit retireEndpoint")
	}
	if len(candidates) != 1 {
		return nil, fmt.Errorf("central replica decrease is ambiguous: %d independently managed members are eligible; an explicit retirement selection is required", len(candidates))
	}

	operationID := uuid.New().String()
	if candidates[0].member != nil && candidates[0].member.RemovalApprovalRef != nil {
		approvalOperationID, err := removalApprovalOperationID(ctx, kube, installation, candidates[0].member.RemovalApprovalRef)
		if err != nil {
			return nil, err
		}
		operationID = approvalOperationID
	}
	request := Request{
		FormatVersion:          1,
		InstallationUID:        string(installation.UID),
		InstallationGeneration: installation.Generation,
		OperationID:            operationID,
		Databases:              inventories(topology),
		RemoveEndpoint:         candidates[0].endpoint,
		Retirement:             &candidates[0].retirement,
	}
	if candidates[0].member != nil {
		setRemovalApproval(&request, candidates[0].member.RemovalApprovalRef)
	}
	return &request, nil
}

// SelectAddition builds one immutable addition request from a member slot that
// the user has explicitly prepared. Replica count alone is never enough to
// choose a node, volume, image or listener address. A same-size request may
// omit replaceEndpoint only when it carries a loss approval and exactly one
// retained endpoint has been proven lost or replaced.
func SelectAddition(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, topology health.CentralTopology) (*Request, error) {
	return selectAddition(ctx, kube, installation, topology, "")
}

// SelectAdditionForMember is the recovery path's explicit-target variant of
// SelectAddition. The member reference is matched by CentralMemberSpec.Name;
// no replica-count or lexical fallback is allowed when it is supplied.
func SelectAdditionForMember(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, topology health.CentralTopology, memberRef string) (*Request, error) {
	memberRef = strings.TrimSpace(memberRef)
	if memberRef == "" {
		return nil, errors.New("explicit member selection requires a member reference")
	}
	return selectAddition(ctx, kube, installation, topology, memberRef)
}

func selectAddition(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, topology health.CentralTopology, memberRef string) (*Request, error) {
	if kube == nil || installation == nil || installation.UID == "" || installation.Generation < 1 {
		return nil, errors.New("automatic membership selection requires an identified installation and reader")
	}
	if installation.Spec.Central.Mode != api.CentralModeRaft || installation.Spec.Mode == api.InstallationModeDataPlaneOnly || installation.Spec.HCP.Enabled {
		return nil, errors.New("automatic membership selection requires a local RAFT installation")
	}
	if err := topology.Validate(); err != nil {
		return nil, fmt.Errorf("retained topology is not selectable: %w", err)
	}
	if !installation.Spec.Central.DynamicMembership {
		return nil, errors.New("central dynamicMembership must be enabled for automatic member addition or replacement")
	}
	desired, current := int(installation.Spec.Central.Replicas), len(topology.Members)
	switch {
	case desired == current:
		// A same-size transition is a replacement. It normally names the old
		// endpoint; a UID-bound approval may opt into unique-loss inference.
	case desired < current:
		return nil, errors.New("automatic membership addition cannot reduce the retained member count")
	}

	existing := make(map[string]bool, len(topology.Members))
	for _, member := range topology.Members {
		existing[member.Workload.Name] = true
	}
	available := make([]api.CentralMemberSpec, 0, len(installation.Spec.Central.MemberRequests))
	for _, member := range installation.Spec.Central.MemberRequests {
		if memberRef != "" && member.Name != memberRef {
			continue
		}
		if existing[workload.RecordName(string(installation.UID), member.Name)] {
			continue
		}
		// Retirement-only entries select an existing Chart Pod for scale-in;
		// they do not describe a workload that can be prepared and started.
		if member.RetireEndpoint != "" {
			continue
		}
		if desired == current && member.ReplaceEndpoint == "" && member.RemovalApprovalRef == nil {
			continue
		}
		if desired > current && member.ReplaceEndpoint == "" && member.RemovalApprovalRef != nil {
			// An approval without an explicit target is reserved for the
			// same-size automatic replacement path. It must never turn a
			// replica increase into an implicit removal.
			continue
		}
		available = append(available, member)
	}
	if len(available) == 0 {
		if memberRef != "" {
			return nil, fmt.Errorf("explicit member request %q is already used, retired or incompatible with the retained replica count", memberRef)
		}
		if desired == current {
			return nil, nil
		}
		return nil, errors.New("central membership increase requires one explicit unused memberRequest")
	}
	if desired == current && len(available) > 1 {
		return nil, errors.New("automatic central replacement requires exactly one eligible memberRequest")
	}
	slices.SortFunc(available, func(a, b api.CentralMemberSpec) int { return cmp.Compare(a.Name, b.Name) })
	member := available[0]
	if err := validateMemberSpec(member); err != nil {
		return nil, err
	}
	protocol, err := memberAddressProtocol(member)
	if err != nil {
		return nil, err
	}
	var tlsSecret *workload.TLSSecretReference
	if member.TLSSecretRef != nil {
		tlsSecret, err = workload.ResolveTLSSecret(ctx, kube, installation.Namespace, member.TLSSecretRef)
		if err != nil {
			return nil, fmt.Errorf("resolve member TLS Secret %q: %w", member.TLSSecretRef.Name, err)
		}
	}
	if (protocol == "ssl") != (tlsSecret != nil) {
		return nil, errors.New("member listener protocol and tlsSecretRef must agree")
	}
	if desired > current && member.ReplaceEndpoint != "" {
		return nil, errors.New("memberRequest.replaceEndpoint requires a same-size central replacement")
	}
	automaticReplacement := desired == current && member.ReplaceEndpoint == ""
	replaceEndpoint := member.ReplaceEndpoint
	if desired == current && replaceEndpoint == "" {
		target, err := automaticReplacementTarget(ctx, kube, installation, topology)
		if err != nil {
			return nil, err
		}
		if target == nil {
			return nil, nil
		}
		replaceEndpoint = target.Pod.Name
	}
	var retirement *Retirement
	if replaceEndpoint != "" {
		if desired != current {
			return nil, errors.New("memberRequest.replaceEndpoint requires replicas to match the retained member count")
		}
		index := slices.IndexFunc(topology.Members, func(retained health.CentralIdentity) bool {
			return retained.Pod.Name == replaceEndpoint
		})
		if index < 0 {
			return nil, fmt.Errorf("replacement endpoint %q must identify a retained central Pod", replaceEndpoint)
		}
		retained := topology.Members[index]
		workloadRef := workload.Reference{Name: retained.Workload.Name, UID: retained.Workload.UID}
		var pinned Retirement
		var err error
		if retained.Workload.Kind == "StatefulSet" && retained.Pod.Name != retained.Workload.Name+"-0" {
			return nil, errors.New("memberRequest.replaceEndpoint must identify the ordinal Pod of an independent StatefulSet")
		}
		pinned, err = pinRetirementForMember(ctx, kube, installation, topology, &member, retained, workloadRef, workload.Reference{Name: retained.Pod.Name, UID: retained.Pod.UID})
		if err != nil && retained.Workload.Kind != "StatefulSet" && retained.Workload.Kind != "Deployment" {
			return nil, errors.New("memberRequest.replaceEndpoint must identify an owned Deployment or independent StatefulSet")
		}
		if err != nil {
			return nil, fmt.Errorf("cannot pin central member %s for replacement: %w", retained.Workload.Name, err)
		}
		retirement = &pinned
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
	if member.RemovalApprovalRef != nil {
		operationID, err = removalApprovalOperationID(ctx, kube, installation, member.RemovalApprovalRef)
		if err != nil {
			return nil, err
		}
	}
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
	request := &Request{FormatVersion: 1, InstallationUID: string(installation.UID), InstallationGeneration: installation.Generation, OperationID: operationID, Databases: inventories(topology), AutomaticReplacement: automaticReplacement, Add: &Addition{
		Workload:          workload.Request{InstallationGeneration: installation.Generation, Preparation: canonicalPlan, Node: workload.Reference{Name: node.Name, UID: node.UID}, Claim: workload.Reference{Name: claim.Name, UID: claim.UID}, Volume: workload.Reference{Name: volume.Name, UID: volume.UID}, Image: member.Image, TLSSecret: tlsSecret},
		NorthboundAddress: member.NorthboundClientAddress, SouthboundAddress: member.SouthboundClientAddress,
	}}
	if retirement != nil {
		request.RemoveEndpoint = replaceEndpoint
		request.Retirement = retirement
	}
	setRemovalApproval(request, member.RemovalApprovalRef)
	if err := request.launch(prepare.Result{}).Validate(); err != nil {
		return nil, fmt.Errorf("validate member launch addresses: %w", err)
	}
	return request, nil
}

// removalApprovalOperationID binds a pre-created permanent-loss decision to
// the lifecycle request that will eventually reserve it. The approval must
// exist before a lost endpoint can be replaced, so generating a second random
// operation UUID here would make every valid approval unusable.
func removalApprovalOperationID(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, ref *api.MemberRemovalApprovalReference) (string, error) {
	if kube == nil || installation == nil || ref == nil {
		return "", errors.New("removal approval requires an installation, reader and reference")
	}
	approval := new(api.KubeOVNMemberRemovalApproval)
	key := client.ObjectKey{Namespace: installation.Namespace, Name: ref.Name}
	if err := kube.Get(ctx, key, approval); err != nil {
		return "", fmt.Errorf("read removal approval %s/%s: %w", key.Namespace, key.Name, err)
	}
	if approval.UID != ref.UID || approval.DeletionTimestamp != nil || len(approval.OwnerReferences) != 0 {
		return "", errors.New("removal approval identity changed, is being deleted, or is subject to owner garbage collection")
	}
	decision := approval.Spec.Decision
	if decision.InstallationRef.Name != installation.Name || decision.InstallationRef.UID != installation.UID {
		return "", errors.New("removal approval does not bind the current installation")
	}
	if !membership.ValidID(decision.OperationID) {
		return "", errors.New("removal approval must contain a full operation UUID")
	}
	return decision.OperationID, nil
}

func validateMemberSpec(member api.CentralMemberSpec) error {
	if len(validation.IsDNS1123Label(member.Name)) != 0 {
		return errors.New("memberRequest.name must be a DNS label")
	}
	if member.ReplaceEndpoint != "" && len(validation.IsDNS1123Subdomain(member.ReplaceEndpoint)) != 0 {
		return errors.New("memberRequest.replaceEndpoint must be a DNS subdomain")
	}
	if member.RemovalApprovalRef != nil {
		if len(validation.IsDNS1123Subdomain(member.RemovalApprovalRef.Name)) != 0 || member.RemovalApprovalRef.UID == "" {
			return errors.New("memberRequest.removalApprovalRef must contain a valid name and UID")
		}
	}
	if member.TLSSecretRef != nil && len(validation.IsDNS1123Subdomain(member.TLSSecretRef.Name)) != 0 {
		return errors.New("memberRequest.tlsSecretRef.name must be a DNS subdomain")
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
	protocol, err := memberAddressProtocol(member)
	if err != nil {
		return err
	}
	if (protocol == "ssl") != (member.TLSSecretRef != nil) {
		return errors.New("member listener protocol and tlsSecretRef must agree")
	}
	return nil
}

func memberAddressProtocol(member api.CentralMemberSpec) (string, error) {
	protocol := ""
	for _, address := range []string{member.NorthboundAddress, member.SouthboundAddress, member.NorthboundClientAddress, member.SouthboundClientAddress} {
		current, endpoint, found := strings.Cut(address, ":")
		if !found || endpoint == "" || current != "tcp" && current != "ssl" {
			return "", errors.New("member NB/SB addresses must use tcp or ssl")
		}
		if protocol == "" {
			protocol = current
		} else if protocol != current {
			return "", errors.New("member NB/SB addresses must use one protocol")
		}
	}
	return protocol, nil
}

// automaticReplacementTarget returns a target only when exactly one retained
// member has disappeared, been replaced, or is still running on its retained
// Node whose advertised InternalIP no longer contains the pinned database
// endpoints. A missing or unhealthy Node alone is insufficient: the endpoint
// Pod must also be gone or have a new UID, or a healthy retained Node must show
// endpoint drift. The caller still requires an explicit removal approval for
// this automatic same-size replacement path.
func automaticReplacementTarget(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, topology health.CentralTopology) (*health.CentralIdentity, error) {
	var candidates []health.CentralIdentity
	for index := range topology.Members {
		member := topology.Members[index]
		lost, err := endpointLostWithNode(ctx, kube, installation, member)
		if err != nil {
			return nil, fmt.Errorf("check retained member %s for automatic replacement: %w", member.Pod.Name, err)
		}
		if !lost {
			lost, err = endpointAddressDrifted(ctx, kube, member)
			if err != nil {
				return nil, fmt.Errorf("check retained member %s endpoint drift for automatic replacement: %w", member.Pod.Name, err)
			}
		}
		if lost {
			candidates = append(candidates, member)
		}
	}
	switch len(candidates) {
	case 0:
		return nil, nil
	case 1:
		return new(candidates[0]), nil
	default:
		return nil, fmt.Errorf("automatic central replacement is ambiguous: %d retained members are lost", len(candidates))
	}
}

// endpointAddressDrifted recognizes only a healthy, unchanged retained Node
// whose current InternalIP no longer contains every pinned NB/SB RAFT and
// client endpoint. Missing, replaced, deleting or NotReady Nodes return false
// here because endpointLostWithNode owns those loss decisions and must still
// require the Pod identity evidence described above.
func endpointAddressDrifted(ctx context.Context, kube client.Reader, member health.CentralIdentity) (bool, error) {
	if kube == nil || member.Node.Name == "" || member.Node.UID == "" {
		return false, errors.New("endpoint drift requires a retained Node identity")
	}
	node := new(corev1.Node)
	if err := kube.Get(ctx, client.ObjectKey{Name: member.Node.Name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if node.UID != member.Node.UID || node.DeletionTimestamp != nil || !nodeReady(node) {
		return false, nil
	}
	internalIPs := make([]netip.Addr, 0, len(node.Status.Addresses))
	for _, candidate := range node.Status.Addresses {
		if candidate.Type != corev1.NodeInternalIP {
			continue
		}
		value, err := netip.ParseAddr(candidate.Address)
		if err == nil {
			internalIPs = append(internalIPs, value)
		}
	}
	// An empty or malformed address list is insufficient evidence of IP drift;
	// the normal topology observer will report that state and keep the
	// installation degraded until Kubernetes publishes a stable Node address.
	if len(internalIPs) == 0 {
		return false, nil
	}
	for _, database := range member.Databases {
		for _, address := range []string{database.Address, database.ClientAddress} {
			_, endpoint, ok := strings.Cut(address, ":")
			if !ok {
				return false, fmt.Errorf("retained endpoint %q has no protocol separator", address)
			}
			host, _, err := net.SplitHostPort(endpoint)
			if err != nil {
				return false, fmt.Errorf("retained endpoint %q is invalid: %w", address, err)
			}
			ip, err := netip.ParseAddr(host)
			if err != nil {
				return false, fmt.Errorf("retained endpoint %q has an invalid IP: %w", address, err)
			}
			if !slices.Contains(internalIPs, ip) {
				return true, nil
			}
		}
	}
	return false, nil
}

func pinRetirementForMember(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, topology health.CentralTopology, member *api.CentralMemberSpec, retained health.CentralIdentity, workloadRef, endpoint workload.Reference) (Retirement, error) {
	var retirement Retirement
	var err error
	switch retained.Workload.Kind {
	case "StatefulSet":
		if endpoint.Name != retained.Workload.Name+"-0" {
			return Retirement{}, errors.New("retirement must identify the ordinal Pod of an independent StatefulSet")
		}
		retirement, err = PinRetirement(ctx, kube, installation, workloadRef)
	case "Deployment":
		retirement, err = PinDeploymentRetirement(ctx, kube, installation, workloadRef, endpoint)
		if err != nil {
			lost, lostErr := endpointLostWithNode(ctx, kube, installation, retained)
			if lostErr != nil {
				return Retirement{}, lostErr
			}
			if !lost {
				return Retirement{}, err
			}
			if member == nil || member.RemovalApprovalRef == nil {
				return Retirement{}, errors.New("lost central endpoint requires memberRequest.removalApprovalRef")
			}
			retirement, err = PinDeploymentRetirementAfterLoss(ctx, kube, installation, workloadRef, endpoint, retainedDeploymentSurvivors(topology, retained))
		}
	default:
		return Retirement{}, errors.New("unsupported retained central workload kind")
	}
	if err != nil {
		return Retirement{}, err
	}
	return retirement, nil
}

func endpointLostWithNode(ctx context.Context, kube client.Reader, installation *api.KubeOVNInstallation, retained health.CentralIdentity) (bool, error) {
	if kube == nil || retained.Node.Name == "" || retained.Node.UID == "" || retained.Pod.Name == "" {
		return false, errors.New("lost endpoint detection requires retained node and Pod identities")
	}
	pod := new(corev1.Pod)
	if installation == nil {
		return false, errors.New("lost endpoint detection requires an installation")
	}
	if err := kube.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: retained.Pod.Name}, pod); err == nil {
		return pod.UID != retained.Pod.UID, nil
	} else if !apierrors.IsNotFound(err) {
		return false, err
	}
	node := new(corev1.Node)
	if err := kube.Get(ctx, client.ObjectKey{Name: retained.Node.Name}, node); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	}
	return node.UID != retained.Node.UID || node.DeletionTimestamp != nil || node.Spec.Unschedulable || !nodeReady(node), nil
}

func retainedDeploymentSurvivors(topology health.CentralTopology, target health.CentralIdentity) []workload.Reference {
	result := make([]workload.Reference, 0, len(topology.Members)-1)
	for _, member := range topology.Members {
		if member.Workload.Kind == target.Workload.Kind && member.Workload.Name == target.Workload.Name && member.Workload.UID == target.Workload.UID && member.Pod.Name != target.Pod.Name {
			result = append(result, workload.Reference{Name: member.Pod.Name, UID: member.Pod.UID})
		}
	}
	slices.SortFunc(result, func(a, b workload.Reference) int { return cmp.Compare(a.Name, b.Name) })
	return result
}

func memberRequestForEndpoint(installation *api.KubeOVNInstallation, endpoint string) *api.CentralMemberSpec {
	if installation == nil || endpoint == "" {
		return nil
	}
	for index := range installation.Spec.Central.MemberRequests {
		member := &installation.Spec.Central.MemberRequests[index]
		if member.ReplaceEndpoint == endpoint || member.RetireEndpoint == endpoint {
			return member
		}
	}
	return nil
}

func setRemovalApproval(request *Request, reference *api.MemberRemovalApprovalReference) {
	if request == nil || reference == nil {
		return
	}
	for index := range request.Databases {
		request.Databases[index].Approval = membership.ApprovalReference{Name: reference.Name, UID: string(reference.UID)}
	}
}

func nodeReady(node *corev1.Node) bool {
	return slices.ContainsFunc(node.Status.Conditions, func(condition corev1.NodeCondition) bool {
		return condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue
	})
}

func addressOnNode(address string, node *corev1.Node) error {
	protocol, endpoint, ok := strings.Cut(address, ":")
	if !ok || protocol != "tcp" && protocol != "ssl" {
		return fmt.Errorf("member address %q must use tcp or ssl", address)
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
