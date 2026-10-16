package lifecycle

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"uuid"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
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
