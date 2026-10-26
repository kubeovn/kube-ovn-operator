// Package lifecycle coordinates durable, joint NB/SB membership operations.
// Each operation adds and/or removes one central member. A topology controller
// composes operations and changes its apply checkpoint only after convergence.
package lifecycle

import (
	"cmp"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/central"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Peer is durable inventory, including unavailable and temporarily added peers.
// Name is the transport endpoint (a Pod name for the Kubernetes adapter).
type Peer struct {
	Name          string `json:"name"`
	ServerID      string `json:"serverID"`
	Address       string `json:"address"`
	ClientAddress string `json:"clientAddress"`
}

type Inventory struct {
	Database  string                       `json:"database"`
	ClusterID string                       `json:"clusterID"`
	Peers     []Peer                       `json:"peers"`
	Approval  membership.ApprovalReference `json:"approval"`
}

type Addition struct {
	Workload          workload.Request `json:"workload"`
	NorthboundAddress string           `json:"northboundAddress"`
	SouthboundAddress string           `json:"southboundAddress"`
}

// Request is immutable once Begin accepts it. Replicas in the installation are
// the eventual topology, not the size of each intermediate RAFT configuration.
type Request struct {
	FormatVersion          int         `json:"formatVersion"`
	InstallationUID        string      `json:"installationUID"`
	InstallationGeneration int64       `json:"installationGeneration"`
	OperationID            string      `json:"operationID"`
	Databases              []Inventory `json:"databases"`
	Add                    *Addition   `json:"add,omitempty"`
	RemoveEndpoint         string      `json:"removeEndpoint,omitempty"`
	Retirement             *Retirement `json:"retirement,omitempty"`
	// AutomaticReplacement records that the selector inferred the removal
	// endpoint from a unique lost/replaced retained member. It prevents a
	// resumed operation from silently changing between explicit and inferred
	// target semantics when the installation spec is edited.
	AutomaticReplacement bool `json:"automaticReplacement,omitempty"`
}

func canonical(request Request) (Request, string, error) {
	data, err := json.Marshal(request)
	if err != nil || len(data) > 48<<10 {
		return Request{}, "", errors.New("invalid or oversized lifecycle request")
	}
	var detached Request
	if err := json.Unmarshal(data, &detached); err != nil {
		return Request{}, "", err
	}
	request = detached
	if request.FormatVersion != 1 || request.InstallationUID == "" || request.InstallationGeneration < 1 || !membership.ValidID(request.OperationID) || len(request.Databases) != 2 {
		return Request{}, "", errors.New("joint lifecycle requires installation identity, generation, operation UUID and both databases")
	}
	if request.Add == nil && request.RemoveEndpoint == "" {
		return Request{}, "", errors.New("operation must add or remove a member")
	}
	if request.AutomaticReplacement && (request.Add == nil || request.RemoveEndpoint == "" || request.Retirement == nil) {
		return Request{}, "", errors.New("automatic replacement must add a member and pin a retirement target")
	}
	if (request.RemoveEndpoint == "") != (request.Retirement == nil) {
		return Request{}, "", errors.New("removal requires an exact workload retirement target")
	}
	if request.Retirement != nil {
		if request.Retirement.Kind == "" {
			request.Retirement.Kind = statefulSetRetirementKind
		}
		if request.Retirement.Workload.UID == "" || len(validation.IsDNS1123Subdomain(request.Retirement.Workload.Name)) != 0 || !validDigest(request.Retirement.SpecDigest) {
			return Request{}, "", errors.New("retirement must pin the workload identity and spec")
		}
		switch request.Retirement.Kind {
		case statefulSetRetirementKind:
			if request.RemoveEndpoint != request.Retirement.Workload.Name+"-0" || request.Retirement.Endpoint.Name != "" && request.Retirement.Endpoint.Name != request.RemoveEndpoint {
				return Request{}, "", errors.New("statefulset retirement must pin its exact ordinal endpoint")
			}
		case deploymentRetirementKind:
			retirement := request.Retirement
			if request.RemoveEndpoint != retirement.Endpoint.Name || retirement.Endpoint.UID == "" || len(validation.IsDNS1123Subdomain(retirement.Endpoint.Name)) != 0 || retirement.ReplicaSet.UID == "" || len(validation.IsDNS1123Subdomain(retirement.ReplicaSet.Name)) != 0 || retirement.Replicas < 2 || len(retirement.Survivors) != int(retirement.Replicas)-1 {
				return Request{}, "", errors.New("deployment retirement must pin the target Pod, ReplicaSet and all surviving Pods")
			}
			if !retirement.EndpointMissing && retirement.Replacement != (workload.Reference{}) {
				return Request{}, "", errors.New("a live deployment retirement cannot pin a replacement Pod")
			}
			if retirement.EndpointMissing && retirement.Replacement.Name != "" && retirement.Replacement.UID == "" {
				return Request{}, "", errors.New("a lost deployment retirement replacement Pod must be UID-pinned")
			}
			seen := map[string]bool{retirement.Endpoint.Name: true}
			for _, survivor := range retirement.Survivors {
				if survivor.UID == "" || len(validation.IsDNS1123Subdomain(survivor.Name)) != 0 || seen[survivor.Name] {
					return Request{}, "", errors.New("deployment retirement survivors must be unique, named and UID-pinned")
				}
				seen[survivor.Name] = true
			}
			if retirement.Replacement.Name != "" {
				if len(validation.IsDNS1123Subdomain(retirement.Replacement.Name)) != 0 || seen[retirement.Replacement.Name] {
					return Request{}, "", errors.New("deployment retirement replacement Pod must be unique and named")
				}
				seen[retirement.Replacement.Name] = true
			}
		default:
			return Request{}, "", errors.New("unsupported retirement workload kind")
		}
	}
	slices.SortFunc(request.Databases, func(a, b Inventory) int { return cmp.Compare(a.Database, b.Database) })
	for i := range request.Databases {
		db := &request.Databases[i]
		if db.Database != databaseNames[i] || !membership.ValidID(db.ClusterID) || len(db.Peers) < 1 || len(db.Peers) > 7 {
			return Request{}, "", errors.New("invalid database identity or initial inventory")
		}
		slices.SortFunc(db.Peers, func(a, b Peer) int { return cmp.Compare(a.Name, b.Name) })
		members, clients := []membership.Member{}, []membership.Member{}
		for j, peer := range db.Peers {
			if len(validation.IsDNS1123Subdomain(peer.Name)) != 0 || j > 0 && peer.Name == db.Peers[j-1].Name {
				return Request{}, "", errors.New("peer endpoint names must be valid and unique")
			}
			members = append(members, membership.Member{ServerID: peer.ServerID, Address: peer.Address})
			clients = append(clients, membership.Member{ServerID: peer.ServerID, Address: peer.ClientAddress})
		}
		if err := errors.Join(membership.ValidateMembers(members), membership.ValidateMembers(clients)); err != nil {
			return Request{}, "", err
		}
		if request.RemoveEndpoint != "" && !slices.ContainsFunc(db.Peers, func(p Peer) bool { return p.Name == request.RemoveEndpoint }) {
			return Request{}, "", errors.New("removal endpoint must exist in both original inventories")
		}
		if request.Add == nil && len(db.Peers) < 2 {
			return Request{}, "", errors.New("cannot remove the last member")
		}
		if (db.Approval.Name == "") != (db.Approval.UID == "") || db.Approval.Name != "" && len(validation.IsDNS1123Subdomain(db.Approval.Name)) != 0 {
			return Request{}, "", errors.New("approval must pin both name and UID")
		}
	}
	if !slices.EqualFunc(request.Databases[0].Peers, request.Databases[1].Peers, func(a, b Peer) bool { return a.Name == b.Name }) {
		return Request{}, "", errors.New("initial NB/SB inventories must describe the same central workloads")
	}
	if request.Add != nil {
		add := request.Add
		plan, _, err := add.Workload.Preparation.Canonical()
		if err != nil {
			return Request{}, "", err
		}
		add.Workload.Preparation = plan
		if plan.OperationID != request.OperationID || plan.InstallationUID != request.InstallationUID || add.Workload.InstallationGeneration != request.InstallationGeneration {
			return Request{}, "", errors.New("preparation belongs to another lifecycle operation")
		}
		for i, db := range request.Databases {
			prepared := plan.Databases[i]
			remotes := make([]string, 0, len(db.Peers))
			for _, peer := range db.Peers {
				remotes = append(remotes, peer.Address)
			}
			slices.Sort(remotes)
			if prepared.ClusterID != db.ClusterID || !slices.Equal(remotes, prepared.RemoteAddresses) {
				return Request{}, "", errors.New("preparation must bind the complete original database inventory")
			}
		}
		if err := request.launch(prepare.Result{}).Validate(); err != nil {
			return Request{}, "", err
		}
	}
	data, err = json.Marshal(request, json.Deterministic(true))
	return request, fmt.Sprintf("sha256:%x", sha256.Sum256(data)), err
}

var databaseNames = [2]string{"OVN_Northbound", "OVN_Southbound"}

func (r Request) launch(result prepare.Result) central.Request {
	launch := central.Request{FormatVersion: 1, Preparation: r.Add.Workload.Preparation, Prepared: result,
		NorthboundAddress: r.Add.NorthboundAddress, SouthboundAddress: r.Add.SouthboundAddress}
	if r.Add.Workload.TLSSecret != nil {
		launch.TLS = &central.TLSFiles{
			PrivateKey:  central.TLSMountPath + "/key",
			Certificate: central.TLSMountPath + "/cert",
			CA:          central.TLSMountPath + "/ca",
		}
	}
	for i, db := range r.Databases {
		remotes := []string{[]string{launch.NorthboundAddress, launch.SouthboundAddress}[i]}
		for _, peer := range db.Peers {
			remotes = append(remotes, peer.ClientAddress)
		}
		slices.Sort(remotes)
		if i == 0 {
			launch.NorthboundRemotes = remotes
		} else {
			launch.SouthboundRemotes = remotes
		}
	}
	return launch
}

func (r Request) inventories(state State) []Inventory {
	result := slices.Clone(r.Databases)
	for i := range result {
		result[i].Peers = slices.Clone(result[i].Peers)
		if state.Prepared != nil {
			db := state.Prepared.Databases[i]
			result[i].Peers = append(result[i].Peers, Peer{Name: state.MemberRecord.Name + "-0", ServerID: db.ServerID, Address: db.Address, ClientAddress: []string{r.Add.NorthboundAddress, r.Add.SouthboundAddress}[i]})
		}
	}
	return result
}

// RebindAutomaticReplacement updates an admitted automatic replacement with
// the fresh NB/SB RAFT and client addresses observed for its exact retained
// endpoint. Only that endpoint's four addresses may change; all Kubernetes
// identities, native server IDs, cluster IDs and every other peer remain
// pinned to the selected request. The preparation plan is canonicalized again
// so Begin persists one deterministic request containing the fresh remotes.
func RebindAutomaticReplacement(request Request, retained, observed health.CentralTopology) (Request, error) {
	if !request.AutomaticReplacement || request.Add == nil || request.RemoveEndpoint == "" || request.Retirement == nil {
		return Request{}, errors.New("automatic replacement request is incomplete")
	}
	if err := retained.Validate(); err != nil {
		return Request{}, fmt.Errorf("validate retained topology for address rebind: %w", err)
	}
	if err := observed.Validate(); err != nil {
		return Request{}, fmt.Errorf("validate observed topology for address rebind: %w", err)
	}
	canonicalRequest, _, err := canonical(request)
	if err != nil {
		return Request{}, err
	}
	request = canonicalRequest
	if len(request.Databases) != len(databaseNames) {
		return Request{}, errors.New("automatic replacement request has an invalid database inventory")
	}
	if len(retained.Members) != len(observed.Members) {
		return Request{}, errors.New("automatic replacement observation changed the retained member count")
	}
	retainedIndex := slices.IndexFunc(retained.Members, func(member health.CentralIdentity) bool { return member.Pod.Name == request.RemoveEndpoint })
	observedIndex := slices.IndexFunc(observed.Members, func(member health.CentralIdentity) bool { return member.Pod.Name == request.RemoveEndpoint })
	if retainedIndex < 0 || observedIndex < 0 {
		return Request{}, errors.New("automatic replacement target is missing from retained or observed topology")
	}
	changed := false
	for database := range request.Databases {
		if request.Databases[database].Database != observed.Members[0].Databases[database].Database {
			return Request{}, errors.New("automatic replacement database order changed")
		}
		for peerIndex := range request.Databases[database].Peers {
			peer := &request.Databases[database].Peers[peerIndex]
			retainedPeer := retained.Members[retainedIndex].Databases[database]
			observedPeer := observed.Members[observedIndex].Databases[database]
			if peer.Name != retained.Members[retainedIndex].Pod.Name {
				continue
			}
			if peer.ServerID != retainedPeer.ServerID || observedPeer.ServerID != retainedPeer.ServerID || observedPeer.ClusterID != retainedPeer.ClusterID || observedPeer.Database != retainedPeer.Database {
				return Request{}, errors.New("automatic replacement target database identity changed")
			}
			if peer.Address != observedPeer.Address || peer.ClientAddress != observedPeer.ClientAddress {
				changed = true
			}
			peer.Address = observedPeer.Address
			peer.ClientAddress = observedPeer.ClientAddress
		}
	}
	if !changed {
		return Request{}, errors.New("automatic replacement did not change the target endpoint addresses")
	}
	for database := range request.Databases {
		if !sameNonTargetPeers(request.Databases[database], retained, observed, request.RemoveEndpoint, database) {
			return Request{}, errors.New("automatic replacement changed a non-target peer")
		}
	}
	plan := request.Add.Workload.Preparation
	for database := range plan.Databases {
		if database >= len(request.Databases) || plan.Databases[database].Name != request.Databases[database].Database {
			return Request{}, errors.New("automatic replacement preparation database order changed")
		}
		remotes := make([]string, 0, len(request.Databases[database].Peers))
		for _, peer := range request.Databases[database].Peers {
			remotes = append(remotes, peer.Address)
		}
		slices.Sort(remotes)
		plan.Databases[database].RemoteAddresses = remotes
	}
	canonicalPlan, _, err := plan.Canonical()
	if err != nil {
		return Request{}, fmt.Errorf("canonicalize rebound preparation: %w", err)
	}
	request.Add.Workload.Preparation = canonicalPlan
	request, _, err = canonical(request)
	return request, err
}

func sameNonTargetPeers(inventory Inventory, retained, observed health.CentralTopology, target string, database int) bool {
	for _, peer := range inventory.Peers {
		if peer.Name == target {
			continue
		}
		retainedIndex := slices.IndexFunc(retained.Members, func(member health.CentralIdentity) bool { return member.Pod.Name == peer.Name })
		observedIndex := slices.IndexFunc(observed.Members, func(member health.CentralIdentity) bool { return member.Pod.Name == peer.Name })
		if retainedIndex < 0 || observedIndex < 0 {
			return false
		}
		oldDB, newDB := retained.Members[retainedIndex].Databases[database], observed.Members[observedIndex].Databases[database]
		if peer.ServerID != oldDB.ServerID || peer.Address != oldDB.Address || peer.ClientAddress != oldDB.ClientAddress || oldDB != newDB {
			return false
		}
	}
	return true
}

func (db Inventory) endpoints() []ovnadmin.Endpoint {
	result := make([]ovnadmin.Endpoint, 0, len(db.Peers))
	for _, peer := range db.Peers {
		result = append(result, ovnadmin.Endpoint{Name: peer.Name, ServerID: peer.ServerID, Address: peer.Address})
	}
	return result
}

func validDigest(value string) bool {
	return len(value) == 71 && strings.HasPrefix(value, "sha256:") && strings.Trim(value[7:], "0123456789abcdef") == ""
}
