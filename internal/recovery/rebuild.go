// Package recovery defines the offline reconstruction contract for a lost OVN
// quorum. It validates an immutable plan but never creates databases, starts a
// process, replaces storage, or changes Kubernetes resources.
package recovery

import (
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"k8s.io/apimachinery/pkg/util/validation"
)

const FormatVersion = 1

type Approval struct {
	Name           string `json:"name"`
	UID            string `json:"uid"`
	DecisionDigest string `json:"decisionDigest"`
}

type StorageIdentity struct {
	NodeUID     string `json:"nodeUID"`
	WorkloadUID string `json:"workloadUID"`
	PodUID      string `json:"podUID"`
	ContainerID string `json:"containerID"`
	VolumeUID   string `json:"volumeUID"`
}

// Validate checks that all immutable ownership components are present.
func (s StorageIdentity) Validate() error {
	return validateStorage(s)
}

type Member struct {
	Name                  string          `json:"name"`
	NodeName              string          `json:"nodeName"`
	Storage               StorageIdentity `json:"storage"`
	NorthboundRAFTAddress string          `json:"northboundRAFTAddress"`
	SouthboundRAFTAddress string          `json:"southboundRAFTAddress"`
	NorthboundClient      string          `json:"northboundClient"`
	SouthboundClient      string          `json:"southboundClient"`
}

type Database struct {
	Name            string `json:"name"`
	BackupRef       string `json:"backupRef"`
	SourceClusterID string `json:"sourceClusterID"`
	TargetClusterID string `json:"targetClusterID"`
}

type Plan struct {
	FormatVersion   int        `json:"formatVersion"`
	InstallationUID string     `json:"installationUID"`
	OperationID     string     `json:"operationID"`
	SourceVersion   string     `json:"sourceVersion"`
	TargetVersion   string     `json:"targetVersion"`
	BackupPairID    string     `json:"backupPairID"`
	Approval        Approval   `json:"approval"`
	RetainedMembers []Member   `json:"retainedMembers"`
	TargetMembers   []Member   `json:"targetMembers"`
	Databases       []Database `json:"databases"`
}

// Canonical validates and deterministically orders a plan. The returned copy
// owns all slices and maps; callers may persist it without retaining mutable
// input. The digest binds every field, including storage and approval identity.
func (p Plan) Canonical() (Plan, string, error) {
	if p.FormatVersion != FormatVersion {
		return Plan{}, "", errors.New("unsupported offline recovery plan format")
	}
	if strings.TrimSpace(p.InstallationUID) == "" || len(p.InstallationUID) > 128 {
		return Plan{}, "", errors.New("offline recovery requires an installation UID")
	}
	if !membership.ValidID(p.OperationID) {
		return Plan{}, "", errors.New("offline recovery requires a full operation UUID")
	}
	if strings.TrimSpace(p.SourceVersion) == "" || strings.TrimSpace(p.TargetVersion) == "" {
		return Plan{}, "", errors.New("offline recovery requires source and target versions")
	}
	if err := validateApproval(p.Approval); err != nil {
		return Plan{}, "", err
	}
	if err := validatePairID(p.BackupPairID, p.Databases); err != nil {
		return Plan{}, "", err
	}
	if err := validateMembers("retained", p.RetainedMembers, false); err != nil {
		return Plan{}, "", err
	}
	if err := validateMembers("target", p.TargetMembers, true); err != nil {
		return Plan{}, "", err
	}
	if err := validateStorageSeparation(p.RetainedMembers, p.TargetMembers); err != nil {
		return Plan{}, "", err
	}
	retainedNames := make(map[string]struct{}, len(p.RetainedMembers))
	for _, member := range p.RetainedMembers {
		retainedNames[member.Name] = struct{}{}
	}
	for _, member := range p.TargetMembers {
		if _, exists := retainedNames[member.Name]; exists {
			return Plan{}, "", fmt.Errorf("target member %q reuses a retained workload name", member.Name)
		}
	}
	p.RetainedMembers = slices.Clone(p.RetainedMembers)
	p.TargetMembers = slices.Clone(p.TargetMembers)
	p.Databases = slices.Clone(p.Databases)
	slices.SortFunc(p.RetainedMembers, func(a, b Member) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(p.TargetMembers, func(a, b Member) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(p.Databases, func(a, b Database) int { return strings.Compare(a.Name, b.Name) })
	data, err := json.Marshal(p)
	if err != nil {
		return Plan{}, "", fmt.Errorf("marshal offline recovery plan: %w", err)
	}
	return p, fmt.Sprintf("sha256:%x", sha256.Sum256(data)), nil
}

// CanonicalDataDigest returns the digest of the exact canonical JSON bytes
// that a plan ConfigMap must mount into the rebuild workload.
func (p Plan) CanonicalDataDigest() (string, error) {
	canonical, _, err := p.Canonical()
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal canonical offline recovery plan: %w", err)
	}
	return ConfigMapDataDigest(string(data)), nil
}

func validateApproval(approval Approval) error {
	if strings.TrimSpace(approval.Name) == "" || strings.TrimSpace(approval.UID) == "" {
		return errors.New("offline recovery requires a named approval and approval UID")
	}
	if !validSHA256(approval.DecisionDigest) {
		return errors.New("offline recovery approval decision digest must be a SHA-256 value")
	}
	return nil
}

func validatePairID(pairID string, databases []Database) error {
	if len(databases) != 2 {
		return errors.New("offline recovery requires exactly the NB and SB backup references")
	}
	refs := make(map[string]string, len(databases))
	targetClusters := make(map[string]string, len(databases))
	for _, database := range databases {
		if database.Name != "OVN_Northbound" && database.Name != "OVN_Southbound" {
			return fmt.Errorf("unsupported offline recovery database %q", database.Name)
		}
		if _, exists := refs[database.Name]; exists || !validSHA256(database.BackupRef) {
			return errors.New("offline recovery database references must be unique SHA-256 values")
		}
		if !membership.ValidID(database.SourceClusterID) || !membership.ValidID(database.TargetClusterID) || database.SourceClusterID == database.TargetClusterID {
			return fmt.Errorf("offline recovery %s must replace its source cluster with a distinct target cluster", database.Name)
		}
		if previous, exists := targetClusters[database.TargetClusterID]; exists {
			return fmt.Errorf("offline recovery target cluster ID is shared by %s and %s", previous, database.Name)
		}
		targetClusters[database.TargetClusterID] = database.Name
		refs[database.Name] = database.BackupRef
	}
	if refs["OVN_Northbound"] == "" || refs["OVN_Southbound"] == "" {
		return errors.New("offline recovery requires both OVN_Northbound and OVN_Southbound references")
	}
	data, err := json.Marshal(refs, json.Deterministic(true))
	if err != nil || pairID != fmt.Sprintf("sha256:%x", sha256.Sum256(data)) {
		return errors.New("offline recovery backup pair ID does not match its database references")
	}
	return nil
}

func validateMembers(role string, members []Member, target bool) error {
	if len(members) == 0 {
		return fmt.Errorf("offline recovery requires at least one %s member", role)
	}
	if target && (len(members) > 5 || len(members)%2 == 0) {
		return errors.New("offline recovery target members must be one, three or five members")
	}
	names := make(map[string]struct{}, len(members))
	raft := make(map[string]struct{}, len(members)*2)
	clients := make(map[string]struct{}, len(members)*2)
	for _, member := range members {
		if len(validation.IsDNS1123Label(member.Name)) != 0 || strings.TrimSpace(member.NodeName) == "" {
			return fmt.Errorf("offline recovery %s member requires a DNS name and node", role)
		}
		if _, exists := names[member.Name]; exists {
			return fmt.Errorf("offline recovery %s member names must be unique", role)
		}
		names[member.Name] = struct{}{}
		if err := validateStorage(member.Storage); err != nil {
			return fmt.Errorf("offline recovery %s member %q: %w", role, member.Name, err)
		}
		for _, address := range []string{member.NorthboundRAFTAddress, member.SouthboundRAFTAddress} {
			if err := validateEndpoint(address); err != nil {
				return fmt.Errorf("offline recovery %s member %q: %w", role, member.Name, err)
			}
			if _, exists := raft[address]; exists {
				return errors.New("offline recovery RAFT addresses must be unique")
			}
			raft[address] = struct{}{}
		}
		for _, address := range []string{member.NorthboundClient, member.SouthboundClient} {
			if err := validateEndpoint(address); err != nil {
				return fmt.Errorf("offline recovery %s member %q: %w", role, member.Name, err)
			}
			if _, exists := clients[address]; exists {
				return errors.New("offline recovery client addresses must be unique")
			}
			clients[address] = struct{}{}
		}
	}
	return nil
}

func validateStorage(storage StorageIdentity) error {
	for _, value := range []string{storage.NodeUID, storage.WorkloadUID, storage.PodUID, storage.ContainerID, storage.VolumeUID} {
		if strings.TrimSpace(value) == "" || len(value) > 256 {
			return errors.New("storage ownership requires node, workload, pod, container and volume identities")
		}
	}
	return nil
}

func validateStorageSeparation(retained, target []Member) error {
	owned := make(map[string]string, (len(retained)+len(target))*5)
	for _, member := range retained {
		for field, value := range storageValues(member.Storage) {
			if previous, exists := owned[value]; exists {
				return fmt.Errorf("retained storage identity %s is shared by %s and %s", value, previous, member.Name+"."+field)
			}
			owned[value] = member.Name + "." + field
		}
	}
	for _, member := range target {
		for field, value := range storageValues(member.Storage) {
			if previous, exists := owned[value]; exists {
				return fmt.Errorf("target storage identity %s reuses %s from %s", value, member.Name+"."+field, previous)
			}
			owned[value] = member.Name + "." + field
		}
	}
	return nil
}

func storageValues(storage StorageIdentity) map[string]string {
	return map[string]string{"nodeUID": storage.NodeUID, "workloadUID": storage.WorkloadUID, "podUID": storage.PodUID, "containerID": storage.ContainerID, "volumeUID": storage.VolumeUID}
}

func validateEndpoint(address string) error {
	if err := membership.ValidateMembers([]membership.Member{{ServerID: "11111111-1111-1111-1111-111111111111", Address: address}}); err != nil {
		return errors.New("offline recovery endpoints must be canonical literal-IP tcp or ssl addresses")
	}
	return nil
}

func validSHA256(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+64 {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}
