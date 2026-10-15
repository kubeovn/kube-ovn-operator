// Package prepare creates restart-safe NB/SB files for a new central member.
// It never starts a server, bootstraps a cluster or replaces existing data.
package prepare

import (
	"cmp"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Plan must be persisted by the lifecycle executor before it is sent to a new
// member. MemberName identifies the stable workload slot, not a reusable Pod UID.
// Addresses must come from live, validated cluster inventory. This local helper
// cannot prove remote quorum or authorize a Kubernetes workload mutation.
type Plan struct {
	FormatVersion   int        `json:"formatVersion"`
	InstallationUID string     `json:"installationUID"`
	OperationID     string     `json:"operationID"`
	MemberName      string     `json:"memberName"`
	Databases       []Database `json:"databases"`
}

type Database struct {
	Name            string   `json:"name"`
	ClusterID       string   `json:"clusterID"`
	LocalAddress    string   `json:"localAddress"`
	RemoteAddresses []string `json:"remoteAddresses"`
}

// Result pins the native generated server UUIDs. Paths are relative to the
// persistent root and never point at the installation's existing database files.
type Result struct {
	PlanDigest string             `json:"planDigest"`
	Databases  []PreparedDatabase `json:"databases"`
}

type PreparedDatabase struct {
	Name      string `json:"name"`
	ClusterID string `json:"clusterID"`
	ServerID  string `json:"serverID"`
	Address   string `json:"address"`
	Path      string `json:"path"`
}

// Canonical validates a plan and returns an independent, deterministically
// ordered copy and its content digest for durable orchestration records.
func (plan Plan) Canonical() (Plan, string, error) { return canonical(plan) }

// Validate checks a published result without reading the native files. The
// launcher must still Acquire those files before starting any native process.
func (result Result) Validate(plan Plan) error {
	plan, digest, err := canonical(plan)
	if err != nil {
		return err
	}
	if result.PlanDigest != digest || len(result.Databases) != len(plan.Databases) {
		return errors.New("prepared result does not match the plan")
	}
	for i, db := range plan.Databases {
		got := result.Databases[i]
		if got.Name != db.Name || got.ClusterID != db.ClusterID || got.Address != db.LocalAddress ||
			!membership.ValidID(got.ServerID) || got.Path != plan.OperationID+"/"+databaseFile(db.Name) {
			return errors.New("prepared database identity does not match the plan")
		}
	}
	return nil
}

func canonical(plan Plan) (Plan, string, error) {
	if plan.FormatVersion != 1 {
		return Plan{}, "", errors.New("unsupported preparation format version")
	}
	if strings.TrimSpace(plan.InstallationUID) == "" || len(plan.InstallationUID) > 128 || !membership.ValidID(plan.OperationID) || len(validation.IsDNS1123Label(plan.MemberName)) != 0 {
		return Plan{}, "", errors.New("preparation requires an installation UID, operation UUID and member name")
	}
	if len(plan.Databases) != 2 {
		return Plan{}, "", errors.New("preparation requires both OVN databases")
	}
	plan.Databases = slices.Clone(plan.Databases)
	slices.SortFunc(plan.Databases, func(a, b Database) int { return cmp.Compare(a.Name, b.Name) })
	for i := range plan.Databases {
		db := &plan.Databases[i]
		if db.Name != []string{"OVN_Northbound", "OVN_Southbound"}[i] || !membership.ValidID(db.ClusterID) || len(db.RemoteAddresses) == 0 || len(db.RemoteAddresses) > 7 {
			return Plan{}, "", errors.New("invalid database identity or remote inventory")
		}
		if err := validAddress(db.LocalAddress); err != nil {
			return Plan{}, "", err
		}
		db.RemoteAddresses = slices.Clone(db.RemoteAddresses)
		slices.Sort(db.RemoteAddresses)
		for j, address := range db.RemoteAddresses {
			if err := validAddress(address); err != nil {
				return Plan{}, "", err
			}
			localProtocol, _, _ := strings.Cut(db.LocalAddress, ":")
			protocol, _, _ := strings.Cut(address, ":")
			if address == db.LocalAddress || (j > 0 && address == db.RemoteAddresses[j-1]) || localProtocol != protocol {
				return Plan{}, "", errors.New("remote addresses must be distinct peers using the local RAFT protocol")
			}
		}
	}
	if plan.Databases[0].LocalAddress == plan.Databases[1].LocalAddress {
		return Plan{}, "", errors.New("NB and SB cannot use the same local RAFT endpoint")
	}
	content, err := json.Marshal(plan)
	if err != nil {
		return Plan{}, "", err
	}
	return plan, fmt.Sprintf("sha256:%x", sha256.Sum256(content)), nil
}

func validAddress(address string) error {
	if len(address) > 80 {
		return errors.New("RAFT address exceeds 80 bytes")
	}
	return membership.ValidateMembers([]membership.Member{{ServerID: "11111111-1111-1111-1111-111111111111", Address: address}})
}

func databaseFile(name string) string {
	if name == "OVN_Northbound" {
		return "ovnnb_db.db"
	}
	return "ovnsb_db.db"
}
