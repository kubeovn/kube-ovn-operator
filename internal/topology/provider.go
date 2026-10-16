// Package topology retains the last admitted complete central identities.
// This baseline survives health failures and is never mutation authorization.
package topology

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Provider interface {
	VerifyRetained(context.Context, *api.KubeOVNInstallation) (*api.CentralTopologyReference, error)
	Ensure(context.Context, *api.KubeOVNInstallation) (*api.CentralTopologyReference, error)
}

// HandoffProvider admits a new immutable baseline only after the caller has
// independently verified a completed lifecycle receipt and fresh final state.
// It is optional on the Provider seam so plan-only and test adapters cannot
// accidentally claim lifecycle authorization.
type HandoffProvider interface {
	Provider
	Handoff(context.Context, *api.KubeOVNInstallation, *api.CentralTopologyReference) (*api.CentralTopologyReference, error)
}

// Kubernetes requires an uncached client and both installation-bound Leases.
// Ensure returns a validated retained reference even when live discovery fails.
// The caller persists that reference before publishing the observation failure.
type Kubernetes struct {
	Client client.Client
	Runner health.CommandRunner
}

type record struct {
	FormatVersion          int                    `json:"formatVersion"`
	InstallationUID        string                 `json:"installationUID"`
	InstallationGeneration int64                  `json:"installationGeneration"`
	Topology               health.CentralTopology `json:"topology"`
}

// VerifyRetained checks an existing baseline before workload reapplication. It
// returns no reference when no baseline exists, allowing the first rollout.
func (p *Kubernetes) VerifyRetained(ctx context.Context, installation *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
	return p.ensure(ctx, installation, false)
}

func (p *Kubernetes) Ensure(ctx context.Context, installation *api.KubeOVNInstallation) (*api.CentralTopologyReference, error) {
	return p.ensure(ctx, installation, true)
}

// Handoff records a newly observed topology under a new immutable name. The
// previous ConfigMap remains retained for audit and is never deleted or
// mutated. The caller is responsible for proving the lifecycle receipt before
// invoking this method.
func (p *Kubernetes) Handoff(ctx context.Context, installation *api.KubeOVNInstallation, previous *api.CentralTopologyReference) (*api.CentralTopologyReference, error) {
	if previous == nil || installation == nil || installation.Status.Topology.Inventory == nil || *installation.Status.Topology.Inventory != *previous {
		return nil, errors.New("topology handoff requires the currently pinned baseline")
	}
	if p.Client == nil || p.Runner == nil {
		return nil, errors.New("central topology handoff requires a direct client and Pod runner")
	}
	if err := p.guard(ctx, installation); err != nil {
		return nil, err
	}
	observed, err := health.DiscoverCentralTopology(ctx, p.Client, p.Runner, installation)
	if err != nil {
		return nil, err
	}
	previousObject := new(corev1.ConfigMap)
	if err := p.Client.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: previous.Name}, previousObject); err != nil {
		return nil, err
	}
	previousRecord, _, err := readRecord(previousObject, installation)
	if err != nil {
		return nil, err
	}
	if previousRecord.Topology.SameIdentity(observed) {
		return nil, errors.New("central topology handoff did not change the retained identity")
	}
	data, err := json.Marshal(record{FormatVersion: 1, InstallationUID: string(installation.UID), InstallationGeneration: installation.Generation, Topology: observed}, json.Deterministic(true))
	if err != nil || len(data) > 64<<10 {
		return nil, errors.New("invalid or oversized central topology handoff")
	}
	installationDigest := sha256.Sum256([]byte(installation.UID))
	topologyDigest := sha256.Sum256(data)
	name := fmt.Sprintf("ovn-topology-%x-%x", installationDigest[:8], topologyDigest[:16])
	object := new(corev1.ConfigMap)
	err = p.Client.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: name}, object)
	if apierrors.IsNotFound(err) {
		if err := p.guard(ctx, installation); err != nil {
			return nil, err
		}
		object = &corev1.ConfigMap{Name: name, Namespace: installation.Namespace, Immutable: new(true), Labels: labels(installation), Data: map[string]string{"topology.json": string(data)}}
		if err := p.Client.Create(ctx, object); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	// The newly created record cannot match the still-pinned previous status;
	// validate its immutable contents before the controller publishes the new
	// status reference.
	candidate := installation.DeepCopy()
	candidate.Status.Topology.Inventory = nil
	_, ref, err := readRecord(object, candidate)
	if err != nil {
		return nil, err
	}
	if !savedTopologyMatches(ref, data, object) {
		return nil, errors.New("central topology handoff record changed")
	}
	return ref, p.guard(ctx, installation)
}

func savedTopologyMatches(ref *api.CentralTopologyReference, data []byte, object *corev1.ConfigMap) bool {
	return ref != nil && object.UID != "" && object.Data["topology.json"] == string(data) && ref.Digest == fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

func (p *Kubernetes) ensure(ctx context.Context, installation *api.KubeOVNInstallation, initialize bool) (*api.CentralTopologyReference, error) {
	if p.Client == nil || p.Runner == nil || installation == nil || installation.UID == "" || installation.Generation < 1 {
		return nil, errors.New("central topology requires an identified installation, direct client and Pod runner")
	}
	if err := p.guard(ctx, installation); err != nil {
		return nil, err
	}
	name := fmt.Sprintf("ovn-topology-%x", sha256.Sum256([]byte(installation.UID)))[:45]
	if installation.Status.Topology.Inventory != nil {
		name = installation.Status.Topology.Inventory.Name
	}
	object := new(corev1.ConfigMap)
	err := p.Client.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: name}, object)
	var saved record
	var ref *api.CentralTopologyReference
	switch {
	case err == nil:
		saved, ref, err = readRecord(object, installation)
		if err != nil {
			return nil, err
		}
	case apierrors.IsNotFound(err):
		if installation.Status.Topology.Inventory != nil {
			return nil, errors.New("retained central topology is missing; refusing to replace its identity")
		}
	case err != nil:
		return nil, err
	}
	if ref == nil && !initialize {
		return nil, nil
	}
	observed, err := health.DiscoverCentralTopology(ctx, p.Client, p.Runner, installation)
	if err != nil {
		return ref, err
	}
	if err := p.guard(ctx, installation); err != nil {
		return ref, err
	}
	if ref != nil {
		if !saved.Topology.SameIdentity(observed) {
			return ref, errors.New("central topology changed without a completed lifecycle handoff")
		}
		// Refuse a deleted/replaced record even if the live topology is unchanged.
		previousUID := object.UID
		if err := p.Client.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			return ref, err
		}
		_, currentRef, err := readRecord(object, installation)
		if err != nil {
			return ref, err
		}
		if object.UID != previousUID || *currentRef != *ref {
			return ref, errors.New("central topology record changed during observation")
		}
		return ref, p.guard(ctx, installation)
	}
	saved = record{FormatVersion: 1, InstallationUID: string(installation.UID), InstallationGeneration: installation.Generation, Topology: observed}
	data, err := json.Marshal(saved, json.Deterministic(true))
	if err != nil || len(data) > 64<<10 {
		return nil, errors.New("invalid or oversized central topology")
	}
	object = &corev1.ConfigMap{Name: name, Namespace: installation.Namespace, Immutable: new(true), Labels: labels(installation), Data: map[string]string{"topology.json": string(data)}}
	if err := p.guard(ctx, installation); err != nil {
		return nil, err
	}
	if err := p.Client.Create(ctx, object); err != nil {
		return nil, err
	}
	uid := object.UID
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
		return nil, err
	}
	_, ref, err = readRecord(object, installation)
	if err != nil {
		return nil, err
	}
	if object.UID != uid || object.Data["topology.json"] != string(data) {
		return nil, errors.New("central topology record changed during creation")
	}
	return ref, p.guard(ctx, installation)
}

func (p *Kubernetes) guard(ctx context.Context, installation *api.KubeOVNInstallation) error {
	if err := lease.CheckFor(ctx, lease.Central, installation.UID); err != nil {
		return err
	}
	current := new(api.KubeOVNInstallation)
	if err := p.Client.Get(ctx, client.ObjectKeyFromObject(installation), current); err != nil {
		return err
	}
	if current.UID != installation.UID || current.Generation != installation.Generation || current.DeletionTimestamp != nil || !reflect.DeepEqual(current.Spec, installation.Spec) || !reflect.DeepEqual(current.Status.Topology.Inventory, installation.Status.Topology.Inventory) {
		return errors.New("installation identity, desired state or topology reference changed")
	}
	return nil
}

func readRecord(object *corev1.ConfigMap, installation *api.KubeOVNInstallation) (record, *api.CentralTopologyReference, error) {
	var saved record
	if object.UID == "" || object.DeletionTimestamp != nil || object.Immutable == nil || !*object.Immutable || len(object.OwnerReferences) != 0 || len(object.BinaryData) != 0 || len(object.Data) != 1 || len(object.Data["topology.json"]) == 0 || len(object.Data["topology.json"]) > 64<<10 {
		return saved, nil, errors.New("invalid retained central topology record")
	}
	for key, value := range labels(installation) {
		if object.Labels[key] != value {
			return saved, nil, errors.New("central topology record belongs to another installation")
		}
	}
	data := []byte(object.Data["topology.json"])
	if err := json.Unmarshal(data, &saved, json.RejectUnknownMembers(true)); err != nil {
		return saved, nil, errors.New("invalid retained central topology data")
	}
	if saved.FormatVersion != 1 || saved.InstallationUID != string(installation.UID) || saved.InstallationGeneration < 1 || saved.InstallationGeneration > installation.Generation || len(saved.Topology.Members) == 0 {
		return saved, nil, errors.New("retained central topology has an invalid installation identity")
	}
	if err := saved.Topology.Validate(); err != nil {
		return saved, nil, err
	}
	ref := &api.CentralTopologyReference{Name: object.Name, UID: object.UID, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(data))}
	if pinned := installation.Status.Topology.Inventory; pinned != nil && *pinned != *ref {
		return saved, nil, errors.New("central topology record differs from its pinned UID or digest")
	}
	return saved, ref, nil
}

func labels(installation *api.KubeOVNInstallation) map[string]string {
	return map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: installation.Name, apply.InstallationNamespaceLabel: installation.Namespace, apply.InstallationUIDLabel: string(installation.UID)}
}
