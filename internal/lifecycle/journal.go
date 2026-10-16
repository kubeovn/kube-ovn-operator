package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/membership"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Phase string

// Reference pins both the installation journal and its immutable request. An
// old caller cannot accidentally drive a later operation using the same slot.
type Reference struct {
	Journal workload.Reference `json:"journal"`
	Request workload.Reference `json:"request"`
}

const (
	Preparing  Phase = "Preparing"
	Starting   Phase = "Starting"
	CatchingUp Phase = "CatchingUp"
	Removing   Phase = "Removing"
	Retiring   Phase = "Retiring"
	Complete   Phase = "Complete"
)

type DatabaseState struct {
	BackupRef        string             `json:"backupRef,omitempty"`
	CapturedAt       time.Time          `json:"capturedAt,omitempty"`
	Intent           *membership.Change `json:"intent,omitempty"`
	ApprovalReserved bool               `json:"approvalReserved,omitzero"`
}

// State contains durable progress, never reusable authorization. Step obtains
// fresh membership and backup evidence before each native/workload mutation.
type State struct {
	Phase        Phase               `json:"phase"`
	MemberRecord workload.Reference  `json:"memberRecord"`
	Prepared     *prepare.Result     `json:"prepared,omitempty"`
	Running      workload.Reference  `json:"running"`
	Databases    [2]DatabaseState    `json:"databases"`
	Final        []ovnadmin.Snapshot `json:"final,omitempty"`
}

type journal struct {
	Request workload.Reference `json:"request"`
	Digest  string             `json:"digest"`
	State   State              `json:"state"`
}

func (e *Executor) guard(ctx context.Context) error {
	if err := lease.CheckFor(ctx, lease.Central, e.installation.UID); err != nil {
		return err
	}
	current := new(api.KubeOVNInstallation)
	if err := e.kube.Get(ctx, client.ObjectKeyFromObject(e.installation), current); err != nil {
		return err
	}
	if current.UID != e.installation.UID || current.Generation != e.installation.Generation || current.DeletionTimestamp != nil {
		return errors.New("installation identity or generation changed during lifecycle operation")
	}
	return nil
}

func (e *Executor) labels() map[string]string {
	return map[string]string{apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: e.installation.Name,
		apply.InstallationNamespaceLabel: e.installation.Namespace, apply.InstallationUIDLabel: string(e.installation.UID)}
}

func (e *Executor) checkRecord(record *corev1.ConfigMap, immutable bool) error {
	if record.UID == "" || record.DeletionTimestamp != nil || len(record.OwnerReferences) != 0 || len(record.BinaryData) != 0 || len(record.Data) != 1 || len(record.Data["record.json"]) == 0 || len(record.Data["record.json"]) > 64<<10 || (record.Immutable != nil && *record.Immutable) != immutable {
		return errors.New("lifecycle record identity or contents changed")
	}
	for key, value := range e.labels() {
		if record.Labels[key] != value {
			return errors.New("lifecycle record belongs to another installation")
		}
	}
	return nil
}

func (e *Executor) record(ctx context.Context, name string, value any, immutable bool) (*corev1.ConfigMap, error) {
	data, err := json.Marshal(value, json.Deterministic(true))
	if err != nil || len(data) > 64<<10 {
		return nil, errors.New("invalid or oversized lifecycle record")
	}
	want := &corev1.ConfigMap{Name: name, Namespace: e.installation.Namespace, Labels: e.labels(), Immutable: new(immutable), Data: map[string]string{"record.json": string(data)}}
	got := new(corev1.ConfigMap)
	err = e.kube.Get(ctx, client.ObjectKeyFromObject(want), got)
	if apierrors.IsNotFound(err) {
		if err := e.guard(ctx); err != nil {
			return nil, err
		}
		if err := e.kube.Create(ctx, want); err != nil {
			return nil, err
		}
		if err := e.kube.Get(ctx, client.ObjectKeyFromObject(want), got); err != nil {
			return nil, err
		}
		if got.UID != want.UID {
			return nil, errors.New("lifecycle record replaced during create")
		}
	} else if err != nil {
		return nil, err
	}
	if err := e.checkRecord(got, immutable); err != nil {
		return nil, err
	}
	if got.Data["record.json"] != string(data) {
		return nil, errors.New("lifecycle record conflicts with persisted operation")
	}
	return got, nil
}

func (e *Executor) activeName() string {
	return fmt.Sprintf("ovn-operation-%x", sha256.Sum256([]byte(e.installation.UID)))[:46]
}

func (e *Executor) load(ctx context.Context, ref Reference, current bool) (*corev1.ConfigMap, Request, journal, error) {
	var saved journal
	if err := e.guard(ctx); err != nil {
		return nil, Request{}, saved, err
	}
	if ref.Journal.Name != e.activeName() || ref.Journal.UID == "" || ref.Request.UID == "" {
		return nil, Request{}, saved, errors.New("pinned installation operation reference is required")
	}
	state := new(corev1.ConfigMap)
	if err := e.kube.Get(ctx, types.NamespacedName{Namespace: e.installation.Namespace, Name: ref.Journal.Name}, state); err != nil {
		return nil, Request{}, saved, err
	}
	if state.UID != ref.Journal.UID {
		return nil, Request{}, saved, errors.New("operation journal was replaced")
	}
	if err := e.checkRecord(state, false); err != nil {
		return nil, Request{}, saved, err
	}
	if err := json.Unmarshal([]byte(state.Data["record.json"]), &saved, json.RejectUnknownMembers(true)); err != nil {
		return nil, Request{}, saved, err
	}
	if saved.Request != ref.Request {
		return nil, Request{}, saved, errors.New("operation reference no longer owns the installation journal")
	}
	requestRecord := new(corev1.ConfigMap)
	if err := e.kube.Get(ctx, types.NamespacedName{Namespace: e.installation.Namespace, Name: saved.Request.Name}, requestRecord); err != nil {
		return nil, Request{}, saved, err
	}
	if requestRecord.UID != saved.Request.UID {
		return nil, Request{}, saved, errors.New("immutable operation request was replaced")
	}
	if err := e.checkRecord(requestRecord, true); err != nil {
		return nil, Request{}, saved, err
	}
	var request Request
	if err := json.Unmarshal([]byte(requestRecord.Data["record.json"]), &request, json.RejectUnknownMembers(true)); err != nil {
		return nil, Request{}, saved, err
	}
	request, digest, err := canonical(request)
	if err != nil {
		return nil, Request{}, saved, err
	}
	if digest != saved.Digest || request.InstallationUID != string(e.installation.UID) || current && request.InstallationGeneration != e.installation.Generation {
		return nil, Request{}, saved, errors.New("operation request identity or digest changed")
	}
	if err := validateState(request, saved.State); err != nil {
		return nil, Request{}, saved, err
	}
	return state, request, saved, nil
}

func (e *Executor) save(ctx context.Context, object *corev1.ConfigMap, saved journal) error {
	if err := e.guard(ctx); err != nil {
		return err
	}
	data, err := json.Marshal(saved, json.Deterministic(true))
	if err != nil || len(data) > 64<<10 {
		return errors.New("invalid or oversized lifecycle progress")
	}
	object.Data = map[string]string{"record.json": string(data)}
	// Update includes resourceVersion. A conflict never becomes a blind retry.
	if err := e.kube.Update(ctx, object); err != nil {
		return fmt.Errorf("persist lifecycle progress: %w", err)
	}
	got := new(corev1.ConfigMap)
	if err := e.kube.Get(ctx, client.ObjectKeyFromObject(object), got); err != nil {
		return err
	}
	if got.UID != object.UID || !reflect.DeepEqual(got.Data, object.Data) {
		return errors.New("lifecycle progress changed during write")
	}
	return e.checkRecord(got, false)
}

func validateState(request Request, state State) error {
	switch state.Phase {
	case Preparing, Starting, CatchingUp, Removing, Retiring, Complete:
	default:
		return errors.New("invalid lifecycle phase")
	}
	if (state.MemberRecord.Name == "") != (state.MemberRecord.UID == "") || (state.Running.Name == "") != (state.Running.UID == "") || state.Phase == Preparing && (state.Prepared != nil || state.Running.Name != "") || state.Phase == Starting && state.Running.Name != "" {
		return errors.New("inconsistent preparation or workload identity in lifecycle phase")
	}
	if request.Add == nil {
		if state.Prepared != nil || state.MemberRecord.Name != "" || state.Running.Name != "" || state.Phase == Preparing || state.Phase == Starting || state.Phase == CatchingUp {
			return errors.New("removal-only operation has invalid member preparation state")
		}
	} else if state.Phase != Preparing {
		if state.Prepared == nil || state.MemberRecord.Name == "" || state.MemberRecord.UID == "" {
			return errors.New("lifecycle phase requires recorded preparation identities")
		}
		if err := state.Prepared.Validate(request.Add.Workload.Preparation); err != nil {
			return err
		}
		if state.Phase != Starting && (state.Running.Name != state.MemberRecord.Name || state.Running.UID == "") {
			return errors.New("lifecycle phase requires a pinned running workload")
		}
	}
	for i, db := range state.Databases {
		if db.ApprovalReserved && request.Databases[i].Approval.UID == "" {
			return errors.New("approval reservation has no pinned decision")
		}
		if db.Intent != nil {
			if state.Phase != Removing && state.Phase != Retiring && state.Phase != Complete {
				return errors.New("removal intent precedes joint catch-up")
			}
			old := peerNamed(request.Databases[i], request.RemoveEndpoint)
			change := db.Intent
			if old == nil || change.ClusterID != request.Databases[i].ClusterID || change.Member.ServerID != old.ServerID || change.Member.Address != old.Address || change.OperationID != request.OperationID || change.BackupRef == "" || (change.Operation != membership.OperationLeave && change.Operation != membership.OperationKick) {
				return errors.New("invalid persisted removal intent")
			}
		}
	}
	if state.Phase == Complete {
		if len(state.Final) != 2 {
			return errors.New("completed lifecycle lacks joint database evidence")
		}
		for i, final := range state.Final {
			if err := final.Validate(); err != nil {
				return err
			}
			if final.Database != request.Databases[i].Database || final.ClusterID != request.Databases[i].ClusterID {
				return errors.New("completion evidence belongs to another database")
			}
			expected := map[string]string{}
			for _, peer := range request.Databases[i].Peers {
				if peer.Name != request.RemoveEndpoint {
					expected[peer.ServerID] = peer.Address
				}
			}
			if state.Prepared != nil {
				db := state.Prepared.Databases[i]
				expected[db.ServerID] = db.Address
			}
			if len(final.Members) != len(expected) {
				return errors.New("completion evidence has the wrong final membership")
			}
			for _, member := range final.Members {
				if !member.Healthy || expected[member.ServerID] != member.Address {
					return errors.New("completion evidence does not prove the healthy target membership")
				}
			}
		}
	} else if len(state.Final) != 0 {
		return errors.New("incomplete lifecycle contains completion evidence")
	}
	return nil
}

// finish preserves the first completion receipt if its create response or the
// following journal update was lost. A later observation may have newer terms
// and indexes; it must not overwrite or conflict with the original audit.
func (e *Executor) finish(ctx context.Context, saved *journal) error {
	name := saved.Request.Name + "-complete"
	got := new(corev1.ConfigMap)
	err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: name}, got)
	if apierrors.IsNotFound(err) {
		_, err := e.record(ctx, name, *saved, true)
		return err
	}
	if err != nil {
		return err
	}
	if err := e.checkRecord(got, true); err != nil {
		return err
	}
	var receipt journal
	if err := json.Unmarshal([]byte(got.Data["record.json"]), &receipt, json.RejectUnknownMembers(true)); err != nil {
		return err
	}
	if receipt.Request != saved.Request || receipt.Digest != saved.Digest || receipt.State.Phase != Complete {
		return errors.New("completion receipt belongs to another operation")
	}
	want, actual := saved.State, receipt.State
	want.Final, actual.Final = nil, nil
	if !reflect.DeepEqual(want, actual) {
		return errors.New("completion receipt disagrees with persisted lifecycle progress")
	}
	saved.State = receipt.State
	return nil
}

func peerNamed(db Inventory, name string) *Peer {
	for _, peer := range db.Peers {
		if peer.Name == name {
			return &peer
		}
	}
	return nil
}
