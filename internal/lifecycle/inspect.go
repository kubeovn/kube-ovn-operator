package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"reflect"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/workload"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Completion pins the immutable receipt, including its original evidence.
type Completion struct {
	Record workload.Reference `json:"record"`
	Digest string             `json:"digest"`
}

type Operation struct {
	Reference  Reference
	Request    Request
	State      State
	Completion *Completion
}

// Inspect discovers the installation slot or reads an already pinned operation.
// It requires both Leases but no mutation providers or backup storage. A missing
// pinned journal is an error, never permission to resume ordinary chart apply.
func Inspect(ctx context.Context, kube client.Client, installation *api.KubeOVNInstallation, pinned *Reference) (*Operation, error) {
	if kube == nil || installation == nil || installation.UID == "" {
		return nil, errors.New("operation inspection requires installation identity and direct client")
	}
	e := &Executor{kube: kube, installation: installation.DeepCopy()}
	if err := e.guard(ctx); err != nil {
		return nil, err
	}
	var ref Reference
	if pinned != nil {
		ref = *pinned
	} else {
		object := new(corev1.ConfigMap)
		err := kube.Get(ctx, client.ObjectKey{Namespace: installation.Namespace, Name: e.activeName()}, object)
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if err := e.checkRecord(object, false); err != nil {
			return nil, err
		}
		var saved journal
		if err := json.Unmarshal([]byte(object.Data["record.json"]), &saved, json.RejectUnknownMembers(true)); err != nil {
			return nil, err
		}
		ref = Reference{Journal: workload.Reference{Name: object.Name, UID: object.UID}, Request: saved.Request}
	}
	_, request, saved, err := e.load(ctx, ref, true)
	if err != nil {
		return nil, err
	}
	operation := &Operation{Reference: ref, Request: request, State: saved.State}
	if saved.State.Phase == Complete {
		operation.Completion, err = e.completion(ctx, request, saved)
		if err != nil {
			return nil, err
		}
	}
	return operation, e.guard(ctx)
}

func (e *Executor) completion(ctx context.Context, request Request, saved journal) (*Completion, error) {
	object := new(corev1.ConfigMap)
	if err := e.kube.Get(ctx, client.ObjectKey{Namespace: e.installation.Namespace, Name: saved.Request.Name + "-complete"}, object); err != nil {
		return nil, fmt.Errorf("read immutable completion receipt: %w", err)
	}
	if err := e.checkRecord(object, true); err != nil {
		return nil, err
	}
	data := []byte(object.Data["record.json"])
	var receipt journal
	if err := json.Unmarshal(data, &receipt, json.RejectUnknownMembers(true)); err != nil {
		return nil, err
	}
	if receipt.State.Phase != Complete || !reflect.DeepEqual(receipt, saved) {
		return nil, errors.New("immutable completion receipt disagrees with operation journal")
	}
	if err := validateState(request, receipt.State); err != nil {
		return nil, err
	}
	return &Completion{Record: workload.Reference{Name: object.Name, UID: object.UID}, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(data))}, nil
}
