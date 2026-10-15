package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("Lease exclusion, renewal, release and loss checks passed")
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		return err
	}
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		return err
	}
	config := ctrl.GetConfigOrDie()
	config.Timeout = 5 * time.Second
	kube, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return err
	}
	installation := new(operatorv1alpha1.KubeOVNInstallation)
	if err := kube.Get(ctx, types.NamespacedName{Namespace: "kube-ovn-operator-system", Name: "kube-ovn"}, installation); err != nil {
		return err
	}
	first, second := lease.New(kube), lease.New(kube)
	key := types.NamespacedName{Namespace: installation.Namespace, Name: lease.Name(lease.Installation, installation.Name)}
	err = first.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		return first.With(operationCtx, installation, lease.Central, func(operationCtx context.Context) error {
			record := new(coordinationv1.Lease)
			if err := kube.Get(operationCtx, key, record); err != nil {
				return err
			}
			if record.Spec.HolderIdentity == nil || *record.Spec.HolderIdentity == string(installation.UID) {
				return errors.New("lease holder is not an operation identity")
			}
			acquired := record.Spec.RenewTime.DeepCopy()
			if err := second.With(ctx, installation, lease.Installation, func(context.Context) error { return errors.New("second actor entered critical section") }); !errors.Is(err, lease.ErrHeld) {
				return fmt.Errorf("contention did not block second actor: %w", err)
			}
			// Hold longer than the initial 30-second duration. Both scopes must renew.
			select {
			case <-operationCtx.Done():
				return context.Cause(operationCtx)
			case <-time.After(35 * time.Second):
			}
			if err := lease.CheckAll(operationCtx); err != nil {
				return err
			}
			if err := kube.Get(operationCtx, key, record); err != nil {
				return err
			}
			if record.Spec.RenewTime == nil || !record.Spec.RenewTime.After(acquired.Time) {
				return errors.New("lease was not renewed")
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	for _, scope := range []lease.Scope{lease.Installation, lease.Central} {
		record := new(coordinationv1.Lease)
		if err := kube.Get(ctx, types.NamespacedName{Namespace: installation.Namespace, Name: lease.Name(scope, installation.Name)}, record); err != nil {
			return err
		}
		if record.Spec.HolderIdentity != nil && *record.Spec.HolderIdentity != "" {
			return errors.New("completed operation did not release lease")
		}
	}
	err = second.With(ctx, installation, lease.Installation, func(operationCtx context.Context) error {
		record := new(coordinationv1.Lease)
		if err := kube.Get(operationCtx, key, record); err != nil {
			return err
		}
		record.Spec.HolderIdentity = new("simulated-replacement")
		if err := kube.Update(operationCtx, record); err != nil {
			return err
		}
		if err := lease.CheckAll(operationCtx); !errors.Is(err, lease.ErrLost) {
			return fmt.Errorf("ownership loss did not stop the operation: %w", err)
		}
		if !errors.Is(context.Cause(operationCtx), lease.ErrLost) {
			return errors.New("operation context survived lease loss")
		}
		return nil
	})
	if !errors.Is(err, lease.ErrLost) {
		return fmt.Errorf("lost lease was not reported: %w", err)
	}
	record := new(coordinationv1.Lease)
	if err := kube.Get(ctx, key, record); err != nil {
		return err
	}
	if record.Spec.HolderIdentity == nil || *record.Spec.HolderIdentity != "simulated-replacement" {
		return errors.New("old actor released the replacement holder")
	}
	// This holder was injected by this test, not acquired by another actor.
	record.Spec.HolderIdentity = new("")
	return kube.Update(ctx, record)
}
