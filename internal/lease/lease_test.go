package lease

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func fixture(t *testing.T) (*Manager, *operatorv1alpha1.KubeOVNInstallation) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := coordinationv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	manager := New(fake.NewClientBuilder().WithScheme(scheme).Build())
	installation := &operatorv1alpha1.KubeOVNInstallation{Name: "kube-ovn", Namespace: "network", UID: "installation-uid"}
	controller := true
	for _, scope := range []Scope{Installation, Central} {
		if err := manager.client.Create(t.Context(), &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{
			Name: Name(scope, installation.Name), Namespace: installation.Namespace, UID: types.UID(string(scope) + "-lease-uid"),
			OwnerReferences: []metav1.OwnerReference{{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: &controller}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	return manager, installation
}

func TestOnlyOneActorCanHoldLeaseAndReleaseAllowsNextActor(t *testing.T) {
	first, installation := fixture(t)
	second := New(first.client)
	var firstHolder string
	err := first.With(t.Context(), installation, Installation, func(ctx context.Context) error {
		firstHolder = ctx.Value(scopeKey(Installation)).(*guard).holder
		if err := CheckAll(ctx); err != nil {
			return err
		}
		err := second.With(ctx, installation, Installation, func(context.Context) error { t.Error("second actor entered critical section"); return nil })
		// Use an independent context: the second actor must contend at Kubernetes,
		// rather than failing a same-context recursive acquisition check.
		if err == nil {
			t.Fatal("recursive lease acquisition succeeded")
		}
		err = second.With(t.Context(), installation, Installation, func(context.Context) error { t.Error("second actor entered critical section"); return nil })
		if !errors.Is(err, ErrHeld) {
			t.Fatalf("expected contention, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.With(t.Context(), installation, Installation, func(ctx context.Context) error {
		holder := ctx.Value(scopeKey(Installation)).(*guard).holder
		if holder == firstHolder {
			t.Fatal("operation holder identity was reused")
		}
		return CheckAll(ctx)
	}); err != nil {
		t.Fatal(err)
	}
}

func TestTakeoverUsesLocallyObservedExpiryAndOldActorCannotReleaseNewHolder(t *testing.T) {
	first, installation := fixture(t)
	firstClock := clocktesting.NewFakeClock(time.Now())
	first.clock = firstClock
	old, err := first.acquire(t.Context(), installation, Installation)
	if err != nil {
		t.Fatal(err)
	}
	record := new(coordinationv1.Lease)
	if err := first.client.Get(t.Context(), old.key, record); err != nil {
		t.Fatal(err)
	}
	// Even an ancient remote timestamp is not proof that a new observer has
	// watched the previous holder stop renewing for a full lease duration.
	record.Spec.RenewTime = new(metav1.NewMicroTime(time.Unix(1, 0)))
	if err := first.client.Update(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	second := New(first.client)
	secondClock := clocktesting.NewFakeClock(time.Now().Add(24 * time.Hour))
	second.clock = secondClock
	if _, err := second.acquire(t.Context(), installation, Installation); !errors.Is(err, ErrHeld) {
		t.Fatalf("remote clock bypassed expiry: %v", err)
	}
	secondClock.Step(duration - time.Nanosecond)
	if _, err := second.acquire(t.Context(), installation, Installation); !errors.Is(err, ErrHeld) {
		t.Fatalf("lease expired early: %v", err)
	}
	secondClock.Step(time.Nanosecond)
	current, err := second.acquire(t.Context(), installation, Installation)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.check(t.Context(), false); !errors.Is(err, ErrLost) {
		t.Fatalf("old actor retained ownership: %v", err)
	}
	if err := old.release(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := current.check(t.Context(), false); err != nil {
		t.Fatalf("old release disturbed new holder: %v", err)
	}
}

func TestRenewalExtendsObservationAndLocalDeadlineStopsWrites(t *testing.T) {
	manager, installation := fixture(t)
	localClock := clocktesting.NewFakeClock(time.Now())
	manager.clock = localClock
	guard, err := manager.acquire(t.Context(), installation, Installation)
	if err != nil {
		t.Fatal(err)
	}
	localClock.Step(renewInterval)
	if err := guard.check(t.Context(), true); err != nil {
		t.Fatal(err)
	}
	localClock.Step(renewInterval)
	if err := guard.check(t.Context(), false); err != nil {
		t.Fatalf("renewal did not extend deadline: %v", err)
	}
	localClock.Step(renewInterval)
	if err := guard.check(t.Context(), false); !errors.Is(err, ErrLost) {
		t.Fatalf("expired local lease still allowed a write: %v", err)
	}
}

func TestFenceTokenBindsLeaseUIDHolderAndTransition(t *testing.T) {
	manager, installation := fixture(t)
	var token FenceToken
	if err := manager.With(t.Context(), installation, Installation, func(ctx context.Context) error {
		var err error
		token, err = Token(ctx, Installation)
		if err != nil {
			return err
		}
		other := New(manager.client)
		if err := other.With(t.Context(), installation, Installation, func(context.Context) error { return nil }); !errors.Is(err, ErrHeld) {
			return fmt.Errorf("unexpected competing lease result: %w", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if token.InstallationUID != installation.UID || token.LeaseUID == "" || token.Scope != Installation || token.Holder == "" || token.Epoch != 1 {
		t.Fatalf("unexpected fence token: %+v", token)
	}
	var next FenceToken
	if err := manager.With(t.Context(), installation, Installation, func(ctx context.Context) error {
		var err error
		next, err = Token(ctx, Installation)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if next.LeaseUID != token.LeaseUID || next.Epoch == token.Epoch || next.Holder == token.Holder {
		t.Fatalf("lease takeover did not produce a new fence epoch: old=%+v new=%+v", token, next)
	}
}

func TestOwnershipChangeCancelsOperationAndDoesNotClearNewHolder(t *testing.T) {
	manager, installation := fixture(t)
	key := types.NamespacedName{Namespace: installation.Namespace, Name: Name(Installation, installation.Name)}
	err := manager.With(t.Context(), installation, Installation, func(ctx context.Context) error {
		record := new(coordinationv1.Lease)
		if err := manager.client.Get(ctx, key, record); err != nil {
			return err
		}
		record.Spec.HolderIdentity = new("replacement-holder")
		if err := manager.client.Update(ctx, record); err != nil {
			return err
		}
		if err := CheckAll(ctx); !errors.Is(err, ErrLost) {
			t.Fatalf("lost holder not rejected: %v", err)
		}
		if !errors.Is(context.Cause(ctx), ErrLost) {
			t.Fatal("operation context was not cancelled on lease loss")
		}
		return nil
	})
	if !errors.Is(err, ErrLost) {
		t.Fatalf("lease loss not returned: %v", err)
	}
	record := new(coordinationv1.Lease)
	if err := manager.client.Get(t.Context(), key, record); err != nil {
		t.Fatal(err)
	}
	if *record.Spec.HolderIdentity != "replacement-holder" {
		t.Fatal("cleanup cleared another holder")
	}
}

func TestCentralLeaseRequiresSameInstallationAndForeignOwnerIsPreserved(t *testing.T) {
	manager, installation := fixture(t)
	if err := manager.With(t.Context(), installation, Central, func(context.Context) error { return nil }); err == nil {
		t.Fatal("central lease acquired without installation lease")
	}
	other := installation.DeepCopy()
	other.UID = "other-installation"
	if err := manager.With(t.Context(), installation, Installation, func(ctx context.Context) error {
		if err := manager.With(ctx, other, Central, func(context.Context) error { return nil }); err == nil {
			t.Fatal("central lease used another installation's parent lock")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.acquire(t.Context(), other, Installation); !errors.Is(err, ErrOwnership) {
		t.Fatalf("foreign installation acquired existing lease: %v", err)
	}
}

func TestCentralCheckRejectsLossOfTheParentInstallationLease(t *testing.T) {
	manager, installation := fixture(t)
	err := manager.With(t.Context(), installation, Installation, func(ctx context.Context) error {
		return manager.With(ctx, installation, Central, func(ctx context.Context) error {
			key := types.NamespacedName{Namespace: installation.Namespace, Name: Name(Installation, installation.Name)}
			record := new(coordinationv1.Lease)
			if err := manager.client.Get(ctx, key, record); err != nil {
				return err
			}
			record.Spec.HolderIdentity = new("replacement-parent")
			if err := manager.client.Update(ctx, record); err != nil {
				return err
			}
			return Check(ctx, Central)
		})
	})
	if !errors.Is(err, ErrLost) {
		t.Fatalf("central mutation survived loss of its parent lease: %v", err)
	}
}

func TestConcurrentAcquisitionsHaveExactlyOneWinner(t *testing.T) {
	first, installation := fixture(t)
	second := New(first.client)
	start := make(chan struct{})
	results := make(chan error, 2)
	var workers sync.WaitGroup
	for _, manager := range []*Manager{first, second} {
		workers.Go(func() {
			<-start
			_, err := manager.acquire(t.Context(), installation, Installation)
			results <- err
		})
	}
	close(start)
	workers.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrHeld) {
			t.Fatalf("unexpected acquisition failure: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("concurrent acquisition produced %d winners", winners)
	}
}
