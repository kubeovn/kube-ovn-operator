// Package lease coordinates installation and central operations using
// Kubernetes Leases. All participants must use this protocol. Lease loss stops
// new writes; fencing an already submitted remote operation still requires
// support from that operation's server-side transport.
package lease

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Scope string

const (
	Installation Scope = "installation"
	Central      Scope = "central"
)

var (
	ErrHeld      = errors.New("operation lease is held by another actor")
	ErrLost      = errors.New("operation lease was lost")
	ErrOwnership = errors.New("operation lease belongs to another installation")
)

const duration = 30 * time.Second
const renewInterval = 10 * time.Second
const renewDeadline = 20 * time.Second
const requestTimeout = 5 * time.Second

type Runner interface {
	With(context.Context, *operatorv1alpha1.KubeOVNInstallation, Scope, func(context.Context) error) error
}

// Manager requires an uncached Kubernetes client. Observations deliberately
// use local elapsed time instead of trusting a remote holder's RenewTime.
type Manager struct {
	client   client.Client
	clock    clock.WithTicker
	mu       sync.Mutex
	observed map[types.NamespacedName]observation
}

type observation struct {
	uid             types.UID
	resourceVersion string
	since           time.Time
}

func New(kube client.Client) *Manager {
	return &Manager{client: kube, clock: clock.RealClock{}, observed: map[types.NamespacedName]observation{}}
}

func Name(scope Scope, installation string) string {
	return "kube-ovn-operator-" + string(scope) + "-" + installation
}

type scopeKey Scope

type guard struct {
	manager      *Manager
	installation *operatorv1alpha1.KubeOVNInstallation
	key          types.NamespacedName
	uid          types.UID
	holder       string
	transitions  int32
	mu           sync.Mutex
	lastRenewal  time.Time
	cancel       context.CancelCauseFunc
}

// With holds a lease for the duration of fn and releases only its own holder.
// Central operations must nest inside an installation operation in that order.
func (m *Manager) With(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, scope Scope, fn func(context.Context) error) (err error) {
	if scope != Installation && scope != Central {
		return errors.New("unknown operation lease scope")
	}
	if scope == Central {
		if installation == nil {
			return errors.New("central operation requires an installation")
		}
		parent, ok := ctx.Value(scopeKey(Installation)).(*guard)
		if !ok || parent.installation.UID != installation.UID || parent.installation.Name != installation.Name || parent.installation.Namespace != installation.Namespace {
			return errors.New("central operation requires the same installation lease")
		}
		if err := Check(ctx, Installation); err != nil {
			return fmt.Errorf("central operation requires the installation lease: %w", err)
		}
	}
	if ctx.Value(scopeKey(scope)) != nil {
		return errors.New("operation already holds this lease scope")
	}
	g, err := m.acquire(ctx, installation, scope)
	if err != nil {
		return err
	}
	operationCtx, cancel := context.WithCancelCause(ctx)
	g.cancel = cancel
	operationCtx = context.WithValue(operationCtx, scopeKey(scope), g)
	stop := make(chan struct{})
	var workers sync.WaitGroup
	workers.Go(func() {
		ticker := m.clock.NewTicker(renewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-operationCtx.Done():
				return
			case <-ticker.C():
				if err := g.check(operationCtx, true); err != nil {
					cancel(err)
					return
				}
			}
		}
	})
	defer func() {
		close(stop)
		workers.Wait()
		err = errors.Join(err, context.Cause(operationCtx))
		cancel(nil)
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), requestTimeout)
		defer releaseCancel()
		err = errors.Join(err, g.release(releaseCtx))
	}()
	return fn(operationCtx)
}

// Check verifies the local renewal deadline and uncached lease ownership.
// Call immediately before every persistent request, in addition to honoring
// the operation context. This does not revoke requests already in flight.
func Check(ctx context.Context, scope Scope) error {
	if scope == Central {
		if err := Check(ctx, Installation); err != nil {
			return err
		}
	}
	g, ok := ctx.Value(scopeKey(scope)).(*guard)
	if !ok {
		return fmt.Errorf("%s operation lease is required", scope)
	}
	if err := g.check(ctx, false); err != nil {
		g.cancel(err)
		return err
	}
	return nil
}

func CheckAll(ctx context.Context) error {
	if ctx.Value(scopeKey(Central)) != nil {
		return Check(ctx, Central)
	}
	return Check(ctx, Installation)
}

func (m *Manager) acquire(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation, scope Scope) (*guard, error) {
	if m.client == nil || installation == nil || installation.Name == "" || installation.Namespace == "" || installation.UID == "" {
		return nil, errors.New("lease client and installation namespace/name/UID are required")
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	key := types.NamespacedName{Namespace: installation.Namespace, Name: Name(scope, installation.Name)}
	record := new(coordinationv1.Lease)
	err := m.client.Get(ctx, key, record)
	exists := err == nil
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("read operation lease: %w", err)
	}
	now := m.clock.Now()
	if exists {
		if err := checkOwner(record, installation); err != nil {
			return nil, err
		}
		if record.Spec.HolderIdentity != nil && *record.Spec.HolderIdentity != "" {
			if record.Spec.LeaseDurationSeconds == nil || *record.Spec.LeaseDurationSeconds <= 0 {
				return nil, errors.New("held operation lease has no valid duration")
			}
			m.mu.Lock()
			observed, seen := m.observed[key]
			if !seen || observed.uid != record.UID || observed.resourceVersion != record.ResourceVersion {
				observed = observation{uid: record.UID, resourceVersion: record.ResourceVersion, since: now}
				m.observed[key] = observed
			}
			expired := now.Sub(observed.since) >= time.Duration(*record.Spec.LeaseDurationSeconds)*time.Second
			m.mu.Unlock()
			if !expired {
				return nil, ErrHeld
			}
		}
	} else {
		record.Name, record.Namespace = key.Name, key.Namespace
		record.OwnerReferences = []metav1.OwnerReference{{APIVersion: operatorv1alpha1.GroupVersion.String(), Kind: "KubeOVNInstallation", Name: installation.Name, UID: installation.UID, Controller: new(true)}}
	}
	transitions := int32(0)
	if record.Spec.LeaseTransitions != nil {
		transitions = *record.Spec.LeaseTransitions
	}
	if transitions == int32(1<<31-1) {
		return nil, errors.New("operation lease transition counter exhausted")
	}
	transitions++
	holder := rand.Text()
	record.Spec = coordinationv1.LeaseSpec{
		HolderIdentity: &holder, LeaseDurationSeconds: new(int32(duration / time.Second)),
		AcquireTime: new(metav1.NewMicroTime(now)), RenewTime: new(metav1.NewMicroTime(now)), LeaseTransitions: &transitions,
	}
	if exists {
		err = m.client.Update(ctx, record)
	} else {
		err = m.client.Create(ctx, record)
	}
	if apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err) {
		return nil, ErrHeld
	}
	if err != nil {
		return nil, fmt.Errorf("acquire operation lease: %w", err)
	}
	m.mu.Lock()
	delete(m.observed, key)
	m.mu.Unlock()
	return &guard{manager: m, installation: installation.DeepCopy(), key: key, uid: record.UID, holder: holder, transitions: transitions, lastRenewal: now}, nil
}

func (g *guard) check(ctx context.Context, renew bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return errors.Join(ErrLost, err)
	}
	if g.manager.clock.Since(g.lastRenewal) >= renewDeadline {
		return fmt.Errorf("%w: renewal deadline exceeded", ErrLost)
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	record := new(coordinationv1.Lease)
	if err := g.manager.client.Get(ctx, g.key, record); err != nil {
		return errors.Join(ErrLost, err)
	}
	if !g.owns(record) || checkOwner(record, g.installation) != nil {
		return ErrLost
	}
	if g.manager.clock.Since(g.lastRenewal) >= renewDeadline {
		return fmt.Errorf("%w: renewal deadline exceeded", ErrLost)
	}
	if !renew {
		return nil
	}
	renewed := g.manager.clock.Now()
	record.Spec.RenewTime = new(metav1.NewMicroTime(renewed))
	if err := g.manager.client.Update(ctx, record); err != nil {
		return errors.Join(ErrLost, err)
	}
	g.lastRenewal = renewed
	return nil
}

func (g *guard) owns(record *coordinationv1.Lease) bool {
	return record.UID == g.uid && record.DeletionTimestamp == nil && record.Spec.HolderIdentity != nil && *record.Spec.HolderIdentity == g.holder &&
		record.Spec.LeaseTransitions != nil && *record.Spec.LeaseTransitions == g.transitions && record.Spec.LeaseDurationSeconds != nil && *record.Spec.LeaseDurationSeconds == int32(duration/time.Second)
}

func (g *guard) release(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	record := new(coordinationv1.Lease)
	if err := g.manager.client.Get(ctx, g.key, record); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("read lease for release: %w", err)
	}
	if !g.owns(record) || checkOwner(record, g.installation) != nil {
		return nil
	}
	record.Spec.HolderIdentity = new("")
	if err := g.manager.client.Update(ctx, record); err != nil {
		return fmt.Errorf("release operation lease: %w", err)
	}
	return nil
}

func checkOwner(record *coordinationv1.Lease, installation *operatorv1alpha1.KubeOVNInstallation) error {
	if record.DeletionTimestamp != nil || len(record.OwnerReferences) != 1 {
		return ErrOwnership
	}
	owner := record.OwnerReferences[0]
	if owner.APIVersion != operatorv1alpha1.GroupVersion.String() || owner.Kind != "KubeOVNInstallation" || owner.Name != installation.Name || owner.UID != installation.UID || owner.Controller == nil || !*owner.Controller {
		return ErrOwnership
	}
	return nil
}
