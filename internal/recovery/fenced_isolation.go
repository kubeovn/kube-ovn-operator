package recovery

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/kubeovn/kube-ovn-operator/internal/fencing"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

var (
	ErrIsolationSessionOnly  = errors.New("offline rebuild isolation requires a continuous session")
	ErrIsolationTokenChanged = errors.New("offline rebuild isolation Lease token changed")
)

// FenceController is the provider boundary for real retained-process fencing.
// Implementations must perform each action through an atomic server-side
// fence, such as the mTLS fence-agent. A readiness probe or a local process
// scan is not sufficient.
type FenceController interface {
	Isolate(context.Context, Member, lease.FenceToken) error
	Check(context.Context, Member, lease.FenceToken) error
	Release(context.Context, Member, lease.FenceToken) error
}

// StorageOwnershipVerifier must check live Kubernetes/PVC ownership and the
// target workload identity. A marker file or a plan field alone is not enough.
type StorageOwnershipVerifier interface {
	Verify(context.Context, Member, string) error
}

// FencedIsolationSession binds a continuous fence to the central operation
// Lease and to live target storage ownership. It intentionally has no default
// controller or storage verifier: callers must provide both capabilities.
type FencedIsolationSession struct {
	Token   func(context.Context) (lease.FenceToken, error)
	Fence   FenceController
	Storage StorageOwnershipVerifier
}

func (s *FencedIsolationSession) Begin(ctx context.Context, plan Plan, target Member, root string) (IsolationGuard, error) {
	if s == nil || s.Token == nil || s.Fence == nil || s.Storage == nil {
		return nil, errors.New("offline rebuild requires Lease token, fence controller and live storage verifier")
	}
	if err := s.Storage.Verify(ctx, target, root); err != nil {
		return nil, fmt.Errorf("verify target storage ownership: %w", err)
	}
	token, err := s.Token(ctx)
	if err != nil {
		return nil, fmt.Errorf("read offline rebuild Lease token: %w", err)
	}
	if err := fencing.ValidateToken(token); err != nil {
		return nil, fmt.Errorf("validate offline rebuild Lease token: %w", err)
	}
	retained := append([]Member(nil), plan.RetainedMembers...)
	isolated := make([]Member, 0, len(retained))
	for _, member := range retained {
		current, err := s.Token(ctx)
		if err != nil {
			return nil, s.releasePartial(ctx, isolated, token, fmt.Errorf("refresh offline rebuild Lease token: %w", err))
		}
		if current != token {
			return nil, s.releasePartial(ctx, isolated, token, ErrIsolationTokenChanged)
		}
		if err := s.Fence.Isolate(ctx, member, token); err != nil {
			return nil, s.releasePartial(ctx, isolated, token, fmt.Errorf("isolate retained member %q: %w", member.Name, err))
		}
		isolated = append(isolated, member)
	}
	guard := &fencedIsolationGuard{session: s, token: token, target: target, root: root, retained: retained}
	if err := guard.Check(ctx); err != nil {
		_ = guard.Close(context.Background())
		return nil, err
	}
	return guard, nil
}

func (s *FencedIsolationSession) releasePartial(ctx context.Context, members []Member, token lease.FenceToken, cause error) error {
	for index := len(members) - 1; index >= 0; index-- {
		cause = errors.Join(cause, s.Fence.Release(ctx, members[index], token))
	}
	return cause
}

// These methods prevent accidental use through the legacy point-in-time
// interface. Executor uses Begin because it requires IsolationSession.
func (*FencedIsolationSession) OldProcessesIsolated(context.Context, Plan) error {
	return ErrIsolationSessionOnly
}

func (*FencedIsolationSession) TargetStorageOwned(context.Context, Member, string) error {
	return ErrIsolationSessionOnly
}

type fencedIsolationGuard struct {
	session  *FencedIsolationSession
	token    lease.FenceToken
	target   Member
	root     string
	retained []Member
	mu       sync.Mutex
	closed   bool
}

func (g *fencedIsolationGuard) Check(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errors.New("offline rebuild isolation session is closed")
	}
	current, err := g.session.Token(ctx)
	if err != nil {
		return fmt.Errorf("refresh offline rebuild Lease token: %w", err)
	}
	if current != g.token {
		return ErrIsolationTokenChanged
	}
	if err := g.session.Storage.Verify(ctx, g.target, g.root); err != nil {
		return fmt.Errorf("verify target storage ownership: %w", err)
	}
	for _, member := range g.retained {
		if err := g.session.Fence.Check(ctx, member, g.token); err != nil {
			return fmt.Errorf("check retained member %q isolation: %w", member.Name, err)
		}
	}
	return nil
}

func (g *fencedIsolationGuard) Close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return nil
	}
	current, err := g.session.Token(ctx)
	if err != nil {
		return fmt.Errorf("refresh offline rebuild Lease token before release: %w", err)
	}
	if current != g.token {
		return ErrIsolationTokenChanged
	}
	var releaseErr error
	for index := len(g.retained) - 1; index >= 0; index-- {
		releaseErr = errors.Join(releaseErr, g.session.Fence.Release(ctx, g.retained[index], g.token))
	}
	if releaseErr != nil {
		return fmt.Errorf("release retained process fencing: %w", releaseErr)
	}
	g.closed = true
	return nil
}

var _ Isolation = (*FencedIsolationSession)(nil)
var _ IsolationSession = (*FencedIsolationSession)(nil)
