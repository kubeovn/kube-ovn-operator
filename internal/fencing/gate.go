// Package fencing contains the server-side compare-and-admit seam for
// destructive OVN commands. The authoritative Lease watcher must call Arm;
// requests cannot arm or replace the fence themselves.
package fencing

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"k8s.io/apimachinery/pkg/types"
)

var (
	ErrNotArmed  = errors.New("remote fence is not armed")
	ErrStale     = errors.New("remote fence token is stale")
	ErrInvalid   = errors.New("remote fence token is invalid")
	ErrNoCommand = errors.New("remote fence command executor is not configured")
)

// Gate holds the one token admitted by a trusted fence controller. Execute
// keeps the comparison and command admission in the same critical section;
// replacing the armed token cannot race with a command that has already been
// admitted.
type Gate struct {
	mu    sync.Mutex
	armed bool
	token lease.FenceToken
}

// Arm replaces the current fence. Only an authoritative Lease watcher or
// equivalent trusted controller may call this method. A caller holding an old
// token cannot reach Arm through the HTTP handler.
func (g *Gate) Arm(token lease.FenceToken) error {
	if err := ValidateToken(token); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.token, g.armed = token, true
	return nil
}

// Disarm clears the fence only when the caller supplies the currently armed
// token. A delayed cleanup from a previous holder cannot disarm a replacement.
func (g *Gate) Disarm(token lease.FenceToken) error {
	if err := ValidateToken(token); err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed {
		return ErrNotArmed
	}
	if g.token != token {
		return ErrStale
	}
	g.armed = false
	g.token = lease.FenceToken{}
	return nil
}

// Clear removes the current fence without accepting a token. It is intended
// only for an authoritative Lease watcher after it observes that the Lease is
// absent, malformed or no longer held; the HTTP handler never calls it.
func (g *Gate) Clear() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.armed = false
	g.token = lease.FenceToken{}
}

// Current returns a copy of the armed token for diagnostics. It does not
// authorize a command; Execute must still receive the token again.
func (g *Gate) Current() (lease.FenceToken, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.token, g.armed
}

// Execute validates token and invokes fn while the fence is held. The callback
// must perform the native command admission synchronously; a delayed,
// independently launched goroutine would fall outside this guarantee.
func (g *Gate) Execute(ctx context.Context, token lease.FenceToken, fn func(context.Context) ([]byte, error)) ([]byte, error) {
	if err := ValidateToken(token); err != nil {
		return nil, err
	}
	if fn == nil {
		return nil, ErrNoCommand
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed {
		return nil, ErrNotArmed
	}
	if g.token != token {
		return nil, ErrStale
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return fn(ctx)
}

// ExecuteRestore validates and admits a streaming restore while holding the
// same server-side fence lock used for membership mutations. The callback must
// synchronously consume the supplied context; it must not defer native restore
// work to another goroutine after returning.
func (g *Gate) ExecuteRestore(ctx context.Context, token lease.FenceToken, fn func(context.Context) error) error {
	if err := ValidateToken(token); err != nil {
		return err
	}
	if fn == nil {
		return ErrNoCommand
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.armed {
		return ErrNotArmed
	}
	if g.token != token {
		return ErrStale
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(ctx)
}

// ValidateToken rejects incomplete or non-central tokens before they reach a
// server-side executor. Scope-specific modules can impose additional checks.
func ValidateToken(token lease.FenceToken) error {
	if token.InstallationUID == "" || token.LeaseUID == "" || token.Scope == "" || token.Holder == "" || token.Epoch < 1 {
		return ErrInvalid
	}
	return nil
}

// TokenFromLease converts one already-read Kubernetes Lease into a server
// fence token. The caller must clear the Gate when the Lease cannot be read;
// an old token is never retained across an observation failure.
func TokenFromLease(installationUID, leaseUID types.UID, holder string, epoch int32) (lease.FenceToken, error) {
	token := lease.FenceToken{InstallationUID: installationUID, LeaseUID: leaseUID, Scope: lease.Central, Holder: holder, Epoch: epoch}
	if err := ValidateToken(token); err != nil {
		return lease.FenceToken{}, err
	}
	return token, nil
}

func wrapCode(err error) (code, message string) {
	switch {
	case errors.Is(err, ErrNotArmed):
		return "fence_not_armed", err.Error()
	case errors.Is(err, ErrStale):
		return "stale_fence", err.Error()
	case errors.Is(err, ErrInvalid):
		return "invalid_fence", err.Error()
	default:
		return "execution_failed", fmt.Sprint(err)
	}
}
