package recovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
)

// FenceEndpoint identifies one sidecar without deriving a routable address
// from the recovery plan. The resolver must read the current Pod/Node state so
// an endpoint change cannot silently target a reused name or stale IP.
type FenceEndpoint struct {
	Namespace string
	Endpoint  string
	Node      string
	Container string
}

// RemoteFenceController adapts the mTLS fence-agent protocol to the recovery
// isolation session. It has no endpoint fallback: callers must provide a live
// resolver for each retained member.
type RemoteFenceController struct {
	Remote  *health.FenceHTTPClient
	Resolve func(context.Context, Member) (FenceEndpoint, error)
}

func (c *RemoteFenceController) Isolate(ctx context.Context, member Member, token lease.FenceToken) error {
	endpoint, err := c.endpoint(ctx, member)
	if err != nil {
		return err
	}
	return c.Remote.IsolateAt(ctx, endpoint.Namespace, endpoint.Endpoint, endpoint.Node, endpoint.Container, token)
}

func (c *RemoteFenceController) Check(ctx context.Context, member Member, token lease.FenceToken) error {
	endpoint, err := c.endpoint(ctx, member)
	if err != nil {
		return err
	}
	return c.Remote.CheckIsolationAt(ctx, endpoint.Namespace, endpoint.Endpoint, endpoint.Node, endpoint.Container, token)
}

func (c *RemoteFenceController) Release(ctx context.Context, member Member, token lease.FenceToken) error {
	endpoint, err := c.endpoint(ctx, member)
	if err != nil {
		return err
	}
	return c.Remote.ReleaseAt(ctx, endpoint.Namespace, endpoint.Endpoint, endpoint.Node, endpoint.Container, token)
}

func (c *RemoteFenceController) endpoint(ctx context.Context, member Member) (FenceEndpoint, error) {
	if c == nil || c.Remote == nil || c.Resolve == nil {
		return FenceEndpoint{}, errors.New("remote fence controller requires mTLS client and live endpoint resolver")
	}
	endpoint, err := c.Resolve(ctx, member)
	if err != nil {
		return FenceEndpoint{}, fmt.Errorf("resolve retained member %q fence endpoint: %w", member.Name, err)
	}
	if endpoint.Endpoint == "" {
		return FenceEndpoint{}, errors.New("retained member fence endpoint is required")
	}
	return endpoint, nil
}

var _ FenceController = (*RemoteFenceController)(nil)
