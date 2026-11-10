package recovery

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/health"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KubernetesFenceEndpointResolver resolves a retained member's live Pod and
// Node identity before each fence action. It never derives an endpoint from a
// recovery-plan name because Pod names and node addresses can be reused.
type KubernetesFenceEndpointResolver struct {
	Reader    client.Reader
	Namespace string
	Container string
}

func (r *KubernetesFenceEndpointResolver) Resolve(ctx context.Context, member Member) (FenceEndpoint, error) {
	if r == nil || r.Reader == nil {
		return FenceEndpoint{}, errors.New("kubernetes fence endpoint resolver requires a live client")
	}
	pod, err := podByUID(ctx, r.Reader, r.Namespace, member.Storage.PodUID)
	if err != nil {
		return FenceEndpoint{}, fmt.Errorf("read retained member Pod: %w", err)
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != member.NodeName {
		return FenceEndpoint{}, errors.New("retained member Pod identity or node changed")
	}
	if r.Namespace != "" && pod.Namespace != r.Namespace {
		return FenceEndpoint{}, errors.New("retained member Pod is outside the configured namespace")
	}
	node := new(corev1.Node)
	if err := r.Reader.Get(ctx, client.ObjectKey{Name: pod.Spec.NodeName}, node); err != nil {
		return FenceEndpoint{}, fmt.Errorf("read retained member node: %w", err)
	}
	if string(node.UID) != member.Storage.NodeUID || node.DeletionTimestamp != nil {
		return FenceEndpoint{}, errors.New("retained member node identity changed")
	}
	address := firstAddress(pod.Status.PodIP, pod.Status.HostIP)
	if address == "" {
		for _, candidate := range node.Status.Addresses {
			if candidate.Type == corev1.NodeInternalIP {
				address = candidate.Address
				break
			}
		}
	}
	if _, err := netip.ParseAddr(address); err != nil {
		return FenceEndpoint{}, errors.New("retained member Pod and Node have no valid fence address")
	}
	container := r.Container
	if container == "" {
		container = "ovn-central"
	}
	return FenceEndpoint{Namespace: pod.Namespace, Endpoint: pod.Name, Node: address, Container: container}, nil
}

// NewKubernetesFencedIsolationSession composes the live provider pieces. The
// caller supplies the central Lease token source so this constructor cannot
// accidentally create or bypass Lease ownership.
func NewKubernetesFencedIsolationSession(reader client.Reader, remote *health.FenceHTTPClient, namespace string, token func(context.Context) (lease.FenceToken, error)) (*FencedIsolationSession, error) {
	if reader == nil || remote == nil || token == nil || strings.TrimSpace(namespace) == "" {
		return nil, errors.New("kubernetes fenced isolation requires client, mTLS fence, namespace and Lease token source")
	}
	resolver := &KubernetesFenceEndpointResolver{Reader: reader, Namespace: namespace}
	return &FencedIsolationSession{
		Token:   token,
		Fence:   &RemoteFenceController{Remote: remote, Resolve: resolver.Resolve},
		Storage: &KubernetesStorageOwnershipVerifier{Reader: reader, Namespace: namespace},
	}, nil
}

func firstAddress(candidates ...string) string {
	for _, candidate := range candidates {
		if _, err := netip.ParseAddr(strings.TrimSpace(candidate)); err == nil {
			return strings.TrimSpace(candidate)
		}
	}
	return ""
}
