package health

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kubeovn/kube-ovn-operator/internal/fencing"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/ovnadmin"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

const (
	maxFenceRequest = 8 << 10
	maxFenceReply   = 128 << 10
)

// FenceHTTPClient is the client side of the remote mutation transport. The
// endpoint must be a trusted HTTPS service (loopback HTTP is accepted only for
// local tests). The service must atomically compare the token with its armed
// fence before admitting the command.
type FenceHTTPClient struct {
	client           *http.Client
	endpointTemplate string
}

// NewFenceHTTPClient validates a URL template used for remote fenced commands.
// Supported placeholders are {namespace}, {pod}, and {container}; they are
// escaped as URL path segments before each request.
func NewFenceHTTPClient(endpointTemplate string, client *http.Client) (*FenceHTTPClient, error) {
	endpointTemplate = strings.TrimSpace(endpointTemplate)
	if endpointTemplate == "" {
		return nil, errors.New("fence endpoint template is required")
	}
	if strings.Contains(endpointTemplate, "?") || strings.Contains(endpointTemplate, "#") {
		return nil, errors.New("fence endpoint template must not contain a query or fragment")
	}
	validationTemplate := strings.ReplaceAll(endpointTemplate, "{node}", "fence-node.invalid")
	parsed, err := url.Parse(validationTemplate)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil {
		return nil, errors.New("fence endpoint template must be an absolute URL without credentials")
	}
	if strings.Contains(parsed.Host, "{") || strings.Contains(parsed.Host, "}") {
		return nil, errors.New("fence endpoint contains an unsupported host placeholder")
	}
	if strings.Contains(parsed.Path, "fence-node.invalid") {
		return nil, errors.New("{node} placeholder must be in the URL host")
	}
	if parsed.Scheme != "https" && !loopbackHost(parsed.Hostname()) {
		return nil, errors.New("fence endpoint must use HTTPS except for loopback tests")
	}
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	} else {
		copy := *client
		client = &copy
	}
	if client.CheckRedirect == nil {
		client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
			return errors.New("fence endpoint redirects are not allowed")
		}
	}
	return &FenceHTTPClient{client: client, endpointTemplate: endpointTemplate}, nil
}

// NewFenceHTTPClientWithTLS creates the operator-side mTLS transport for a
// production fence service. Certificate paths are deliberately accepted as
// paths only; private key material never enters the URL template or the wire
// request. The server certificate is trusted only through caFile.
func NewFenceHTTPClientWithTLS(endpointTemplate, caFile, clientCertFile, clientKeyFile string) (*FenceHTTPClient, error) {
	caFile = strings.TrimSpace(caFile)
	clientCertFile = strings.TrimSpace(clientCertFile)
	clientKeyFile = strings.TrimSpace(clientKeyFile)
	if caFile == "" || clientCertFile == "" || clientKeyFile == "" {
		return nil, errors.New("fence mTLS requires CA, client certificate and client key files")
	}
	caData, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("read fence CA file: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caData) {
		return nil, errors.New("fence CA file contains no PEM certificates")
	}
	certificate, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load fence client certificate: %w", err)
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			RootCAs:      pool,
			Certificates: []tls.Certificate{certificate},
			// The endpoint hostname is verified by the standard TLS client. Do
			// not disable verification or force a caller-provided server name.
		},
	}
	return NewFenceHTTPClient(endpointTemplate, &http.Client{Transport: transport, Timeout: 30 * time.Second})
}

// ProbeReady checks that the target fence service has armed its current Lease
// before a destructive operation pauses workloads. It deliberately does not
// carry a mutation token: the service's readiness endpoint only reports the
// state installed by its trusted Lease watcher.
func (c *FenceHTTPClient) ProbeReady(ctx context.Context, namespace, endpoint, container string) error {
	return c.ProbeReadyAt(ctx, namespace, endpoint, "", container)
}

// ProbeReadyAt is ProbeReady with an optional Pod or Node IP for templates
// that route directly to a host-network sidecar through the {node} placeholder.
func (c *FenceHTTPClient) ProbeReadyAt(ctx context.Context, namespace, endpoint, node, container string) error {
	if c == nil || c.client == nil {
		return errors.New("fence HTTP client is not configured")
	}
	if strings.TrimSpace(endpoint) == "" {
		return errors.New("fence endpoint is required")
	}
	if namespace == "" {
		namespace = "kube-ovn-operator-system"
	}
	if container == "" {
		container = "ovn-central"
	}
	requestURL, err := c.endpointURLForAt(namespace, endpoint, node, container, fencing.ReadyPath)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("create fence readiness request: %w", err)
	}
	response, err := c.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("probe fence readiness: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	reply, err := io.ReadAll(io.LimitReader(response.Body, maxFenceReply+1))
	if err != nil {
		return fmt.Errorf("read fence readiness response: %w", err)
	}
	if len(reply) > maxFenceReply {
		return errors.New("fence readiness response exceeds 128 KiB")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		var envelope fencing.Response
		if err := json.Unmarshal(reply, &envelope); err != nil {
			envelope.Message = strings.TrimSpace(string(reply))
		}
		return remoteFenceError(response.StatusCode, envelope)
	}
	if strings.TrimSpace(string(reply)) != "ok" {
		return errors.New("fence readiness endpoint returned an invalid response")
	}
	return nil
}

// RunFenced performs a mutating request with the exact lease token. The
// remote service, rather than this client, is responsible for atomic fence
// comparison and command admission.
func (c *FenceHTTPClient) RunFenced(ctx context.Context, endpoint string, command []string, token lease.FenceToken) ([]byte, error) {
	if err := validateFenceToken(token); err != nil {
		return nil, err
	}
	if err := fencing.ValidateMembershipCommand(command); err != nil {
		return nil, err
	}
	return c.do(ctx, endpoint, "", "", command, &token)
}

// RestoreFenced streams one verified native backup through the remote fence
// service. The service compares the complete token and keeps its gate held
// until the native restore process consumes the body.
func (c *FenceHTTPClient) RestoreFenced(ctx context.Context, namespace, endpoint, container, database string, source io.Reader, token lease.FenceToken) error {
	return c.RestoreFencedAt(ctx, namespace, endpoint, "", container, database, source, token)
}

// RestoreFencedAt is RestoreFenced with an optional Pod or Node IP for
// templates that route directly to a host-network sidecar through {node}.
func (c *FenceHTTPClient) RestoreFencedAt(ctx context.Context, namespace, endpoint, node, container, database string, source io.Reader, token lease.FenceToken) error {
	if err := validateFenceToken(token); err != nil {
		return err
	}
	if err := fencing.ValidateRestoreDatabase(database); err != nil {
		return err
	}
	if source == nil {
		return errors.New("fenced restore source is required")
	}
	if c == nil || c.client == nil {
		return errors.New("fence HTTP client is not configured")
	}
	if strings.TrimSpace(endpoint) == "" {
		return errors.New("fence endpoint is required")
	}
	if namespace == "" {
		namespace = "kube-ovn-operator-system"
	}
	if container == "" {
		container = "ovn-central"
	}
	requestURL, err := c.endpointURLForAt(namespace, endpoint, node, container, fencing.RestorePath)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, source)
	if err != nil {
		return fmt.Errorf("create fenced restore request: %w", err)
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("X-Kube-OVN-Database", database)
	request.Header.Set("X-Kube-OVN-Installation-UID", string(token.InstallationUID))
	request.Header.Set("X-Kube-OVN-Lease-UID", string(token.LeaseUID))
	request.Header.Set("X-Kube-OVN-Fence-Scope", string(token.Scope))
	request.Header.Set("X-Kube-OVN-Fence-Holder", token.Holder)
	request.Header.Set("X-Kube-OVN-Fence-Epoch", strconv.FormatInt(int64(token.Epoch), 10))
	response, err := c.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("send fenced restore request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	_, err = decodeFenceResponse(response, "fenced restore")
	return err
}

func (c *FenceHTTPClient) do(ctx context.Context, endpoint, namespace, container string, command []string, token *lease.FenceToken) ([]byte, error) {
	return c.doAt(ctx, endpoint, namespace, "", container, command, token)
}

func (c *FenceHTTPClient) doAt(ctx context.Context, endpoint, namespace, node, container string, command []string, token *lease.FenceToken) ([]byte, error) {
	if c == nil || c.client == nil {
		return nil, errors.New("fence HTTP client is not configured")
	}
	if strings.TrimSpace(endpoint) == "" {
		return nil, errors.New("fence endpoint is required")
	}
	if err := fencing.ValidateCommand(command); err != nil {
		return nil, err
	}
	// CommandClient passes the endpoint as the central Pod name. The template
	// still accepts explicit namespace/container placeholders for deployments
	// that route one sidecar per Pod; empty values simply omit those segments.
	if namespace == "" {
		namespace = "kube-ovn-operator-system"
	}
	if container == "" {
		container = "ovn-central"
	}
	requestBody := fencing.Request{Command: command, Token: token}
	body, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshal fenced command: %w", err)
	}
	if len(body) > maxFenceRequest {
		return nil, errors.New("fenced command request exceeds 8 KiB")
	}
	requestURL, err := c.endpointURLForAt(namespace, endpoint, node, container, fencing.ExecutePath)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create fenced command request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("send fenced command request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	return decodeFenceResponse(response, "fenced command")
}

func (c *FenceHTTPClient) usesNodePlaceholder() bool {
	return c != nil && strings.Contains(c.endpointTemplate, "{node}")
}

func (c *FenceHTTPClient) endpointURLForAt(namespace, endpoint, node, container, path string) (string, error) {
	template := c.endpointTemplate
	if strings.Contains(template, "{node}") {
		address, err := netip.ParseAddr(strings.TrimSpace(node))
		if err != nil {
			return "", errors.New("fence endpoint template requires a valid Pod or Node IP")
		}
		replacement := address.String()
		if address.Is6() {
			replacement = "[" + replacement + "]"
		}
		template = strings.ReplaceAll(template, "{node}", replacement)
	}
	replacer := strings.NewReplacer(
		"{namespace}", url.PathEscape(namespace),
		"{pod}", url.PathEscape(endpoint),
		"{container}", url.PathEscape(container),
	)
	parsed, err := url.Parse(replacer.Replace(template))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("fence endpoint template expanded to an invalid URL")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + path
	return parsed.String(), nil
}

func decodeFenceResponse(response *http.Response, operation string) ([]byte, error) {
	reply, err := io.ReadAll(io.LimitReader(response.Body, maxFenceReply+1))
	if err != nil {
		return nil, fmt.Errorf("read %s response: %w", operation, err)
	}
	if len(reply) > maxFenceReply {
		return nil, errors.New("fenced response exceeds 128 KiB")
	}
	var envelope fencing.Response
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		if err := json.Unmarshal(reply, &envelope); err != nil {
			envelope.Message = strings.TrimSpace(string(reply))
		}
		return nil, remoteFenceError(response.StatusCode, envelope)
	}
	if err := json.Unmarshal(reply, &envelope); err != nil {
		return nil, fmt.Errorf("decode %s response: %w", operation, err)
	}
	if envelope.Code != "" || envelope.Message != "" {
		return nil, remoteFenceError(response.StatusCode, envelope)
	}
	return []byte(envelope.Output), nil
}

func validateFenceToken(token lease.FenceToken) error {
	if token.InstallationUID == "" || token.LeaseUID == "" || token.Scope != lease.Central || token.Holder == "" || token.Epoch < 1 {
		return errors.New("fenced command requires a complete central lease token")
	}
	return nil
}

func remoteFenceError(status int, envelope fencing.Response) error {
	message := strings.TrimSpace(envelope.Message)
	if message == "" {
		message = fmt.Sprintf("remote fence service returned HTTP %d", status)
	}
	if envelope.Code == "stale_fence" || envelope.Code == "fence_not_armed" {
		return fmt.Errorf("%w: %s", ovnadmin.ErrStaleFence, message)
	}
	if status == http.StatusNotFound || status == http.StatusNotImplemented || status == http.StatusServiceUnavailable {
		return fmt.Errorf("%w: %s", ovnadmin.ErrFencingUnavailable, message)
	}
	return errors.New(message)
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}

// FencedPodExecutor preserves the normal Kubernetes Pod transport for reads
// and streaming backup. Membership mutations and destructive restore input are
// sent to the separately configured remote fence service.
type FencedPodExecutor struct {
	*PodExecutor
	Remote *FenceHTTPClient
}

func NewFencedPodExecutor(config *rest.Config, endpointTemplate string, client *http.Client) (*FencedPodExecutor, error) {
	pod, err := NewPodExecutor(config)
	if err != nil {
		return nil, err
	}
	remote, err := NewFenceHTTPClient(endpointTemplate, client)
	if err != nil {
		return nil, err
	}
	return &FencedPodExecutor{PodExecutor: pod, Remote: remote}, nil
}

// NewFencedPodExecutorWithRemote wires an already configured remote client to
// the normal observation and backup Pod executor. Keeping construction
// separate lets the caller enforce mTLS before the operator starts reconciling.
func NewFencedPodExecutorWithRemote(config *rest.Config, remote *FenceHTTPClient) (*FencedPodExecutor, error) {
	pod, err := NewPodExecutor(config)
	if err != nil {
		return nil, err
	}
	if remote == nil {
		return nil, errors.New("fence HTTP client is not configured")
	}
	return &FencedPodExecutor{PodExecutor: pod, Remote: remote}, nil
}

func (e *FencedPodExecutor) RunFenced(ctx context.Context, namespace, pod, container string, command []string, token lease.FenceToken) ([]byte, error) {
	if e == nil || e.Remote == nil {
		return nil, fmt.Errorf("%w: remote fence client is not configured", ovnadmin.ErrFencingUnavailable)
	}
	if err := fencing.ValidateMembershipCommand(command); err != nil {
		return nil, err
	}
	node := ""
	if e.Remote.usesNodePlaceholder() {
		var err error
		node, err = e.podAddress(ctx, namespace, pod)
		if err != nil {
			return nil, err
		}
	}
	return e.Remote.doAt(ctx, pod, namespace, node, container, command, &token)
}

// StreamInputFenced sends a database restore through the remote fence agent;
// it never falls back to Kubernetes pods/exec for destructive input.
func (e *FencedPodExecutor) StreamInputFenced(ctx context.Context, namespace, pod, container, database string, source io.Reader, token lease.FenceToken) error {
	if e == nil || e.Remote == nil {
		return fmt.Errorf("%w: remote fence client is not configured", ovnadmin.ErrFencingUnavailable)
	}
	if err := fencing.ValidateRestoreDatabase(database); err != nil {
		return err
	}
	node := ""
	if e.Remote.usesNodePlaceholder() {
		var err error
		node, err = e.podAddress(ctx, namespace, pod)
		if err != nil {
			return err
		}
	}
	return e.Remote.RestoreFencedAt(ctx, namespace, pod, node, container, database, source, token)
}

func (e *FencedPodExecutor) podAddress(ctx context.Context, namespace, pod string) (string, error) {
	if e == nil || e.client == nil {
		return "", errors.New("fenced Pod executor is not configured")
	}
	current, err := e.client.Pods(namespace).Get(ctx, pod, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read fenced Pod %s: %w", pod, err)
	}
	for _, candidate := range []string{current.Status.PodIP, current.Status.HostIP} {
		if address, parseErr := netip.ParseAddr(strings.TrimSpace(candidate)); parseErr == nil {
			return address.String(), nil
		}
	}
	if current.Spec.NodeName == "" {
		return "", errors.New("fenced Pod has no Pod IP, host IP or Node name")
	}
	node, err := e.client.Nodes().Get(ctx, current.Spec.NodeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("read fenced Pod Node %s: %w", current.Spec.NodeName, err)
	}
	for _, address := range node.Status.Addresses {
		if address.Type != corev1.NodeInternalIP {
			continue
		}
		if parsed, parseErr := netip.ParseAddr(strings.TrimSpace(address.Address)); parseErr == nil {
			return parsed.String(), nil
		}
	}
	return "", fmt.Errorf("node %s has no valid InternalIP", current.Spec.NodeName)
}
