// Package probe implements the bounded, credential-free network canary protocol.
package probe

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"time"
)

const (
	Port            = 8080
	ClientApp       = "kube-ovn-network-client"
	ServerApp       = "kube-ovn-network-server"
	Container       = "probe"
	ProtocolVersion = 1
)

func Name(uid string) string { return fmt.Sprintf("koi-probe-%x", sha256.Sum256([]byte(uid)))[:26] }

type Identity struct {
	Pod    string `json:"pod"`
	PodUID string `json:"podUID"`
	Node   string `json:"node"`
}

type Request struct {
	Host            string `json:"host"`
	Family          string `json:"family"`
	ExpectedAddress string `json:"expectedAddress"`
	Nonce           string `json:"nonce"`
}

type Response struct {
	Version     int      `json:"version"`
	Nonce       string   `json:"nonce"`
	Source      Identity `json:"source"`
	Destination Identity `json:"destination"`
	Address     string   `json:"address"`
}

func validNonce(value string) bool {
	data, err := hex.DecodeString(value)
	return err == nil && len(data) == 16
}

func Handler(identity Identity) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /echo", func(w http.ResponseWriter, r *http.Request) {
		nonce := r.URL.Query().Get("nonce")
		if !validNonce(nonce) {
			http.Error(w, "invalid nonce", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.MarshalWrite(w, Response{Version: ProtocolVersion, Nonce: nonce, Destination: identity})
	})
	return mux
}

// Check resolves the requested address family inside the source pod, checks
// the answer against the Kubernetes inventory, then dials that exact address.
// It bypasses environment proxies and refuses redirects and oversized replies.
func Check(ctx context.Context, source Identity, request Request) (Response, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	expected, err := resolveAddress(ctx, request)
	if err != nil {
		return Response{}, err
	}
	return checkAddress(ctx, source, request, net.JoinHostPort(expected.String(), fmt.Sprint(Port)))
}

func resolveAddress(ctx context.Context, request Request) (netip.Addr, error) {
	if !validNonce(request.Nonce) || (request.Family != "IPv4" && request.Family != "IPv6") {
		return netip.Addr{}, fmt.Errorf("invalid probe request")
	}
	expected, err := netip.ParseAddr(request.ExpectedAddress)
	if err != nil || expected.Is4In6() || expected.Zone() != "" || expected.Is4() != (request.Family == "IPv4") {
		return netip.Addr{}, fmt.Errorf("invalid expected address")
	}
	network := "ip4"
	if expected.Is6() {
		network = "ip6"
	}
	addresses, err := net.DefaultResolver.LookupNetIP(ctx, network, request.Host)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("resolve probe destination: %w", err)
	}
	// Go can return IPv4 literals as IPv4-mapped IPv6 addresses. Compare
	// canonical addresses while retaining the requested family constraint.
	for i := range addresses {
		addresses[i] = addresses[i].Unmap()
	}
	if len(addresses) == 0 || !slices.Contains(addresses, expected) {
		return netip.Addr{}, fmt.Errorf("probe DNS did not return the expected Service address")
	}
	for _, address := range addresses {
		if address != expected {
			return netip.Addr{}, fmt.Errorf("probe DNS returned an unexpected address")
		}
	}
	return expected, nil
}

func checkAddress(ctx context.Context, source Identity, request Request, address string) (Response, error) {
	target := url.URL{Scheme: "http", Host: address, Path: "/echo", RawQuery: url.Values{"nonce": {request.Nonce}}.Encode()}
	transport := &http.Transport{DisableKeepAlives: true, DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return Response{}, err
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		return Response{}, fmt.Errorf("probe connection failed: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return Response{}, fmt.Errorf("probe returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 4097))
	if err != nil || len(data) > 4096 {
		return Response{}, fmt.Errorf("invalid probe response size")
	}
	var result Response
	if err := json.Unmarshal(data, &result, json.RejectUnknownMembers(true)); err != nil {
		return Response{}, fmt.Errorf("invalid probe response")
	}
	if result.Version != ProtocolVersion || result.Nonce != request.Nonce || result.Destination.PodUID == "" || result.Destination.Pod == "" || result.Destination.Node == "" {
		return Response{}, fmt.Errorf("probe response identity or nonce mismatch")
	}
	result.Source, result.Address = source, request.ExpectedAddress
	return result, nil
}
