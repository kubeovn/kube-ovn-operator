package probe

import (
	"context"
	"encoding/json/v2"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestLiveProbeEchoAndRefusals(t *testing.T) {
	identity := Identity{Pod: "server", PodUID: "server-uid", Node: "node-1"}
	source := Identity{Pod: "client", PodUID: "client-uid", Node: "node-0"}
	request := Request{Nonce: strings.Repeat("ab", 16), ExpectedAddress: "10.0.0.1", Family: "IPv4"}
	for _, scenario := range []string{"healthy", "wrong nonce", "empty identity", "redirect", "oversized", "unknown field", "duplicate field", "cancelled"} {
		t.Run(scenario, func(t *testing.T) {
			handler := Handler(identity)
			if scenario != "healthy" {
				handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch scenario {
					case "redirect":
						http.Redirect(w, r, "/healthz", http.StatusFound)
					case "oversized":
						_, _ = io.WriteString(w, strings.Repeat(" ", 4097))
					case "cancelled":
						<-r.Context().Done()
					case "unknown field":
						_, _ = io.WriteString(w, `{"untrusted":true}`)
					case "duplicate field":
						_, _ = io.WriteString(w, `{"nonce":"a","nonce":"b"}`)
					default:
						response := Response{Version: ProtocolVersion, Nonce: request.Nonce, Destination: identity}
						if scenario == "wrong nonce" {
							response.Nonce = strings.Repeat("cd", 16)
						} else {
							response.Destination.PodUID = ""
						}
						_ = json.MarshalWrite(w, response)
					}
				})
			}
			server := httptest.NewServer(handler)
			defer server.Close()
			ctx := t.Context()
			if scenario == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 30*time.Millisecond)
				defer cancel()
			}
			response, err := checkAddress(ctx, source, request, strings.TrimPrefix(server.URL, "http://"))
			if scenario == "healthy" {
				if err != nil || response.Destination != identity || response.Source != source || response.Address != request.ExpectedAddress {
					t.Fatalf("healthy echo: %+v, %v", response, err)
				}
			} else if err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}

func TestLiveProbeIPv6(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	server := httptest.NewUnstartedServer(Handler(Identity{Pod: "v6", PodUID: "v6-uid", Node: "v6-node"}))
	_ = server.Listener.Close()
	server.Listener = listener
	server.Start()
	defer server.Close()
	request := Request{Nonce: strings.Repeat("12", 16), ExpectedAddress: "fd00::1", Family: "IPv6"}
	if _, err := checkAddress(t.Context(), Identity{Pod: "client", PodUID: "uid", Node: "node"}, request, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
}

func TestProbeRejectsAddressFamilyAndDNSMismatch(t *testing.T) {
	for _, request := range []Request{
		{Host: "127.0.0.1", ExpectedAddress: "127.0.0.2", Family: "IPv4", Nonce: strings.Repeat("ab", 16)},
		{Host: "::1", ExpectedAddress: "::1", Family: "IPv4", Nonce: strings.Repeat("ab", 16)},
		{Host: "127.0.0.1", ExpectedAddress: "127.0.0.1", Family: "IPv4", Nonce: "stale"},
	} {
		if _, err := Check(t.Context(), Identity{}, request); err == nil {
			t.Fatalf("accepted %+v", request)
		}
	}
}
