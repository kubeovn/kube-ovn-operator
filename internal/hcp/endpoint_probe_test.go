package hcp

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

func TestProbeEndpointChecksTCPTransport(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan struct{})
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			close(accepted)
			_ = connection.Close()
		}
	}()
	if err := ProbeEndpoint(t.Context(), "tcp:"+listener.Addr().String(), nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-accepted:
	case <-t.Context().Done():
		t.Fatal("TCP probe did not reach the listener")
	}
}

func TestProbeEndpointRequiresTLSConfigForSSL(t *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	if err := ProbeEndpoint(t.Context(), "ssl:"+server.Listener.Addr().String(), nil); err == nil {
		t.Fatal("SSL endpoint was probed without TLS configuration")
	}
}

func TestProbeEndpointCompletesTLSHandshake(t *testing.T) {
	server := httptest.NewTLSServer(nil)
	defer server.Close()
	transport := server.Client().Transport.(*http.Transport)
	config := transport.TLSClientConfig.Clone()
	if err := ProbeEndpoint(t.Context(), "ssl:"+server.Listener.Addr().String(), config); err != nil {
		t.Fatal(err)
	}
}

func TestProbeEndpointsDoesNotProbeDisabledHCP(t *testing.T) {
	if err := ProbeEndpoints(t.Context(), operatorv1alpha1.HCPConfig{Enabled: false}, nil); err != nil {
		t.Fatal(err)
	}
}
