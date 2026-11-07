// Package fenceagent constructs the shared native membership fence sidecar.
package fenceagent

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kvalidation "k8s.io/apimachinery/pkg/util/validation"
)

// Config describes the optional sidecar injected into every
// rendered ovn-central workload. The sidecar shares the central Pod's OVN
// socket volume and observes the installation's central operation Lease.
// Certificate Secrets are referenced by name; their data never enters the
// rendered values or plan ConfigMap.
type Config struct {
	Image           string
	ServerTLSSecret string
	ClientCASecret  string
	Port            int32
}

// Validate requires an immutable image and explicit existing certificate Secrets.
func (c *Config) Validate() error {
	if c == nil {
		return nil
	}
	_, digest, hasDigest := strings.Cut(strings.TrimSpace(c.Image), "@sha256:")
	if !hasDigest || digest == "" || len(digest) != 64 {
		return errors.New("fence-agent image must be digest-pinned")
	}
	if _, err := hex.DecodeString(digest); err != nil {
		return errors.New("fence-agent image must use a hexadecimal sha256 digest")
	}
	for name, value := range map[string]string{"server TLS Secret": c.ServerTLSSecret, "client CA Secret": c.ClientCASecret} {
		if errs := kvalidation.IsDNS1123Subdomain(value); len(errs) != 0 {
			return fmt.Errorf("fence-agent %s is invalid: %s", name, strings.Join(errs, "; "))
		}
	}
	if c.Port < 1 || c.Port > 65535 {
		return errors.New("fence-agent port must be between 1 and 65535")
	}
	return nil
}

// Inject adds the fence sidecar using the native member's existing socket volume.
func Inject(object *unstructured.Unstructured, config Config, installation, namespace string) error {
	templateSpec, found, err := unstructured.NestedMap(object.Object, "spec", "template", "spec")
	if err != nil || !found {
		return errors.New("ovn-central workload has no Pod template")
	}
	containers, found, err := unstructured.NestedSlice(templateSpec, "containers")
	if err != nil || !found || len(containers) == 0 {
		return errors.New("ovn-central workload has no containers")
	}
	var socketVolume, socketPath string
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if !ok || container["name"] != "ovn-central" {
			continue
		}
		mounts, _, _ := unstructured.NestedSlice(container, "volumeMounts")
		for _, rawMount := range mounts {
			mount, ok := rawMount.(map[string]any)
			if ok && (mount["mountPath"] == "/var/run/ovn" || mount["mountPath"] == "/var/run") {
				socketVolume, _ = mount["name"].(string)
				socketPath, _ = mount["mountPath"].(string)
				break
			}
		}
	}
	if socketVolume == "" {
		return errors.New("ovn-central container does not expose /var/run/ovn")
	}
	for _, raw := range containers {
		container, ok := raw.(map[string]any)
		if ok && container["name"] == "kube-ovn-fence-agent" {
			return errors.New("ovn-central workload already contains kube-ovn-fence-agent")
		}
	}
	port := int64(config.Port)
	leaseName := lease.Name(lease.Central, installation)
	sidecar := map[string]any{
		"name":    "kube-ovn-fence-agent",
		"image":   config.Image,
		"command": []any{"/fence-agent"},
		"args": []any{
			"--listen-address=:" + fmt.Sprint(config.Port),
			"--lease-namespace=" + namespace,
			"--lease-name=" + leaseName,
			"--tls-cert-file=/var/run/fence/server/tls.crt",
			"--tls-key-file=/var/run/fence/server/tls.key",
			"--tls-client-ca-file=/var/run/fence/client-ca/ca.crt",
		},
		"ports": []any{map[string]any{"name": "fence", "containerPort": port, "protocol": "TCP"}},
		"securityContext": map[string]any{
			"allowPrivilegeEscalation": false,
			"capabilities":             map[string]any{"drop": []any{"ALL"}},
			"readOnlyRootFilesystem":   true,
			"runAsNonRoot":             true,
			"runAsUser":                int64(65532),
			"runAsGroup":               int64(65532),
		},
		"volumeMounts": []any{
			map[string]any{"name": socketVolume, "mountPath": socketPath, "readOnly": true},
			map[string]any{"name": "kube-ovn-fence-server-tls", "mountPath": "/var/run/fence/server", "readOnly": true},
			map[string]any{"name": "kube-ovn-fence-client-ca", "mountPath": "/var/run/fence/client-ca", "readOnly": true},
			map[string]any{"name": "kube-ovn-fence-tmp", "mountPath": "/tmp"},
		},
		// The fence endpoint requires a client certificate for every TLS
		// handshake. Kubernetes HTTP probes cannot present one, so use a TCP
		// probe to check that the mTLS listener is accepting connections.
		"readinessProbe": map[string]any{"tcpSocket": map[string]any{"port": port}, "periodSeconds": int64(5), "timeoutSeconds": int64(3)},
		"livenessProbe":  map[string]any{"tcpSocket": map[string]any{"port": port}, "periodSeconds": int64(10), "timeoutSeconds": int64(3)},
	}
	templateSpec["containers"] = append(containers, sidecar)
	volumes, _, err := unstructured.NestedSlice(templateSpec, "volumes")
	if err != nil {
		return fmt.Errorf("read ovn-central volumes: %w", err)
	}
	volumes = append(volumes,
		map[string]any{"name": "kube-ovn-fence-server-tls", "secret": map[string]any{"secretName": config.ServerTLSSecret}},
		map[string]any{"name": "kube-ovn-fence-client-ca", "secret": map[string]any{"secretName": config.ClientCASecret}},
		map[string]any{"name": "kube-ovn-fence-tmp", "emptyDir": map[string]any{}},
	)
	templateSpec["volumes"] = volumes
	if err := unstructured.SetNestedMap(object.Object, templateSpec, "spec", "template", "spec"); err != nil {
		return fmt.Errorf("write ovn-central Pod template: %w", err)
	}
	return nil
}

// ServiceAccount returns the rendered central workload identity.
func ServiceAccount(object *unstructured.Unstructured) string {
	templateSpec, _, _ := unstructured.NestedMap(object.Object, "spec", "template", "spec")
	name, _, _ := unstructured.NestedString(templateSpec, "serviceAccountName")
	return name
}

// ResourceName returns the installation-scoped fence resource name.
func ResourceName(installation string) string {
	name := "kube-ovn-fence-agent-" + installation
	if len(name) > 63 {
		digest := sha256.Sum256([]byte(installation))
		suffix := "-" + hex.EncodeToString(digest[:])[:8]
		name = strings.TrimSuffix(name[:63-len(suffix)], "-") + suffix
	}
	return name
}

// Role permits reads and a name-filtered watch of one central Lease.
func Role(installation, namespace string) *unstructured.Unstructured {
	role := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
		"metadata": map[string]any{"name": ResourceName(installation), "namespace": namespace},
		"rules":    []any{map[string]any{"apiGroups": []any{"coordination.k8s.io"}, "resources": []any{"leases"}, "verbs": []any{"get", "watch"}, "resourceNames": []any{lease.Name(lease.Central, installation)}}},
	}}
	return role
}

// RoleBinding authorizes the rendered central and replacement member identities.
func RoleBinding(installation, namespace, serviceAccount string) *unstructured.Unstructured {
	binding := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "RoleBinding",
		"metadata": map[string]any{"name": ResourceName(installation), "namespace": namespace},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": ResourceName(installation)},
		"subjects": []any{map[string]any{"kind": "ServiceAccount", "name": serviceAccount, "namespace": namespace}, map[string]any{"kind": "ServiceAccount", "name": ResourceName(installation), "namespace": namespace}},
	}}
	return binding
}

// Account is used by replacement members. Its token is projected only into
// the fence sidecar; native database and preparation containers have no token.
func Account(installation, namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ServiceAccount",
		"metadata":                     map[string]any{"name": ResourceName(installation), "namespace": namespace},
		"automountServiceAccountToken": false,
	}}
}
