package chart

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"helm.sh/helm/v3/pkg/chartutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testArchive(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&buffer)
	writer := tar.NewWriter(gzipWriter)
	for name, data := range files {
		if err := writer.WriteHeader(&tar.Header{Name: "kube-ovn/" + name, Mode: 0600, Size: int64(len(data))}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func chartFixture(t *testing.T, template string) (*HelmRenderer, operatorv1alpha1.KubeOVNInstallationSpec) {
	t.Helper()
	data := testArchive(t, map[string]string{
		"Chart.yaml":              "apiVersion: v2\nname: kube-ovn\nversion: 1.17.0\nappVersion: 1.17.0\nkubeVersion: '>=1.29.0'\n",
		"templates/workload.yaml": template,
	})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(data) }))
	t.Cleanup(server.Close)
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, {Group: "apps", Version: "v1"}})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, meta.RESTScopeNamespace)
	caps := chartutil.DefaultCapabilities.Copy()
	caps.KubeVersion = chartutil.KubeVersion{Version: "v1.34.3", Major: "1", Minor: "34"}
	return &HelmRenderer{Name: "tenant", Namespace: "network", Mapper: mapper, Capabilities: caps, HTTPClient: server.Client()}, operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Chart: operatorv1alpha1.ChartSpec{Source: server.URL + "/chart.tgz", Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(data))},
		Mode:      operatorv1alpha1.InstallationModeDataPlaneOnly,
		Central:   operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		DataPlane: operatorv1alpha1.DataPlaneSpec{ExternalOVNCentral: operatorv1alpha1.EndpointSpec{NBEndpoint: "nb.example", SBEndpoint: "sb.example"}},
	}
}

func TestHelmRendererUsesUpstreamValuesAndOmitsLifecycleHooks(t *testing.T) {
	renderer, spec := chartFixture(t, `apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .Release.Name }}
  namespace: {{ .Values.namespace }}
spec:
  template:
    spec:
      containers:
      - name: controller
        image: kubeovn/kube-ovn:{{ .Values.global.images.kubeovn.tag }}
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: upgrade-only
  annotations:
    helm.sh/hook: pre-upgrade
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: delete-only
  annotations:
    helm.sh/hook: post-delete
`)
	got, err := renderer.Render(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Objects) != 1 || got.Objects[0].GetName() != "tenant" || got.Objects[0].GetNamespace() != "network" {
		t.Fatalf("unexpected objects: %#v", got.Objects)
	}
	if !strings.Contains(string(got.ValuesYAML), "OVN_CENTRAL_MODE: cluster") || !strings.Contains(string(got.ValuesYAML), "tag: v1.17.0") {
		t.Fatal("upstream values were not mapped")
	}
}

func TestHelmRendererRejectsUnverifiedAndUnsupportedInputs(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*HelmRenderer, *operatorv1alpha1.KubeOVNInstallationSpec)
		want   string
	}{
		{"missing digest", func(_ *HelmRenderer, s *operatorv1alpha1.KubeOVNInstallationSpec) { s.Chart.Digest = "" }, "sha256"},
		{"wrong digest", func(_ *HelmRenderer, s *operatorv1alpha1.KubeOVNInstallationSpec) {
			s.Chart.Digest = "sha256:" + strings.Repeat("0", 64)
		}, "mismatch"},
		{"credentials", func(_ *HelmRenderer, s *operatorv1alpha1.KubeOVNInstallationSpec) {
			s.Chart.Source = "https://user:password@example.com/chart.tgz"
		}, "without credentials"},
		{"HTTP", func(_ *HelmRenderer, s *operatorv1alpha1.KubeOVNInstallationSpec) {
			s.Chart.Source = "http://example.com/chart.tgz"
		}, "HTTPS"},
		{"wrong app version", func(_ *HelmRenderer, s *operatorv1alpha1.KubeOVNInstallationSpec) { s.Version = "v1.16.0" }, "appVersion"},
		{"old Kubernetes", func(r *HelmRenderer, _ *operatorv1alpha1.KubeOVNInstallationSpec) {
			r.Capabilities.KubeVersion.Version = "v1.28.0"
		}, "kubeVersion"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			renderer, spec := chartFixture(t, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: example\n")
			tt.mutate(renderer, &spec)
			got, err := renderer.Render(t.Context(), spec)
			if err == nil || !strings.Contains(err.Error(), tt.want) || len(got.Objects) != 0 {
				t.Fatalf("got %v, objects %d", err, len(got.Objects))
			}
		})
	}
}

func TestHelmRendererRejectsInstallHookAndCrossNamespace(t *testing.T) {
	for _, fragment := range []string{"  annotations:\n    helm.sh/hook: pre-install\n", "  namespace: another-tenant\n"} {
		renderer, spec := chartFixture(t, "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: example\n"+fragment)
		if _, err := renderer.Render(t.Context(), spec); err == nil {
			t.Fatal("unsafe chart was accepted")
		}
	}
}

func TestHelmRendererChecksRaftBootstrapAndStorageMapping(t *testing.T) {
	renderer, spec := chartFixture(t, `apiVersion: v1
kind: ConfigMap
metadata:
  name: central-values
data:
  mode: {{ .Values.OVN_CENTRAL_MODE | quote }}
  nodes: {{ .Values.MASTER_NODES | quote }}
  storage: {{ index .Values "ovn-central" "storage" "size" | quote }}
`)
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	node := &corev1.Node{Name: "control-plane", Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "192.0.2.1"}}}}
	renderer.Reader = fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
	spec.Mode = operatorv1alpha1.InstallationModeControlPlaneOnly
	if _, err := renderer.Render(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "expected 3") {
		t.Fatalf("unexpected mismatch result: %v", err)
	}
	spec.Central.Mode = operatorv1alpha1.CentralModeSingle
	spec.Central.Replicas = 1
	spec.Central.Storage.Size = "5Gi"
	got, err := renderer.Render(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	data := got.Objects[0].Object["data"].(map[string]any)
	if data["mode"] != "single" || data["storage"] != "5Gi" || data["nodes"] != "192.0.2.1" {
		t.Fatalf("unexpected central values: %#v", data)
	}
}

// This test renders a checksum-pinned upstream release, not an imitation of
// its templates. CI supplies the public release archive explicitly.
func TestReleasedKubeOVNChart(t *testing.T) {
	archive := os.Getenv("KUBE_OVN_CHART_ARCHIVE")
	if archive == "" {
		t.Skip("set KUBE_OVN_CHART_ARCHIVE to the v1.16.13 release archive")
	}
	data, err := os.ReadFile(archive)
	if err != nil {
		t.Fatal(err)
	}
	const digest = "sha256:24df922c7aa78e4a21e77289a6eec9a19ac50cb00953df9ada9291b61a638475"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(data) }))
	defer server.Close()
	mapper := meta.NewDefaultRESTMapper(nil)
	for _, entry := range []struct {
		group, kind string
		scope       meta.RESTScope
	}{
		{"", "ConfigMap", meta.RESTScopeNamespace}, {"", "Secret", meta.RESTScopeNamespace}, {"", "Service", meta.RESTScopeNamespace}, {"", "ServiceAccount", meta.RESTScopeNamespace}, {"", "PersistentVolumeClaim", meta.RESTScopeNamespace},
		{"apps", "Deployment", meta.RESTScopeNamespace}, {"apps", "StatefulSet", meta.RESTScopeNamespace}, {"apps", "DaemonSet", meta.RESTScopeNamespace},
		{"rbac.authorization.k8s.io", "ClusterRole", meta.RESTScopeRoot}, {"rbac.authorization.k8s.io", "ClusterRoleBinding", meta.RESTScopeRoot}, {"rbac.authorization.k8s.io", "Role", meta.RESTScopeNamespace}, {"rbac.authorization.k8s.io", "RoleBinding", meta.RESTScopeNamespace},
		{"apiextensions.k8s.io", "CustomResourceDefinition", meta.RESTScopeRoot},
	} {
		mapper.Add(schema.GroupVersionKind{Group: entry.group, Version: "v1", Kind: entry.kind}, entry.scope)
	}
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	nodes := []client.Object{}
	for i := range 3 {
		nodes = append(nodes, &corev1.Node{Name: fmt.Sprintf("node-%d", i), Labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}, Status: corev1.NodeStatus{Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: fmt.Sprintf("192.0.2.%d", i+1)}}}})
	}
	caps := chartutil.DefaultCapabilities.Copy()
	caps.KubeVersion = chartutil.KubeVersion{Version: "v1.34.3", Major: "1", Minor: "34"}
	renderer := &HelmRenderer{Name: "kube-ovn", Namespace: "network", Mapper: mapper, Capabilities: caps, HTTPClient: server.Client(), Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodes...).Build()}
	for _, mode := range []operatorv1alpha1.InstallationMode{operatorv1alpha1.InstallationModeFull, operatorv1alpha1.InstallationModeControlPlaneOnly, operatorv1alpha1.InstallationModeDataPlaneOnly} {
		t.Run(string(mode), func(t *testing.T) {
			spec := operatorv1alpha1.KubeOVNInstallationSpec{
				Version: "v1.16.13", Chart: operatorv1alpha1.ChartSpec{Source: server.URL, Digest: digest}, Mode: mode,
				Central:   operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
				DataPlane: operatorv1alpha1.DataPlaneSpec{ExternalOVNCentral: operatorv1alpha1.EndpointSpec{NBEndpoint: "nb.example", SBEndpoint: "sb.example"}},
			}
			got, err := renderer.Render(t.Context(), spec)
			if err != nil {
				t.Fatal(err)
			}
			central, controller, ovs, cni := false, false, false, false
			for _, object := range got.Objects {
				if object.GetAnnotations()["helm.sh/hook"] != "" {
					t.Fatal("hook included in apply batch")
				}
				if object.GetKind() == "Deployment" && object.GetName() == "ovn-central" {
					central = true
					replicas, _, _ := unstructured.NestedInt64(object.Object, "spec", "replicas")
					if replicas != 3 {
						t.Fatalf("central replicas=%d", replicas)
					}
				}
				if object.GetKind() == "Deployment" && object.GetName() == "kube-ovn-controller" {
					controller = true
				}
				if object.GetKind() == "DaemonSet" && object.GetName() == "ovs-ovn" {
					ovs = true
				}
				if object.GetKind() == "DaemonSet" && object.GetName() == "kube-ovn-cni" {
					cni = true
				}
				if object.GetNamespace() != "" && object.GetNamespace() != "network" {
					t.Fatal("namespace mismatch")
				}
			}
			if central != (mode != operatorv1alpha1.InstallationModeDataPlaneOnly) || controller != (mode != operatorv1alpha1.InstallationModeControlPlaneOnly) || ovs != controller || cni != controller {
				t.Fatalf("incorrect mode split: central=%t controller=%t ovs=%t cni=%t", central, controller, ovs, cni)
			}
		})
	}
}
