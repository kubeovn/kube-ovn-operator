package chart

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
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

func TestHelmRendererAppliesDataPlaneNodeSelector(t *testing.T) {
	renderer, spec := chartFixture(t, `apiVersion: apps/v1
kind: Deployment
metadata:
  name: kube-ovn-controller
  namespace: {{ .Values.namespace }}
spec:
  template:
    spec:
      containers:
      - name: controller
        image: kubeovn/kube-ovn:{{ .Values.global.images.kubeovn.tag }}
      nodeSelector:
        kubernetes.io/os: linux
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ovn-central
  namespace: {{ .Values.namespace }}
spec:
  template:
    spec:
      containers:
      - name: ovn-central
        image: kubeovn/kube-ovn:{{ .Values.global.images.kubeovn.tag }}
---
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: ovs-ovn
  namespace: {{ .Values.namespace }}
spec:
  template:
    spec:
      containers:
      - name: ovs
        image: kubeovn/kube-ovn:{{ .Values.global.images.kubeovn.tag }}
      nodeSelector:
        kubernetes.io/os: linux
`)
	mapper, ok := renderer.Mapper.(*meta.DefaultRESTMapper)
	if !ok {
		t.Fatal("chart fixture did not provide a mutable REST mapper")
	}
	mapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "DaemonSet"}, meta.RESTScopeNamespace)
	spec.DataPlane.NodeSelector = map[string]string{"network.example/zone": "edge-a"}
	got, err := renderer.Render(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range got.Objects {
		selector, found, err := unstructured.NestedStringMap(object.Object, "spec", "template", "spec", "nodeSelector")
		if err != nil {
			t.Fatal(err)
		}
		switch object.GetName() {
		case "kube-ovn-controller", "ovs-ovn":
			if !found || selector["kubernetes.io/os"] != "linux" || selector["network.example/zone"] != "edge-a" {
				t.Fatalf("data-plane selector was not merged for %s: found=%t selector=%v", object.GetName(), found, selector)
			}
		case "ovn-central":
			if found {
				t.Fatalf("central workload unexpectedly received data-plane selector: %v", selector)
			}
		}
	}
}

func TestHelmRendererRejectsConflictingDataPlaneNodeSelector(t *testing.T) {
	renderer, spec := chartFixture(t, `apiVersion: apps/v1
kind: Deployment
metadata:
  name: kube-ovn-controller
spec:
  template:
    spec:
      containers:
      - name: controller
        image: kubeovn/kube-ovn:v1.17.0
      nodeSelector:
        kubernetes.io/os: linux
`)
	spec.DataPlane.NodeSelector = map[string]string{"kubernetes.io/os": "windows"}
	if _, err := renderer.Render(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("conflicting data-plane selector was accepted: %v", err)
	}
}

func TestHelmRendererBindsExternalHCPSSLSecretOffline(t *testing.T) {
	data := testArchive(t, map[string]string{
		"Chart.yaml": "apiVersion: v2\nname: kube-ovn\nversion: 1.17.0\nappVersion: 1.17.0\n",
		"templates/workload.yaml": `apiVersion: apps/v1
kind: Deployment
metadata:
  name: kube-ovn-controller
spec:
  template:
    spec:
      containers:
      - name: controller
        env:
        - name: OVN_NB_ADDR
          value: tcp:nb.example:6641
        - name: OVN_SB_ADDR
          value: tcp:sb.example:6642
      volumes:
      - name: kube-ovn-tls
        secret:
          secretName: kube-ovn-tls
`,
		// The upstream chart uses this template to perform a live lookup. An
		// offline operator render must remove it for an external HCP Secret.
		"templates/ovn-tls-secret.yaml": `{{ fail "offline lookup must not run" }}`,
	})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(data) }))
	t.Cleanup(server.Close)
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Group: "apps", Version: "v1"}})
	mapper.Add(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, meta.RESTScopeNamespace)
	caps := chartutil.DefaultCapabilities.Copy()
	caps.KubeVersion = chartutil.KubeVersion{Version: "v1.34.3", Major: "1", Minor: "34"}
	renderer := &HelmRenderer{Name: "tenant", Namespace: "network", Mapper: mapper, Capabilities: caps, HTTPClient: server.Client()}
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.17.0", Chart: operatorv1alpha1.ChartSpec{Source: server.URL, Digest: fmt.Sprintf("sha256:%x", sha256.Sum256(data))},
		Mode:    operatorv1alpha1.InstallationModeDataPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		HCP: operatorv1alpha1.HCPConfig{
			Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
			NBEndpoint: "ssl:nb.example:6641", SBEndpoint: "ssl:sb.example:6642",
			TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "remote-ovn-tls"},
		},
	}
	got, err := renderer.Render(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Objects) != 1 {
		t.Fatalf("unexpected rendered object count: %d", len(got.Objects))
	}
	templateSpec, found, err := unstructured.NestedMap(got.Objects[0].Object, "spec", "template", "spec")
	if err != nil || !found {
		t.Fatalf("deployment Pod template missing: %v", err)
	}
	containers, found, err := unstructured.NestedSlice(templateSpec, "containers")
	if err != nil || !found || len(containers) != 1 {
		t.Fatalf("unexpected containers: %#v", containers)
	}
	env, _, _ := unstructured.NestedSlice(containers[0].(map[string]any), "env")
	addresses := make(map[string]string, len(env))
	for _, raw := range env {
		entry := raw.(map[string]any)
		addresses[entry["name"].(string)] = entry["value"].(string)
	}
	if addresses["OVN_NB_ADDR"] != "ssl:nb.example:6641" || addresses["OVN_SB_ADDR"] != "ssl:sb.example:6642" {
		t.Fatalf("external HCP addresses were not rendered as SSL: %#v", addresses)
	}
	volumes, _, _ := unstructured.NestedSlice(templateSpec, "volumes")
	secret := volumes[0].(map[string]any)["secret"].(map[string]any)
	if secret["secretName"] != "remote-ovn-tls" {
		t.Fatalf("HCP TLS Secret was not bound: %#v", secret)
	}
}

func TestHelmRendererInjectsFenceAgentIntoCentralWorkload(t *testing.T) {
	renderer, spec := chartFixture(t, `apiVersion: apps/v1
kind: Deployment
metadata:
  name: ovn-central
spec:
  template:
    spec:
      serviceAccountName: ovn-ovs
      containers:
      - name: ovn-central
        image: kubeovn/kube-ovn:v1.17.0
        volumeMounts:
        - name: host-run-ovn
          mountPath: /var/run/ovn
      volumes:
      - name: host-run-ovn
        hostPath:
          path: /run/ovn
`)
	renderer.FenceAgent = &FenceAgentConfig{
		Image:           "registry.example/fence-agent@sha256:" + strings.Repeat("a", 64),
		ServerTLSSecret: "fence-server",
		ClientCASecret:  "fence-client-ca",
		Port:            8443,
	}
	got, err := renderer.Render(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	var central *unstructured.Unstructured
	var roleBinding *unstructured.Unstructured
	for _, object := range got.Objects {
		switch {
		case object.GetKind() == "Deployment" && object.GetName() == "ovn-central":
			central = object
		case object.GetKind() == "RoleBinding" && strings.HasPrefix(object.GetName(), "kube-ovn-fence-agent-"):
			roleBinding = object
		}
	}
	if central == nil || roleBinding == nil {
		t.Fatalf("fence-agent resources missing: central=%v roleBinding=%v", central != nil, roleBinding != nil)
	}
	templateSpec, found, err := unstructured.NestedMap(central.Object, "spec", "template", "spec")
	if err != nil || !found {
		t.Fatalf("central Pod template missing: %v", err)
	}
	containers, found, err := unstructured.NestedSlice(templateSpec, "containers")
	if err != nil || !found || len(containers) != 2 {
		t.Fatalf("central containers = %#v", containers)
	}
	sidecar := containers[1].(map[string]any)
	if sidecar["name"] != "kube-ovn-fence-agent" || sidecar["image"] != renderer.FenceAgent.Image {
		t.Fatalf("unexpected fence sidecar: %#v", sidecar)
	}
	args, _, _ := unstructured.NestedStringSlice(sidecar, "args")
	if !slices.Contains(args, "--lease-name=kube-ovn-operator-central-tenant") || !slices.Contains(args, "--lease-namespace=network") {
		t.Fatalf("sidecar Lease args missing: %v", args)
	}
	volumes, _, _ := unstructured.NestedSlice(templateSpec, "volumes")
	if len(volumes) != 4 {
		t.Fatalf("central volumes = %#v", volumes)
	}
}

func TestHelmRendererPullsOCIChartLayer(t *testing.T) {
	chartData := testArchive(t, map[string]string{
		"Chart.yaml":              "apiVersion: v2\nname: kube-ovn\nversion: 1.17.0\nappVersion: 1.17.0\n",
		"templates/workload.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: {{ .Release.Name }}\n",
	})
	configData := []byte(`{"apiVersion":"v2","name":"kube-ovn","version":"1.17.0","appVersion":"1.17.0"}`)
	digest := func(data []byte) string { return fmt.Sprintf("sha256:%x", sha256.Sum256(data)) }
	chartDigest, configDigest := digest(chartData), digest(configData)
	manifestData, err := json.Marshal(map[string]any{
		"schemaVersion": 2,
		"config":        map[string]any{"mediaType": "application/vnd.cncf.helm.config.v1+json", "digest": configDigest, "size": len(configData)},
		"layers":        []any{map[string]any{"mediaType": "application/vnd.cncf.helm.chart.content.v1.tar+gzip", "digest": chartDigest, "size": len(chartData)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := digest(manifestData)
	blobs := map[string][]byte{configDigest: configData, chartDigest: chartData, manifestDigest: manifestData}
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/v2/" {
			writer.WriteHeader(http.StatusOK)
			return
		}
		if strings.Contains(request.URL.Path, "/manifests/") {
			if request.Method == http.MethodHead {
				writer.Header().Set("Docker-Content-Digest", manifestDigest)
				writer.Header().Set("Content-Length", fmt.Sprint(len(manifestData)))
				writer.WriteHeader(http.StatusOK)
				return
			}
			writer.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			_, _ = writer.Write(manifestData)
			return
		}
		if index := strings.Index(request.URL.Path, "/blobs/"); index >= 0 {
			data, ok := blobs[request.URL.Path[index+len("/blobs/"):]]
			if !ok {
				http.NotFound(writer, request)
				return
			}
			if request.Method == http.MethodHead {
				writer.Header().Set("Content-Length", fmt.Sprint(len(data)))
				writer.WriteHeader(http.StatusOK)
				return
			}
			_, _ = writer.Write(data)
			return
		}
		http.NotFound(writer, request)
	}))
	defer server.Close()

	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
	mapper.Add(corev1.SchemeGroupVersion.WithKind("ConfigMap"), meta.RESTScopeNamespace)
	caps := chartutil.DefaultCapabilities.Copy()
	caps.KubeVersion = chartutil.KubeVersion{Version: "v1.34.3", Major: "1", Minor: "34"}
	renderer := &HelmRenderer{Name: "tenant", Namespace: "network", Mapper: mapper, Capabilities: caps, HTTPClient: server.Client()}
	spec := operatorv1alpha1.KubeOVNInstallationSpec{Version: "v1.17.0", Chart: operatorv1alpha1.ChartSpec{Source: "oci://" + server.Listener.Addr().String() + "/repo/kube-ovn:1.17.0", Digest: chartDigest}, Mode: operatorv1alpha1.InstallationModeDataPlaneOnly, Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3}, DataPlane: operatorv1alpha1.DataPlaneSpec{ExternalOVNCentral: operatorv1alpha1.EndpointSpec{NBEndpoint: "nb.example:6641", SBEndpoint: "sb.example:6642"}}}
	got, err := renderer.Render(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Objects) != 1 || got.Objects[0].GetName() != "tenant" {
		t.Fatalf("unexpected OCI-rendered objects: %#v", got.Objects)
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
		{"OCI credentials", func(_ *HelmRenderer, s *operatorv1alpha1.KubeOVNInstallationSpec) {
			s.Chart.Source = "oci://user:password@example.com/kube-ovn:1.17.0"
		}, "without credentials"},
		{"OCI query", func(_ *HelmRenderer, s *operatorv1alpha1.KubeOVNInstallationSpec) {
			s.Chart.Source = "oci://registry.example.com/kube-ovn:1.17.0?source=unsafe"
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

// TestReleasedKubeOVNChartBindsExternalHCPSSL renders the real release chart
// with an external HCP TLS Secret. This exercises every rendered workload and
// RBAC object that the post-render binding must touch, including the chart's
// live-lookup TLS template.
func TestReleasedKubeOVNChartBindsExternalHCPSSL(t *testing.T) {
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
	caps := chartutil.DefaultCapabilities.Copy()
	caps.KubeVersion = chartutil.KubeVersion{Version: "v1.34.3", Major: "1", Minor: "34"}
	renderer := &HelmRenderer{Name: "kube-ovn", Namespace: "network", Mapper: mapper, Capabilities: caps, HTTPClient: server.Client()}
	spec := operatorv1alpha1.KubeOVNInstallationSpec{
		Version: "v1.16.13", Chart: operatorv1alpha1.ChartSpec{Source: server.URL, Digest: digest},
		Mode:    operatorv1alpha1.InstallationModeDataPlaneOnly,
		Central: operatorv1alpha1.CentralSpec{Mode: operatorv1alpha1.CentralModeRaft, Replicas: 3},
		HCP: operatorv1alpha1.HCPConfig{
			Enabled: true, Role: operatorv1alpha1.HCPRoleDataPlane, PairRef: "management", CentralNamespace: "hcp",
			NBEndpoint: "ssl:nb.example:6641", SBEndpoint: "ssl:sb.example:6642",
			TLSSecretRef: &operatorv1alpha1.LocalObjectReference{Name: "remote-ovn-tls"},
		},
	}
	got, err := renderer.Render(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	addresses := map[string]string{}
	boundSecret := false
	boundRBAC := false
	for _, object := range got.Objects {
		if object.GetKind() == "Secret" && object.GetName() == "kube-ovn-tls" {
			t.Fatal("external HCP render emitted the chart-owned kube-ovn-tls Secret")
		}
		visitRenderedMaps(object.Object, func(value map[string]any) {
			if name, ok := value["name"].(string); ok && (name == "OVN_NB_ADDR" || name == "OVN_SB_ADDR") {
				if address, ok := value["value"].(string); ok {
					addresses[name] = address
				}
			}
			if name, ok := value["secretName"].(string); ok && name == "remote-ovn-tls" {
				boundSecret = true
			}
			if names, ok := value["resourceNames"].([]any); ok {
				for _, raw := range names {
					if name, ok := raw.(string); ok && name == "remote-ovn-tls" {
						boundRBAC = true
					}
				}
			}
		})
		if hasExternalTLSReference(object.Object, "kube-ovn-tls") {
			t.Fatalf("rendered object %s/%s still references kube-ovn-tls", object.GetKind(), object.GetName())
		}
	}
	if addresses["OVN_NB_ADDR"] != "ssl:nb.example:6641" || addresses["OVN_SB_ADDR"] != "ssl:sb.example:6642" {
		t.Fatalf("external HCP addresses were not bound across the release chart: %#v", addresses)
	}
	if !boundSecret || !boundRBAC {
		t.Fatalf("release chart did not retain external HCP Secret and RBAC bindings: secret=%t rbac=%t", boundSecret, boundRBAC)
	}
}

func visitRenderedMaps(value any, visit func(map[string]any)) {
	switch node := value.(type) {
	case map[string]any:
		visit(node)
		for _, child := range node {
			visitRenderedMaps(child, visit)
		}
	case []any:
		for _, child := range node {
			visitRenderedMaps(child, visit)
		}
	}
}

func hasExternalTLSReference(value any, want string) bool {
	found := false
	visitRenderedMaps(value, func(node map[string]any) {
		if name, ok := node["secretName"].(string); ok && name == want {
			found = true
			return
		}
		if reference, ok := node["secretKeyRef"].(map[string]any); ok {
			if name, ok := reference["name"].(string); ok && name == want {
				found = true
				return
			}
		}
		if names, ok := node["resourceNames"].([]any); ok {
			for _, raw := range names {
				if name, ok := raw.(string); ok && name == want {
					found = true
					return
				}
			}
		}
	})
	return found
}
