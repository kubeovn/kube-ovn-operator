package chart

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/fenceagent"
	"github.com/kubeovn/kube-ovn-operator/internal/hcp"
	"helm.sh/helm/v3/pkg/chart"
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	"helm.sh/helm/v3/pkg/registry"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const maxArchiveSize = 20 << 20

// HelmRenderer renders an immutable upstream chart archive in install mode.
// Helm release storage and hooks are deliberately not used as reconcilers.
// The controller pins the rendered manifest before the first write; changes
// need a lifecycle executor rather than re-running install with new inputs.
type HelmRenderer struct {
	Name         string
	Namespace    string
	Reader       client.Reader
	Mapper       meta.RESTMapper
	Capabilities *chartutil.Capabilities
	HTTPClient   *http.Client
	FenceAgent   *FenceAgentConfig
	// AllowedChartHosts is the exact HTTPS/OCI host allowlist for chart
	// archives. A nil slice preserves the in-process renderer's legacy test
	// behavior; production wiring supplies a non-nil list, so an empty list
	// rejects every remote chart until an operator administrator configures a
	// trusted registry.
	AllowedChartHosts []string
}

// FenceAgentConfig configures the shared central membership fence sidecar.
type FenceAgentConfig = fenceagent.Config

func (r *HelmRenderer) Render(ctx context.Context, spec operatorv1alpha1.KubeOVNInstallationSpec) (Rendered, error) {
	values, err := installationValues(spec)
	if err != nil {
		return Rendered{}, err
	}
	if r.Name == "" || r.Namespace == "" || r.Mapper == nil {
		return Rendered{}, errors.New("chart release identity and REST mapper are required")
	}
	// HCP peer admission is performed by the controller before any workload
	// write. The renderer only maps the public endpoint references and never
	// reads or copies TLS Secret data.
	if spec.Central.Placement.TopologyKey != "" && spec.Central.Placement.TopologyKey != "kubernetes.io/hostname" {
		return Rendered{}, errors.New("the upstream chart supports central anti-affinity by kubernetes.io/hostname only")
	}
	data, err := r.fetch(ctx, spec.Chart)
	if err != nil {
		return Rendered{}, err
	}
	ch, err := loader.LoadArchive(bytes.NewReader(data))
	if err != nil {
		return Rendered{}, fmt.Errorf("load chart archive: %w", err)
	}
	if ch.Metadata.Name != "kube-ovn" || strings.TrimPrefix(ch.Metadata.AppVersion, "v") != strings.TrimPrefix(spec.Version, "v") {
		return Rendered{}, errors.New("chart must be kube-ovn with appVersion matching spec.version")
	}
	if len(ch.Metadata.Dependencies) != 0 {
		return Rendered{}, errors.New("chart dependencies are not supported by the initial installation executor")
	}
	caps := r.Capabilities
	if caps == nil {
		return Rendered{}, errors.New("target Kubernetes capabilities are required")
	}
	if ch.Metadata.KubeVersion != "" && !chartutil.IsCompatibleRange(ch.Metadata.KubeVersion, caps.KubeVersion.Version) {
		return Rendered{}, errors.New("chart kubeVersion constraint does not match the target cluster")
	}
	values["namespace"] = r.Namespace
	if err := r.bootstrapNodes(ctx, spec, values); err != nil {
		return Rendered{}, err
	}
	externalTLSSecret, externalTLS, err := externalHCPChartTLS(spec)
	if err != nil {
		return Rendered{}, err
	}
	if externalTLS {
		// The upstream data-plane chart's TLS Secret template uses Helm lookup
		// and fails when rendered offline. The Secret is owned by the HCP
		// contract, so remove that template and bind every generated reference
		// to the caller-provided Secret instead.
		ch.Templates = slices.DeleteFunc(ch.Templates, func(file *chart.File) bool {
			return strings.HasSuffix(file.Name, "templates/ovn-tls-secret.yaml")
		})
	}
	valuesYAML, err := yaml.Marshal(values)
	if err != nil {
		return Rendered{}, err
	}
	// Normalize numeric/map types exactly as Helm's values-file path does.
	normalized, err := chartutil.ReadValues(valuesYAML)
	if err != nil {
		return Rendered{}, err
	}
	renderValues, err := chartutil.ToRenderValues(ch, normalized, chartutil.ReleaseOptions{
		Name: r.Name, Namespace: r.Namespace, Revision: 1, IsInstall: true,
	}, caps)
	if err != nil {
		return Rendered{}, fmt.Errorf("prepare Helm values: %w", err)
	}
	renderValues["Release"].(map[string]any)["Service"] = "kube-ovn-operator"
	// Offline Helm rendering is intentional. Live safety gates belong to the
	// controller; template lookup must not be mistaken for a membership check.
	manifests, err := engine.Render(ch, renderValues)
	if err != nil {
		return Rendered{}, fmt.Errorf("render Helm chart: %w", err)
	}
	for _, crd := range ch.CRDObjects() {
		manifests[crd.Filename] = string(crd.File.Data)
	}
	var objects []*unstructured.Unstructured
	for _, name := range slices.Sorted(maps.Keys(manifests)) {
		if strings.HasSuffix(name, "/NOTES.txt") {
			continue
		}
		decoded, err := r.decode(manifests[name])
		if err != nil {
			return Rendered{}, fmt.Errorf("decode chart file %s: %w", name, err)
		}
		objects = append(objects, decoded...)
	}
	if len(objects) == 0 {
		return Rendered{}, errors.New("chart rendered no workload resources")
	}
	if externalTLS {
		if err := bindExternalHCPSecret(objects, externalTLSSecret); err != nil {
			return Rendered{}, err
		}
	}
	if err := applyDataPlaneNodeSelector(objects, spec.DataPlane.NodeSelector); err != nil {
		return Rendered{}, fmt.Errorf("apply data-plane node selector: %w", err)
	}
	if err := r.injectFenceAgent(&objects); err != nil {
		return Rendered{}, fmt.Errorf("inject fence-agent sidecar: %w", err)
	}
	return Rendered{Source: spec.Chart.Source, Digest: spec.Chart.Digest, ValuesYAML: valuesYAML, Objects: objects}, ctx.Err()
}

// applyDataPlaneNodeSelector applies the operator-owned data-plane placement
// contract after rendering. The upstream chart exposes a fixed Linux selector
// on its node agents and controller, but does not expose a values key for an
// additional selector. Merging after rendering keeps the chart reusable while
// preserving the upstream Linux requirement and rejecting an unschedulable
// conflict instead of silently overwriting it.
func applyDataPlaneNodeSelector(objects []*unstructured.Unstructured, selector map[string]string) error {
	if len(selector) == 0 {
		return nil
	}
	matched := 0
	for _, object := range objects {
		if !isDataPlaneWorkload(object) {
			continue
		}
		path := []string{"spec", "template", "spec"}
		podSpec, found, err := unstructured.NestedMap(object.Object, path...)
		if err != nil {
			return fmt.Errorf("read Pod template for %s/%s: %w", object.GetKind(), object.GetName(), err)
		}
		if !found {
			return fmt.Errorf("%s/%s has no Pod template", object.GetKind(), object.GetName())
		}
		current, found, err := unstructured.NestedStringMap(podSpec, "nodeSelector")
		if err != nil {
			return fmt.Errorf("read node selector for %s/%s: %w", object.GetKind(), object.GetName(), err)
		}
		if !found {
			current = make(map[string]string, len(selector))
		}
		for key, value := range selector {
			if existing, ok := current[key]; ok && existing != value {
				return fmt.Errorf("%s/%s selector %q conflicts with upstream value %q", object.GetKind(), object.GetName(), key, existing)
			}
			current[key] = value
		}
		if err := unstructured.SetNestedStringMap(podSpec, current, "nodeSelector"); err != nil {
			return fmt.Errorf("set node selector for %s/%s: %w", object.GetKind(), object.GetName(), err)
		}
		if err := unstructured.SetNestedMap(object.Object, podSpec, path...); err != nil {
			return fmt.Errorf("persist Pod template for %s/%s: %w", object.GetKind(), object.GetName(), err)
		}
		matched++
	}
	if matched == 0 {
		return errors.New("data-plane node selector did not match a rendered data-plane workload")
	}
	return nil
}

func isDataPlaneWorkload(object *unstructured.Unstructured) bool {
	if object == nil {
		return false
	}
	switch object.GetKind() {
	case "DaemonSet":
		return true
	case "Deployment":
		switch object.GetName() {
		case "kube-ovn-controller", "ovn-ic-controller":
			return true
		default:
			return false
		}
	default:
		return false
	}
}

func externalHCPChartTLS(spec operatorv1alpha1.KubeOVNInstallationSpec) (string, bool, error) {
	if spec.Mode != operatorv1alpha1.InstallationModeNodeNetworking || !spec.HCP.Enabled {
		return "", false, nil
	}
	northbound, err := hcp.ParseEndpoint(spec.HCP.NBEndpoint)
	if err != nil {
		return "", false, fmt.Errorf("parse HCP northbound endpoint: %w", err)
	}
	southbound, err := hcp.ParseEndpoint(spec.HCP.SBEndpoint)
	if err != nil {
		return "", false, fmt.Errorf("parse HCP southbound endpoint: %w", err)
	}
	if northbound.Protocol != southbound.Protocol {
		return "", false, errors.New("HCP northbound and southbound endpoints must use the same protocol")
	}
	if northbound.Protocol != "ssl" {
		return "", false, nil
	}
	if spec.HCP.TLSSecretRef == nil || strings.TrimSpace(spec.HCP.TLSSecretRef.Name) == "" {
		return "", false, errors.New("HCP SSL endpoints require tlsSecretRef")
	}
	return spec.HCP.TLSSecretRef.Name, true, nil
}

func bindExternalHCPSecret(objects []*unstructured.Unstructured, secretName string) error {
	if strings.TrimSpace(secretName) == "" {
		return errors.New("external HCP TLS Secret name is empty")
	}
	for _, object := range objects {
		if err := rewriteExternalHCPSecret(object.Object, secretName); err != nil {
			return fmt.Errorf("bind HCP TLS Secret in %s/%s: %w", object.GetKind(), object.GetName(), err)
		}
	}
	return nil
}

func rewriteExternalHCPSecret(value any, secretName string) error {
	switch node := value.(type) {
	case map[string]any:
		if name, ok := node["name"].(string); ok && (name == "OVN_NB_ADDR" || name == "OVN_SB_ADDR") {
			if address, ok := node["value"].(string); ok && strings.HasPrefix(address, "tcp:") {
				node["value"] = "ssl:" + strings.TrimPrefix(address, "tcp:")
			}
		}
		for key, child := range node {
			switch key {
			case "secretName":
				if name, ok := child.(string); ok && name == "kube-ovn-tls" {
					node[key] = secretName
				}
			case "secretKeyRef":
				if reference, ok := child.(map[string]any); ok {
					if name, ok := reference["name"].(string); ok && name == "kube-ovn-tls" {
						reference["name"] = secretName
					}
				}
			case "resourceNames":
				if names, ok := child.([]any); ok {
					for index, raw := range names {
						if name, ok := raw.(string); ok && name == "kube-ovn-tls" {
							names[index] = secretName
						}
					}
				}
			}
			if err := rewriteExternalHCPSecret(child, secretName); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range node {
			if err := rewriteExternalHCPSecret(child, secretName); err != nil {
				return err
			}
		}
	case nil, string, bool, int64, float64:
		return nil
	default:
		return fmt.Errorf("unsupported rendered value type %T", value)
	}
	return nil
}

func (r *HelmRenderer) injectFenceAgent(objects *[]*unstructured.Unstructured) error {
	if r.FenceAgent == nil {
		return nil
	}
	if err := r.FenceAgent.Validate(); err != nil {
		return err
	}
	for _, object := range *objects {
		if object.GetName() != "ovn-central" || object.GetKind() != "Deployment" && object.GetKind() != "StatefulSet" {
			continue
		}
		if err := fenceagent.Inject(object, *r.FenceAgent, r.Name, r.Namespace); err != nil {
			return err
		}
		serviceAccount := fenceagent.ServiceAccount(object)
		if serviceAccount == "" {
			return errors.New("ovn-central Pod template has no service account for fence-agent Lease access")
		}
		*objects = append(*objects,
			fenceagent.Role(r.Name, r.Namespace),
			fenceagent.RoleBinding(r.Name, r.Namespace, serviceAccount),
			fenceagent.Account(r.Name, r.Namespace),
		)
		return nil
	}
	return errors.New("fence-agent sidecar requires an ovn-central Deployment or StatefulSet")
}

func (r *HelmRenderer) fetch(ctx context.Context, spec operatorv1alpha1.ChartSpec) ([]byte, error) {
	hash, ok := strings.CutPrefix(spec.Digest, "sha256:")
	expected, err := hex.DecodeString(hash)
	if !ok || err != nil || len(expected) != sha256.Size {
		return nil, errors.New("spec.chart.digest must be the sha256 digest of the chart .tgz archive")
	}
	source, err := url.Parse(spec.Source)
	if err != nil || source.Host == "" || source.User != nil || source.RawQuery != "" || source.Fragment != "" {
		return nil, errors.New("spec.chart.source must be an archive or OCI URL without credentials, query or fragment")
	}
	if r.AllowedChartHosts != nil && !chartHostAllowed(source, r.AllowedChartHosts) {
		return nil, fmt.Errorf("spec.chart.source host %q is not in the operator chart host allowlist", source.Hostname())
	}
	if source.Scheme == registry.OCIScheme {
		return r.fetchOCI(ctx, source, expected)
	}
	if !allowedChartURL(source) {
		return nil, errors.New("chart source requires HTTPS (HTTP loopback is supported for local tests)")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.String(), nil)
	if err != nil {
		return nil, err
	}
	httpClient := r.chartHTTPClientWithRedirectPolicy()
	response, err := httpClient.Do(request)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New("chart download failed; verify source availability and TLS")
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch chart returned HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxArchiveSize+1))
	if err != nil {
		return nil, fmt.Errorf("read chart archive: %w", err)
	}
	if len(data) > maxArchiveSize {
		return nil, errors.New("chart archive exceeds 20 MiB")
	}
	actual := sha256.Sum256(data)
	if !bytes.Equal(expected, actual[:]) {
		return nil, errors.New("chart archive sha256 mismatch")
	}
	return data, nil
}

func chartHostAllowed(source *url.URL, allowed []string) bool {
	if source == nil || source.Hostname() == "" {
		return false
	}
	host := strings.ToLower(strings.TrimSpace(source.Host))
	hostname := strings.ToLower(strings.TrimSpace(source.Hostname()))
	for _, candidate := range allowed {
		candidate = strings.ToLower(strings.TrimSpace(candidate))
		if candidate == host || candidate == hostname {
			return true
		}
	}
	return false
}

func (r *HelmRenderer) fetchOCI(ctx context.Context, source *url.URL, expected []byte) ([]byte, error) {
	if source == nil || source.Host == "" || source.Path == "" || source.User != nil || source.RawQuery != "" || source.Fragment != "" {
		return nil, errors.New("OCI chart source must identify a repository reference without credentials, query or fragment")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, err := registry.NewClient(registry.ClientOptHTTPClient(r.chartHTTPClientWithRedirectPolicy()), registry.ClientOptEnableCache(false))
	if err != nil {
		return nil, fmt.Errorf("create OCI chart client: %w", err)
	}
	result, err := client.Pull(source.String(), registry.PullOptWithChart(true), registry.PullOptWithProv(false))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("pull OCI chart: %w", err)
	}
	if result == nil || result.Chart == nil || len(result.Chart.Data) == 0 {
		return nil, errors.New("OCI chart did not contain a chart layer")
	}
	actual := sha256.Sum256(result.Chart.Data)
	if !bytes.Equal(expected, actual[:]) || result.Chart.Digest != "sha256:"+hex.EncodeToString(actual[:]) {
		return nil, errors.New("OCI chart layer sha256 mismatch")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result.Chart.Data, nil
}

func (r *HelmRenderer) chartHTTPClient() *http.Client {
	httpClient := &http.Client{Timeout: 30 * time.Second}
	if r.HTTPClient != nil {
		copy := *r.HTTPClient
		httpClient = &copy
	}
	return httpClient
}

func (r *HelmRenderer) chartHTTPClientWithRedirectPolicy() *http.Client {
	httpClient := r.chartHTTPClient()
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 || !allowedChartURL(req.URL) || r.AllowedChartHosts != nil && !chartHostAllowed(req.URL, r.AllowedChartHosts) {
			return errors.New("chart redirect rejected")
		}
		return nil
	}
	return httpClient
}

func allowedChartURL(u *url.URL) bool {
	if u.Scheme == "https" {
		return true
	}
	address, err := netip.ParseAddr(u.Hostname())
	return u.Scheme == "http" && err == nil && address.IsLoopback()
}

func (r *HelmRenderer) bootstrapNodes(ctx context.Context, spec operatorv1alpha1.KubeOVNInstallationSpec, values map[string]any) error {
	// NODE_IPS is an initial bootstrap input, never evidence of live membership.
	if spec.Mode == operatorv1alpha1.InstallationModeNodeNetworking {
		return nil
	}
	if r.Reader == nil {
		return errors.New("node reader is required for central placement")
	}
	selector := spec.Central.Placement.NodeSelector
	if len(selector) == 0 {
		selector = map[string]string{"node-role.kubernetes.io/control-plane": ""}
	}
	if len(selector) != 1 {
		return errors.New("upstream MASTER_NODES_LABEL supports exactly one central placement label")
	}
	for key, value := range selector {
		values["MASTER_NODES_LABEL"] = key + "=" + value
	}
	nodes := new(corev1.NodeList)
	if err := r.Reader.List(ctx, nodes, client.MatchingLabels(selector)); err != nil {
		return fmt.Errorf("read central nodes: %w", err)
	}
	memberNodes := make(map[string]struct{}, len(spec.Central.MemberRequests))
	if spec.Central.DynamicMembership {
		for _, member := range spec.Central.MemberRequests {
			memberNodes[member.NodeName] = struct{}{}
		}
	}
	var addresses []string
	for _, node := range nodes.Items {
		if node.DeletionTimestamp != nil {
			continue
		}
		if _, reserved := memberNodes[node.Name]; reserved {
			continue
		}
		for _, address := range node.Status.Addresses {
			if address.Type == corev1.NodeInternalIP {
				if _, err := netip.ParseAddr(address.Address); err != nil {
					return errors.New("central node InternalIP is invalid")
				}
				addresses = append(addresses, address.Address)
				break
			}
		}
	}
	slices.Sort(addresses)
	addresses = slices.Compact(addresses)
	if len(addresses) == 0 {
		return errors.New("no central nodes with InternalIP match the placement selector")
	}
	// During a dynamic membership transition, explicitly prepared member
	// requests are rendered as independently managed workloads. Their nodes
	// are intentionally excluded above, so requiring the final desired count
	// here would reject every add or replacement before the lifecycle join can
	// run. The retained Chart members still must fit within the requested size;
	// the lifecycle handoff validates the complete final inventory.
	if spec.Central.Mode == operatorv1alpha1.CentralModeCluster {
		if !spec.Central.DynamicMembership || len(spec.Central.MemberRequests) == 0 {
			if len(addresses) != int(spec.Central.Replicas) {
				return fmt.Errorf("central placement selected %d addresses, expected %d", len(addresses), spec.Central.Replicas)
			}
		} else if len(addresses) > int(spec.Central.Replicas) {
			return fmt.Errorf("central placement selected %d retained addresses, exceeding desired replicas %d", len(addresses), spec.Central.Replicas)
		}
	}
	values["MASTER_NODES"] = strings.Join(addresses, ",")
	return nil
}

func (r *HelmRenderer) decode(manifest string) ([]*unstructured.Unstructured, error) {
	decoder := k8syaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
	var objects []*unstructured.Unstructured
	for {
		object := new(unstructured.Unstructured)
		err := decoder.Decode(object)
		if errors.Is(err, io.EOF) {
			return objects, nil
		}
		if err != nil {
			return nil, err
		}
		if len(object.Object) == 0 {
			continue
		}
		if hook := object.GetAnnotations()["helm.sh/hook"]; hook != "" {
			for event := range strings.SplitSeq(hook, ",") {
				switch strings.TrimSpace(event) {
				case "pre-upgrade", "post-upgrade", "pre-delete", "post-delete", "test", "test-success", "test-failure":
				default:
					return nil, fmt.Errorf("unsupported installation hook %q", hook)
				}
			}
			continue
		}
		gvk := object.GroupVersionKind()
		if gvk.Version == "" || gvk.Kind == "" || object.GetName() == "" || object.IsList() {
			return nil, errors.New("chart resources must have apiVersion, kind and a fixed name; Lists are unsupported")
		}
		mapping, err := r.Mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		if err != nil {
			return nil, fmt.Errorf("resolve chart resource %s: %w", gvk, err)
		}
		if mapping.Scope.Name() == meta.RESTScopeNameNamespace && object.GetNamespace() == "" {
			object.SetNamespace(r.Namespace)
		}
		if object.GetNamespace() != "" && object.GetNamespace() != r.Namespace {
			return nil, errors.New("chart resource targets another namespace")
		}
		objects = append(objects, object)
	}
}
