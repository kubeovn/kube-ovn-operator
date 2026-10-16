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
	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
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
}

func (r *HelmRenderer) Render(ctx context.Context, spec operatorv1alpha1.KubeOVNInstallationSpec) (Rendered, error) {
	values, err := installationValues(spec)
	if err != nil {
		return Rendered{}, err
	}
	if r.Name == "" || r.Namespace == "" || r.Mapper == nil {
		return Rendered{}, errors.New("chart release identity and REST mapper are required")
	}
	// HCP and dynamic membership need execution and observation across lifecycle
	// steps. Adoption is handled by the controller after rendering and before
	// the regular workload apply.
	if spec.HCP.Enabled || spec.Central.DynamicMembership {
		return Rendered{}, errors.New("HCP and dynamic membership require lifecycle executors")
	}
	if spec.Central.Placement.TopologyKey != "" && spec.Central.Placement.TopologyKey != "kubernetes.io/hostname" {
		return Rendered{}, errors.New("the upstream chart supports central anti-affinity by kubernetes.io/hostname only")
	}
	if len(spec.DataPlane.NodeSelector) != 0 {
		return Rendered{}, errors.New("data-plane node selection is not exposed by the upstream chart")
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
	return Rendered{Source: spec.Chart.Source, Digest: spec.Chart.Digest, ValuesYAML: valuesYAML, Objects: objects}, ctx.Err()
}

func (r *HelmRenderer) fetch(ctx context.Context, spec operatorv1alpha1.ChartSpec) ([]byte, error) {
	hash, ok := strings.CutPrefix(spec.Digest, "sha256:")
	expected, err := hex.DecodeString(hash)
	if !ok || err != nil || len(expected) != sha256.Size {
		return nil, errors.New("spec.chart.digest must be the sha256 digest of the chart .tgz archive")
	}
	source, err := url.Parse(spec.Source)
	if err != nil || source.Host == "" || source.User != nil || source.RawQuery != "" || source.Fragment != "" {
		return nil, errors.New("spec.chart.source must be an archive URL without credentials, query or fragment")
	}
	if !allowedChartURL(source) {
		return nil, errors.New("chart source requires HTTPS (HTTP loopback is supported for local tests)")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, source.String(), nil)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{Timeout: 30 * time.Second}
	if r.HTTPClient != nil {
		copy := *r.HTTPClient
		httpClient = &copy
	}
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 || !allowedChartURL(req.URL) {
			return errors.New("chart redirect rejected")
		}
		return nil
	}
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

func allowedChartURL(u *url.URL) bool {
	if u.Scheme == "https" {
		return true
	}
	address, err := netip.ParseAddr(u.Hostname())
	return u.Scheme == "http" && err == nil && address.IsLoopback()
}

func (r *HelmRenderer) bootstrapNodes(ctx context.Context, spec operatorv1alpha1.KubeOVNInstallationSpec, values map[string]any) error {
	// NODE_IPS is an initial bootstrap input, never evidence of live membership.
	if spec.Mode == operatorv1alpha1.InstallationModeDataPlaneOnly {
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
	var addresses []string
	for _, node := range nodes.Items {
		if node.DeletionTimestamp != nil {
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
	if spec.Central.Mode == operatorv1alpha1.CentralModeRaft && len(addresses) != int(spec.Central.Replicas) {
		return fmt.Errorf("central placement selected %d addresses, expected %d", len(addresses), spec.Central.Replicas)
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
