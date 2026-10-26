// Package chart renders the existing Kube-OVN Helm chart.
package chart

import (
	"context"
	"fmt"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/hcp"
	"github.com/kubeovn/kube-ovn-operator/internal/validation"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/yaml"
)

type Renderer interface {
	Render(context.Context, operatorv1alpha1.KubeOVNInstallationSpec) (Rendered, error)
}

// ValuesRenderer produces a plan without fetching or rendering a chart.
type ValuesRenderer struct{}

type Rendered struct {
	Source     string
	Digest     string
	ValuesYAML []byte
	Objects    []*unstructured.Unstructured
}

func (ValuesRenderer) Render(ctx context.Context, spec operatorv1alpha1.KubeOVNInstallationSpec) (Rendered, error) {
	if err := ctx.Err(); err != nil {
		return Rendered{}, err
	}
	values, err := installationValues(spec)
	if err != nil {
		return Rendered{}, err
	}
	data, err := yaml.Marshal(values)
	return Rendered{Source: spec.Chart.Source, Digest: spec.Chart.Digest, ValuesYAML: data}, err
}

// Operator policy (backup, rollback and membership) belongs to the reconciler,
// not invented chart values that upstream templates would silently ignore.
func installationValues(spec operatorv1alpha1.KubeOVNInstallationSpec) (map[string]any, error) {
	if errs := validation.ValidateInstallationSpec(spec); len(errs) != 0 {
		return nil, fmt.Errorf("render chart values: %s", validation.JoinErrors(errs))
	}
	mode := "cluster"
	if spec.Central.Mode == operatorv1alpha1.CentralModeSingle {
		mode = "single"
	}
	central := map[string]any{
		"storage": map[string]any{"size": spec.Central.Storage.Size, "storageClassName": spec.Central.Storage.ClassName},
	}
	if len(spec.Central.Placement.NodeSelector) != 0 {
		central["nodeSelector"] = spec.Central.Placement.NodeSelector
	}
	values := map[string]any{
		"installMode": string(spec.Mode), "OVN_CENTRAL_MODE": mode,
		"global": map[string]any{"images": map[string]any{
			"kubeovn": map[string]any{"tag": spec.Version}, "natgateway": map[string]any{"tag": spec.Version},
		}},
		"ovn-central": central,
	}
	if spec.Mode == operatorv1alpha1.InstallationModeDataPlaneOnly {
		endpoint := spec.DataPlane.ExternalOVNCentral
		if spec.HCP.Enabled {
			northbound, err := hcp.ParseEndpoint(spec.HCP.NBEndpoint)
			if err != nil {
				return nil, fmt.Errorf("parse HCP northbound endpoint: %w", err)
			}
			southbound, err := hcp.ParseEndpoint(spec.HCP.SBEndpoint)
			if err != nil {
				return nil, fmt.Errorf("parse HCP southbound endpoint: %w", err)
			}
			if northbound.Protocol != southbound.Protocol {
				return nil, fmt.Errorf("HCP northbound and southbound endpoints must use the same protocol")
			}
			endpoint.NBEndpoint, endpoint.NBPort = northbound.Host, northbound.Port
			endpoint.SBEndpoint, endpoint.SBPort = southbound.Host, southbound.Port
			values["networking"] = map[string]any{"ENABLE_SSL": northbound.Protocol == "ssl"}
		}
		external := map[string]any{"nbEndpoint": endpoint.NBEndpoint, "sbEndpoint": endpoint.SBEndpoint}
		if endpoint.NBPort != 0 {
			external["nbPort"] = endpoint.NBPort
		}
		if endpoint.SBPort != 0 {
			external["sbPort"] = endpoint.SBPort
		}
		values["externalOvnCentral"] = external
	}
	if spec.DataPlane.OVSUpdateStrategy != "" {
		values["ovs-ovn"] = map[string]any{"updateStrategy": map[string]any{"type": spec.DataPlane.OVSUpdateStrategy}}
	}
	return values, nil
}
