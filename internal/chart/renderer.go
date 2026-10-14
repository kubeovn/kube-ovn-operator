// Package chart translates the public installation API into the values
// contract consumed by the Kube-OVN Helm chart.
package chart

import (
	"context"
	"fmt"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/validation"
	"sigs.k8s.io/yaml"
)

type Renderer interface {
	Render(ctx context.Context, spec operatorv1alpha1.KubeOVNInstallationSpec) (Rendered, error)
}

type ValuesRenderer struct{}

type Rendered struct {
	Source     string
	Digest     string
	ValuesYAML []byte
}

type Values struct {
	InstallMode        string          `json:"installMode"`
	Image              ImageValues     `json:"image"`
	Central            CentralValues   `json:"ovn-central"`
	ExternalOVNCentral *ExternalValues `json:"externalOvnCentral,omitempty"`
	HCP                *HCPValues      `json:"hcp,omitempty"`
	Upgrade            UpgradeValues   `json:"upgrade"`
}

type ImageValues struct {
	Tag string `json:"tag"`
}

type CentralValues struct {
	Mode              string            `json:"mode"`
	Replicas          int32             `json:"replicas"`
	NodeSelector      map[string]string `json:"nodeSelector,omitempty"`
	TopologyKey       string            `json:"topologyKey,omitempty"`
	StorageClassName  string            `json:"storageClassName,omitempty"`
	StorageSize       string            `json:"storageSize,omitempty"`
	DynamicMembership bool              `json:"dynamicMembership"`
}

type ExternalValues struct {
	NBEndpoint string `json:"nbEndpoint"`
	SBEndpoint string `json:"sbEndpoint"`
	NBPort     int32  `json:"nbPort,omitempty"`
	SBPort     int32  `json:"sbPort,omitempty"`
}

type HCPValues struct {
	Enabled          bool   `json:"enabled"`
	Role             string `json:"role"`
	PairRef          string `json:"pairRef"`
	CentralNamespace string `json:"centralNamespace,omitempty"`
	NBEndpoint       string `json:"nbEndpoint"`
	SBEndpoint       string `json:"sbEndpoint"`
	TLSSecretName    string `json:"tlsSecretName,omitempty"`
}

type UpgradeValues struct {
	Strategy          string `json:"strategy,omitempty"`
	PauseOnDegraded   bool   `json:"pauseOnDegraded"`
	AllowRollback     bool   `json:"allowRollback"`
	BackupRequired    bool   `json:"backupRequired"`
	BackupDestination string `json:"backupDestination,omitempty"`
}

func (ValuesRenderer) Render(ctx context.Context, spec operatorv1alpha1.KubeOVNInstallationSpec) (Rendered, error) {
	if err := ctx.Err(); err != nil {
		return Rendered{}, err
	}
	if errs := validation.ValidateInstallationSpec(spec); len(errs) != 0 {
		return Rendered{}, fmt.Errorf("render chart values: %s", validation.JoinErrors(errs))
	}
	values := Values{
		InstallMode: string(spec.Mode),
		Image:       ImageValues{Tag: spec.Version},
		Central: CentralValues{
			Mode:              string(spec.Central.Mode),
			Replicas:          spec.Central.Replicas,
			NodeSelector:      spec.Central.Placement.NodeSelector,
			TopologyKey:       spec.Central.Placement.TopologyKey,
			StorageClassName:  spec.Central.Storage.ClassName,
			StorageSize:       spec.Central.Storage.Size,
			DynamicMembership: spec.Central.DynamicMembership,
		},
		Upgrade: UpgradeValues{
			Strategy:          spec.Upgrade.Strategy,
			PauseOnDegraded:   spec.Upgrade.PauseOnDegraded,
			AllowRollback:     spec.Upgrade.AllowRollback,
			BackupRequired:    spec.Upgrade.Backup.Required,
			BackupDestination: spec.Upgrade.Backup.Destination,
		},
	}
	if spec.DataPlane.ExternalOVNCentral.NBEndpoint != "" || spec.DataPlane.ExternalOVNCentral.SBEndpoint != "" {
		values.ExternalOVNCentral = &ExternalValues{
			NBEndpoint: spec.DataPlane.ExternalOVNCentral.NBEndpoint,
			SBEndpoint: spec.DataPlane.ExternalOVNCentral.SBEndpoint,
			NBPort:     spec.DataPlane.ExternalOVNCentral.NBPort,
			SBPort:     spec.DataPlane.ExternalOVNCentral.SBPort,
		}
	}
	if spec.HCP.Enabled {
		tlsSecretName := ""
		if spec.HCP.TLSSecretRef != nil {
			tlsSecretName = spec.HCP.TLSSecretRef.Name
		}
		values.HCP = &HCPValues{
			Enabled:          true,
			Role:             string(spec.HCP.Role),
			PairRef:          spec.HCP.PairRef,
			CentralNamespace: spec.HCP.CentralNamespace,
			NBEndpoint:       spec.HCP.NBEndpoint,
			SBEndpoint:       spec.HCP.SBEndpoint,
			TLSSecretName:    tlsSecretName,
		}
	}
	valuesYAML, err := yaml.Marshal(values)
	if err != nil {
		return Rendered{}, err
	}
	return Rendered{Source: spec.Chart.Source, Digest: spec.Chart.Digest, ValuesYAML: valuesYAML}, nil
}

var _ Renderer = ValuesRenderer{}
