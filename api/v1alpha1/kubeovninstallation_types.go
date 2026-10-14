package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type InstallationMode string

const (
	InstallationModeFull             InstallationMode = "full"
	InstallationModeControlPlaneOnly InstallationMode = "controlPlaneOnly"
	InstallationModeDataPlaneOnly    InstallationMode = "dataPlaneOnly"
)

type CentralMode string

const (
	CentralModeRaft   CentralMode = "raft"
	CentralModeSingle CentralMode = "single"
)

type HCPRole string

const (
	HCPRoleNone       HCPRole = "none"
	HCPRoleManagement HCPRole = "management"
	HCPRoleDataPlane  HCPRole = "dataPlane"
)

type Phase string

const (
	PhasePending       Phase = "Pending"
	PhaseInstalling    Phase = "Installing"
	PhaseAdopting      Phase = "Adopting"
	PhaseReconfiguring Phase = "Reconfiguring"
	PhaseUpgrading     Phase = "Upgrading"
	PhaseRecovering    Phase = "Recovering"
	PhaseDegraded      Phase = "Degraded"
	PhaseReady         Phase = "Ready"
)

type KubeOVNInstallationSpec struct {
	Version   string           `json:"version"`
	Chart     ChartSpec        `json:"chart"`
	Mode      InstallationMode `json:"mode"`
	Central   CentralSpec      `json:"central"`
	DataPlane DataPlaneSpec    `json:"dataPlane,omitempty"`
	HCP       HCPConfig        `json:"hcp,omitempty"`
	Upgrade   UpgradeSpec      `json:"upgrade,omitempty"`
	Adoption  AdoptionSpec     `json:"adoption,omitempty"`
}

type ChartSpec struct {
	Source string `json:"source,omitempty"`
	Digest string `json:"digest,omitempty"`
}

type CentralSpec struct {
	Mode              CentralMode   `json:"mode"`
	Replicas          int32         `json:"replicas"`
	Placement         PlacementSpec `json:"placement,omitempty"`
	Storage           StorageSpec   `json:"storage,omitempty"`
	DynamicMembership bool          `json:"dynamicMembership,omitempty"`
}

type PlacementSpec struct {
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`
	TopologyKey  string            `json:"topologyKey,omitempty"`
}

type StorageSpec struct {
	ClassName string `json:"className,omitempty"`
	Size      string `json:"size,omitempty"`
}

type DataPlaneSpec struct {
	NodeSelector       map[string]string `json:"nodeSelector,omitempty"`
	OVSUpdateStrategy  string            `json:"ovsUpdateStrategy,omitempty"`
	ExternalOVNCentral EndpointSpec      `json:"externalOvnCentral,omitempty"`
}

type EndpointSpec struct {
	NBEndpoint string `json:"nbEndpoint,omitempty"`
	SBEndpoint string `json:"sbEndpoint,omitempty"`
	NBPort     int32  `json:"nbPort,omitempty"`
	SBPort     int32  `json:"sbPort,omitempty"`
}

type HCPConfig struct {
	Enabled          bool                  `json:"enabled,omitempty"`
	Role             HCPRole               `json:"role,omitempty"`
	PairRef          string                `json:"pairRef,omitempty"`
	CentralNamespace string                `json:"centralNamespace,omitempty"`
	NBEndpoint       string                `json:"nbEndpoint,omitempty"`
	SBEndpoint       string                `json:"sbEndpoint,omitempty"`
	TLSSecretRef     *LocalObjectReference `json:"tlsSecretRef,omitempty"`
}

type LocalObjectReference struct {
	Name string `json:"name"`
}

type UpgradeSpec struct {
	Strategy        string     `json:"strategy,omitempty"`
	PauseOnDegraded bool       `json:"pauseOnDegraded,omitempty"`
	Backup          BackupSpec `json:"backup,omitempty"`
	AllowRollback   bool       `json:"allowRollback,omitempty"`
}

type BackupSpec struct {
	Required    bool   `json:"required,omitempty"`
	Destination string `json:"destination,omitempty"`
}

type AdoptionSpec struct {
	Enabled  bool              `json:"enabled,omitempty"`
	Selector map[string]string `json:"selector,omitempty"`
}

type KubeOVNInstallationStatus struct {
	ObservedVersion    string             `json:"observedVersion,omitempty"`
	Phase              Phase              `json:"phase,omitempty"`
	Conditions         []metav1.Condition `json:"conditions,omitempty"`
	Topology           TopologyStatus     `json:"topology,omitempty"`
	Upgrade            UpgradeStatus      `json:"upgrade,omitempty"`
	HCP                HCPStatus          `json:"hcp,omitempty"`
	LastBackup         *BackupStatus      `json:"lastBackup,omitempty"`
	ObservedGeneration int64              `json:"observedGeneration,omitempty"`
}

type TopologyStatus struct {
	DesiredCentralReplicas int32                 `json:"desiredCentralReplicas,omitempty"`
	ReadyCentralReplicas   int32                 `json:"readyCentralReplicas,omitempty"`
	Members                []CentralMemberStatus `json:"members,omitempty"`
}

type CentralMemberStatus struct {
	Database     string      `json:"database"`
	ServerID     string      `json:"serverID"`
	Pod          string      `json:"pod,omitempty"`
	Address      string      `json:"address,omitempty"`
	State        string      `json:"state,omitempty"`
	LastObserved metav1.Time `json:"lastObserved,omitempty"`
}

type UpgradeStatus struct {
	PlanID      string `json:"planID,omitempty"`
	Step        string `json:"step,omitempty"`
	CanRollback bool   `json:"canRollback,omitempty"`
}

type HCPStatus struct {
	Paired              bool   `json:"paired,omitempty"`
	PeerObservedVersion string `json:"peerObservedVersion,omitempty"`
	EndpointReady       bool   `json:"endpointReady,omitempty"`
}

type BackupStatus struct {
	ID          string      `json:"id,omitempty"`
	CompletedAt metav1.Time `json:"completedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=koi
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".status.observedVersion"
type KubeOVNInstallation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KubeOVNInstallationSpec   `json:"spec,omitempty"`
	Status            KubeOVNInstallationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KubeOVNInstallationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KubeOVNInstallation `json:"items"`
}

type RecoveryMode string

const (
	RecoveryModeRejoin         RecoveryMode = "rejoin"
	RecoveryModeReconvert      RecoveryMode = "reconvert"
	RecoveryModeFreshBootstrap RecoveryMode = "freshBootstrap"
)

type KubeOVNRecoveryRequestSpec struct {
	InstallationRef string       `json:"installationRef"`
	Reason          string       `json:"reason"`
	Mode            RecoveryMode `json:"mode"`
	BackupRef       string       `json:"backupRef,omitempty"`
	ApprovedBy      string       `json:"approvedBy"`
	ExpiresAt       metav1.Time  `json:"expiresAt"`
}

type KubeOVNRecoveryRequestStatus struct {
	Phase       string             `json:"phase,omitempty"`
	Conditions  []metav1.Condition `json:"conditions,omitempty"`
	CompletedAt *metav1.Time       `json:"completedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type KubeOVNRecoveryRequest struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              KubeOVNRecoveryRequestSpec   `json:"spec,omitempty"`
	Status            KubeOVNRecoveryRequestStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KubeOVNRecoveryRequestList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KubeOVNRecoveryRequest `json:"items"`
}
