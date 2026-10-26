package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
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
	Version      string            `json:"version"`
	Chart        ChartSpec         `json:"chart"`
	Mode         InstallationMode  `json:"mode"`
	Central      CentralSpec       `json:"central"`
	DataPlane    DataPlaneSpec     `json:"dataPlane,omitempty"`
	HCP          HCPConfig         `json:"hcp,omitempty"`
	Upgrade      UpgradeSpec       `json:"upgrade,omitempty"`
	Adoption     AdoptionSpec      `json:"adoption,omitempty"`
	NetworkProbe *NetworkProbeSpec `json:"networkProbe,omitempty"`
}

// NetworkProbeSpec enables ordinary pod-network canaries using this operator's
// network-probe binary. Use an immutable image digest in production.
type NetworkProbeSpec struct {
	Image string `json:"image"`
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
	// MemberRequests pin the Kubernetes identities and listener addresses for
	// members that the lifecycle executor may add one at a time. The operator
	// never discovers a replacement from replica count alone.
	MemberRequests []CentralMemberSpec `json:"memberRequests,omitempty"`
}

// CentralMemberSpec is an explicit member slot. Addition and replacement
// entries must identify a pre-provisioned claim, volume, node, image and all
// listener addresses; the operator records their live UIDs before creating a
// preparation Job or StatefulSet. When replaceEndpoint is set, the slot is a
// same-size replacement for the pinned old endpoint. A retirement-only entry
// may set retireEndpoint to select one retained Chart Deployment Pod for a
// one-member scale-in transition.
type CentralMemberSpec struct {
	Name            string `json:"name"`
	ReplaceEndpoint string `json:"replaceEndpoint,omitempty"`
	RetireEndpoint  string `json:"retireEndpoint,omitempty"`
	// RemovalApprovalRef authorizes a permanent-loss kick when the retained
	// endpoint has disappeared or been replaced. Without replaceEndpoint, the
	// reference opts into automatic same-size replacement only when exactly one
	// retained endpoint is proven lost. The operator never creates this approval
	// and only forwards its immutable name/UID to the joint lifecycle executor.
	RemovalApprovalRef      *MemberRemovalApprovalReference `json:"removalApprovalRef,omitempty"`
	NodeName                string                          `json:"nodeName,omitempty"`
	ClaimName               string                          `json:"claimName,omitempty"`
	VolumeName              string                          `json:"volumeName,omitempty"`
	Image                   string                          `json:"image,omitempty"`
	NorthboundAddress       string                          `json:"northboundAddress,omitempty"`
	SouthboundAddress       string                          `json:"southboundAddress,omitempty"`
	NorthboundClientAddress string                          `json:"northboundClientAddress,omitempty"`
	SouthboundClientAddress string                          `json:"southboundClientAddress,omitempty"`
	// TLSSecretRef names an existing Secret containing either tls.key/tls.crt/ca.crt
	// or the native key/cert/cacert keys. The lifecycle executor records its
	// identity before creating a workload and never copies Secret data.
	TLSSecretRef *LocalObjectReference `json:"tlsSecretRef,omitempty"`
}

// MemberRemovalApprovalReference identifies a pre-created
// KubeOVNMemberRemovalApproval. Both fields are required so a name reuse
// cannot redirect a replacement operation to a different approval record.
type MemberRemovalApprovalReference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
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
	// RollbackApprovalRef is required when a rollback observes unavailable
	// quorum and must use an explicit human-created recovery approval.
	RollbackApprovalRef *RecoveryApprovalReference `json:"rollbackApprovalRef,omitempty"`
}

type BackupSpec struct {
	Required    bool   `json:"required,omitempty"`
	Destination string `json:"destination,omitempty"`
	// Schedule is an interval duration such as "6h" or "24h". A scheduled
	// capture only records a verified NB/SB pair and never authorizes Chart
	// writes.
	Schedule  string              `json:"schedule,omitempty"`
	Retention BackupRetentionSpec `json:"retention,omitempty"`
}

// BackupRetentionSpec enables deletion of complete operator-owned pairs. A
// zero KeepLast and empty MaxAge disable cleanup; either field may be used.
type BackupRetentionSpec struct {
	KeepLast int32  `json:"keepLast,omitzero"`
	MaxAge   string `json:"maxAge,omitempty"`
}

type AdoptionSpec struct {
	Enabled  bool              `json:"enabled,omitempty"`
	Selector map[string]string `json:"selector,omitempty"`
}

// ApplyCheckpointStatus pins the inputs before the first workload write.
// A lifecycle executor must authorize changes after this checkpoint exists.
type ApplyCheckpointStatus struct {
	SpecHash string `json:"specHash"`
	// BaseSpecHash is the hash of the installation spec with lifecycle member
	// requests omitted. It lets a later explicit retirement or replacement
	// change only that lifecycle input and the replica count without weakening
	// the checkpoint for version, chart, placement, storage or network changes.
	BaseSpecHash string `json:"baseSpecHash,omitempty"`
	ManifestHash string `json:"manifestHash"`
	Version      string `json:"version"`
}

type KubeOVNInstallationStatus struct {
	ApplyCheckpoint    *ApplyCheckpointStatus `json:"applyCheckpoint,omitempty"`
	ObservedVersion    string                 `json:"observedVersion,omitempty"`
	Phase              Phase                  `json:"phase,omitempty"`
	Conditions         []metav1.Condition     `json:"conditions,omitempty"`
	Topology           TopologyStatus         `json:"topology,omitempty"`
	Upgrade            UpgradeStatus          `json:"upgrade,omitempty"`
	HCP                HCPStatus              `json:"hcp,omitempty"`
	LastBackup         *BackupStatus          `json:"lastBackup,omitempty"`
	BackupRetention    *BackupRetentionStatus `json:"backupRetention,omitempty"`
	ObservedGeneration int64                  `json:"observedGeneration,omitempty"`
	Network            *NetworkStatus         `json:"network,omitempty"`
}

type NetworkStatus struct {
	ObservedNodes    int32       `json:"observedNodes"`
	AddressFamilies  []string    `json:"addressFamilies"`
	SuccessfulProbes int32       `json:"successfulProbes"`
	LastObserved     metav1.Time `json:"lastObserved"`
}

// CentralTopologyReference pins retained identity evidence, not current health.
type CentralTopologyReference struct {
	Name   string    `json:"name"`
	UID    types.UID `json:"uid"`
	Digest string    `json:"digest"`
}

// CentralOperationReference retains the admitted operation and its original
// topology across restarts. Completion does not authorize replaying old charts.
type CentralOperationReference struct {
	JournalName      string                    `json:"journalName"`
	JournalUID       types.UID                 `json:"journalUID"`
	RequestName      string                    `json:"requestName"`
	RequestUID       types.UID                 `json:"requestUID"`
	Inventory        CentralTopologyReference  `json:"inventory"`
	HandoffInventory *CentralTopologyReference `json:"handoffInventory,omitempty"`
	CompletionDigest string                    `json:"completionDigest,omitempty"`
	HandoffApplied   bool                      `json:"handoffApplied"`
}

type TopologyStatus struct {
	Inventory              *CentralTopologyReference  `json:"inventory,omitempty"`
	Operation              *CentralOperationReference `json:"operation,omitempty"`
	DesiredCentralReplicas int32                      `json:"desiredCentralReplicas,omitempty"`
	ReadyCentralReplicas   int32                      `json:"readyCentralReplicas,omitempty"`
	Members                []CentralMemberStatus      `json:"members,omitempty"`
	Databases              []CentralDatabaseStatus    `json:"databases,omitempty"`
	NorthdProcesses        []NorthdProcessStatus      `json:"northdProcesses,omitempty"`
	Nodes                  []DataPlaneNodeStatus      `json:"nodes,omitempty"`
}

type NorthdProcessStatus struct {
	Pod          string      `json:"pod"`
	State        string      `json:"state"`
	NBConnected  bool        `json:"nbConnected,omitzero"`
	SBConnected  bool        `json:"sbConnected,omitzero"`
	LastObserved metav1.Time `json:"lastObserved,omitzero"`
}

type DataPlaneNodeStatus struct {
	Node                   string      `json:"node"`
	NodeUID                string      `json:"nodeUID"`
	Pod                    string      `json:"pod"`
	ChassisID              string      `json:"chassisID"`
	SBConnected            bool        `json:"sbConnected,omitzero"`
	IntegrationBridgeReady bool        `json:"integrationBridgeReady,omitzero"`
	LastObserved           metav1.Time `json:"lastObserved,omitzero"`
}

type CentralMemberStatus struct {
	Database string `json:"database"`
	ServerID string `json:"serverID"`
	Pod      string `json:"pod,omitempty"`
	// Node is the last observed Kubernetes node hosting the central Pod. It is
	// retained in status so a node label change, cordon or deletion still
	// wakes reconciliation after the Node no longer matches placement.
	Node         string      `json:"node,omitempty"`
	Address      string      `json:"address,omitempty"`
	State        string      `json:"state,omitempty"`
	Leader       bool        `json:"leader,omitzero"`
	AppliedIndex int64       `json:"appliedIndex,omitzero"`
	LastObserved metav1.Time `json:"lastObserved,omitempty"`
}

// CentralDatabaseStatus summarizes the most recent read-only observation.
// It is health evidence, not authorization for a membership operation.
type CentralDatabaseStatus struct {
	Database         string `json:"database"`
	ClusterID        string `json:"clusterID,omitempty"`
	LeaderServerID   string `json:"leaderServerID,omitempty"`
	ObservedMembers  int32  `json:"observedMembers,omitzero"`
	ConnectedMembers int32  `json:"connectedMembers,omitzero"`
}

// UpgradeCheckpointPhase records the durable cross-database upgrade gate.
// The checkpoint is separate from LastBackup so scheduled or lifecycle
// captures cannot replace the pair that authorizes this target version.
type UpgradeCheckpointPhase string

const (
	UpgradeCheckpointCapturing UpgradeCheckpointPhase = "Capturing"
	UpgradeCheckpointVerified  UpgradeCheckpointPhase = "Verified"
	UpgradeCheckpointApplying  UpgradeCheckpointPhase = "Applying"
	UpgradeCheckpointApplied   UpgradeCheckpointPhase = "Applied"
	UpgradeCheckpointFailed    UpgradeCheckpointPhase = "Failed"
)

// UpgradeCheckpointStatus is the durable, installation-bound NB/SB pair used
// by one version-only upgrade. DatabaseRefs are immutable store references;
// the pair is usable only after both databases have been verified against the
// same source version, target version, destination and retained topology.
type UpgradeCheckpointStatus struct {
	ID             string                 `json:"id,omitempty"`
	DatabaseRefs   map[string]string      `json:"databaseRefs,omitempty"`
	SourceVersion  string                 `json:"sourceVersion,omitempty"`
	TargetVersion  string                 `json:"targetVersion,omitempty"`
	Destination    string                 `json:"destination,omitempty"`
	TopologyDigest string                 `json:"topologyDigest,omitempty"`
	CapturedAt     metav1.Time            `json:"capturedAt,omitzero"`
	AppliedAt      metav1.Time            `json:"appliedAt,omitzero"`
	Phase          UpgradeCheckpointPhase `json:"phase,omitempty"`
	Reason         string                 `json:"reason,omitempty"`
}

type UpgradeStatus struct {
	PlanID               string                   `json:"planID,omitempty"`
	Step                 string                   `json:"step,omitempty"`
	CanRollback          bool                     `json:"canRollback,omitempty"`
	BackupID             string                   `json:"backupID,omitempty"`
	TargetVersion        string                   `json:"targetVersion,omitempty"`
	CheckpointAuthorized bool                     `json:"checkpointAuthorized,omitzero"`
	Checkpoint           *UpgradeCheckpointStatus `json:"checkpoint,omitempty"`
	Rollback             *RollbackStatus          `json:"rollback,omitempty"`
}

// RollbackPhase records the durable progress of a version-aware rollback.
// Blocked is used while the target is lower than the observed version but one
// of the restore, workload or fencing capabilities is unavailable.
type RollbackPhase string

const (
	RollbackPhaseBlocked   RollbackPhase = "Blocked"
	RollbackPhaseQuiescing RollbackPhase = "Quiescing"
	RollbackPhaseRestoring RollbackPhase = "Restoring"
	RollbackPhaseWorkloads RollbackPhase = "Workloads"
	RollbackPhaseVerifying RollbackPhase = "Verifying"
	RollbackPhaseSucceeded RollbackPhase = "Succeeded"
	RollbackPhaseFailed    RollbackPhase = "Failed"
)

// RollbackStatus is an auditable, installation-bound summary of rollback
// admission and execution. RPOSeconds is the age of the verified pair at
// admission; RTOSeconds is measured from StartedAt after execution completes.
type RollbackStatus struct {
	Phase          RollbackPhase              `json:"phase,omitempty"`
	SourceVersion  string                     `json:"sourceVersion,omitempty"`
	TargetVersion  string                     `json:"targetVersion,omitempty"`
	BackupID       string                     `json:"backupID,omitempty"`
	TopologyDigest string                     `json:"topologyDigest,omitempty"`
	ChartDigest    string                     `json:"chartDigest,omitempty"`
	ApprovalRef    *RecoveryApprovalReference `json:"approvalRef,omitempty"`
	StartedAt      metav1.Time                `json:"startedAt,omitzero"`
	CompletedAt    metav1.Time                `json:"completedAt,omitzero"`
	RPOSeconds     int64                      `json:"rpoSeconds,omitzero"`
	RTOSeconds     int64                      `json:"rtoSeconds,omitzero"`
	Reason         string                     `json:"reason,omitempty"`
}

type HCPStatus struct {
	Paired              bool   `json:"paired,omitempty"`
	PeerObservedVersion string `json:"peerObservedVersion,omitempty"`
	EndpointReady       bool   `json:"endpointReady,omitempty"`
	// TLSSecretUID and TLSSecretResourceVersion identify the exact Secret
	// generation used by the last accepted peer observation. They contain no
	// credential material and force a fresh HCP gate after Secret replacement
	// or rotation.
	TLSSecretUID             types.UID `json:"tlsSecretUID,omitempty"`
	TLSSecretResourceVersion string    `json:"tlsSecretResourceVersion,omitempty"`
}

// BackupStatus records a verified pair captured by the durable lifecycle
// executor. DatabaseRefs are immutable store references, not credentials.
type BackupStatus struct {
	ID             string            `json:"id,omitempty"`
	DatabaseRefs   map[string]string `json:"databaseRefs,omitempty"`
	Destination    string            `json:"destination,omitempty"`
	Version        string            `json:"version,omitempty"`
	TopologyDigest string            `json:"topologyDigest,omitempty"`
	CompletedAt    metav1.Time       `json:"completedAt,omitempty"`
}

// BackupRetentionStatus records the last cleanup attempt. A failed attempt
// remains visible so the next reconcile can retry without claiming success.
type BackupRetentionStatus struct {
	LastAttempt metav1.Time `json:"lastAttempt,omitzero"`
	LastSuccess metav1.Time `json:"lastSuccess,omitzero"`
	Scanned     int32       `json:"scanned,omitzero"`
	Protected   int32       `json:"protected,omitzero"`
	Deleted     int32       `json:"deleted,omitzero"`
	Reason      string      `json:"reason,omitempty"`
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
	// TargetMemberRef is required for rejoin. It names one explicit
	// Central.MemberRequests entry and prevents recovery from inferring a
	// target from replica count or whichever Pod happens to be available.
	TargetMemberRef string `json:"targetMemberRef,omitempty"`
	BackupRef       string `json:"backupRef,omitempty"`
	// ApprovalRef may authorize destructive recovery only after the executor
	// proves that live quorum is unavailable. It is never inferred from
	// approvedBy.
	ApprovalRef *RecoveryApprovalReference `json:"approvalRef,omitempty"`
	ApprovedBy  string                     `json:"approvedBy"`
	ExpiresAt   metav1.Time                `json:"expiresAt"`
}

type RecoveryPhase string

const (
	RecoveryPhasePending           RecoveryPhase = "Pending"
	RecoveryPhaseAwaitingExecution RecoveryPhase = "AwaitingExecution"
	RecoveryPhaseExecuting         RecoveryPhase = "Executing"
	RecoveryPhaseSucceeded         RecoveryPhase = "Succeeded"
	RecoveryPhaseFailed            RecoveryPhase = "Failed"
	RecoveryPhaseExpired           RecoveryPhase = "Expired"
)

// RecoveryPlanStatus pins the installation and request identities validated
// before an external recovery executor may be called. The digest covers the
// complete request, including the self-reported approver; that field is never
// treated as authority by the operator.
type RecoveryPlanStatus struct {
	Digest                 string                     `json:"digest"`
	InstallationUID        types.UID                  `json:"installationUID"`
	InstallationGeneration int64                      `json:"installationGeneration"`
	RequestGeneration      int64                      `json:"requestGeneration"`
	Mode                   RecoveryMode               `json:"mode"`
	SourceVersion          string                     `json:"sourceVersion,omitempty"`
	TargetVersion          string                     `json:"targetVersion,omitempty"`
	ChartDigest            string                     `json:"chartDigest,omitempty"`
	TargetMemberRef        string                     `json:"targetMemberRef,omitempty"`
	TargetMemberDigest     string                     `json:"targetMemberDigest,omitempty"`
	TopologyDigest         string                     `json:"topologyDigest,omitempty"`
	BackupRef              string                     `json:"backupRef,omitempty"`
	DatabaseRefs           map[string]string          `json:"databaseRefs,omitempty"`
	ApprovalRef            *RecoveryApprovalReference `json:"approvalRef,omitempty"`
	ExpiresAt              metav1.Time                `json:"expiresAt"`
}

type KubeOVNRecoveryRequestStatus struct {
	Phase       RecoveryPhase            `json:"phase,omitempty"`
	Plan        *RecoveryPlanStatus      `json:"plan,omitempty"`
	Execution   *RecoveryExecutionStatus `json:"execution,omitempty"`
	Conditions  []metav1.Condition       `json:"conditions,omitempty"`
	CompletedAt *metav1.Time             `json:"completedAt,omitempty"`
}

// RecoveryExecutionStatus records restart-safe recovery execution. Destructive
// modes persist their phase and timing; rejoin persists the admitted lifecycle
// operation reference so a retry resumes the same journal after a restart.
type RecoveryExecutionStatus struct {
	Phase       RollbackPhase              `json:"phase,omitempty"`
	StartedAt   metav1.Time                `json:"startedAt,omitzero"`
	CompletedAt metav1.Time                `json:"completedAt,omitzero"`
	RPOSeconds  int64                      `json:"rpoSeconds,omitzero"`
	RTOSeconds  int64                      `json:"rtoSeconds,omitzero"`
	Reason      string                     `json:"reason,omitempty"`
	Operation   *CentralOperationReference `json:"operation,omitempty"`
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
