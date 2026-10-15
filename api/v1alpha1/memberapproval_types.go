package v1alpha1

import (
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

type InstallationReference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

type MemberRemovalTarget struct {
	Database  string `json:"database"`
	ClusterID string `json:"clusterID"`
	ServerID  string `json:"serverID"`
	Address   string `json:"address"`
}

// MemberRemovalDecision is immutable. Permission to create an approval is the
// authority to confirm permanent loss; no self-reported username grants it.
// Kubernetes API audit records identify the authenticated creating principal.
type MemberRemovalDecision struct {
	InstallationRef InstallationReference `json:"installationRef"`
	OperationID     string                `json:"operationID"`
	Targets         []MemberRemovalTarget `json:"targets"`
	Reason          string                `json:"reason"`
	ExpiresAt       metav1.Time           `json:"expiresAt"`
}

type MemberRemovalApprovalSpec struct {
	Decision MemberRemovalDecision `json:"decision"`
	// Revocation is one-way and prevents new dispatches. It cannot cancel an
	// already accepted OVSDB operation; completion still records observation.
	Revoked bool `json:"revoked"`
}

type MemberRemovalObservation struct {
	Database    string      `json:"database"`
	ClusterID   string      `json:"clusterID"`
	Term        int64       `json:"term"`
	CommitIndex int64       `json:"commitIndex"`
	ObservedAt  metav1.Time `json:"observedAt"`
}

type MemberRemovalApprovalStatus struct {
	Phase          string                     `json:"phase,omitempty"`
	OperationID    string                     `json:"operationID,omitempty"`
	DecisionDigest string                     `json:"decisionDigest,omitempty"`
	ReservedAt     *metav1.Time               `json:"reservedAt,omitempty"`
	CompletedAt    *metav1.Time               `json:"completedAt,omitempty"`
	Observations   []MemberRemovalObservation `json:"observations,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// KubeOVNMemberRemovalApproval authorizes one operation on explicit lost member
// identities. It has no installation owner reference so its audit survives the
// installation. The operator may update status but must not create approvals.
type KubeOVNMemberRemovalApproval struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              MemberRemovalApprovalSpec   `json:"spec"`
	Status            MemberRemovalApprovalStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KubeOVNMemberRemovalApprovalList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KubeOVNMemberRemovalApproval `json:"items"`
}

func (in *KubeOVNMemberRemovalApproval) DeepCopyInto(out *KubeOVNMemberRemovalApproval) {
	*out = *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec.Decision.Targets = slices.Clone(in.Spec.Decision.Targets)
	out.Status.Observations = slices.Clone(in.Status.Observations)
	if in.Status.ReservedAt != nil {
		out.Status.ReservedAt = in.Status.ReservedAt.DeepCopy()
	}
	if in.Status.CompletedAt != nil {
		out.Status.CompletedAt = in.Status.CompletedAt.DeepCopy()
	}
}

func (in *KubeOVNMemberRemovalApproval) DeepCopy() *KubeOVNMemberRemovalApproval {
	if in == nil {
		return nil
	}
	out := new(KubeOVNMemberRemovalApproval)
	in.DeepCopyInto(out)
	return out
}

func (in *KubeOVNMemberRemovalApproval) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *KubeOVNMemberRemovalApprovalList) DeepCopyInto(out *KubeOVNMemberRemovalApprovalList) {
	*out = *in
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]KubeOVNMemberRemovalApproval, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *KubeOVNMemberRemovalApprovalList) DeepCopy() *KubeOVNMemberRemovalApprovalList {
	if in == nil {
		return nil
	}
	out := new(KubeOVNMemberRemovalApprovalList)
	in.DeepCopyInto(out)
	return out
}

func (in *KubeOVNMemberRemovalApprovalList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}
