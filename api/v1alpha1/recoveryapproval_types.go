package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// RecoveryApprovalReference pins a recovery approval by name and UID. A name
// alone is insufficient because an approval object can be recreated.
type RecoveryApprovalReference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

// RecoveryRequestReference identifies the request bound by a destructive
// recovery approval. Upgrade rollbacks leave this reference nil and bind the
// approval directly to the installation generation and target pair.
type RecoveryRequestReference struct {
	Name       string    `json:"name"`
	UID        types.UID `json:"uid"`
	Generation int64     `json:"generation"`
}

func (in *RecoveryApprovalReference) DeepCopyInto(out *RecoveryApprovalReference) {
	*out = *in
}

func (in *RecoveryApprovalReference) DeepCopy() *RecoveryApprovalReference {
	if in == nil {
		return nil
	}
	out := new(RecoveryApprovalReference)
	in.DeepCopyInto(out)
	return out
}

func (in *RecoveryRequestReference) DeepCopyInto(out *RecoveryRequestReference) {
	*out = *in
}

func (in *RecoveryRequestReference) DeepCopy() *RecoveryRequestReference {
	if in == nil {
		return nil
	}
	out := new(RecoveryRequestReference)
	in.DeepCopyInto(out)
	return out
}

// RecoveryApprovalSpec is an immutable, human-created decision. The operator
// may reserve and complete the status but must not create or alter the
// decision itself.
type RecoveryApprovalSpec struct {
	InstallationRef        InstallationReference     `json:"installationRef"`
	InstallationGeneration int64                     `json:"installationGeneration"`
	RequestRef             *RecoveryRequestReference `json:"requestRef,omitempty"`
	Mode                   string                    `json:"mode"`
	SourceVersion          string                    `json:"sourceVersion"`
	TargetVersion          string                    `json:"targetVersion"`
	BackupRef              string                    `json:"backupRef"`
	TopologyDigest         string                    `json:"topologyDigest"`
	ChartDigest            string                    `json:"chartDigest"`
	Reason                 string                    `json:"reason"`
	ExpiresAt              metav1.Time               `json:"expiresAt"`
	Revoked                bool                      `json:"revoked,omitempty"`
}

const (
	RecoveryApprovalReserved  = "Reserved"
	RecoveryApprovalCompleted = "Completed"
)

// RecoveryApprovalStatus is the operator-owned audit record for one decision
// reservation. A completed approval can never be reused for another target.
type RecoveryApprovalStatus struct {
	Phase          string       `json:"phase,omitempty"`
	DecisionDigest string       `json:"decisionDigest,omitempty"`
	ReservedAt     *metav1.Time `json:"reservedAt,omitempty"`
	CompletedAt    *metav1.Time `json:"completedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type KubeOVNRecoveryApproval struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              RecoveryApprovalSpec   `json:"spec"`
	Status            RecoveryApprovalStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type KubeOVNRecoveryApprovalList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:",inline"`
	Items           []KubeOVNRecoveryApproval `json:"items"`
}

func (in *KubeOVNRecoveryApproval) DeepCopyInto(out *KubeOVNRecoveryApproval) {
	*out = *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *KubeOVNRecoveryApproval) DeepCopy() *KubeOVNRecoveryApproval {
	if in == nil {
		return nil
	}
	out := new(KubeOVNRecoveryApproval)
	in.DeepCopyInto(out)
	return out
}

func (in *KubeOVNRecoveryApproval) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *KubeOVNRecoveryApprovalList) DeepCopyInto(out *KubeOVNRecoveryApprovalList) {
	*out = *in
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		out.Items = make([]KubeOVNRecoveryApproval, len(in.Items))
		for i := range in.Items {
			in.Items[i].DeepCopyInto(&out.Items[i])
		}
	}
}

func (in *KubeOVNRecoveryApprovalList) DeepCopy() *KubeOVNRecoveryApprovalList {
	if in == nil {
		return nil
	}
	out := new(KubeOVNRecoveryApprovalList)
	in.DeepCopyInto(out)
	return out
}

func (in *KubeOVNRecoveryApprovalList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *RecoveryApprovalSpec) DeepCopyInto(out *RecoveryApprovalSpec) {
	*out = *in
	in.ExpiresAt.DeepCopyInto(&out.ExpiresAt)
	if in.RequestRef != nil {
		out.RequestRef = new(RecoveryRequestReference)
		*out.RequestRef = *in.RequestRef
	}
}

func (in *RecoveryApprovalStatus) DeepCopyInto(out *RecoveryApprovalStatus) {
	*out = *in
	if in.ReservedAt != nil {
		out.ReservedAt = in.ReservedAt.DeepCopy()
	}
	if in.CompletedAt != nil {
		out.CompletedAt = in.CompletedAt.DeepCopy()
	}
}
