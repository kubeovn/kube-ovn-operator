// Package workload provisions one explicitly prepared central member. It keeps
// durable, create-only records; it never deletes storage or removes OVN members.
package workload

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/apply"
	"github.com/kubeovn/kube-ovn-operator/internal/central"
	"github.com/kubeovn/kube-ovn-operator/internal/lease"
	"github.com/kubeovn/kube-ovn-operator/internal/prepare"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const reservationAnnotation = "operator.kubeovn.io/member-reservation"
const memberLabel = "operator.kubeovn.io/member-record"
const dataPath = "/var/lib/ovn-member"

// Reference pins an API object's identity, including after a controller restart.
type Reference struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

// Request uses an existing Bound filesystem PVC. Its mount root must already be
// private and owned by UID 65532, as required by member-prepare. Neither this
// adapter nor its containers chmod/chown existing storage. Node and PV identity
// are caller-selected evidence, never inferred from a reusable resource name.
type Request struct {
	InstallationGeneration int64                       `json:"installationGeneration"`
	Preparation            prepare.Plan                `json:"preparation"`
	Node                   Reference                   `json:"node"`
	Claim                  Reference                   `json:"claim"`
	Volume                 Reference                   `json:"volume"`
	Image                  string                      `json:"image"`
	Resources              corev1.ResourceRequirements `json:"resources"`
	Tolerations            []corev1.Toleration         `json:"tolerations,omitempty"`
}

type Preparation struct {
	Record Reference
	// Result is nil until the native preparation Job has completed and its
	// exact output and producer identities have been durably recorded.
	Result *prepare.Result
}

type produced struct {
	Job         Reference      `json:"job"`
	Pod         Reference      `json:"pod"`
	ContainerID string         `json:"containerID"`
	Result      prepare.Result `json:"result"`
}

type binding struct {
	Object Reference `json:"object"`
	Digest string    `json:"digest"`
}

// Provisioner requires an uncached client and both installation-bound Leases
// for every operation. The lifecycle caller owns remote quorum/backup checks,
// live startup authorization and joint NB/SB convergence. A running workload is
// never reported as a healthy or caught-up member by this adapter.
type Provisioner struct {
	kube         client.Client
	installation *api.KubeOVNInstallation
}

func New(kube client.Client, installation *api.KubeOVNInstallation) (*Provisioner, error) {
	if kube == nil || installation == nil || installation.Name == "" || installation.Namespace == "" || installation.UID == "" {
		return nil, errors.New("uncached client and installation identity are required")
	}
	return &Provisioner{kube: kube, installation: installation.DeepCopy()}, nil
}

// Prepare persists a reservation before creating a Job. Retry the same request
// to observe completion. No server is started and no OVN membership is changed.
func (p *Provisioner) Prepare(ctx context.Context, request Request) (Preparation, error) {
	request, err := p.canonical(request)
	if err != nil {
		return Preparation{}, err
	}
	if err := p.guard(ctx, request, ""); err != nil {
		return Preparation{}, err
	}
	name := fmt.Sprintf("ovn-member-%x", sha256.Sum256([]byte(string(p.installation.UID)+"/"+request.Preparation.MemberName)))[:43]
	record, err := p.record(ctx, name, "request.json", request, nil)
	if err != nil {
		return Preparation{}, err
	}
	ref := Reference{Name: record.Name, UID: record.UID}
	if err := p.reserve(ctx, request, ref); err != nil {
		return Preparation{}, err
	}
	if err := p.available(ctx, request, ref); err != nil {
		return Preparation{}, err
	}
	if _, err := p.record(ctx, ref.Name+"-plan", "request.json", request.Preparation, &ref); err != nil {
		return Preparation{}, err
	}
	job := p.job(request, ref)
	if err := p.ensure(ctx, request, ref, job, ref.Name+"-job"); err != nil {
		return Preparation{}, err
	}
	output := Preparation{Record: ref}
	completed := false
	for _, condition := range job.Status.Conditions {
		if condition.Status == corev1.ConditionTrue && condition.Type == batchv1.JobFailed {
			return output, errors.New("member preparation failed; retained storage requires investigation")
		}
		completed = completed || condition.Status == corev1.ConditionTrue && condition.Type == batchv1.JobComplete
	}
	if !completed {
		return output, nil
	}
	result, err := p.collect(ctx, request, ref, job)
	if err != nil {
		return output, err
	}
	if err := p.guard(ctx, request, string(ref.UID)); err != nil {
		return output, err
	}
	if _, err := p.record(ctx, ref.Name+"-prepared", "result.json", result, &ref); err != nil {
		return output, err
	}
	output.Result = &result.Result
	return output, nil
}

// Start records the exact launch request before creating a one-member
// StatefulSet. Calling Start is an explicit authorization by the lifecycle
// executor, which must have re-observed both databases and checked backup and
// operation state. Retries cannot change that authorization or regenerate files.
func (p *Provisioner) Start(ctx context.Context, ref Reference, launch central.Request) (Reference, error) {
	request, err := p.load(ctx, ref)
	if err != nil {
		return Reference{}, err
	}
	if err := p.guard(ctx, request, string(ref.UID)); err != nil {
		return Reference{}, err
	}
	result := new(produced)
	if err := p.read(ctx, ref.Name+"-prepared", "result.json", result, &ref); err != nil {
		return Reference{}, err
	}
	if err := result.Result.Validate(request.Preparation); err != nil {
		return Reference{}, err
	}
	plan, _, err := launch.Preparation.Canonical()
	if err != nil {
		return Reference{}, err
	}
	launch.Preparation = plan
	if !reflect.DeepEqual(plan, request.Preparation) || !reflect.DeepEqual(launch.Prepared, result.Result) {
		return Reference{}, errors.New("launch must use the reserved plan and recorded native identities")
	}
	if err := launch.Validate(); err != nil {
		return Reference{}, err
	}
	// Secret identity/mount lifecycle is a separate contract. Do not silently
	// downgrade an SSL request or synthesize credentials in this adapter.
	if launch.TLS != nil {
		return Reference{}, errors.New("TLS workload provisioning requires a Secret identity provider")
	}
	for _, address := range []string{launch.NorthboundAddress, launch.SouthboundAddress} {
		if err := p.nodeAddress(ctx, request, address); err != nil {
			return Reference{}, err
		}
	}
	job := p.job(request, ref)
	if err := p.existing(ctx, job, ref.Name+"-job", ref); err != nil {
		return Reference{}, err
	}
	observed, err := p.collect(ctx, request, ref, job)
	if err != nil || !reflect.DeepEqual(observed, *result) {
		return Reference{}, errors.New("preparation producer changed before startup")
	}
	if err := p.guard(ctx, request, string(ref.UID)); err != nil {
		return Reference{}, err
	}
	config, err := p.record(ctx, ref.Name+"-launch", "request.json", launch, &ref)
	if err != nil {
		return Reference{}, err
	}
	service := p.service(ref)
	if err := p.ensure(ctx, request, ref, service, ref.Name+"-service"); err != nil {
		return Reference{}, err
	}
	workload := p.statefulSet(request, ref, config)
	if err := p.ensure(ctx, request, ref, workload, ref.Name+"-workload"); err != nil {
		return Reference{}, err
	}
	return Reference{Name: workload.Name, UID: workload.UID}, nil
}

func (p *Provisioner) canonical(request Request) (Request, error) {
	content, err := json.Marshal(request)
	if err != nil || len(content) > 16<<10 {
		return Request{}, errors.New("invalid or oversized member request")
	}
	// Detach all maps, slices and pointers supplied by a caller.
	if err := json.Unmarshal(content, &request); err != nil {
		return Request{}, err
	}
	request.Preparation, _, err = request.Preparation.Canonical()
	if err != nil {
		return Request{}, err
	}
	if request.Preparation.InstallationUID != string(p.installation.UID) {
		return Request{}, errors.New("member plan belongs to another installation")
	}
	if request.InstallationGeneration < 1 || request.InstallationGeneration != p.installation.Generation {
		return Request{}, errors.New("member request belongs to another installation generation")
	}
	for _, ref := range []Reference{request.Node, request.Claim, request.Volume} {
		if ref.UID == "" || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 {
			return Request{}, errors.New("node, claim and volume names and UIDs are required")
		}
	}
	image, digest, found := strings.Cut(request.Image, "@sha256:")
	if !found || image == "" || len(digest) != 64 || strings.Trim(digest, "0123456789abcdef") != "" || strings.ContainsAny(image, " \t\n@") {
		return Request{}, errors.New("runtime image must be pinned to a sha256 digest")
	}
	for _, db := range request.Preparation.Databases {
		if !strings.HasPrefix(db.LocalAddress, "tcp:") {
			return Request{}, errors.New("TLS workload provisioning requires a Secret identity provider")
		}
	}
	return request, nil
}

func (p *Provisioner) guard(ctx context.Context, request Request, reservation string) error {
	if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
		return err
	}
	installation := new(api.KubeOVNInstallation)
	if err := p.kube.Get(ctx, client.ObjectKeyFromObject(p.installation), installation); err != nil {
		return err
	}
	if installation.UID != p.installation.UID || installation.DeletionTimestamp != nil || installation.Generation != p.installation.Generation {
		return errors.New("installation changed during member provisioning")
	}
	for _, db := range request.Preparation.Databases {
		if err := p.nodeAddress(ctx, request, db.LocalAddress); err != nil {
			return err
		}
	}
	claim := new(corev1.PersistentVolumeClaim)
	if err := p.kube.Get(ctx, client.ObjectKey{Namespace: p.installation.Namespace, Name: request.Claim.Name}, claim); err != nil {
		return err
	}
	if claim.UID != request.Claim.UID || claim.DeletionTimestamp != nil || claim.Status.Phase != corev1.ClaimBound || claim.Spec.VolumeName != request.Volume.Name ||
		claim.Spec.VolumeMode != nil && *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return errors.New("member claim identity, binding or filesystem mode changed")
	}
	if reservation != "" && claim.Annotations[reservationAnnotation] != reservation {
		return errors.New("member claim reservation changed")
	}
	volume := new(corev1.PersistentVolume)
	if err := p.kube.Get(ctx, client.ObjectKey{Name: request.Volume.Name}, volume); err != nil {
		return err
	}
	owner := volume.Spec.ClaimRef
	if volume.UID != request.Volume.UID || volume.DeletionTimestamp != nil || volume.Status.Phase != corev1.VolumeBound || owner == nil ||
		owner.UID != claim.UID || owner.Name != claim.Name || owner.Namespace != claim.Namespace {
		return errors.New("member volume identity or claim binding changed")
	}
	return nil
}

func (p *Provisioner) nodeAddress(ctx context.Context, request Request, address string) error {
	node := new(corev1.Node)
	if err := p.kube.Get(ctx, client.ObjectKey{Name: request.Node.Name}, node); err != nil {
		return err
	}
	ready := slices.ContainsFunc(node.Status.Conditions, func(c corev1.NodeCondition) bool {
		return c.Type == corev1.NodeReady && c.Status == corev1.ConditionTrue
	})
	if node.UID != request.Node.UID || node.DeletionTimestamp != nil || node.Spec.Unschedulable || !ready {
		return errors.New("member node identity or readiness changed")
	}
	_, endpoint, _ := strings.Cut(address, ":")
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1024 {
		return errors.New("unprivileged member requires nonprivileged listener ports")
	}
	if !slices.ContainsFunc(node.Status.Addresses, func(a corev1.NodeAddress) bool {
		return a.Type == corev1.NodeInternalIP && a.Address == host
	}) {
		return errors.New("member listener is not a current InternalIP of the pinned node")
	}
	return nil
}

func (p *Provisioner) reserve(ctx context.Context, request Request, ref Reference) error {
	claim := new(corev1.PersistentVolumeClaim)
	if err := p.kube.Get(ctx, client.ObjectKey{Namespace: p.installation.Namespace, Name: request.Claim.Name}, claim); err != nil {
		return err
	}
	current := claim.Annotations[reservationAnnotation]
	if current != "" && current != string(ref.UID) {
		return errors.New("claim is already reserved by another member")
	}
	if current == "" {
		if err := p.guard(ctx, request, ""); err != nil {
			return err
		}
		if claim.UID != request.Claim.UID {
			return errors.New("claim identity changed before reservation")
		}
		if claim.Annotations == nil {
			claim.Annotations = make(map[string]string)
		}
		claim.Annotations[reservationAnnotation] = string(ref.UID)
		// Update carries resourceVersion: another member cannot reserve the same
		// claim concurrently, even under another installation's Lease.
		if err := p.kube.Update(ctx, claim); err != nil {
			return err
		}
	}
	return p.guard(ctx, request, string(ref.UID))
}

func (p *Provisioner) collect(ctx context.Context, request Request, ref Reference, job *batchv1.Job) (produced, error) {
	if err := p.available(ctx, request, ref); err != nil {
		return produced{}, err
	}
	complete := slices.ContainsFunc(job.Status.Conditions, func(c batchv1.JobCondition) bool {
		return c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue
	})
	failed := slices.ContainsFunc(job.Status.Conditions, func(c batchv1.JobCondition) bool {
		return c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue
	})
	if !complete || failed || job.Status.Active != 0 || job.DeletionTimestamp != nil || job.Status.Terminating != nil && *job.Status.Terminating != 0 {
		return produced{}, errors.New("preparation job has not completed")
	}
	pods := new(corev1.PodList)
	if err := p.kube.List(ctx, pods, client.InNamespace(p.installation.Namespace)); err != nil {
		return produced{}, err
	}
	var output *produced
	for _, pod := range pods.Items {
		owner := metav1.GetControllerOf(&pod)
		ours := owner != nil && owner.Kind == "Job" && owner.APIVersion == "batch/v1" && owner.Name == job.Name && owner.UID == job.UID
		if !ours {
			continue
		}
		if pod.Status.Phase != corev1.PodSucceeded {
			if pod.Status.Phase == corev1.PodFailed && len(pod.Status.ContainerStatuses) == 1 && pod.Status.ContainerStatuses[0].State.Terminated != nil {
				continue
			}
			return produced{}, errors.New("preparation still has a live pod")
		}
		if output != nil || pod.UID == "" || pod.DeletionTimestamp != nil || !matchesPod(job.Spec.Template.Spec, pod.Spec) || len(pod.Status.ContainerStatuses) != 1 {
			return produced{}, errors.New("ambiguous or modified preparation pod")
		}
		status := pod.Status.ContainerStatuses[0]
		ended := status.State.Terminated
		if status.Name != "prepare" || status.ContainerID == "" || ended == nil || ended.ExitCode != 0 || len(ended.Message) > 4096 {
			return produced{}, errors.New("preparation container has no successful bounded result")
		}
		var result prepare.Result
		if err := json.Unmarshal([]byte(ended.Message), &result, json.RejectUnknownMembers(true)); err != nil {
			return produced{}, errors.New("invalid preparation result JSON")
		}
		if err := result.Validate(request.Preparation); err != nil {
			return produced{}, err
		}
		output = &produced{Job: Reference{Name: job.Name, UID: job.UID}, Pod: Reference{Name: pod.Name, UID: pod.UID}, ContainerID: status.ContainerID, Result: result}
	}
	if output == nil {
		return produced{}, errors.New("completed preparation job has no verified result producer")
	}
	return *output, nil
}

func usesClaim(pod *corev1.Pod, name string) bool {
	return slices.ContainsFunc(pod.Spec.Volumes, func(volume corev1.Volume) bool {
		return volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == name
	})
}

func (p *Provisioner) available(ctx context.Context, request Request, ref Reference) error {
	pods := new(corev1.PodList)
	if err := p.kube.List(ctx, pods, client.InNamespace(p.installation.Namespace)); err != nil {
		return err
	}
	for _, pod := range pods.Items {
		if !usesClaim(&pod, request.Claim.Name) || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		owner := metav1.GetControllerOf(&pod)
		var expected client.Object
		bindingName := ref.Name + "-job"
		if owner != nil && owner.APIVersion == "batch/v1" && owner.Kind == "Job" {
			expected = p.job(request, ref)
		} else if owner != nil && owner.APIVersion == "apps/v1" && owner.Kind == "StatefulSet" {
			expected = p.statefulSet(request, ref, &corev1.ConfigMap{Name: ref.Name + "-launch"})
			bindingName = ref.Name + "-workload"
		}
		if expected == nil || owner.Name != expected.GetName() {
			return errors.New("another live pod uses the member claim")
		}
		actual := expected.DeepCopyObject().(client.Object)
		if err := p.kube.Get(ctx, client.ObjectKeyFromObject(actual), actual); err != nil {
			return err
		}
		if actual.GetUID() != owner.UID || !p.matches(expected, actual, ref) {
			return errors.New("member claim consumer identity changed")
		}
		var saved binding
		if err := p.read(ctx, bindingName, "binding.json", &saved, &ref); err == nil {
			if err := p.existing(ctx, actual, bindingName, ref); err != nil {
				return err
			}
		} else if !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (p *Provisioner) load(ctx context.Context, ref Reference) (Request, error) {
	if ref.Name == "" || ref.UID == "" {
		return Request{}, errors.New("persisted member reference is required")
	}
	record := new(corev1.ConfigMap)
	if err := p.kube.Get(ctx, client.ObjectKey{Namespace: p.installation.Namespace, Name: ref.Name}, record); err != nil {
		return Request{}, err
	}
	if record.UID != ref.UID {
		return Request{}, errors.New("member record identity changed")
	}
	var request Request
	if err := p.decode(record, "request.json", &request, nil); err != nil {
		return Request{}, err
	}
	return p.canonical(request)
}

func (p *Provisioner) read(ctx context.Context, name, key string, value any, parent *Reference) error {
	record := new(corev1.ConfigMap)
	if err := p.kube.Get(ctx, client.ObjectKey{Namespace: p.installation.Namespace, Name: name}, record); err != nil {
		return err
	}
	return p.decode(record, key, value, parent)
}

func (p *Provisioner) decode(record *corev1.ConfigMap, key string, value any, parent *Reference) error {
	if record.UID == "" || record.DeletionTimestamp != nil || record.Immutable == nil || !*record.Immutable ||
		len(record.OwnerReferences) != 0 || len(record.Data) != 1 || len(record.BinaryData) != 0 || record.Data[key] == "" || len(record.Data[key]) > 32<<10 || !p.owned(record, parent) {
		return errors.New("invalid member record identity or immutable contents")
	}
	return json.Unmarshal([]byte(record.Data[key]), value, json.RejectUnknownMembers(true))
}

func (p *Provisioner) record(ctx context.Context, name, key string, value any, parent *Reference) (*corev1.ConfigMap, error) {
	content, err := json.Marshal(value, json.Deterministic(true))
	if err != nil || len(content) > 32<<10 {
		return nil, errors.New("invalid or oversized member record")
	}
	record := &corev1.ConfigMap{Name: name, Namespace: p.installation.Namespace, Labels: p.labels(parent), Immutable: new(true), Data: map[string]string{key: string(content)}}
	existing := new(corev1.ConfigMap)
	err = p.kube.Get(ctx, client.ObjectKeyFromObject(record), existing)
	if apierrors.IsNotFound(err) {
		if err := lease.CheckFor(ctx, lease.Central, p.installation.UID); err != nil {
			return nil, err
		}
		if err := p.kube.Create(ctx, record); err != nil {
			return nil, err
		}
		if err := p.kube.Get(ctx, client.ObjectKeyFromObject(record), existing); err != nil {
			return nil, err
		}
		if record.UID != existing.UID {
			return nil, errors.New("member record changed during creation")
		}
	} else if err != nil {
		return nil, err
	}
	var raw any
	if err := p.decode(existing, key, &raw, parent); err != nil {
		return nil, err
	}
	if existing.Data[key] != string(content) {
		return nil, errors.New("member record conflicts with the persisted operation")
	}
	return existing, nil
}

func (p *Provisioner) labels(parent *Reference) map[string]string {
	labels := map[string]string{
		apply.ManagedByLabel: apply.ManagedByValue, apply.InstallationLabel: p.installation.Name,
		apply.InstallationNamespaceLabel: p.installation.Namespace, apply.InstallationUIDLabel: string(p.installation.UID),
	}
	if parent != nil {
		labels[memberLabel] = string(parent.UID)
	}
	return labels
}

func (p *Provisioner) owned(object client.Object, parent *Reference) bool {
	for key, value := range p.labels(parent) {
		if object.GetLabels()[key] != value {
			return false
		}
	}
	return true
}
