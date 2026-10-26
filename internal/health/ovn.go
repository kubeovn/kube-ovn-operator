package health

import (
	"context"
	"fmt"
	"strings"
	"time"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type OVNReport struct {
	Ready     bool
	Reason    string
	Message   string
	Members   []operatorv1alpha1.CentralMemberStatus
	Databases []operatorv1alpha1.CentralDatabaseStatus
}

type OVNProvider interface {
	Observe(context.Context, *operatorv1alpha1.KubeOVNInstallation) (OVNReport, error)
}

type OVNObserver struct {
	Reader client.Reader
	Runner CommandRunner
}

func (o *OVNObserver) Observe(ctx context.Context, installation *operatorv1alpha1.KubeOVNInstallation) (OVNReport, error) {
	report := OVNReport{Reason: "OVNVerificationPending", Message: "Waiting for OVN database health evidence"}
	if installation.Spec.Mode == operatorv1alpha1.InstallationModeDataPlaneOnly {
		report.Ready = true
		report.Reason = "NotRequired"
		report.Message = "This installation uses an external OVN central"
		return report, nil
	}
	if o.Reader == nil || o.Runner == nil {
		return report, fmt.Errorf("OVN observation requires a reader and command runner")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	pods, err := ownedPods(ctx, o.Reader, installation, "ovn-central")
	if err != nil {
		return report, err
	}
	if len(pods) == 0 || len(pods) != int(installation.Spec.Central.Replicas) {
		report.Reason = "CentralPodsPending"
		report.Message = fmt.Sprintf("Observed %d owned central pods; need %d", len(pods), installation.Spec.Central.Replicas)
		return report, nil
	}
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || !podReady(pod) {
			report.Reason = "CentralPodsPending"
			report.Message = fmt.Sprintf("Central pod %s is not ready", pod.Name)
			return report, nil
		}
	}
	var failures []string
	for _, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
		command, err := DatabaseCommand(name)
		if err != nil {
			return report, err
		}
		states := make([]Database, 0, len(pods))
		for _, pod := range pods {
			data, err := o.Runner.Run(ctx, pod.Namespace, pod.Name, "ovn-central", command)
			if err != nil {
				return OVNReport{}, fmt.Errorf("observe %s on %s: %w", name, pod.Name, err)
			}
			db, err := ParseDatabase(data, name)
			if err != nil {
				return OVNReport{}, fmt.Errorf("observe %s on %s: %w", name, pod.Name, err)
			}
			states = append(states, db)
			state := "Disconnected"
			if db.Connected {
				state = "Connected"
			}
			report.Members = append(report.Members, operatorv1alpha1.CentralMemberStatus{Database: name, ServerID: db.ServerID, Pod: pod.Name, Node: pod.Spec.NodeName, State: state, Leader: db.Leader, AppliedIndex: db.Index})
		}
		summary, err := assessDatabase(name, installation.Spec.Central.Mode, states)
		report.Databases = append(report.Databases, summary)
		if err != nil {
			failures = append(failures, err.Error())
		}
	}
	if len(failures) > 0 {
		report.Reason, report.Message = "OVNDatabaseUnhealthy", strings.Join(failures, "; ")
		return report, nil
	}
	// Exec addresses a pod by name and has no UID precondition. Revalidate the
	// owner chain after sampling so a replacement cannot inherit old evidence.
	current, err := ownedPods(ctx, o.Reader, installation, "ovn-central")
	if err != nil {
		return OVNReport{}, err
	}
	if !samePods(pods, current) {
		return OVNReport{Reason: "CentralPodsChanged", Message: "Central pod identities or readiness changed during OVN observation"}, nil
	}
	report.Ready, report.Reason = true, "OVNDatabasesHealthy"
	report.Message = "NB/SB identities, leaders, storage connectivity and client-visible data are verified; network probes remain required"
	return report, nil
}

func assessDatabase(name string, mode operatorv1alpha1.CentralMode, states []Database) (operatorv1alpha1.CentralDatabaseStatus, error) {
	summary := operatorv1alpha1.CentralDatabaseStatus{Database: name, ObservedMembers: int32(len(states))}
	model := "clustered"
	if mode == operatorv1alpha1.CentralModeSingle {
		model = "standalone"
	}
	leaders := 0
	servers := make(map[string]bool)
	for _, db := range states {
		if db.Model != model {
			return summary, fmt.Errorf("%s storage model does not match %s", name, mode)
		}
		if model == "clustered" {
			if db.ClusterID == "" || db.ServerID == "" || db.Index <= 0 || servers[db.ServerID] {
				return summary, fmt.Errorf("%s lacks unique server identities or client-visible committed data", name)
			}
			servers[db.ServerID] = true
			if summary.ClusterID == "" {
				summary.ClusterID = db.ClusterID
			} else if summary.ClusterID != db.ClusterID {
				return summary, fmt.Errorf("%s reports different cluster IDs", name)
			}
		}
		if db.Connected {
			summary.ConnectedMembers++
		}
		if db.Leader {
			leaders++
			summary.LeaderServerID = db.ServerID
		}
	}
	if len(states) == 0 || summary.ConnectedMembers != int32(len(states)) || leaders != 1 {
		return summary, fmt.Errorf("%s has %d/%d storage-connected members and %d leaders", name, summary.ConnectedMembers, len(states), leaders)
	}
	return summary, nil
}
