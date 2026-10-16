package lifecycle

import (
	"strings"
	"testing"
	"time"

	api "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBackupPairReferencesAreCompleteAndContentAddressed(t *testing.T) {
	refs := map[string]string{
		"OVN_Northbound": "sha256:" + strings.Repeat("a", 64),
		"OVN_Southbound": "sha256:" + strings.Repeat("b", 64),
	}
	status := &api.BackupStatus{ID: backupPairID(refs), DatabaseRefs: refs, Version: "v1.17.0", TopologyDigest: "sha256:" + strings.Repeat("c", 64), CompletedAt: metav1.NewTime(time.Unix(10, 0))}
	if err := validateBackupReferences(status); err != nil {
		t.Fatalf("valid backup pair rejected: %v", err)
	}

	status.ID = "sha256:" + strings.Repeat("d", 64)
	if err := validateBackupReferences(status); err == nil {
		t.Fatal("backup pair with a mismatched content ID was accepted")
	}
	status.ID = backupPairID(refs)
	delete(status.DatabaseRefs, "OVN_Southbound")
	if err := validateBackupReferences(status); err == nil {
		t.Fatal("partial backup pair was accepted")
	}
}
