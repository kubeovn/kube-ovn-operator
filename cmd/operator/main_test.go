package main

import (
	"path/filepath"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

func TestConfiguredBackupStoreBindsDestinationToBackupDir(t *testing.T) {
	directory := t.TempDir()
	installation := &operatorv1alpha1.KubeOVNInstallation{}
	installation.Spec.Upgrade.Backup.Destination = "file://" + directory

	store, err := configuredBackupStore(directory, installation)
	if err != nil {
		t.Fatalf("configured backup store: %v", err)
	}
	if closer, ok := store.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			t.Fatalf("close backup store: %v", err)
		}
	} else {
		t.Fatal("configured backup store is not closable")
	}
}

func TestConfiguredBackupStoreRejectsUnboundDestination(t *testing.T) {
	directory := t.TempDir()
	installation := &operatorv1alpha1.KubeOVNInstallation{}
	installation.Spec.Upgrade.Backup.Destination = "file://" + filepath.Join(directory, "other")
	if _, err := configuredBackupStore(directory, installation); err == nil {
		t.Fatal("accepted a destination outside --backup-dir")
	}
	if _, err := configuredBackupStore("", installation); err == nil || !strings.Contains(err.Error(), "--backup-dir") {
		t.Fatalf("empty --backup-dir error = %v", err)
	}
}

func TestConfiguredBackupStoreRejectsUnsupportedDestination(t *testing.T) {
	installation := &operatorv1alpha1.KubeOVNInstallation{}
	installation.Spec.Upgrade.Backup.Destination = "s3://bucket/kube-ovn"
	if _, err := configuredBackupStore(t.TempDir(), installation); err == nil {
		t.Fatal("accepted unsupported object-store destination")
	}
}
