package main

import (
	"path/filepath"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
	"github.com/kubeovn/kube-ovn-operator/internal/backup"
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

func TestConfiguredBackupStoreAcceptsS3WithExistingCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	t.Setenv("AWS_REGION", "us-east-1")
	t.Setenv("AWS_S3_ENDPOINT", "http://127.0.0.1:19000")
	installation := &operatorv1alpha1.KubeOVNInstallation{}
	installation.Spec.Upgrade.Backup.Destination = "s3://bucket/kube-ovn"
	store, err := configuredBackupStore("", installation)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := store.(*backup.S3Store); !ok {
		t.Fatalf("configured store has type %T, want *backup.S3Store", store)
	}
}

func TestConfiguredBackupStoreRejectsS3WithoutExistingCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	installation := &operatorv1alpha1.KubeOVNInstallation{}
	installation.Spec.Upgrade.Backup.Destination = "s3://bucket/kube-ovn"
	if _, err := configuredBackupStore("", installation); err == nil || !strings.Contains(err.Error(), "AWS_ACCESS_KEY_ID") {
		t.Fatalf("S3 credential error = %v", err)
	}
}
