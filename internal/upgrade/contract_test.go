package upgrade

import "testing"

func TestValidateRequiresBackupAndCRDs(t *testing.T) {
	if err := Validate(Contract{FromVersion: "v1.16.0", ToVersion: "v1.17.0", FromCentralMode: "raft", ToCentralMode: "raft"}); err == nil {
		t.Fatal("expected upgrade gates")
	}
}

func TestValidateRejectsCentralModeSwitch(t *testing.T) {
	if err := Validate(Contract{FromVersion: "v1.16.0", ToVersion: "v1.17.0", FromCentralMode: "raft", ToCentralMode: "single", BackupReady: true, CRDsReady: true}); err == nil {
		t.Fatal("expected central mode transition rejection")
	}
}
