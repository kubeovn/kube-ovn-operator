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

func TestCompareVersionsClassifiesPinnedTransitions(t *testing.T) {
	for _, test := range []struct {
		from, to string
		want     VersionDirection
	}{
		{from: "v1.16.0", to: "v1.17.0", want: VersionUpgrade},
		{from: "v1.17.0", to: "v1.17.0", want: VersionUnchanged},
		{from: "v1.17.0", to: "v1.16.0", want: VersionRollback},
	} {
		got, err := CompareVersions(test.from, test.to)
		if err != nil || got != test.want {
			t.Fatalf("CompareVersions(%q, %q) = %q, %v; want %q", test.from, test.to, got, err, test.want)
		}
	}
}

func TestValidateRejectsRollbackWithoutRestoreExecutor(t *testing.T) {
	contract := Contract{FromVersion: "v1.17.0", ToVersion: "v1.16.0", FromCentralMode: "raft", ToCentralMode: "raft", BackupReady: true, CRDsReady: true, AllowRollback: true}
	if err := Validate(contract); err == nil {
		t.Fatal("rollback was admitted without a restore executor")
	}
	contract.RollbackReady = true
	if err := Validate(contract); err != nil {
		t.Fatalf("version-aware rollback was rejected: %v", err)
	}
}
