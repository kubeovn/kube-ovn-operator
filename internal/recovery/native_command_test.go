package recovery

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestCommandNativeBindsTargetIDAndInspectsIdentity(t *testing.T) {
	var calls [][]string
	native := CommandNative{Run: func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, append([]string(nil), args...))
		switch args[0] {
		case "--target-cluster-id=cccc0000-1111-2222-3333-444444444444":
			if len(args) != 5 || args[1] != "create-cluster" {
				return "", errors.New("unexpected create arguments")
			}
			return "", nil
		case "db-cid":
			return targetNB + "\n", nil
		case "db-sid":
			return "eeee0000-1111-2222-3333-555555555555\n", nil
		case "db-local-address":
			return "tcp:192.0.2.4:6643\n", nil
		case "db-is-clustered":
			return "yes\n", nil
		default:
			return "", errors.New("unexpected command")
		}
	}}
	if err := native.CreateCluster(t.Context(), "/var/lib/target.db", "/var/lib/backup.db", "tcp:192.0.2.4:6643", targetNB); err != nil {
		t.Fatal(err)
	}
	identity, err := native.Inspect(t.Context(), "/var/lib/target.db")
	if err != nil {
		t.Fatal(err)
	}
	if identity.ClusterID != targetNB || identity.Address != "tcp:192.0.2.4:6643" || len(calls) != 5 {
		t.Fatalf("unexpected native identity or calls: %+v %v", identity, calls)
	}
}

func TestCommandNativeRejectsUnsafeInputsAndInspection(t *testing.T) {
	for _, scenario := range []string{"relative database", "relative contents", "invalid cluster", "invalid address", "nonclustered", "invalid server"} {
		t.Run(scenario, func(t *testing.T) {
			native := CommandNative{Run: func(_ context.Context, args ...string) (string, error) {
				switch args[0] {
				case "db-cid":
					return targetNB, nil
				case "db-sid":
					if scenario == "invalid server" {
						return "bad", nil
					}
					return "eeee0000-1111-2222-3333-555555555555", nil
				case "db-local-address":
					return "tcp:192.0.2.4:6643", nil
				case "db-is-clustered":
					if scenario == "nonclustered" {
						return "no", nil
					}
					return "yes", nil
				default:
					return "", nil
				}
			}}
			switch scenario {
			case "relative database":
				if err := native.CreateCluster(t.Context(), "target.db", "/var/lib/backup.db", "tcp:192.0.2.4:6643", targetNB); err == nil {
					t.Fatal("relative database path accepted")
				}
			case "relative contents":
				if err := native.CreateCluster(t.Context(), "/var/lib/target.db", "backup.db", "tcp:192.0.2.4:6643", targetNB); err == nil {
					t.Fatal("relative contents path accepted")
				}
			case "invalid cluster":
				if err := native.CreateCluster(t.Context(), "/var/lib/target.db", "/var/lib/backup.db", "tcp:192.0.2.4:6643", "bad"); err == nil {
					t.Fatal("invalid target cluster accepted")
				}
			case "invalid address":
				if err := native.CreateCluster(t.Context(), "/var/lib/target.db", "/var/lib/backup.db", "tcp:host:6643", targetNB); err == nil {
					t.Fatal("invalid address accepted")
				}
			default:
				if _, err := native.Inspect(t.Context(), "/var/lib/target.db"); err == nil {
					t.Fatal("unsafe native inspection accepted")
				}
			}
		})
	}
}

func TestNativeOutputIsBounded(t *testing.T) {
	output := new(nativeOutput)
	if _, err := output.Write([]byte(strings.Repeat("x", 4<<10+1))); err == nil {
		t.Fatal("oversized native output accepted")
	}
}
