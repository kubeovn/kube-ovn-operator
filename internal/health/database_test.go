package health

import (
	"encoding/json/v2"
	"slices"
	"strings"
	"testing"

	operatorv1alpha1 "github.com/kubeovn/kube-ovn-operator/api/v1alpha1"
)

const (
	clusterID = "11223344-1122-3344-5566-778899aabbcc"
	serverID  = "22334455-1122-3344-5566-778899aabbcc"
)

func databaseResponse(t *testing.T, db Database) []byte {
	t.Helper()
	empty := []any{"set", []any{}}
	row := map[string]any{"name": db.Name, "model": db.Model, "connected": db.Connected, "leader": db.Leader, "cid": empty, "sid": empty, "index": empty}
	if db.Model == "clustered" {
		row["cid"], row["sid"], row["index"] = []string{"uuid", db.ClusterID}, []string{"uuid", db.ServerID}, db.Index
	}
	data, err := json.Marshal([]any{map[string]any{"rows": []any{row}}}, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestParseDatabaseUsesClientVisibleIndexAndFullIdentities(t *testing.T) {
	want := Database{Name: "OVN_Northbound", Model: "clustered", Connected: true, Leader: true, ClusterID: clusterID, ServerID: serverID, Index: 9007199254740993}
	data := databaseResponse(t, want)
	for _, data := range []string{string(data), strings.ReplaceAll(strings.ReplaceAll(string(data), `"index":9007199254740993`, `"index":["set",[9007199254740993]]`), `["uuid","`+clusterID+`"]`, `["set",[["uuid","`+clusterID+`"]]]`)} {
		got, err := ParseDatabase([]byte(data), want.Name)
		if err != nil || got != want {
			t.Fatalf("got %+v, %v; want %+v", got, err, want)
		}
	}
	want = Database{Name: "OVN_Southbound", Model: "standalone", Connected: true, Leader: true}
	if got, err := ParseDatabase(databaseResponse(t, want), want.Name); err != nil || got != want {
		t.Fatalf("standalone: got %+v, %v", got, err)
	}
}

func TestParseDatabaseRejectsMissingAmbiguousAndInvalidEvidence(t *testing.T) {
	valid := string(databaseResponse(t, Database{Name: "OVN_Northbound", Model: "clustered", Connected: true, Leader: true, ClusterID: clusterID, ServerID: serverID, Index: 7}))
	cases := map[string]string{
		"malformed":            `not JSON with private-server-output`,
		"server error":         `[{"error":"private-server-output"}]`,
		"missing row":          `[{"rows":[]}]`,
		"extra result":         valid[:len(valid)-1] + `,{}]`,
		"wrong name":           strings.ReplaceAll(valid, "OVN_Northbound", "OVN_Southbound"),
		"missing connected":    strings.ReplaceAll(valid, `"connected":true,`, ""),
		"missing leader":       strings.ReplaceAll(valid, `"leader":true,`, ""),
		"duplicate field":      strings.ReplaceAll(valid, `"connected":true`, `"connected":true,"connected":false`),
		"bad uuid":             strings.ReplaceAll(valid, clusterID, "1122"),
		"zero uuid":            strings.ReplaceAll(valid, clusterID, "00000000-0000-0000-0000-000000000000"),
		"missing identity":     strings.ReplaceAll(valid, `["uuid","`+serverID+`"]`, `["set",[]]`),
		"multiple indices":     strings.ReplaceAll(valid, `"index":7`, `"index":["set",[7,8]]`),
		"null set":             strings.ReplaceAll(valid, `"index":7`, `"index":["set",null]`),
		"fractional index":     strings.ReplaceAll(valid, `"index":7`, `"index":7.5`),
		"negative index":       strings.ReplaceAll(valid, `"index":7`, `"index":-1`),
		"overflow index":       strings.ReplaceAll(valid, `"index":7`, `"index":9223372036854775808`),
		"relay model":          strings.ReplaceAll(valid, `"clustered"`, `"relay"`),
		"standalone with raft": strings.ReplaceAll(valid, `"clustered"`, `"standalone"`),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseDatabase([]byte(data), "OVN_Northbound")
			if err == nil {
				t.Fatal("accepted invalid evidence")
			}
			if strings.Contains(err.Error(), "private-server-output") {
				t.Fatal("raw server response escaped into error")
			}
		})
	}
}

func TestAssessDatabaseRequiresOneClusterLeaderAndConnectedMembers(t *testing.T) {
	base := []Database{
		{Name: "OVN_Northbound", Model: "clustered", Connected: true, Leader: true, ClusterID: clusterID, ServerID: serverID, Index: 10},
		{Name: "OVN_Northbound", Model: "clustered", Connected: true, ClusterID: clusterID, ServerID: "33445566-1122-3344-5566-778899aabbcc", Index: 9},
		{Name: "OVN_Northbound", Model: "clustered", Connected: true, ClusterID: clusterID, ServerID: "44556677-1122-3344-5566-778899aabbcc", Index: 10},
	}
	if got, err := assessDatabase("OVN_Northbound", operatorv1alpha1.CentralModeRaft, base); err != nil || got.LeaderServerID != serverID || got.ConnectedMembers != 3 {
		t.Fatalf("healthy observation: %+v %v", got, err)
	}
	for name, change := range map[string]func([]Database){
		"different cluster":      func(s []Database) { s[1].ClusterID = serverID },
		"duplicate server":       func(s []Database) { s[1].ServerID = serverID },
		"missing committed data": func(s []Database) { s[1].Index = 0 },
		"disconnected":           func(s []Database) { s[1].Connected = false },
		"no leader":              func(s []Database) { s[0].Leader = false },
		"two leaders":            func(s []Database) { s[1].Leader = true },
		"wrong mode":             func(s []Database) { s[1].Model = "standalone" },
	} {
		t.Run(name, func(t *testing.T) {
			states := slices.Clone(base)
			change(states)
			if _, err := assessDatabase("OVN_Northbound", operatorv1alpha1.CentralModeRaft, states); err == nil {
				t.Fatal("unhealthy database accepted")
			}
		})
	}
	if _, err := assessDatabase("OVN_Northbound", operatorv1alpha1.CentralModeRaft, nil); err == nil {
		t.Fatal("empty snapshot accepted")
	}
}

func TestDatabaseCommandUsesReadOnlyLocalMetadataQuery(t *testing.T) {
	for _, name := range []string{"OVN_Northbound", "OVN_Southbound"} {
		command, err := DatabaseCommand(name)
		if err != nil {
			t.Fatal(err)
		}
		if command[0] != "ovsdb-client" || command[1] != "--timeout=5" || command[2] != "query" || !strings.HasPrefix(command[3], "unix:/var/run/ovn/") {
			t.Fatalf("unsafe command: %v", command)
		}
		var transaction []any
		if err := json.Unmarshal([]byte(command[4]), &transaction); err != nil {
			t.Fatal(err)
		}
		if transaction[0] != "_Server" || transaction[1].(map[string]any)["op"] != "select" {
			t.Fatalf("not a metadata read: %v", transaction)
		}
	}
	if _, err := DatabaseCommand("OVN_Northbound; echo injected"); err == nil {
		t.Fatal("accepted unsupported database")
	}
}
