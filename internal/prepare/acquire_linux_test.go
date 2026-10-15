package prepare

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"testing"
	"uuid"
)

func TestLiveOVNPreparedAcquireDoesNotRepairOrRecreateFiles(t *testing.T) {
	for _, scenario := range []string{"missing request", "missing result", "missing NB", "partial result", "foreign result", "changed SID", "changed plan"} {
		t.Run(scenario, func(t *testing.T) {
			p := openTest(t, privateTempDir(t), nativeTool(t))
			plan := testPlan()
			result, err := p.Prepare(t.Context(), plan)
			if err != nil {
				t.Fatal(err)
			}
			missing := ""
			switch scenario {
			case "missing request":
				missing = plan.OperationID + "/request.json"
			case "missing result":
				missing = plan.OperationID + "/prepared.json"
			case "missing NB":
				missing = result.Databases[0].Path
			case "partial result":
				if err := p.writeJSON(plan.OperationID+"/prepared.json", Result{PlanDigest: result.PlanDigest}); err != nil {
					t.Fatal(err)
				}
			case "foreign result":
				result.Databases[0].ServerID = uuid.New().String()
			case "changed SID":
				result.Databases[0].ServerID = uuid.New().String()
				if err := p.writeJSON(plan.OperationID+"/prepared.json", result); err != nil {
					t.Fatal(err)
				}
			case "changed plan":
				plan.MemberName = "another-member"
			}
			if missing != "" {
				if err := p.root.Remove(missing); err != nil {
					t.Fatal(err)
				}
			}
			snapshot := func() string {
				t.Helper()
				contents := map[string]string{}
				entries, err := p.root.ReadFile(plan.OperationID + "/ovnsb_db.db")
				if err != nil {
					t.Fatal(err)
				}
				contents["SB"] = string(entries)
				for _, name := range []string{"ovnnb_db.db", "request.json", "prepared.json"} {
					data, err := p.root.ReadFile(filepath.Join(plan.OperationID, name))
					if err == nil {
						contents[name] = string(data)
					} else if !os.IsNotExist(err) {
						t.Fatal(err)
					}
				}
				data, err := json.Marshal(contents, json.Deterministic(true))
				if err != nil {
					t.Fatal(err)
				}
				return string(data)
			}
			before := snapshot()
			if held, err := p.Acquire(t.Context(), plan, result); err == nil {
				_ = held.Close()
				t.Fatal("incomplete or mismatched prepared pair accepted for launch")
			}
			if snapshot() != before {
				t.Fatal("launch acquisition modified or recreated data")
			}
		})
	}
}
