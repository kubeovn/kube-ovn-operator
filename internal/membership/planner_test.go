package membership

import (
	"slices"
	"testing"
)

const testCluster = "dddd0000-1111-2222-3333-444444444444"

func members() []Member {
	return []Member{
		{ServerID: "aaaa0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.1:6643", Healthy: true, Leader: true},
		{ServerID: "bbbb0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.2:6643", Healthy: true},
		{ServerID: "cccc0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.3:6643", Healthy: true},
	}
}

func TestReplacementJoinsAndCatchesUpBeforeRemovingAndScaling(t *testing.T) {
	snapshot := Snapshot{ClusterID: testCluster, CommittedData: true, Members: members()}
	replacement := Member{ServerID: "eeee0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.4:6643"}
	old := snapshot.Members[1].ServerID
	plan, err := BuildPlan(snapshot, 3, replacement, old, false)
	if err != nil || len(plan.Changes) != 1 || plan.Changes[0].Operation != OperationJoin || plan.ReadyToScale || plan.ScaleWorkloadTo != 0 {
		t.Fatalf("join: %+v %v", plan, err)
	}
	snapshot.Members = append(snapshot.Members, replacement)
	if _, err := BuildPlan(snapshot, 3, replacement, old, false); err == nil {
		t.Fatal("removed old member before replacement caught up")
	}
	snapshot.Members[3].Healthy = true
	plan, err = BuildPlan(snapshot, 3, replacement, old, false)
	if err != nil || len(plan.Changes) != 1 || plan.Changes[0].Operation != OperationLeave || plan.Changes[0].ClusterID != testCluster || plan.ReadyToScale || plan.ScaleWorkloadTo != 0 {
		t.Fatalf("leave: %+v %v", plan, err)
	}
	snapshot.Members = slices.Delete(snapshot.Members, 1, 2)
	plan, err = BuildPlan(snapshot, 3, replacement, old, false)
	if err != nil || len(plan.Changes) != 0 || !plan.ReadyToScale || plan.ScaleWorkloadTo != 3 {
		t.Fatalf("convergence: %+v %v", plan, err)
	}
}

func TestScaleUpOnlyAuthorizesOneJoin(t *testing.T) {
	snapshot := Snapshot{ClusterID: testCluster, CommittedData: true, Members: members()}
	replacement := Member{ServerID: "eeee0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.4:6643"}
	plan, err := BuildPlan(snapshot, 5, replacement, "", false)
	if err != nil || len(plan.Changes) != 1 || plan.Changes[0].Operation != OperationJoin || plan.ReadyToScale {
		t.Fatalf("plan: %+v %v", plan, err)
	}
}

func TestPlannerRefusesUnsafeChanges(t *testing.T) {
	for _, scenario := range []string{"short identity", "duplicate identity", "duplicate address", "no quorum", "no leader", "no committed data", "healthy removal loses quorum", "unapproved loss", "replacement collision", "unknown removal", "unknown replacement target", "unnecessary replacement"} {
		t.Run(scenario, func(t *testing.T) {
			snapshot := Snapshot{ClusterID: testCluster, CommittedData: true, Members: members()}
			desired, remove := 1, snapshot.Members[1].ServerID
			var replacement Member
			switch scenario {
			case "short identity":
				snapshot.Members[0].ServerID = "aaaa"
			case "duplicate identity":
				snapshot.Members[1].ServerID = snapshot.Members[0].ServerID
			case "duplicate address":
				snapshot.Members[1].Address = snapshot.Members[0].Address
			case "no quorum":
				snapshot.Members[1].Healthy = false
				snapshot.Members[2].Healthy = false
			case "no leader":
				snapshot.Members[0].Leader = false
			case "no committed data":
				snapshot.CommittedData = false
			case "healthy removal loses quorum":
				snapshot.Members[2].Healthy = false
			case "unapproved loss":
				snapshot.Members[1].Healthy = false
			case "replacement collision":
				replacement = Member{ServerID: "eeee0000-1111-2222-3333-444444444444", Address: snapshot.Members[0].Address}
				desired = 3
			case "unknown removal":
				remove = "eeee0000-1111-2222-3333-444444444444"
			case "unknown replacement target":
				replacement = Member{ServerID: "eeee0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.4:6643"}
				remove = "ffff0000-1111-2222-3333-444444444444"
				desired = 3
			case "unnecessary replacement":
				replacement = Member{ServerID: "eeee0000-1111-2222-3333-444444444444", Address: "tcp:192.0.2.4:6643"}
				remove = ""
				desired = 3
			}
			if _, err := BuildPlan(snapshot, desired, replacement, remove, false); err == nil {
				t.Fatal("unsafe plan accepted")
			}
		})
	}
}

func TestKickBindsPermanentLossApprovalToExactMember(t *testing.T) {
	snapshot := Snapshot{ClusterID: testCluster, CommittedData: true, Members: members()}
	snapshot.Members[1].Healthy = false
	plan, err := BuildPlan(snapshot, 1, Member{}, snapshot.Members[1].ServerID, true)
	if err != nil || len(plan.Changes) != 1 {
		t.Fatalf("plan: %+v %v", plan, err)
	}
	change := plan.Changes[0]
	if change.Operation != OperationKick || !change.PermanentLossApproved || change.ClusterID != testCluster || change.Member != snapshot.Members[1] || plan.ReadyToScale {
		t.Fatalf("unbound change: %+v", plan)
	}
}

func TestReplacementCannotShrinkBelowDesiredSize(t *testing.T) {
	snapshot := Snapshot{ClusterID: testCluster, CommittedData: true, Members: members()}
	replacement := snapshot.Members[2]
	if _, err := BuildPlan(snapshot, 3, replacement, snapshot.Members[1].ServerID, false); err == nil {
		t.Fatal("replacement removed a member without an extra joined member")
	}
	if _, err := BuildPlan(snapshot, 3, Member{}, snapshot.Members[1].ServerID, false); err == nil {
		t.Fatal("pending removal was reported as a completed plan")
	}
}
