package membership

import "testing"

func TestBuildPlanJoinsBeforeScaling(t *testing.T) {
	plan, err := BuildPlan(Snapshot{ClusterID: "cluster-a", CommittedData: true, Members: []Member{{ServerID: "a", Address: "a", Healthy: true}, {ServerID: "b", Address: "b", Healthy: true}, {ServerID: "c", Address: "c", Healthy: true}}}, 5, Member{ServerID: "d", Address: "d"}, "", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 1 || plan.Changes[0].Operation != OperationJoin || plan.ScaleWorkloadTo != 5 {
		t.Fatalf("unexpected plan: %#v", plan)
	}
}

func TestBuildPlanRefusesKickWithoutPermanentLoss(t *testing.T) {
	_, err := BuildPlan(Snapshot{ClusterID: "cluster-a", CommittedData: true, Members: []Member{{ServerID: "a", Address: "a", Healthy: true}, {ServerID: "b", Address: "b", Healthy: false}, {ServerID: "c", Address: "c", Healthy: true}}}, 1, Member{}, "b", false)
	if err == nil {
		t.Fatal("expected explicit permanent-loss gate")
	}
}

func TestBuildPlanKicksOnlyWithQuorumAndApproval(t *testing.T) {
	plan, err := BuildPlan(Snapshot{ClusterID: "cluster-a", CommittedData: true, Members: []Member{{ServerID: "a", Address: "a", Healthy: true}, {ServerID: "b", Address: "b", Healthy: false}, {ServerID: "c", Address: "c", Healthy: true}}}, 1, Member{}, "b", true)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Changes[0].Operation != OperationKick {
		t.Fatalf("unexpected plan: %#v", plan)
	}
}
