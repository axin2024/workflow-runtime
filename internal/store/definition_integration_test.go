package store

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

func TestMySQLInFlightRunExecutesItsOriginalInputContract(t *testing.T) {
	db, _ := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	scope := Scope{Workspace: "contract-" + strings.ToLower(rand.Text()), Actor: "test-operator"}
	if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
		t.Fatal(err)
	}
	const v1Source = `{"key":"typed-work","inputs":{"value":"integer"},"nodes":[
		{"key":"prepare","task":"prepare","queue":"typed","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none"},
		{"key":"consume","task":"consume","queue":"typed","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none",
		"depends_on":[{"node":"prepare","when":"succeeded"}],
		"input":{"value":{"type":"integer","input":"value"}},"output":{"value":"integer"}}]}`
	v1, err := db.Publish(ctx, scope, 0, []byte(v1Source))
	if err != nil {
		t.Fatal(err)
	}
	worker, err := db.RegisterWorker(ctx, scope, "input-contract", 2, []WorkerPermission{
		{Queue: "typed", Task: "prepare"}, {Queue: "typed", Task: "consume"},
	})
	if err != nil {
		t.Fatal(err)
	}
	trigger := func(key, inputs string) Run {
		t.Helper()
		receipt, err := db.Trigger(ctx, scope, TriggerRequest{DAG: "typed-work", Key: key, Kind: "manual", Inputs: []byte(inputs)})
		if err != nil {
			t.Fatal(err)
		}
		return receipt.Run
	}
	claim := func(runID int64, task, inputs string) LeaseRef {
		t.Helper()
		lease, err := db.Claim(ctx, worker.Token, ClaimRequest{Tasks: []string{task}, LeaseSeconds: 20})
		if err != nil || lease == nil || lease.RunID != runID || string(lease.Inputs) != inputs {
			t.Fatalf("lease for %d/%s: %+v, %v", runID, task, lease, err)
		}
		return lease.LeaseRef
	}
	complete := func(lease LeaseRef, outputs string) {
		t.Helper()
		receipt, err := db.Complete(ctx, worker.Token, CompletionRequest{Lease: lease, Success: true, Outputs: []byte(outputs)})
		if err != nil || receipt.State != workflow.NodeSucceeded {
			t.Fatalf("complete %d/%s: %+v, %v", lease.RunID, lease.Node, receipt, err)
		}
	}
	oldRun := trigger("old-input", `{"value":12}`)
	inFlight := claim(oldRun.ID, "prepare", "{}")
	v2Source := strings.ReplaceAll(v1Source, `"integer"`, `"string"`)
	v2, err := db.Publish(ctx, scope, 1, []byte(v2Source))
	if err != nil || v2.Revision != 2 || v1.Graph.Digest == v2.Graph.Digest {
		t.Fatalf("publish changed input type: %+v, %v", v2, err)
	}
	newRun := trigger("new-input", `{"value":"twelve"}`)
	complete(inFlight, "{}")
	complete(claim(oldRun.ID, "consume", `{"value":12}`), `{"value":12}`)
	complete(claim(newRun.ID, "prepare", "{}"), "{}")
	complete(claim(newRun.ID, "consume", `{"value":"twelve"}`), `{"value":"twelve"}`)
	for _, expected := range []struct {
		run     Run
		version Version
	}{{oldRun, v1}, {newRun, v2}} {
		report, err := db.ExportRun(ctx, scope, expected.run.ID)
		if err != nil || report.Detail.Run.State != "succeeded" || report.Detail.Version.Revision != expected.version.Revision ||
			report.Detail.Version.Graph.Digest != expected.version.Graph.Digest || report.Detail.InputDigest == "" {
			t.Fatalf("versioned run %d: %+v, %v", expected.run.ID, report, err)
		}
		t.Logf("run=%d revision=%d input_sha256=%s graph_sha256=%s", expected.run.ID, expected.version.Revision,
			report.Detail.InputDigest, report.Detail.Version.Graph.Digest)
	}
}

func TestMySQLConditionalJoinExplainsSkippedBranches(t *testing.T) {
	db, _ := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	scope := Scope{Workspace: "branch-" + strings.ToLower(rand.Text()), Actor: "test-operator"}
	if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
		t.Fatal(err)
	}
	const source = `{"key":"branch-joins","nodes":[
		{"key":"select","task":"select","queue":"joins","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none","branches":["left","right"]},
		{"key":"left","task":"left","queue":"joins","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none",
		"depends_on":[{"node":"select","when":"branch","branch":"left"}]},
		{"key":"right","task":"right","queue":"joins","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none",
		"depends_on":[{"node":"select","when":"branch","branch":"right"}]},
		{"key":"relaxed","task":"relaxed","queue":"joins","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none",
		"depends_on":[{"node":"left","when":"succeeded"},{"node":"right","when":"finished"}]},
		{"key":"strict","task":"strict","queue":"joins","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none",
		"depends_on":[{"node":"left","when":"succeeded"},{"node":"right","when":"succeeded"}]}]}`
	if _, err := db.Publish(ctx, scope, 0, []byte(source)); err != nil {
		t.Fatal(err)
	}
	run, err := db.Trigger(ctx, scope, TriggerRequest{DAG: "branch-joins", Key: "choose-left", Kind: "manual", Inputs: []byte("{}")})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := db.RegisterWorker(ctx, scope, "branch-joins", 1, []WorkerPermission{
		{Queue: "joins", Task: "select"}, {Queue: "joins", Task: "left"}, {Queue: "joins", Task: "right"},
		{Queue: "joins", Task: "relaxed"}, {Queue: "joins", Task: "strict"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []struct{ task, branch string }{{"select", "left"}, {"left", ""}, {"relaxed", ""}} {
		lease, err := db.Claim(ctx, worker.Token, ClaimRequest{Tasks: []string{step.task}, LeaseSeconds: 20})
		if err != nil || lease == nil || lease.RunID != run.Run.ID {
			t.Fatalf("claim %s: %+v, %v", step.task, lease, err)
		}
		if _, err := db.Complete(ctx, worker.Token, CompletionRequest{Lease: lease.LeaseRef, Success: true, Branch: step.branch}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := db.GetRun(ctx, scope, run.Run.ID)
	if err != nil || report.Run.State != "succeeded" {
		t.Fatalf("branch run: %+v, %v", report.Run, err)
	}
	for _, expected := range []struct{ node, from, state, reason, status string }{
		{"right", "select", "skipped", "branch_not_selected", "unmet"},
		{"relaxed", "right", "succeeded", "upstream_skipped", "satisfied"},
		{"strict", "right", "skipped", "upstream_skipped", "unmet"},
	} {
		found := false
		for _, node := range report.Nodes {
			if node.Key != expected.node || node.State != expected.state {
				continue
			}
			for _, dependency := range node.Dependencies {
				if dependency.Node == expected.from && dependency.Reason == expected.reason && dependency.Status == expected.status {
					found = true
					t.Logf("run=%d node=%s state=%s dependency=%s/%s", report.Run.ID, node.Key, node.State, dependency.Status, dependency.Reason)
				}
			}
		}
		if !found {
			t.Fatalf("missing branch decision %+v", expected)
		}
	}
}
