package store

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

func TestMySQLWorkersFenceExpiredLeaseAndAdvanceDiamond(t *testing.T) {
	db, _ := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	scope := Scope{Workspace: "exec-" + strings.ToLower(rand.Text()), Actor: "test-operator"}
	foreign := Scope{Workspace: scope.Workspace + "-foreign", Actor: scope.Actor}
	for _, workspace := range []Scope{scope, foreign} {
		if err := db.CreateWorkspace(ctx, workspace.Workspace); err != nil {
			t.Fatal(err)
		}
	}
	definition, err := os.ReadFile("../../examples/diamond.json")
	if err != nil {
		t.Fatal(err)
	}
	version, err := db.Publish(ctx, scope, 0, definition)
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := db.Trigger(ctx, scope, TriggerRequest{DAG: version.Graph.Definition.Key, Key: "fixture", Kind: "manual",
		Inputs: json.RawMessage(`{"rows":12,"source":"fixture"}`)})
	if err != nil {
		t.Fatal(err)
	}
	register := func(workspace Scope, key, queue string, tasks ...string) string {
		t.Helper()
		var permissions []WorkerPermission
		for _, task := range tasks {
			permissions = append(permissions, WorkerPermission{Queue: queue, Task: task})
		}
		worker, err := db.RegisterWorker(ctx, workspace, key, 1, permissions)
		if err != nil {
			t.Fatal(err)
		}
		return worker.Token
	}
	tokenA := register(scope, "a", "reports", "extract", "aggregate", "report")
	tokenB := register(scope, "b", "reports", "extract", "aggregate", "inspect", "report", "cleanup")
	tokenForeign := register(foreign, "foreign", "reports", "extract")
	tokenQueue := register(scope, "different-queue", "default", "extract")
	tokenTask := register(scope, "different-task", "reports", "inspect")
	claim := func(token, task string, seconds int) *ClaimedTask {
		t.Helper()
		result, err := db.Claim(ctx, token, ClaimRequest{Tasks: []string{task}, LeaseSeconds: seconds})
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	requireClaim := func(token, task string, seconds int) *ClaimedTask {
		t.Helper()
		result := claim(token, task, seconds)
		if result == nil || result.RunID != trigger.Run.ID || result.Node != task {
			t.Fatalf("claim %s: %+v", task, result)
		}
		return result
	}
	if _, err := db.Claim(ctx, "invalid", ClaimRequest{Tasks: []string{"extract"}, LeaseSeconds: 1}); !errors.Is(err, ErrWorkerUnauthorized) {
		t.Fatalf("wrong token: %v", err)
	}
	for _, token := range []string{tokenForeign, tokenQueue, tokenTask} {
		if result := claim(token, "extract", 1); result != nil {
			t.Fatalf("unauthorized claim: %+v", result)
		}
	}
	stale := requireClaim(tokenA, "extract", 1)
	if stale.Epoch != 1 || len(stale.OperationKey) != 64 || string(stale.Inputs) != `{"rows":12,"source":"fixture"}` {
		t.Fatalf("first claim: %+v", stale)
	}
	renewed, err := db.Renew(ctx, tokenA, RenewRequest{Lease: stale.LeaseRef, LeaseSeconds: 1})
	if err != nil || renewed.LeaseUntil.Before(stale.LeaseUntil) || renewed.LeaseUntil.After(stale.Deadline) {
		t.Fatalf("renew current lease: %+v, %v", renewed, err)
	}
	const records = `{"records":[{"source":"fixture","index":1}]}`
	staleResult := CompletionRequest{Lease: stale.LeaseRef, Success: true, Outputs: json.RawMessage(records)}
	if _, err := db.Complete(ctx, tokenB, staleResult); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong worker completed live lease: %v", err)
	}
	waitUntil := time.Now().Add(5 * time.Second)
	var takeover *ClaimedTask
	for takeover == nil && time.Now().Before(waitUntil) {
		takeover = claim(tokenB, "extract", 10)
		if takeover == nil {
			time.Sleep(25 * time.Millisecond)
		}
	}
	if takeover == nil || takeover.RunID != stale.RunID || takeover.Node != stale.Node ||
		takeover.Epoch != 2 || takeover.OperationKey != stale.OperationKey {
		t.Fatalf("takeover: %+v", takeover)
	}
	if _, err := db.Renew(ctx, tokenA, RenewRequest{Lease: stale.LeaseRef, LeaseSeconds: 1}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale renewal: %v", err)
	}
	if _, err := db.Complete(ctx, tokenA, staleResult); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale result: %v", err)
	}
	accepted := CompletionRequest{Lease: takeover.LeaseRef, Success: true, Outputs: json.RawMessage(records)}
	if result, err := db.Complete(ctx, tokenB, accepted); err != nil || result.Replayed || result.State != workflow.NodeSucceeded {
		t.Fatalf("complete: %+v, %v", result, err)
	}
	if result, err := db.Complete(ctx, tokenB, accepted); err != nil || !result.Replayed || result.State != workflow.NodeSucceeded {
		t.Fatalf("replay: %+v, %v", result, err)
	}
	accepted.Outputs = json.RawMessage(`{"records":[]}`)
	if _, err := db.Complete(ctx, tokenB, accepted); !errors.Is(err, ErrCompletionConflict) {
		t.Fatalf("changed completion: %v", err)
	}
	inspect := requireClaim(tokenB, "inspect", 10)
	if result := claim(tokenB, "aggregate", 10); result != nil {
		t.Fatalf("worker exceeded its only active lease slot: %+v", result)
	}
	aggregate := requireClaim(tokenA, "aggregate", 10)
	if premature := claim(tokenA, "report", 10); premature != nil {
		t.Fatalf("premature report: %+v", premature)
	}
	start := make(chan struct{})
	completed := make(chan error, 2)
	finish := func(token string, task *ClaimedTask, outputs, branch string) {
		<-start
		result, err := db.Complete(ctx, token, CompletionRequest{Lease: task.LeaseRef, Success: true, Branch: branch, Outputs: json.RawMessage(outputs)})
		if err == nil && (result.Replayed || result.State != workflow.NodeSucceeded) {
			err = errors.New("unexpected parallel completion state")
		}
		completed <- err
	}
	go finish(tokenA, aggregate, `{"count":1}`, "")
	go finish(tokenB, inspect, `{"grade":"pass"}`, "report")
	close(start)
	for range 2 {
		if err := <-completed; err != nil {
			t.Fatal(err)
		}
	}
	report := requireClaim(tokenA, "report", 10)
	if report.Epoch != 1 || string(report.Inputs) != `{"count":1,"grade":"pass"}` || claim(tokenB, "report", 10) != nil {
		t.Fatalf("single joined task: %+v", report)
	}
	if _, err := db.Complete(ctx, tokenA, CompletionRequest{Lease: report.LeaseRef, Success: true, Outputs: json.RawMessage(`{"summary":1}`)}); !errors.Is(err, ErrInvalidWorkerRequest) {
		t.Fatalf("invalid typed output: %v", err)
	}
	if _, err := db.Complete(ctx, tokenA, CompletionRequest{Lease: report.LeaseRef, Success: true, Outputs: json.RawMessage(`{"summary":"done"}`)}); err != nil {
		t.Fatal(err)
	}
	detail, err := db.GetRun(ctx, scope, trigger.Run.ID)
	if err != nil || detail.Run.State != "succeeded" || detail.Run.FinishedAt == nil {
		t.Fatalf("run completion: %+v, %v", detail.Run, err)
	}
	nodes := make(map[string]NodeRun)
	for _, node := range detail.Nodes {
		nodes[node.Key] = node
	}
	extractNode := nodes["extract"]
	if len(extractNode.Attempts) != 2 || extractNode.Attempts[0].State != "expired" || extractNode.Attempts[1].State != "succeeded" ||
		extractNode.OutputDigest == nil {
		t.Fatalf("extract history: %+v", extractNode)
	}
	reportNode := nodes["report"]
	if len(reportNode.Attempts) != 1 || len(reportNode.Dependencies) != 2 ||
		reportNode.Dependencies[0].Status != "satisfied" || reportNode.Dependencies[1].Status != "satisfied" {
		t.Fatalf("report history: %+v", reportNode)
	}
	cleanup := nodes["cleanup"]
	if cleanup.State != "skipped" || len(cleanup.Attempts) != 0 || len(cleanup.Dependencies) != 1 || cleanup.Dependencies[0].Status != "unmet" {
		t.Fatalf("cleanup history: %+v", cleanup)
	}

	trigger, err = db.Trigger(ctx, scope, TriggerRequest{DAG: version.Graph.Definition.Key, Key: "bad-batch", Kind: "manual",
		Inputs: json.RawMessage(`{"rows":12,"source":"fixture"}`)})
	if err != nil {
		t.Fatal(err)
	}
	secondExtract := requireClaim(tokenA, "extract", 10)
	if _, err := db.Complete(ctx, tokenA, CompletionRequest{Lease: secondExtract.LeaseRef, Success: true, Outputs: json.RawMessage(records)}); err != nil {
		t.Fatal(err)
	}
	inspect = requireClaim(tokenB, "inspect", 10)
	if result, err := db.Complete(ctx, tokenB, CompletionRequest{Lease: inspect.LeaseRef, FailureCode: "invalid_record"}); err != nil || result.State != workflow.NodeFailed {
		t.Fatalf("terminal inspect failure: %+v, %v", result, err)
	}
	cleanupTask := requireClaim(tokenB, "cleanup", 10)
	if _, err := db.Complete(ctx, tokenB, CompletionRequest{Lease: cleanupTask.LeaseRef, Success: true}); err != nil {
		t.Fatal(err)
	}
	aggregate = requireClaim(tokenA, "aggregate", 10)
	if _, err := db.Complete(ctx, tokenA, CompletionRequest{Lease: aggregate.LeaseRef, Success: true, Outputs: json.RawMessage(`{"count":1}`)}); err != nil {
		t.Fatal(err)
	}
	failed, err := db.GetRun(ctx, scope, trigger.Run.ID)
	if err != nil || failed.Run.State != "failed" || claim(tokenB, "report", 10) != nil {
		t.Fatalf("the failure branch did not settle as expected: %+v, %v", failed.Run, err)
	}
}

func TestMySQLWorkersBackOffAndQuarantineExternalExpiry(t *testing.T) {
	db, _ := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	scope := Scope{Workspace: "expiry-" + strings.ToLower(rand.Text()), Actor: "test-operator"}
	if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
		t.Fatal(err)
	}
	graph := workflow.Definition{Key: "shipping", Nodes: []workflow.Node{
		{Key: "prepare", Task: "prepare", Queue: "dispatch", TimeoutSeconds: 30, Effect: workflow.NoEffect,
			Retry: workflow.RetryPolicy{MaxAttempts: 2, BackoffSeconds: 1, On: []string{"temporary"}}},
		{Key: "send", Task: "send", Queue: "dispatch", TimeoutSeconds: 30, Effect: workflow.ExternalEffect,
			DependsOn: []workflow.Edge{{Node: "prepare", When: workflow.Succeeded}},
			Retry:     workflow.RetryPolicy{MaxAttempts: 2, BackoffSeconds: 1, On: []string{"temporary"}},
			Output:    map[string]workflow.ValueType{"receipt": workflow.String}},
		{Key: "notify", Task: "notify", Queue: "dispatch", TimeoutSeconds: 30, Effect: workflow.NoEffect,
			DependsOn: []workflow.Edge{{Node: "send", When: workflow.Succeeded}},
			Retry:     workflow.RetryPolicy{MaxAttempts: 1}},
	}}
	definition, err := json.Marshal(graph)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Publish(ctx, scope, 0, definition); err != nil {
		t.Fatal(err)
	}
	trigger, err := db.Trigger(ctx, scope, TriggerRequest{DAG: graph.Key, Key: "dispatch-001", Kind: "manual", Inputs: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	permissions := []WorkerPermission{{Queue: "dispatch", Task: "prepare"}, {Queue: "dispatch", Task: "send"}, {Queue: "dispatch", Task: "notify"}}
	first, err := db.RegisterWorker(ctx, scope, "first", 1, permissions)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.RegisterWorker(ctx, scope, "second", 1, permissions)
	if err != nil {
		t.Fatal(err)
	}
	claim := func(token, task string, lease int) *ClaimedTask {
		t.Helper()
		item, err := db.Claim(ctx, token, ClaimRequest{Tasks: []string{task}, LeaseSeconds: lease})
		if err != nil {
			t.Fatal(err)
		}
		return item
	}
	initial := claim(first.Token, "prepare", 10)
	if initial == nil || initial.Epoch != 1 {
		t.Fatalf("first attempt: %+v", initial)
	}
	retry := CompletionRequest{Lease: initial.LeaseRef, FailureCode: "temporary"}
	if result, err := db.Complete(ctx, first.Token, retry); err != nil || result.State != workflow.NodeRetryWait {
		t.Fatalf("schedule retry: %+v, %v", result, err)
	}
	if item := claim(second.Token, "prepare", 10); item != nil {
		t.Fatalf("claimed during backoff: %+v", item)
	}
	var prepared *ClaimedTask
	for limit := time.Now().Add(5 * time.Second); prepared == nil && time.Now().Before(limit); {
		time.Sleep(25 * time.Millisecond)
		prepared = claim(second.Token, "prepare", 10)
	}
	if prepared == nil || prepared.Epoch != 2 || prepared.OperationKey != initial.OperationKey {
		t.Fatalf("available retry: %+v", prepared)
	}
	if _, err := db.Complete(ctx, second.Token, CompletionRequest{Lease: prepared.LeaseRef, Success: true}); err != nil {
		t.Fatal(err)
	}
	external := claim(first.Token, "send", 1)
	if external == nil || external.Effect != workflow.ExternalEffect {
		t.Fatalf("external task: %+v", external)
	}
	for limit := time.Now().Add(5 * time.Second); ; {
		if _, err := db.SweepExecution(ctx, 64); err != nil {
			t.Fatal(err)
		}
		detail, err := db.GetRun(ctx, scope, trigger.Run.ID)
		if err != nil {
			t.Fatal(err)
		}
		if detail.Run.State == "action_required" {
			send := detail.Nodes[2]
			if send.Key != "send" || send.State != "action_required" || len(send.Attempts) != 1 || send.Attempts[0].State != "expired" {
				t.Fatalf("external task evidence: %+v", send)
			}
			break
		}
		if time.Now().After(limit) {
			t.Fatalf("expired external action did not enter action_required: %+v", detail.Run)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if _, err := db.Complete(ctx, first.Token, CompletionRequest{Lease: external.LeaseRef, Success: true,
		Outputs: json.RawMessage(`{"receipt":"unverified"}`)}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired external completion: %v", err)
	}
	if claim(second.Token, "send", 10) != nil || claim(second.Token, "notify", 10) != nil {
		t.Fatal("external task repeated or downstream released without evidence")
	}
	resolution := ResolveRequest{Key: "receipt-9020", Epoch: external.Epoch, Evidence: "Carrier receipt 9020 from dispatch ledger",
		State: workflow.NodeSucceeded, Outputs: json.RawMessage(`{"receipt":"shp-9020"}`)}
	wrongEpoch := resolution
	wrongEpoch.Epoch++
	if _, err := db.ResolveExternal(ctx, scope, external.RunID, external.Node, wrongEpoch); !errors.Is(err, ErrRunControlConflict) {
		t.Fatalf("reconcile wrong epoch: %v", err)
	}
	badType := resolution
	badType.Outputs = json.RawMessage(`{"receipt":9020}`)
	if _, err := db.ResolveExternal(ctx, scope, external.RunID, external.Node, badType); !errors.Is(err, ErrInvalidWorkerRequest) {
		t.Fatalf("reconcile invalid output: %v", err)
	}
	resolved, err := db.ResolveExternal(ctx, scope, external.RunID, external.Node, resolution)
	if err != nil || resolved.Replayed || resolved.OperationKey != external.OperationKey || resolved.Actor != scope.Actor {
		t.Fatalf("resolve: %+v, %v", resolved, err)
	}
	replayed, err := db.ResolveExternal(ctx, scope, external.RunID, external.Node, resolution)
	if err != nil || !replayed.Replayed || replayed.ResolvedAt != resolved.ResolvedAt {
		t.Fatalf("resolve replay: %+v, %v", replayed, err)
	}
	resolution.Evidence = "Changed"
	if _, err := db.ResolveExternal(ctx, scope, external.RunID, external.Node, resolution); !errors.Is(err, ErrRunControlConflict) {
		t.Fatalf("changed evidence: %v", err)
	}
	notify := claim(second.Token, "notify", 10)
	if notify == nil || notify.RunID != external.RunID || claim(first.Token, "send", 10) != nil {
		t.Fatalf("resolution did not release only downstream: %+v", notify)
	}
	if _, err := db.Complete(ctx, second.Token, CompletionRequest{Lease: notify.LeaseRef, Success: true}); err != nil {
		t.Fatal(err)
	}
	detail, err := db.GetRun(ctx, scope, external.RunID)
	if err != nil || detail.Run.State != "succeeded" || detail.Nodes[2].Resolution == nil ||
		detail.Nodes[2].Resolution.Evidence != resolved.Evidence || detail.Nodes[2].Attempts[0].State != "expired" {
		t.Fatalf("read reconciliation history: %+v, %v", detail, err)
	}
}

func TestMySQLCanceledExternalActionPreservesUncertaintyUntilResolved(t *testing.T) {
	db, _ := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	scope := Scope{Workspace: "cancel-ext-" + strings.ToLower(rand.Text()), Actor: "operator"}
	if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
		t.Fatal(err)
	}
	graph := []byte(`{"key":"external-cancel","nodes":[
		{"key":"send","task":"send","queue":"dispatch","effect":"external","timeout_seconds":30,"retry":{"max_attempts":1}},
		{"key":"notify","task":"notify","queue":"dispatch","effect":"none","timeout_seconds":30,"retry":{"max_attempts":1},
		 "depends_on":[{"node":"send","when":"finished"}]}]}`)
	if _, err := db.Publish(ctx, scope, 0, graph); err != nil {
		t.Fatal(err)
	}
	trigger, err := db.Trigger(ctx, scope, TriggerRequest{DAG: "external-cancel", Key: "fixture", Kind: "manual", Inputs: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	registration, err := db.RegisterWorker(ctx, scope, "sender", 1, []WorkerPermission{
		{Queue: "dispatch", Task: "send"}, {Queue: "dispatch", Task: "notify"},
	})
	if err != nil {
		t.Fatal(err)
	}
	task, err := db.Claim(ctx, registration.Token, ClaimRequest{Tasks: []string{"send"}, LeaseSeconds: 30})
	if err != nil || task == nil || task.RunID != trigger.Run.ID {
		t.Fatalf("claim: %+v, %v", task, err)
	}
	control, err := db.CancelRun(ctx, scope, task.RunID, CancelRequest{Key: "stop-1", Reason: "Wrong shipment", GraceSeconds: 5})
	if err != nil || control.FinishedAt != nil {
		t.Fatalf("cancel: %+v, %v", control, err)
	}
	ack, err := db.AcknowledgeCancel(ctx, registration.Token, task.LeaseRef)
	if err != nil || ack.State != workflow.NodeActionRequired {
		t.Fatalf("external worker cancellation: %+v, %v", ack, err)
	}
	detail, err := db.GetRun(ctx, scope, task.RunID)
	if err != nil || detail.Run.State != "action_required" || detail.Cancellation.FinishedAt != nil ||
		detail.Nodes[0].Key != "notify" || detail.Nodes[0].State != "canceled" {
		t.Fatalf("held external status: %+v, %v", detail, err)
	}
	if result, err := db.ResolveExternal(ctx, scope, task.RunID, "send", ResolveRequest{Key: "invoice-21", Evidence: "Confirmed rejected shipment 21",
		Epoch: task.Epoch, State: workflow.NodeFailed}); err != nil || result.State != workflow.NodeFailed {
		t.Fatalf("manual result: %+v, %v", result, err)
	}
	detail, err = db.GetRun(ctx, scope, task.RunID)
	if err != nil || detail.Run.State != "canceled" || detail.Cancellation.FinishedAt == nil ||
		detail.Nodes[0].State != "canceled" || detail.Nodes[1].State != "failed" || detail.Nodes[1].Resolution == nil {
		t.Fatalf("canceled run reopened on resolution: %+v, %v", detail, err)
	}
}
