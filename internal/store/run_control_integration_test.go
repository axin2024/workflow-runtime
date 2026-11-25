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

func TestMySQLCancelPreservesResultsAndSettlesUnresponsiveWorker(t *testing.T) {
	db, _ := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	scope := Scope{Workspace: "cancel-" + strings.ToLower(rand.Text()), Actor: "on-call"}
	foreign := Scope{Workspace: scope.Workspace + "-foreign", Actor: scope.Actor}
	for _, tenant := range []Scope{scope, foreign} {
		if err := db.CreateWorkspace(ctx, tenant.Workspace); err != nil {
			t.Fatal(err)
		}
	}
	source, err := os.ReadFile("../../examples/diamond.json")
	if err != nil {
		t.Fatal(err)
	}
	version, err := db.Publish(ctx, scope, 0, source)
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := db.Trigger(ctx, scope, TriggerRequest{DAG: version.Graph.Definition.Key, Key: "stop-fixture", Kind: "manual",
		Inputs: json.RawMessage(`{"rows":1,"source":"fixture"}`)})
	if err != nil {
		t.Fatal(err)
	}
	runID := trigger.Run.ID
	var permissions []WorkerPermission
	for _, node := range version.Graph.Definition.Nodes {
		permissions = append(permissions, WorkerPermission{Queue: node.Queue, Task: node.Task})
	}
	first, err := db.RegisterWorker(ctx, scope, "responsive", 1, permissions)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.RegisterWorker(ctx, scope, "unresponsive", 1, permissions)
	if err != nil {
		t.Fatal(err)
	}
	requireClaim := func(token, task string) LeaseRef {
		t.Helper()
		result, err := db.Claim(ctx, token, ClaimRequest{Tasks: []string{task}, LeaseSeconds: 15})
		if err != nil || result == nil || result.RunID != runID {
			t.Fatalf("claim %s: %+v, %v", task, result, err)
		}
		return result.LeaseRef
	}
	extract := requireClaim(first.Token, "extract")
	extractResult := CompletionRequest{Lease: extract, Success: true, Outputs: json.RawMessage(`{"records":[{"source":"fixture","index":1}]}`)}
	if _, err := db.Complete(ctx, first.Token, extractResult); err != nil {
		t.Fatal(err)
	}
	aggregate := requireClaim(first.Token, "aggregate")
	inspect := requireClaim(second.Token, "inspect")
	request := CancelRequest{Key: "incident-25", Reason: "Source correction", GraceSeconds: 2}
	if _, err := db.CancelRun(ctx, foreign, runID, request); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign workspace cancellation: %v", err)
	}
	record, err := db.CancelRun(ctx, scope, runID, request)
	if err != nil || record.Replayed || record.Actor != scope.Actor || record.FinishedAt != nil ||
		record.Deadline.Sub(record.RequestedAt) != 2*time.Second {
		t.Fatalf("cancel receipt: %+v, %v", record, err)
	}
	if retry, err := db.CancelRun(ctx, scope, runID, request); err != nil || !retry.Replayed || retry.Deadline != record.Deadline {
		t.Fatalf("cancel replay: %+v, %v", retry, err)
	}
	changed := request
	changed.Reason = "different reason"
	if _, err := db.CancelRun(ctx, scope, runID, changed); !errors.Is(err, ErrRunControlConflict) {
		t.Fatalf("changed cancellation: %v", err)
	}
	renewal, err := db.Renew(ctx, first.Token, RenewRequest{Lease: aggregate, LeaseSeconds: 120})
	if err != nil || renewal.CancelDeadline == nil || *renewal.CancelDeadline != record.Deadline || renewal.LeaseUntil.After(record.Deadline) {
		t.Fatalf("renewal cancellation signal: %+v, %v", renewal, err)
	}
	result := CompletionRequest{Lease: aggregate, Success: true, Outputs: json.RawMessage(`{"count":1}`)}
	if _, err := db.Complete(ctx, first.Token, result); !errors.Is(err, ErrCancellationRequested) {
		t.Fatalf("new result during cancellation: %v", err)
	}
	if receipt, err := db.Complete(ctx, first.Token, extractResult); err != nil || !receipt.Replayed || receipt.State != workflow.NodeSucceeded {
		t.Fatalf("preexisting completion receipt: %+v, %v", receipt, err)
	}
	if receipt, err := db.AcknowledgeCancel(ctx, first.Token, aggregate); err != nil || receipt.Replayed || receipt.State != workflow.NodeCanceled {
		t.Fatalf("worker cancellation acknowledgement: %+v, %v", receipt, err)
	}
	if receipt, err := db.AcknowledgeCancel(ctx, first.Token, aggregate); err != nil || !receipt.Replayed {
		t.Fatalf("worker cancellation acknowledgement replay: %+v, %v", receipt, err)
	}
	if task, err := db.Claim(ctx, first.Token, ClaimRequest{Tasks: []string{"cleanup", "report"}, LeaseSeconds: 10}); err != nil || task != nil {
		t.Fatalf("successor released during cancellation: %+v, %v", task, err)
	}
	for limit := time.Now().Add(5 * time.Second); ; {
		sweep, err := db.SweepExecution(ctx, 64)
		if err != nil {
			t.Fatal(err)
		}
		if sweep.CancelTimedOut == 1 {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("unresponsive worker was not settled by sweep")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := db.AcknowledgeCancel(ctx, second.Token, inspect); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late cancellation acknowledgement: %v", err)
	}
	detail, err := db.GetRun(ctx, scope, runID)
	if err != nil || detail.Run.State != "canceled" || detail.Run.FinishedAt == nil ||
		detail.Cancellation == nil || detail.Cancellation.FinishedAt == nil {
		t.Fatalf("final cancel detail: %+v, %v", detail, err)
	}
	nodes := make(map[string]NodeRun)
	for _, node := range detail.Nodes {
		nodes[node.Key] = node
	}
	if nodes["extract"].State != "succeeded" || nodes["extract"].OutputDigest == nil ||
		len(nodes["aggregate"].Attempts) != 1 || nodes["aggregate"].Attempts[0].State != "canceled" ||
		len(nodes["inspect"].Attempts) != 1 || nodes["inspect"].Attempts[0].State != "cancel_timeout" {
		t.Fatalf("terminal evidence after cancel: %+v", nodes)
	}
	for _, key := range []string{"report", "cleanup"} {
		node := nodes[key]
		if node.State != "canceled" || len(node.Dependencies) == 0 || node.Dependencies[0].Reason != "run_canceled" || len(node.Attempts) != 0 {
			t.Fatalf("canceled downstream node: %+v", node)
		}
	}
}
