package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

func TestMySQLProcessCompensationReceiptAndUnknownOutcome(t *testing.T) {
	db, dsn := openTestStore(t)
	ctx := t.Context()
	scope := newTestWorkspace(t, db)
	scope.Actor = "purchase-operator"
	adapter := Scope{Workspace: scope.Workspace, Actor: "purchasing-adapter"}
	const definition = "purchase"
	_, err := db.PublishProcess(ctx, scope, definition, 0, process.Definition{
		Start: "reserve", Operations: []string{"freeze", "release", "place", "withdraw"}, Steps: []process.Step{
			{Key: "reserve", Kind: process.Action, Operation: "freeze", Compensation: "release", Next: "order", OnFailure: "failed"},
			{Key: "order", Kind: process.Action, Operation: "place", Compensation: "withdraw", Next: "collect", OnFailure: "failed"},
			{Key: "collect", Kind: process.Signal, SignalKind: "delivery", AfterSeconds: 3600, Next: "done", OnTimeout: "failed"},
			{Key: "done", Kind: process.End, Result: "succeeded"}, {Key: "failed", Kind: process.End, Result: "failed"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := db.StartProcess(ctx, scope, definition, NewProcessRun{Business: "P-7", Input: json.RawMessage(`{"currency":"CNY","units":3}`)})
	if err != nil || run.Status != "running" {
		t.Fatalf("start: %+v %v", run, err)
	}
	claim := func(s *Store, name string) ProcessAction {
		t.Helper()
		out, err := s.ClaimProcessAction(ctx, adapter, definition, name, time.Minute)
		if err != nil || out.Run != run.ID || out.Status != "leased" {
			t.Fatalf("claim %s: %+v %v", name, out, err)
		}
		return out
	}
	report := func(s *Store, action ProcessAction, request, outcome, receipt string) ProcessAction {
		t.Helper()
		in := ProcessActionReport{Epoch: action.Epoch, Outcome: outcome, Receipt: receipt, Evidence: json.RawMessage(`{"source":"test-adapter"}`)}
		out, err := s.ReportProcessAction(ctx, adapter, definition, action.Operation, action.ID, request, in)
		if err != nil || out.ID != action.ID {
			t.Fatalf("report %s: %+v %v", action.Operation, out, err)
		}
		return out
	}
	forbid := func(s *Store, name string) {
		t.Helper()
		if out, err := s.ClaimProcessAction(ctx, adapter, definition, name, time.Minute); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unreleased operation %s: %+v %v", name, out, err)
		}
	}
	forbid(db, "place")
	freeze := claim(db, "freeze")
	report(db, freeze, "freeze-result", "succeeded", "credit-341")
	place := claim(db, "place")
	report(db, place, "order-result", "succeeded", "purchase-519")
	if parked, err := db.ProcessRun(ctx, scope, run.ID); err != nil || parked.Step != "collect" || parked.Status != "waiting" {
		t.Fatalf("wait for delivery: %+v %v", parked, err)
	}
	if cancelling, err := db.CancelProcess(ctx, scope, run.ID, "cancel-519"); err != nil || cancelling.Status != "cancelling" {
		t.Fatalf("begin compensation: %+v %v", cancelling, err)
	}
	forbid(db, "release")
	withdraw := claim(db, "withdraw")
	if withdraw.OriginalID == nil || *withdraw.OriginalID != place.ID || withdraw.OriginalReceipt == nil || *withdraw.OriginalReceipt != "purchase-519" {
		t.Fatalf("withdraw lost original identity: %+v", withdraw)
	}
	unknown := report(db, withdraw, "withdraw-timeout", "unknown", "")
	if unknown.Status != "unknown" || unknown.ResolutionOwner == nil || *unknown.ResolutionOwner != scope.Actor {
		t.Fatalf("unknown owner: %+v", unknown)
	}
	restarted, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err = restarted.ScanProcessCompensations(ctx, 32); err != nil {
		t.Fatal(err)
	}
	forbid(restarted, "release")
	forbid(restarted, "withdraw")
	if held, err := restarted.ProcessRun(ctx, scope, run.ID); err != nil || held.Status != "reconcile" {
		t.Fatalf("operator interruption not retained: %+v %v", held, err)
	}
	late, err := restarted.SubmitProcessSignal(ctx, scope, run.ID, "delivery-during-cancel", ProcessSignalInput{
		Event: "delivery-519", Correlation: "P-7", Kind: "delivery", Payload: json.RawMessage(`{"units":3}`),
	})
	if err != nil || late.Outcome != "late" || late.ConsumedBy != nil {
		t.Fatalf("cancelled wait accepted a new delivery: %+v %v", late, err)
	}
	resolution := ProcessActionResolution{Key: withdraw.Key, Decision: "not_applied", Reason: "Supplier lookup confirms the order is active",
		Evidence: json.RawMessage(`{"supplier_ticket":"T17"}`)}
	if _, err = restarted.ResolveProcessAction(ctx, Scope{Workspace: scope.Workspace, Actor: "other-operator"}, withdraw.ID, "lookup-T17", resolution); !errors.Is(err, ErrProcessConflict) {
		t.Fatalf("unassigned operator resolved: %v", err)
	}
	if ready, err := restarted.ResolveProcessAction(ctx, scope, withdraw.ID, "lookup-T17", resolution); err != nil || ready.Status != "ready" {
		t.Fatalf("recover after lookup: %+v %v", ready, err)
	}
	reclaim := claim(restarted, "withdraw")
	if reclaim.ID != withdraw.ID || reclaim.Key != withdraw.Key || reclaim.Epoch != withdraw.Epoch+1 {
		t.Fatalf("retry changed external identity: %+v -> %+v", withdraw, reclaim)
	}
	stale := ProcessActionReport{Epoch: withdraw.Epoch, Outcome: "succeeded", Receipt: "stale-callback"}
	if _, err = restarted.ReportProcessAction(ctx, adapter, definition, "withdraw", withdraw.ID, "stale-callback", stale); !errors.Is(err, ErrProcessConflict) {
		t.Fatalf("old action lease replaced result: %v", err)
	}
	conf := report(restarted, reclaim, "withdraw-receipt", "succeeded", "withdraw-519")
	release := claim(restarted, "release")
	if release.OriginalID == nil || *release.OriginalID != freeze.ID || release.OriginalReceipt == nil || *release.OriginalReceipt != "credit-341" {
		t.Fatalf("release lost original receipt: %+v", release)
	}
	report(restarted, release, "release-receipt", "succeeded", "unfreeze-341")
	if same := report(restarted, reclaim, "withdraw-receipt", "succeeded", "withdraw-519"); !same.ConfirmedAt.Equal(*conf.ConfirmedAt) {
		t.Fatalf("duplicate receipt changed confirmation: %+v %+v", same, conf)
	}
	if done, err := restarted.ProcessRun(ctx, scope, run.ID); err != nil || done.Status != "cancelled" || done.Transition != 4 {
		t.Fatalf("final: %+v %v", done, err)
	}
	var actions, reversed, unknownEvents int
	err = db.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM process_actions WHERE run_id=?),
		(SELECT COUNT(*) FROM process_actions WHERE run_id=? AND direction='reverse' AND status='succeeded'),
		(SELECT COUNT(*) FROM process_events WHERE run_id=? AND kind='action_reported' AND request_key='withdraw-timeout')`,
		run.ID, run.ID, run.ID).Scan(&actions, &reversed, &unknownEvents)
	if err != nil || actions != 4 || reversed != 2 || unknownEvents != 1 {
		t.Fatalf("persisted evidence actions=%d reversed=%d unknown=%d %v", actions, reversed, unknownEvents, err)
	}
	t.Logf("run=%d withdraw=%d release=%d owner=%s stable_key=%s confirmed_reversals=%d", run.ID, withdraw.ID, release.ID, *unknown.ResolutionOwner, withdraw.Key, reversed)
}
