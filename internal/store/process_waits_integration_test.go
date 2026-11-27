package store

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

func TestMySQLProcessDurableEarlySignalAndTimeout(t *testing.T) {
	db, dsn := openTestStore(t)
	ctx := t.Context()
	scope := newTestWorkspace(t, db)
	version, err := db.PublishProcess(ctx, scope, "purchasing", 0, process.Definition{
		Start: "release",
		Steps: []process.Step{
			{Key: "release", Kind: process.Timer, Next: "received", AfterSeconds: 3600},
			{Key: "received", Kind: process.Signal, Next: "done", OnTimeout: "missing", SignalKind: "goods_received", AfterSeconds: 3600},
			{Key: "done", Kind: process.End, Result: "succeeded"},
			{Key: "missing", Kind: process.End, Result: "failed"},
		},
	})
	if err != nil || version.Revision != 1 {
		t.Fatalf("publish: %+v %v", version, err)
	}
	run, err := db.StartProcess(ctx, scope, version.Key, NewProcessRun{Business: "PO-7", Input: json.RawMessage(`{"warehouse":"west","quantity":3}`)})
	if err != nil || run.Status != "waiting" || run.Step != "release" {
		t.Fatalf("start: %+v %v", run, err)
	}
	replay, err := db.StartProcess(ctx, scope, version.Key, NewProcessRun{Business: "PO-7", Input: json.RawMessage(`{"quantity":3,"warehouse":"west"}`)})
	if err != nil || replay.ID != run.ID {
		t.Fatalf("start replay: %+v %v", replay, err)
	}
	connector := Scope{Workspace: scope.Workspace, Actor: "receiving"}
	signal := ProcessSignalInput{Event: "E3", Correlation: "PO-7", Kind: "goods_received", Payload: json.RawMessage(`{"qty":3}`)}
	first, err := db.SubmitProcessSignal(ctx, connector, run.ID, "event-3", signal)
	if err != nil || first.Outcome != "waiting_for_match" || first.ConsumedBy != nil {
		t.Fatalf("early signal: %+v %v", first, err)
	}
	duplicate, err := db.SubmitProcessSignal(ctx, connector, run.ID, "delivery-retry", signal)
	if err != nil || duplicate.ID != first.ID {
		t.Fatalf("duplicate event id: %+v %v", duplicate, err)
	}
	wrong := signal
	wrong.Payload = json.RawMessage(`{"qty":4}`)
	if _, err = db.SubmitProcessSignal(ctx, connector, run.ID, "delivery-retry", wrong); !errors.Is(err, ErrProcessConflict) {
		t.Fatalf("idempotent collision: %v", err)
	}
	if _, err = db.SubmitProcessSignal(ctx, connector, run.ID, "collision", wrong); !errors.Is(err, ErrProcessConflict) {
		t.Fatalf("event collision: %v", err)
	}
	outside := newTestWorkspace(t, db)
	if _, err = db.SubmitProcessSignal(ctx, outside, run.ID, "foreign", signal); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other workspace accessed signal: %v", err)
	}
	restarted, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	wait := processTestWait(t, restarted, run)
	won, err := restarted.observeProcessTimer(ctx, timerCandidate{run.workspace, run.ID, wait.ID}, wait.Deadline)
	if err != nil || !won {
		t.Fatalf("scan due after restart: %v %v", won, err)
	}
	won, err = restarted.observeProcessTimer(ctx, timerCandidate{run.workspace, run.ID, wait.ID}, wait.Deadline)
	if err != nil || won {
		t.Fatalf("timer replay: %v %v", won, err)
	}
	final, err := restarted.ProcessRun(ctx, scope, run.ID)
	if err != nil || final.Status != "succeeded" || final.Step != "done" || final.Transition != 3 {
		t.Fatalf("original event failed to advance exactly once: %+v %v", final, err)
	}
	consumed, err := restarted.SubmitProcessSignal(ctx, connector, run.ID, "event-3", signal)
	var payload map[string]int
	payloadErr := json.Unmarshal(consumed.Payload, &payload)
	if err != nil || consumed.ID != first.ID || consumed.Outcome != "consumed" || consumed.ConsumedAt == nil || consumed.MatchedAt == nil ||
		consumed.ConsumedBy == nil || *consumed.ConsumedBy == wait.ID || payloadErr != nil || payload["qty"] != 3 {
		t.Fatalf("consumed receipt: %+v %v", consumed, err)
	}
	lateInput := signal
	lateInput.Event = "E4"
	late, err := restarted.SubmitProcessSignal(ctx, connector, run.ID, "late-event", lateInput)
	if err != nil || late.Outcome != "late" {
		t.Fatalf("late signal: %+v %v", late, err)
	}
	var waits, conflicts, finished int
	err = db.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM process_waits WHERE run_id=? AND status IN ('accepted','timeout')),
		(SELECT COUNT(*) FROM process_events WHERE run_id=? AND kind='signal_conflict'),
		(SELECT COUNT(*) FROM process_events WHERE run_id=? AND kind='run_finished')`, run.ID, run.ID, run.ID).Scan(&waits, &conflicts, &finished)
	if err != nil || waits != 2 || conflicts != 2 || finished != 1 {
		t.Fatalf("durable evidence waits=%d conflicts=%d terminal=%d %v", waits, conflicts, finished, err)
	}
	t.Logf("process=%d first_receipt=%d consumed_by=%d completed_rev=%d late_receipt=%d", run.ID, first.ID, *consumed.ConsumedBy, final.Transition, late.ID)
}

func newTestWorkspace(t *testing.T, db *Store) Scope {
	t.Helper()
	scope := Scope{Workspace: "process-" + strings.ToLower(rand.Text()), Actor: "operator"}
	if err := db.CreateWorkspace(t.Context(), scope.Workspace); err != nil {
		t.Fatal(err)
	}
	return scope
}

func processTestWait(t *testing.T, db *Store, run ProcessRun) ProcessWait {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	wait, err := currentProcessWait(ctx, tx, run)
	if err != nil {
		t.Fatal(err)
	}
	return wait
}
