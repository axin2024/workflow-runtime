package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/axin2024/workflow-runtime/internal/process"
)

func TestMySQLProcessMigrationSelectedWaitsAndOlderSignals(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := t.Context()
	scope, version, buyer, _ := approvalFixture(t, db)
	const newStep = "credit-review"
	old := make([]ProcessRun, 10)
	for i := range old {
		old[i], _ = newApprovalRun(t, db, scope, version, fmt.Sprintf("migration-a11-%d", i))
	}
	wait := processTestWait(t, db, old[0])
	queue, err := db.ProcessTaskQueue(ctx, buyer)
	if err != nil || len(queue) != 10 {
		t.Fatalf("inbox: %d %v", len(queue), err)
	}
	task := queue[0]
	if _, err = db.ChangeProcessTask(ctx, buyer, task.ID, "claim-legacy-form", ProcessTaskInput{Action: "claim", Revision: 1, ClaimSecs: 3600}); err != nil {
		t.Fatal(err)
	}
	version.Definition.Start = newStep
	version.Definition.Steps[0].Key = newStep
	version2, err := db.PublishProcess(ctx, scope, version.Key, 1, version.Definition)
	if err != nil || version2.Revision != 2 {
		t.Fatalf("publish v2: %+v %v", version2, err)
	}
	spec := ProcessMigrationSpec{Process: version.Key, Source: 1, Target: 2,
		Runs: []int64{old[4].ID, old[0].ID, old[9].ID}, Mapping: map[string]string{"credit-check": newStep}}
	preview, err := db.PreviewProcessMigration(ctx, scope, "three-of-ten", spec)
	if err != nil || len(preview.Items) != 3 || preview.ApplyAt != nil || len(preview.Skipped) != 0 || preview.Items[0].State != "ready" {
		t.Fatalf("preview: %+v %v", preview, err)
	}
	if state, err := db.ProcessRun(ctx, scope, old[0].ID); err != nil || state.Revision != 1 {
		t.Fatalf("preview modified source: %+v %v", state, err)
	}
	spec.Runs = []int64{old[0].ID, old[9].ID, old[4].ID}
	if same, err := db.PreviewProcessMigration(ctx, scope, "three-of-ten", spec); err != nil || same.ID != preview.ID {
		t.Fatalf("preview retry: %+v %v", same, err)
	}
	spec.Runs = []int64{old[1].ID}
	if _, err = db.PreviewProcessMigration(ctx, scope, "three-of-ten", spec); !errors.Is(err, ErrProcessConflict) {
		t.Fatalf("preview key overwrite succeeded: %v", err)
	}
	if _, err = db.ApplyProcessMigration(ctx, newTestWorkspace(t, db), preview.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other workspace applied batch: %v", err)
	}
	result, err := db.ApplyProcessMigration(ctx, scope, preview.ID)
	if err != nil || result.ApplyAt == nil || result.ApplyBy == nil || *result.ApplyBy != scope.Actor {
		t.Fatalf("apply: %+v %v", result, err)
	}
	if again, err := db.ApplyProcessMigration(ctx, scope, preview.ID); err != nil || again.ApplyAt == nil || !again.ApplyAt.Equal(*result.ApplyAt) {
		t.Fatalf("apply retry: %+v %v", again, err)
	}
	for i, original := range old {
		state, err := db.ProcessRun(ctx, scope, original.ID)
		want, step := 1, "credit-check"
		if i == 0 || i == 4 || i == 9 {
			want, step = 2, newStep
		}
		if err != nil || state.Revision != want || state.StartRevision != 1 || state.Step != step || state.Status != "waiting" || state.Transition != uint64(want) {
			t.Fatalf("selected migration i=%d: %+v %v", i, state, err)
		}
		if i == 0 {
			mapped := processTestWait(t, db, state)
			if mapped.ID == wait.ID || !mapped.Deadline.Equal(wait.Deadline) {
				t.Fatalf("migration refreshed deadline: %+v -> %+v", wait, mapped)
			}
		}
	}
	if _, err = db.ChangeProcessTask(ctx, buyer, task.ID, "old-form", ProcessTaskInput{Action: "approve", Revision: 2}); !errors.Is(err, ErrProcessConflict) {
		t.Fatalf("old task could approve after migration: %v", err)
	}
	var oldEvents, newEvents int
	err = db.db.QueryRowContext(ctx, `SELECT COUNT(CASE WHEN definition_rev=1 THEN 1 END),COUNT(CASE WHEN definition_rev=2 THEN 1 END)
		FROM process_events WHERE run_id=? AND kind<>'task_input_rejected'`, old[0].ID).Scan(&oldEvents, &newEvents)
	if err != nil || oldEvents < 3 || newEvents < 2 {
		t.Fatalf("incomplete old/new timeline: %d %d %v", oldEvents, newEvents, err)
	}
	define := func(key string) process.Definition {
		return process.Definition{Start: key, Steps: []process.Step{
			{Key: key, Kind: process.Signal, SignalKind: "arrive", AfterSeconds: 3600, Next: "done", OnTimeout: "late"},
			{Key: "done", Kind: process.End, Result: "succeeded"}, {Key: "late", Kind: process.End, Result: "failed"},
		}}
	}
	if _, err = db.PublishProcess(ctx, scope, "shipment", 0, define("confirm-arrival")); err != nil {
		t.Fatal(err)
	}
	original, err := db.StartProcess(ctx, scope, "shipment", NewProcessRun{Business: "D-347"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.PublishProcess(ctx, scope, "shipment", 1, define("arrive-v2")); err != nil {
		t.Fatal(err)
	}
	missing, err := db.PreviewProcessMigration(ctx, scope, "missing-mapping", ProcessMigrationSpec{Process: "shipment", Source: 1, Target: 2, Runs: []int64{original.ID}})
	if err != nil || len(missing.Skipped) != 1 || missing.Items[0].Reason != "missing_mapping" {
		t.Fatalf("missing mapping preview: %+v %v", missing, err)
	}
	if _, err = db.ApplyProcessMigration(ctx, scope, missing.ID); err != nil {
		t.Fatal(err)
	}
	receipt, err := db.SubmitProcessSignal(ctx, scope, original.ID, "arrival-request", ProcessSignalInput{
		Event: "arrival-event", Kind: "arrive", Correlation: original.Business, Payload: json.RawMessage(`{"quantity":21}`),
	})
	if err != nil || receipt.Outcome != "consumed" {
		t.Fatalf("old signal broken: %+v %v", receipt, err)
	}
	if state, err := db.ProcessRun(ctx, scope, original.ID); err != nil || state.Revision != 1 || state.Status != "succeeded" {
		t.Fatalf("unmapped run: %+v %v", state, err)
	}
	t.Logf("batch=%d selected=%d migrated=%d untouched=7 missing_batch=%d original_signal=%d", result.ID, len(result.Items), len(result.Items)-len(result.Skipped), missing.ID, receipt.ID)
}
