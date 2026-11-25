package store

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestMySQLConcurrentSchedulersCatchUpAndRespectLimits(t *testing.T) {
	db, dsn := openTestStore(t)
	second, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	key := strings.ToLower(rand.Text())
	scope := Scope{Workspace: "schedule-" + key, Actor: "author"}
	if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../examples/diamond.json")
	if err != nil {
		t.Fatal(err)
	}
	graph, err := db.Publish(ctx, scope, 0, raw)
	if err != nil {
		t.Fatal(err)
	}
	dag := graph.Graph.Definition.Key
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	spec := ScheduleSpec{Key: "minute-dispatch", Cron: "* * * * *", Timezone: "Asia/Shanghai", Misfire: "catch_up",
		MaxActiveRuns: 3, Enabled: true, StartAfter: base, Inputs: []byte(`{"rows":7,"source":"schedule-fixture"}`)}
	config, err := db.PutSchedule(ctx, scope, dag, 0, spec)
	if err != nil || config.Generation != 1 || config.NextAt == nil || !config.NextAt.Equal(base.Add(time.Minute)) {
		t.Fatalf("schedule create: %+v, %v", config, err)
	}
	_, err = second.PutSchedule(ctx, scope, dag, 0, spec)
	var conflict *ScheduleConflict
	if !errors.As(err, &conflict) || conflict.Generation != 1 {
		t.Fatalf("stale schedule update: %v", err)
	}
	type response struct {
		result ScheduleScan
		err    error
	}
	start := make(chan struct{})
	completed := make(chan response, 2)
	for i, store := range []*Store{db, second} {
		go func() {
			<-start
			actor := []string{"scanner-a", "scanner-b"}[i]
			result, err := store.ScanDue(ctx, actor, base.Add(3*time.Minute), 32)
			completed <- response{result, err}
		}()
	}
	close(start)
	var runIDs []int64
	for range 2 {
		scan := <-completed
		if scan.err != nil {
			t.Fatal(scan.err)
		}
		for _, outcome := range scan.result.Outcomes {
			if outcome.ScheduleID == config.ID {
				runIDs = append(runIDs, outcome.RunIDs...)
			}
		}
	}
	if len(runIDs) != 3 {
		t.Fatalf("wanted three catch-up runs, got %v", runIDs)
	}
	for index, id := range runIDs {
		detail, err := db.GetRun(ctx, scope, id)
		instant := base.Add(time.Duration(index+1) * time.Minute)
		if err != nil || detail.Run.LogicalAt != instant || detail.Run.Revision != 1 || detail.Run.Kind != "scheduled" ||
			detail.Run.TriggerKey != scheduledTriggerKey(config.ID, instant) || detail.Run.ScheduleID == nil || *detail.Run.ScheduleID != config.ID || len(detail.Nodes) != 5 {
			t.Fatalf("wrong scheduled run %d: %+v, %v", index, detail, err)
		}
	}
	var count int
	err = db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE schedule_id = ?`, config.ID).Scan(&count)
	if err != nil || count != 3 {
		t.Fatalf("stored run count = %d, %v", count, err)
	}
	scan, err := db.ScanDue(ctx, "scanner-a", base.Add(4*time.Minute), 32)
	if err != nil || len(scan.Outcomes) != 1 || scan.Outcomes[0].Status != "active_limit" || len(scan.Outcomes[0].RunIDs) != 0 ||
		scan.Outcomes[0].NextAt == nil || !scan.Outcomes[0].NextAt.Equal(base.Add(4*time.Minute)) {
		t.Fatalf("running limit lost fourth occurrence: %+v, %v", scan, err)
	}
	// Simulate a worker's terminal write so a queued occurrence can take the slot.
	if _, err := db.db.ExecContext(ctx, `UPDATE runs SET state = 'succeeded' WHERE id = ?`, runIDs[0]); err != nil {
		t.Fatal(err)
	}
	scan, err = second.ScanDue(ctx, "scanner-b", base.Add(4*time.Minute+5*time.Second), 32)
	if err != nil || len(scan.Outcomes) != 1 || len(scan.Outcomes[0].RunIDs) != 1 || scan.Outcomes[0].NextAt == nil ||
		!scan.Outcomes[0].NextAt.Equal(base.Add(5*time.Minute)) {
		t.Fatalf("catch-up did not resume: %+v, %v", scan, err)
	}
	spec.Enabled = false
	paused, err := db.PutSchedule(ctx, scope, dag, 1, spec)
	if err != nil || paused.Enabled || paused.NextAt == nil || !paused.NextAt.Equal(base.Add(5*time.Minute)) {
		t.Fatalf("pause lost the cursor: %+v, %v", paused, err)
	}

	spec.Key, spec.Enabled, spec.Misfire, spec.GraceSeconds = "skip-dispatch", true, "skip", 30
	skip, err := db.PutSchedule(ctx, scope, dag, 0, spec)
	if err != nil {
		t.Fatal(err)
	}
	scan, err = db.ScanDue(ctx, "scanner-a", base.Add(6*time.Minute+20*time.Second), 32)
	if err != nil || len(scan.Outcomes) != 1 || scan.Outcomes[0].ScheduleID != skip.ID || len(scan.Outcomes[0].RunIDs) != 1 ||
		scan.Outcomes[0].SkippedFrom == nil || scan.Outcomes[0].SkippedBefore == nil ||
		!scan.Outcomes[0].SkippedFrom.Equal(base.Add(time.Minute)) || !scan.Outcomes[0].SkippedBefore.Equal(base.Add(5*time.Minute+50*time.Second)) {
		t.Fatalf("wrong misfire scan: %+v, %v", scan, err)
	}
	detail, err := db.GetRun(ctx, scope, scan.Outcomes[0].RunIDs[0])
	if err != nil || !detail.Run.LogicalAt.Equal(base.Add(6*time.Minute)) {
		t.Fatalf("skip retained the wrong occurrence: %+v, %v", detail.Run, err)
	}
	if _, err := db.Trigger(ctx, scope, TriggerRequest{DAG: dag, Key: detail.Run.TriggerKey, Kind: "external", Inputs: spec.Inputs}); err == nil {
		t.Fatal("manual or external trigger must not use the scheduler key namespace")
	}

	if _, err := db.Publish(ctx, scope, 1, []byte(`{"key":"dispatch-report","inputs":{"batch":"integer"},"nodes":[
		{"key":"extract","task":"extract","queue":"reports","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none"}
	]}`)); err != nil {
		t.Fatal(err)
	}
	blockedAt := base.Add(7 * time.Minute)
	scan, err = db.ScanDue(ctx, "scanner-a", blockedAt, 32)
	if err != nil || len(scan.Outcomes) != 1 || scan.Outcomes[0].Status != "input_contract_changed" || len(scan.Outcomes[0].RunIDs) != 0 {
		t.Fatalf("incompatible schedule inputs should hold the cursor: %+v, %v", scan, err)
	}
	blocked, err := db.GetSchedule(ctx, scope, dag, spec.Key)
	if err != nil || blocked.LastError == nil || *blocked.LastError != "input_contract_changed" || blocked.NextAt == nil || !blocked.NextAt.Equal(blockedAt) {
		t.Fatalf("missing durable schedule error: %+v, %v", blocked, err)
	}
	spec.Enabled = false
	if _, err := db.PutSchedule(ctx, scope, dag, 1, spec); err != nil {
		t.Fatalf("operator must be able to pause an incompatible schedule: %v", err)
	}
	spec.Enabled, spec.Revision = true, 1
	if _, err := db.PutSchedule(ctx, scope, dag, 2, spec); err != nil {
		t.Fatal(err)
	}
	scan, err = second.ScanDue(ctx, "scanner-b", blockedAt, 32)
	if err != nil || len(scan.Outcomes) != 1 || len(scan.Outcomes[0].RunIDs) != 1 {
		t.Fatalf("restored schedule did not resume at its earlier cursor: %+v, %v", scan, err)
	}
	restored, err := db.GetRun(ctx, scope, scan.Outcomes[0].RunIDs[0])
	if err != nil || restored.Run.Revision != 1 || !restored.Run.LogicalAt.Equal(blockedAt) {
		t.Fatalf("restored schedule used the wrong revision or instant: %+v, %v", restored.Run, err)
	}
}
