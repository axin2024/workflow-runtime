package store

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestMySQLSelectiveBackfillFreezesThreeOfSevenAndResumes(t *testing.T) {
	db, dsn := openTestStore(t)
	peer, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
	defer cancel()
	key := strings.ToLower(rand.Text())
	scope, other := Scope{Workspace: "backfill-" + key, Actor: "operator"}, Scope{Workspace: "other-" + key, Actor: "operator"}
	for _, tenant := range []Scope{scope, other} {
		if err := db.CreateWorkspace(ctx, tenant.Workspace); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile("../../examples/diamond.json")
	if err != nil {
		t.Fatal(err)
	}
	v1, err := db.Publish(ctx, scope, 0, raw)
	if err != nil {
		t.Fatal(err)
	}
	dag := v1.Graph.Definition.Key
	base := time.Date(2025, 2, 28, 16, 0, 0, 0, time.UTC)
	espec := ScheduleSpec{Key: "historical-dispatch", Cron: "0 * * * *", Timezone: "Asia/Shanghai", Misfire: "catch_up",
		MaxActiveRuns: 2, Enabled: true, StartAfter: base, Inputs: []byte(`{"rows":7,"source":"frozen-fixture"}`)}
	config, err := db.PutSchedule(ctx, scope, dag, 0, espec)
	if err != nil {
		t.Fatal(err)
	}
	v2, err := db.Publish(ctx, scope, 1, []byte(`{"key":"dispatch-report","inputs":{"batch":"integer"},"nodes":[
		{"key":"import","task":"load","queue":"reports","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none"}
	]}`))
	if err != nil || v2.Revision != 2 {
		t.Fatalf("publish new workflow: %+v, %v", v2, err)
	}
	spec := BackfillSpec{Schedule: espec.Key, Revision: 1, From: base.Add(time.Hour), Through: base.Add(7 * time.Hour)}
	preview, err := db.PreviewBackfill(ctx, scope, dag, spec)
	if err != nil || preview.State != "draft" || preview.Revision != 1 || preview.GraphDigest != v1.Graph.Digest || len(preview.Items) != 7 ||
		preview.Timezone != "Asia/Shanghai" || len(preview.InputDigest) != 64 {
		t.Fatalf("incorrect preview: %+v, %v", preview, err)
	}
	for index, item := range preview.Items {
		want := base.Add(time.Duration(index+1) * time.Hour)
		if item.Ordinal != index+1 || item.State != "available" || item.LogicalAt != want || !strings.HasSuffix(item.LocalAt, "+08:00") ||
			item.TriggerKey != scheduledTriggerKey(config.ID, want) || len(item.Impact) != 5 || len(item.Impact[0].OperationKey) != 64 {
			t.Fatalf("incorrect candidate %d: %+v", index+1, item)
		}
	}
	if _, err := db.GetBackfill(ctx, other, preview.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-workspace preview disclosure: %v", err)
	}
	if _, err := peer.ConfirmBackfill(ctx, other, preview.ID, []int{1}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-workspace confirm: %v", err)
	}
	overlap, err := peer.PreviewBackfill(ctx, scope, dag, spec)
	if err != nil {
		t.Fatal(err)
	}
	// A new calendar and input contract must not edit the previewed snapshot.
	espec.StartAfter = base.Add(6*time.Hour - time.Minute)
	espec.Inputs = []byte(`{"batch":99}`)
	espec.Timezone = "Asia/Tokyo"
	if _, err := peer.PutSchedule(ctx, scope, dag, 1, espec); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	confirmed := make(chan error, 2)
	for _, store := range []*Store{db, peer} {
		go func() {
			<-start
			_, err := store.ConfirmBackfill(ctx, scope, preview.ID, []int{6, 1, 3})
			confirmed <- err
		}()
	}
	close(start)
	for range 2 {
		if err := <-confirmed; err != nil {
			t.Fatal(err)
		}
	}
	partial, err := db.GetBackfill(ctx, scope, preview.ID)
	if err != nil || partial.State != "confirmed" || partial.ScheduleGeneration != 1 || partial.Timezone != preview.Timezone || len(partial.Items) != 7 ||
		partial.Items[0].State != "created" || partial.Items[2].State != "created" || partial.Items[5].State != "pending" ||
		partial.Items[0].RunID == nil || partial.Items[2].RunID == nil || partial.Items[5].RunID != nil {
		t.Fatalf("expected two receipts and one pending candidate: %+v, %v", partial, err)
	}
	if _, err := peer.ConfirmBackfill(ctx, scope, overlap.ID, []int{6}); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("overlapping batch claimed a pending occurrence: %v", err)
	}
	if failed, err := db.GetBackfill(ctx, scope, overlap.ID); err != nil || failed.State != "draft" || len(failed.Items) != 7 || failed.Items[5].State != "available" {
		t.Fatalf("overlap did not roll back: %+v, %v", failed, err)
	}
	if _, err := db.ConfirmBackfill(ctx, scope, preview.ID, []int{1, 2, 3}); !errors.Is(err, ErrBackfillSelectionChanged) {
		t.Fatalf("confirmed selection was not frozen: %v", err)
	}
	scan, err := db.ScanDue(ctx, "scheduler", base.Add(6*time.Hour), 32)
	if err != nil || len(scan.Outcomes) != 1 || scan.Outcomes[0].ScheduleID != config.ID || scan.Outcomes[0].Reserved != 1 || len(scan.Outcomes[0].RunIDs) != 0 {
		t.Fatalf("scheduler overrode a confirmed historical occurrence: %+v, %v", scan, err)
	}
	// Simulate the terminal write that releases one schedule slot for the pending item.
	if _, err := db.db.ExecContext(ctx, `UPDATE runs SET state = 'succeeded' WHERE id = ?`, *partial.Items[0].RunID); err != nil {
		t.Fatal(err)
	}
	resumed, err := peer.ResumeBackfill(ctx, scope, preview.ID)
	if err != nil || resumed.State != "completed" || resumed.CompletedAt == nil || resumed.InputDigest != preview.InputDigest {
		t.Fatalf("resume: %+v, %v", resumed, err)
	}
	var completedIDs []int64
	for index, item := range resumed.Items {
		if !slices.Contains([]int{1, 3, 6}, item.Ordinal) {
			if item.State != "excluded" || item.RunID != nil {
				t.Fatalf("unexpected unselected run: %+v", item)
			}
			continue
		}
		if item.State != "created" || item.RunID == nil || item.RunRevision == nil || *item.RunRevision != 1 || item.ProcessedAt == nil {
			t.Fatalf("invalid completed receipt: %+v", item)
		}
		detail, err := db.GetRun(ctx, scope, *item.RunID)
		if err != nil || detail.Run.Kind != "backfill" || detail.Run.TriggerKey != item.TriggerKey || detail.Run.LogicalAt != item.LogicalAt ||
			detail.Version.Graph.Digest != v1.Graph.Digest || string(detail.Run.Inputs) != `{"rows":7,"source":"frozen-fixture"}` {
			t.Fatalf("occurrence %d lost its snapshot: %+v, %v", item.Ordinal, detail, err)
		}
		operations := make(map[string]string, len(detail.Nodes))
		for _, node := range detail.Nodes {
			operations[node.Key] = node.OperationKey
		}
		for _, impact := range preview.Items[index].Impact {
			if operations[impact.Node] != impact.OperationKey {
				t.Fatalf("previewed operation changed: %+v", impact)
			}
		}
		completedIDs = append(completedIDs, *item.RunID)
	}
	if len(completedIDs) != 3 || completedIDs[0] != *partial.Items[0].RunID || completedIDs[1] != *partial.Items[2].RunID {
		t.Fatalf("resume replaced existing run receipts: %v", completedIDs)
	}
	repeat, err := peer.ConfirmBackfill(ctx, scope, preview.ID, []int{1, 3, 6})
	if err != nil || repeat.State != "completed" || len(repeat.Items) != 7 || repeat.Items[5].RunID == nil || *repeat.Items[5].RunID != completedIDs[2] {
		t.Fatalf("repeat confirm: %+v, %v", repeat, err)
	}
	var count int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE schedule_id = ?`, config.ID).Scan(&count); err != nil || count != 3 {
		t.Fatalf("expected three persisted runs total, got %d, %v", count, err)
	}
	spec.Inputs = []byte(`{"rows":7,"source":"frozen-fixture"}`)
	baseline, err := db.PreviewBackfill(ctx, scope, dag, spec)
	if err != nil || len(baseline.Items) != 2 || baseline.Items[0].State != "already_registered" ||
		baseline.Items[0].BaselineRunID == nil || *baseline.Items[0].BaselineRunID != completedIDs[2] {
		t.Fatalf("baseline run missing from new preview: %+v, %v", baseline, err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE runs SET state = 'succeeded' WHERE id = ?`, completedIDs[1]); err != nil {
		t.Fatal(err)
	}
	scan, err = peer.ScanDue(ctx, "scheduler", base.Add(7*time.Hour), 32)
	if err != nil || len(scan.Outcomes) != 1 || len(scan.Outcomes[0].RunIDs) != 1 {
		t.Fatalf("general scan should create the unreserved occurrence: %+v, %v", scan, err)
	}
	already, err := db.ConfirmBackfill(ctx, scope, baseline.ID, []int{2})
	if err != nil || already.State != "completed" || len(already.Items) != 2 || already.Items[1].State != "reused" ||
		already.Items[1].RunID == nil || *already.Items[1].RunID != scan.Outcomes[0].RunIDs[0] ||
		already.Items[1].RunRevision == nil || *already.Items[1].RunRevision != 2 {
		t.Fatalf("existing occurrence should keep the actual run and revision: %+v, %v", already, err)
	}
	espec.Enabled = false
	if _, err := peer.PutSchedule(ctx, scope, dag, 2, espec); err != nil {
		t.Fatal(err)
	}
}
