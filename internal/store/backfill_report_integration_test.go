package store

import (
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMySQLBackfillReportShowsFrozenVersionAndActualArtifacts(t *testing.T) {
	db, _ := openTestStore(t)
	ctx := t.Context()
	key := strings.ToLower(rand.Text())
	scope := Scope{Workspace: "backfill-report-" + key, Actor: "fixture-operator"}
	foreign := Scope{Workspace: "foreign-" + key, Actor: "fixture-reader"}
	for _, identity := range []Scope{scope, foreign} {
		if err := db.CreateWorkspace(ctx, identity.Workspace); err != nil {
			t.Fatal(err)
		}
	}
	const dag = "batch-report"
	original := []byte(`{"key":"batch-report","nodes":[{"key":"result","task":"probe","queue":"reports","timeout_seconds":30,
		"output":{"receipt":"integer"},"retry":{"max_attempts":1},"effect":"none"}]}`)
	version, err := db.Publish(ctx, scope, 0, original)
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2025, time.November, 1, 0, 0, 0, 0, time.UTC)
	const schedule = "hourly"
	if _, err := db.PutSchedule(ctx, scope, dag, 0, ScheduleSpec{Key: schedule, Cron: "0 * * * *", Timezone: "Asia/Shanghai",
		Misfire: "catch_up", MaxActiveRuns: 4, StartAfter: base, Inputs: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	current := []byte(strings.Replace(string(original), `"task":"probe"`, `"task":"other"`, 1))
	if _, err := db.Publish(ctx, scope, 1, current); err != nil {
		t.Fatal(err)
	}
	preview, err := db.PreviewBackfill(ctx, scope, dag, BackfillSpec{Schedule: schedule, Revision: 1,
		From: base.Add(time.Hour), Through: base.Add(7 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if report, err := db.ExportBackfill(ctx, scope, preview.ID); err != nil || report.SelectionDigest != "" || len(report.Runs) != 0 || len(report.Batch.Items) != 7 {
		t.Fatalf("draft report: %+v, %v", report, err)
	}
	batch, err := db.ConfirmBackfill(ctx, scope, preview.ID, []int{1, 3, 6})
	if err != nil || batch.State != "completed" {
		t.Fatalf("batch confirmation: %+v, %v", batch, err)
	}
	before, err := db.ExportBackfill(ctx, scope, preview.ID)
	if err != nil || len(before.Runs) != 3 || len(before.Runs[0].Artifacts) != 0 || before.Runs[0].State != "queued" {
		t.Fatalf("queued artifact evidence: %+v, %v", before, err)
	}
	worker, err := db.RegisterWorker(ctx, scope, "report-worker", 1, []WorkerPermission{{Queue: "reports", Task: "probe"}})
	if err != nil {
		t.Fatal(err)
	}
	digests := make(map[int64]string, 3)
	for ordinal := 1; ordinal <= 3; ordinal++ {
		job, err := db.Claim(ctx, worker.Token, ClaimRequest{Tasks: []string{"probe"}, LeaseSeconds: 30})
		if err != nil || job == nil {
			t.Fatalf("claim old graph: %+v, %v", job, err)
		}
		outputs := []byte(fmt.Sprintf(`{"receipt":%d}`, ordinal))
		if _, err := db.Complete(ctx, worker.Token, CompletionRequest{Lease: job.LeaseRef, Success: true, Outputs: outputs}); err != nil {
			t.Fatal(err)
		}
		digests[job.RunID] = fmt.Sprintf("%x", sha256.Sum256(outputs))
	}
	retry, err := db.ConfirmBackfill(ctx, scope, batch.ID, []int{6, 3, 1})
	if err != nil || retry.ID != batch.ID {
		t.Fatalf("repeat batch confirmation: %+v, %v", retry, err)
	}
	if job, err := db.Claim(ctx, worker.Token, ClaimRequest{Tasks: []string{"probe"}, LeaseSeconds: 30}); err != nil || job != nil {
		t.Fatalf("repeat confirmation produced a new job: %+v, %v", job, err)
	}
	report, err := db.ExportBackfill(ctx, scope, batch.ID)
	if err != nil || report.Format != "workflow-runtime/backfill-report-v1" || report.GeneratedBy != scope.Actor || report.GeneratedAt.IsZero() ||
		report.SelectionDigest != fmt.Sprintf("%x", sha256.Sum256([]byte(`[1,3,6]`))) || report.Batch.Revision != 1 ||
		report.Batch.GraphDigest != version.Graph.Digest || report.Batch.Timezone != "Asia/Shanghai" || len(report.Runs) != 3 {
		t.Fatalf("completed report: %+v, %v", report, err)
	}
	keys := make(map[string]bool, 3)
	for index, run := range report.Runs {
		item := report.Batch.Items[run.Ordinal-1]
		if run.Ordinal != []int{1, 3, 6}[index] || item.RunID == nil || *item.RunID != run.RunID || keys[item.TriggerKey] || item.TriggerKey == "" ||
			run.Revision != 1 || run.State != "succeeded" || len(run.Artifacts) != 1 || run.Artifacts[0].RunID != run.RunID ||
			run.Artifacts[0].Epoch != 1 || run.Artifacts[0].Node != "result" || run.Artifacts[0].SHA256 != digests[run.RunID] {
			t.Fatalf("unexpected exported evidence for occurrence %+v: %+v", item, run)
		}
		keys[item.TriggerKey] = true
	}
	if _, err := db.ExportBackfill(ctx, foreign, batch.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-workspace export: %v", err)
	}
}
