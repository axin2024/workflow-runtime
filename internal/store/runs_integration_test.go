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
	"github.com/go-sql-driver/mysql"
)

func TestMySQLVersionAndTriggerIdentity(t *testing.T) {
	dsn := os.Getenv("WORKFLOW_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set WORKFLOW_TEST_MYSQL_DSN to an isolated MySQL test database")
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasPrefix(config.DBName, "workflow_test_") {
		t.Fatal("test database name must start with workflow_test_")
	}
	for i := 0; i < 2; i++ {
		version, err := Migrate(dsn)
		if err != nil || version != SchemaVersion {
			t.Fatalf("migrate: version=%d, error=%v", version, err)
		}
	}
	db, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	if err := db.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	key := strings.ToLower(rand.Text())
	scope := Scope{Workspace: "test-" + key, Actor: "publisher"}
	other := Scope{Workspace: "other-" + key, Actor: "other-operator"}
	for _, tenant := range []Scope{scope, other} {
		if err := db.CreateWorkspace(ctx, tenant.Workspace); err != nil {
			t.Fatal(err)
		}
	}
	v1Source, err := os.ReadFile("../../examples/diamond.json")
	if err != nil {
		t.Fatal(err)
	}
	first, err := db.Publish(ctx, scope, 0, v1Source)
	if err != nil {
		t.Fatal(err)
	}
	if first.Revision != 1 || first.Publisher != scope.Actor || first.PublishedAt.IsZero() {
		t.Fatalf("invalid published revision: %+v", first)
	}
	firstInputs, err := os.ReadFile("../../examples/diamond-inputs.json")
	if err != nil {
		t.Fatal(err)
	}
	logical, err := time.Parse(time.RFC3339Nano, "2026-01-01T16:30:00.123456789+08:00")
	if err != nil {
		t.Fatal(err)
	}
	request := TriggerRequest{DAG: first.Graph.Definition.Key, Key: "ext:import-17", Kind: "external", LogicalAt: &logical, Inputs: firstInputs}
	start := make(chan struct{})
	type delivery struct {
		result TriggerResult
		err    error
	}
	const attempts = 12
	receipts := make(chan delivery, attempts)
	for range attempts {
		go func() {
			<-start
			result, err := db.Trigger(ctx, scope, request)
			receipts <- delivery{result, err}
		}()
	}
	close(start)
	var oldID int64
	created := 0
	for range attempts {
		receipt := <-receipts
		if receipt.err != nil {
			t.Fatal(receipt.err)
		}
		if !receipt.result.Replayed {
			created++
		}
		run := receipt.result.Run
		if oldID == 0 {
			oldID = run.ID
		}
		if oldID <= 0 || run.ID != oldID || run.Revision != 1 || run.State != "queued" ||
			run.LogicalAt != logical.UTC().Truncate(time.Microsecond) {
			t.Fatalf("wrong concurrent response: %+v", run)
		}
	}
	if created != 1 {
		t.Fatalf("expected 1 creation out of %d requests, got %d", attempts, created)
	}

	v2Definition := workflow.Definition{Key: request.DAG, Inputs: map[string]workflow.ValueType{"batch": workflow.Integer}, Nodes: []workflow.Node{{
		Key: "import", Task: "load", Queue: "default", TimeoutSeconds: 20,
		Retry: workflow.RetryPolicy{MaxAttempts: 1}, Effect: workflow.NoEffect,
	}}}
	v2Source, err := json.Marshal(v2Definition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.Publish(ctx, scope, 1, v2Source)
	if err != nil || second.Revision != 2 || second.Graph.Digest == first.Graph.Digest {
		t.Fatalf("publish v2: %+v, %v", second, err)
	}
	_, err = db.Publish(ctx, scope, 1, v2Source)
	var revisionConflict *RevisionConflict
	if !errors.As(err, &revisionConflict) || revisionConflict.Active != 2 {
		t.Fatalf("stale publish response: %v", err)
	}
	var original map[string]json.RawMessage
	if err := json.Unmarshal(firstInputs, &original); err != nil {
		t.Fatal(err)
	}
	request.Inputs, err = json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := db.Trigger(ctx, scope, request)
	if err != nil || !replay.Replayed || replay.Run.ID != oldID || replay.Run.Revision != 1 {
		t.Fatalf("retry after v2: %+v, %v", replay, err)
	}
	changed := request
	changed.Inputs = []byte(`{"batch":9223372036854775807}`)
	_, err = db.Trigger(ctx, scope, changed)
	var triggerConflict *TriggerConflict
	if !errors.As(err, &triggerConflict) || triggerConflict.RunID != oldID {
		t.Fatalf("conflicting trigger response: %v", err)
	}
	changed.Key = "ext:import-18"
	newRun, err := db.Trigger(ctx, scope, changed)
	if err != nil || newRun.Replayed || newRun.Run.Revision != 2 || string(newRun.Run.Inputs) != string(changed.Inputs) {
		t.Fatalf("new active revision: %+v, %v", newRun, err)
	}
	changed.Key, changed.Inputs = "invalid-under-v2", firstInputs
	if _, err := db.Trigger(ctx, scope, changed); err == nil {
		t.Fatal("v1 inputs unexpectedly accepted by the new active revision")
	}
	changed.Revision = 1
	if pinned, err := db.Trigger(ctx, scope, changed); err != nil || pinned.Run.Revision != 1 {
		t.Fatalf("explicit historical trigger: %+v, %v", pinned, err)
	}
	old, err := db.GetRun(ctx, scope, oldID)
	if err != nil || old.Version.Graph.Digest != first.Graph.Digest || old.Version.Revision != 1 || len(old.Nodes) != 5 {
		t.Fatalf("frozen v1 run: %+v, %v", old, err)
	}
	for _, node := range old.Nodes {
		if node.Key == "extract" {
			if node.State != "ready" || node.AvailableAt == nil {
				t.Fatalf("root not ready: %+v", node)
			}
		} else if node.State != "waiting" || node.AvailableAt != nil {
			t.Fatalf("dependent ready prematurely: %+v", node)
		}
	}
	if _, err := db.GetRun(ctx, other, oldID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-workspace run lookup: %v", err)
	}
	if _, err := db.GetVersion(ctx, other, request.DAG, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-workspace revision lookup: %v", err)
	}
	if _, err := db.Publish(ctx, other, 0, v1Source); err != nil {
		t.Fatal(err)
	}
	separate, err := db.Trigger(ctx, other, request)
	if err != nil || separate.Replayed || separate.Run.ID == oldID {
		t.Fatalf("logical keys must be workspace-scoped: %+v, %v", separate, err)
	}
	var originalDigest, previous, received string
	err = db.db.QueryRowContext(ctx, `SELECT LOWER(HEX(r.request_digest)),
		JSON_UNQUOTE(JSON_EXTRACT(a.details, '$.original_digest')), JSON_UNQUOTE(JSON_EXTRACT(a.details, '$.received_digest'))
		FROM audit_events a JOIN runs r ON r.id = a.run_id WHERE a.run_id = ? AND a.event_kind = 'trigger_conflict'`, oldID).
		Scan(&originalDigest, &previous, &received)
	if err != nil || len(previous) != 64 || len(received) != 64 || originalDigest != previous || previous == received {
		t.Fatalf("invalid conflict audit: %q %q %q, %v", originalDigest, previous, received, err)
	}
}
