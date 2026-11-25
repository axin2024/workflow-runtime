package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/go-sql-driver/mysql"
)

func TestMySQLTerminalCallbackDeliveryAndLeaseRecovery(t *testing.T) {
	dsn := os.Getenv("WORKFLOW_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set WORKFLOW_TEST_MYSQL_DSN to an isolated MySQL test database")
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil || !strings.HasPrefix(config.DBName, "workflow_test_") {
		t.Fatal("test database name must start with workflow_test_")
	}
	if _, err := store.Migrate(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	scope := store.Scope{Workspace: "callback-" + strings.ToLower(rand.Text()), Actor: "callback-client"}
	other := store.Scope{Workspace: scope.Workspace, Actor: "other-client"}
	const dag = "callback-workflow"
	const graph = `{"key":"callback-workflow","nodes":[{"key":"record","task":"record","queue":"callbacks","timeout_seconds":30,"retry":{"max_attempts":1},"effect":"none"}]}`
	if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Publish(ctx, scope, 0, []byte(graph)); err != nil {
		t.Fatal(err)
	}
	grant, err := db.GrantTrigger(ctx, scope, dag)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.GrantTrigger(ctx, other, dag); err != nil {
		t.Fatal(err)
	}
	worker, err := db.RegisterWorker(ctx, scope, "callback-worker", 1, []store.WorkerPermission{{Task: "record", Queue: "callbacks"}})
	if err != nil {
		t.Fatal(err)
	}
	trigger := func(key string) store.TriggerReceipt {
		t.Helper()
		result, err := db.ExternalTrigger(ctx, grant.Token, store.ExternalTriggerRequest{Key: key, Inputs: []byte("{}")})
		if err != nil {
			t.Fatal(err)
		}
		return result.Run
	}
	pending := trigger("committed-receipt")
	secret := sha256.Sum256([]byte("synthetic callback signing fixture"))
	var deliveredMu sync.Mutex
	var delivered []store.CallbackEvent
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		var event store.CallbackEvent
		if err != nil || json.Unmarshal(body, &event) != nil || r.Method != http.MethodPost {
			http.Error(w, "invalid callback body", http.StatusBadRequest)
			return
		}
		timestamp := r.Header.Get("Workflow-Timestamp")
		mac := hmac.New(sha256.New, secret[:])
		mac.Write([]byte(timestamp + "." + event.ID + "."))
		mac.Write(body)
		signed := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		if timestamp == "" || r.Header.Get("Workflow-Event-ID") != event.ID ||
			!hmac.Equal([]byte(signed), []byte(r.Header.Get("Workflow-Signature"))) {
			http.Error(w, "invalid callback signature", http.StatusUnauthorized)
			return
		}
		receipt, err := db.ExternalReceipt(r.Context(), grant.Token, event.Run.ID, "")
		if err != nil || receipt.State != "succeeded" || receipt.FinishedAt == nil || event.Run.FinishedAt == nil ||
			event.Run.ID != pending.ID || event.Workspace != scope.Workspace || event.Actor != scope.Actor ||
			event.Type != "workflow.run.finished.v1" || event.Run.Key != receipt.Key || event.Run.Revision != receipt.Revision ||
			!receipt.FinishedAt.Equal(*event.Run.FinishedAt) {
			http.Error(w, "callback result not committed", http.StatusConflict)
			return
		}
		deliveredMu.Lock()
		defer deliveredMu.Unlock()
		delivered = append(delivered, event)
		if len(delivered) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer receiver.Close()
	t.Setenv("WORKFLOW_MYSQL_DSN", dsn)
	t.Setenv("WORKFLOW_CALLBACK_URL", receiver.URL)
	t.Setenv("WORKFLOW_CALLBACK_SECRET", hex.EncodeToString(secret[:]))
	dispatch := func() *store.CallbackStatus {
		t.Helper()
		var output bytes.Buffer
		if err := run(ctx, []string{"callback-dispatch", "--once", scope.Workspace, dag, scope.Actor}, &output); err != nil {
			t.Fatal(err)
		}
		var record *store.CallbackStatus
		if err := json.Unmarshal(output.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		return record
	}
	if record := dispatch(); record != nil {
		t.Fatalf("pending run produced a delivery: %+v", record)
	}
	lease, err := db.Claim(ctx, worker.Token, store.ClaimRequest{Tasks: []string{"record"}, LeaseSeconds: 20})
	if err != nil || lease == nil || lease.RunID != pending.ID {
		t.Fatalf("claim work: %+v, %v", lease, err)
	}
	completion := store.CompletionRequest{Lease: lease.LeaseRef, Success: true}
	if _, err := db.Complete(ctx, worker.Token, completion); err != nil {
		t.Fatal(err)
	}
	if receipt, err := db.Complete(ctx, worker.Token, completion); err != nil || !receipt.Replayed {
		t.Fatalf("replay completion: %+v, %v", receipt, err)
	}
	if claimed, err := db.ClaimCallback(ctx, other, dag); err != nil || claimed != nil {
		t.Fatalf("callback leaked to another identity: %+v, %v", claimed, err)
	}
	first := dispatch()
	if first == nil || first.Attempts != 1 || first.DeliveredAt != nil || first.LastHTTPStatus == nil || *first.LastHTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("retryable callback attempt: %+v", first)
	}
	if record := dispatch(); record != nil {
		t.Fatalf("callback retried before its backoff: %+v", record)
	}
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(time.Until(first.AvailableAt) + 150*time.Millisecond):
	}
	second := dispatch()
	if second == nil || second.Attempts != 2 || second.DeliveredAt == nil || second.EventID != first.EventID ||
		second.LastHTTPStatus == nil || *second.LastHTTPStatus != http.StatusNoContent {
		t.Fatalf("callback completion: %+v", second)
	}
	if record := dispatch(); record != nil {
		t.Fatalf("acknowledged event was delivered again: %+v", record)
	}
	var statusJSON bytes.Buffer
	if err := run(ctx, []string{"callback-status", scope.Workspace, dag, scope.Actor, strconv.FormatInt(pending.ID, 10)}, &statusJSON); err != nil {
		t.Fatal(err)
	}
	var status store.CallbackStatus
	if err := json.Unmarshal(statusJSON.Bytes(), &status); err != nil || status.EventID != first.EventID ||
		status.DeliveredAt == nil || !status.DeliveredAt.Equal(*second.DeliveredAt) {
		t.Fatalf("durable delivery status: %+v, %v", status, err)
	}
	if _, err := db.GetCallbackStatus(ctx, other, dag, pending.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("callback status leaked to another identity: %v", err)
	}
	deliveredMu.Lock()
	if len(delivered) != 2 || delivered[0].ID != delivered[1].ID || delivered[0].Run.ID != pending.ID {
		t.Errorf("receiver did not observe exactly two attempts of one event: %+v", delivered)
	}
	deliveredMu.Unlock()

	stop := trigger("canceled-receipt")
	if _, err := db.CancelRun(ctx, scope, stop.ID, store.CancelRequest{Key: "cancel-callback", Reason: "synthetic cancellation"}); err != nil {
		t.Fatal(err)
	}
	stale, err := db.ClaimCallback(ctx, scope, dag)
	if err != nil || stale == nil || stale.Event.Run.ID != stop.ID || stale.Event.Run.State != "canceled" {
		t.Fatalf("claim canceled run: %+v, %v", stale, err)
	}
	raw, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	if _, err := raw.ExecContext(ctx, "UPDATE callback_deliveries SET available_at = UTC_TIMESTAMP(6) WHERE run_id = ?", stop.ID); err != nil {
		t.Fatal(err)
	}
	current, err := db.ClaimCallback(ctx, scope, dag)
	if err != nil || current == nil || current.Attempts != 2 || current.Event.ID != stale.Event.ID {
		t.Fatalf("reclaim expired callback: %+v, %v", current, err)
	}
	if _, err := db.FinishCallback(ctx, *stale, http.StatusOK); !errors.Is(err, store.ErrCallbackLeaseLost) {
		t.Fatalf("expired dispatcher mutated current delivery: %v", err)
	}
	if settled, err := db.FinishCallback(ctx, *current, http.StatusNoContent); err != nil || settled.DeliveredAt == nil {
		t.Fatalf("settle recovered delivery: %+v, %v", settled, err)
	}
	if err := db.RevokeTrigger(ctx, scope, dag); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ClaimCallback(ctx, scope, dag); !errors.Is(err, store.ErrTriggerUnauthorized) {
		t.Fatalf("revoked callback identity acquired work: %v", err)
	}
	t.Logf("run=%d first_http=503 final_http=204 attempts=%d; canceled_run=%d reclaimed and fenced", pending.ID, second.Attempts, stop.ID)
}
