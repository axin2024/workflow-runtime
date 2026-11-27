package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/axin2024/workflow-runtime/internal/process"
	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workerapi"
)

func processIntegrationStore(t *testing.T) (*store.Store, string, store.Scope) {
	t.Helper()
	dsn := os.Getenv("WORKFLOW_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("set WORKFLOW_TEST_MYSQL_DSN to a disposable MySQL database")
	}
	if _, err := store.Migrate(dsn); err != nil {
		t.Fatal(err)
	}
	db, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	scope := store.Scope{Workspace: "process-http-" + strings.ToLower(rand.Text()), Actor: "operator"}
	if err = db.CreateWorkspace(t.Context(), scope.Workspace); err != nil {
		t.Fatal(err)
	}
	return db, dsn, scope
}

func processSignalDefinition(withTimer bool) process.Definition {
	def := process.Definition{Start: "receive", Steps: []process.Step{
		{Key: "receive", Kind: process.Signal, Next: "done", OnTimeout: "failed", SignalKind: "goods_received", AfterSeconds: 3600},
		{Key: "done", Kind: process.End, Result: "succeeded"}, {Key: "failed", Kind: process.End, Result: "failed"},
	}}
	if withTimer {
		def.Start = "dispatch"
		def.Steps = append(def.Steps, process.Step{Key: "dispatch", Kind: process.Timer, Next: "receive", AfterSeconds: 3600})
	}
	return def
}

func processConnectorRequest(ctx context.Context, client *http.Client, method, url, token, key string, body []byte) (int, http.Header, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return 0, nil, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 256<<10))
	return response.StatusCode, response.Header, data, err
}

func TestMySQLProcessConnectorSignalContract(t *testing.T) {
	db, dsn, scope := processIntegrationStore(t)
	ctx := t.Context()
	const key = "purchasing"
	if _, err := db.PublishProcess(ctx, scope, key, 0, processSignalDefinition(true)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PublishProcess(ctx, scope, "unrelated", 0, processSignalDefinition(true)); err != nil {
		t.Fatal(err)
	}
	run, err := db.StartProcess(ctx, scope, key, store.NewProcessRun{Business: "PO-7"})
	if err != nil {
		t.Fatal(err)
	}
	other, err := db.StartProcess(ctx, scope, "unrelated", store.NewProcessRun{Business: "PO-7"})
	if err != nil {
		t.Fatal(err)
	}
	identity := store.ProcessConnector{Scope: store.Scope{Workspace: scope.Workspace, Actor: "goods-receiver"}, Process: key}
	connector, err := db.GrantProcessConnector(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(workerapi.Handler(db, log.New(io.Discard, "", 0)))
	defer server.Close()
	url := fmt.Sprintf("%s/process-runs/%d/signals", server.URL, run.ID)
	send := func(method, endpoint, token, idem, payload string, expected int) (http.Header, []byte) {
		t.Helper()
		status, headers, result, err := processConnectorRequest(ctx, server.Client(), method, endpoint, token, idem, []byte(payload))
		if err != nil || status != expected || headers.Get("Cache-Control") != "no-store" {
			t.Fatalf("%s %s expected=%d actual=%d err=%v body=%s", method, endpoint, expected, status, err, result)
		}
		return headers, result
	}
	const payload = `{"event_id":"E3","correlation_key":"PO-7","kind":"goods_received","payload":{"qty":3}}`
	send(http.MethodPost, url, "", "e3", payload, 401)
	send(http.MethodPost, fmt.Sprintf("%s/process-runs/%d/signals", server.URL, other.ID), connector.Token, "e3", payload, 404)
	_, invalid := send(http.MethodPost, url, connector.Token, "wrong-correlation", strings.Replace(payload, "PO-7", "PO-8", 1), 422)
	if !bytes.Contains(invalid, []byte(`"field":"correlation_key"`)) {
		t.Fatalf("missing field: %s", invalid)
	}
	headers, accepted := send(http.MethodPost, url, connector.Token, "e3", payload, 202)
	var first, final store.SignalReceipt
	if err = json.Unmarshal(accepted, &first); err != nil || first.ID == 0 || first.Outcome != "waiting_for_match" || headers.Get("Location") == "" {
		t.Fatalf("initial receipt: %+v %v %v", first, headers, err)
	}
	send(http.MethodPost, url, connector.Token, "collision", strings.Replace(payload, `:3`, `:4`, 1), 409)
	if _, err = processFixtureSQL(t, dsn).ExecContext(ctx, `UPDATE process_waits SET deadline=UTC_TIMESTAMP(6) WHERE run_id=? AND step_key='dispatch'`, run.ID); err != nil {
		t.Fatal(err)
	}
	if err = runScheduler(ctx, []string{"--once"}, dsn, "test-scheduler", io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	_, current := send(http.MethodGet, server.URL+headers.Get("Location"), connector.Token, "", "", 200)
	if err = json.Unmarshal(current, &final); err != nil || final.ID != first.ID || final.Outcome != "consumed" || final.ConsumedAt == nil || final.MatchedAt == nil {
		t.Fatalf("read receipt: %+v %v", final, err)
	}
	newConnector, err := db.GrantProcessConnector(ctx, identity)
	if err != nil {
		t.Fatal(err)
	}
	send(http.MethodGet, server.URL+headers.Get("Location"), connector.Token, "", "", 401)
	send(http.MethodPost, url, newConnector.Token, "e3", payload, 200)
	t.Logf("run=%d receipt=%d early_http=202 collision_http=409 scoped_http=404 validation_http=422 final_state=%s", run.ID, final.ID, final.Outcome)
}
