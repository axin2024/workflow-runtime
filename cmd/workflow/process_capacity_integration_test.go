package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workerapi"
	"github.com/go-sql-driver/mysql"
)

func processFixtureSQL(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.ParseTime = true
	config.Loc = time.UTC
	connector, err := mysql.NewConnector(config)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestMySQLProcessHundredThousandWaits(t *testing.T) {
	if os.Getenv("WORKFLOW_CAPACITY_LOAD") != "1" {
		t.Skip("set WORKFLOW_CAPACITY_LOAD=1 for the 100,000-wait scenario")
	}
	db, dsn, scope := processIntegrationStore(t)
	ctx := t.Context()
	const sleeping, warmup, samples = 100000, 200, 3000
	const processKey = "inventory-intake"
	version, err := db.PublishProcess(ctx, scope, processKey, 0, processSignalDefinition(false))
	if err != nil {
		t.Fatal(err)
	}
	sqlDB := processFixtureSQL(t, dsn)
	var workspace int64
	err = sqlDB.QueryRowContext(ctx, `SELECT id FROM workspaces WHERE workspace_key=?`, scope.Workspace).Scan(&workspace)
	if err != nil {
		t.Fatal(err)
	}
	seedStarted := time.Now()
	seedProcessWaits(t, sqlDB, workspace, version.ID, sleeping)
	seedDuration := time.Since(seedStarted)
	var staged int
	err = sqlDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM process_waits WHERE workspace_id=? AND status='waiting'`, workspace).Scan(&staged)
	if err != nil || staged != sleeping {
		t.Fatalf("initial waiting rows=%d err=%v", staged, err)
	}
	credential, err := db.GrantProcessConnector(ctx, store.ProcessConnector{Scope: store.Scope{Workspace: scope.Workspace, Actor: "receiving-service"}, Process: processKey})
	if err != nil {
		t.Fatal(err)
	}
	type target struct {
		id       int64
		business string
	}
	rows, err := sqlDB.QueryContext(ctx, `SELECT id,business_key FROM process_runs WHERE workspace_id=? ORDER BY id LIMIT ?`, workspace, warmup+samples)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var targets []target
	for rows.Next() {
		var hit target
		if err = rows.Scan(&hit.id, &hit.business); err != nil {
			t.Fatal(err)
		}
		targets = append(targets, hit)
	}
	if err = rows.Err(); err != nil || len(targets) != warmup+samples {
		t.Fatalf("dispatch set=%d err=%v", len(targets), err)
	}
	if err = rows.Close(); err != nil {
		t.Fatal(err)
	}
	logger := log.New(io.Discard, "", 0)
	server := httptest.NewServer(workerapi.Handler(db, logger))
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	type outcome struct {
		http        int
		latency     time.Duration
		concurrency int32
		err         error
		receipt     store.SignalReceipt
	}
	send := func(host string, client *http.Client, hit target) outcome {
		req := store.ProcessSignalInput{Event: "event-" + hit.business, Correlation: hit.business, Kind: "goods_received", Payload: json.RawMessage(`{"qty":3}`)}
		body, err := json.Marshal(req)
		if err != nil {
			return outcome{err: err}
		}
		address := fmt.Sprintf("%s/process-runs/%d/signals", host, hit.id)
		started := time.Now()
		status, _, data, err := processConnectorRequest(ctx, client, http.MethodPost, address, credential.Token, hit.business, body)
		out := outcome{http: status, latency: time.Since(started), err: err}
		if err == nil && status == 200 {
			out.err = json.Unmarshal(data, &out.receipt)
			if out.err == nil && (out.receipt.Outcome != "consumed" || out.receipt.ConsumedBy == nil || out.receipt.ConsumedAt == nil) {
				out.err = fmt.Errorf("unconsumed business receipt: %s", data)
			}
		}
		return out
	}
	pace := func(list []target) ([]outcome, time.Duration, time.Duration) {
		started := time.Now()
		results := make([]outcome, len(list))
		workers := make(chan struct{}, 24)
		var group sync.WaitGroup
		var inFlight atomic.Int32
		for i, hit := range list {
			time.Sleep(time.Until(started.Add(time.Duration(i) * 10 * time.Millisecond)))
			workers <- struct{}{}
			group.Go(func() {
				defer func() { <-workers }()
				concurrency := inFlight.Add(1)
				out := send(server.URL, client, hit)
				inFlight.Add(-1)
				out.concurrency = concurrency
				results[i] = out
			})
		}
		dispatch := time.Since(started)
		group.Wait()
		return results, dispatch, time.Since(started)
	}
	preheat, warmupSend, _ := pace(targets[:warmup])
	for i, out := range preheat {
		if out.err != nil || out.http != 200 {
			t.Fatalf("warm-up i=%d http=%d err=%v", i, out.http, out.err)
		}
	}
	results, dispatch, elapsed := pace(targets[warmup:])
	var times []time.Duration
	var rejected, timedOut, otherErrors int
	var sampleRecords [][]string
	if os.Getenv("WORKFLOW_SIGNAL_SAMPLE_CSV") != "" {
		sampleRecords = append(sampleRecords, []string{"request_key", "run_id", "http_status", "result", "elapsed_us", "concurrent_at_start"})
	}
	for i, out := range results {
		var result string
		switch {
		case os.IsTimeout(out.err):
			timedOut++
			result = "timeout"
		case out.err != nil:
			otherErrors++
			result = "error"
		case out.http != 200:
			rejected++
			result = "rejected"
		default:
			times = append(times, out.latency)
			result = "success"
		}
		if sampleRecords != nil {
			hit := targets[warmup+i]
			sampleRecords = append(sampleRecords, []string{hit.business, strconv.FormatInt(hit.id, 10), strconv.Itoa(out.http),
				result, strconv.FormatInt(out.latency.Microseconds(), 10), strconv.Itoa(int(out.concurrency))})
		}
	}
	if sampleRecords != nil {
		file, err := os.Create(os.Getenv("WORKFLOW_SIGNAL_SAMPLE_CSV"))
		if err != nil {
			t.Fatal(err)
		}
		err = csv.NewWriter(file).WriteAll(sampleRecords)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("write capacity evidence: write=%v close=%v", err, closeErr)
		}
	}
	if len(times) == 0 {
		t.Fatalf("no successful receipts: rejected=%d timeouts=%d errors=%d", rejected, timedOut, otherErrors)
	}
	slices.Sort(times)
	p95 := times[(95*len(times)+99)/100-1]
	rate := float64(samples-1) / dispatch.Seconds()
	var versionSQL string
	if err = sqlDB.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&versionSQL); err != nil {
		t.Fatal(err)
	}
	t.Logf("staged_waits=%d seed=%s cpu=%d os=%s arch=%s go=%s mysql=%s warmup_count=%d warmup=%s samples=%d dispatch=%s elapsed=%s rate_per_second=%.2f success=%d rejected=%d timeouts=%d errors=%d p95=%s",
		sleeping, seedDuration.Round(time.Millisecond), runtime.NumCPU(), runtime.GOOS, runtime.GOARCH, runtime.Version(), versionSQL,
		warmup, warmupSend.Round(time.Millisecond), samples, dispatch.Round(time.Millisecond), elapsed.Round(time.Millisecond), rate, len(times), rejected, timedOut, otherErrors, p95)
	if len(times) != samples || rate < 99.5 || p95 > 2*time.Second {
		t.Fatal("signal throughput or latency missed the acceptance criteria")
	}
	server.Close()
	if err = runScheduler(ctx, []string{"--once"}, dsn, "restart-scheduler", io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	restarted, err := store.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	newServer := httptest.NewServer(workerapi.Handler(restarted, logger))
	defer newServer.Close()
	for i, hit := range targets[warmup : warmup+100] {
		out := send(newServer.URL, newServer.Client(), hit)
		if out.err != nil || out.http != 200 || out.receipt.ID != results[i].receipt.ID ||
			!out.receipt.ConsumedAt.Equal(*results[i].receipt.ConsumedAt) {
			t.Fatalf("restart replay: i=%d http=%d receipt=%+v error=%v", i, out.http, out.receipt, out.err)
		}
	}
	var ready, completed, transitions, receipts, duplicates int
	err = sqlDB.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM process_runs WHERE workspace_id=? AND status='waiting' AND transition_rev=1),
		(SELECT COUNT(*) FROM process_runs WHERE workspace_id=? AND status='succeeded' AND transition_rev=2),
		(SELECT COUNT(*) FROM process_events WHERE workspace_id=? AND kind='wait_resolved'),
		(SELECT COUNT(*) FROM process_signals WHERE workspace_id=? AND outcome='consumed'),
		(SELECT COUNT(*) FROM (SELECT run_id FROM process_events WHERE workspace_id=? AND kind='wait_resolved' GROUP BY run_id HAVING COUNT(*)>1) d)`,
		workspace, workspace, workspace, workspace, workspace).Scan(&ready, &completed, &transitions, &receipts, &duplicates)
	if err != nil || ready != sleeping-warmup-samples || completed != warmup+samples || transitions != completed || receipts != completed || duplicates != 0 {
		t.Fatalf("restart evidence remaining=%d completed=%d transitions=%d receipts=%d duplicate=%d err=%v", ready, completed, transitions, receipts, duplicates, err)
	}
	t.Logf("restart_replays=100 waiting_remaining=%d completed=%d transitions=%d receipts=%d duplicates=%d", ready, completed, transitions, receipts, duplicates)
}

func seedProcessWaits(t *testing.T, db *sql.DB, workspace, processID int64, count int) {
	t.Helper()
	ctx := t.Context()
	for offset := 0; offset < count; offset += 1000 {
		batch := min(1000, count-offset)
		_, err := db.ExecContext(ctx, `INSERT INTO process_runs
			(workspace_id,process_id,start_revision,definition_rev,business_key,input,input_digest,actor,status,step_key)
			WITH RECURSIVE seq(n) AS (SELECT 0 UNION ALL SELECT n+1 FROM seq WHERE n+1<?)
			SELECT ?,?,1,1,CONCAT('cap-',LPAD(n+?,6,'0')),JSON_OBJECT(),UNHEX(SHA2('{}',256)),'fixture','waiting','receive' FROM seq`,
			batch, workspace, processID, offset)
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := db.ExecContext(ctx, `INSERT INTO process_waits (workspace_id,run_id,definition_rev,step_key,iteration,kind,signal_kind,deadline)
		SELECT workspace_id,id,definition_rev,step_key,transition_rev,'signal','goods_received',TIMESTAMPADD(HOUR,4,UTC_TIMESTAMP(6))
		FROM process_runs WHERE workspace_id=? AND process_id=?`, workspace, processID)
	if err != nil {
		t.Fatal(err)
	}
}
