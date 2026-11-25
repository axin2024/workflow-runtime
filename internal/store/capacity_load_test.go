package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMySQLCapacityLoad(t *testing.T) {
	if os.Getenv("WORKFLOW_CAPACITY_LOAD") != "1" {
		t.Skip("set WORKFLOW_CAPACITY_LOAD=1 to run the 1000-workflow capacity test")
	}
	db, _ := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	suffix := strings.ToLower(rand.Text())
	pool := "load-" + suffix
	scopes := []Scope{
		{Workspace: "load-a-" + suffix, Actor: "load-operator"},
		{Workspace: "load-b-" + suffix, Actor: "load-operator"},
	}
	const workers, slots = 10, 20
	if _, err := db.PutPoolCapacity(ctx, scopes[0].Actor, pool, 0, slots); err != nil {
		t.Fatal(err)
	}
	const graph = `{"key":"capacity-load","nodes":[
		{"key":"one","task":"load","queue":"load","timeout_seconds":120,"effect":"none","retry":{"max_attempts":2}},
		{"key":"two","task":"load","queue":"load","timeout_seconds":120,"effect":"none","retry":{"max_attempts":1},"depends_on":[{"node":"one","when":"succeeded"}]},
		{"key":"three","task":"load","queue":"load","timeout_seconds":120,"effect":"none","retry":{"max_attempts":1},"depends_on":[{"node":"two","when":"succeeded"}]},
		{"key":"four","task":"load","queue":"load","timeout_seconds":120,"effect":"none","retry":{"max_attempts":1},"depends_on":[{"node":"three","when":"succeeded"}]},
		{"key":"five","task":"load","queue":"load","timeout_seconds":120,"effect":"none","retry":{"max_attempts":1},"depends_on":[{"node":"four","when":"succeeded"}]}]}`
	for _, scope := range scopes {
		if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Publish(ctx, scope, 0, []byte(graph)); err != nil {
			t.Fatal(err)
		}
		limit := 12
		if _, err := db.PutWorkspaceCapacity(ctx, scope, 0, &limit); err != nil {
			t.Fatal(err)
		}
		if _, err := db.PutQueueCapacity(ctx, scope, "load", 0, QueueCapacitySpec{MaxActive: &limit, Pool: pool}); err != nil {
			t.Fatal(err)
		}
	}
	var tokens []string
	for index := range workers {
		worker, err := db.RegisterWorker(ctx, scopes[index%len(scopes)], fmt.Sprintf("runner-%d", index), 3,
			[]WorkerPermission{{Queue: "load", Task: "load"}})
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, worker.Token)
	}
	seed := func(start, count int) {
		t.Helper()
		for offset := range count {
			index := start + offset
			if _, err := db.Trigger(ctx, scopes[index%len(scopes)], TriggerRequest{
				DAG: "capacity-load", Key: fmt.Sprintf("run-%04d", index), Kind: "manual", Inputs: []byte(`{}`),
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	seed(0, 50)
	warmup := runCapacityPhase(t, ctx, db, tokens, 250, slots, nil)
	logCapacityPhase(t, "warmup", warmup)
	seed(50, 950)
	stale, err := db.Claim(ctx, tokens[0], ClaimRequest{Tasks: []string{"load"}, LeaseSeconds: 1})
	if err != nil || stale == nil {
		t.Fatalf("seed a lease to expire: %+v, %v", stale, err)
	}
	for {
		state, err := db.GetPoolCapacity(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		if state.Active == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	checkStale := func() {
		t.Helper()
		for range 20 {
			if receipt, err := db.Complete(ctx, tokens[0], CompletionRequest{Lease: stale.LeaseRef, Success: true}); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("stale attempt accepted or returned an unrelated error: %+v, %v", receipt, err)
			}
		}
	}
	steady := runCapacityPhase(t, ctx, db, tokens, 4750, slots, func() {
		state, err := db.GetPoolCapacity(ctx, pool)
		if err != nil || state.Active != slots {
			t.Fatalf("live lease saturation: %+v, %v", state, err)
		}
		checkStale()
	})
	logCapacityPhase(t, "steady", steady)
	checkStale()
	recovered, err := db.GetRun(ctx, scopes[0], stale.RunID)
	if err != nil || recovered.Run.State != "succeeded" {
		t.Fatalf("recovered run: %+v, %v", recovered.Run, err)
	}
	retried := false
	for _, node := range recovered.Nodes {
		if node.Key == stale.Node && node.OperationKey == stale.OperationKey && len(node.Attempts) == 2 &&
			node.Attempts[0].State == "expired" && node.Attempts[1].State == "succeeded" {
			retried = true
		}
	}
	if !retried {
		t.Fatal("expired task did not recover with the same operation key and a new epoch")
	}
	verifyCapacityHistory(t, ctx, db, pool, slots, 5000)
}

type capacitySample struct{ claim, complete time.Duration }

type capacityPhase struct {
	elapsed time.Duration
	samples []capacitySample
	idle    int64
}

func runCapacityPhase(t *testing.T, parent context.Context, db *Store, tokens []string, expected, slots int, onSaturated func()) capacityPhase {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	results := make(chan capacitySample, len(tokens)*3)
	failures := make(chan error, len(tokens)*3)
	saturated, release := make(chan struct{}), make(chan struct{})
	var arrived, idle atomic.Int64
	begin := time.Now()
	for _, token := range tokens {
		for range 3 {
			workers.Go(func() {
				for ctx.Err() == nil {
					start := time.Now()
					task, err := db.Claim(ctx, token, ClaimRequest{Tasks: []string{"load"}, LeaseSeconds: 30})
					if err != nil {
						if ctx.Err() == nil {
							failures <- fmt.Errorf("claim: %w", err)
						}
						return
					}
					claimTime := time.Since(start)
					if task == nil {
						idle.Add(1)
						select {
						case <-ctx.Done():
						case <-time.After(2 * time.Millisecond):
						}
						continue
					}
					if onSaturated != nil {
						if arrived.Add(1) == int64(slots) {
							close(saturated)
						}
						select {
						case <-release:
						case <-ctx.Done():
							return
						}
					}
					start = time.Now()
					receipt, err := db.Complete(ctx, token, CompletionRequest{Lease: task.LeaseRef, Success: true})
					if err != nil || receipt.State != "succeeded" || receipt.Replayed {
						if ctx.Err() == nil {
							failures <- fmt.Errorf("completion: %v, error=%v", receipt.State, err)
						}
						return
					}
					select {
					case results <- capacitySample{claimTime, time.Since(start)}:
					case <-ctx.Done():
						return
					}
				}
			})
		}
	}
	if onSaturated != nil {
		select {
		case <-saturated:
			onSaturated()
			close(release)
		case err := <-failures:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	samples := make([]capacitySample, 0, expected)
	for len(samples) < expected {
		select {
		case sample := <-results:
			samples = append(samples, sample)
		case err := <-failures:
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatalf("completed %d of %d nodes: %v", len(samples), expected, ctx.Err())
		}
	}
	return capacityPhase{time.Since(begin), samples, idle.Load()}
}

func logCapacityPhase(t *testing.T, name string, result capacityPhase) {
	t.Helper()
	claim := make([]time.Duration, 0, len(result.samples))
	complete := make([]time.Duration, 0, len(result.samples))
	for _, sample := range result.samples {
		claim, complete = append(claim, sample.claim), append(complete, sample.complete)
	}
	slices.Sort(claim)
	slices.Sort(complete)
	index := (len(claim)*95+99)/100 - 1
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	t.Logf("%s: nodes=%d elapsed=%s nodes/s=%.1f claim_p95=%s complete_p95=%s idle_claims=%d errors=0 go_heap_mib=%.1f cpus=%d",
		name, len(claim), result.elapsed.Round(time.Millisecond), float64(len(claim))/result.elapsed.Seconds(),
		claim[index].Round(time.Microsecond), complete[index].Round(time.Microsecond), result.idle,
		float64(memory.HeapAlloc)/(1<<20), runtime.NumCPU())
}

func verifyCapacityHistory(t *testing.T, ctx context.Context, db *Store, pool string, slots, expected int) {
	t.Helper()
	rows, err := db.db.QueryContext(ctx, `SELECT a.run_id, a.started_at, LEAST(a.lease_until, a.finished_at), a.state, r.state
		FROM attempts a JOIN execution_pools p ON p.id = a.pool_id JOIN runs r ON r.id = a.run_id WHERE p.pool_key = ?`, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	type event struct {
		at    time.Time
		delta int
	}
	var events []event
	successes, expired := 0, 0
	runs := make(map[int64]bool)
	for rows.Next() {
		var run int64
		var start, end time.Time
		var attempt, outcome string
		if err := rows.Scan(&run, &start, &end, &attempt, &outcome); err != nil {
			t.Fatal(err)
		}
		if outcome != "succeeded" {
			t.Fatalf("run %d remained in state %s", run, outcome)
		}
		runs[run] = true
		switch attempt {
		case "succeeded":
			successes++
		case "expired":
			expired++
		default:
			t.Fatalf("unexpected attempt state: %s", attempt)
		}
		if end.After(start) {
			events = append(events, event{start, 1}, event{end, -1})
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(events, func(a, b event) int {
		if a.at.Equal(b.at) {
			return a.delta - b.delta
		}
		return a.at.Compare(b.at)
	})
	active, maximum := 0, 0
	for _, item := range events {
		active += item.delta
		maximum = max(maximum, active)
	}
	if len(runs) != expected/5 || successes != expected || expired != 1 || active != 0 || maximum != slots {
		t.Fatalf("history: runs=%d completed=%d expired=%d peak=%d live_after=%d", len(runs), successes, expired, maximum, active)
	}
	t.Logf("history: runs=%d completed=%d expired=%d peak_active=%d live_after=%d stale_rejected=40", len(runs), successes, expired, maximum, active)
}
