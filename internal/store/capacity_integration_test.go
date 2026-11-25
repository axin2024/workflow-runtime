package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestMySQLSharedPoolEnforcesTenantAndQueueCapacity(t *testing.T) {
	db, _ := openTestStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	id := strings.ToLower(rand.Text())
	first := Scope{Workspace: "capacity-a-" + id, Actor: "capacity-operator"}
	second := Scope{Workspace: "capacity-b-" + id, Actor: first.Actor}
	pool := "shared-" + id
	for _, scope := range []Scope{first, second} {
		if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
			t.Fatal(err)
		}
	}
	if result, err := db.PutPoolCapacity(ctx, first.Actor, pool, 0, 3); err != nil || result.Generation != 1 {
		t.Fatalf("create pool: %+v, %v", result, err)
	}
	workspaceLimitA, workspaceLimitB := 3, 4
	if _, err := db.PutWorkspaceCapacity(ctx, first, 0, &workspaceLimitA); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PutWorkspaceCapacity(ctx, second, 0, &workspaceLimitB); err != nil {
		t.Fatal(err)
	}
	firstQueueLimit, secondQueueLimit := 2, 4
	for _, config := range []struct {
		scope Scope
		queue string
		limit *int
		pool  string
	}{
		{first, "primary", &firstQueueLimit, pool},
		{first, "local", nil, ""},
		{second, "primary", &secondQueueLimit, pool},
	} {
		result, err := db.PutQueueCapacity(ctx, config.scope, config.queue, 0, QueueCapacitySpec{MaxActive: config.limit, Pool: config.pool})
		if err != nil || result.Generation != 1 || result.Pool != config.pool || result.Active != 0 {
			t.Fatalf("configure queue: %+v, %v", result, err)
		}
	}
	seed := func(scope Scope, queue string, runs int) {
		t.Helper()
		graph := []byte(fmt.Sprintf(`{"key":%q,"nodes":[
		 {"key":"work","task":%q,"queue":%q,"effect":"none","timeout_seconds":60,"retry":{"max_attempts":2}}]}`, queue, queue, queue))
		if _, err := db.Publish(ctx, scope, 0, graph); err != nil {
			t.Fatal(err)
		}
		for index := range runs {
			request := TriggerRequest{DAG: queue, Key: fmt.Sprintf("seed-%d", index), Kind: "manual", Inputs: []byte(`{}`)}
			if _, err := db.Trigger(ctx, scope, request); err != nil {
				t.Fatal(err)
			}
		}
	}
	seed(first, "primary", 42)
	seed(first, "local", 2)
	seed(second, "primary", 18)
	register := func(scope Scope, key string, permissions ...WorkerPermission) string {
		t.Helper()
		worker, err := db.RegisterWorker(ctx, scope, key, 16, permissions)
		if err != nil {
			t.Fatal(err)
		}
		return worker.Token
	}
	primary := WorkerPermission{Queue: "primary", Task: "primary"}
	firstToken := register(first, "first", primary, WorkerPermission{Queue: "local", Task: "local"})
	secondToken := register(second, "second", primary)
	claim := func(token string, seconds int, tasks ...string) *ClaimedTask {
		t.Helper()
		task, err := db.Claim(ctx, token, ClaimRequest{Tasks: tasks, LeaseSeconds: seconds})
		if err != nil {
			t.Fatal(err)
		}
		return task
	}
	finish := func(token string, task *ClaimedTask) {
		t.Helper()
		if _, err := db.Complete(ctx, token, CompletionRequest{Lease: task.LeaseRef, Success: true}); err != nil {
			t.Fatal(err)
		}
	}
	held := claim(firstToken, 30, "primary")
	completable := claim(firstToken, 30, "primary")
	if held == nil || completable == nil || held.Pool != pool || completable.Pool != pool || claim(firstToken, 30, "primary") != nil {
		t.Fatalf("primary queue limit: first=%+v, second=%+v", held, completable)
	}
	local := claim(firstToken, 30, "primary", "local")
	if local == nil || local.Queue != "local" || local.Pool != "" {
		t.Fatalf("full queue starved local work beyond first page: %+v", local)
	}
	if claim(firstToken, 30, "local") != nil {
		t.Fatal("worker exceeded workspace capacity across queues")
	}
	finish(firstToken, completable)
	renewer, err := db.Renew(ctx, firstToken, RenewRequest{Lease: held.LeaseRef, LeaseSeconds: 45})
	if err != nil || !renewer.LeaseUntil.After(held.LeaseUntil) {
		t.Fatalf("renew pooled lease: %+v, %v", renewer, err)
	}

	type contender struct {
		scope Scope
		token string
	}
	var contenders []contender
	for _, scope := range []Scope{first, second} {
		for index := range 8 {
			contenders = append(contenders, contender{scope, register(scope, fmt.Sprintf("contender-%d", index), primary)})
		}
	}
	type grant struct {
		worker contender
		task   *ClaimedTask
		err    error
	}
	granted := make(chan grant, len(contenders))
	start := make(chan struct{})
	for _, worker := range contenders {
		go func() {
			<-start
			var task *ClaimedTask
			var err error
			for range 5 {
				task, err = db.Claim(ctx, worker.token, ClaimRequest{Tasks: []string{"primary"}, LeaseSeconds: 30})
				if task != nil || err != nil {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			granted <- grant{worker, task, err}
		}()
	}
	close(start)
	grants := []grant{{contender{first, firstToken}, held, nil}}
	for range len(contenders) {
		result := <-granted
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.task != nil {
			grants = append(grants, result)
		}
	}
	if len(grants) != 3 {
		t.Fatalf("expected exactly three grants across both workspaces, got %d", len(grants))
	}
	byTenant := make(map[string]int)
	for _, result := range grants {
		byTenant[result.worker.scope.Workspace]++
		if result.task.Pool != pool {
			t.Fatalf("wrong pinned pool: %+v", result.task)
		}
	}
	if byTenant[first.Workspace] < 1 || byTenant[first.Workspace] > 2 || byTenant[second.Workspace] < 1 {
		t.Fatalf("unexpected tenant allocation: %+v", byTenant)
	}
	shared, err := db.GetPoolCapacity(ctx, pool)
	if err != nil || shared.Active != 3 || shared.MaxActive == nil || *shared.MaxActive != 3 {
		t.Fatalf("pool usage readback: %+v, %v", shared, err)
	}
	own, err := db.GetWorkspaceCapacity(ctx, first)
	if err != nil || own.MaxActive == nil || own.Active != byTenant[first.Workspace]+1 || own.Active > *own.MaxActive {
		t.Fatalf("workspace usage readback: %+v, %v", own, err)
	}
	actualQueue, err := db.GetQueueCapacity(ctx, second, "primary")
	if err != nil || actualQueue.Active != byTenant[second.Workspace] || actualQueue.Pool != pool {
		t.Fatalf("queue usage readback: %+v, %v", actualQueue, err)
	}
	if _, err := db.PutPoolCapacity(ctx, first.Actor, pool, shared.Generation, 2); !errors.Is(err, ErrCapacityConflict) {
		t.Fatalf("pool lowered below usage: %v", err)
	}
	if _, err := db.PutPoolCapacity(ctx, first.Actor, pool, 0, 7); !errors.Is(err, ErrCapacityConflict) {
		t.Fatalf("stale pool generation accepted: %v", err)
	}
	if _, err := db.PutQueueCapacity(ctx, second, "primary", actualQueue.Generation, QueueCapacitySpec{MaxActive: &secondQueueLimit}); !errors.Is(err, ErrCapacityConflict) {
		t.Fatalf("busy queue moved out of shared pool: %v", err)
	}
	finish(firstToken, held)
	replacement := claim(secondToken, 1, "primary")
	if replacement == nil {
		t.Fatal("pool slot not reclaimed after completion")
	}
	detail, err := db.GetRun(ctx, first, held.RunID)
	if err != nil || len(detail.Nodes) != 1 || len(detail.Nodes[0].Attempts) != 1 ||
		detail.Nodes[0].Attempts[0].Pool == nil || *detail.Nodes[0].Attempts[0].Pool != pool {
		t.Fatalf("pool history: %+v, %v", detail.Nodes, err)
	}
	limit := time.Now().Add(5 * time.Second)
	for {
		shared, err := db.GetPoolCapacity(ctx, pool)
		if err != nil {
			t.Fatal(err)
		}
		if shared.Active == 2 {
			break
		}
		if time.Now().After(limit) {
			t.Fatal("expired lease still occupies the pool")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if task := claim(secondToken, 30, "primary"); task == nil {
		t.Fatal("pool slot not reclaimed after lease expiry")
	}
	if _, err := db.Complete(ctx, secondToken, CompletionRequest{Lease: replacement.LeaseRef, Success: true}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("accepted completion of expired pooled lease: %v", err)
	}
}
