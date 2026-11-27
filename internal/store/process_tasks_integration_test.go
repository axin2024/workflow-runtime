package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/axin2024/workflow-runtime/internal/process"
)

func TestMySQLProcessApprovalClaimAndDelegation(t *testing.T) {
	db, _ := openTestStore(t)
	scope, version, buyer, manager := approvalFixture(t, db)
	ctx := t.Context()
	first, task := newApprovalRun(t, db, scope, version, "PO-claims")
	type claimResult struct {
		actor Scope
		task  ProcessTask
		err   error
	}
	claims := make(chan claimResult, 2)
	for _, actor := range []Scope{buyer, manager} {
		go func() {
			result, err := db.ChangeProcessTask(ctx, actor, task.ID, "claim-"+actor.Actor,
				ProcessTaskInput{Action: "claim", Revision: task.Revision, ClaimSecs: 3600})
			claims <- claimResult{actor, result, err}
		}()
	}
	winner, loser := <-claims, <-claims
	if winner.err != nil {
		winner, loser = loser, winner
	}
	if winner.err != nil || !errors.Is(loser.err, ErrProcessConflict) || winner.task.Revision != 2 {
		t.Fatalf("claim results winner=%+v loser=%+v", winner, loser)
	}
	if _, err := db.ChangeProcessTask(ctx, loser.actor, task.ID, "wrong-actor", ProcessTaskInput{Action: "approve", Revision: 2}); !errors.Is(err, ErrProcessConflict) {
		t.Fatalf("non-holder decided: %v", err)
	}
	delegated, err := db.ChangeProcessTask(ctx, winner.actor, task.ID, "delegate", ProcessTaskInput{
		Action: "delegate", Revision: 2, Assignee: loser.actor.Actor, ClaimSecs: 3600,
	})
	if err != nil || delegated.Revision != 3 || delegated.Assigned == nil || *delegated.Assigned != loser.actor.Actor {
		t.Fatalf("delegate: %+v %v", delegated, err)
	}
	queue, err := db.ProcessTaskQueue(ctx, loser.actor)
	if err != nil || len(queue) != 1 || queue[0].ID != task.ID || queue[0].Revision != 3 {
		t.Fatalf("delegated inbox: %+v %v", queue, err)
	}
	if _, err = db.db.ExecContext(ctx, `UPDATE process_tasks SET claim_until=TIMESTAMPADD(SECOND,-1,UTC_TIMESTAMP(6)) WHERE id=?`, task.ID); err != nil {
		t.Fatal(err)
	}
	stale := ProcessTaskInput{Action: "approve", Revision: 3}
	if _, err = db.ChangeProcessTask(ctx, loser.actor, task.ID, "expired-form", stale); !errors.Is(err, ErrProcessConflict) {
		t.Fatalf("expired form submitted: %v", err)
	}
	claimed, err := db.ChangeProcessTask(ctx, winner.actor, task.ID, "reclaim", ProcessTaskInput{Action: "claim", Revision: 3, ClaimSecs: 3600})
	if err != nil || claimed.Revision != 4 {
		t.Fatalf("reclaim: %+v %v", claimed, err)
	}
	decision := ProcessTaskInput{Action: "approve", Revision: 4, Note: "credit approved"}
	approved, err := db.ChangeProcessTask(ctx, winner.actor, task.ID, "approve-current", decision)
	if err != nil || approved.Status != "approved" || approved.CompletedBy == nil || *approved.CompletedBy != winner.actor.Actor {
		t.Fatalf("approved: %+v %v", approved, err)
	}
	replay, err := db.ChangeProcessTask(ctx, winner.actor, task.ID, "approve-current", decision)
	if err != nil || replay.Revision != approved.Revision || replay.CompletedAt == nil || !replay.CompletedAt.Equal(*approved.CompletedAt) {
		t.Fatalf("decision replay: %+v %v", replay, err)
	}
	final, err := db.ProcessRun(ctx, scope, first.ID)
	if err != nil || final.Step != "approved" || final.Transition != 2 {
		t.Fatalf("final: %+v %v", final, err)
	}
	t.Logf("task=%d initial_holder=%s delegated=%s stale_revision=%d final_revision=%d decided_by=%s", task.ID,
		winner.actor.Actor, loser.actor.Actor, delegated.Revision, approved.Revision, *approved.CompletedBy)
}

func TestMySQLProcessApprovalTimerRace(t *testing.T) {
	db, dsn := openTestStore(t)
	scope, version, buyer, _ := approvalFixture(t, db)
	ctx := t.Context()
	for _, order := range []string{"approval_first", "timer_first", "parallel"} {
		t.Run(order, func(t *testing.T) {
			approvalWins, timerWins := 0, 0
			for i := range 100 {
				run, task := newApprovalRun(t, db, scope, version, fmt.Sprintf("%s-%d", order, i))
				claimed, err := db.ChangeProcessTask(ctx, buyer, task.ID, "claim", ProcessTaskInput{Action: "claim", Revision: 1, ClaimSecs: 3600})
				if err != nil {
					t.Fatal(err)
				}
				approvalKey := fmt.Sprintf("approve-%d", run.ID)
				timerKey := fmt.Sprintf("timer:%d", task.Wait)
				approval := func() error {
					_, err := db.ChangeProcessTask(ctx, buyer, task.ID, approvalKey, ProcessTaskInput{Action: "approve", Revision: claimed.Revision})
					return err
				}
				timer := func() (bool, error) {
					return db.observeProcessTimer(ctx, timerCandidate{run.workspace, run.ID, task.Wait}, task.Deadline)
				}
				var approved, elapsed error
				var timedOut bool
				switch order {
				case "approval_first":
					approved = approval()
					timedOut, elapsed = timer()
				case "timer_first":
					timedOut, elapsed = timer()
					approved = approval()
				default:
					start := make(chan struct{})
					done := make(chan error, 1)
					go func() {
						<-start
						done <- approval()
					}()
					close(start)
					timedOut, elapsed = timer()
					approved = <-done
				}
				if elapsed != nil || (approved != nil && !errors.Is(approved, ErrProcessConflict)) || timedOut == (approved == nil) {
					t.Fatalf("both or neither won run=%d timer=%v/%v approval=%v", run.ID, timedOut, elapsed, approved)
				}
				if (order == "approval_first" && timedOut) || (order == "timer_first" && !timedOut) {
					t.Fatalf("wrong first input: run=%d timer=%v", run.ID, timedOut)
				}
				restarted, err := Open(dsn)
				if err != nil {
					t.Fatal(err)
				}
				won, err := restarted.observeProcessTimer(ctx, timerCandidate{run.workspace, run.ID, task.Wait}, task.Deadline)
				if err != nil || won {
					restarted.Close()
					t.Fatalf("restart advanced the same wait: %v %v", won, err)
				}
				final, err := restarted.ProcessRun(ctx, scope, run.ID)
				restarted.Close()
				want, approvalEvent, timerEvent := "approved", "task_approve", "timer_late"
				if timedOut {
					want, approvalEvent, timerEvent = "expired", "task_input_rejected", "wait_resolved"
					timerWins++
				} else {
					approvalWins++
				}
				if err != nil || final.Step != want || final.Transition != 2 {
					t.Fatalf("final: %+v %v", final, err)
				}
				var terminal, resolved, seenApproval, seenTimer int
				err = db.db.QueryRowContext(ctx, `SELECT COUNT(CASE WHEN kind='run_finished' THEN 1 END),
					COUNT(CASE WHEN kind='wait_resolved' THEN 1 END),COUNT(CASE WHEN kind=? AND request_key=? THEN 1 END),
					COUNT(CASE WHEN kind=? AND request_key=? THEN 1 END) FROM process_events WHERE run_id=?`,
					approvalEvent, approvalKey, timerEvent, timerKey, run.ID).Scan(&terminal, &resolved, &seenApproval, &seenTimer)
				if err != nil || terminal != 1 || resolved != 1 || seenApproval != 1 || seenTimer != 1 {
					t.Fatalf("evidence run=%d terminal=%d resolved=%d approval=%d timer=%d %v", run.ID, terminal, resolved, seenApproval, seenTimer, err)
				}
			}
			t.Logf("approval_won=%d timer_won=%d restarted_runs=100 duplicate_transitions=0", approvalWins, timerWins)
		})
	}
}

func approvalFixture(t *testing.T, db *Store) (Scope, ProcessVersion, Scope, Scope) {
	t.Helper()
	ctx := t.Context()
	scope := newTestWorkspace(t, db)
	buyer := Scope{Workspace: scope.Workspace, Actor: "buyer"}
	manager := Scope{Workspace: scope.Workspace, Actor: "manager"}
	for _, actor := range []Scope{buyer, manager} {
		if _, err := db.GrantConsole(ctx, actor, ConsoleViewer); err != nil {
			t.Fatal(err)
		}
		if err := db.SetProcessDuty(ctx, scope, actor.Actor, "credit"); err != nil {
			t.Fatal(err)
		}
	}
	version, err := db.PublishProcess(ctx, scope, "credit", 0, process.Definition{Start: "credit-check", Steps: []process.Step{
		{Key: "credit-check", Kind: process.Approval, Next: "approved", OnFailure: "rejected", OnTimeout: "expired", AfterSeconds: 3600, Role: "credit"},
		{Key: "approved", Kind: process.End, Result: "succeeded"}, {Key: "rejected", Kind: process.End, Result: "failed"},
		{Key: "expired", Kind: process.End, Result: "failed"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return scope, version, buyer, manager
}

func newApprovalRun(t *testing.T, db *Store, scope Scope, definition ProcessVersion, business string) (ProcessRun, ProcessTask) {
	t.Helper()
	run, err := db.StartProcess(t.Context(), scope, definition.Key, NewProcessRun{Business: business})
	if err != nil {
		t.Fatal(err)
	}
	task, err := readProcessTask(db.db.QueryRowContext(t.Context(), `SELECT `+processTaskColumns+`
		FROM process_tasks t JOIN process_waits w ON w.id=t.wait_id WHERE t.workspace_id=? AND t.run_id=?`, run.workspace, run.ID))
	if err != nil {
		t.Fatal(err)
	}
	return run, task
}
