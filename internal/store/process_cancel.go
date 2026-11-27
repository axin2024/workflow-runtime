package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func terminalProcessStatus(status string) bool {
	return status == "succeeded" || status == "failed" || status == "cancelled"
}

func (s *Store) CancelProcess(ctx context.Context, scope Scope, id int64, request string) (ProcessRun, error) {
	var out ProcessRun
	if scope.validate() != nil || id <= 0 || !processReference.MatchString(request) {
		return out, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return out, err
	}
	out, err = loadProcessRun(ctx, tx, workspace, id, true)
	if err != nil {
		return out, err
	}
	if err = cancelProcessInTx(ctx, tx, &out, scope.Actor, request, out.Status == "failed"); err != nil {
		return out, err
	}
	out, err = loadProcessRun(ctx, tx, workspace, id, false)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func cancelProcessInTx(ctx context.Context, tx *sql.Tx, run *ProcessRun, actor, request string, reverseTerminal bool) error {
	if terminalProcessStatus(run.Status) && !reverseTerminal {
		return nil
	}
	if _, err := processCancellationByRun(ctx, tx, *run); !errors.Is(err, ErrNotFound) {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO process_cancellations (workspace_id,run_id,initiator,request_key,initial_status)
		VALUES (?,?,?,?,?)`, run.workspace, run.ID, actor, request, run.Status)
	if err != nil {
		return err
	}
	event := "cancel_requested"
	if terminalProcessStatus(run.Status) {
		event = "reversal_requested"
	}
	if err := processEvent(ctx, tx, *run, actor, event, request,
		map[string]any{"previous_status": run.Status, "step": run.Step}); err != nil {
		return err
	}
	wait, err := currentProcessWait(ctx, tx, *run)
	if err == nil {
		if _, err = tx.ExecContext(ctx, `UPDATE process_waits SET status='cancelled',winner_key=?,resolved_at=UTC_TIMESTAMP(6)
			WHERE workspace_id=? AND run_id=? AND id=?`, request, run.workspace, run.ID, wait.ID); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE process_tasks SET status='cancelled',claim_rev=claim_rev+1
			WHERE workspace_id=? AND wait_id=?`, run.workspace, wait.ID); err != nil {
			return err
		}
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_child_groups SET status='cancelled',finished_at=UTC_TIMESTAMP(6)
		WHERE workspace_id=? AND parent_run_id=? AND status IN ('starting','waiting')`, run.workspace, run.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_actions SET status='discarded' WHERE workspace_id=? AND run_id=? AND direction='forward' AND status='ready'`,
		run.workspace, run.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_actions SET resolution_owner=? WHERE workspace_id=? AND run_id=? AND status='unknown'`,
		actor, run.workspace, run.ID)
	if err != nil {
		return err
	}
	if !terminalProcessStatus(run.Status) {
		if err = updateProcessStatus(ctx, tx, run, "cancelling"); err != nil {
			return err
		}
	}
	return advanceProcessCancellation(ctx, tx, run)
}

type ProcessCancellation struct {
	Initiator     string     `json:"initiator"`
	Request       string     `json:"request_key"`
	InitialStatus string     `json:"initial_status"`
	Status        string     `json:"status"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

func processCancellationByRun(ctx context.Context, tx *sql.Tx, run ProcessRun) (ProcessCancellation, error) {
	var out ProcessCancellation
	err := tx.QueryRowContext(ctx, `SELECT initiator,request_key,initial_status,status,finished_at FROM process_cancellations
		WHERE workspace_id=? AND run_id=?`, run.workspace, run.ID).Scan(&out.Initiator, &out.Request, &out.InitialStatus, &out.Status, &out.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	return out, err
}

func updateProcessStatus(ctx context.Context, tx *sql.Tx, run *ProcessRun, status string) error {
	run.Status = status
	_, err := tx.ExecContext(ctx, `UPDATE process_runs SET status=? WHERE workspace_id=? AND id=?`, status, run.workspace, run.ID)
	return err
}

func finishProcessCancellation(ctx context.Context, tx *sql.Tx, run *ProcessRun, cancellation ProcessCancellation, state string) error {
	_, err := tx.ExecContext(ctx, `UPDATE process_cancellations SET status=?,finished_at=UTC_TIMESTAMP(6)
		WHERE workspace_id=? AND run_id=?`, state, run.workspace, run.ID)
	if err != nil {
		return err
	}
	if err = processEvent(ctx, tx, *run, cancellation.Initiator, "compensation_finished", cancellation.Request,
		map[string]any{"initial_state": cancellation.InitialStatus, "resolution": state}); err != nil {
		return err
	}
	if terminalProcessStatus(cancellation.InitialStatus) {
		return nil
	}
	run.Status = "cancelled"
	if state == "stopped" {
		run.Status = "failed"
	}
	run.Transition++
	_, err = tx.ExecContext(ctx, `UPDATE process_runs SET status=?,transition_rev=? WHERE workspace_id=? AND id=?`,
		run.Status, run.Transition, run.workspace, run.ID)
	if err != nil {
		return err
	}
	return processEvent(ctx, tx, *run, cancellation.Initiator, "run_finished", cancellation.Request,
		map[string]any{"status": run.Status, "step": run.Step})
}
