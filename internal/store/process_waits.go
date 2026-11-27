package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

type ProcessWait struct {
	ID          int64      `json:"wait_id"`
	Revision    int        `json:"definition_rev"`
	Step        string     `json:"step_key"`
	Iteration   uint64     `json:"iteration"`
	Kind        string     `json:"kind"`
	SignalKind  string     `json:"signal_kind,omitempty"`
	Status      string     `json:"status"`
	Deadline    time.Time  `json:"deadline"`
	Winner      *string    `json:"winner_key,omitempty"`
	ResolvedAt  *time.Time `json:"resolved_at,omitempty"`
	TimerSeenAt *time.Time `json:"timer_observed_at,omitempty"`
}

const processWaitColumns = "id,definition_rev,step_key,iteration,kind,signal_kind,status,deadline,winner_key,resolved_at,timer_observed_at"

func scanProcessWait(row interface{ Scan(...any) error }) (ProcessWait, error) {
	var out ProcessWait
	err := row.Scan(&out.ID, &out.Revision, &out.Step, &out.Iteration, &out.Kind, &out.SignalKind, &out.Status,
		&out.Deadline, &out.Winner, &out.ResolvedAt, &out.TimerSeenAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	return out, err
}

func currentProcessWait(ctx context.Context, tx *sql.Tx, run ProcessRun) (ProcessWait, error) {
	return scanProcessWait(tx.QueryRowContext(ctx, `SELECT `+processWaitColumns+` FROM process_waits WHERE workspace_id=? AND active_run=?`,
		run.workspace, run.ID))
}

func createProcessWait(ctx context.Context, tx *sql.Tx, run *ProcessRun, definition process.Definition, step process.Step) error {
	_, err := insertProcessWait(ctx, tx, run, definition, step, nil)
	return err
}

func insertProcessWait(ctx context.Context, tx *sql.Tx, run *ProcessRun, definition process.Definition, step process.Step, deadline *time.Time) (int64, error) {
	_, err := tx.ExecContext(ctx, `INSERT INTO process_waits (workspace_id,run_id,definition_rev,step_key,iteration,kind,signal_kind,deadline)
		VALUES (?,?,?,?,?,?,?,COALESCE(?,TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6))))`,
		run.workspace, run.ID, run.Revision, step.Key, run.Transition, step.Kind, step.SignalKind, deadline, step.AfterSeconds)
	if err != nil {
		return 0, err
	}
	wait, err := currentProcessWait(ctx, tx, *run)
	if err != nil {
		return 0, err
	}
	if err = processEvent(ctx, tx, *run, "scheduler", "wait_created", "", wait); err != nil {
		return wait.ID, err
	}
	if step.Kind == process.Signal {
		return wait.ID, matchPendingProcessSignal(ctx, tx, run, definition, wait)
	}
	if step.Kind == process.Approval {
		return wait.ID, createProcessTask(ctx, tx, *run, wait, step.Role)
	}
	return wait.ID, nil
}

func resolveProcessWait(ctx context.Context, tx *sql.Tx, run *ProcessRun, def process.Definition, wait ProcessWait, actor, state, key string) error {
	step, ok := def.Step(wait.Step)
	if !ok || run.Step != wait.Step || run.Revision != wait.Revision || run.Status != "waiting" ||
		run.Transition != wait.Iteration || wait.Status != "waiting" {
		return ErrProcessConflict
	}
	next := step.Next
	switch state {
	case "accepted":
	case "rejected":
		next = step.OnFailure
	case "timeout":
		if step.Kind != process.Timer {
			next = step.OnTimeout
		}
	default:
		return ErrProcessConflict
	}
	if next == "" {
		return ErrProcessConflict
	}
	_, err := tx.ExecContext(ctx, `UPDATE process_waits SET status=?,winner_key=?,resolved_at=UTC_TIMESTAMP(6)
		WHERE workspace_id=? AND run_id=? AND id=?`, state, key, run.workspace, run.ID, wait.ID)
	if err != nil {
		return err
	}
	if err = processEvent(ctx, tx, *run, actor, "wait_resolved", key,
		map[string]any{"wait_id": wait.ID, "step": wait.Step, "state": state, "successor": next}); err != nil {
		return err
	}
	run.Step = next
	run.Transition++
	return enterProcessStep(ctx, tx, run, def)
}

type timerCandidate struct {
	workspace, run, wait int64
}

func (s *Store) ProcessDueWaits(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	if limit < 1 || limit > 256 {
		return 0, ErrInvalidProcess
	}
	var clock any
	if !cutoff.IsZero() {
		clock = cutoff.UTC()
	}
	rows, err := s.db.QueryContext(ctx, `SELECT workspace_id,run_id,id FROM process_waits
		WHERE watch_deadline<=COALESCE(?,UTC_TIMESTAMP(6)) ORDER BY watch_deadline,id LIMIT ?`, clock, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	candidates := make([]timerCandidate, 0, limit)
	for rows.Next() {
		var item timerCandidate
		if err = rows.Scan(&item.workspace, &item.run, &item.wait); err != nil {
			return 0, err
		}
		candidates = append(candidates, item)
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	resolved := 0
	for _, item := range candidates {
		won, err := s.observeProcessTimer(ctx, item, clock)
		if err != nil {
			return resolved, err
		}
		if won {
			resolved++
		}
	}
	return resolved, nil
}

func (s *Store) observeProcessTimer(ctx context.Context, candidate timerCandidate, cutoff any) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	run, err := loadProcessRun(ctx, tx, candidate.workspace, candidate.run, true)
	if err != nil {
		return false, err
	}
	wait, err := scanProcessWait(tx.QueryRowContext(ctx, `SELECT `+processWaitColumns+` FROM process_waits
		WHERE workspace_id=? AND run_id=? AND id=? AND deadline<=COALESCE(?,UTC_TIMESTAMP(6))`,
		run.workspace, run.ID, candidate.wait, cutoff))
	if errors.Is(err, ErrNotFound) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	if wait.TimerSeenAt != nil {
		return false, tx.Commit()
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_waits SET timer_observed_at=UTC_TIMESTAMP(6) WHERE id=?`, wait.ID)
	if err != nil {
		return false, err
	}
	key := fmt.Sprintf("timer:%d", wait.ID)
	won := wait.Status == "waiting"
	if !won {
		if err = processEvent(ctx, tx, run, "scheduler", "timer_late", key,
			map[string]any{"wait_id": wait.ID, "winning_outcome": wait.Status}); err != nil {
			return false, err
		}
		return false, tx.Commit()
	}
	if wait.Kind == process.Approval {
		if err = expireProcessTask(ctx, tx, run, wait); err != nil {
			return false, err
		}
	}
	version, err := loadProcessVersion(ctx, tx, run.workspace, run.Key, run.Revision)
	if err != nil {
		return false, err
	}
	if err = resolveProcessWait(ctx, tx, &run, version.Definition, wait, "scheduler", "timeout", key); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
