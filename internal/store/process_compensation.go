package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"github.com/axin2024/workflow-runtime/internal/process"
)

func advanceProcessCancellation(ctx context.Context, tx *sql.Tx, run *ProcessRun) error {
	cancellation, err := processCancellationByRun(ctx, tx, *run)
	if err != nil || cancellation.Status != "waiting" {
		return err
	}
	// Within a run only the newest outstanding effect or child group may be reversed.
	for range process.MaxSteps + 1 {
		action, err := processActionToReverse(ctx, tx, *run)
		if err != nil {
			return err
		}
		var group uint64
		var groupStatus string
		err = tx.QueryRowContext(ctx, `SELECT iteration,reversal_status FROM process_child_groups
			WHERE workspace_id=? AND parent_run_id=? AND reversal_status<>'succeeded' ORDER BY iteration DESC LIMIT 1`,
			run.workspace, run.ID).Scan(&group, &groupStatus)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if group > action.iteration {
			state, err := advanceChildReversal(ctx, tx, *run, cancellation, group, groupStatus)
			if err != nil {
				return err
			}
			switch state {
			case "succeeded":
				continue
			case "stopped":
				return finishProcessCancellation(ctx, tx, run, cancellation, "stopped")
			default:
				return delayProcessCancellationScan(ctx, tx, *run)
			}
		}
		if action.id == 0 {
			return finishProcessCancellation(ctx, tx, run, cancellation, "succeeded")
		}
		if action.state == "unknown" || action.reverseState == "unknown" {
			return reconcileProcessCancellation(ctx, tx, run, cancellation)
		}
		if action.state == "succeeded" && action.reverseState == "" {
			if err = queueProcessReversal(ctx, tx, *run, action); err != nil {
				return err
			}
		}
		return delayProcessCancellationScan(ctx, tx, *run)
	}
	return ErrProcessConflict
}

type actionToReverse struct {
	id           int64
	iteration    uint64
	step         string
	state        string
	key          string
	operation    string
	receipt      string
	reverseState string
}

func processActionToReverse(ctx context.Context, tx *sql.Tx, run ProcessRun) (actionToReverse, error) {
	var action actionToReverse
	err := tx.QueryRowContext(ctx, `SELECT a.id,a.iteration,a.step_key,a.status,a.operation_key,a.reverse_operation,
		COALESCE(a.external_receipt,''),COALESCE(r.status,'') FROM process_actions a
		LEFT JOIN process_actions r ON r.workspace_id=a.workspace_id AND r.original_id=a.id
		WHERE a.workspace_id=? AND a.run_id=? AND a.direction='forward' AND (a.status IN ('leased','unknown') OR
		  (a.status='succeeded' AND a.reverse_operation<>'' AND (r.id IS NULL OR r.status<>'succeeded')))
		ORDER BY a.iteration DESC LIMIT 1`, run.workspace, run.ID).Scan(&action.id, &action.iteration, &action.step, &action.state,
		&action.key, &action.operation, &action.receipt, &action.reverseState)
	if errors.Is(err, sql.ErrNoRows) {
		return action, nil
	}
	return action, err
}

func queueProcessReversal(ctx context.Context, tx *sql.Tx, run ProcessRun, action actionToReverse) error {
	digest := sha256.Sum256(fmt.Appendf(nil, "workflow-compensation-v1\x00%s\x00%s", action.key, action.receipt))
	key := fmt.Sprintf("undo-%x", digest)
	result, err := tx.ExecContext(ctx, `INSERT INTO process_actions
		(workspace_id,run_id,step_key,iteration,direction,original_id,operation_name,operation_key)
		VALUES (?,?,?,?,'reverse',?,?,?)`, run.workspace, run.ID, action.step, action.iteration, action.id, action.operation, key)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	return processEvent(ctx, tx, run, "scheduler", "compensation_queued", key,
		map[string]any{"compensation_id": id, "original_action_id": action.id, "original_receipt": action.receipt, "operation": action.operation})
}

func advanceChildReversal(ctx context.Context, tx *sql.Tx, run ProcessRun, cancellation ProcessCancellation, group uint64, state string) (string, error) {
	if state == "stopped" {
		return state, nil
	}
	if state == "none" {
		_, err := tx.ExecContext(ctx, `UPDATE process_child_groups SET reversal_status='waiting'
			WHERE workspace_id=? AND parent_run_id=? AND iteration=?`, run.workspace, run.ID, group)
		if err != nil {
			return state, err
		}
		_, err = tx.ExecContext(ctx, `UPDATE process_child_links SET reverse_actor=?,reverse_requested_at=UTC_TIMESTAMP(6)
			WHERE workspace_id=? AND parent_run_id=? AND iteration=? AND reverse_requested_at IS NULL`, cancellation.Initiator, run.workspace, run.ID, group)
		if err != nil {
			return state, err
		}
	}
	var pending, stopped int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(CASE WHEN x.status IS NULL OR x.status IN ('waiting','reconcile') THEN 1 END),
		COUNT(CASE WHEN x.status='stopped' THEN 1 END) FROM process_child_links c
		LEFT JOIN process_cancellations x ON x.workspace_id=c.workspace_id AND x.run_id=c.child_run_id
		WHERE c.workspace_id=? AND c.parent_run_id=? AND c.iteration=?`, run.workspace, run.ID, group).Scan(&pending, &stopped)
	if err != nil {
		return state, err
	}
	if pending != 0 {
		return "waiting", nil
	}
	state = "succeeded"
	if stopped != 0 {
		state = "stopped"
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_child_groups SET reversal_status=?
		WHERE workspace_id=? AND parent_run_id=? AND iteration=?`, state, run.workspace, run.ID, group)
	if err != nil {
		return state, err
	}
	return state, processEvent(ctx, tx, run, "scheduler", "children_reversed", cancellation.Request,
		map[string]any{"iteration": group, "resolution": state})
}

func delayProcessCancellationScan(ctx context.Context, tx *sql.Tx, run ProcessRun) error {
	_, err := tx.ExecContext(ctx, `UPDATE process_cancellations SET next_scan_at=TIMESTAMPADD(SECOND,1,UTC_TIMESTAMP(6))
		WHERE workspace_id=? AND run_id=?`, run.workspace, run.ID)
	return err
}

func reconcileProcessCancellation(ctx context.Context, tx *sql.Tx, run *ProcessRun, cancel ProcessCancellation) error {
	_, err := tx.ExecContext(ctx, `UPDATE process_cancellations SET status='reconcile' WHERE workspace_id=? AND run_id=?`, run.workspace, run.ID)
	if err != nil || terminalProcessStatus(cancel.InitialStatus) {
		return err
	}
	return updateProcessStatus(ctx, tx, run, "reconcile")
}

func (s *Store) ScanProcessCompensations(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 256 {
		return 0, ErrInvalidProcess
	}
	delivered, err := s.deliverChildReversals(ctx, limit)
	if err != nil {
		return delivered, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT workspace_id,run_id FROM process_cancellations
		WHERE status='waiting' AND next_scan_at<=UTC_TIMESTAMP(6) ORDER BY next_scan_at,run_id LIMIT ?`, limit)
	if err != nil {
		return delivered, err
	}
	defer rows.Close()
	type target struct{ workspace, run int64 }
	var candidates []target
	for rows.Next() {
		var item target
		if err = rows.Scan(&item.workspace, &item.run); err != nil {
			return delivered, err
		}
		candidates = append(candidates, item)
	}
	if err = rows.Err(); err != nil {
		return delivered, err
	}
	if err = rows.Close(); err != nil {
		return delivered, err
	}
	for _, item := range candidates {
		if err = s.scanProcessCancellation(ctx, item.workspace, item.run); err != nil {
			return delivered, err
		}
	}
	return delivered, nil
}

func (s *Store) scanProcessCancellation(ctx context.Context, workspace, id int64) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	run, err := loadProcessRun(ctx, tx, workspace, id, true)
	if err != nil {
		return err
	}
	if err = advanceProcessCancellation(ctx, tx, &run); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) deliverChildReversals(ctx context.Context, limit int) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT workspace_id,child_run_id FROM process_child_links
		WHERE pending_reverse IS NOT NULL ORDER BY pending_reverse LIMIT ?`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var candidates [][2]int64
	for rows.Next() {
		var child [2]int64
		if err = rows.Scan(&child[0], &child[1]); err != nil {
			return 0, err
		}
		candidates = append(candidates, child)
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	delivered := 0
	for _, item := range candidates {
		changed, err := s.deliverChildReversal(ctx, item[0], item[1])
		if err != nil {
			return delivered, err
		}
		if changed {
			delivered++
		}
	}
	return delivered, nil
}

func (s *Store) deliverChildReversal(ctx context.Context, workspace, child int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	run, err := loadProcessRun(ctx, tx, workspace, child, true)
	if err != nil {
		return false, err
	}
	var actor string
	var parent int64
	err = tx.QueryRowContext(ctx, `SELECT parent_run_id,reverse_actor FROM process_child_links
		WHERE workspace_id=? AND child_run_id=? AND pending_reverse IS NOT NULL`, workspace, child).Scan(&parent, &actor)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	request := fmt.Sprintf("parent-cancel:%d:%d", parent, child)
	if err = cancelProcessInTx(ctx, tx, &run, actor, request, true); err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_child_links SET reverse_delivered_at=UTC_TIMESTAMP(6)
		WHERE workspace_id=? AND child_run_id=?`, workspace, child)
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}
