package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

type ProcessAction struct {
	ID              int64           `json:"action_id"`
	Run             int64           `json:"run_id"`
	Step            string          `json:"step_key"`
	Iteration       uint64          `json:"iteration"`
	Direction       string          `json:"direction"`
	OriginalID      *int64          `json:"original_action_id,omitempty"`
	Operation       string          `json:"operation"`
	Key             string          `json:"operation_key"`
	Status          string          `json:"status"`
	Epoch           uint64          `json:"lease_epoch"`
	LeasedBy        *string         `json:"leased_by,omitempty"`
	LeaseUntil      *time.Time      `json:"lease_until,omitempty"`
	ExternalReceipt *string         `json:"external_receipt,omitempty"`
	OriginalReceipt *string         `json:"original_receipt,omitempty"`
	Evidence        json.RawMessage `json:"evidence,omitempty"`
	ResolutionOwner *string         `json:"resolution_owner,omitempty"`
	Resolution      *string         `json:"resolution,omitempty"`
	ConfirmedAt     *time.Time      `json:"confirmed_at,omitempty"`
	Input           json.RawMessage `json:"input,omitempty"`
	compensation    string
}

const processActionColumns = `a.id,a.run_id,a.step_key,a.iteration,a.direction,a.original_id,a.operation_name,a.operation_key,
	a.status,a.lease_epoch,a.leased_by,a.lease_until,a.external_receipt,o.external_receipt,COALESCE(a.evidence,JSON_OBJECT()),a.resolution_owner,
	a.resolution,a.confirmed_at,a.reverse_operation`

const processActionTable = ` FROM process_actions a LEFT JOIN process_actions o ON o.workspace_id=a.workspace_id AND o.id=a.original_id `

func readProcessAction(row interface{ Scan(...any) error }) (ProcessAction, error) {
	var out ProcessAction
	err := row.Scan(&out.ID, &out.Run, &out.Step, &out.Iteration, &out.Direction, &out.OriginalID, &out.Operation, &out.Key,
		&out.Status, &out.Epoch, &out.LeasedBy, &out.LeaseUntil, &out.ExternalReceipt, &out.OriginalReceipt, &out.Evidence,
		&out.ResolutionOwner, &out.Resolution, &out.ConfirmedAt, &out.compensation)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	return out, err
}

func processActionByID(ctx context.Context, tx *sql.Tx, workspace, id int64) (ProcessAction, error) {
	return readProcessAction(tx.QueryRowContext(ctx, `SELECT `+processActionColumns+processActionTable+`WHERE a.workspace_id=? AND a.id=?`, workspace, id))
}

func queueProcessAction(ctx context.Context, tx *sql.Tx, run ProcessRun, step process.Step) error {
	digest := sha256.Sum256(fmt.Appendf(nil, "workflow-action-v1:%d:%d:%d:%s", run.workspace, run.ID, run.Transition, step.Key))
	key := fmt.Sprintf("act-%x", digest)
	result, err := tx.ExecContext(ctx, `INSERT INTO process_actions
		(workspace_id,run_id,step_key,iteration,direction,operation_name,reverse_operation,operation_key)
		VALUES (?,?,?,?,'forward',?,?,?)`, run.workspace, run.ID, step.Key, run.Transition, step.Operation, step.Compensation, key)
	if err != nil {
		return err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return err
	}
	return processEvent(ctx, tx, run, "scheduler", "action_queued", key,
		map[string]any{"action_id": id, "operation": step.Operation, "step": step.Key})
}

func (s *Store) ClaimProcessAction(ctx context.Context, scope Scope, processKey, operation string, lease time.Duration) (ProcessAction, error) {
	var out ProcessAction
	if scope.validate() != nil || !process.ValidKey(processKey) || !process.ValidKey(operation) ||
		lease < time.Second || lease > time.Hour || lease%time.Second != 0 {
		return out, ErrInvalidProcess
	}
	rows, err := s.db.QueryContext(ctx, `SELECT a.workspace_id,a.run_id,a.id FROM process_actions a
		JOIN workspaces w ON w.id=a.workspace_id JOIN process_runs r ON r.workspace_id=a.workspace_id AND r.id=a.run_id
		JOIN process_names n ON n.workspace_id=r.workspace_id AND n.id=r.process_id
		WHERE w.workspace_key=? AND n.process_key=? AND a.operation_name=? AND a.status='ready' ORDER BY a.id LIMIT 32`,
		scope.Workspace, processKey, operation)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	var candidates []processActionCandidate
	for rows.Next() {
		var item processActionCandidate
		if err = rows.Scan(&item.workspace, &item.run, &item.action); err != nil {
			return out, err
		}
		candidates = append(candidates, item)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if err = rows.Close(); err != nil {
		return out, err
	}
	for _, item := range candidates {
		out, err = s.claimProcessAction(ctx, item, scope.Actor, lease)
		if !errors.Is(err, ErrNotFound) {
			return out, err
		}
	}
	return out, ErrNotFound
}

type processActionCandidate struct{ workspace, run, action int64 }

func (s *Store) claimProcessAction(ctx context.Context, item processActionCandidate, actor string, lease time.Duration) (ProcessAction, error) {
	var out ProcessAction
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	run, err := loadProcessRun(ctx, tx, item.workspace, item.run, true)
	if err != nil {
		return out, err
	}
	out, err = processActionByID(ctx, tx, run.workspace, item.action)
	if err != nil {
		return out, err
	}
	if out.Status != "ready" {
		return out, ErrNotFound
	}
	if out.Direction == "forward" && (run.Status != "running" || run.Transition != out.Iteration) {
		return out, ErrNotFound
	}
	if out.Direction == "reverse" {
		cancellation, err := processCancellationByRun(ctx, tx, run)
		if err != nil {
			return out, err
		}
		if cancellation.Status != "waiting" {
			return out, ErrNotFound
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_actions SET status='leased',leased_by=?,lease_epoch=lease_epoch+1,
		lease_until=TIMESTAMPADD(SECOND,?,UTC_TIMESTAMP(6)) WHERE workspace_id=? AND id=?`, actor, int(lease.Seconds()), run.workspace, out.ID)
	if err != nil {
		return out, err
	}
	out, err = processActionByID(ctx, tx, run.workspace, out.ID)
	if err != nil {
		return out, err
	}
	if err = processEvent(ctx, tx, run, actor, "action_claimed", out.Key, map[string]any{"action_id": out.ID, "lease_epoch": out.Epoch}); err != nil {
		return out, err
	}
	out.Input = run.Input
	return out, tx.Commit()
}

func processActionsForRun(ctx context.Context, tx *sql.Tx, run ProcessRun) ([]ProcessAction, error) {
	rows, err := tx.QueryContext(ctx, `SELECT `+processActionColumns+processActionTable+
		`WHERE a.workspace_id=? AND a.run_id=? ORDER BY a.id`, run.workspace, run.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	actions := []ProcessAction{}
	for rows.Next() {
		action, err := readProcessAction(rows)
		if err != nil {
			return nil, err
		}
		actions = append(actions, action)
	}
	return actions, rows.Err()
}
