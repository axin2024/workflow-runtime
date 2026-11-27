package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

var ErrProcessPaused = errors.New("new business process runs are paused in this workspace")

type ProcessIntake struct {
	Workspace string     `json:"workspace"`
	Paused    bool       `json:"paused"`
	Actor     string     `json:"actor,omitempty"`
	ChangedAt *time.Time `json:"changed_at,omitempty"`
}

func processIntakeWorkspace(ctx context.Context, tx *sql.Tx, key string, exclusive bool) (int64, error) {
	var id int64
	lock := " FOR SHARE"
	if exclusive {
		lock = " FOR UPDATE"
	}
	err := tx.QueryRowContext(ctx, `SELECT id FROM workspaces WHERE workspace_key=?`+lock, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	return id, err
}

func readProcessIntake(ctx context.Context, tx *sql.Tx, workspace int64, key string) (ProcessIntake, error) {
	out := ProcessIntake{Workspace: key}
	err := tx.QueryRowContext(ctx, `SELECT paused,actor,updated_at FROM process_intake_controls WHERE workspace_id=?`, workspace).
		Scan(&out.Paused, &out.Actor, &out.ChangedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return out, nil
	}
	return out, err
}

func (s *Store) SetProcessIntake(ctx context.Context, scope Scope, paused bool) (ProcessIntake, error) {
	var out ProcessIntake
	if scope.validate() != nil {
		return out, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := processIntakeWorkspace(ctx, tx, scope.Workspace, true)
	if err != nil {
		return out, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO process_intake_controls (workspace_id,paused,actor) VALUES (?,?,?) AS incoming
		ON DUPLICATE KEY UPDATE paused=incoming.paused,actor=incoming.actor,updated_at=UTC_TIMESTAMP(6)`, workspace, paused, scope.Actor)
	if err != nil {
		return out, err
	}
	out, err = readProcessIntake(ctx, tx, workspace, scope.Workspace)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

type ProcessDrainItem struct {
	Run          int64      `json:"run_id"`
	Process      string     `json:"process_key"`
	Revision     int        `json:"definition_rev"`
	Business     string     `json:"business_key"`
	State        string     `json:"status"`
	Step         string     `json:"step_key"`
	Wait         *int64     `json:"wait_id,omitempty"`
	Deadline     *time.Time `json:"deadline,omitempty"`
	Task         *int64     `json:"task_id,omitempty"`
	Action       *int64     `json:"action_id,omitempty"`
	Operation    *string    `json:"operation_key,omitempty"`
	ActionStatus *string    `json:"action_status,omitempty"`
	Owner        *string    `json:"resolution_owner,omitempty"`
	Compensation *string    `json:"compensation_status,omitempty"`
}

type ProcessDrainReport struct {
	Intake             ProcessIntake      `json:"intake"`
	LegacyRuns         int                `json:"legacy_dag_runs"`
	LegacyActive       int                `json:"legacy_dag_active"`
	Definitions        int                `json:"definition_revisions"`
	PendingWaits       int                `json:"pending_waits"`
	PendingActions     int                `json:"pending_actions"`
	OwnershipConflicts int                `json:"ownership_conflicts"`
	Items              []ProcessDrainItem `json:"pending_instances"`
	After              int64              `json:"next_after_run_id,omitempty"`
}

func (s *Store) ProcessDrain(ctx context.Context, scope Scope, after int64) (ProcessDrainReport, error) {
	var out ProcessDrainReport
	if scope.validate() != nil || after < 0 {
		return out, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return out, err
	}
	out.Intake, err = readProcessIntake(ctx, tx, workspace, scope.Workspace)
	if err != nil {
		return out, err
	}
	err = tx.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM runs WHERE workspace_id=?),
		(SELECT COUNT(*) FROM runs WHERE workspace_id=? AND state NOT IN ('succeeded','failed','canceled')),
		(SELECT COUNT(*) FROM process_definitions WHERE workspace_id=?),
		(SELECT COUNT(*) FROM process_waits WHERE workspace_id=? AND status='waiting'),
		(SELECT COUNT(*) FROM process_actions WHERE workspace_id=? AND status IN ('ready','leased','unknown')),
		(SELECT COUNT(*) FROM process_tasks t JOIN process_waits w ON w.id=t.wait_id
		WHERE t.workspace_id=? AND (t.workspace_id<>w.workspace_id OR t.run_id<>w.run_id)) +
		(SELECT COUNT(*) FROM process_signals s JOIN process_waits w ON w.id=s.wait_id
		WHERE s.workspace_id=? AND (s.workspace_id<>w.workspace_id OR s.run_id<>w.run_id))`,
		workspace, workspace, workspace, workspace, workspace, workspace, workspace).
		Scan(&out.LegacyRuns, &out.LegacyActive, &out.Definitions, &out.PendingWaits, &out.PendingActions, &out.OwnershipConflicts)
	if err != nil {
		return out, err
	}
	out.Items, err = collectProcessRows(ctx, tx, `SELECT r.id,p.process_key,r.definition_rev,r.business_key,r.status,r.step_key,
		w.id,w.deadline,t.id,a.id,a.operation_key,a.status,a.resolution_owner,c.status FROM process_runs r
		JOIN process_names p ON p.workspace_id=r.workspace_id AND p.id=r.process_id
		LEFT JOIN process_waits w ON w.workspace_id=r.workspace_id AND w.active_run=r.id
		LEFT JOIN process_tasks t ON t.workspace_id=r.workspace_id AND t.wait_id=w.id
		LEFT JOIN process_actions a ON a.workspace_id=r.workspace_id AND a.run_id=r.id AND a.status IN ('ready','leased','unknown')
		LEFT JOIN process_cancellations c ON c.workspace_id=r.workspace_id AND c.run_id=r.id
		WHERE r.workspace_id=? AND r.id>? AND (r.status IN ('waiting','running','reconcile','cancelling') OR c.status IN ('waiting','reconcile'))
		ORDER BY r.id LIMIT 251`, readProcessDrain, workspace, after)
	if err != nil {
		return out, err
	}
	if len(out.Items) > 250 {
		out.Items = out.Items[:250]
		out.After = out.Items[249].Run
	}
	return out, tx.Commit()
}

func readProcessDrain(row interface{ Scan(...any) error }) (ProcessDrainItem, error) {
	var out ProcessDrainItem
	err := row.Scan(&out.Run, &out.Process, &out.Revision, &out.Business, &out.State, &out.Step, &out.Wait,
		&out.Deadline, &out.Task, &out.Action, &out.Operation, &out.ActionStatus, &out.Owner, &out.Compensation)
	return out, err
}
