package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type ScheduleSummary struct {
	Key, Cron, Timezone string
	Enabled             bool
	MaxActiveRuns       int
	NextAt              *time.Time
}

func (s *Store) ListSchedules(ctx context.Context, scope Scope, dagKey, after string) ([]ScheduleSummary, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	if !identifier.MatchString(dagKey) || (after != "" && !identifier.MatchString(after)) {
		return nil, errors.New("invalid workflow or schedule cursor")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	workspace, dag, _, err := scheduleDAG(ctx, tx, scope.Workspace, dagKey, false)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT schedule_key, cron_expression, timezone, enabled, max_active_runs, next_at FROM schedules
		WHERE workspace_id = ? AND dag_id = ? AND schedule_key > ? ORDER BY schedule_key LIMIT ?`,
		workspace, dag, after, ConsolePageSize+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []ScheduleSummary
	for rows.Next() {
		var item ScheduleSummary
		if err := rows.Scan(&item.Key, &item.Cron, &item.Timezone, &item.Enabled, &item.MaxActiveRuns, &item.NextAt); err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, rows.Err()
}

type BackfillSummary struct {
	ID        int64
	DAG       string
	Schedule  string
	Revision  uint64
	State     string
	CreatedAt time.Time
}

func (s *Store) ListBackfillBatches(ctx context.Context, scope Scope, before int64) ([]BackfillSummary, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	if before < 0 {
		return nil, errors.New("invalid backfill cursor")
	}
	statement := `SELECT b.id, d.dag_key, p.schedule_key, b.revision, b.state, b.created_at
		FROM workspaces w JOIN backfill_batches b ON b.workspace_id = w.id JOIN dags d ON d.id = b.dag_id
		JOIN schedules p ON p.id = b.schedule_id WHERE w.workspace_key = ?`
	args := []any{scope.Workspace}
	if before != 0 {
		statement += ` AND b.id < ?`
		args = append(args, before)
	}
	statement += ` ORDER BY b.id DESC LIMIT ?`
	args = append(args, ConsolePageSize+1)
	rows, err := s.db.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []BackfillSummary
	for rows.Next() {
		var item BackfillSummary
		if err := rows.Scan(&item.ID, &item.DAG, &item.Schedule, &item.Revision, &item.State, &item.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, rows.Err()
}

func (s *Store) ListCapacityQueues(ctx context.Context, scope Scope, after string) ([]Capacity, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	if after != "" && !identifier.MatchString(after) {
		return nil, errors.New("invalid queue cursor")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT q.queue_key, q.max_active, q.generation, q.updated_by, q.updated_at, COALESCE(p.pool_key, '')
		FROM workspaces w JOIN queue_capacity q ON q.workspace_id = w.id LEFT JOIN execution_pools p ON p.id = q.pool_id
		WHERE w.workspace_key = ? AND q.queue_key > ? ORDER BY q.queue_key LIMIT ?`, scope.Workspace, after, ConsolePageSize+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []Capacity
	for rows.Next() {
		item := Capacity{Workspace: scope.Workspace}
		if err := rows.Scan(&item.Queue, &item.MaxActive, &item.Generation, &item.UpdatedBy, &item.UpdatedAt, &item.Pool); err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, rows.Err()
}
