package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const ConsolePageSize = 40

type WorkflowSummary struct {
	Key         string
	Revision    uint64
	Publisher   string
	PublishedAt time.Time
}

func (s *Store) ListWorkflows(ctx context.Context, scope Scope, after string) ([]WorkflowSummary, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	if after != "" && !identifier.MatchString(after) {
		return nil, errors.New("invalid workflow cursor")
	}
	rows, err := s.db.QueryContext(ctx, `SELECT d.dag_key, v.revision, v.publisher, v.published_at
		FROM workspaces w JOIN dags d ON d.workspace_id = w.id
		JOIN dag_versions v ON v.dag_id = d.id AND v.revision = d.active_revision
		WHERE w.workspace_key = ? AND d.dag_key > ? ORDER BY d.dag_key LIMIT ?`, scope.Workspace, after, ConsolePageSize+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []WorkflowSummary
	for rows.Next() {
		var item WorkflowSummary
		if err := rows.Scan(&item.Key, &item.Revision, &item.Publisher, &item.PublishedAt); err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, rows.Err()
}

type RunFilter struct {
	DAG     string     `json:"dag,omitempty"`
	State   string     `json:"state,omitempty"`
	From    *time.Time `json:"from,omitempty"`
	Through *time.Time `json:"through,omitempty"`
	Before  int64      `json:"before,omitempty"`
}

type RunSummary struct {
	ID         int64      `json:"id"`
	DAG        string     `json:"dag"`
	Revision   uint64     `json:"revision"`
	Kind       string     `json:"kind"`
	State      string     `json:"state"`
	CreatedAt  time.Time  `json:"created_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

func (s *Store) ListRuns(ctx context.Context, scope Scope, filter RunFilter) ([]RunSummary, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	if (filter.DAG != "" && !identifier.MatchString(filter.DAG)) || filter.Before < 0 ||
		(filter.From != nil && filter.Through != nil && filter.From.After(*filter.Through)) {
		return nil, errors.New("invalid workflow or date range")
	}
	statement := `SELECT r.id, d.dag_key, r.revision, r.trigger_kind, r.state, r.created_at, r.finished_at
		FROM workspaces w JOIN runs r ON r.workspace_id = w.id JOIN dags d ON d.id = r.dag_id WHERE w.workspace_key = ?`
	values := []any{scope.Workspace}
	if filter.DAG != "" {
		statement += ` AND d.dag_key = ?`
		values = append(values, filter.DAG)
	}
	if filter.State != "" {
		statement += ` AND r.state = ?`
		values = append(values, filter.State)
	}
	if filter.From != nil {
		statement += ` AND r.created_at >= ?`
		values = append(values, filter.From.UTC())
	}
	if filter.Through != nil {
		statement += ` AND r.created_at <= ?`
		values = append(values, filter.Through.UTC())
	}
	if filter.Before > 0 {
		statement += ` AND r.id < ?`
		values = append(values, filter.Before)
	}
	statement += ` ORDER BY r.id DESC LIMIT ?`
	values = append(values, ConsolePageSize+1)
	rows, err := s.db.QueryContext(ctx, statement, values...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := make([]RunSummary, 0)
	for rows.Next() {
		var item RunSummary
		if err := rows.Scan(&item.ID, &item.DAG, &item.Revision, &item.Kind, &item.State, &item.CreatedAt, &item.FinishedAt); err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, rows.Err()
}

type RunAuditEvent struct {
	Actor     string          `json:"actor"`
	Kind      string          `json:"kind"`
	Details   json.RawMessage `json:"details"`
	CreatedAt time.Time       `json:"created_at"`
}

func listRunAudit(ctx context.Context, tx *sql.Tx, workspace, runID int64) ([]RunAuditEvent, error) {
	rows, err := tx.QueryContext(ctx, `SELECT actor, event_kind, details, created_at FROM audit_events
		WHERE workspace_id = ? AND run_id = ? ORDER BY id DESC LIMIT 100`, workspace, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []RunAuditEvent
	for rows.Next() {
		var item RunAuditEvent
		if err := rows.Scan(&item.Actor, &item.Kind, &item.Details, &item.CreatedAt); err != nil {
			return nil, err
		}
		list = append(list, item)
	}
	return list, rows.Err()
}
