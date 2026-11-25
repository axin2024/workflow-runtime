package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const MaxLogRetentionDays = 3650
const MaxLogCleanupBatch = 128

var ErrLogRetentionConflict = errors.New("log retention policy or request has changed")
var ErrLogRetentionDisabled = errors.New("log retention is disabled")

type LogRetentionPolicy struct {
	Generation uint64     `json:"generation"`
	Days       int        `json:"days"`
	UpdatedBy  string     `json:"updated_by,omitempty"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
}

type ClearedLog struct {
	RunID int64  `json:"run_id"`
	Node  string `json:"node"`
	Epoch uint64 `json:"epoch"`
}

type LogCleanup struct {
	ID          int64        `json:"id"`
	RequestKey  string       `json:"request_key"`
	Generation  uint64       `json:"generation"`
	Cutoff      time.Time    `json:"cutoff"`
	CommittedAt time.Time    `json:"committed_at"`
	Actor       string       `json:"actor"`
	Limit       int          `json:"limit"`
	HasMore     bool         `json:"has_more"`
	Cleared     []ClearedLog `json:"cleared"`
}

type LogRetentionReport struct {
	Format      string             `json:"format"`
	Workspace   string             `json:"workspace"`
	GeneratedAt time.Time          `json:"generated_at"`
	Policy      LogRetentionPolicy `json:"policy"`
	Before      int64              `json:"before,omitempty"`
	NextBefore  int64              `json:"next_before,omitempty"`
	Jobs        []LogCleanup       `json:"jobs"`
}

func loadLogRetention(ctx context.Context, tx *sql.Tx, workspace int64, lock bool) (LogRetentionPolicy, error) {
	query := `SELECT generation, days, updated_by, updated_at FROM log_retention_policies WHERE workspace_id = ?`
	if lock {
		query += " FOR UPDATE"
	}
	var policy LogRetentionPolicy
	err := tx.QueryRowContext(ctx, query, workspace).Scan(&policy.Generation, &policy.Days, &policy.UpdatedBy, &policy.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return LogRetentionPolicy{}, nil
	}
	return policy, err
}

func (s *Store) SaveLogRetention(ctx context.Context, scope Scope, expected uint64, days int) (LogRetentionPolicy, error) {
	if err := scope.validate(); err != nil {
		return LogRetentionPolicy{}, err
	}
	if days < 0 || days > MaxLogRetentionDays {
		return LogRetentionPolicy{}, fmt.Errorf("retention days must be between 0 and %d", MaxLogRetentionDays)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return LogRetentionPolicy{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return LogRetentionPolicy{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO log_retention_policies (workspace_id, generation, days, updated_by, updated_at)
		VALUES (?, 0, 0, ?, UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE generation = generation`, workspace, scope.Actor); err != nil {
		return LogRetentionPolicy{}, err
	}
	current, err := loadLogRetention(ctx, tx, workspace, true)
	if err != nil {
		return LogRetentionPolicy{}, err
	}
	if current.Generation != expected {
		return LogRetentionPolicy{}, ErrLogRetentionConflict
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return LogRetentionPolicy{}, err
	}
	policy := LogRetentionPolicy{Generation: expected + 1, Days: days, UpdatedBy: scope.Actor, UpdatedAt: &now}
	if _, err := tx.ExecContext(ctx, `UPDATE log_retention_policies SET generation = ?, days = ?, updated_by = ?, updated_at = ?
		WHERE workspace_id = ?`, policy.Generation, days, policy.UpdatedBy, now, workspace); err != nil {
		return LogRetentionPolicy{}, err
	}
	return policy, tx.Commit()
}

// CleanExpiredLogs clears expired log text in terminal runs for the current workspace.
// Attempt changes and the receipt commit together so retries can return the original receipt.
func (s *Store) CleanExpiredLogs(ctx context.Context, scope Scope, expected uint64, key string, limit int) (LogCleanup, error) {
	if err := scope.validate(); err != nil {
		return LogCleanup{}, err
	}
	if !identifier.MatchString(key) {
		return LogCleanup{}, errors.New("cleanup request key must be a lowercase identifier")
	}
	if limit < 1 || limit > MaxLogCleanupBatch {
		return LogCleanup{}, fmt.Errorf("cleanup batch limit must be between 1 and %d", MaxLogCleanupBatch)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return LogCleanup{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return LogCleanup{}, err
	}
	policy, err := loadLogRetention(ctx, tx, workspace, true)
	if err != nil {
		return LogCleanup{}, err
	}
	prior, err := scanLogCleanup(tx.QueryRowContext(ctx, `SELECT `+logCleanupColumns+`
		FROM log_retention_jobs WHERE workspace_id = ? AND request_key = ?`, workspace, key))
	if err == nil {
		if prior.Generation != expected || prior.Limit != limit || prior.Actor != scope.Actor {
			return LogCleanup{}, ErrLogRetentionConflict
		}
		return prior, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return LogCleanup{}, err
	}
	if policy.Generation != expected {
		return LogCleanup{}, ErrLogRetentionConflict
	}
	if policy.Days == 0 {
		return LogCleanup{}, ErrLogRetentionDisabled
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return LogCleanup{}, err
	}
	cutoff := now.AddDate(0, 0, -policy.Days)
	candidates, err := eligibleLogs(ctx, tx, workspace, cutoff, limit+1)
	if err != nil {
		return LogCleanup{}, err
	}
	job := LogCleanup{RequestKey: key, Generation: expected, Cutoff: cutoff, CommittedAt: now, Actor: scope.Actor, Limit: limit,
		HasMore: len(candidates) > limit, Cleared: candidates[:min(limit, len(candidates))]}
	for _, log := range job.Cleared {
		updated, err := tx.ExecContext(ctx, `UPDATE attempts SET log_excerpt = NULL, log_cleared_at = ?
			WHERE run_id = ? AND node_key = ? AND ordinal = ? AND log_excerpt IS NOT NULL AND log_cleared_at IS NULL`,
			now, log.RunID, log.Node, log.Epoch)
		if err != nil {
			return LogCleanup{}, err
		}
		count, err := updated.RowsAffected()
		if err != nil {
			return LogCleanup{}, err
		}
		if count != 1 {
			return LogCleanup{}, fmt.Errorf("retention candidate has no live log: run %d node %s attempt %d", log.RunID, log.Node, log.Epoch)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM retention_log_queue
			WHERE workspace_id = ? AND run_id = ? AND node_key = ? AND ordinal = ?`, workspace, log.RunID, log.Node, log.Epoch); err != nil {
			return LogCleanup{}, err
		}
	}
	items, err := json.Marshal(job.Cleared)
	if err != nil {
		return LogCleanup{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO log_retention_jobs
		(workspace_id, request_key, generation, cutoff, committed_at, actor, batch_limit, has_more, cleared)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, workspace, key, expected, cutoff, now, scope.Actor, limit, job.HasMore, items)
	if err != nil {
		return LogCleanup{}, err
	}
	job.ID, err = result.LastInsertId()
	if err != nil {
		return LogCleanup{}, err
	}
	return job, tx.Commit()
}

func eligibleLogs(ctx context.Context, tx *sql.Tx, workspace int64, cutoff time.Time, limit int) ([]ClearedLog, error) {
	rows, err := tx.QueryContext(ctx, `SELECT q.run_id, q.node_key, q.ordinal
		FROM retention_log_queue q JOIN runs r ON r.id = q.run_id AND r.workspace_id = q.workspace_id
		WHERE q.workspace_id = ? AND q.finished_at <= ? AND r.finished_at <= ? AND r.state IN ('succeeded', 'failed', 'canceled')
		ORDER BY q.finished_at, q.run_id, q.node_key, q.ordinal LIMIT ?`, workspace, cutoff, cutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	logs := make([]ClearedLog, 0, limit)
	for rows.Next() {
		var log ClearedLog
		if err := rows.Scan(&log.RunID, &log.Node, &log.Epoch); err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, rows.Err()
}

const logCleanupColumns = `id, request_key, generation, cutoff, committed_at, actor, batch_limit, has_more, cleared`

func scanLogCleanup(row interface{ Scan(...any) error }) (LogCleanup, error) {
	var job LogCleanup
	var items []byte
	if err := row.Scan(&job.ID, &job.RequestKey, &job.Generation, &job.Cutoff, &job.CommittedAt, &job.Actor, &job.Limit, &job.HasMore, &items); err != nil {
		return LogCleanup{}, err
	}
	err := json.Unmarshal(items, &job.Cleared)
	return job, err
}

func (s *Store) ExportLogRetention(ctx context.Context, scope Scope, before int64) (LogRetentionReport, error) {
	if err := scope.validate(); err != nil {
		return LogRetentionReport{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return LogRetentionReport{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return LogRetentionReport{}, err
	}
	policy, err := loadLogRetention(ctx, tx, workspace, false)
	if err != nil {
		return LogRetentionReport{}, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return LogRetentionReport{}, err
	}
	report := LogRetentionReport{Format: "workflow.log-retention.v1", Workspace: scope.Workspace, GeneratedAt: now,
		Before: before, Policy: policy, Jobs: []LogCleanup{}}
	query := `SELECT ` + logCleanupColumns + ` FROM log_retention_jobs WHERE workspace_id = ?`
	args := []any{workspace}
	if before > 0 {
		query += ` AND id < ?`
		args = append(args, before)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	rows, err := tx.QueryContext(ctx, query, append(args, ConsolePageSize+1)...)
	if err != nil {
		return LogRetentionReport{}, err
	}
	defer rows.Close()
	for rows.Next() {
		if len(report.Jobs) == ConsolePageSize {
			report.NextBefore = report.Jobs[ConsolePageSize-1].ID
			break
		}
		job, err := scanLogCleanup(rows)
		if err != nil {
			return LogRetentionReport{}, err
		}
		report.Jobs = append(report.Jobs, job)
	}
	if err := rows.Err(); err != nil {
		return LogRetentionReport{}, err
	}
	if err := rows.Close(); err != nil {
		return LogRetentionReport{}, err
	}
	return report, tx.Commit()
}
