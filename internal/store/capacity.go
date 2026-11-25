package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

var ErrCapacityConflict = errors.New("capacity generation changed or existing leases prevent this update")

type Capacity struct {
	Workspace  string     `json:"workspace,omitempty"`
	Queue      string     `json:"queue,omitempty"`
	Pool       string     `json:"pool,omitempty"`
	Generation uint64     `json:"generation"`
	MaxActive  *int       `json:"max_active"`
	Active     int        `json:"active"`
	UpdatedBy  *string    `json:"updated_by,omitempty"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
}

type QueueCapacitySpec struct {
	MaxActive *int   `json:"max_active"`
	Pool      string `json:"pool,omitempty"`
}

func validCapacity(limit *int) bool { return limit == nil || (*limit >= 0 && *limit <= 10000) }

func exceeded(limit *int, active int) bool { return limit != nil && active > *limit }

func (s *Store) GetWorkspaceCapacity(ctx context.Context, scope Scope) (Capacity, error) {
	return s.workspaceCapacity(ctx, scope, nil, nil)
}

func (s *Store) PutWorkspaceCapacity(ctx context.Context, scope Scope, generation uint64, limit *int) (Capacity, error) {
	if !validCapacity(limit) {
		return Capacity{}, errors.New("workspace capacity must be unlimited or 0 to 10000")
	}
	return s.workspaceCapacity(ctx, scope, &generation, limit)
}

func (s *Store) workspaceCapacity(ctx context.Context, scope Scope, expected *uint64, limit *int) (Capacity, error) {
	if err := scope.validate(); err != nil {
		return Capacity{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Capacity{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return Capacity{}, err
	}
	record, err := lockWorkspaceCapacity(ctx, tx, workspace)
	if err != nil {
		return Capacity{}, err
	}
	record.Workspace = scope.Workspace
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return Capacity{}, err
	}
	record.Active, err = countWorkspaceLeases(ctx, tx, workspace, "", now)
	if err != nil {
		return Capacity{}, err
	}
	if expected == nil {
		return record, nil
	}
	if *expected != record.Generation || exceeded(limit, record.Active) {
		return Capacity{}, ErrCapacityConflict
	}
	record.Generation++
	if _, err := tx.ExecContext(ctx, `UPDATE workspace_capacity SET max_active = ?, generation = ?, updated_by = ?, updated_at = ?
		WHERE workspace_id = ?`, limit, record.Generation, scope.Actor, now, workspace); err != nil {
		return Capacity{}, err
	}
	record.MaxActive, record.UpdatedBy, record.UpdatedAt = limit, &scope.Actor, &now
	return record, tx.Commit()
}

func (s *Store) GetQueueCapacity(ctx context.Context, scope Scope, queue string) (Capacity, error) {
	return s.queueCapacity(ctx, scope, queue, nil, QueueCapacitySpec{})
}

func (s *Store) PutQueueCapacity(ctx context.Context, scope Scope, queue string, generation uint64, spec QueueCapacitySpec) (Capacity, error) {
	if !validCapacity(spec.MaxActive) || (spec.Pool != "" && !identifier.MatchString(spec.Pool)) {
		return Capacity{}, errors.New("queue capacity requires an optional limit of 0 to 10000 and a valid pool name")
	}
	return s.queueCapacity(ctx, scope, queue, &generation, spec)
}

func (s *Store) queueCapacity(ctx context.Context, scope Scope, queue string, expected *uint64, spec QueueCapacitySpec) (Capacity, error) {
	if err := scope.validate(); err != nil {
		return Capacity{}, err
	}
	if !identifier.MatchString(queue) {
		return Capacity{}, errors.New("invalid queue key")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Capacity{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return Capacity{}, err
	}
	if _, err := lockWorkspaceCapacity(ctx, tx, workspace); err != nil {
		return Capacity{}, err
	}
	record, _, err := readQueueCapacity(ctx, tx, workspace, queue)
	if err != nil {
		return Capacity{}, err
	}
	record.Workspace, record.Queue = scope.Workspace, queue
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return Capacity{}, err
	}
	record.Active, err = countWorkspaceLeases(ctx, tx, workspace, queue, now)
	if err != nil {
		return Capacity{}, err
	}
	if expected == nil {
		return record, nil
	}
	if *expected != record.Generation || exceeded(spec.MaxActive, record.Active) || (spec.Pool != record.Pool && record.Active != 0) {
		return Capacity{}, ErrCapacityConflict
	}
	var poolID *int64
	if spec.Pool != "" {
		var id int64
		err := tx.QueryRowContext(ctx, `SELECT id FROM execution_pools WHERE pool_key = ?`, spec.Pool).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) {
			return Capacity{}, ErrNotFound
		}
		if err != nil {
			return Capacity{}, err
		}
		poolID = &id
	}
	record.Generation++
	if _, err := tx.ExecContext(ctx, `INSERT INTO queue_capacity (workspace_id, queue_key, pool_id, max_active, generation, updated_by, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?) AS incoming ON DUPLICATE KEY UPDATE pool_id = incoming.pool_id, max_active = incoming.max_active,
		generation = incoming.generation, updated_by = incoming.updated_by, updated_at = incoming.updated_at`,
		workspace, queue, poolID, spec.MaxActive, record.Generation, scope.Actor, now); err != nil {
		return Capacity{}, err
	}
	record.Pool, record.MaxActive, record.UpdatedBy, record.UpdatedAt = spec.Pool, spec.MaxActive, &scope.Actor, &now
	return record, tx.Commit()
}

func (s *Store) GetPoolCapacity(ctx context.Context, pool string) (Capacity, error) {
	return s.poolCapacity(ctx, pool, "", nil, 0)
}

func (s *Store) PutPoolCapacity(ctx context.Context, actor, pool string, generation uint64, limit int) (Capacity, error) {
	if !identifier.MatchString(actor) || !validCapacity(&limit) {
		return Capacity{}, errors.New("pool capacity requires an actor and a limit from 0 to 10000")
	}
	return s.poolCapacity(ctx, pool, actor, &generation, limit)
}

func (s *Store) poolCapacity(ctx context.Context, pool, actor string, expected *uint64, limit int) (Capacity, error) {
	if !identifier.MatchString(pool) {
		return Capacity{}, errors.New("invalid pool key")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Capacity{}, err
	}
	defer tx.Rollback()
	record := Capacity{Pool: pool}
	var id int64
	err = tx.QueryRowContext(ctx, `SELECT id, max_active, generation, updated_by, updated_at FROM execution_pools
		WHERE pool_key = ? FOR UPDATE`, pool).Scan(&id, &record.MaxActive, &record.Generation, &record.UpdatedBy, &record.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) && (expected == nil || *expected != 0) {
		return Capacity{}, ErrNotFound
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Capacity{}, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return Capacity{}, err
	}
	if id != 0 {
		record.Active, err = countPoolLeases(ctx, tx, id, now)
		if err != nil {
			return Capacity{}, err
		}
	}
	if expected == nil {
		return record, nil
	}
	if *expected != record.Generation || exceeded(&limit, record.Active) {
		return Capacity{}, ErrCapacityConflict
	}
	record.Generation++
	record.MaxActive, record.UpdatedBy, record.UpdatedAt = &limit, &actor, &now
	if id == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO execution_pools (pool_key, max_active, generation, updated_by, updated_at)
			VALUES (?, ?, ?, ?, ?)`, pool, limit, record.Generation, actor, now)
		var driver *mysql.MySQLError
		if errors.As(err, &driver) && driver.Number == 1062 {
			return Capacity{}, ErrCapacityConflict
		}
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE execution_pools SET max_active = ?, generation = ?, updated_by = ?, updated_at = ? WHERE id = ?`,
			limit, record.Generation, actor, now, id)
	}
	if err != nil {
		return Capacity{}, err
	}
	return record, tx.Commit()
}

func lockWorkspaceCapacity(ctx context.Context, tx *sql.Tx, workspace int64) (Capacity, error) {
	var record Capacity
	err := tx.QueryRowContext(ctx, `SELECT max_active, generation, updated_by, updated_at FROM workspace_capacity
		WHERE workspace_id = ? FOR UPDATE`, workspace).Scan(&record.MaxActive, &record.Generation, &record.UpdatedBy, &record.UpdatedAt)
	return record, err
}

func readQueueCapacity(ctx context.Context, tx *sql.Tx, workspace int64, queue string) (Capacity, *int64, error) {
	var record Capacity
	var poolID *int64
	err := tx.QueryRowContext(ctx, `SELECT q.max_active, q.generation, q.updated_by, q.updated_at, q.pool_id, COALESCE(p.pool_key, '')
		FROM queue_capacity q LEFT JOIN execution_pools p ON p.id = q.pool_id WHERE q.workspace_id = ? AND q.queue_key = ?`, workspace, queue).
		Scan(&record.MaxActive, &record.Generation, &record.UpdatedBy, &record.UpdatedAt, &poolID, &record.Pool)
	if errors.Is(err, sql.ErrNoRows) {
		return record, nil, nil
	}
	return record, poolID, err
}

func countWorkspaceLeases(ctx context.Context, tx *sql.Tx, workspace int64, queue string, now time.Time) (int, error) {
	query := `SELECT COUNT(*) FROM workers w JOIN node_runs n ON n.lease_worker_id = w.id
		WHERE w.workspace_id = ? AND n.lease_until > ?`
	args := []any{workspace, now}
	if queue != "" {
		query += ` AND n.queue = ?`
		args = append(args, queue)
	}
	var active int
	if err := tx.QueryRowContext(ctx, query, args...).Scan(&active); err != nil {
		return 0, fmt.Errorf("count workspace leases: %w", err)
	}
	return active, nil
}

func countPoolLeases(ctx context.Context, tx *sql.Tx, pool int64, now time.Time) (int, error) {
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_runs WHERE lease_pool_id = ? AND lease_until > ?`, pool, now).Scan(&active); err != nil {
		return 0, fmt.Errorf("count pool leases: %w", err)
	}
	return active, nil
}
