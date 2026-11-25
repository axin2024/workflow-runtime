package store

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type queueAdmission struct {
	allowed bool
	pool    string
	poolID  *int64
}

type claimAdmission struct {
	queues  map[string]queueAdmission
	pools   map[int64]bool
	blocked []string
}

func (a *claimAdmission) check(ctx context.Context, tx *sql.Tx, workspace int64, queue string, now time.Time) (queueAdmission, error) {
	if recorded, ok := a.queues[queue]; ok {
		return recorded, nil
	}
	capacity, poolID, err := readQueueCapacity(ctx, tx, workspace, queue)
	if err != nil {
		return queueAdmission{}, err
	}
	result := queueAdmission{allowed: true, pool: capacity.Pool, poolID: poolID}
	if capacity.MaxActive != nil {
		active, err := countWorkspaceLeases(ctx, tx, workspace, queue, now)
		if err != nil {
			return queueAdmission{}, err
		}
		result.allowed = active < *capacity.MaxActive
	}
	if result.allowed && poolID != nil {
		result.allowed, err = a.poolCanAdmit(ctx, tx, *poolID)
		if err != nil {
			return queueAdmission{}, err
		}
	}
	if !result.allowed {
		a.blocked = append(a.blocked, queue)
	}
	a.queues[queue] = result
	return result, nil
}

func (a *claimAdmission) poolCanAdmit(ctx context.Context, tx *sql.Tx, pool int64) (bool, error) {
	if recorded, ok := a.pools[pool]; ok {
		return recorded, nil
	}
	var maxActive int
	// A worker can scan multiple queues and pools on one claim. Avoid
	// waiting on another claimant's pool while holding a different pool.
	err := tx.QueryRowContext(ctx, `SELECT max_active FROM execution_pools WHERE id = ? FOR UPDATE SKIP LOCKED`, pool).Scan(&maxActive)
	if errors.Is(err, sql.ErrNoRows) {
		a.pools[pool] = false
		return false, nil
	}
	if err != nil {
		return false, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return false, err
	}
	active, err := countPoolLeases(ctx, tx, pool, now)
	if err != nil {
		return false, err
	}
	allowed := active < maxActive
	a.pools[pool] = allowed
	return allowed, nil
}

// Renewal can move a lease's expiry into the future just as a claimant
// counts active leases. Take the same capacity locks before the run lock.
func lockRenewalCapacity(ctx context.Context, tx *sql.Tx, worker workerIdentity, ref LeaseRef) error {
	if _, err := lockWorkspaceCapacity(ctx, tx, worker.workspace); err != nil {
		return err
	}
	var poolID *int64
	err := tx.QueryRowContext(ctx, `SELECT n.lease_pool_id FROM node_runs n JOIN runs r ON r.id = n.run_id
		WHERE r.workspace_id = ? AND n.run_id = ? AND n.node_key = ? AND n.lease_epoch = ? AND n.lease_worker_id = ?`,
		worker.workspace, ref.RunID, ref.Node, ref.Epoch, worker.id).Scan(&poolID)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil || poolID == nil {
		return err
	}
	var locked int64
	return tx.QueryRowContext(ctx, `SELECT id FROM execution_pools WHERE id = ? FOR UPDATE`, *poolID).Scan(&locked)
}
