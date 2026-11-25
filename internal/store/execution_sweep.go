package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type SweepResult struct {
	Expired        int `json:"expired"`
	CancelTimedOut int `json:"cancel_timed_out"`
}

type expiredTask struct {
	taskCandidate
	workspace int64
}

// SweepExecution settles elapsed lease deadlines even when no worker for the
// task is available. Each candidate is rechecked under its run lock.
func (s *Store) SweepExecution(ctx context.Context, limit int) (SweepResult, error) {
	var result SweepResult
	if limit < 1 || limit > 256 {
		return result, errors.New("execution sweep limit must be between 1 and 256")
	}
	candidates, err := s.expiredTasks(ctx, limit)
	if err != nil {
		return result, err
	}
	for _, candidate := range candidates {
		expired, canceled, err := s.sweepTask(ctx, candidate)
		if err != nil {
			return result, err
		}
		if canceled {
			result.CancelTimedOut++
		} else if expired {
			result.Expired++
		}
	}
	return result, nil
}

func (s *Store) expiredTasks(ctx context.Context, limit int) ([]expiredTask, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT n.run_id, n.node_key, r.workspace_id
		FROM node_runs n JOIN runs r ON r.id = n.run_id WHERE n.state = 'running' AND n.lease_until <= UTC_TIMESTAMP(6)
		ORDER BY n.lease_until, n.run_id, n.node_key LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list expired node leases: %w", err)
	}
	defer rows.Close()
	var result []expiredTask
	for rows.Next() {
		var item expiredTask
		if err := rows.Scan(&item.runID, &item.node, &item.workspace); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (s *Store) sweepTask(ctx context.Context, candidate expiredTask) (expired, canceled bool, err error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, false, err
	}
	defer tx.Rollback()
	run, dag, err := lockWorkerRun(ctx, tx, candidate.workspace, candidate.runID, true)
	if errors.Is(err, ErrNotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	task, err := readTask(ctx, tx, run.ID, candidate.node)
	if err != nil {
		return false, false, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return false, false, err
	}
	if task.state != workflow.NodeRunning || task.leaseUntil == nil || task.leaseUntil.After(now) {
		return false, false, nil
	}
	version, err := loadVersion(ctx, tx, dag, run.Revision)
	if err != nil {
		return false, false, err
	}
	node, err := nodeDefinition(version.Graph.Definition, candidate.node)
	if err != nil {
		return false, false, err
	}
	if run.State == "canceling" {
		_, err = cancelAttempt(ctx, tx, run.ID, node, task.epoch, now, "cancel_timeout")
	} else {
		_, err = expireAttempt(ctx, tx, run.ID, version.Graph.Definition, node, task, now)
	}
	if err != nil {
		return false, false, err
	}
	if err := tx.Commit(); err != nil {
		return false, false, err
	}
	return true, run.State == "canceling", nil
}
