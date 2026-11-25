package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type ClaimRequest struct {
	Tasks        []string `json:"tasks"`
	LeaseSeconds int      `json:"lease_seconds"`
}

type LeaseRef struct {
	RunID        int64  `json:"run_id"`
	Node         string `json:"node"`
	Epoch        uint64 `json:"epoch"`
	OperationKey string `json:"operation_key"`
}

type ClaimedTask struct {
	LeaseRef
	Task       string          `json:"task"`
	Queue      string          `json:"queue"`
	Pool       string          `json:"pool,omitempty"`
	Effect     workflow.Effect `json:"effect"`
	Inputs     json.RawMessage `json:"inputs"`
	ClaimedAt  time.Time       `json:"claimed_at"`
	LeaseUntil time.Time       `json:"lease_until"`
	Deadline   time.Time       `json:"deadline"`
}

func validLeaseSeconds(seconds int) bool { return seconds >= 1 && seconds <= 120 }

func (ref LeaseRef) valid() bool {
	if ref.RunID <= 0 || !identifier.MatchString(ref.Node) || ref.Epoch == 0 || len(ref.OperationKey) != 64 {
		return false
	}
	_, err := hex.DecodeString(ref.OperationKey)
	return err == nil
}

type taskCandidate struct {
	runID int64
	node  string
	queue string
}

const claimPageSize = 32

// Claim commits a new attempt and lease together under the run row lock.
func (s *Store) Claim(ctx context.Context, token string, request ClaimRequest) (*ClaimedTask, error) {
	if !validLeaseSeconds(request.LeaseSeconds) || len(request.Tasks) == 0 || len(request.Tasks) > 128 {
		return nil, fmt.Errorf("%w: claim requires 1 to 128 supported tasks and a lease of 1 to 120 seconds", ErrInvalidWorkerRequest)
	}
	for _, task := range request.Tasks {
		if !identifier.MatchString(task) {
			return nil, fmt.Errorf("%w: invalid supported task identifier", ErrInvalidWorkerRequest)
		}
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	worker, err := lockWorker(ctx, tx, token)
	if err != nil {
		return nil, err
	}
	capacity, err := lockWorkspaceCapacity(ctx, tx, worker.workspace)
	if err != nil {
		return nil, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM node_runs WHERE lease_worker_id = ? AND lease_until > ?`, worker.id, now).Scan(&active); err != nil {
		return nil, fmt.Errorf("count worker leases: %w", err)
	}
	if active >= worker.maxActive {
		return nil, nil
	}
	if capacity.MaxActive != nil {
		active, err := countWorkspaceLeases(ctx, tx, worker.workspace, "", now)
		if err != nil || active >= *capacity.MaxActive {
			return nil, err
		}
	}
	admission := claimAdmission{
		queues: make(map[string]queueAdmission),
		pools:  make(map[int64]bool),
	}
	for {
		previouslyBlocked := len(admission.blocked)
		candidates, err := findTaskCandidates(ctx, tx, worker, request.Tasks, now, admission.blocked)
		if err != nil {
			return nil, err
		}
		for _, candidate := range candidates {
			queue, err := admission.check(ctx, tx, worker.workspace, candidate.queue, now)
			if err != nil {
				return nil, err
			}
			if !queue.allowed {
				continue
			}
			leased, err := claimCandidate(ctx, tx, worker, candidate, queue, request.LeaseSeconds)
			if err != nil {
				return nil, err
			}
			if leased != nil {
				if err := tx.Commit(); err != nil {
					return nil, err
				}
				return leased, nil
			}
		}
		if len(candidates) < claimPageSize || len(admission.blocked) == previouslyBlocked {
			return nil, tx.Commit()
		}
	}
}

func findTaskCandidates(ctx context.Context, tx *sql.Tx, worker workerIdentity, tasks []string, now time.Time, blocked []string) ([]taskCandidate, error) {
	values := make([]any, 0, 5+len(tasks)+len(blocked))
	values = append(values, worker.id, worker.workspace, now, now)
	for _, task := range tasks {
		values = append(values, task)
	}
	placeholders := strings.Repeat("?,", len(tasks)-1) + "?"
	query := `SELECT n.run_id, n.node_key, n.queue FROM worker_permissions p
		JOIN node_runs n ON n.queue = p.queue AND n.task = p.task
		JOIN runs r ON r.id = n.run_id
		WHERE p.worker_id = ? AND r.workspace_id = ? AND r.state IN ('queued', 'running')
		AND ((n.state IN ('ready', 'retry_wait') AND n.available_at <= ?) OR (n.state = 'running' AND n.lease_until <= ?))
		AND n.task IN (` + placeholders + `)`
	if len(blocked) != 0 {
		query += ` AND n.queue NOT IN (` + strings.Repeat("?,", len(blocked)-1) + `?)`
		for _, queue := range blocked {
			values = append(values, queue)
		}
	}
	query += ` ORDER BY COALESCE(n.lease_until, n.available_at), n.run_id, n.node_key LIMIT ?`
	values = append(values, claimPageSize)
	rows, err := tx.QueryContext(ctx, query, values...)
	if err != nil {
		return nil, fmt.Errorf("list claimable nodes: %w", err)
	}
	defer rows.Close()
	var candidates []taskCandidate
	for rows.Next() {
		var candidate taskCandidate
		if err := rows.Scan(&candidate.runID, &candidate.node, &candidate.queue); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rows.Err()
}

func lockWorkerRun(ctx context.Context, tx *sql.Tx, workspace, runID int64, skip bool) (Run, int64, error) {
	query := `SELECT ` + runColumns + ` FROM runs WHERE workspace_id = ? AND id = ? FOR UPDATE`
	if skip {
		query += ` SKIP LOCKED`
	}
	run, dag, _, err := readRun(tx.QueryRowContext(ctx, query, workspace, runID))
	return run, dag, err
}

func claimCandidate(ctx context.Context, tx *sql.Tx, worker workerIdentity, candidate taskCandidate, queue queueAdmission, seconds int) (*ClaimedTask, error) {
	run, dag, err := lockWorkerRun(ctx, tx, worker.workspace, candidate.runID, true)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if run.State != "queued" && run.State != "running" {
		return nil, nil
	}
	task, err := readTask(ctx, tx, run.ID, candidate.node)
	if err != nil {
		return nil, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	ready := task.state == workflow.NodeReady || task.state == workflow.NodeRetryWait
	due := ready && task.availableAt != nil && !task.availableAt.After(now)
	expired := task.state == workflow.NodeRunning && task.leaseUntil != nil && !task.leaseUntil.After(now)
	if !due && !expired {
		return nil, nil
	}
	version, err := loadVersion(ctx, tx, dag, run.Revision)
	if err != nil {
		return nil, err
	}
	node, err := nodeDefinition(version.Graph.Definition, candidate.node)
	if err != nil {
		return nil, err
	}
	if expired {
		change, err := expireAttempt(ctx, tx, run.ID, version.Graph.Definition, node, task, now)
		if err != nil || change.state != workflow.NodeRetryWait || change.availableAt.After(now) {
			return nil, err
		}
		task.availableAt = change.availableAt
	}
	inputs, err := taskInputs(ctx, tx, run, node)
	if errors.Is(err, errBoundInputsTooLarge) {
		change := nodeChange{state: workflow.NodeFailed, failure: "input_too_large"}
		if err := applyNodeChange(ctx, tx, run.ID, node.Key, change, now); err != nil {
			return nil, err
		}
		return nil, advanceRun(ctx, tx, run.ID, version.Graph.Definition, node.Key, now)
	}
	if err != nil {
		return nil, err
	}
	return startAttempt(ctx, tx, worker, run, dag, node, task, inputs, queue, seconds, now)
}

func expireAttempt(ctx context.Context, tx *sql.Tx, runID int64, graph workflow.Definition, node workflow.Node, task taskRecord, now time.Time) (nodeChange, error) {
	code := "lease_expired"
	if task.deadline != nil && !task.deadline.After(now) {
		code = "timeout"
	}
	change := nodeChange{state: workflow.NodeFailed, failure: code}
	switch {
	case node.Effect == workflow.ExternalEffect:
		change.state = workflow.NodeActionRequired
	case task.epoch < uint64(node.Retry.MaxAttempts) && (code == "lease_expired" || slices.Contains(node.Retry.On, code)):
		next := now
		if code == "timeout" {
			next = now.Add(time.Duration(node.Retry.BackoffSeconds) * time.Second)
		}
		change.state, change.availableAt = workflow.NodeRetryWait, &next
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET state = 'expired', node_state = ?, failure_code = ?, finished_at = ?
		WHERE run_id = ? AND node_key = ? AND ordinal = ? AND state = 'running'`, change.state, code, now, runID, node.Key, task.epoch); err != nil {
		return nodeChange{}, fmt.Errorf("close expired attempt: %w", err)
	}
	if err := applyNodeChange(ctx, tx, runID, node.Key, change, now); err != nil {
		return nodeChange{}, err
	}
	if change.state != workflow.NodeRetryWait {
		if err := advanceRun(ctx, tx, runID, graph, node.Key, now); err != nil {
			return nodeChange{}, err
		}
	}
	return change, nil
}

func startAttempt(ctx context.Context, tx *sql.Tx, worker workerIdentity, run Run, dag int64, node workflow.Node, task taskRecord, inputs json.RawMessage, queue queueAdmission, seconds int, now time.Time) (*ClaimedTask, error) {
	epoch := task.epoch + 1
	deadline := now.Add(time.Duration(node.TimeoutSeconds) * time.Second)
	until := now.Add(time.Duration(seconds) * time.Second)
	if deadline.Before(until) {
		until = deadline
	}
	op := run.operationKey(worker.workspace, dag, node.Key)
	digest, err := hex.DecodeString(op)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE node_runs SET state = 'running', lease_epoch = ?, lease_worker_id = ?, lease_until = ?,
		deadline_at = ?, lease_pool_id = ?, failure_code = NULL WHERE run_id = ? AND node_key = ?`,
		epoch, worker.id, until, deadline, queue.poolID, run.ID, node.Key); err != nil {
		return nil, fmt.Errorf("save claim: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO attempts (run_id, node_key, ordinal, worker_id, operation_key, state,
		queued_at, started_at, lease_until, deadline_at, pool_id) VALUES (?, ?, ?, ?, ?, 'running', ?, ?, ?, ?, ?)`,
		run.ID, node.Key, epoch, worker.id, digest, task.availableAt, now, until, deadline, queue.poolID); err != nil {
		return nil, fmt.Errorf("record attempt: %w", err)
	}
	if run.State == "queued" {
		if _, err := tx.ExecContext(ctx, `UPDATE runs SET state = 'running' WHERE id = ?`, run.ID); err != nil {
			return nil, err
		}
	}
	return &ClaimedTask{LeaseRef: LeaseRef{RunID: run.ID, Node: node.Key, Epoch: epoch, OperationKey: op},
		Task: node.Task, Queue: node.Queue, Pool: queue.pool, Effect: node.Effect, Inputs: inputs, ClaimedAt: now, LeaseUntil: until, Deadline: deadline}, nil
}
