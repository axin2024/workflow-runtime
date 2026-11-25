package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

var ErrInvalidWorkerRequest = errors.New("invalid worker request")

type RenewRequest struct {
	Lease        LeaseRef `json:"lease"`
	LeaseSeconds int      `json:"lease_seconds"`
}

type LeaseRenewal struct {
	RenewedAt      time.Time  `json:"renewed_at"`
	LeaseUntil     time.Time  `json:"lease_until"`
	CancelDeadline *time.Time `json:"cancel_deadline,omitempty"`
}

type CompletionRequest struct {
	Lease          LeaseRef        `json:"lease"`
	Success        bool            `json:"success"`
	Branch         string          `json:"branch,omitempty"`
	Outputs        json.RawMessage `json:"outputs,omitempty"`
	FailureCode    string          `json:"failure_code,omitempty"`
	FailureMessage string          `json:"failure_message,omitempty"`
	LogExcerpt     string          `json:"log_excerpt,omitempty"`
}

type CompletionReceipt struct {
	LeaseRef
	State    workflow.NodeState `json:"state"`
	Replayed bool               `json:"replayed"`
}

func (s *Store) Renew(ctx context.Context, token string, request RenewRequest) (LeaseRenewal, error) {
	if !request.Lease.valid() || !validLeaseSeconds(request.LeaseSeconds) {
		return LeaseRenewal{}, fmt.Errorf("%w: a complete lease and a duration of 1 to 120 seconds are required", ErrInvalidWorkerRequest)
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return LeaseRenewal{}, err
	}
	defer tx.Rollback()
	worker, err := lockWorker(ctx, tx, token)
	if err != nil {
		return LeaseRenewal{}, err
	}
	if err := lockRenewalCapacity(ctx, tx, worker, request.Lease); err != nil {
		return LeaseRenewal{}, err
	}
	run, _, task, err := lockLeaseRun(ctx, tx, worker, request.Lease)
	if err != nil {
		return LeaseRenewal{}, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return LeaseRenewal{}, err
	}
	if !liveLease(task, worker.id, now) {
		return LeaseRenewal{}, ErrLeaseLost
	}
	if run.State == "canceling" {
		cancel, err := loadCancellation(ctx, tx, run.ID)
		if err != nil {
			return LeaseRenewal{}, err
		}
		return LeaseRenewal{RenewedAt: now, LeaseUntil: *task.leaseUntil, CancelDeadline: &cancel.Deadline}, nil
	}
	until := now.Add(time.Duration(request.LeaseSeconds) * time.Second)
	if task.leaseUntil.After(until) {
		until = *task.leaseUntil
	}
	if task.deadline.Before(until) {
		until = *task.deadline
	}
	if _, err := tx.ExecContext(ctx, `UPDATE node_runs SET lease_until = ? WHERE run_id = ? AND node_key = ?`, until, run.ID, request.Lease.Node); err != nil {
		return LeaseRenewal{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET lease_until = ? WHERE run_id = ? AND node_key = ? AND ordinal = ?`,
		until, run.ID, request.Lease.Node, request.Lease.Epoch); err != nil {
		return LeaseRenewal{}, err
	}
	return LeaseRenewal{RenewedAt: now, LeaseUntil: until}, tx.Commit()
}

// Complete fences expired attempts and records the accepted result and all
// affected dependency states in a single transaction.
func (s *Store) Complete(ctx context.Context, token string, request CompletionRequest) (CompletionReceipt, error) {
	digest, err := normalizeCompletion(&request)
	if err != nil {
		return CompletionReceipt{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return CompletionReceipt{}, err
	}
	defer tx.Rollback()
	worker, run, dag, task, err := lockLease(ctx, tx, token, request.Lease)
	if err != nil {
		return CompletionReceipt{}, err
	}
	if task.state != workflow.NodeRunning {
		return previousCompletion(ctx, tx, worker.id, request.Lease, digest)
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return CompletionReceipt{}, err
	}
	if !liveLease(task, worker.id, now) {
		return CompletionReceipt{}, ErrLeaseLost
	}
	if run.State == "canceling" {
		return CompletionReceipt{}, ErrCancellationRequested
	}
	version, err := loadVersion(ctx, tx, dag, run.Revision)
	if err != nil {
		return CompletionReceipt{}, err
	}
	node, err := nodeDefinition(version.Graph.Definition, request.Lease.Node)
	if err != nil {
		return CompletionReceipt{}, err
	}
	change, outcome, err := completionChange(request, node, now)
	if err != nil {
		return CompletionReceipt{}, err
	}
	var logDigest []byte
	if request.LogExcerpt != "" {
		hash := sha256.Sum256([]byte(request.LogExcerpt))
		logDigest = hash[:]
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET state = ?, node_state = ?, finished_at = ?, result_digest = ?,
		failure_code = NULLIF(?, ''), failure_message = NULLIF(?, ''), log_excerpt = NULLIF(?, ''), log_digest = ?
		WHERE run_id = ? AND node_key = ? AND ordinal = ?`,
		outcome, change.state, now, digest[:], request.FailureCode, request.FailureMessage, request.LogExcerpt, logDigest,
		run.ID, node.Key, request.Lease.Epoch); err != nil {
		return CompletionReceipt{}, fmt.Errorf("write attempt result: %w", err)
	}
	if logDigest != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO retention_log_queue (workspace_id, run_id, node_key, ordinal, finished_at)
			VALUES (?, ?, ?, ?, ?)`, worker.workspace, run.ID, node.Key, request.Lease.Epoch, now); err != nil {
			return CompletionReceipt{}, fmt.Errorf("index attempt log: %w", err)
		}
	}
	if err := applyNodeChange(ctx, tx, run.ID, node.Key, change, now); err != nil {
		return CompletionReceipt{}, err
	}
	if change.state != workflow.NodeRetryWait {
		if err := advanceRun(ctx, tx, run.ID, version.Graph.Definition, node.Key, now); err != nil {
			return CompletionReceipt{}, err
		}
	}
	return CompletionReceipt{LeaseRef: request.Lease, State: change.state}, tx.Commit()
}

func lockLease(ctx context.Context, tx *sql.Tx, token string, ref LeaseRef) (workerIdentity, Run, int64, taskRecord, error) {
	worker, err := lockWorker(ctx, tx, token)
	if err != nil {
		return workerIdentity{}, Run{}, 0, taskRecord{}, err
	}
	run, dag, task, err := lockLeaseRun(ctx, tx, worker, ref)
	return worker, run, dag, task, err
}

func lockLeaseRun(ctx context.Context, tx *sql.Tx, worker workerIdentity, ref LeaseRef) (Run, int64, taskRecord, error) {
	run, dag, err := lockWorkerRun(ctx, tx, worker.workspace, ref.RunID, false)
	if errors.Is(err, ErrNotFound) {
		return Run{}, 0, taskRecord{}, ErrLeaseLost
	}
	if err != nil {
		return Run{}, 0, taskRecord{}, err
	}
	if run.operationKey(worker.workspace, dag, ref.Node) != ref.OperationKey {
		return Run{}, 0, taskRecord{}, ErrLeaseLost
	}
	task, err := readTask(ctx, tx, run.ID, ref.Node)
	if errors.Is(err, ErrNotFound) || (err == nil && task.epoch != ref.Epoch) {
		return Run{}, 0, taskRecord{}, ErrLeaseLost
	}
	return run, dag, task, err
}

func liveLease(task taskRecord, workerID int64, now time.Time) bool {
	return task.state == workflow.NodeRunning && task.workerID != nil && *task.workerID == workerID &&
		task.leaseUntil != nil && task.leaseUntil.After(now) && task.deadline != nil && task.deadline.After(now)
}

func previousCompletion(ctx context.Context, tx *sql.Tx, workerID int64, ref LeaseRef, digest [32]byte) (CompletionReceipt, error) {
	var state *workflow.NodeState
	var existing []byte
	err := tx.QueryRowContext(ctx, `SELECT node_state, result_digest FROM attempts
		WHERE run_id = ? AND node_key = ? AND ordinal = ? AND worker_id = ?`, ref.RunID, ref.Node, ref.Epoch, workerID).Scan(&state, &existing)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && (len(existing) == 0 || state == nil)) {
		return CompletionReceipt{}, ErrLeaseLost
	}
	if err != nil {
		return CompletionReceipt{}, err
	}
	if !bytes.Equal(existing, digest[:]) {
		return CompletionReceipt{}, ErrCompletionConflict
	}
	return CompletionReceipt{LeaseRef: ref, State: *state, Replayed: true}, nil
}

func normalizeCompletion(request *CompletionRequest) ([32]byte, error) {
	if !request.Lease.valid() {
		return [32]byte{}, fmt.Errorf("%w: a complete lease is required", ErrInvalidWorkerRequest)
	}
	if len(request.LogExcerpt) > 4096 || !utf8.ValidString(request.LogExcerpt) {
		return [32]byte{}, fmt.Errorf("%w: a log excerpt must be valid UTF-8 and at most 4096 bytes", ErrInvalidWorkerRequest)
	}
	if request.Success {
		if request.FailureCode != "" || request.FailureMessage != "" {
			return [32]byte{}, fmt.Errorf("%w: a successful result cannot include a failure", ErrInvalidWorkerRequest)
		}
		if len(request.Outputs) == 0 {
			request.Outputs = json.RawMessage(`{}`)
		}
		output, err := normalizedInputs(request.Outputs)
		if err != nil {
			return [32]byte{}, fmt.Errorf("%w: invalid output object: %v", ErrInvalidWorkerRequest, err)
		}
		request.Outputs = output
	} else if !identifier.MatchString(request.FailureCode) || len(request.FailureMessage) > 1024 ||
		!utf8.ValidString(request.FailureMessage) || request.Branch != "" || len(request.Outputs) != 0 {
		return [32]byte{}, fmt.Errorf("%w: a failure requires a code and may include a message", ErrInvalidWorkerRequest)
	}
	data, err := json.Marshal(request)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}

func completionChange(request CompletionRequest, node workflow.Node, now time.Time) (nodeChange, string, error) {
	if request.Success {
		if (len(node.Branches) == 0 && request.Branch != "") || (len(node.Branches) > 0 && !slices.Contains(node.Branches, request.Branch)) {
			return nodeChange{}, "", fmt.Errorf("%w: success must select an allowed branch when the node declares branches", ErrInvalidWorkerRequest)
		}
		var output map[string]json.RawMessage
		if err := json.Unmarshal(request.Outputs, &output); err != nil {
			return nodeChange{}, "", err
		}
		if err := workflow.ValidateValues("outputs", node.Output, output); err != nil {
			return nodeChange{}, "", fmt.Errorf("%w: %v", ErrInvalidWorkerRequest, err)
		}
		return nodeChange{state: workflow.NodeSucceeded, outputs: request.Outputs, branch: request.Branch}, "succeeded", nil
	}
	result := nodeChange{state: workflow.NodeFailed, failure: request.FailureCode}
	if node.Effect == workflow.ExternalEffect && (request.FailureCode == "unknown" || request.FailureCode == "timeout") {
		result.state = workflow.NodeActionRequired
	} else if request.Lease.Epoch < uint64(node.Retry.MaxAttempts) && slices.Contains(node.Retry.On, request.FailureCode) {
		readyAt := now.Add(time.Duration(node.Retry.BackoffSeconds) * time.Second)
		result.state, result.availableAt = workflow.NodeRetryWait, &readyAt
	}
	return result, "failed", nil
}
