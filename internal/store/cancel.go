package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

var (
	ErrRunControlConflict    = errors.New("run operation conflicts with recorded state or request")
	ErrCancellationRequested = errors.New("run cancellation was requested")
)

type CancelRequest struct {
	Key          string
	Reason       string
	GraceSeconds int
}

type Cancellation struct {
	RunID        int64      `json:"run_id"`
	Key          string     `json:"request_key"`
	Actor        string     `json:"actor"`
	Reason       string     `json:"reason"`
	GraceSeconds int        `json:"grace_seconds"`
	RequestedAt  time.Time  `json:"requested_at"`
	Deadline     time.Time  `json:"deadline"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	Replayed     bool       `json:"replayed"`
}

func loadCancellation(ctx context.Context, tx *sql.Tx, runID int64) (*Cancellation, error) {
	record := Cancellation{RunID: runID}
	err := tx.QueryRowContext(ctx, `SELECT request_key, actor, reason, grace_seconds, requested_at, deadline_at, finished_at
		FROM cancel_requests WHERE run_id = ?`, runID).
		Scan(&record.Key, &record.Actor, &record.Reason, &record.GraceSeconds, &record.RequestedAt, &record.Deadline, &record.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &record, err
}

func validControlNote(note string) bool {
	return strings.TrimSpace(note) != "" && len(note) <= 1024 && utf8.ValidString(note)
}

func (s *Store) CancelRun(ctx context.Context, scope Scope, runID int64, request CancelRequest) (Cancellation, error) {
	if err := scope.validate(); err != nil {
		return Cancellation{}, err
	}
	if runID <= 0 || !triggerKey.MatchString(request.Key) || !validControlNote(request.Reason) ||
		request.GraceSeconds < 0 || request.GraceSeconds > 120 {
		return Cancellation{}, errors.New("cancel requires a run, request key, reason, and grace of 0 to 120 seconds")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Cancellation{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return Cancellation{}, err
	}
	run, dag, err := lockWorkerRun(ctx, tx, workspace, runID, false)
	if err != nil {
		return Cancellation{}, err
	}
	existing, err := loadCancellation(ctx, tx, run.ID)
	if err != nil {
		return Cancellation{}, err
	}
	if existing != nil {
		if existing.Key != request.Key || existing.Reason != request.Reason || existing.GraceSeconds != request.GraceSeconds {
			return Cancellation{}, ErrRunControlConflict
		}
		existing.Replayed = true
		return *existing, nil
	}
	switch run.State {
	case "queued", "running", "action_required":
	default:
		return Cancellation{}, ErrRunControlConflict
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return Cancellation{}, err
	}
	deadline := now.Add(time.Duration(request.GraceSeconds) * time.Second)
	if _, err := tx.ExecContext(ctx, `INSERT INTO cancel_requests
		(run_id, request_key, actor, reason, grace_seconds, requested_at, deadline_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		run.ID, request.Key, scope.Actor, request.Reason, request.GraceSeconds, now, deadline); err != nil {
		return Cancellation{}, err
	}
	if err := cancelUnstarted(ctx, tx, run, dag, now); err != nil {
		return Cancellation{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE node_runs n JOIN attempts a
		ON a.run_id = n.run_id AND a.node_key = n.node_key AND a.ordinal = n.lease_epoch
		SET n.lease_until = LEAST(n.lease_until, ?), a.lease_until = LEAST(a.lease_until, ?)
		WHERE n.run_id = ? AND n.state = 'running'`, deadline, deadline, run.ID); err != nil {
		return Cancellation{}, err
	}
	if err := refreshCanceledRun(ctx, tx, run.ID, now); err != nil {
		return Cancellation{}, err
	}
	record, err := loadCancellation(ctx, tx, run.ID)
	if err != nil {
		return Cancellation{}, err
	}
	return *record, tx.Commit()
}

func cancelUnstarted(ctx context.Context, tx *sql.Tx, run Run, dag int64, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `UPDATE node_runs SET state = 'canceled', available_at = NULL, finished_at = ?, failure_code = 'run_canceled'
		WHERE run_id = ? AND state IN ('ready', 'waiting', 'retry_wait')`, now, run.ID); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO edge_results (run_id, to_node, from_node, satisfied, reason, decided_at)
		SELECT n.run_id, e.to_node, e.from_node, FALSE, 'run_canceled', ?
		FROM dag_edges e JOIN node_runs n ON n.run_id = ? AND n.node_key = e.to_node
		LEFT JOIN edge_results existing ON existing.run_id = n.run_id AND existing.to_node = e.to_node AND existing.from_node = e.from_node
		WHERE e.dag_id = ? AND e.revision = ? AND n.state = 'canceled' AND existing.run_id IS NULL`, now, run.ID, dag, run.Revision)
	return err
}

func refreshCanceledRun(ctx context.Context, tx *sql.Tx, runID int64, now time.Time) error {
	var running, manual bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM node_runs WHERE run_id = ? AND state = 'running'),
		EXISTS(SELECT 1 FROM node_runs WHERE run_id = ? AND state = 'action_required')`, runID, runID).Scan(&running, &manual); err != nil {
		return err
	}
	state := "canceled"
	var finished *time.Time
	switch {
	case running:
		state = "canceling"
	case manual:
		state = "action_required"
	default:
		finished = &now
		if _, err := tx.ExecContext(ctx, `UPDATE cancel_requests SET finished_at = COALESCE(finished_at, ?) WHERE run_id = ?`, now, runID); err != nil {
			return err
		}
	}
	return saveRunState(ctx, tx, runID, state, finished)
}

func cancelAttempt(ctx context.Context, tx *sql.Tx, runID int64, node workflow.Node, epoch uint64, now time.Time, outcome string) (workflow.NodeState, error) {
	change := nodeChange{state: workflow.NodeCanceled, failure: outcome}
	if node.Effect == workflow.ExternalEffect {
		change.state, change.failure = workflow.NodeActionRequired, "external_cancellation_unknown"
	}
	if _, err := tx.ExecContext(ctx, `UPDATE attempts SET state = ?, node_state = ?, failure_code = ?, finished_at = ?
		WHERE run_id = ? AND node_key = ? AND ordinal = ? AND state = 'running'`, outcome, change.state, change.failure, now, runID, node.Key, epoch); err != nil {
		return "", err
	}
	if err := applyNodeChange(ctx, tx, runID, node.Key, change, now); err != nil {
		return "", err
	}
	return change.state, refreshCanceledRun(ctx, tx, runID, now)
}

func (s *Store) AcknowledgeCancel(ctx context.Context, token string, ref LeaseRef) (CompletionReceipt, error) {
	if !ref.valid() {
		return CompletionReceipt{}, ErrInvalidWorkerRequest
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return CompletionReceipt{}, err
	}
	defer tx.Rollback()
	worker, run, dag, task, err := lockLease(ctx, tx, token, ref)
	if err != nil {
		return CompletionReceipt{}, err
	}
	if task.state != workflow.NodeRunning {
		var state workflow.NodeState
		err := tx.QueryRowContext(ctx, `SELECT node_state FROM attempts
			WHERE run_id = ? AND node_key = ? AND ordinal = ? AND worker_id = ? AND state = 'canceled'`,
			ref.RunID, ref.Node, ref.Epoch, worker.id).Scan(&state)
		if errors.Is(err, sql.ErrNoRows) {
			return CompletionReceipt{}, ErrLeaseLost
		}
		return CompletionReceipt{LeaseRef: ref, State: state, Replayed: true}, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return CompletionReceipt{}, err
	}
	if !liveLease(task, worker.id, now) {
		return CompletionReceipt{}, ErrLeaseLost
	}
	if run.State != "canceling" {
		return CompletionReceipt{}, ErrRunControlConflict
	}
	version, err := loadVersion(ctx, tx, dag, run.Revision)
	if err != nil {
		return CompletionReceipt{}, err
	}
	node, err := nodeDefinition(version.Graph.Definition, ref.Node)
	if err != nil {
		return CompletionReceipt{}, err
	}
	state, err := cancelAttempt(ctx, tx, run.ID, node, task.epoch, now, "canceled")
	if err != nil {
		return CompletionReceipt{}, err
	}
	return CompletionReceipt{LeaseRef: ref, State: state}, tx.Commit()
}
