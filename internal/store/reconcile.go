package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type ResolveRequest struct {
	Key      string             `json:"request_key"`
	Epoch    uint64             `json:"epoch"`
	Evidence string             `json:"evidence"`
	State    workflow.NodeState `json:"state"`
	Branch   string             `json:"branch,omitempty"`
	Outputs  json.RawMessage    `json:"outputs,omitempty"`
}

type ExternalResolution struct {
	LeaseRef
	Key        string             `json:"request_key"`
	Actor      string             `json:"actor"`
	Evidence   string             `json:"evidence"`
	State      workflow.NodeState `json:"state"`
	ResolvedAt time.Time          `json:"resolved_at"`
	Replayed   bool               `json:"replayed"`
}

const resolutionsQuery = `SELECT r.node_key, r.ordinal, LOWER(HEX(a.operation_key)), r.request_key,
	r.actor, r.evidence, r.state, r.resolved_at, r.request_digest FROM external_resolutions r
	JOIN attempts a ON a.run_id = r.run_id AND a.node_key = r.node_key AND a.ordinal = r.ordinal WHERE r.run_id = ?`

func readResolution(row interface{ Scan(...any) error }, runID int64) (ExternalResolution, []byte, error) {
	record := ExternalResolution{LeaseRef: LeaseRef{RunID: runID}}
	var digest []byte
	err := row.Scan(&record.Node, &record.Epoch, &record.OperationKey, &record.Key, &record.Actor,
		&record.Evidence, &record.State, &record.ResolvedAt, &digest)
	return record, digest, err
}

func (s *Store) ResolveExternal(ctx context.Context, scope Scope, runID int64, key string, request ResolveRequest) (ExternalResolution, error) {
	if err := scope.validate(); err != nil {
		return ExternalResolution{}, err
	}
	if runID <= 0 || !identifier.MatchString(key) || !triggerKey.MatchString(request.Key) ||
		request.Epoch == 0 || !validControlNote(request.Evidence) {
		return ExternalResolution{}, errors.New("resolution requires a run, node, request key, epoch and evidence")
	}
	switch request.State {
	case workflow.NodeSucceeded:
		if len(request.Outputs) == 0 {
			request.Outputs = json.RawMessage(`{}`)
		}
		outputs, err := normalizedInputs(request.Outputs)
		if err != nil {
			return ExternalResolution{}, err
		}
		request.Outputs = outputs
	case workflow.NodeFailed:
		if request.Branch != "" || len(request.Outputs) != 0 {
			return ExternalResolution{}, errors.New("failed resolution cannot include a branch or output")
		}
	default:
		return ExternalResolution{}, errors.New("resolution state must be succeeded or failed")
	}
	identity, err := json.Marshal(request)
	if err != nil {
		return ExternalResolution{}, err
	}
	digest := sha256.Sum256(identity)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ExternalResolution{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return ExternalResolution{}, err
	}
	run, dag, err := lockWorkerRun(ctx, tx, workspace, runID, false)
	if err != nil {
		return ExternalResolution{}, err
	}
	record, original, err := readResolution(tx.QueryRowContext(ctx, resolutionsQuery+` AND r.node_key = ?`, run.ID, key), run.ID)
	if err == nil {
		if !bytes.Equal(original, digest[:]) {
			return ExternalResolution{}, ErrRunControlConflict
		}
		record.Replayed = true
		return record, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return ExternalResolution{}, err
	}
	task, err := readTask(ctx, tx, run.ID, key)
	if err != nil {
		return ExternalResolution{}, err
	}
	if task.state != workflow.NodeActionRequired || task.epoch != request.Epoch {
		return ExternalResolution{}, ErrRunControlConflict
	}
	version, err := loadVersion(ctx, tx, dag, run.Revision)
	if err != nil {
		return ExternalResolution{}, err
	}
	node, err := nodeDefinition(version.Graph.Definition, key)
	if err != nil {
		return ExternalResolution{}, err
	}
	if node.Effect != workflow.ExternalEffect {
		return ExternalResolution{}, ErrRunControlConflict
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return ExternalResolution{}, err
	}
	change := nodeChange{state: workflow.NodeFailed, failure: "external_confirmed_failed"}
	if request.State == workflow.NodeSucceeded {
		change, _, err = completionChange(CompletionRequest{Success: true, Branch: request.Branch, Outputs: request.Outputs}, node, now)
		if err != nil {
			return ExternalResolution{}, err
		}
	}
	record = ExternalResolution{
		LeaseRef: LeaseRef{RunID: run.ID, Node: key, Epoch: task.epoch, OperationKey: run.operationKey(workspace, dag, key)},
		Key:      request.Key, Actor: scope.Actor, Evidence: request.Evidence, State: request.State, ResolvedAt: now,
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO external_resolutions
		(run_id, node_key, ordinal, request_key, request_digest, actor, evidence, state, resolved_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, run.ID, key, task.epoch, request.Key, digest[:], scope.Actor, request.Evidence, request.State, now); err != nil {
		return ExternalResolution{}, err
	}
	if err := applyNodeChange(ctx, tx, run.ID, key, change, now); err != nil {
		return ExternalResolution{}, err
	}
	canceled, err := loadCancellation(ctx, tx, run.ID)
	if err != nil {
		return ExternalResolution{}, err
	}
	if canceled != nil {
		err = refreshCanceledRun(ctx, tx, run.ID, now)
	} else {
		err = advanceRun(ctx, tx, run.ID, version.Graph.Definition, key, now)
	}
	if err != nil {
		return ExternalResolution{}, err
	}
	return record, tx.Commit()
}
