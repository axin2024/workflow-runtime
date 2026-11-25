package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

var triggerKey = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)

type TriggerRequest struct {
	DAG       string
	Key       string
	Kind      string
	Revision  uint64
	LogicalAt *time.Time
	Inputs    []byte
}

type TriggerConflict struct{ RunID int64 }

func (c *TriggerConflict) Error() string {
	return fmt.Sprintf("trigger key already belongs to run %d with a different request", c.RunID)
}

type Run struct {
	ID         int64           `json:"id"`
	Workspace  string          `json:"workspace"`
	DAG        string          `json:"dag"`
	Revision   uint64          `json:"revision"`
	TriggerKey string          `json:"trigger_key"`
	Kind       string          `json:"trigger_kind"`
	LogicalAt  time.Time       `json:"logical_at"`
	Inputs     json.RawMessage `json:"inputs"`
	State      string          `json:"state"`
	CreatedAt  time.Time       `json:"created_at"`
}

type TriggerResult struct {
	Run      Run  `json:"run"`
	Replayed bool `json:"replayed"`
}

type NodeRun struct {
	Key         string     `json:"key"`
	Task        string     `json:"task"`
	Queue       string     `json:"queue"`
	State       string     `json:"state"`
	AvailableAt *time.Time `json:"available_at,omitempty"`
}

type RunDetail struct {
	Run     Run       `json:"run"`
	Version Version   `json:"version"`
	Nodes   []NodeRun `json:"nodes"`
}

// Trigger installs the run and all its initial nodes in one transaction. Looking
// up the logical key before resolving the active revision makes retries stable.
func (s *Store) Trigger(ctx context.Context, scope Scope, request TriggerRequest) (TriggerResult, error) {
	if err := scope.validate(); err != nil {
		return TriggerResult{}, err
	}
	if !identifier.MatchString(request.DAG) || !triggerKey.MatchString(request.Key) {
		return TriggerResult{}, errors.New("invalid workflow or trigger key")
	}
	if request.Kind != "manual" && request.Kind != "external" {
		return TriggerResult{}, errors.New("trigger kind must be manual or external")
	}
	inputs, err := normalizedInputs(request.Inputs)
	if err != nil {
		return TriggerResult{}, err
	}
	if request.LogicalAt != nil {
		logical := request.LogicalAt.UTC().Truncate(time.Microsecond)
		if logical.Year() < 1000 || logical.Year() > 9999 {
			return TriggerResult{}, errors.New("logical time is outside the supported range")
		}
		request.LogicalAt = &logical
	}
	identity, err := json.Marshal(struct {
		Kind      string          `json:"kind"`
		Revision  uint64          `json:"revision"`
		LogicalAt *time.Time      `json:"logical_at"`
		Inputs    json.RawMessage `json:"inputs"`
	}{request.Kind, request.Revision, request.LogicalAt, inputs})
	if err != nil {
		return TriggerResult{}, fmt.Errorf("encode trigger identity: %w", err)
	}
	digest := sha256.Sum256(identity)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return TriggerResult{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return TriggerResult{}, err
	}
	dag, active, err := findDAG(ctx, tx, workspace, request.DAG, true)
	if err != nil {
		return TriggerResult{}, err
	}
	existing, _, previousDigest, err := readRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+`
		FROM runs WHERE workspace_id = ? AND dag_id = ? AND trigger_key = ?`, workspace, dag, request.Key))
	if err == nil {
		if !bytes.Equal(previousDigest, digest[:]) {
			if err := writeAudit(ctx, tx, workspace, dag, &existing.ID, scope.Actor, "trigger_conflict", map[string]string{
				"original_digest": hex.EncodeToString(previousDigest), "received_digest": hex.EncodeToString(digest[:]),
			}); err != nil {
				return TriggerResult{}, err
			}
			if err := tx.Commit(); err != nil {
				return TriggerResult{}, err
			}
			return TriggerResult{}, &TriggerConflict{RunID: existing.ID}
		}
		existing.Workspace, existing.DAG = scope.Workspace, request.DAG
		return TriggerResult{Run: existing, Replayed: true}, tx.Commit()
	}
	if !errors.Is(err, ErrNotFound) {
		return TriggerResult{}, err
	}
	revision := request.Revision
	if revision == 0 {
		revision = active
	}
	version, err := loadVersion(ctx, tx, dag, revision)
	if err != nil {
		return TriggerResult{}, err
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(inputs, &values); err != nil {
		return TriggerResult{}, fmt.Errorf("decode workflow inputs: %w", err)
	}
	if err := workflow.ValidateValues("inputs", version.Graph.Definition.Inputs, values); err != nil {
		return TriggerResult{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO runs
		(workspace_id, dag_id, revision, trigger_kind, trigger_key, request_digest, input_values, logical_at, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, COALESCE(?, UTC_TIMESTAMP(6)), 'queued')`,
		workspace, dag, revision, request.Kind, request.Key, digest[:], string(inputs), request.LogicalAt)
	if err != nil {
		return TriggerResult{}, fmt.Errorf("create run: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return TriggerResult{}, fmt.Errorf("read created run ID: %w", err)
	}
	if err := putNodes(ctx, tx, id, version.Graph.Definition); err != nil {
		return TriggerResult{}, err
	}
	if err := writeAudit(ctx, tx, workspace, dag, &id, scope.Actor, "run_created", map[string]any{
		"revision": revision, "trigger_kind": request.Kind, "request_digest": hex.EncodeToString(digest[:]),
	}); err != nil {
		return TriggerResult{}, err
	}
	run, _, _, err := readRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE id = ?`, id))
	if err != nil {
		return TriggerResult{}, err
	}
	run.Workspace, run.DAG = scope.Workspace, request.DAG
	return TriggerResult{Run: run}, tx.Commit()
}

func (s *Store) GetRun(ctx context.Context, scope Scope, id int64) (RunDetail, error) {
	if err := scope.validate(); err != nil {
		return RunDetail{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return RunDetail{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return RunDetail{}, err
	}
	run, dag, _, err := readRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE workspace_id = ? AND id = ?`, workspace, id))
	if err != nil {
		return RunDetail{}, err
	}
	version, err := loadVersion(ctx, tx, dag, run.Revision)
	if err != nil {
		return RunDetail{}, err
	}
	nodes, err := listNodes(ctx, tx, id)
	if err != nil {
		return RunDetail{}, err
	}
	run.Workspace, run.DAG = scope.Workspace, version.Graph.Definition.Key
	return RunDetail{Run: run, Version: version, Nodes: nodes}, tx.Commit()
}

const runColumns = `id, dag_id, revision, trigger_key, trigger_kind, logical_at,
	input_values, state, created_at, request_digest`

func readRun(row *sql.Row) (Run, int64, []byte, error) {
	var run Run
	var dag int64
	var inputs, digest []byte
	err := row.Scan(&run.ID, &dag, &run.Revision, &run.TriggerKey, &run.Kind, &run.LogicalAt,
		&inputs, &run.State, &run.CreatedAt, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, 0, nil, ErrNotFound
	}
	if err != nil {
		return Run{}, 0, nil, fmt.Errorf("read run: %w", err)
	}
	run.Inputs = inputs
	return run, dag, digest, nil
}

func normalizedInputs(raw []byte) (json.RawMessage, error) {
	if len(raw) > workflow.MaxDefinitionBytes {
		return nil, errors.New("inputs exceed the size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var values map[string]any
	if decoder.Decode(&values) != nil || values == nil || decoder.Decode(new(json.RawMessage)) != io.EOF {
		return nil, errors.New("inputs must be one JSON object")
	}
	inputs, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("normalize inputs: %w", err)
	}
	if len(inputs) > workflow.MaxDefinitionBytes {
		return nil, errors.New("normalized inputs exceed the size limit")
	}
	return inputs, nil
}

func putNodes(ctx context.Context, tx *sql.Tx, id int64, definition workflow.Definition) error {
	values := make([]any, 0, len(definition.Nodes)*6)
	for _, node := range definition.Nodes {
		state := "waiting"
		if len(node.DependsOn) == 0 {
			state = "ready"
		}
		values = append(values, id, node.Key, node.Task, node.Queue, state, state)
	}
	if err := insertRows(ctx, tx, `INSERT INTO node_runs (run_id, node_key, task, queue, state, available_at) VALUES `,
		`(?, ?, ?, ?, ?, IF(? = 'ready', UTC_TIMESTAMP(6), NULL))`, 6, values); err != nil {
		return fmt.Errorf("write initial run nodes: %w", err)
	}
	return nil
}

func listNodes(ctx context.Context, tx *sql.Tx, id int64) ([]NodeRun, error) {
	rows, err := tx.QueryContext(ctx, `SELECT node_key, task, queue, state, available_at
		FROM node_runs WHERE run_id = ? ORDER BY node_key`, id)
	if err != nil {
		return nil, fmt.Errorf("list run nodes: %w", err)
	}
	defer rows.Close()
	nodes := make([]NodeRun, 0)
	for rows.Next() {
		var node NodeRun
		if err := rows.Scan(&node.Key, &node.Task, &node.Queue, &node.State, &node.AvailableAt); err != nil {
			return nil, fmt.Errorf("read run node: %w", err)
		}
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}
