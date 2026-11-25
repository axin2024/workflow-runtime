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
	ScheduleID *int64          `json:"schedule_id,omitempty"`
	Revision   uint64          `json:"revision"`
	TriggerKey string          `json:"trigger_key"`
	Kind       string          `json:"trigger_kind"`
	LogicalAt  time.Time       `json:"logical_at"`
	Inputs     json.RawMessage `json:"inputs"`
	State      string          `json:"state"`
	CreatedAt  time.Time       `json:"created_at"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
}

type TriggerResult struct {
	Run      Run  `json:"run"`
	Replayed bool `json:"replayed"`
}

type NodeRun struct {
	Key          string              `json:"key"`
	Task         string              `json:"task"`
	Queue        string              `json:"queue"`
	OperationKey string              `json:"operation_key"`
	State        string              `json:"state"`
	AvailableAt  *time.Time          `json:"available_at,omitempty"`
	FinishedAt   *time.Time          `json:"finished_at,omitempty"`
	LeaseEpoch   uint64              `json:"lease_epoch"`
	LeaseUntil   *time.Time          `json:"lease_until,omitempty"`
	Branch       *string             `json:"branch,omitempty"`
	Outputs      json.RawMessage     `json:"outputs,omitempty"`
	OutputDigest *string             `json:"output_digest,omitempty"`
	FailureCode  *string             `json:"failure_code,omitempty"`
	Dependencies []DependencyResult  `json:"dependencies,omitempty"`
	Attempts     []AttemptResult     `json:"attempts,omitempty"`
	Resolution   *ExternalResolution `json:"external_resolution,omitempty"`
}

type RunDetail struct {
	Run          Run           `json:"run"`
	Version      Version       `json:"version"`
	Nodes        []NodeRun     `json:"nodes"`
	Cancellation *Cancellation `json:"cancellation,omitempty"`
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
	digest, err := triggerDigest(request.Kind, request.Revision, request.LogicalAt, inputs)
	if err != nil {
		return TriggerResult{}, err
	}
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
	existing, previousDigest, err := runByTrigger(ctx, tx, workspace, dag, request.Key)
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
	if err := validateInputContract(inputs, version.Graph.Definition); err != nil {
		return TriggerResult{}, err
	}
	id, err := insertRun(ctx, tx, workspace, dag, nil, version, request, inputs, digest, scope.Actor)
	if err != nil {
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
	for i := range nodes {
		nodes[i].OperationKey = operationKey(workspace, dag, run.TriggerKey, nodes[i].Key)
	}
	if err := attachExecutionDetails(ctx, tx, id, version.Graph.Definition, nodes); err != nil {
		return RunDetail{}, err
	}
	cancellation, err := loadCancellation(ctx, tx, id)
	if err != nil {
		return RunDetail{}, err
	}
	run.Workspace, run.DAG = scope.Workspace, version.Graph.Definition.Key
	return RunDetail{Run: run, Version: version, Nodes: nodes, Cancellation: cancellation}, tx.Commit()
}

const runColumns = `id, dag_id, schedule_id, revision, trigger_key, trigger_kind, logical_at,
	input_values, state, created_at, finished_at, request_digest`

func readRun(row *sql.Row) (Run, int64, []byte, error) {
	var run Run
	var dag int64
	var inputs, digest []byte
	err := row.Scan(&run.ID, &dag, &run.ScheduleID, &run.Revision, &run.TriggerKey, &run.Kind, &run.LogicalAt,
		&inputs, &run.State, &run.CreatedAt, &run.FinishedAt, &digest)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, 0, nil, ErrNotFound
	}
	if err != nil {
		return Run{}, 0, nil, fmt.Errorf("read run: %w", err)
	}
	run.Inputs = inputs
	return run, dag, digest, nil
}

func runByTrigger(ctx context.Context, tx *sql.Tx, workspace, dag int64, key string) (Run, []byte, error) {
	run, _, digest, err := readRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+`
		FROM runs WHERE workspace_id = ? AND dag_id = ? AND trigger_key = ?`, workspace, dag, key))
	return run, digest, err
}

func triggerDigest(kind string, revision uint64, logicalAt *time.Time, inputs json.RawMessage) ([32]byte, error) {
	identity, err := json.Marshal(struct {
		Kind      string          `json:"kind"`
		Revision  uint64          `json:"revision"`
		LogicalAt *time.Time      `json:"logical_at"`
		Inputs    json.RawMessage `json:"inputs"`
	}{kind, revision, logicalAt, inputs})
	if err != nil {
		return [32]byte{}, fmt.Errorf("encode trigger identity: %w", err)
	}
	return sha256.Sum256(identity), nil
}

func operationKey(workspace, dag int64, trigger, node string) string {
	identity := fmt.Sprintf("workflow/v1/%d/%d/%x/%s", workspace, dag, trigger, node)
	digest := sha256.Sum256([]byte(identity))
	return hex.EncodeToString(digest[:])
}

func validateInputContract(inputs json.RawMessage, definition workflow.Definition) error {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(inputs, &values); err != nil {
		return fmt.Errorf("decode workflow inputs: %w", err)
	}
	return workflow.ValidateValues("inputs", definition.Inputs, values)
}

func insertRun(ctx context.Context, tx *sql.Tx, workspace, dag int64, scheduleID *int64, version Version,
	request TriggerRequest, inputs json.RawMessage, digest [32]byte, actor string) (int64, error) {
	result, err := tx.ExecContext(ctx, `INSERT INTO runs
		(workspace_id, dag_id, schedule_id, revision, trigger_kind, trigger_key, request_digest, input_values, logical_at, state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, UTC_TIMESTAMP(6)), 'queued')`,
		workspace, dag, scheduleID, version.Revision, request.Kind, request.Key, digest[:], string(inputs), request.LogicalAt)
	if err != nil {
		return 0, fmt.Errorf("create run: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read created run ID: %w", err)
	}
	if err := putNodes(ctx, tx, id, version.Graph.Definition); err != nil {
		return 0, err
	}
	if err := writeAudit(ctx, tx, workspace, dag, &id, actor, "run_created", map[string]any{
		"revision": version.Revision, "trigger_kind": request.Kind, "request_digest": hex.EncodeToString(digest[:]),
	}); err != nil {
		return 0, err
	}
	return id, nil
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
	rows, err := tx.QueryContext(ctx, `SELECT node_key, task, queue, state, available_at, finished_at, lease_epoch,
		lease_until, result_branch, output_values, LOWER(HEX(output_digest)), failure_code
		FROM node_runs WHERE run_id = ? ORDER BY node_key`, id)
	if err != nil {
		return nil, fmt.Errorf("list run nodes: %w", err)
	}
	defer rows.Close()
	nodes := make([]NodeRun, 0)
	for rows.Next() {
		var node NodeRun
		var outputs []byte
		if err := rows.Scan(&node.Key, &node.Task, &node.Queue, &node.State, &node.AvailableAt, &node.FinishedAt, &node.LeaseEpoch,
			&node.LeaseUntil, &node.Branch, &outputs, &node.OutputDigest, &node.FailureCode); err != nil {
			return nil, fmt.Errorf("read run node: %w", err)
		}
		node.Outputs = outputs
		nodes = append(nodes, node)
	}
	return nodes, rows.Err()
}
