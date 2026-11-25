package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

var errBoundInputsTooLarge = errors.New("bound node inputs exceed the size limit")

type taskRecord struct {
	state       workflow.NodeState
	epoch       uint64
	workerID    *int64
	availableAt *time.Time
	leaseUntil  *time.Time
	deadline    *time.Time
}

type nodeChange struct {
	state       workflow.NodeState
	availableAt *time.Time
	outputs     json.RawMessage
	branch      string
	failure     string
}

func readTask(ctx context.Context, tx *sql.Tx, runID int64, key string) (taskRecord, error) {
	var task taskRecord
	err := tx.QueryRowContext(ctx, `SELECT state, lease_epoch, lease_worker_id, available_at, lease_until, deadline_at
		FROM node_runs WHERE run_id = ? AND node_key = ?`, runID, key).
		Scan(&task.state, &task.epoch, &task.workerID, &task.availableAt, &task.leaseUntil, &task.deadline)
	if errors.Is(err, sql.ErrNoRows) {
		return taskRecord{}, ErrNotFound
	}
	if err != nil {
		return taskRecord{}, fmt.Errorf("read node state: %w", err)
	}
	return task, nil
}

func nodeDefinition(graph workflow.Definition, key string) (workflow.Node, error) {
	for _, node := range graph.Nodes {
		if node.Key == key {
			return node, nil
		}
	}
	return workflow.Node{}, fmt.Errorf("node %s missing from the frozen graph", key)
}

func taskInputs(ctx context.Context, tx *sql.Tx, run Run, node workflow.Node) (json.RawMessage, error) {
	var source map[string]json.RawMessage
	if err := json.Unmarshal(run.Inputs, &source); err != nil {
		return nil, fmt.Errorf("decode frozen run inputs: %w", err)
	}
	inputs := make(map[string]json.RawMessage, len(node.Input))
	outputs := make(map[string]map[string]json.RawMessage)
	for field, binding := range node.Input {
		if binding.Input != "" {
			inputs[field] = source[binding.Input]
			continue
		}
		values, found := outputs[binding.Node]
		if !found {
			var raw []byte
			if err := tx.QueryRowContext(ctx, `SELECT output_values FROM node_runs WHERE run_id = ? AND node_key = ? AND state = 'succeeded'`,
				run.ID, binding.Node).Scan(&raw); err != nil {
				return nil, fmt.Errorf("read node outputs: %w", err)
			}
			if err := json.Unmarshal(raw, &values); err != nil {
				return nil, fmt.Errorf("decode node outputs: %w", err)
			}
			outputs[binding.Node] = values
		}
		inputs[field] = values[binding.Field]
	}
	result, err := json.Marshal(inputs)
	if err != nil {
		return nil, fmt.Errorf("bind node inputs: %w", err)
	}
	if len(result) > workflow.MaxDefinitionBytes {
		return nil, errBoundInputsTooLarge
	}
	for key, raw := range inputs {
		if len(raw) == 0 {
			return nil, fmt.Errorf("missing bound input %s", key)
		}
	}
	return result, nil
}

func applyNodeChange(ctx context.Context, tx *sql.Tx, runID int64, key string, change nodeChange, now time.Time) error {
	var output, digest, branch, failure any
	var finished *time.Time
	if change.outputs != nil {
		output = string(change.outputs)
		sum := sha256.Sum256(change.outputs)
		digest = sum[:]
	}
	if change.branch != "" {
		branch = change.branch
	}
	if change.failure != "" {
		failure = change.failure
	}
	if change.state.Terminal() {
		finished = &now
	}
	_, err := tx.ExecContext(ctx, `UPDATE node_runs SET state = ?, available_at = ?, lease_worker_id = NULL, lease_until = NULL,
		deadline_at = NULL, lease_pool_id = NULL, result_branch = ?, output_values = ?, output_digest = ?, failure_code = ?, finished_at = ?
		WHERE run_id = ? AND node_key = ?`, change.state, change.availableAt, branch, output, digest, failure, finished, runID, key)
	if err != nil {
		return fmt.Errorf("save node result: %w", err)
	}
	return nil
}

func advanceRun(ctx context.Context, tx *sql.Tx, runID int64, graph workflow.Definition, settled string, now time.Time) error {
	states, err := loadPredecessors(ctx, tx, runID)
	if err != nil {
		return err
	}
	type consumers struct {
		node workflow.Node
		edge workflow.Edge
	}
	outgoing := make(map[string][]consumers)
	for _, node := range graph.Nodes {
		for _, edge := range node.DependsOn {
			outgoing[edge.Node] = append(outgoing[edge.Node], consumers{node, edge})
		}
	}
	queue := []string{settled}
	for i := 0; i < len(queue); i++ {
		key := queue[i]
		if !states[key].State.Terminal() {
			continue
		}
		for _, next := range outgoing[key] {
			edge := workflow.EvaluateEdge(next.edge, states[key])
			if _, err := tx.ExecContext(ctx, `INSERT INTO edge_results (run_id, to_node, from_node, satisfied, reason, decided_at)
				VALUES (?, ?, ?, ?, ?, ?)`, runID, next.node.Key, key, edge.Satisfied, edge.Reason, now); err != nil {
				return fmt.Errorf("save edge decision: %w", err)
			}
			if states[next.node.Key].State != workflow.NodeWaiting {
				continue
			}
			state := workflow.ResolveDependencies(next.node.DependsOn, states)
			if state == workflow.NodeWaiting {
				continue
			}
			change := nodeChange{state: state}
			if state == workflow.NodeReady {
				change.availableAt = &now
			} else {
				queue = append(queue, next.node.Key)
			}
			if err := applyNodeChange(ctx, tx, runID, next.node.Key, change, now); err != nil {
				return err
			}
			states[next.node.Key] = workflow.Predecessor{State: state}
		}
	}
	return refreshRunState(ctx, tx, runID, states, now)
}

func loadPredecessors(ctx context.Context, tx *sql.Tx, runID int64) (map[string]workflow.Predecessor, error) {
	rows, err := tx.QueryContext(ctx, `SELECT node_key, state, COALESCE(result_branch, '') FROM node_runs WHERE run_id = ?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	states := make(map[string]workflow.Predecessor)
	for rows.Next() {
		var key string
		var value workflow.Predecessor
		if err := rows.Scan(&key, &value.State, &value.Branch); err != nil {
			return nil, err
		}
		states[key] = value
	}
	return states, rows.Err()
}

func refreshRunState(ctx context.Context, tx *sql.Tx, runID int64, states map[string]workflow.Predecessor, now time.Time) error {
	allTerminal, failed, canceled, manual, runnable := true, false, false, false, false
	for _, node := range states {
		allTerminal = allTerminal && node.State.Terminal()
		failed = failed || node.State == workflow.NodeFailed
		canceled = canceled || node.State == workflow.NodeCanceled
		manual = manual || node.State == workflow.NodeActionRequired
		runnable = runnable || node.State == workflow.NodeReady || node.State == workflow.NodeRunning || node.State == workflow.NodeRetryWait
	}
	state := "running"
	var finished *time.Time
	if allTerminal {
		state, finished = "succeeded", &now
		if failed {
			state = "failed"
		} else if canceled {
			state = "canceled"
		}
	} else if manual && !runnable {
		state = "action_required"
	}
	_, err := tx.ExecContext(ctx, `UPDATE runs SET state = ?, finished_at = ? WHERE id = ?`, state, finished, runID)
	return err
}
