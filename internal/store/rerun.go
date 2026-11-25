package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type OutputArtifact struct {
	RunID  int64  `json:"run_id"`
	Node   string `json:"node"`
	Epoch  uint64 `json:"epoch"`
	SHA256 string `json:"sha256"`
}

type RerunPlan struct {
	ParentRunID int64       `json:"parent_run_id"`
	DAG         string      `json:"dag"`
	Revision    uint64      `json:"revision"`
	GraphDigest string      `json:"graph_digest"`
	InputDigest string      `json:"input_digest"`
	Selected    []string    `json:"selected"`
	Nodes       []RerunNode `json:"nodes"`
}

type RerunNode struct {
	Key       string             `json:"key"`
	Action    string             `json:"action"`
	State     workflow.NodeState `json:"initial_state"`
	SourceRun *int64             `json:"source_run_id,omitempty"`
	Artifact  *OutputArtifact    `json:"source_artifact,omitempty"`
}

type RerunRecord struct {
	RunID     int64     `json:"run_id"`
	Key       string    `json:"request_key"`
	Actor     string    `json:"actor"`
	CreatedAt time.Time `json:"created_at"`
	Plan      RerunPlan `json:"plan"`
	Replayed  bool      `json:"replayed"`
}

type RerunChild struct {
	RunID     int64     `json:"run_id"`
	Key       string    `json:"request_key"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
}

func normalizeSelection(selection []string) ([]string, error) {
	if len(selection) == 0 || len(selection) > 256 {
		return nil, errors.New("select 1 to 256 failed nodes")
	}
	keys := slices.Clone(selection)
	slices.Sort(keys)
	for i, key := range keys {
		if !identifier.MatchString(key) || (i > 0 && key == keys[i-1]) {
			return nil, errors.New("selected nodes must be distinct lowercase identifiers")
		}
	}
	return keys, nil
}

func (s *Store) PreviewRerun(ctx context.Context, scope Scope, parentID int64, selection []string) (RerunPlan, error) {
	if err := scope.validate(); err != nil {
		return RerunPlan{}, err
	}
	selected, err := normalizeSelection(selection)
	if err != nil {
		return RerunPlan{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return RerunPlan{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return RerunPlan{}, err
	}
	parent, dag, _, err := readRun(tx.QueryRowContext(ctx, `SELECT `+runColumns+` FROM runs WHERE workspace_id = ? AND id = ?`, workspace, parentID))
	if err != nil {
		return RerunPlan{}, err
	}
	plan, _, _, err := loadRerunPlan(ctx, tx, parent, dag, selected)
	if err != nil {
		return RerunPlan{}, err
	}
	return plan, tx.Commit()
}

func (s *Store) CreateRerun(ctx context.Context, scope Scope, parentID int64, key string, selection []string) (RerunRecord, error) {
	if err := scope.validate(); err != nil {
		return RerunRecord{}, err
	}
	if !triggerKey.MatchString(key) {
		return RerunRecord{}, errors.New("invalid rerun request key")
	}
	selected, err := normalizeSelection(selection)
	if err != nil {
		return RerunRecord{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return RerunRecord{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return RerunRecord{}, err
	}
	var dag int64
	// Match the lock order of publication and scheduling before inserting a
	// child run with foreign keys to this workflow and its frozen revision.
	err = tx.QueryRowContext(ctx, `SELECT d.id FROM dags d WHERE d.workspace_id = ?
		AND d.id = (SELECT dag_id FROM runs WHERE workspace_id = ? AND id = ?) FOR UPDATE`,
		workspace, workspace, parentID).Scan(&dag)
	if errors.Is(err, sql.ErrNoRows) {
		return RerunRecord{}, ErrNotFound
	}
	if err != nil {
		return RerunRecord{}, err
	}
	parent, _, err := lockWorkerRun(ctx, tx, workspace, parentID, false)
	if err != nil {
		return RerunRecord{}, err
	}
	exists, err := readRerun(tx.QueryRowContext(ctx, rerunColumns+` WHERE parent_run_id = ? AND request_key = ?`, parentID, key))
	if err != nil {
		return RerunRecord{}, err
	}
	if exists != nil {
		if !slices.Equal(exists.Plan.Selected, selected) {
			return RerunRecord{}, ErrRunControlConflict
		}
		exists.Replayed = true
		return *exists, nil
	}
	plan, version, baseline, err := loadRerunPlan(ctx, tx, parent, dag, selected)
	if err != nil {
		return RerunRecord{}, err
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return RerunRecord{}, err
	}
	runKey := fmt.Sprintf("~rerun:%d:%x", parentID, sha256.Sum256([]byte(key)))
	child, err := insertRun(ctx, tx, workspace, dag, nil, version, TriggerRequest{Key: runKey, Kind: "rerun", LogicalAt: &parent.LogicalAt},
		parent.Inputs, sha256.Sum256(raw), scope.Actor)
	if err != nil {
		return RerunRecord{}, err
	}
	operationTrigger := parent.TriggerKey
	if parent.operationTriggerKey != nil {
		operationTrigger = *parent.operationTriggerKey
	}
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET operation_trigger_key = ? WHERE id = ?`, operationTrigger, child); err != nil {
		return RerunRecord{}, err
	}
	if err := initializeRerun(ctx, tx, child, plan, baseline, version.Graph); err != nil {
		return RerunRecord{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO reruns (new_run_id, workspace_id, dag_id, parent_run_id, request_key, actor, plan)
		VALUES (?, ?, ?, ?, ?, ?, ?)`, child, workspace, dag, parentID, key, scope.Actor, string(raw)); err != nil {
		return RerunRecord{}, err
	}
	record, err := readRerun(tx.QueryRowContext(ctx, rerunColumns+` WHERE new_run_id = ?`, child))
	if err != nil {
		return RerunRecord{}, err
	}
	return *record, tx.Commit()
}

func loadRerunPlan(ctx context.Context, tx *sql.Tx, parent Run, dag int64, selection []string) (RerunPlan, Version, map[string]NodeRun, error) {
	if parent.State != "failed" && parent.State != "canceled" {
		return RerunPlan{}, Version{}, nil, fmt.Errorf("%w: select failed nodes after the source run terminates", ErrRunControlConflict)
	}
	version, err := loadVersion(ctx, tx, dag, parent.Revision)
	if err != nil {
		return RerunPlan{}, Version{}, nil, err
	}
	nodes, err := listNodes(ctx, tx, parent.ID, false)
	if err != nil {
		return RerunPlan{}, Version{}, nil, err
	}
	baseline := make(map[string]NodeRun, len(nodes))
	for _, node := range nodes {
		if !workflow.NodeState(node.State).Terminal() {
			return RerunPlan{}, Version{}, nil, fmt.Errorf("%w: source node %s is still unsettled", ErrRunControlConflict, node.Key)
		}
		baseline[node.Key] = node
	}
	plan, err := buildRerunPlan(parent, version.Graph, baseline, selection)
	return plan, version, baseline, err
}

func buildRerunPlan(parent Run, graph workflow.Compiled, baseline map[string]NodeRun, selection []string) (RerunPlan, error) {
	plan := RerunPlan{ParentRunID: parent.ID, DAG: graph.Definition.Key, Revision: parent.Revision,
		GraphDigest: graph.Digest, InputDigest: inputDigest(parent.Inputs), Selected: selection}
	execute := make(map[string]bool, len(baseline))
	for _, key := range selection {
		if baseline[key].State != string(workflow.NodeFailed) {
			return RerunPlan{}, fmt.Errorf("%w: %s is not a failed node of the source", ErrRunControlConflict, key)
		}
		execute[key] = true
	}
	nodes := make(map[string]workflow.Node, len(graph.Definition.Nodes))
	for _, node := range graph.Definition.Nodes {
		nodes[node.Key] = node
	}
	states := make(map[string]workflow.Predecessor, len(nodes))
	var ready bool
	for _, key := range graph.Order {
		node := nodes[key]
		for _, edge := range node.DependsOn {
			execute[key] = execute[key] || execute[edge.Node]
		}
		source, ok := baseline[key]
		if !ok {
			return RerunPlan{}, fmt.Errorf("missing source node %s", key)
		}
		if execute[key] {
			if node.Effect == workflow.ExternalEffect && source.sourceEpoch > 0 {
				return RerunPlan{}, fmt.Errorf("%w: %s has a previously attempted non-idempotent action", ErrRunControlConflict, key)
			}
			state := workflow.ResolveDependencies(node.DependsOn, states)
			ready = ready || state == workflow.NodeReady
			plan.Nodes = append(plan.Nodes, RerunNode{Key: key, Action: "execute", State: state})
			states[key] = workflow.Predecessor{State: state}
			continue
		}
		origin := source.ReusedFrom
		if origin == nil {
			origin = &parent.ID
		}
		reused := workflow.Predecessor{State: workflow.NodeState(source.State)}
		if source.Branch != nil {
			reused.Branch = *source.Branch
		}
		states[key] = reused
		plan.Nodes = append(plan.Nodes, RerunNode{Key: key, Action: "reuse", State: reused.State, SourceRun: origin, Artifact: source.Artifact})
	}
	if !ready {
		return RerunPlan{}, fmt.Errorf("%w: the selected path has no ready nodes", ErrRunControlConflict)
	}
	return plan, nil
}

func initializeRerun(ctx context.Context, tx *sql.Tx, child int64, plan RerunPlan, baseline map[string]NodeRun, graph workflow.Compiled) error {
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return err
	}
	states := make(map[string]workflow.Predecessor, len(plan.Nodes))
	for _, node := range plan.Nodes {
		state := workflow.Predecessor{State: node.State}
		var available, finished *time.Time
		var branch, failure *string
		if node.SourceRun != nil {
			source := baseline[node.Key]
			branch, failure = source.Branch, source.FailureCode
			if branch != nil {
				state.Branch = *branch
			}
		}
		states[node.Key] = state
		if node.State.Terminal() {
			finished = &now
		} else if node.State == workflow.NodeReady {
			available = &now
		}
		if _, err := tx.ExecContext(ctx, `UPDATE node_runs SET state = ?, available_at = ?, finished_at = ?, result_branch = ?, failure_code = ?,
			reused_from_run_id = ? WHERE run_id = ? AND node_key = ?`, node.State, available, finished, branch, failure, node.SourceRun, child, node.Key); err != nil {
			return err
		}
	}
	var edges []any
	for _, node := range graph.Definition.Nodes {
		for _, edge := range node.DependsOn {
			decision := workflow.EvaluateEdge(edge, states[edge.Node])
			if decision.Decided {
				edges = append(edges, child, node.Key, edge.Node, decision.Satisfied, decision.Reason, now)
			}
		}
	}
	return insertRows(ctx, tx, `INSERT INTO edge_results (run_id, to_node, from_node, satisfied, reason, decided_at) VALUES `,
		`(?, ?, ?, ?, ?, ?)`, 6, edges)
}

const rerunColumns = `SELECT new_run_id, request_key, actor, plan, created_at FROM reruns`

func readRerun(row *sql.Row) (*RerunRecord, error) {
	var record RerunRecord
	var plan []byte
	err := row.Scan(&record.RunID, &record.Key, &record.Actor, &plan, &record.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(plan, &record.Plan); err != nil {
		return nil, err
	}
	return &record, nil
}

func readRerunTrace(ctx context.Context, tx *sql.Tx, runID int64) (*RerunRecord, []RerunChild, error) {
	record, err := readRerun(tx.QueryRowContext(ctx, rerunColumns+` WHERE new_run_id = ?`, runID))
	if err != nil {
		return nil, nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT a.new_run_id, a.request_key, r.state, a.created_at
		FROM reruns a JOIN runs r ON r.id = a.new_run_id WHERE a.parent_run_id = ? ORDER BY a.new_run_id DESC LIMIT 64`, runID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var children []RerunChild
	for rows.Next() {
		var child RerunChild
		if err := rows.Scan(&child.RunID, &child.Key, &child.State, &child.CreatedAt); err != nil {
			return nil, nil, err
		}
		children = append(children, child)
	}
	return record, children, rows.Err()
}
