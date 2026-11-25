package store

import (
	"context"
	"database/sql"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type DependencyResult struct {
	workflow.Edge
	Status    string     `json:"status"`
	Reason    string     `json:"reason"`
	DecidedAt *time.Time `json:"decided_at,omitempty"`
}

type AttemptResult struct {
	Epoch          uint64     `json:"epoch"`
	Worker         string     `json:"worker"`
	State          string     `json:"state"`
	QueuedAt       time.Time  `json:"queued_at"`
	StartedAt      time.Time  `json:"started_at"`
	LeaseUntil     time.Time  `json:"lease_until"`
	Deadline       time.Time  `json:"deadline"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	FailureCode    *string    `json:"failure_code,omitempty"`
	FailureMessage *string    `json:"failure_message,omitempty"`
	ResultDigest   *string    `json:"result_digest,omitempty"`
}

func attachExecutionDetails(ctx context.Context, tx *sql.Tx, runID int64, graph workflow.Definition, nodes []NodeRun) error {
	byKey := make(map[string]*NodeRun, len(nodes))
	for i := range nodes {
		byKey[nodes[i].Key] = &nodes[i]
	}
	decisions := make(map[[2]string]DependencyResult)
	rows, err := tx.QueryContext(ctx, `SELECT to_node, from_node, satisfied, reason, decided_at FROM edge_results WHERE run_id = ?`, runID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key [2]string
		var decision DependencyResult
		var satisfied bool
		if err := rows.Scan(&key[0], &key[1], &satisfied, &decision.Reason, &decision.DecidedAt); err != nil {
			return err
		}
		decision.Status = "unmet"
		if satisfied {
			decision.Status = "satisfied"
		}
		decisions[key] = decision
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, node := range graph.Nodes {
		for _, edge := range node.DependsOn {
			decision, found := decisions[[2]string{node.Key, edge.Node}]
			if !found {
				decision.Status, decision.Reason = "waiting", "upstream_unfinished"
			}
			decision.Edge = edge
			byKey[node.Key].Dependencies = append(byKey[node.Key].Dependencies, decision)
		}
	}
	attempts, err := tx.QueryContext(ctx, `SELECT a.node_key, a.ordinal, w.worker_key, a.state, a.queued_at, a.started_at,
		a.lease_until, a.deadline_at, a.finished_at, a.failure_code, a.failure_message, LOWER(HEX(a.result_digest))
		FROM attempts a JOIN workers w ON w.id = a.worker_id WHERE a.run_id = ? ORDER BY a.node_key, a.ordinal`, runID)
	if err != nil {
		return err
	}
	defer attempts.Close()
	for attempts.Next() {
		var key string
		var value AttemptResult
		if err := attempts.Scan(&key, &value.Epoch, &value.Worker, &value.State, &value.QueuedAt, &value.StartedAt, &value.LeaseUntil,
			&value.Deadline, &value.FinishedAt, &value.FailureCode, &value.FailureMessage, &value.ResultDigest); err != nil {
			return err
		}
		byKey[key].Attempts = append(byKey[key].Attempts, value)
	}
	if err := attempts.Err(); err != nil {
		return err
	}
	resolutions, err := tx.QueryContext(ctx, resolutionsQuery, runID)
	if err != nil {
		return err
	}
	defer resolutions.Close()
	for resolutions.Next() {
		record, _, err := readResolution(resolutions, runID)
		if err != nil {
			return err
		}
		byKey[record.Node].Resolution = &record
	}
	return resolutions.Err()
}
