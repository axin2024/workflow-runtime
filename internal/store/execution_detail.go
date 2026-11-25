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
	Pool           *string    `json:"pool,omitempty"`
	State          string     `json:"state"`
	QueuedAt       time.Time  `json:"queued_at"`
	StartedAt      time.Time  `json:"started_at"`
	LeaseUntil     time.Time  `json:"lease_until"`
	Deadline       time.Time  `json:"deadline"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	FailureCode    *string    `json:"failure_code,omitempty"`
	FailureMessage *string    `json:"failure_message,omitempty"`
	ResultDigest   *string    `json:"result_digest,omitempty"`
	LogExcerpt     *string    `json:"log_excerpt,omitempty"`
	LogSHA256      *string    `json:"log_sha256,omitempty"`
	QueueWaitMS    int64      `json:"queue_wait_ms"`
	ExecutionMS    *int64     `json:"execution_ms,omitempty"`
}

func attachExecutionDetails(ctx context.Context, tx *sql.Tx, run Run, graph workflow.Definition, nodes []NodeRun) error {
	runID := run.ID
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
		decided := true
		readyAt := run.CreatedAt
		for _, edge := range node.DependsOn {
			decision, found := decisions[[2]string{node.Key, edge.Node}]
			if !found {
				decision.Status, decision.Reason = "waiting", "upstream_unfinished"
				decided = false
			} else if decision.DecidedAt.After(readyAt) {
				readyAt = *decision.DecidedAt
			}
			decision.Edge = edge
			byKey[node.Key].Dependencies = append(byKey[node.Key].Dependencies, decision)
		}
		if decided && byKey[node.Key].ReusedFrom == nil {
			wait := readyAt.Sub(run.CreatedAt).Milliseconds()
			byKey[node.Key].DependencyWaitMS = &wait
		}
	}
	attempts, err := tx.QueryContext(ctx, `SELECT a.node_key, a.ordinal, w.worker_key, p.pool_key, a.state, a.queued_at, a.started_at,
		a.lease_until, a.deadline_at, a.finished_at, a.failure_code, a.failure_message, LOWER(HEX(a.result_digest)),
		a.log_excerpt, LOWER(HEX(a.log_digest))
		FROM attempts a JOIN workers w ON w.id = a.worker_id LEFT JOIN execution_pools p ON p.id = a.pool_id
		WHERE a.run_id = ? ORDER BY a.node_key, a.ordinal`, runID)
	if err != nil {
		return err
	}
	defer attempts.Close()
	for attempts.Next() {
		var key string
		var value AttemptResult
		if err := attempts.Scan(&key, &value.Epoch, &value.Worker, &value.Pool, &value.State, &value.QueuedAt, &value.StartedAt, &value.LeaseUntil,
			&value.Deadline, &value.FinishedAt, &value.FailureCode, &value.FailureMessage, &value.ResultDigest,
			&value.LogExcerpt, &value.LogSHA256); err != nil {
			return err
		}
		value.QueueWaitMS = max(0, value.StartedAt.Sub(value.QueuedAt).Milliseconds())
		if value.FinishedAt != nil {
			duration := max(0, value.FinishedAt.Sub(value.StartedAt).Milliseconds())
			value.ExecutionMS = &duration
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

type RunReport struct {
	Format      string    `json:"format"`
	GeneratedAt time.Time `json:"generated_at"`
	GeneratedBy string    `json:"generated_by"`
	Detail      RunDetail `json:"detail"`
}

func (s *Store) ExportRun(ctx context.Context, scope Scope, id int64) (RunReport, error) {
	detail, err := s.GetRun(ctx, scope, id)
	if err != nil {
		return RunReport{}, err
	}
	return RunReport{Format: "workflow-runtime/run-report-v1", GeneratedAt: time.Now().UTC(), GeneratedBy: scope.Actor, Detail: detail}, nil
}
