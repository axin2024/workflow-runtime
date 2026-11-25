package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/schedule"
	"github.com/axin2024/workflow-runtime/internal/workflow"
)

const maxBackfillOccurrences = 128

type BackfillSpec struct {
	Schedule string
	Revision uint64
	From     time.Time
	Through  time.Time
	Inputs   json.RawMessage
}

type BackfillBatch struct {
	ID                 int64          `json:"id"`
	Workspace          string         `json:"workspace"`
	DAG                string         `json:"dag"`
	Schedule           string         `json:"schedule"`
	ScheduleID         int64          `json:"schedule_id"`
	ScheduleGeneration uint64         `json:"schedule_generation"`
	Revision           uint64         `json:"revision"`
	GraphDigest        string         `json:"graph_digest"`
	InputDigest        string         `json:"input_digest"`
	Timezone           string         `json:"timezone"`
	DSTPolicy          string         `json:"dst_policy"`
	From               time.Time      `json:"from"`
	Through            time.Time      `json:"through"`
	State              string         `json:"state"`
	CreatedBy          string         `json:"created_by"`
	CreatedAt          time.Time      `json:"created_at"`
	ConfirmedAt        *time.Time     `json:"confirmed_at,omitempty"`
	CompletedAt        *time.Time     `json:"completed_at,omitempty"`
	Items              []BackfillItem `json:"items"`
}

type BackfillItem struct {
	Ordinal          int              `json:"ordinal"`
	LogicalAt        time.Time        `json:"logical_at"`
	LocalAt          string           `json:"local_at"`
	TriggerKey       string           `json:"trigger_key"`
	State            string           `json:"state"`
	BaselineRunID    *int64           `json:"baseline_run_id,omitempty"`
	BaselineRevision *uint64          `json:"baseline_revision,omitempty"`
	RunID            *int64           `json:"run_id,omitempty"`
	RunRevision      *uint64          `json:"run_revision,omitempty"`
	ProcessedAt      *time.Time       `json:"processed_at,omitempty"`
	Impact           []BackfillImpact `json:"impact,omitempty"`
}

type BackfillImpact struct {
	Node         string          `json:"node"`
	Task         string          `json:"task"`
	Effect       workflow.Effect `json:"effect"`
	OperationKey string          `json:"operation_key"`
}

type backfillRecord struct {
	BackfillBatch
	workspaceID, dagID int64
	inputs             json.RawMessage
	selection          []byte
}

// PreviewBackfill snapshots the complete candidate window before selection.
func (s *Store) PreviewBackfill(ctx context.Context, scope Scope, dagKey string, spec BackfillSpec) (BackfillBatch, error) {
	if err := scope.validate(); err != nil {
		return BackfillBatch{}, err
	}
	if !identifier.MatchString(dagKey) || !identifier.MatchString(spec.Schedule) || spec.Revision == 0 {
		return BackfillBatch{}, errors.New("backfill requires a workflow, schedule and explicit revision")
	}
	spec.From = spec.From.UTC().Truncate(time.Microsecond)
	spec.Through = spec.Through.UTC().Truncate(time.Microsecond)
	if spec.From.Year() < 1000 || spec.Through.Year() > 9999 || spec.Through.Before(spec.From) || spec.Through.Sub(spec.From) > 31*24*time.Hour {
		return BackfillBatch{}, errors.New("backfill requires an ordered time window of at most 31 days")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return BackfillBatch{}, err
	}
	defer tx.Rollback()
	workspace, dag, _, err := scheduleDAG(ctx, tx, scope.Workspace, dagKey, true)
	if err != nil {
		return BackfillBatch{}, err
	}
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(6)`).Scan(&now); err != nil {
		return BackfillBatch{}, err
	}
	if spec.Through.After(now) {
		return BackfillBatch{}, errors.New("backfill window must end at or before the current database time")
	}
	config, err := scheduleByKey(ctx, tx, workspace, dag, spec.Schedule)
	if err != nil {
		return BackfillBatch{}, err
	}
	version, err := loadVersion(ctx, tx, dag, spec.Revision)
	if err != nil {
		return BackfillBatch{}, err
	}
	inputs := config.Inputs
	if len(spec.Inputs) > 0 {
		inputs, err = normalizedInputs(spec.Inputs)
		if err != nil {
			return BackfillBatch{}, err
		}
	}
	if err := validateInputContract(inputs, version.Graph.Definition); err != nil {
		return BackfillBatch{}, err
	}
	calendar, err := schedule.Parse(config.Cron, config.Timezone)
	if err != nil {
		return BackfillBatch{}, err
	}
	instants, err := backfillInstants(calendar, config.StartAfter, spec.From, spec.Through)
	if err != nil {
		return BackfillBatch{}, err
	}
	baseline, err := baselineRuns(ctx, tx, config.ID, instants)
	if err != nil {
		return BackfillBatch{}, err
	}
	digest := sha256.Sum256(inputs)
	result, err := tx.ExecContext(ctx, `INSERT INTO backfill_batches (workspace_id, dag_id, schedule_id, schedule_generation,
		revision, timezone, from_at, through_at, input_values, input_digest, created_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		workspace, dag, config.ID, config.Generation, spec.Revision, config.Timezone, spec.From, spec.Through, string(inputs), digest[:], scope.Actor)
	if err != nil {
		return BackfillBatch{}, fmt.Errorf("create backfill preview: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return BackfillBatch{}, err
	}
	values := make([]any, 0, len(instants)*9)
	for index, instant := range instants {
		state := "available"
		var baselineID *int64
		if previous, found := baseline[instant.UnixMicro()]; found {
			state, baselineID = "already_registered", &previous
		}
		values = append(values, id, index+1, workspace, dag, config.ID, instant, calendar.Local(instant).Format(time.RFC3339Nano), state, baselineID)
	}
	if err := insertRows(ctx, tx, `INSERT INTO backfill_items (batch_id, ordinal, workspace_id, dag_id, schedule_id,
		logical_at, local_at, state, baseline_run_id) VALUES `, `(?, ?, ?, ?, ?, ?, ?, ?, ?)`, 9, values); err != nil {
		return BackfillBatch{}, fmt.Errorf("save backfill candidates: %w", err)
	}
	if err := writeAudit(ctx, tx, workspace, dag, nil, scope.Actor, "backfill_previewed", map[string]any{
		"batch_id": id, "schedule_id": config.ID, "candidates": len(instants), "revision": spec.Revision,
	}); err != nil {
		return BackfillBatch{}, err
	}
	if err := tx.Commit(); err != nil {
		return BackfillBatch{}, err
	}
	return s.GetBackfill(ctx, scope, id)
}

func backfillInstants(calendar schedule.Calendar, start, from, through time.Time) ([]time.Time, error) {
	after := from.Add(-time.Nanosecond)
	if start.After(after) {
		after = start
	}
	var instants []time.Time
	for instant, ok := calendar.Next(after); ok && !instant.After(through); instant, ok = calendar.Next(instant) {
		if len(instants) == maxBackfillOccurrences {
			return nil, fmt.Errorf("backfill preview exceeds %d occurrences; use a shorter window", maxBackfillOccurrences)
		}
		instants = append(instants, instant)
	}
	return instants, nil
}

func baselineRuns(ctx context.Context, tx *sql.Tx, scheduleID int64, instants []time.Time) (map[int64]int64, error) {
	if len(instants) == 0 {
		return nil, nil
	}
	values := make([]any, 1, len(instants)+1)
	values[0] = scheduleID
	for _, instant := range instants {
		values = append(values, instant)
	}
	placeholders := strings.Repeat("?,", len(instants)-1) + "?"
	rows, err := tx.QueryContext(ctx, `SELECT id, logical_at FROM runs WHERE schedule_id = ? AND logical_at IN (`+placeholders+`)`, values...)
	if err != nil {
		return nil, fmt.Errorf("list baseline runs: %w", err)
	}
	defer rows.Close()
	baseline := make(map[int64]int64)
	for rows.Next() {
		var id int64
		var instant time.Time
		if err := rows.Scan(&id, &instant); err != nil {
			return nil, err
		}
		baseline[instant.UnixMicro()] = id
	}
	return baseline, rows.Err()
}

func (s *Store) GetBackfill(ctx context.Context, scope Scope, id int64) (BackfillBatch, error) {
	if err := scope.validate(); err != nil {
		return BackfillBatch{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return BackfillBatch{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return BackfillBatch{}, err
	}
	record, err := readBackfill(ctx, tx, workspace, id, false)
	if err != nil {
		return BackfillBatch{}, err
	}
	version, err := loadVersion(ctx, tx, record.dagID, record.Revision)
	if err != nil {
		return BackfillBatch{}, err
	}
	if err := tx.QueryRowContext(ctx, `SELECT schedule_key FROM schedules WHERE workspace_id = ? AND dag_id = ? AND id = ?`,
		workspace, record.dagID, record.ScheduleID).Scan(&record.Schedule); err != nil {
		return BackfillBatch{}, err
	}
	record.Items, err = readBackfillItems(ctx, tx, record, version.Graph)
	if err != nil {
		return BackfillBatch{}, err
	}
	record.Workspace, record.DAG = scope.Workspace, version.Graph.Definition.Key
	record.GraphDigest = version.Graph.Digest
	record.DSTPolicy = "skip_nonexistent_preserve_repeated_instants"
	return record.BackfillBatch, tx.Commit()
}

func readBackfill(ctx context.Context, tx *sql.Tx, workspace, id int64, lock bool) (backfillRecord, error) {
	query := `SELECT id, workspace_id, dag_id, schedule_id, schedule_generation, revision, timezone, from_at, through_at,
		input_values, input_digest, selection_digest, state, created_by, created_at, confirmed_at, completed_at
		FROM backfill_batches WHERE workspace_id = ? AND id = ?`
	if lock {
		query += ` FOR UPDATE`
	}
	var record backfillRecord
	var inputs, digest []byte
	err := tx.QueryRowContext(ctx, query, workspace, id).Scan(&record.ID, &record.workspaceID, &record.dagID, &record.ScheduleID,
		&record.ScheduleGeneration, &record.Revision, &record.Timezone, &record.From, &record.Through,
		&inputs, &digest, &record.selection, &record.State, &record.CreatedBy, &record.CreatedAt, &record.ConfirmedAt, &record.CompletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return backfillRecord{}, ErrNotFound
	}
	if err != nil {
		return backfillRecord{}, fmt.Errorf("read backfill: %w", err)
	}
	record.inputs, record.InputDigest = inputs, hex.EncodeToString(digest)
	return record, nil
}

func readBackfillItems(ctx context.Context, tx *sql.Tx, record backfillRecord, graph workflow.Compiled) ([]BackfillItem, error) {
	rows, err := tx.QueryContext(ctx, `SELECT i.ordinal, i.logical_at, i.local_at, i.state, i.baseline_run_id, b.revision,
		i.result_run_id, r.revision, i.processed_at FROM backfill_items i
		LEFT JOIN runs b ON b.id = i.baseline_run_id LEFT JOIN runs r ON r.id = i.result_run_id
		WHERE i.batch_id = ? ORDER BY i.ordinal`, record.ID)
	if err != nil {
		return nil, fmt.Errorf("list backfill items: %w", err)
	}
	defer rows.Close()
	items := make([]BackfillItem, 0)
	nodes := make(map[string]workflow.Node, len(graph.Definition.Nodes))
	for _, node := range graph.Definition.Nodes {
		nodes[node.Key] = node
	}
	for rows.Next() {
		var item BackfillItem
		if err := rows.Scan(&item.Ordinal, &item.LogicalAt, &item.LocalAt, &item.State, &item.BaselineRunID, &item.BaselineRevision,
			&item.RunID, &item.RunRevision, &item.ProcessedAt); err != nil {
			return nil, err
		}
		item.TriggerKey = scheduledTriggerKey(record.ScheduleID, item.LogicalAt)
		if item.State != "already_registered" && item.State != "excluded" {
			item.Impact = make([]BackfillImpact, 0, len(graph.Order))
			for _, key := range graph.Order {
				node := nodes[key]
				item.Impact = append(item.Impact, BackfillImpact{Node: key, Task: node.Task, Effect: node.Effect,
					OperationKey: operationKey(record.workspaceID, record.dagID, item.TriggerKey, key)})
			}
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
