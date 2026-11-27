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

	"github.com/axin2024/workflow-runtime/internal/process"
)

type ProcessMigrationSpec struct {
	Process string            `json:"process_key"`
	Source  int               `json:"source_revision"`
	Target  int               `json:"target_revision"`
	Runs    []int64           `json:"run_ids"`
	Mapping map[string]string `json:"mapping"`
}

type ProcessMigrationItem struct {
	Run        int64   `json:"run_id"`
	OldWait    *int64  `json:"old_wait_id,omitempty"`
	NewWait    *int64  `json:"new_wait_id,omitempty"`
	Transition *uint64 `json:"observed_transition,omitempty"`
	OldStep    string  `json:"source_step,omitempty"`
	NewStep    string  `json:"target_step,omitempty"`
	State      string  `json:"status"`
	Reason     string  `json:"reason,omitempty"`
}

type ProcessMigration struct {
	ID        int64                  `json:"migration_id"`
	Process   string                 `json:"process_key"`
	Source    int                    `json:"source_revision"`
	Target    int                    `json:"target_revision"`
	Mapping   map[string]string      `json:"mapping"`
	PreviewBy string                 `json:"previewed_by"`
	PreviewAt time.Time              `json:"previewed_at"`
	ApplyBy   *string                `json:"applied_by,omitempty"`
	ApplyAt   *time.Time             `json:"applied_at,omitempty"`
	Items     []ProcessMigrationItem `json:"instances"`
	Skipped   []int64                `json:"skipped_instances"`
	process   int64
	digest    []byte
}

func processMigrationSpec(input ProcessMigrationSpec) (ProcessMigrationSpec, error) {
	if !process.ValidKey(input.Process) || input.Source <= 0 || input.Target <= input.Source ||
		len(input.Runs) < 1 || len(input.Runs) > 100 || len(input.Mapping) > process.MaxSteps {
		return input, ErrInvalidProcess
	}
	input.Runs = slices.Clone(input.Runs)
	slices.Sort(input.Runs)
	for i, run := range input.Runs {
		if run <= 0 || (i > 0 && input.Runs[i-1] == run) {
			return input, ErrInvalidProcess
		}
	}
	for old, next := range input.Mapping {
		if !process.ValidKey(old) || !process.ValidKey(next) {
			return input, ErrInvalidProcess
		}
	}
	if input.Mapping == nil {
		input.Mapping = map[string]string{}
	}
	return input, nil
}

func (s *Store) PreviewProcessMigration(ctx context.Context, scope Scope, request string, spec ProcessMigrationSpec) (ProcessMigration, error) {
	var out ProcessMigration
	if scope.validate() != nil || !processReference.MatchString(request) {
		return out, ErrInvalidProcess
	}
	input, err := processMigrationSpec(spec)
	if err != nil {
		return out, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return out, err
	}
	mapping, err := json.Marshal(input.Mapping)
	if err != nil {
		return out, err
	}
	digest := sha256.Sum256(raw)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return out, err
	}
	source, err := loadProcessVersion(ctx, tx, workspace, input.Process, input.Source)
	if err != nil {
		return out, err
	}
	target, err := loadProcessVersion(ctx, tx, workspace, input.Process, input.Target)
	if err != nil {
		return out, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO process_migration_batches
		(workspace_id,process_id,source_rev,target_rev,request_key,digest,mapping,previewed_by)
		VALUES (?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(process_migration_batches.id)`,
		workspace, source.ID, source.Revision, target.Revision, request, digest[:], mapping, scope.Actor)
	if err != nil {
		return out, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return out, err
	}
	created, err := result.RowsAffected()
	if err != nil {
		return out, err
	}
	if created == 1 {
		for _, run := range input.Runs {
			item, err := previewProcessMigrationItem(ctx, tx, workspace, run, source, target, input.Mapping)
			if err != nil {
				return out, err
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO process_migration_targets
				(workspace_id,migration_id,run_id,observed_wait_id,observed_transition,source_step,target_step,status,reason)
				VALUES (?,?,?,?,?,?,?,?,?)`, workspace, id, item.Run, item.OldWait, item.Transition, item.OldStep, item.NewStep, item.State, item.Reason)
			if err != nil {
				return out, err
			}
		}
	}
	out, err = loadProcessMigration(ctx, tx, workspace, id, false)
	if err != nil {
		return out, err
	}
	if !bytes.Equal(out.digest, digest[:]) {
		return ProcessMigration{}, ErrProcessConflict
	}
	return out, tx.Commit()
}

func previewProcessMigrationItem(ctx context.Context, tx *sql.Tx, workspace, id int64, source, target ProcessVersion, mapping map[string]string) (ProcessMigrationItem, error) {
	item := ProcessMigrationItem{Run: id, State: "skipped", Reason: "unavailable"}
	run, err := loadProcessRun(ctx, tx, workspace, id, false)
	if errors.Is(err, ErrNotFound) {
		return item, nil
	}
	if err != nil {
		return item, err
	}
	if run.process != source.ID || run.Revision != source.Revision || run.Status != "waiting" {
		return item, nil
	}
	wait, err := currentProcessWait(ctx, tx, run)
	if errors.Is(err, ErrNotFound) {
		return item, nil
	}
	if err != nil {
		return item, err
	}
	if wait.Iteration != run.Transition || wait.Revision != run.Revision || wait.Step != run.Step {
		item.Reason = "state_changed"
		return item, nil
	}
	item.OldWait, item.Transition, item.OldStep = &wait.ID, &run.Transition, wait.Step
	item.NewStep = mapping[wait.Step]
	if item.NewStep == "" {
		item.Reason = "missing_mapping"
		return item, nil
	}
	old, oldExists := source.Definition.Step(wait.Step)
	next, nextExists := target.Definition.Step(item.NewStep)
	if !oldExists || !nextExists || old.Kind != next.Kind || old.SignalKind != next.SignalKind || old.Role != next.Role {
		item.Reason = "incompatible_wait"
		return item, nil
	}
	item.State, item.Reason = "ready", ""
	return item, nil
}

func (s *Store) ProcessMigration(ctx context.Context, scope Scope, id int64) (ProcessMigration, error) {
	return s.processMigration(ctx, scope, id, false)
}

func (s *Store) ApplyProcessMigration(ctx context.Context, scope Scope, id int64) (ProcessMigration, error) {
	return s.processMigration(ctx, scope, id, true)
}

func (s *Store) processMigration(ctx context.Context, scope Scope, id int64, apply bool) (ProcessMigration, error) {
	var out ProcessMigration
	if scope.validate() != nil || id <= 0 {
		return out, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted, ReadOnly: !apply})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return out, err
	}
	out, err = loadProcessMigration(ctx, tx, workspace, id, apply)
	if err != nil {
		return out, err
	}
	if !apply || out.ApplyAt != nil {
		return out, tx.Commit()
	}
	target, err := loadProcessVersion(ctx, tx, workspace, out.Process, out.Target)
	if err != nil {
		return out, err
	}
	for _, item := range out.Items {
		if item.State != "ready" {
			continue
		}
		if err = applyProcessMigrationItem(ctx, tx, workspace, id, scope.Actor, out.Source, target, item); err != nil {
			return out, err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_migration_batches SET applied_by=?,applied_at=UTC_TIMESTAMP(6) WHERE workspace_id=? AND id=?`,
		scope.Actor, workspace, id)
	if err != nil {
		return out, err
	}
	out, err = loadProcessMigration(ctx, tx, workspace, id, false)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func loadProcessMigration(ctx context.Context, tx *sql.Tx, workspace, id int64, lock bool) (ProcessMigration, error) {
	var out ProcessMigration
	query := `SELECT b.id,b.process_id,n.process_key,b.source_rev,b.target_rev,b.mapping,b.previewed_by,b.previewed_at,
		b.applied_by,b.applied_at,b.digest FROM process_migration_batches b
		JOIN process_names n ON n.workspace_id=b.workspace_id AND n.id=b.process_id WHERE b.workspace_id=? AND b.id=?`
	if lock {
		query += " FOR UPDATE OF b"
	}
	var mapping []byte
	err := tx.QueryRowContext(ctx, query, workspace, id).Scan(&out.ID, &out.process, &out.Process, &out.Source, &out.Target,
		&mapping, &out.PreviewBy, &out.PreviewAt, &out.ApplyBy, &out.ApplyAt, &out.digest)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(mapping, &out.Mapping); err != nil {
		return out, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT run_id,observed_wait_id,new_wait_id,observed_transition,source_step,target_step,status,reason
		FROM process_migration_targets WHERE workspace_id=? AND migration_id=? ORDER BY run_id`, workspace, id)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	out.Items, out.Skipped = []ProcessMigrationItem{}, []int64{}
	for rows.Next() {
		var item ProcessMigrationItem
		if err = rows.Scan(&item.Run, &item.OldWait, &item.NewWait, &item.Transition, &item.OldStep, &item.NewStep, &item.State, &item.Reason); err != nil {
			return out, err
		}
		out.Items = append(out.Items, item)
		if item.State == "skipped" {
			out.Skipped = append(out.Skipped, item.Run)
		}
	}
	return out, rows.Err()
}

func applyProcessMigrationItem(ctx context.Context, tx *sql.Tx, workspace, batch int64, actor string, source int, target ProcessVersion, item ProcessMigrationItem) error {
	run, err := loadProcessRun(ctx, tx, workspace, item.Run, true)
	if err != nil {
		return err
	}
	old, err := currentProcessWait(ctx, tx, run)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if item.OldWait == nil || item.Transition == nil || old.ID != *item.OldWait || run.Transition != *item.Transition ||
		run.Revision != source || run.process != target.ID || run.Status != "waiting" {
		_, err = tx.ExecContext(ctx, `UPDATE process_migration_targets SET status='skipped',reason='state_changed'
			WHERE workspace_id=? AND migration_id=? AND run_id=?`, workspace, batch, item.Run)
		return err
	}
	step, ok := target.Definition.Step(item.NewStep)
	if !ok {
		return ErrProcessConflict
	}
	request := fmt.Sprintf("migration:%d", batch)
	_, err = tx.ExecContext(ctx, `UPDATE process_waits SET status='migrated',winner_key=?,resolved_at=UTC_TIMESTAMP(6)
		WHERE workspace_id=? AND run_id=? AND id=?`, request, workspace, run.ID, old.ID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_tasks SET status='migrated',claim_rev=claim_rev+1 WHERE workspace_id=? AND wait_id=?`, workspace, old.ID)
	if err != nil {
		return err
	}
	run.Revision, run.Step = target.Revision, step.Key
	run.Transition++
	_, err = tx.ExecContext(ctx, `UPDATE process_runs SET definition_rev=?,step_key=?,transition_rev=? WHERE workspace_id=? AND id=?`,
		run.Revision, run.Step, run.Transition, workspace, run.ID)
	if err != nil {
		return err
	}
	newWait, err := insertProcessWait(ctx, tx, &run, target.Definition, step, &old.Deadline)
	if err != nil {
		return err
	}
	if err = processEvent(ctx, tx, run, actor, "wait_migrated", request, map[string]any{
		"migration_id": batch, "old_wait_id": old.ID, "new_wait_id": newWait, "source_revision": source,
		"target_revision": target.Revision, "source_step": old.Step, "target_step": step.Key,
	}); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_migration_targets SET status='migrated',new_wait_id=?
		WHERE workspace_id=? AND migration_id=? AND run_id=?`, newWait, workspace, batch, run.ID)
	return err
}
