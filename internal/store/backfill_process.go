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

	"github.com/go-sql-driver/mysql"
)

var ErrBackfillSelectionChanged = errors.New("backfill selection is already frozen")

// ConfirmBackfill freezes explicit candidate ordinals before registering any run.
// Repeating the same selection resumes the existing batch.
func (s *Store) ConfirmBackfill(ctx context.Context, scope Scope, id int64, selection []int) (BackfillBatch, error) {
	if err := scope.validate(); err != nil {
		return BackfillBatch{}, err
	}
	if err := s.freezeBackfill(ctx, scope, id, selection); err != nil {
		return BackfillBatch{}, err
	}
	return s.ResumeBackfill(ctx, scope, id)
}

func (s *Store) freezeBackfill(ctx context.Context, scope Scope, id int64, selection []int) error {
	ordinals := slices.Clone(selection)
	slices.Sort(ordinals)
	if len(ordinals) == 0 || len(ordinals) > maxBackfillOccurrences || ordinals[0] < 1 || ordinals[len(ordinals)-1] > maxBackfillOccurrences {
		return errors.New("select one or more valid candidate ordinals")
	}
	for i := 1; i < len(ordinals); i++ {
		if ordinals[i-1] == ordinals[i] {
			return errors.New("select each candidate at most once")
		}
	}
	encoded, err := json.Marshal(ordinals)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	batch, err := lockBackfill(ctx, tx, scope.Workspace, id)
	if err != nil {
		return err
	}
	if batch.State != "draft" {
		if !bytes.Equal(batch.selection, digest[:]) {
			return ErrBackfillSelectionChanged
		}
		return tx.Commit()
	}
	for _, ordinal := range ordinals {
		result, err := tx.ExecContext(ctx, `UPDATE backfill_items SET state = 'pending'
			WHERE batch_id = ? AND ordinal = ? AND state = 'available'`, id, ordinal)
		if err != nil {
			var serverError *mysql.MySQLError
			if errors.As(err, &serverError) && serverError.Number == 1062 {
				return fmt.Errorf("candidate %d is reserved by another backfill batch", ordinal)
			}
			return fmt.Errorf("reserve backfill candidate %d: %w", ordinal, err)
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows != 1 {
			return fmt.Errorf("candidate %d is unavailable", ordinal)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backfill_items SET state = 'excluded' WHERE batch_id = ? AND state = 'available'`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backfill_batches SET state = 'confirmed', selection_digest = ?, confirmed_at = UTC_TIMESTAMP(6)
		WHERE id = ?`, digest[:], id); err != nil {
		return err
	}
	if err := writeAudit(ctx, tx, batch.workspaceID, batch.dagID, nil, scope.Actor, "backfill_confirmed", map[string]any{
		"batch_id": id, "selected_ordinals": ordinals, "revision": batch.Revision,
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// ResumeBackfill commits one occurrence at a time, so a stopped command can
// resume from durable item receipts. Each call processes a bounded batch.
func (s *Store) ResumeBackfill(ctx context.Context, scope Scope, id int64) (BackfillBatch, error) {
	if err := scope.validate(); err != nil {
		return BackfillBatch{}, err
	}
	for range maxOccurrencesPerScan {
		more, err := s.processBackfillOccurrence(ctx, scope, id)
		if err != nil {
			return BackfillBatch{}, err
		}
		if !more {
			break
		}
	}
	return s.GetBackfill(ctx, scope, id)
}

func (s *Store) processBackfillOccurrence(ctx context.Context, scope Scope, id int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	batch, err := lockBackfill(ctx, tx, scope.Workspace, id)
	if err != nil {
		return false, err
	}
	if batch.State == "draft" {
		return false, errors.New("confirm the preview before resuming the backfill")
	}
	if batch.State == "completed" {
		return false, nil
	}
	var ordinal int
	var logical time.Time
	err = tx.QueryRowContext(ctx, `SELECT ordinal, logical_at FROM backfill_items
		WHERE batch_id = ? AND state = 'pending' ORDER BY ordinal LIMIT 1`, id).Scan(&ordinal, &logical)
	if err != nil {
		return false, fmt.Errorf("read next backfill occurrence: %w", err)
	}
	key := scheduledTriggerKey(batch.ScheduleID, logical)
	state := "reused"
	run, _, err := runByTrigger(ctx, tx, batch.workspaceID, batch.dagID, key)
	if errors.Is(err, ErrNotFound) {
		var limit int
		if err := tx.QueryRowContext(ctx, `SELECT max_active_runs FROM schedules WHERE workspace_id = ? AND dag_id = ? AND id = ?`,
			batch.workspaceID, batch.dagID, batch.ScheduleID).Scan(&limit); err != nil {
			return false, fmt.Errorf("read backfill schedule limit: %w", err)
		}
		active, err := activeScheduleRuns(ctx, tx, batch.ScheduleID)
		if err != nil {
			return false, err
		}
		if active >= limit {
			return false, nil
		}
		revision, err := loadVersion(ctx, tx, batch.dagID, batch.Revision)
		if err != nil {
			return false, err
		}
		if err := validateInputContract(batch.inputs, revision.Graph.Definition); err != nil {
			return false, fmt.Errorf("invalid frozen backfill inputs: %w", err)
		}
		request := TriggerRequest{Key: key, Kind: "backfill", LogicalAt: &logical}
		digest, err := triggerDigest(request.Kind, batch.Revision, &logical, batch.inputs)
		if err != nil {
			return false, err
		}
		run.ID, err = insertRun(ctx, tx, batch.workspaceID, batch.dagID, &batch.ScheduleID, revision, request, batch.inputs, digest, scope.Actor)
		if err != nil {
			return false, err
		}
		state = "created"
	} else if err != nil {
		return false, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE backfill_items SET state = ?, result_run_id = ?, processed_at = UTC_TIMESTAMP(6)
		WHERE batch_id = ? AND ordinal = ? AND state = 'pending'`, state, run.ID, id, ordinal); err != nil {
		return false, fmt.Errorf("write backfill receipt: %w", err)
	}
	if err := writeAudit(ctx, tx, batch.workspaceID, batch.dagID, &run.ID, scope.Actor, "backfill_item_processed", map[string]any{
		"batch_id": id, "ordinal": ordinal, "result": state, "trigger_key": key,
	}); err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE backfill_batches SET state = 'completed', completed_at = UTC_TIMESTAMP(6)
		WHERE id = ? AND NOT EXISTS (SELECT 1 FROM backfill_items WHERE batch_id = ? AND state = 'pending')`, id, id)
	if err != nil {
		return false, err
	}
	completed, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return completed == 0, tx.Commit()
}

func lockBackfill(ctx context.Context, tx *sql.Tx, workspaceKey string, id int64) (backfillRecord, error) {
	workspace, err := workspaceID(ctx, tx, workspaceKey)
	if err != nil {
		return backfillRecord{}, err
	}
	var dag int64
	err = tx.QueryRowContext(ctx, `SELECT dag_id FROM backfill_batches WHERE workspace_id = ? AND id = ?`, workspace, id).Scan(&dag)
	if errors.Is(err, sql.ErrNoRows) {
		return backfillRecord{}, ErrNotFound
	}
	if err != nil {
		return backfillRecord{}, err
	}
	// Use the same lock order as schedule scans and configuration writes.
	if err := tx.QueryRowContext(ctx, `SELECT id FROM dags WHERE workspace_id = ? AND id = ? FOR UPDATE`, workspace, dag).Scan(&dag); err != nil {
		return backfillRecord{}, err
	}
	return readBackfill(ctx, tx, workspace, id, true)
}

func reservedOccurrence(ctx context.Context, tx *sql.Tx, scheduleID int64, logical time.Time) (bool, error) {
	var reserved bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM backfill_items WHERE schedule_id = ? AND reserved_at = ?)`,
		scheduleID, logical).Scan(&reserved)
	if err != nil {
		return false, fmt.Errorf("find backfill reservation: %w", err)
	}
	return reserved, nil
}
