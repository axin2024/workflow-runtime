package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/axin2024/workflow-runtime/internal/schedule"
)

type ScheduleSpec struct {
	Key           string          `json:"key"`
	Cron          string          `json:"cron"`
	Timezone      string          `json:"timezone"`
	Misfire       string          `json:"misfire"`
	GraceSeconds  int             `json:"grace_seconds"`
	MaxActiveRuns int             `json:"max_active_runs"`
	Revision      uint64          `json:"revision,omitempty"`
	Inputs        json.RawMessage `json:"inputs"`
	Enabled       bool            `json:"enabled"`
	StartAfter    time.Time       `json:"start_after"`
}

type Schedule struct {
	ScheduleSpec
	ID         int64      `json:"id"`
	Generation uint64     `json:"generation"`
	NextAt     *time.Time `json:"next_at,omitempty"`
	ScanAfter  *time.Time `json:"scan_after,omitempty"`
	LastError  *string    `json:"last_error,omitempty"`
}

type ScheduleConflict struct{ Generation uint64 }

func (c *ScheduleConflict) Error() string {
	return fmt.Sprintf("schedule generation is %d", c.Generation)
}

// PutSchedule creates or replaces a complete configuration. Input and quota
// edits preserve the cursor; changing the calendar or start resets it.
func (s *Store) PutSchedule(ctx context.Context, scope Scope, dagKey string, expected uint64, spec ScheduleSpec) (Schedule, error) {
	if err := scope.validate(); err != nil {
		return Schedule{}, err
	}
	if !identifier.MatchString(dagKey) || !identifier.MatchString(spec.Key) {
		return Schedule{}, errors.New("invalid workflow or schedule key")
	}
	if spec.Misfire != "catch_up" && spec.Misfire != "skip" {
		return Schedule{}, errors.New("misfire must be catch_up or skip")
	}
	if spec.GraceSeconds < 0 || spec.GraceSeconds > 86400 || spec.MaxActiveRuns < 1 || spec.MaxActiveRuns > 10000 {
		return Schedule{}, errors.New("grace_seconds must be 0 to 86400 and max_active_runs must be 1 to 10000")
	}
	spec.StartAfter = spec.StartAfter.UTC().Truncate(time.Microsecond)
	if spec.StartAfter.Year() < 1000 || spec.StartAfter.Year() > 9999 {
		return Schedule{}, errors.New("start_after must be a valid RFC3339 time in the supported range")
	}
	calendar, err := schedule.Parse(spec.Cron, spec.Timezone)
	if err != nil {
		return Schedule{}, err
	}
	spec.Cron = calendar.Expression()
	spec.Inputs, err = normalizedInputs(spec.Inputs)
	if err != nil {
		return Schedule{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Schedule{}, err
	}
	defer tx.Rollback()
	workspaceID, dagID, active, err := scheduleDAG(ctx, tx, scope.Workspace, dagKey, true)
	if err != nil {
		return Schedule{}, err
	}
	previous, err := scheduleByKey(ctx, tx, workspaceID, dagID, spec.Key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Schedule{}, err
	}
	if previous.Generation != expected {
		return Schedule{}, &ScheduleConflict{Generation: previous.Generation}
	}
	var pinned *uint64
	revision := active
	if spec.Revision != 0 {
		revision, pinned = spec.Revision, &spec.Revision
	}
	version, err := loadVersion(ctx, tx, dagID, revision)
	if err != nil {
		return Schedule{}, err
	}
	if spec.Enabled {
		if err := validateInputContract(spec.Inputs, version.Graph.Definition); err != nil {
			return Schedule{}, err
		}
	}
	next := previous.NextAt
	if previous.Generation == 0 || previous.Cron != spec.Cron || previous.Timezone != spec.Timezone || !previous.StartAfter.Equal(spec.StartAfter) {
		instant, ok := calendar.Next(spec.StartAfter)
		if !ok {
			return Schedule{}, errors.New("cron has no upcoming occurrence in the supported calendar")
		}
		next = &instant
	}
	generation := expected + 1
	if expected == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO schedules (workspace_id, dag_id, schedule_key, generation,
			cron_expression, timezone, misfire, grace_seconds, max_active_runs, pinned_revision,
			input_values, enabled, start_after, next_at, scan_after) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			workspaceID, dagID, spec.Key, generation, spec.Cron, spec.Timezone, spec.Misfire, spec.GraceSeconds,
			spec.MaxActiveRuns, pinned, string(spec.Inputs), spec.Enabled, spec.StartAfter, next, next)
	} else {
		_, err = tx.ExecContext(ctx, `UPDATE schedules SET generation = ?, cron_expression = ?, timezone = ?, misfire = ?,
			grace_seconds = ?, max_active_runs = ?, pinned_revision = ?, input_values = ?, enabled = ?, start_after = ?,
			next_at = ?, scan_after = ?, last_error = NULL WHERE id = ?`,
			generation, spec.Cron, spec.Timezone, spec.Misfire, spec.GraceSeconds, spec.MaxActiveRuns,
			pinned, string(spec.Inputs), spec.Enabled, spec.StartAfter, next, next, previous.ID)
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("save schedule: %w", err)
	}
	current, err := scheduleByKey(ctx, tx, workspaceID, dagID, spec.Key)
	if err != nil {
		return Schedule{}, err
	}
	if err := writeAudit(ctx, tx, workspaceID, dagID, nil, scope.Actor, "schedule_saved", map[string]any{
		"schedule_id": current.ID, "generation": generation, "enabled": spec.Enabled, "revision": revision,
	}); err != nil {
		return Schedule{}, err
	}
	return current, tx.Commit()
}

func (s *Store) GetSchedule(ctx context.Context, scope Scope, dagKey, key string) (Schedule, error) {
	if err := scope.validate(); err != nil {
		return Schedule{}, err
	}
	if !identifier.MatchString(dagKey) || !identifier.MatchString(key) {
		return Schedule{}, errors.New("invalid workflow or schedule key")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return Schedule{}, err
	}
	defer tx.Rollback()
	workspaceID, dagID, _, err := scheduleDAG(ctx, tx, scope.Workspace, dagKey, false)
	if err != nil {
		return Schedule{}, err
	}
	result, err := scheduleByKey(ctx, tx, workspaceID, dagID, key)
	if err != nil {
		return Schedule{}, err
	}
	return result, tx.Commit()
}

func scheduleDAG(ctx context.Context, tx *sql.Tx, workspace, key string, lock bool) (int64, int64, uint64, error) {
	workspaceID, err := workspaceID(ctx, tx, workspace)
	if err != nil {
		return 0, 0, 0, err
	}
	dagID, active, err := findDAG(ctx, tx, workspaceID, key, lock)
	return workspaceID, dagID, active, err
}

const scheduleColumns = `id, generation, schedule_key, cron_expression, timezone, misfire, grace_seconds,
	max_active_runs, pinned_revision, input_values, enabled, start_after, next_at, scan_after, last_error`

func scheduleByKey(ctx context.Context, tx *sql.Tx, workspace, dag int64, key string) (Schedule, error) {
	return readSchedule(tx.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE workspace_id = ? AND dag_id = ? AND schedule_key = ?`, workspace, dag, key))
}

func readSchedule(row *sql.Row) (Schedule, error) {
	var value Schedule
	var revision *uint64
	var inputs []byte
	err := row.Scan(&value.ID, &value.Generation, &value.Key, &value.Cron, &value.Timezone, &value.Misfire,
		&value.GraceSeconds, &value.MaxActiveRuns, &revision, &inputs, &value.Enabled, &value.StartAfter,
		&value.NextAt, &value.ScanAfter, &value.LastError)
	if errors.Is(err, sql.ErrNoRows) {
		return Schedule{}, ErrNotFound
	}
	if err != nil {
		return Schedule{}, fmt.Errorf("read schedule: %w", err)
	}
	if revision != nil {
		value.Revision = *revision
	}
	value.Inputs = inputs
	return value, nil
}
