package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/axin2024/workflow-runtime/internal/schedule"
	"github.com/axin2024/workflow-runtime/internal/workflow"
)

const maxOccurrencesPerScan = 32

type ScheduleScan struct {
	At       time.Time         `json:"at"`
	Outcomes []ScheduleOutcome `json:"outcomes"`
}

type ScheduleOutcome struct {
	ScheduleID    int64      `json:"schedule_id"`
	RunIDs        []int64    `json:"run_ids,omitempty"`
	NextAt        *time.Time `json:"next_at,omitempty"`
	SkippedFrom   *time.Time `json:"skipped_from,omitempty"`
	SkippedBefore *time.Time `json:"skipped_before,omitempty"`
	Status        string     `json:"status"`
}

type dueSchedule struct {
	id, workspace, dag int64
}

// ScanDue processes bounded batches. With a zero time it uses the shared
// MySQL clock. Each schedule's run creation and cursor advancement are atomic.
func (s *Store) ScanDue(ctx context.Context, actor string, now time.Time, limit int) (ScheduleScan, error) {
	if !identifier.MatchString(actor) {
		return ScheduleScan{}, errors.New("invalid scheduler actor")
	}
	if limit < 1 || limit > 256 {
		return ScheduleScan{}, errors.New("scan limit must be 1 to 256")
	}
	if now.IsZero() {
		if err := s.db.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(6)`).Scan(&now); err != nil {
			return ScheduleScan{}, fmt.Errorf("read database clock: %w", err)
		}
	}
	now = now.UTC().Truncate(time.Microsecond)
	if now.Year() < 1000 || now.Year() > 9998 {
		return ScheduleScan{}, errors.New("scan time is outside the supported range")
	}
	result := ScheduleScan{At: now, Outcomes: make([]ScheduleOutcome, 0)}
	due, err := s.listDue(ctx, now, limit)
	if err != nil {
		return result, err
	}
	var failures []error
	for _, item := range due {
		outcome, err := s.runDueSchedule(ctx, actor, now, item)
		if err != nil {
			failures = append(failures, fmt.Errorf("scan schedule %d: %w", item.id, err))
			result.Outcomes = append(result.Outcomes, ScheduleOutcome{ScheduleID: item.id, Status: "error"})
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if outcome.Status != "unchanged" {
			result.Outcomes = append(result.Outcomes, outcome)
		}
	}
	return result, errors.Join(failures...)
}

func (s *Store) listDue(ctx context.Context, now time.Time, limit int) ([]dueSchedule, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, workspace_id, dag_id FROM schedules
		WHERE enabled = TRUE AND scan_after <= ? ORDER BY scan_after, id LIMIT ?`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list due schedules: %w", err)
	}
	defer rows.Close()
	var due []dueSchedule
	for rows.Next() {
		var item dueSchedule
		if err := rows.Scan(&item.id, &item.workspace, &item.dag); err != nil {
			return nil, err
		}
		due = append(due, item)
	}
	return due, rows.Err()
}

func (s *Store) runDueSchedule(ctx context.Context, actor string, now time.Time, item dueSchedule) (ScheduleOutcome, error) {
	result := ScheduleOutcome{ScheduleID: item.id, Status: "unchanged"}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return result, err
	}
	defer tx.Rollback()
	var active uint64
	// All schedule writers lock the DAG first. The index scan above holds no locks.
	if err := tx.QueryRowContext(ctx, `SELECT active_revision FROM dags WHERE id = ? AND workspace_id = ? FOR UPDATE`,
		item.dag, item.workspace).Scan(&active); err != nil {
		return result, err
	}
	config, err := readSchedule(tx.QueryRowContext(ctx, `SELECT `+scheduleColumns+` FROM schedules
		WHERE workspace_id = ? AND dag_id = ? AND id = ? FOR UPDATE`, item.workspace, item.dag, item.id))
	if err != nil {
		return result, err
	}
	if !config.Enabled || config.NextAt == nil || config.ScanAfter == nil || config.ScanAfter.After(now) || config.NextAt.After(now) {
		return result, nil
	}
	calendar, err := schedule.Parse(config.Cron, config.Timezone)
	if err != nil {
		return result, err
	}
	next := config.NextAt
	if config.Misfire == "skip" {
		cutoff := now.Add(-time.Duration(config.GraceSeconds) * time.Second)
		if next.Before(cutoff) {
			if err := writeAudit(ctx, tx, item.workspace, item.dag, nil, actor, "schedule_misfire_skipped", map[string]any{
				"schedule_id": item.id, "from": next, "before": cutoff,
			}); err != nil {
				return result, err
			}
			result.SkippedFrom, result.SkippedBefore = next, &cutoff
			next = nextOccurrence(calendar, cutoff.Add(-time.Nanosecond))
			result.Status = "skipped_window"
		}
	}
	if next == nil || next.After(now) {
		result.NextAt = next
		return result, advanceSchedule(ctx, tx, item.id, next, next, nil)
	}
	if config.Revision != 0 {
		active = config.Revision
	}
	version, err := loadVersion(ctx, tx, item.dag, active)
	if err != nil {
		return result, err
	}
	if err := validateInputContract(config.Inputs, version.Graph.Definition); err != nil {
		var issue *workflow.Issue
		if !errors.As(err, &issue) {
			return result, err
		}
		const invalid = "input_contract_changed"
		if config.LastError == nil || *config.LastError != invalid {
			if err := writeAudit(ctx, tx, item.workspace, item.dag, nil, actor, "schedule_input_rejected", map[string]any{
				"schedule_id": item.id, "revision": active, "path": issue.Path, "reason": issue.Message,
			}); err != nil {
				return result, err
			}
		}
		result.NextAt, result.Status = next, invalid
		retryAt := now.Add(30 * time.Second)
		return result, advanceSchedule(ctx, tx, item.id, next, &retryAt, invalid)
	}
	var activeRuns int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs
		WHERE schedule_id = ? AND state NOT IN ('succeeded', 'failed', 'canceled')`, item.id).Scan(&activeRuns); err != nil {
		return result, fmt.Errorf("count active scheduled runs: %w", err)
	}
	result.Status = "advanced"
	var scanAfter *time.Time
	for range maxOccurrencesPerScan {
		if next == nil || next.After(now) {
			break
		}
		key := scheduledTriggerKey(item.id, *next)
		_, _, err := runByTrigger(ctx, tx, item.workspace, item.dag, key)
		if errors.Is(err, ErrNotFound) {
			if activeRuns >= config.MaxActiveRuns {
				result.Status = "active_limit"
				retryAt := now.Add(5 * time.Second)
				scanAfter = &retryAt
				break
			}
			request := TriggerRequest{Key: key, Kind: "scheduled", LogicalAt: next}
			digest, err := triggerDigest(request.Kind, version.Revision, next, config.Inputs)
			if err != nil {
				return result, err
			}
			id, err := insertRun(ctx, tx, item.workspace, item.dag, &item.id, version, request, config.Inputs, digest, actor)
			if err != nil {
				return result, err
			}
			result.RunIDs = append(result.RunIDs, id)
			activeRuns++
		} else if err != nil {
			return result, err
		}
		next = nextOccurrence(calendar, *next)
	}
	if scanAfter == nil {
		scanAfter = next
	}
	result.NextAt = next
	return result, advanceSchedule(ctx, tx, item.id, next, scanAfter, nil)
}

func advanceSchedule(ctx context.Context, tx *sql.Tx, id int64, next, scanAfter *time.Time, status any) error {
	if _, err := tx.ExecContext(ctx, `UPDATE schedules SET next_at = ?, scan_after = ?, last_error = ? WHERE id = ?`,
		next, scanAfter, status, id); err != nil {
		return fmt.Errorf("advance schedule: %w", err)
	}
	return tx.Commit()
}

func nextOccurrence(calendar schedule.Calendar, after time.Time) *time.Time {
	if next, ok := calendar.Next(after); ok {
		return &next
	}
	return nil
}

func scheduledTriggerKey(scheduleID int64, at time.Time) string {
	return "@schedule/" + strconv.FormatInt(scheduleID, 10) + "/" + strconv.FormatInt(at.Unix(), 10)
}
