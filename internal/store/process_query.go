package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

type ProcessSummary struct {
	Key      string `json:"process_key"`
	Revision int    `json:"definition_rev"`
}

type ProcessEvent struct {
	ID       int64           `json:"event_id"`
	Revision int             `json:"definition_rev"`
	Actor    string          `json:"actor"`
	Kind     string          `json:"kind"`
	Request  *string         `json:"request_key,omitempty"`
	Details  json.RawMessage `json:"details"`
	At       time.Time       `json:"occurred_at"`
}

type ProcessRunDetail struct {
	Run          ProcessRun           `json:"run"`
	Definition   process.Definition   `json:"definition"`
	Waits        []ProcessWait        `json:"waits"`
	Tasks        []ProcessTask        `json:"tasks"`
	Children     []ProcessChild       `json:"children"`
	Actions      []ProcessAction      `json:"actions"`
	Events       []ProcessEvent       `json:"events"`
	Signals      []SignalReceipt      `json:"signals"`
	Cancellation *ProcessCancellation `json:"cancellation,omitempty"`
	LastEvent    int64                `json:"older_events_before,omitempty"`
}

func (s *Store) ProcessDefinitions(ctx context.Context, scope Scope, after string) ([]ProcessSummary, error) {
	if scope.validate() != nil || (after != "" && !process.ValidKey(after)) {
		return nil, ErrInvalidProcess
	}
	rows, err := s.db.QueryContext(ctx, `SELECT p.process_key,p.current_revision FROM process_names p JOIN workspaces w ON w.id=p.workspace_id
		WHERE w.workspace_key=? AND p.process_key>? ORDER BY p.process_key LIMIT ?`, scope.Workspace, after, ConsolePageSize+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ProcessSummary{}
	for rows.Next() {
		var item ProcessSummary
		if err = rows.Scan(&item.Key, &item.Revision); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) CheckProcessDefinition(ctx context.Context, scope Scope, def process.Definition) ([]process.Diagnostic, error) {
	if scope.validate() != nil {
		return nil, ErrInvalidProcess
	}
	if diagnostics := def.Validate(); len(diagnostics) != 0 {
		return diagnostics, nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return nil, err
	}
	diagnostics, err := validateProcessChildren(ctx, tx, workspace, def)
	if err != nil {
		return nil, err
	}
	return diagnostics, tx.Commit()
}

func (s *Store) ProcessRuns(ctx context.Context, scope Scope, key string, before int64) ([]ProcessRun, error) {
	if scope.validate() != nil || (key != "" && !process.ValidKey(key)) || before < 0 {
		return nil, ErrInvalidProcess
	}
	query := `SELECT ` + processRunColumns + ` FROM process_runs r JOIN workspaces w ON w.id=r.workspace_id
		JOIN process_names n ON n.workspace_id=r.workspace_id AND n.id=r.process_id WHERE w.workspace_key=?`
	args := []any{scope.Workspace}
	if key != "" {
		query += " AND n.process_key=?"
		args = append(args, key)
	}
	if before > 0 {
		query += " AND r.id<?"
		args = append(args, before)
	}
	args = append(args, ConsolePageSize+1)
	rows, err := s.db.QueryContext(ctx, query+" ORDER BY r.id DESC LIMIT ?", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ProcessRun{}
	for rows.Next() {
		run, err := readProcessRun(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, run)
	}
	return out, rows.Err()
}

func collectProcessRows[T any](ctx context.Context, tx *sql.Tx, query string, read func(interface{ Scan(...any) error }) (T, error), args ...any) ([]T, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []T{}
	for rows.Next() {
		item, err := read(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) ProcessDetail(ctx context.Context, scope Scope, id, before int64) (ProcessRunDetail, error) {
	var out ProcessRunDetail
	if scope.validate() != nil || id <= 0 || before < 0 {
		return out, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return out, err
	}
	out.Run, err = loadProcessRun(ctx, tx, workspace, id, false)
	if err != nil {
		return out, err
	}
	version, err := loadProcessVersion(ctx, tx, workspace, out.Run.Key, out.Run.Revision)
	if err != nil {
		return out, err
	}
	out.Definition = version.Definition
	out.Waits, err = collectProcessRows(ctx, tx, `SELECT `+processWaitColumns+` FROM process_waits
		WHERE workspace_id=? AND run_id=? ORDER BY id`, scanProcessWait, workspace, id)
	if err != nil {
		return out, err
	}
	out.Tasks, err = collectProcessRows(ctx, tx, `SELECT `+processTaskColumns+` FROM process_tasks t JOIN process_waits w ON w.id=t.wait_id
		WHERE t.workspace_id=? AND t.run_id=? ORDER BY t.id`, readProcessTask, workspace, id)
	if err != nil {
		return out, err
	}
	out.Children, err = processChildrenForRun(ctx, tx, out.Run)
	if err != nil {
		return out, err
	}
	out.Actions, err = processActionsForRun(ctx, tx, out.Run)
	if err != nil {
		return out, err
	}
	out.Signals, err = collectProcessRows(ctx, tx, `SELECT `+processSignalColumns+` FROM process_signals s
		WHERE s.workspace_id=? AND s.run_id=? ORDER BY s.id DESC LIMIT 100`, readProcessSignal, workspace, id)
	if err != nil {
		return out, err
	}
	cancellation, err := processCancellationByRun(ctx, tx, out.Run)
	if err == nil {
		out.Cancellation = &cancellation
	} else if !errors.Is(err, ErrNotFound) {
		return out, err
	}
	out.Events, err = collectProcessRows(ctx, tx, `SELECT id,definition_rev,actor,kind,request_key,details,occurred_at FROM process_events
		WHERE workspace_id=? AND run_id=? AND (?=0 OR id<?) ORDER BY id DESC LIMIT 201`, readProcessEvent, workspace, id, before, before)
	if err != nil {
		return out, err
	}
	if len(out.Events) > 200 {
		out.Events = out.Events[:200]
		out.LastEvent = out.Events[199].ID
	}
	return out, tx.Commit()
}

func readProcessEvent(row interface{ Scan(...any) error }) (ProcessEvent, error) {
	var out ProcessEvent
	err := row.Scan(&out.ID, &out.Revision, &out.Actor, &out.Kind, &out.Request, &out.Details, &out.At)
	return out, err
}
