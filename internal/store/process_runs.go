package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

var processReference = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`)

type NewProcessRun struct {
	Revision int             `json:"definition_rev,omitempty"`
	Business string          `json:"business_key"`
	Input    json.RawMessage `json:"input"`
}

type ProcessRun struct {
	ID            int64           `json:"run_id"`
	Key           string          `json:"process_key"`
	Revision      int             `json:"definition_rev"`
	StartRevision int             `json:"start_revision"`
	Business      string          `json:"business_key"`
	Input         json.RawMessage `json:"input"`
	Actor         string          `json:"actor"`
	Status        string          `json:"status"`
	Step          string          `json:"step_key"`
	Transition    uint64          `json:"transition_rev"`
	StartedAt     time.Time       `json:"started_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
	workspace     int64
	process       int64
	inputDigest   []byte
}

const processRunColumns = `r.id,n.process_key,r.definition_rev,r.start_revision,r.business_key,r.input,r.actor,r.status,r.step_key,
	r.transition_rev,r.started_at,r.updated_at,r.workspace_id,r.process_id,r.input_digest`

func readProcessRun(row interface{ Scan(...any) error }) (ProcessRun, error) {
	var out ProcessRun
	err := row.Scan(&out.ID, &out.Key, &out.Revision, &out.StartRevision, &out.Business, &out.Input, &out.Actor, &out.Status,
		&out.Step, &out.Transition, &out.StartedAt, &out.UpdatedAt, &out.workspace, &out.process, &out.inputDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	return out, err
}

func loadProcessRun(ctx context.Context, tx *sql.Tx, workspace, run int64, lock bool) (ProcessRun, error) {
	query := `SELECT ` + processRunColumns + ` FROM process_runs r
		JOIN process_names n ON n.workspace_id=r.workspace_id AND n.id=r.process_id WHERE r.workspace_id=? AND r.id=?`
	if lock {
		query += " FOR UPDATE OF r"
	}
	return readProcessRun(tx.QueryRowContext(ctx, query, workspace, run))
}

func processObject(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return json.RawMessage("{}"), nil
	}
	if len(raw) > 64<<10 || !json.Valid(raw) {
		return nil, ErrInvalidProcess
	}
	var value map[string]any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil || value == nil {
		return nil, ErrInvalidProcess
	}
	return json.Marshal(value)
}

func (s *Store) StartProcess(ctx context.Context, scope Scope, key string, input NewProcessRun) (ProcessRun, error) {
	var out ProcessRun
	if scope.validate() != nil || !process.ValidKey(key) || !processReference.MatchString(input.Business) ||
		strings.HasPrefix(input.Business, "child:") || input.Revision < 0 {
		return out, ErrInvalidProcess
	}
	canonical, err := processObject(input.Input)
	if err != nil {
		return out, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := processIntakeWorkspace(ctx, tx, scope.Workspace, false)
	if err != nil {
		return out, err
	}
	intake, err := readProcessIntake(ctx, tx, workspace, scope.Workspace)
	if err != nil {
		return out, err
	}
	if intake.Paused {
		out, err = readProcessRun(tx.QueryRowContext(ctx, `SELECT `+processRunColumns+` FROM process_runs r
			JOIN process_names n ON n.workspace_id=r.workspace_id AND n.id=r.process_id
			WHERE r.workspace_id=? AND n.process_key=? AND r.business_key=?`, workspace, key, input.Business))
		if errors.Is(err, ErrNotFound) {
			return out, ErrProcessPaused
		}
		if err != nil {
			return out, err
		}
		digest := sha256.Sum256(canonical)
		if !bytes.Equal(out.inputDigest, digest[:]) || (input.Revision != 0 && out.StartRevision != input.Revision) {
			return ProcessRun{}, ErrProcessConflict
		}
		return out, tx.Commit()
	}
	out, err = startProcessInTx(ctx, tx, workspace, scope.Actor, key, input.Revision, input.Business, canonical)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func startProcessInTx(ctx context.Context, tx *sql.Tx, workspace int64, actor, key string, revision int, business string, input json.RawMessage) (ProcessRun, error) {
	var out ProcessRun
	version, err := loadProcessVersion(ctx, tx, workspace, key, revision)
	if err != nil {
		return out, err
	}
	digest := sha256.Sum256(input)
	result, err := tx.ExecContext(ctx, `INSERT INTO process_runs
		(workspace_id,process_id,start_revision,definition_rev,business_key,input,input_digest,actor,step_key)
		VALUES (?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(process_runs.id)`,
		workspace, version.ID, version.Revision, version.Revision, business, input, digest[:], actor, version.Definition.Start)
	if err != nil {
		return out, err
	}
	id, err := result.LastInsertId()
	if err != nil {
		return out, err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return out, err
	}
	out, err = loadProcessRun(ctx, tx, workspace, id, true)
	if err != nil {
		return out, err
	}
	if inserted == 0 {
		if !bytes.Equal(out.inputDigest, digest[:]) || (revision != 0 && out.StartRevision != revision) {
			return out, ErrProcessConflict
		}
		return out, nil
	}
	if err = processEvent(ctx, tx, out, actor, "run_started", business, map[string]string{"step": out.Step}); err != nil {
		return out, err
	}
	if err = enterProcessStep(ctx, tx, &out, version.Definition); err != nil {
		return out, err
	}
	return loadProcessRun(ctx, tx, workspace, id, false)
}

func processEvent(ctx context.Context, tx *sql.Tx, run ProcessRun, actor, kind, key string, details any) error {
	raw, err := json.Marshal(details)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO process_events
		(workspace_id,run_id,definition_rev,actor,kind,request_key,details) VALUES (?,?,?,?,?,NULLIF(?,''),?)`,
		run.workspace, run.ID, run.Revision, actor, kind, key, raw)
	return err
}

func enterProcessStep(ctx context.Context, tx *sql.Tx, run *ProcessRun, definition process.Definition) error {
	step, ok := definition.Step(run.Step)
	if !ok {
		return fmt.Errorf("process %d revision %d has no step %q", run.ID, run.Revision, run.Step)
	}
	run.Status = "running"
	switch step.Kind {
	case process.Signal, process.Approval, process.Timer, process.Children:
		run.Status = "waiting"
	case process.End:
		run.Status = step.Result
	}
	_, err := tx.ExecContext(ctx, `UPDATE process_runs SET status=?,step_key=?,transition_rev=? WHERE workspace_id=? AND id=?`,
		run.Status, run.Step, run.Transition, run.workspace, run.ID)
	if err != nil {
		return err
	}
	switch step.Kind {
	case process.Signal, process.Approval, process.Timer:
		return createProcessWait(ctx, tx, run, definition, step)
	case process.Children:
		return queueProcessChildren(ctx, tx, *run, step)
	case process.Action:
		return queueProcessAction(ctx, tx, *run, step)
	case process.End:
		return processEvent(ctx, tx, *run, "scheduler", "run_finished", "", map[string]string{"step": run.Step, "status": run.Status})
	default:
		return processEvent(ctx, tx, *run, "scheduler", "step_entered", "", map[string]string{"step": run.Step, "kind": step.Kind})
	}
}

func (s *Store) ProcessRun(ctx context.Context, scope Scope, id int64) (ProcessRun, error) {
	if scope.validate() != nil || id <= 0 {
		return ProcessRun{}, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProcessRun{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return ProcessRun{}, err
	}
	out, err := loadProcessRun(ctx, tx, workspace, id, false)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}
