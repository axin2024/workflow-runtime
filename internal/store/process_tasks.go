package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

type ProcessTask struct {
	ID          int64      `json:"task_id"`
	Run         int64      `json:"run_id"`
	Wait        int64      `json:"wait_id"`
	Step        string     `json:"step_key"`
	Duty        string     `json:"duty_role"`
	Status      string     `json:"status"`
	Revision    uint64     `json:"claim_rev"`
	Assigned    *string    `json:"assigned_actor,omitempty"`
	ClaimUntil  *time.Time `json:"claim_until,omitempty"`
	Deadline    time.Time  `json:"deadline"`
	CompletedBy *string    `json:"completed_by,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

type ProcessTaskInput struct {
	Action    string `json:"action"`
	Revision  uint64 `json:"claim_rev"`
	Assignee  string `json:"assignee,omitempty"`
	ClaimSecs int    `json:"claim_seconds,omitempty"`
	Note      string `json:"note,omitempty"`
}

const processTaskColumns = `t.id,t.run_id,t.wait_id,w.step_key,t.duty_role,t.status,t.claim_rev,t.assigned_actor,t.claim_until,w.deadline,
	t.completed_by,t.completed_at`

func readProcessTask(row interface{ Scan(...any) error }) (ProcessTask, error) {
	var task ProcessTask
	err := row.Scan(&task.ID, &task.Run, &task.Wait, &task.Step, &task.Duty, &task.Status, &task.Revision, &task.Assigned,
		&task.ClaimUntil, &task.Deadline, &task.CompletedBy, &task.CompletedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return task, ErrNotFound
	}
	return task, err
}

func createProcessTask(ctx context.Context, tx *sql.Tx, run ProcessRun, wait ProcessWait, duty string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO process_tasks (workspace_id,run_id,wait_id,duty_role) VALUES (?,?,?,?)`,
		run.workspace, run.ID, wait.ID, duty)
	return err
}

func expireProcessTask(ctx context.Context, tx *sql.Tx, run ProcessRun, wait ProcessWait) error {
	_, err := tx.ExecContext(ctx, `UPDATE process_tasks SET status='expired',claim_rev=claim_rev+1
		WHERE workspace_id=? AND wait_id=? AND status IN ('pending','claimed')`, run.workspace, wait.ID)
	return err
}

func (s *Store) SetProcessDuty(ctx context.Context, scope Scope, actor, duty string) error {
	if scope.validate() != nil || !identifier.MatchString(actor) || !process.ValidKey(duty) {
		return ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return err
	}
	var id int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM console_identities WHERE workspace_id=? AND actor=?`, workspace, actor).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO process_duty_roles (identity_id,duty_role) VALUES (?,?)
		ON DUPLICATE KEY UPDATE duty_role=process_duty_roles.duty_role`, id, duty)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func hasProcessDuty(ctx context.Context, tx *sql.Tx, workspace int64, actor, duty string) (bool, error) {
	var present bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM console_identities i
		JOIN process_duty_roles p ON p.identity_id=i.id WHERE i.workspace_id=? AND i.actor=? AND p.duty_role=?)`,
		workspace, actor, duty).Scan(&present)
	return present, err
}

func (s *Store) ProcessTaskQueue(ctx context.Context, scope Scope) ([]ProcessTask, error) {
	if scope.validate() != nil {
		return nil, ErrInvalidProcess
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
	rows, err := tx.QueryContext(ctx, `SELECT `+processTaskColumns+` FROM process_tasks t JOIN process_waits w ON w.id=t.wait_id
		JOIN console_identities i ON i.workspace_id=t.workspace_id AND i.actor=?
		JOIN process_duty_roles p ON p.identity_id=i.id AND p.duty_role=t.duty_role
		WHERE t.workspace_id=? AND t.status IN ('pending','claimed') AND w.deadline>UTC_TIMESTAMP(6)
		ORDER BY w.deadline,t.id LIMIT 200`, scope.Actor, workspace)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []ProcessTask{}
	for rows.Next() {
		item, err := readProcessTask(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	return items, tx.Commit()
}

func taskInputOK(input ProcessTaskInput) bool {
	if input.Revision == 0 || len(input.Note) > 1024 {
		return false
	}
	switch input.Action {
	case "claim":
		return input.Assignee == "" && input.ClaimSecs > 0 && input.ClaimSecs <= process.MaxWaitSeconds
	case "delegate":
		return identifier.MatchString(input.Assignee) && input.ClaimSecs > 0 && input.ClaimSecs <= process.MaxWaitSeconds
	case "approve", "reject":
		return input.Assignee == "" && input.ClaimSecs == 0
	}
	return false
}

func (s *Store) ChangeProcessTask(ctx context.Context, scope Scope, id int64, request string, input ProcessTaskInput) (ProcessTask, error) {
	var out ProcessTask
	if scope.validate() != nil || id <= 0 || !processReference.MatchString(request) || !taskInputOK(input) {
		return out, ErrInvalidProcess
	}
	input.Note = strings.TrimSpace(input.Note)
	raw, err := json.Marshal(input)
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
	var runID int64
	err = tx.QueryRowContext(ctx, `SELECT run_id FROM process_tasks WHERE workspace_id=? AND id=?`, workspace, id).Scan(&runID)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	run, err := loadProcessRun(ctx, tx, workspace, runID, true)
	if err != nil {
		return out, err
	}
	out, err = processTaskByID(ctx, tx, workspace, id)
	if err != nil {
		return out, err
	}
	allowed, err := hasProcessDuty(ctx, tx, workspace, scope.Actor, out.Duty)
	if err != nil {
		return out, err
	}
	if !allowed {
		return out, ErrNotFound
	}
	var priorActor string
	var priorHash, receipt []byte
	var conflict bool
	err = tx.QueryRowContext(ctx, `SELECT actor,digest,receipt,conflict FROM process_task_requests
		WHERE workspace_id=? AND task_id=? AND request_key=?`, workspace, id, request).Scan(&priorActor, &priorHash, &receipt, &conflict)
	if err == nil {
		if priorActor != scope.Actor || !bytes.Equal(priorHash, digest[:]) {
			return out, ErrProcessConflict
		}
		if err = json.Unmarshal(receipt, &out); err != nil {
			return out, err
		}
		if conflict {
			return out, ErrProcessConflict
		}
		return out, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(6)`).Scan(&now); err != nil {
		return out, err
	}
	out, conflict, err = applyProcessTask(ctx, tx, run, out, scope.Actor, request, input, now)
	if err != nil {
		return out, err
	}
	receipt, err = json.Marshal(out)
	if err != nil {
		return out, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO process_task_requests (workspace_id,task_id,request_key,actor,digest,receipt,conflict)
		VALUES (?,?,?,?,?,?,?)`, workspace, id, request, scope.Actor, digest[:], receipt, conflict)
	if err != nil {
		return out, err
	}
	if err = tx.Commit(); err != nil {
		return out, err
	}
	if conflict {
		return out, ErrProcessConflict
	}
	return out, nil
}

func processTaskByID(ctx context.Context, tx *sql.Tx, workspace, id int64) (ProcessTask, error) {
	return readProcessTask(tx.QueryRowContext(ctx, `SELECT `+processTaskColumns+`
		FROM process_tasks t JOIN process_waits w ON w.id=t.wait_id WHERE t.workspace_id=? AND t.id=?`, workspace, id))
}

func applyProcessTask(ctx context.Context, tx *sql.Tx, run ProcessRun, task ProcessTask, actor, request string, input ProcessTaskInput, now time.Time) (ProcessTask, bool, error) {
	active := run.Status == "waiting" && run.Step == task.Step && now.Before(task.Deadline)
	held := task.Status == "claimed" && task.ClaimUntil != nil && now.Before(*task.ClaimUntil)
	mine := held && task.Assigned != nil && *task.Assigned == actor
	canClaim := task.Status == "pending" || (task.Status == "claimed" && !held)
	allowed := input.Revision == task.Revision && active && ((input.Action == "claim" && canClaim) || (input.Action != "claim" && mine))
	if !allowed {
		err := processEvent(ctx, tx, run, actor, "task_input_rejected", request,
			map[string]any{"task_id": task.ID, "action": input.Action, "observed_claim_rev": task.Revision, "submitted_claim_rev": input.Revision})
		return task, true, err
	}
	if input.Action == "claim" || input.Action == "delegate" {
		target := actor
		if input.Action == "delegate" {
			target = input.Assignee
			valid, err := hasProcessDuty(ctx, tx, run.workspace, target, task.Duty)
			if err != nil {
				return task, false, err
			}
			if !valid || target == actor {
				return task, false, ErrInvalidProcess
			}
		}
		until := now.Add(time.Duration(input.ClaimSecs) * time.Second)
		if until.After(task.Deadline) {
			until = task.Deadline
		}
		_, err := tx.ExecContext(ctx, `UPDATE process_tasks SET status='claimed',assigned_actor=?,claim_until=?,claim_rev=claim_rev+1
			WHERE workspace_id=? AND id=?`, target, until, run.workspace, task.ID)
		if err != nil {
			return task, false, err
		}
	} else {
		state, resolution := "approved", "accepted"
		if input.Action == "reject" {
			state, resolution = "rejected", "rejected"
		}
		_, err := tx.ExecContext(ctx, `UPDATE process_tasks SET status=?,completed_by=?,completed_at=UTC_TIMESTAMP(6),claim_rev=claim_rev+1
			WHERE workspace_id=? AND id=?`, state, actor, run.workspace, task.ID)
		if err != nil {
			return task, false, err
		}
		wait, err := currentProcessWait(ctx, tx, run)
		if err != nil || wait.ID != task.Wait {
			if err == nil {
				err = ErrProcessConflict
			}
			return task, false, err
		}
		version, err := loadProcessVersion(ctx, tx, run.workspace, run.Key, run.Revision)
		if err != nil {
			return task, false, err
		}
		if err = resolveProcessWait(ctx, tx, &run, version.Definition, wait, actor, resolution, request); err != nil {
			return task, false, err
		}
	}
	out, err := processTaskByID(ctx, tx, run.workspace, task.ID)
	if err != nil {
		return out, false, err
	}
	err = processEvent(ctx, tx, run, actor, "task_"+input.Action, request, map[string]any{"task": out, "note": input.Note})
	return out, false, err
}
