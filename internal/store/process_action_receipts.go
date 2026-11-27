package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

type ProcessActionReport struct {
	Epoch    uint64          `json:"lease_epoch"`
	Outcome  string          `json:"outcome"`
	Receipt  string          `json:"external_receipt,omitempty"`
	Evidence json.RawMessage `json:"evidence"`
}

type ProcessActionResolution struct {
	Key      string          `json:"operation_key"`
	Decision string          `json:"decision"`
	Receipt  string          `json:"external_receipt,omitempty"`
	Evidence json.RawMessage `json:"evidence"`
	Reason   string          `json:"reason"`
}

func (s *Store) ReportProcessAction(ctx context.Context, scope Scope, processKey, operation string, id int64, request string, report ProcessActionReport) (ProcessAction, error) {
	if !process.ValidKey(processKey) || !process.ValidKey(operation) || report.Epoch == 0 || !validActionReport(report.Outcome, report.Receipt) {
		return ProcessAction{}, ErrInvalidProcess
	}
	evidence, err := processObject(report.Evidence)
	if err != nil {
		return ProcessAction{}, err
	}
	report.Evidence = evidence
	return s.recordProcessActionInput(ctx, scope, processKey, operation, id, request, "adapter", report,
		func(tx *sql.Tx, run *ProcessRun, action ProcessAction) error {
			var now time.Time
			if err := tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(6)`).Scan(&now); err != nil {
				return err
			}
			if action.Status != "leased" || action.Epoch != report.Epoch || action.LeasedBy == nil || *action.LeasedBy != scope.Actor ||
				action.LeaseUntil == nil || !now.Before(*action.LeaseUntil) {
				return ErrProcessConflict
			}
			return applyActionReport(ctx, tx, run, action, scope.Actor, request, report)
		})
}

func validActionReport(outcome, receipt string) bool {
	return (outcome == "succeeded" || outcome == "failed" || outcome == "unknown") &&
		((outcome != "succeeded" && receipt == "") || processReference.MatchString(receipt))
}

func (s *Store) ResolveProcessAction(ctx context.Context, scope Scope, id int64, request string, resolution ProcessActionResolution) (ProcessAction, error) {
	resolution.Reason = strings.TrimSpace(resolution.Reason)
	if !processReference.MatchString(resolution.Key) || len(resolution.Reason) < 1 || len(resolution.Reason) > 1024 ||
		(resolution.Decision != "applied" && resolution.Decision != "not_applied" && resolution.Decision != "stop") ||
		(resolution.Receipt != "" && !processReference.MatchString(resolution.Receipt)) ||
		(resolution.Decision == "applied" && resolution.Receipt == "") {
		return ProcessAction{}, ErrInvalidProcess
	}
	evidence, err := processObject(resolution.Evidence)
	if err != nil {
		return ProcessAction{}, err
	}
	resolution.Evidence = evidence
	return s.recordProcessActionInput(ctx, scope, "", "", id, request, "operator", resolution,
		func(tx *sql.Tx, run *ProcessRun, action ProcessAction) error {
			if action.Status != "unknown" || action.Key != resolution.Key || action.ResolutionOwner == nil || *action.ResolutionOwner != scope.Actor {
				return ErrProcessConflict
			}
			return applyActionResolution(ctx, tx, run, action, scope.Actor, request, resolution)
		})
}

func (s *Store) recordProcessActionInput(ctx context.Context, scope Scope, processKey, operation string, id int64, request, source string,
	input any, apply func(*sql.Tx, *ProcessRun, ProcessAction) error) (ProcessAction, error) {
	var out ProcessAction
	if scope.validate() != nil || id <= 0 || !processReference.MatchString(request) {
		return out, ErrInvalidProcess
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return out, err
	}
	digest := sha256.Sum256(append([]byte(source+"\x00"), raw...))
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
	err = tx.QueryRowContext(ctx, `SELECT run_id FROM process_actions WHERE workspace_id=? AND id=?`, workspace, id).Scan(&runID)
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
	out, err = processActionByID(ctx, tx, workspace, id)
	if err != nil {
		return out, err
	}
	if processKey != "" && (processKey != run.Key || operation != out.Operation) {
		return ProcessAction{}, ErrNotFound
	}
	var priorActor string
	var priorHash, priorReceipt []byte
	err = tx.QueryRowContext(ctx, `SELECT actor,digest,receipt FROM process_action_requests WHERE workspace_id=? AND action_id=? AND request_key=?`,
		workspace, id, request).Scan(&priorActor, &priorHash, &priorReceipt)
	if err == nil {
		if priorActor != scope.Actor || !bytes.Equal(priorHash, digest[:]) {
			return out, ErrProcessConflict
		}
		if err = json.Unmarshal(priorReceipt, &out); err != nil {
			return out, err
		}
		return out, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return out, err
	}
	if err = apply(tx, &run, out); err != nil {
		return out, err
	}
	out, err = processActionByID(ctx, tx, workspace, id)
	if err != nil {
		return out, err
	}
	receipt, err := json.Marshal(out)
	if err != nil {
		return out, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO process_action_requests (workspace_id,action_id,request_key,actor,digest,receipt) VALUES (?,?,?,?,?,?)`,
		workspace, id, request, scope.Actor, digest[:], receipt)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func applyActionReport(ctx context.Context, tx *sql.Tx, run *ProcessRun, action ProcessAction, actor, request string, report ProcessActionReport) error {
	cancellation, err := processCancellationByRun(ctx, tx, *run)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	state, owner, resolution := report.Outcome, "", "adapter_confirmed"
	if state == "unknown" || (action.Direction == "reverse" && state == "failed") {
		state, owner, resolution = "unknown", run.Actor, "adapter_unconfirmed"
		if cancellation.Status != "" {
			owner = cancellation.Initiator
		}
	}
	if err = storeActionOutcome(ctx, tx, *run, action, state, report.Receipt, report.Evidence, owner, resolution); err != nil {
		return err
	}
	if err = processEvent(ctx, tx, *run, actor, "action_reported", request, map[string]any{
		"action_id": action.ID, "operation_key": action.Key, "outcome": report.Outcome, "state": state, "external_receipt": report.Receipt,
		"lease_epoch": report.Epoch, "evidence": report.Evidence, "resolution_owner": owner,
	}); err != nil {
		return err
	}
	if cancellation.Status != "" {
		return advanceProcessCancellation(ctx, tx, run)
	}
	if state == "unknown" {
		return updateProcessStatus(ctx, tx, run, "reconcile")
	}
	return advanceProcessAction(ctx, tx, run, action, state)
}

func storeActionOutcome(ctx context.Context, tx *sql.Tx, run ProcessRun, action ProcessAction, state, receipt string, evidence json.RawMessage, owner, resolution string) error {
	_, err := tx.ExecContext(ctx, `UPDATE process_actions SET status=?,external_receipt=NULLIF(?,''),evidence=?,resolution_owner=NULLIF(?,''),
		resolution=?,lease_until=NULL,confirmed_at=IF(? IN ('succeeded','failed'),UTC_TIMESTAMP(6),NULL) WHERE workspace_id=? AND id=?`,
		state, receipt, evidence, owner, resolution, state, run.workspace, action.ID)
	return err
}

func advanceProcessAction(ctx context.Context, tx *sql.Tx, run *ProcessRun, action ProcessAction, state string) error {
	if (run.Status != "running" && run.Status != "reconcile") || run.Step != action.Step || run.Transition != action.Iteration {
		return ErrProcessConflict
	}
	version, err := loadProcessVersion(ctx, tx, run.workspace, run.Key, run.Revision)
	if err != nil {
		return err
	}
	step, ok := version.Definition.Step(run.Step)
	if !ok || step.Kind != process.Action {
		return ErrProcessConflict
	}
	run.Step = step.Next
	if state == "failed" {
		run.Step = step.OnFailure
	}
	run.Transition++
	return enterProcessStep(ctx, tx, run, version.Definition)
}

func applyActionResolution(ctx context.Context, tx *sql.Tx, run *ProcessRun, action ProcessAction, actor, request string, outcome ProcessActionResolution) error {
	cancellation, err := processCancellationByRun(ctx, tx, *run)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	state := "succeeded"
	switch outcome.Decision {
	case "not_applied":
		state = "failed"
		if action.Direction == "reverse" {
			state = "ready"
		}
	case "stop":
		state = "discarded"
	}
	if err = storeActionOutcome(ctx, tx, *run, action, state, outcome.Receipt, outcome.Evidence, actor, outcome.Decision); err != nil {
		return err
	}
	if err = processEvent(ctx, tx, *run, actor, "action_reconciled", request, map[string]any{
		"action_id": action.ID, "operation_key": action.Key, "decision": outcome.Decision, "reason": outcome.Reason,
		"external_receipt": outcome.Receipt, "evidence": outcome.Evidence,
	}); err != nil {
		return err
	}
	if cancellation.Status != "" {
		if outcome.Decision == "stop" {
			return finishProcessCancellation(ctx, tx, run, cancellation, "stopped")
		}
		if _, err = tx.ExecContext(ctx, `UPDATE process_cancellations SET status='waiting' WHERE workspace_id=? AND run_id=? AND status='reconcile'`,
			run.workspace, run.ID); err != nil {
			return err
		}
		if !terminalProcessStatus(cancellation.InitialStatus) {
			if err = updateProcessStatus(ctx, tx, run, "cancelling"); err != nil {
				return err
			}
		}
		return advanceProcessCancellation(ctx, tx, run)
	}
	if outcome.Decision == "stop" {
		if err = updateProcessStatus(ctx, tx, run, "failed"); err != nil {
			return err
		}
		return processEvent(ctx, tx, *run, actor, "run_finished", request, map[string]any{"status": "failed", "step": run.Step})
	}
	return advanceProcessAction(ctx, tx, run, action, state)
}

func (s *Store) ExpireProcessActionClaims(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 256 {
		return 0, ErrInvalidProcess
	}
	rows, err := s.db.QueryContext(ctx, `SELECT workspace_id,run_id,id FROM process_actions
		WHERE status='leased' AND lease_until<=UTC_TIMESTAMP(6) ORDER BY lease_until,id LIMIT ?`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var candidates []processActionCandidate
	for rows.Next() {
		var item processActionCandidate
		if err = rows.Scan(&item.workspace, &item.run, &item.action); err != nil {
			return 0, err
		}
		candidates = append(candidates, item)
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	expired := 0
	for _, item := range candidates {
		changed, err := s.expireProcessActionClaim(ctx, item)
		if err != nil {
			return expired, err
		}
		if changed {
			expired++
		}
	}
	return expired, nil
}

func (s *Store) expireProcessActionClaim(ctx context.Context, item processActionCandidate) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	run, err := loadProcessRun(ctx, tx, item.workspace, item.run, true)
	if err != nil {
		return false, err
	}
	action, err := readProcessAction(tx.QueryRowContext(ctx, `SELECT `+processActionColumns+processActionTable+
		`WHERE a.workspace_id=? AND a.id=? AND a.status='leased' AND a.lease_until<=UTC_TIMESTAMP(6)`, item.workspace, item.action))
	if errors.Is(err, ErrNotFound) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	request := fmt.Sprintf("action-lease:%d:%d", action.ID, action.Epoch)
	report := ProcessActionReport{Epoch: action.Epoch, Outcome: "unknown", Evidence: json.RawMessage(`{"reason":"adapter_claim_expired"}`)}
	if err = applyActionReport(ctx, tx, &run, action, "scheduler", request, report); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
