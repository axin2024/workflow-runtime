package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

type ProcessSignalInput struct {
	Event       string          `json:"event_id"`
	Correlation string          `json:"correlation_key"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
}

type ProcessFieldError struct{ Field string }

func (e *ProcessFieldError) Error() string { return "invalid " + e.Field }
func (e *ProcessFieldError) Unwrap() error { return ErrInvalidProcess }

type SignalReceipt struct {
	ID          int64           `json:"receipt_id"`
	Event       string          `json:"event_id"`
	Request     string          `json:"idempotency_key"`
	Correlation string          `json:"correlation_key"`
	Kind        string          `json:"kind"`
	Actor       string          `json:"actor"`
	Payload     json.RawMessage `json:"payload"`
	Outcome     string          `json:"outcome"`
	ConsumedBy  *int64          `json:"consumed_by"`
	ReceivedAt  time.Time       `json:"received_at"`
	MatchedAt   *time.Time      `json:"matched_at"`
	ConsumedAt  *time.Time      `json:"consumed_at"`
	ExpiresAt   time.Time       `json:"expires_at"`
	digest      []byte
}

const processSignalColumns = `s.id,s.event_key,s.request_key,s.correlation_key,s.kind,s.actor,s.payload,
	IF(s.outcome='waiting_for_match' AND s.expires_at<=UTC_TIMESTAMP(6),'expired',s.outcome),
	s.wait_id,s.received_at,s.matched_at,s.consumed_at,s.expires_at,s.payload_hash`

func readProcessSignal(row interface{ Scan(...any) error }) (SignalReceipt, error) {
	var out SignalReceipt
	err := row.Scan(&out.ID, &out.Event, &out.Request, &out.Correlation, &out.Kind, &out.Actor, &out.Payload, &out.Outcome,
		&out.ConsumedBy, &out.ReceivedAt, &out.MatchedAt, &out.ConsumedAt, &out.ExpiresAt, &out.digest)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	return out, err
}

func processSignalByEvent(ctx context.Context, tx *sql.Tx, run ProcessRun, event string) (SignalReceipt, error) {
	return readProcessSignal(tx.QueryRowContext(ctx, `SELECT `+processSignalColumns+
		` FROM process_signals s WHERE s.workspace_id=? AND s.run_id=? AND s.event_key=?`, run.workspace, run.ID, event))
}

func (s *Store) SubmitProcessSignal(ctx context.Context, scope Scope, id int64, request string, input ProcessSignalInput) (SignalReceipt, error) {
	return s.submitProcessSignal(ctx, scope, "", id, request, input)
}

func (s *Store) submitProcessSignal(ctx context.Context, scope Scope, processKey string, id int64, request string, input ProcessSignalInput) (SignalReceipt, error) {
	var out SignalReceipt
	if scope.validate() != nil || id <= 0 {
		return out, ErrInvalidProcess
	}
	for _, check := range []struct {
		field string
		ok    bool
	}{
		{"headers.Idempotency-Key", processReference.MatchString(request)},
		{"event_id", processReference.MatchString(input.Event)},
		{"correlation_key", processReference.MatchString(input.Correlation)},
		{"kind", process.ValidKey(input.Kind)},
		{"payload", len(input.Payload) != 0},
	} {
		if !check.ok {
			return out, &ProcessFieldError{Field: check.field}
		}
	}
	payload, err := processObject(input.Payload)
	if err != nil {
		return out, &ProcessFieldError{Field: "payload"}
	}
	input.Payload = payload
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
	run, err := loadProcessRun(ctx, tx, workspace, id, true)
	if err != nil {
		return out, err
	}
	if processKey != "" && run.Key != processKey {
		return out, ErrNotFound
	}
	out, err = readProcessSignal(tx.QueryRowContext(ctx, `SELECT `+processSignalColumns+`
		FROM process_signal_requests k JOIN process_signals s ON s.id=k.signal_id
		WHERE k.workspace_id=? AND k.run_id=? AND k.request_key=?`, run.workspace, run.ID, request))
	if err == nil {
		if !bytes.Equal(out.digest, digest[:]) {
			return out, commitProcessSignalConflict(ctx, tx, run, scope.Actor, request, input.Event, digest, out.ID)
		}
		return out, tx.Commit()
	}
	if !errors.Is(err, ErrNotFound) {
		return out, err
	}
	out, err = processSignalByEvent(ctx, tx, run, input.Event)
	if err == nil {
		if !bytes.Equal(out.digest, digest[:]) {
			return out, commitProcessSignalConflict(ctx, tx, run, scope.Actor, request, input.Event, digest, out.ID)
		}
		if err = addProcessSignalKey(ctx, tx, run, request, out.ID); err != nil {
			return out, err
		}
		return out, tx.Commit()
	}
	if !errors.Is(err, ErrNotFound) {
		return out, err
	}
	if input.Correlation != run.Business {
		return out, &ProcessFieldError{Field: "correlation_key"}
	}
	wait, err := currentProcessWait(ctx, tx, run)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return out, err
	}
	matches := wait.Kind == process.Signal && wait.SignalKind == input.Kind
	outcome := "waiting_for_match"
	if !matches {
		mayAdvance := run.Status == "running" || run.Status == "waiting"
		if run.Status == "reconcile" {
			_, cancellationErr := processCancellationByRun(ctx, tx, run)
			if cancellationErr != nil && !errors.Is(cancellationErr, ErrNotFound) {
				return out, cancellationErr
			}
			mayAdvance = errors.Is(cancellationErr, ErrNotFound)
		}
		if !mayAdvance {
			outcome = "late"
		} else {
			version, err := loadProcessVersion(ctx, tx, workspace, run.Key, run.Revision)
			if err != nil {
				return out, err
			}
			if !version.Definition.ReachesSignal(run.Step, input.Kind) {
				outcome = "late"
			}
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO process_signals
		(workspace_id,run_id,event_key,request_key,correlation_key,kind,actor,payload,payload_hash,outcome,expires_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,TIMESTAMPADD(DAY,7,UTC_TIMESTAMP(6)))`,
		run.workspace, run.ID, input.Event, request, input.Correlation, input.Kind, scope.Actor, payload, digest[:], outcome)
	if err != nil {
		return out, err
	}
	signalID, err := result.LastInsertId()
	if err != nil {
		return out, err
	}
	if err = addProcessSignalKey(ctx, tx, run, request, signalID); err != nil {
		return out, err
	}
	if err = processEvent(ctx, tx, run, scope.Actor, "signal_received", request,
		map[string]any{"receipt_id": signalID, "event_id": input.Event, "outcome": outcome}); err != nil {
		return out, err
	}
	if matches {
		version, err := loadProcessVersion(ctx, tx, workspace, run.Key, run.Revision)
		if err != nil {
			return out, err
		}
		if err = matchPendingProcessSignal(ctx, tx, &run, version.Definition, wait); err != nil {
			return out, err
		}
	}
	out, err = processSignalByEvent(ctx, tx, run, input.Event)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func addProcessSignalKey(ctx context.Context, tx *sql.Tx, run ProcessRun, request string, id int64) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO process_signal_requests (workspace_id,run_id,request_key,signal_id)
		VALUES (?,?,?,?)`, run.workspace, run.ID, request, id)
	return err
}

func commitProcessSignalConflict(ctx context.Context, tx *sql.Tx, run ProcessRun, actor, request, event string, digest [32]byte, prior int64) error {
	if err := processEvent(ctx, tx, run, actor, "signal_conflict", request,
		map[string]any{"receipt_id": prior, "event_id": event, "attempted_sha256": hex.EncodeToString(digest[:])}); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return ErrProcessConflict
}

func matchPendingProcessSignal(ctx context.Context, tx *sql.Tx, run *ProcessRun, def process.Definition, wait ProcessWait) error {
	var id int64
	var request, actor string
	err := tx.QueryRowContext(ctx, `SELECT id,request_key,actor FROM process_signals
		WHERE workspace_id=? AND run_id=? AND kind=? AND outcome='waiting_for_match' AND expires_at>UTC_TIMESTAMP(6)
		ORDER BY id LIMIT 1 FOR UPDATE`, run.workspace, run.ID, wait.SignalKind).Scan(&id, &request, &actor)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_signals SET outcome='consumed',wait_id=?,matched_at=UTC_TIMESTAMP(6),
		consumed_at=UTC_TIMESTAMP(6) WHERE id=?`, wait.ID, id)
	if err != nil {
		return err
	}
	return resolveProcessWait(ctx, tx, run, def, wait, actor, "accepted", request)
}

func (s *Store) ExpireProcessSignals(ctx context.Context, limit int) (int64, error) {
	if limit < 1 || limit > 1000 {
		return 0, ErrInvalidProcess
	}
	result, err := s.db.ExecContext(ctx, `UPDATE process_signals SET outcome='expired'
		WHERE outcome='waiting_for_match' AND expires_at<=UTC_TIMESTAMP(6) ORDER BY expires_at,id LIMIT ?`, limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
