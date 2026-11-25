package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"errors"
	"time"
)

const callbackLease = 30 * time.Second

var ErrCallbackLeaseLost = errors.New("callback delivery lease lost")

type CallbackEvent struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Workspace string         `json:"workspace"`
	Actor     string         `json:"actor"`
	Run       TriggerReceipt `json:"run"`
}

type CallbackClaim struct {
	Event    CallbackEvent
	Attempts uint64
	token    [32]byte
}

type CallbackStatus struct {
	EventID        string     `json:"event_id"`
	RunID          int64      `json:"run_id"`
	Attempts       uint64     `json:"attempts"`
	AvailableAt    time.Time  `json:"available_at"`
	LastHTTPStatus *int       `json:"last_http_status,omitempty"`
	DeliveredAt    *time.Time `json:"delivered_at,omitempty"`
}

func saveRunState(ctx context.Context, tx *sql.Tx, runID int64, state string, finished *time.Time) error {
	if _, err := tx.ExecContext(ctx, `UPDATE runs SET state = ?, finished_at = ? WHERE id = ?`, state, finished, runID); err != nil {
		return err
	}
	if finished == nil {
		return nil
	}
	// The run row is locked until commit. Only trigger identities have receipts.
	_, err := tx.ExecContext(ctx, `INSERT INTO callback_deliveries (run_id, identity_id, event_id, available_at)
		SELECT t.run_id, t.identity_id, RANDOM_BYTES(16), ? FROM trigger_receipts t
		LEFT JOIN callback_deliveries c ON c.run_id = t.run_id WHERE t.run_id = ? AND c.run_id IS NULL`,
		*finished, runID)
	return err
}

func (s *Store) ClaimCallback(ctx context.Context, scope Scope, dagKey string) (*CallbackClaim, error) {
	if err := scope.validate(); err != nil {
		return nil, err
	}
	if !identifier.MatchString(dagKey) {
		return nil, errors.New("invalid workflow key")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	workspace, dag, err := triggerScopeIDs(ctx, tx, scope, dagKey)
	if err != nil {
		return nil, err
	}
	var identity int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM trigger_identities
		WHERE workspace_id = ? AND dag_id = ? AND actor = ? AND token_digest IS NOT NULL FOR SHARE`, workspace, dag, scope.Actor).Scan(&identity)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrTriggerUnauthorized
	}
	if err != nil {
		return nil, err
	}
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	claim := CallbackClaim{Event: CallbackEvent{Type: "workflow.run.finished.v1", Workspace: scope.Workspace, Actor: scope.Actor}}
	run := &claim.Event.Run
	run.DAG = dagKey
	err = tx.QueryRowContext(ctx, `SELECT LOWER(HEX(c.event_id)), c.attempts, r.id, r.trigger_key, r.revision, r.state, r.logical_at, r.finished_at
		FROM callback_deliveries c JOIN runs r ON r.id = c.run_id
		WHERE c.identity_id = ? AND c.delivered_at IS NULL AND c.available_at <= ?
		ORDER BY c.available_at, c.run_id LIMIT 1 FOR UPDATE OF c SKIP LOCKED`, identity, now).
		Scan(&claim.Event.ID, &claim.Attempts, &run.ID, &run.Key, &run.Revision, &run.State, &run.LogicalAt, &run.FinishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	rand.Read(claim.token[:])
	claim.Attempts++
	if _, err := tx.ExecContext(ctx, `UPDATE callback_deliveries SET attempts = ?, available_at = ?, lease_token = ? WHERE run_id = ?`,
		claim.Attempts, now.Add(callbackLease), claim.token[:], run.ID); err != nil {
		return nil, err
	}
	return &claim, tx.Commit()
}

func (s *Store) FinishCallback(ctx context.Context, claim CallbackClaim, httpStatus int) (CallbackStatus, error) {
	if claim.Attempts == 0 || claim.token == [32]byte{} || httpStatus < 0 || httpStatus > 599 || (httpStatus > 0 && httpStatus < 100) {
		return CallbackStatus{}, errors.New("invalid callback completion")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return CallbackStatus{}, err
	}
	defer tx.Rollback()
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return CallbackStatus{}, err
	}
	record := CallbackStatus{EventID: claim.Event.ID, RunID: claim.Event.Run.ID, Attempts: claim.Attempts}
	if httpStatus > 0 {
		record.LastHTTPStatus = &httpStatus
	}
	if httpStatus >= 200 && httpStatus < 300 {
		record.AvailableAt = now
		record.DeliveredAt = &now
	} else {
		interval := 5 * time.Second << min(claim.Attempts-1, 6)
		record.AvailableAt = now.Add(min(interval, 5*time.Minute))
	}
	result, err := tx.ExecContext(ctx, `UPDATE callback_deliveries SET available_at = ?, delivered_at = ?,
		last_http_status = ?, lease_token = NULL WHERE run_id = ? AND lease_token = ? AND delivered_at IS NULL AND available_at > UTC_TIMESTAMP(6)`,
		record.AvailableAt, record.DeliveredAt, record.LastHTTPStatus, record.RunID, claim.token[:])
	if err != nil {
		return CallbackStatus{}, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return CallbackStatus{}, err
	}
	if count != 1 {
		return CallbackStatus{}, ErrCallbackLeaseLost
	}
	return record, tx.Commit()
}

func (s *Store) GetCallbackStatus(ctx context.Context, scope Scope, dagKey string, runID int64) (CallbackStatus, error) {
	if err := scope.validate(); err != nil {
		return CallbackStatus{}, err
	}
	if !identifier.MatchString(dagKey) || runID <= 0 {
		return CallbackStatus{}, errors.New("invalid workflow key or run ID")
	}
	var record CallbackStatus
	err := s.db.QueryRowContext(ctx, `SELECT LOWER(HEX(c.event_id)), c.run_id, c.attempts, c.available_at, c.last_http_status, c.delivered_at
		FROM callback_deliveries c JOIN trigger_identities i ON i.id = c.identity_id
		JOIN workspaces w ON w.id = i.workspace_id JOIN dags d ON d.id = i.dag_id
		WHERE c.run_id = ? AND w.workspace_key = ? AND d.dag_key = ? AND i.actor = ?`, runID, scope.Workspace, dagKey, scope.Actor).
		Scan(&record.EventID, &record.RunID, &record.Attempts, &record.AvailableAt, &record.LastHTTPStatus, &record.DeliveredAt)
	if errors.Is(err, sql.ErrNoRows) {
		return CallbackStatus{}, ErrNotFound
	}
	return record, err
}
