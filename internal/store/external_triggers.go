package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

const triggerTokenPrefix = "wtr_"

var (
	ErrTriggerUnauthorized   = errors.New("invalid or revoked trigger credential")
	ErrTriggerKeyUnavailable = errors.New("trigger key is already in use")
)

type TriggerGrant struct {
	Workspace string `json:"workspace"`
	DAG       string `json:"dag"`
	Actor     string `json:"actor"`
	Token     string `json:"token"`
}

type triggerIdentity struct {
	id, workspace, dag int64
	scope              Scope
	dagKey             string
}

type ExternalTriggerRequest struct {
	Key       string          `json:"key"`
	Revision  uint64          `json:"revision,omitempty"`
	LogicalAt *time.Time      `json:"logical_at,omitempty"`
	Inputs    json.RawMessage `json:"inputs"`
}

type TriggerReceipt struct {
	ID         int64      `json:"id"`
	DAG        string     `json:"dag"`
	Key        string     `json:"key"`
	Revision   uint64     `json:"revision"`
	State      string     `json:"state"`
	LogicalAt  time.Time  `json:"logical_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

type ExternalTriggerResult struct {
	Run      TriggerReceipt `json:"run"`
	Replayed bool           `json:"replayed"`
}

// GrantTrigger rotates a credential without changing the identity that owns its receipts.
func (s *Store) GrantTrigger(ctx context.Context, scope Scope, dagKey string) (TriggerGrant, error) {
	if err := scope.validate(); err != nil {
		return TriggerGrant{}, err
	}
	if !identifier.MatchString(dagKey) {
		return TriggerGrant{}, errors.New("invalid workflow key")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return TriggerGrant{}, err
	}
	defer tx.Rollback()
	workspace, dag, err := triggerScopeIDs(ctx, tx, scope, dagKey)
	if err != nil {
		return TriggerGrant{}, err
	}
	token, digest := newToken()
	updated, err := tx.ExecContext(ctx, `UPDATE trigger_identities SET token_digest = ?
		WHERE workspace_id = ? AND dag_id = ? AND actor = ?`, digest[:], workspace, dag, scope.Actor)
	if err != nil {
		return TriggerGrant{}, fmt.Errorf("rotate trigger credential: %w", err)
	}
	count, err := updated.RowsAffected()
	if err != nil {
		return TriggerGrant{}, err
	}
	if count == 0 {
		_, err := tx.ExecContext(ctx, `INSERT INTO trigger_identities (workspace_id, dag_id, actor, token_digest)
			VALUES (?, ?, ?, ?) AS incoming ON DUPLICATE KEY UPDATE token_digest = incoming.token_digest`, workspace, dag, scope.Actor, digest[:])
		if err != nil {
			return TriggerGrant{}, fmt.Errorf("save trigger credential: %w", err)
		}
	}
	return TriggerGrant{Workspace: scope.Workspace, DAG: dagKey, Actor: scope.Actor, Token: triggerTokenPrefix + token}, tx.Commit()
}

func (s *Store) RevokeTrigger(ctx context.Context, scope Scope, dagKey string) error {
	if err := scope.validate(); err != nil {
		return err
	}
	if !identifier.MatchString(dagKey) {
		return errors.New("invalid workflow key")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	workspace, dag, err := triggerScopeIDs(ctx, tx, scope, dagKey)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE trigger_identities SET token_digest = NULL
		WHERE workspace_id = ? AND dag_id = ? AND actor = ?`, workspace, dag, scope.Actor); err != nil {
		return err
	}
	return tx.Commit()
}

func triggerScopeIDs(ctx context.Context, tx *sql.Tx, scope Scope, dagKey string) (int64, int64, error) {
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return 0, 0, err
	}
	dag, _, err := findDAG(ctx, tx, workspace, dagKey, false)
	return workspace, dag, err
}

func (s *Store) ExternalTrigger(ctx context.Context, token string, request ExternalTriggerRequest) (ExternalTriggerResult, error) {
	digest, err := externalTokenDigest(token)
	if err != nil {
		return ExternalTriggerResult{}, err
	}
	result, err := s.trigger(ctx, Scope{}, TriggerRequest{Key: request.Key, Kind: "external", Revision: request.Revision,
		LogicalAt: request.LogicalAt, Inputs: request.Inputs}, &digest)
	if err != nil {
		return ExternalTriggerResult{}, err
	}
	run := result.Run
	return ExternalTriggerResult{Run: TriggerReceipt{ID: run.ID, DAG: run.DAG, Key: run.TriggerKey, Revision: run.Revision,
		State: run.State, LogicalAt: run.LogicalAt, FinishedAt: run.FinishedAt}, Replayed: result.Replayed}, nil
}

func (s *Store) ExternalReceipt(ctx context.Context, token string, id int64, key string) (TriggerReceipt, error) {
	digest, err := externalTokenDigest(token)
	if err != nil {
		return TriggerReceipt{}, err
	}
	if id <= 0 && !triggerKey.MatchString(key) {
		return TriggerReceipt{}, &workflow.Issue{Path: "key", Message: "expected a valid trigger key"}
	}
	query := `SELECT r.id, r.trigger_key, r.revision, r.state, r.logical_at, r.finished_at, d.dag_key
		FROM trigger_identities i JOIN dags d ON d.id = i.dag_id
		JOIN runs r ON r.workspace_id = i.workspace_id AND r.dag_id = i.dag_id
		JOIN trigger_receipts receipt ON receipt.run_id = r.id AND receipt.identity_id = i.id
		WHERE i.token_digest = ? AND `
	var field string
	var lookup any
	if id > 0 {
		field, lookup = "r.id = ?", id
	} else {
		field, lookup = "r.trigger_key = ?", key
	}
	// Read authentication and ownership from the same snapshot, including a missing receipt.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return TriggerReceipt{}, err
	}
	defer tx.Rollback()
	var receipt TriggerReceipt
	err = tx.QueryRowContext(ctx, query+field, digest[:], lookup).Scan(&receipt.ID, &receipt.Key, &receipt.Revision,
		&receipt.State, &receipt.LogicalAt, &receipt.FinishedAt, &receipt.DAG)
	if errors.Is(err, sql.ErrNoRows) {
		var exists bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM trigger_identities WHERE token_digest = ?)`, digest[:]).Scan(&exists); err != nil {
			return TriggerReceipt{}, err
		}
		if !exists {
			return TriggerReceipt{}, ErrTriggerUnauthorized
		}
		return TriggerReceipt{}, ErrNotFound
	}
	if err != nil {
		return TriggerReceipt{}, err
	}
	return receipt, tx.Commit()
}

func externalTokenDigest(token string) ([32]byte, error) {
	value, prefixed := strings.CutPrefix(token, triggerTokenPrefix)
	digest, valid := tokenDigest(value)
	if !prefixed || !valid {
		return [32]byte{}, ErrTriggerUnauthorized
	}
	return digest, nil
}

func lockTriggerIdentity(ctx context.Context, tx *sql.Tx, digest [32]byte) (triggerIdentity, error) {
	var identity triggerIdentity
	err := tx.QueryRowContext(ctx, `SELECT i.id, i.workspace_id, i.dag_id, w.workspace_key, i.actor, d.dag_key
		FROM trigger_identities i JOIN workspaces w ON w.id = i.workspace_id JOIN dags d ON d.id = i.dag_id
		WHERE i.token_digest = ? FOR UPDATE OF i`, digest[:]).Scan(&identity.id, &identity.workspace, &identity.dag,
		&identity.scope.Workspace, &identity.scope.Actor, &identity.dagKey)
	if errors.Is(err, sql.ErrNoRows) {
		return triggerIdentity{}, ErrTriggerUnauthorized
	}
	return identity, err
}
