package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"time"
)

type ConsoleRole string

const (
	ConsoleAuthor          ConsoleRole = "author"
	ConsoleOperator        ConsoleRole = "operator"
	ConsoleViewer          ConsoleRole = "viewer"
	ConsoleSessionLifetime             = 8 * time.Hour
)

var ErrConsoleUnauthorized = errors.New("invalid or expired console credentials")

type ConsoleGrant struct {
	Workspace string      `json:"workspace"`
	Actor     string      `json:"actor"`
	Role      ConsoleRole `json:"role"`
	Token     string      `json:"token"`
}

type ConsoleIdentity struct {
	Scope
	Role ConsoleRole
}

// GrantConsole rotates the sign-in credential and removes existing sessions.
func (s *Store) GrantConsole(ctx context.Context, scope Scope, role ConsoleRole) (ConsoleGrant, error) {
	if err := scope.validate(); err != nil {
		return ConsoleGrant{}, err
	}
	if role != ConsoleAuthor && role != ConsoleOperator && role != ConsoleViewer {
		return ConsoleGrant{}, errors.New("console role must be author, operator, or viewer")
	}
	token, digest := newToken()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return ConsoleGrant{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return ConsoleGrant{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO console_identities (workspace_id, actor, role, token_digest)
		VALUES (?, ?, ?, ?) AS incoming ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(console_identities.id),
		role = incoming.role, token_digest = incoming.token_digest`, workspace, scope.Actor, role, digest[:])
	if err != nil {
		return ConsoleGrant{}, err
	}
	identity, err := result.LastInsertId()
	if err != nil {
		return ConsoleGrant{}, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM console_sessions WHERE identity_id = ?`, identity); err != nil {
		return ConsoleGrant{}, err
	}
	if err := tx.Commit(); err != nil {
		return ConsoleGrant{}, err
	}
	return ConsoleGrant{Workspace: scope.Workspace, Actor: scope.Actor, Role: role, Token: token}, nil
}

func (s *Store) RevokeConsole(ctx context.Context, scope Scope) error {
	if err := scope.validate(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE i FROM console_identities i JOIN workspaces w ON w.id = i.workspace_id
		WHERE w.workspace_key = ? AND i.actor = ?`, scope.Workspace, scope.Actor)
	return err
}

func (s *Store) StartConsoleSession(ctx context.Context, credential string) (string, error) {
	digest, err := consoleDigest(credential)
	if err != nil {
		return "", err
	}
	token, sessionDigest := newToken()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	var id int64
	err = tx.QueryRowContext(ctx, `SELECT id FROM console_identities WHERE token_digest = ? FOR UPDATE`, digest[:]).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrConsoleUnauthorized
	}
	if err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM console_sessions WHERE identity_id = ? AND expires_at <= UTC_TIMESTAMP(6)`, id); err != nil {
		return "", err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO console_sessions (identity_id, token_digest, expires_at)
		VALUES (?, ?, TIMESTAMPADD(SECOND, ?, UTC_TIMESTAMP(6)))`, id, sessionDigest[:], int(ConsoleSessionLifetime.Seconds())); err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return token, nil
}

func (s *Store) ConsoleIdentity(ctx context.Context, token string) (ConsoleIdentity, error) {
	digest, err := consoleDigest(token)
	if err != nil {
		return ConsoleIdentity{}, err
	}
	var identity ConsoleIdentity
	err = s.db.QueryRowContext(ctx, `SELECT w.workspace_key, i.actor, i.role FROM console_sessions s
		JOIN console_identities i ON i.id = s.identity_id JOIN workspaces w ON w.id = i.workspace_id
		WHERE s.token_digest = ? AND s.expires_at > UTC_TIMESTAMP(6)`, digest[:]).Scan(&identity.Workspace, &identity.Actor, &identity.Role)
	if errors.Is(err, sql.ErrNoRows) {
		return ConsoleIdentity{}, ErrConsoleUnauthorized
	}
	return identity, err
}

func (s *Store) EndConsoleSession(ctx context.Context, token string) error {
	digest, err := consoleDigest(token)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM console_sessions WHERE token_digest = ?`, digest[:])
	return err
}

func newToken() (string, [32]byte) {
	var secret [32]byte
	rand.Read(secret[:])
	return base64.RawURLEncoding.EncodeToString(secret[:]), sha256.Sum256(secret[:])
}

func consoleDigest(token string) ([32]byte, error) {
	digest, ok := tokenDigest(token)
	if !ok {
		return [32]byte{}, ErrConsoleUnauthorized
	}
	return digest, nil
}

func tokenDigest(token string) ([32]byte, bool) {
	if len(token) != base64.RawURLEncoding.EncodedLen(32) {
		return [32]byte{}, false
	}
	secret, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(secret) != 32 {
		return [32]byte{}, false
	}
	return sha256.Sum256(secret), true
}
