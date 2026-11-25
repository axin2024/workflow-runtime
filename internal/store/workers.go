package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	ErrWorkerUnauthorized = errors.New("worker credentials are missing or invalid")
	ErrWorkerExists       = errors.New("worker key already exists in this workspace")
	ErrLeaseLost          = errors.New("lease is no longer current")
	ErrCompletionConflict = errors.New("attempt already has a different completion")
)

type WorkerPermission struct {
	Queue string `json:"queue"`
	Task  string `json:"task"`
}

type WorkerRegistration struct {
	ID        int64  `json:"id"`
	Workspace string `json:"workspace"`
	Key       string `json:"key"`
	Token     string `json:"token"`
}

type workerIdentity struct {
	id, workspace int64
	key           string
	maxActive     int
}

func (s *Store) RegisterWorker(ctx context.Context, scope Scope, key string, maxActive int, permissions []WorkerPermission) (WorkerRegistration, error) {
	if err := scope.validate(); err != nil {
		return WorkerRegistration{}, err
	}
	if !identifier.MatchString(key) || maxActive < 1 || maxActive > 64 || len(permissions) == 0 || len(permissions) > 128 {
		return WorkerRegistration{}, errors.New("worker requires a valid key, 1 to 64 active leases and 1 to 128 permissions")
	}
	seen := make(map[WorkerPermission]bool, len(permissions))
	for _, permission := range permissions {
		if !identifier.MatchString(permission.Queue) || !identifier.MatchString(permission.Task) || seen[permission] {
			return WorkerRegistration{}, errors.New("worker permissions must be distinct queue and task identifiers")
		}
		seen[permission] = true
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return WorkerRegistration{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(secret[:])
	digest := sha256.Sum256(secret[:])
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkerRegistration{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return WorkerRegistration{}, err
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO workers (workspace_id, worker_key, token_digest, max_active, created_by)
		VALUES (?, ?, ?, ?, ?)`, workspace, key, digest[:], maxActive, scope.Actor)
	if err != nil {
		var server *mysql.MySQLError
		if errors.As(err, &server) && server.Number == 1062 {
			return WorkerRegistration{}, ErrWorkerExists
		}
		return WorkerRegistration{}, fmt.Errorf("register worker: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return WorkerRegistration{}, err
	}
	values := make([]any, 0, len(permissions)*3)
	for _, permission := range permissions {
		values = append(values, id, permission.Queue, permission.Task)
	}
	if err := insertRows(ctx, tx, `INSERT INTO worker_permissions (worker_id, queue, task) VALUES `, `(?, ?, ?)`, 3, values); err != nil {
		return WorkerRegistration{}, fmt.Errorf("save worker permissions: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return WorkerRegistration{}, err
	}
	return WorkerRegistration{ID: id, Workspace: scope.Workspace, Key: key, Token: token}, nil
}

func lockWorker(ctx context.Context, tx *sql.Tx, token string) (workerIdentity, error) {
	if len(token) != base64.RawURLEncoding.EncodedLen(32) {
		return workerIdentity{}, ErrWorkerUnauthorized
	}
	secret, err := base64.RawURLEncoding.Strict().DecodeString(token)
	if err != nil || len(secret) != 32 {
		return workerIdentity{}, ErrWorkerUnauthorized
	}
	digest := sha256.Sum256(secret)
	var worker workerIdentity
	err = tx.QueryRowContext(ctx, `SELECT id, workspace_id, worker_key, max_active FROM workers
		WHERE token_digest = ? AND enabled = TRUE FOR UPDATE`, digest[:]).Scan(&worker.id, &worker.workspace, &worker.key, &worker.maxActive)
	if errors.Is(err, sql.ErrNoRows) {
		return workerIdentity{}, ErrWorkerUnauthorized
	}
	if err != nil {
		return workerIdentity{}, fmt.Errorf("authenticate worker: %w", err)
	}
	return worker, nil
}

func databaseTime(ctx context.Context, tx *sql.Tx) (time.Time, error) {
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT UTC_TIMESTAMP(6)`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database clock: %w", err)
	}
	return now, nil
}
