package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var ErrNotFound = errors.New("not found")

var identifier = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// Scope identifies the workspace and actor resolved by the caller's trusted boundary.
type Scope struct {
	Workspace string
	Actor     string
}

func (s Scope) validate() error {
	if !identifier.MatchString(s.Workspace) || !identifier.MatchString(s.Actor) {
		return errors.New("workspace and actor must be lowercase identifiers of up to 64 characters")
	}
	return nil
}

type Store struct {
	db *sql.DB
}

func Open(dsn string) (*Store, error) {
	db, err := openDB(dsn, false)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(16)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(time.Minute)
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Ready(ctx context.Context) error {
	var version uint
	var dirty bool
	if err := s.db.QueryRowContext(ctx, "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if version != SchemaVersion || dirty {
		return fmt.Errorf("expected clean schema version %d", SchemaVersion)
	}
	return nil
}

func openDB(dsn string, migration bool) (*sql.DB, error) {
	if dsn == "" {
		return nil, errors.New("set WORKFLOW_MYSQL_DSN")
	}
	config, err := mysql.ParseDSN(dsn)
	if err != nil || config.DBName == "" {
		return nil, errors.New("invalid MySQL DSN or missing database name")
	}
	config.ParseTime = true
	config.Loc = time.UTC
	config.MultiStatements = migration
	config.InterpolateParams = false
	config.AllowAllFiles = false
	config.AllowCleartextPasswords = false
	config.Timeout = 5 * time.Second
	config.ReadTimeout = 10 * time.Second
	config.WriteTimeout = 10 * time.Second
	if config.Params == nil {
		config.Params = make(map[string]string)
	}
	config.Params["time_zone"] = "'+00:00'"
	connector, err := mysql.NewConnector(config)
	if err != nil {
		return nil, errors.New("invalid MySQL connection settings")
	}
	return sql.OpenDB(connector), nil
}

func (s *Store) CreateWorkspace(ctx context.Context, key string) error {
	if !identifier.MatchString(key) {
		return errors.New("workspace must be a lowercase identifier of up to 64 characters")
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO workspaces (workspace_key) VALUES (?)
		ON DUPLICATE KEY UPDATE workspace_key = workspace_key`, key)
	if err != nil {
		return fmt.Errorf("create workspace: %w", err)
	}
	return nil
}

func workspaceID(ctx context.Context, tx *sql.Tx, key string) (int64, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM workspaces WHERE workspace_key = ?`, key).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, fmt.Errorf("find workspace: %w", err)
	}
	return id, nil
}

func writeAudit(ctx context.Context, tx *sql.Tx, workspace, dag int64, run *int64, actor, kind string, details any) error {
	data, err := json.Marshal(details)
	if err != nil {
		return fmt.Errorf("encode audit details: %w", err)
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO audit_events (workspace_id, dag_id, run_id, actor, event_kind, details)
		VALUES (?, ?, ?, ?, ?, ?)`, workspace, dag, run, actor, kind, string(data))
	if err != nil {
		return fmt.Errorf("write audit event: %w", err)
	}
	return nil
}

// insertRows batches values while keeping the statement and tuple fixed by the caller.
func insertRows(ctx context.Context, tx *sql.Tx, statement, tuple string, width int, values []any) error {
	for offset := 0; offset < len(values); {
		end := min(offset+128*width, len(values))
		count := (end - offset) / width
		if _, err := tx.ExecContext(ctx, statement+strings.Repeat(tuple+",", count-1)+tuple, values[offset:end]...); err != nil {
			return err
		}
		offset = end
	}
	return nil
}
