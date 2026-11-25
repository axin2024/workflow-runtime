package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type Version struct {
	Revision    uint64            `json:"revision"`
	Publisher   string            `json:"publisher"`
	PublishedAt time.Time         `json:"published_at"`
	Graph       workflow.Compiled `json:"graph"`
}

type RevisionConflict struct {
	Active uint64
}

func (c *RevisionConflict) Error() string {
	return fmt.Sprintf("active revision is %d", c.Active)
}

// Publish increments the active revision if it still matches the author's base revision.
func (s *Store) Publish(ctx context.Context, scope Scope, expected uint64, raw []byte) (Version, error) {
	if err := scope.validate(); err != nil {
		return Version{}, err
	}
	graph, err := workflow.Parse(raw)
	if err != nil {
		return Version{}, err
	}
	definition, err := json.Marshal(graph.Definition)
	if err != nil {
		return Version{}, fmt.Errorf("encode published definition: %w", err)
	}
	if len(definition) > workflow.MaxDefinitionBytes {
		return Version{}, errors.New("normalized definition exceeds the size limit")
	}
	digest := sha256.Sum256(definition)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return Version{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return Version{}, err
	}
	if expected == 0 {
		_, err = tx.ExecContext(ctx, `INSERT INTO dags (workspace_id, dag_key) VALUES (?, ?)
			ON DUPLICATE KEY UPDATE dag_key = dag_key`, workspace, graph.Definition.Key)
		if err != nil {
			return Version{}, fmt.Errorf("create workflow: %w", err)
		}
	}
	dag, active, err := findDAG(ctx, tx, workspace, graph.Definition.Key, true)
	if errors.Is(err, ErrNotFound) {
		return Version{}, &RevisionConflict{Active: 0}
	}
	if err != nil {
		return Version{}, err
	}
	if expected != active {
		return Version{}, &RevisionConflict{Active: active}
	}
	revision := active + 1
	_, err = tx.ExecContext(ctx, `INSERT INTO dag_versions (dag_id, revision, digest, definition, publisher)
		VALUES (?, ?, ?, ?, ?)`, dag, revision, digest[:], string(definition), scope.Actor)
	if err != nil {
		return Version{}, fmt.Errorf("write workflow version: %w", err)
	}
	if err := putEdges(ctx, tx, dag, revision, graph.Definition); err != nil {
		return Version{}, err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE dags SET active_revision = ? WHERE id = ?`, revision, dag); err != nil {
		return Version{}, fmt.Errorf("activate workflow version: %w", err)
	}
	if err := writeAudit(ctx, tx, workspace, dag, nil, scope.Actor, "dag_published", map[string]any{
		"revision": revision, "digest": graph.Digest, "previous_revision": active,
	}); err != nil {
		return Version{}, err
	}
	version, err := loadVersion(ctx, tx, dag, revision)
	if err != nil {
		return Version{}, err
	}
	return version, tx.Commit()
}

// GetVersion returns the current revision when revision is zero.
func (s *Store) GetVersion(ctx context.Context, scope Scope, key string, revision uint64) (Version, error) {
	if err := scope.validate(); err != nil {
		return Version{}, err
	}
	if !identifier.MatchString(key) {
		return Version{}, errors.New("invalid workflow key")
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return Version{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return Version{}, err
	}
	dag, active, err := findDAG(ctx, tx, workspace, key, false)
	if err != nil {
		return Version{}, err
	}
	if revision == 0 {
		revision = active
	}
	version, err := loadVersion(ctx, tx, dag, revision)
	if err != nil {
		return Version{}, err
	}
	return version, tx.Commit()
}

func findDAG(ctx context.Context, tx *sql.Tx, workspace int64, key string, lock bool) (int64, uint64, error) {
	query := `SELECT id, active_revision FROM dags WHERE workspace_id = ? AND dag_key = ?`
	if lock {
		query += ` FOR UPDATE`
	}
	var id int64
	var active uint64
	err := tx.QueryRowContext(ctx, query, workspace, key).Scan(&id, &active)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, ErrNotFound
	}
	if err != nil {
		return 0, 0, fmt.Errorf("find workflow: %w", err)
	}
	return id, active, nil
}

func loadVersion(ctx context.Context, tx *sql.Tx, dag int64, revision uint64) (Version, error) {
	version := Version{Revision: revision}
	var digest, raw []byte
	err := tx.QueryRowContext(ctx, `SELECT digest, definition, publisher, published_at
		FROM dag_versions WHERE dag_id = ? AND revision = ?`, dag, revision).
		Scan(&digest, &raw, &version.Publisher, &version.PublishedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Version{}, ErrNotFound
	}
	if err != nil {
		return Version{}, fmt.Errorf("read workflow version: %w", err)
	}
	graph, err := workflow.Parse(raw)
	if err != nil {
		return Version{}, fmt.Errorf("invalid definition at revision %d: %w", revision, err)
	}
	if graph.Digest != hex.EncodeToString(digest) {
		return Version{}, fmt.Errorf("definition digest mismatch at revision %d", revision)
	}
	version.Graph = graph
	return version, nil
}

func putEdges(ctx context.Context, tx *sql.Tx, dag int64, revision uint64, definition workflow.Definition) error {
	var values []any
	for _, node := range definition.Nodes {
		for _, edge := range node.DependsOn {
			values = append(values, dag, revision, edge.Node, node.Key, edge.When, edge.Branch)
		}
	}
	if err := insertRows(ctx, tx, `INSERT INTO dag_edges (dag_id, revision, from_node, to_node, edge_condition, branch) VALUES `,
		`(?, ?, ?, ?, ?, ?)`, 6, values); err != nil {
		return fmt.Errorf("write dependency edges: %w", err)
	}
	return nil
}
