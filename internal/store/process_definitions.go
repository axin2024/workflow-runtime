package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

var ErrProcessConflict = errors.New("process request conflicts with the recorded state")
var ErrInvalidProcess = errors.New("invalid process request")

type ProcessValidationError struct {
	Diagnostics []process.Diagnostic `json:"validation_errors"`
}

func (e *ProcessValidationError) Error() string { return "process definition has validation errors" }

type ProcessVersion struct {
	ID          int64              `json:"process_id"`
	Key         string             `json:"key"`
	Revision    int                `json:"definition_rev"`
	Author      string             `json:"author"`
	PublishedAt time.Time          `json:"published_at"`
	Definition  process.Definition `json:"definition"`
}

func (s *Store) PublishProcess(ctx context.Context, scope Scope, key string, expected int, definition process.Definition) (ProcessVersion, error) {
	var out ProcessVersion
	if scope.validate() != nil || !process.ValidKey(key) || expected < 0 {
		return out, ErrInvalidProcess
	}
	if diagnostics := definition.Validate(); len(diagnostics) != 0 {
		return out, &ProcessValidationError{Diagnostics: diagnostics}
	}
	data, err := json.Marshal(definition)
	if err != nil {
		return out, err
	}
	if len(data) > process.MaxDefinitionBytes {
		return out, ErrInvalidProcess
	}
	digest := sha256.Sum256(data)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return out, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO process_names (workspace_id,process_key) VALUES (?,?)
		ON DUPLICATE KEY UPDATE process_key=process_names.process_key`, workspace, key)
	if err != nil {
		return out, err
	}
	var id int64
	var revision int
	err = tx.QueryRowContext(ctx, `SELECT id,current_revision FROM process_names WHERE workspace_id=? AND process_key=? FOR UPDATE`,
		workspace, key).Scan(&id, &revision)
	if err != nil {
		return out, err
	}
	if revision != 0 && (expected == revision || expected == revision-1) {
		var currentDigest []byte
		err = tx.QueryRowContext(ctx, `SELECT digest FROM process_definitions WHERE workspace_id=? AND process_id=? AND revision=?`,
			workspace, id, revision).Scan(&currentDigest)
		if err != nil {
			return out, err
		}
		if bytes.Equal(digest[:], currentDigest) {
			out, err = loadProcessVersion(ctx, tx, workspace, key, revision)
			if err != nil {
				return out, err
			}
			return out, tx.Commit()
		}
	}
	if revision != expected {
		return out, ErrProcessConflict
	}
	diagnostics, err := validateProcessChildren(ctx, tx, workspace, definition)
	if err != nil {
		return out, err
	}
	if len(diagnostics) != 0 {
		return out, &ProcessValidationError{Diagnostics: diagnostics}
	}
	revision++
	_, err = tx.ExecContext(ctx, `INSERT INTO process_definitions (workspace_id,process_id,revision,definition,digest,author)
		VALUES (?,?,?,?,?,?)`, workspace, id, revision, data, digest[:], scope.Actor)
	if err != nil {
		return out, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE process_names SET current_revision=? WHERE id=?`, revision, id); err != nil {
		return out, err
	}
	out, err = loadProcessVersion(ctx, tx, workspace, key, revision)
	if err != nil {
		return out, err
	}
	return out, tx.Commit()
}

func validateProcessChildren(ctx context.Context, tx *sql.Tx, workspace int64, definition process.Definition) ([]process.Diagnostic, error) {
	var diagnostics []process.Diagnostic
	for i, step := range definition.Steps {
		if step.Kind != process.Children {
			continue
		}
		for j, child := range step.Children {
			var present bool
			err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM process_definitions d
				JOIN process_names n ON n.workspace_id=d.workspace_id AND n.id=d.process_id
				WHERE n.workspace_id=? AND n.process_key=? AND d.revision=?)`,
				workspace, child.Process, child.Revision).Scan(&present)
			if err != nil {
				return nil, err
			}
			if !present {
				diagnostics = append(diagnostics, process.Diagnostic{Path: fmt.Sprintf("steps[%d].children[%d]", i, j), Code: "child_revision_not_found"})
			}
		}
	}
	return diagnostics, nil
}

func loadProcessVersion(ctx context.Context, tx *sql.Tx, workspace int64, key string, revision int) (ProcessVersion, error) {
	var out ProcessVersion
	var raw []byte
	err := tx.QueryRowContext(ctx, `SELECT n.id,n.process_key,d.revision,d.author,d.published_at,d.definition
		FROM process_names n JOIN process_definitions d ON d.workspace_id=n.workspace_id AND d.process_id=n.id
		WHERE n.workspace_id=? AND n.process_key=? AND d.revision=IF(?=0,n.current_revision,?)`,
		workspace, key, revision, revision).Scan(&out.ID, &out.Key, &out.Revision, &out.Author, &out.PublishedAt, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return out, ErrNotFound
	}
	if err == nil {
		out.Definition, err = process.Decode(raw)
	}
	return out, err
}

func (s *Store) ProcessDefinition(ctx context.Context, scope Scope, key string, revision int) (ProcessVersion, error) {
	if scope.validate() != nil || !process.ValidKey(key) || revision < 0 {
		return ProcessVersion{}, ErrInvalidProcess
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return ProcessVersion{}, err
	}
	defer tx.Rollback()
	workspace, err := workspaceID(ctx, tx, scope.Workspace)
	if err != nil {
		return ProcessVersion{}, err
	}
	out, err := loadProcessVersion(ctx, tx, workspace, key, revision)
	if err != nil {
		return ProcessVersion{}, err
	}
	return out, tx.Commit()
}
