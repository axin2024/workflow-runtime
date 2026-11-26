package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"time"
)

type BackfillReport struct {
	Format          string                `json:"format"`
	GeneratedAt     time.Time             `json:"generated_at"`
	GeneratedBy     string                `json:"generated_by"`
	SelectionDigest string                `json:"selection_digest,omitempty"`
	Batch           BackfillBatch         `json:"batch"`
	Runs            []BackfillRunEvidence `json:"runs"`
}

type BackfillRunEvidence struct {
	Ordinal   int              `json:"ordinal"`
	RunID     int64            `json:"run_id"`
	Revision  uint64           `json:"revision"`
	State     string           `json:"state"`
	Artifacts []OutputArtifact `json:"artifacts"`
}

func (s *Store) ExportBackfill(ctx context.Context, scope Scope, id int64) (BackfillReport, error) {
	if err := scope.validate(); err != nil {
		return BackfillReport{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return BackfillReport{}, err
	}
	defer tx.Rollback()
	record, err := loadBackfill(ctx, tx, scope, id)
	if err != nil {
		return BackfillReport{}, err
	}
	runs, err := backfillRunEvidence(ctx, tx, record)
	if err != nil {
		return BackfillReport{}, err
	}
	return BackfillReport{
		Format: "workflow-runtime/backfill-report-v1", GeneratedAt: time.Now().UTC(), GeneratedBy: scope.Actor,
		SelectionDigest: hex.EncodeToString(record.selection), Batch: record.BackfillBatch, Runs: runs,
	}, tx.Commit()
}

func backfillRunEvidence(ctx context.Context, tx *sql.Tx, record backfillRecord) ([]BackfillRunEvidence, error) {
	rows, err := tx.QueryContext(ctx, `SELECT i.ordinal, r.id, r.revision, r.state,
		n.node_key, n.lease_epoch, LOWER(HEX(n.output_digest))
		FROM backfill_items i JOIN runs r ON r.id = i.result_run_id AND r.workspace_id = i.workspace_id AND r.dag_id = i.dag_id
		LEFT JOIN node_runs n ON n.run_id = r.id AND n.output_digest IS NOT NULL
		WHERE i.workspace_id = ? AND i.dag_id = ? AND i.batch_id = ? ORDER BY i.ordinal, n.node_key`,
		record.workspaceID, record.dagID, record.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	runs := make([]BackfillRunEvidence, 0)
	for rows.Next() {
		var row BackfillRunEvidence
		var node, digest *string
		var epoch *uint64
		if err := rows.Scan(&row.Ordinal, &row.RunID, &row.Revision, &row.State, &node, &epoch, &digest); err != nil {
			return nil, err
		}
		if len(runs) == 0 || runs[len(runs)-1].Ordinal != row.Ordinal {
			row.Artifacts = make([]OutputArtifact, 0)
			runs = append(runs, row)
		}
		if node != nil && epoch != nil && digest != nil {
			run := &runs[len(runs)-1]
			run.Artifacts = append(run.Artifacts, OutputArtifact{RunID: row.RunID, Node: *node, Epoch: *epoch, SHA256: *digest})
		}
	}
	return runs, rows.Err()
}
