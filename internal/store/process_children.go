package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/axin2024/workflow-runtime/internal/process"
)

type ProcessChild struct {
	Group              uint64     `json:"iteration"`
	Ordinal            int        `json:"ordinal"`
	Run                int64      `json:"child_id"`
	Step               string     `json:"parent_step"`
	JoinState          string     `json:"join_state"`
	PinnedRevision     int        `json:"pinned_revision"`
	Status             string     `json:"child_status"`
	CancelRequestedAt  *time.Time `json:"cancel_requested_at,omitempty"`
	CancelDeliveredAt  *time.Time `json:"cancel_delivered_at,omitempty"`
	ReverseRequestedAt *time.Time `json:"reverse_requested_at,omitempty"`
	ReverseDeliveredAt *time.Time `json:"reverse_delivered_at,omitempty"`
	ReversalStatus     string     `json:"child_reversal_status,omitempty"`
}

type processChildCandidate struct {
	workspace, parent int64
	iteration         uint64
}

func processChildrenForRun(ctx context.Context, query interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}, run ProcessRun) ([]ProcessChild, error) {
	rows, err := query.QueryContext(ctx, `SELECT c.iteration,c.ordinal,c.child_run_id,g.step_key,g.status,
		c.pinned_revision,r.status,c.cancel_requested_at,c.cancel_delivered_at,c.reverse_requested_at,c.reverse_delivered_at,
		COALESCE(x.status,'') FROM process_child_links c
		JOIN process_child_groups g ON g.workspace_id=c.workspace_id AND g.parent_run_id=c.parent_run_id AND g.iteration=c.iteration
		JOIN process_runs r ON r.workspace_id=c.workspace_id AND r.id=c.child_run_id
		LEFT JOIN process_cancellations x ON x.workspace_id=c.workspace_id AND x.run_id=c.child_run_id
		WHERE c.workspace_id=? AND c.parent_run_id=? ORDER BY c.iteration,c.ordinal`, run.workspace, run.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ProcessChild{}
	for rows.Next() {
		var child ProcessChild
		if err = rows.Scan(&child.Group, &child.Ordinal, &child.Run, &child.Step, &child.JoinState, &child.PinnedRevision,
			&child.Status, &child.CancelRequestedAt, &child.CancelDeliveredAt, &child.ReverseRequestedAt, &child.ReverseDeliveredAt, &child.ReversalStatus); err != nil {
			return nil, err
		}
		out = append(out, child)
	}
	return out, rows.Err()
}

func queueProcessChildren(ctx context.Context, tx *sql.Tx, run ProcessRun, step process.Step) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO process_child_groups
		(workspace_id,parent_run_id,iteration,definition_rev,step_key,required_successes) VALUES (?,?,?,?,?,?)`,
		run.workspace, run.ID, run.Transition, run.Revision, step.Key, step.Successes)
	if err != nil {
		return err
	}
	return processEvent(ctx, tx, run, "scheduler", "children_queued", "",
		map[string]any{"step": step.Key, "count": len(step.Children), "required_successes": step.Successes})
}

func (s *Store) ScanProcessChildren(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > 256 {
		return 0, ErrInvalidProcess
	}
	delivered, err := s.deliverChildCancellations(ctx, limit)
	if err != nil {
		return delivered, err
	}
	rows, err := s.db.QueryContext(ctx, `(SELECT workspace_id,parent_run_id,iteration FROM process_child_groups
		WHERE status='starting' ORDER BY parent_run_id LIMIT ?)
		UNION ALL
		(SELECT DISTINCT g.workspace_id,g.parent_run_id,g.iteration FROM process_runs r
		JOIN process_child_links c ON c.workspace_id=r.workspace_id AND c.child_run_id=r.id
		JOIN process_child_groups g ON g.workspace_id=c.workspace_id AND g.parent_run_id=c.parent_run_id AND g.iteration=c.iteration
		WHERE r.status IN ('succeeded','failed','cancelled') AND c.observed_status IS NULL AND g.status='waiting'
		ORDER BY g.parent_run_id LIMIT ?)`, limit, limit)
	if err != nil {
		return delivered, err
	}
	defer rows.Close()
	var candidates []processChildCandidate
	for rows.Next() {
		var item processChildCandidate
		if err = rows.Scan(&item.workspace, &item.parent, &item.iteration); err != nil {
			return delivered, err
		}
		candidates = append(candidates, item)
	}
	if err = rows.Err(); err != nil {
		return delivered, err
	}
	if err = rows.Close(); err != nil {
		return delivered, err
	}
	for _, item := range candidates {
		changed, err := s.scanProcessChildGroup(ctx, item)
		if err != nil {
			return delivered, err
		}
		if changed {
			delivered++
		}
	}
	return delivered, nil
}

func (s *Store) scanProcessChildGroup(ctx context.Context, item processChildCandidate) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	run, err := loadProcessRun(ctx, tx, item.workspace, item.parent, true)
	if err != nil {
		return false, err
	}
	if run.Status != "waiting" || run.Transition != item.iteration {
		return false, tx.Commit()
	}
	var group string
	err = tx.QueryRowContext(ctx, `SELECT status FROM process_child_groups WHERE workspace_id=? AND parent_run_id=? AND iteration=?`,
		run.workspace, run.ID, run.Transition).Scan(&group)
	if err != nil {
		return false, err
	}
	if group != "starting" && group != "waiting" {
		return false, tx.Commit()
	}
	version, err := loadProcessVersion(ctx, tx, run.workspace, run.Key, run.Revision)
	if err != nil {
		return false, err
	}
	step, ok := version.Definition.Step(run.Step)
	if !ok || step.Kind != process.Children {
		return false, ErrProcessConflict
	}
	if group == "starting" {
		if err = startProcessChildren(ctx, tx, run, step); err != nil {
			return false, err
		}
	}
	changed, err := recordProcessChildOutcomes(ctx, tx, &run, version.Definition, step)
	if err != nil {
		return false, err
	}
	return group == "starting" || changed, tx.Commit()
}

func startProcessChildren(ctx context.Context, tx *sql.Tx, run ProcessRun, step process.Step) error {
	childIDs := make([]int64, 0, len(step.Children))
	input, err := processObject(run.Input)
	if err != nil {
		return err
	}
	for i, child := range step.Children {
		business := fmt.Sprintf("child:%d:%d:%d", run.ID, run.Transition, i+1)
		nested, err := startProcessInTx(ctx, tx, run.workspace, run.Actor, child.Process, child.Revision, business, input)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO process_child_links
			(workspace_id,parent_run_id,iteration,ordinal,child_run_id,pinned_revision) VALUES (?,?,?,?,?,?)`,
			run.workspace, run.ID, run.Transition, i+1, nested.ID, child.Revision)
		if err != nil {
			return err
		}
		childIDs = append(childIDs, nested.ID)
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_child_groups SET status='waiting'
		WHERE workspace_id=? AND parent_run_id=? AND iteration=?`, run.workspace, run.ID, run.Transition)
	if err != nil {
		return err
	}
	return processEvent(ctx, tx, run, "scheduler", "children_started", "", map[string]any{"child_ids": childIDs})
}

func recordProcessChildOutcomes(ctx context.Context, tx *sql.Tx, run *ProcessRun, definition process.Definition, step process.Step) (bool, error) {
	// Read child state without locking child runs. Cancelling a child is delivered in a separate transaction.
	rows, err := tx.QueryContext(ctx, `SELECT c.child_run_id,r.status,c.observed_status FROM process_child_links c
		JOIN process_runs r ON r.workspace_id=c.workspace_id AND r.id=c.child_run_id
		WHERE c.workspace_id=? AND c.parent_run_id=? AND c.iteration=? ORDER BY c.ordinal`, run.workspace, run.ID, run.Transition)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	type outcome struct {
		id     int64
		status string
	}
	var newOutcomes []outcome
	succeeded, pending := 0, 0
	for rows.Next() {
		var child outcome
		var observed *string
		if err = rows.Scan(&child.id, &child.status, &observed); err != nil {
			return false, err
		}
		if !terminalProcessStatus(child.status) {
			pending++
			continue
		}
		if child.status == "succeeded" {
			succeeded++
		}
		if observed == nil {
			newOutcomes = append(newOutcomes, child)
		}
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	if err = rows.Close(); err != nil {
		return false, err
	}
	for _, child := range newOutcomes {
		_, err = tx.ExecContext(ctx, `UPDATE process_child_links SET observed_status=? WHERE workspace_id=? AND child_run_id=?`,
			child.status, run.workspace, child.id)
		if err != nil {
			return false, err
		}
		if err = processEvent(ctx, tx, *run, "scheduler", "child_finished", fmt.Sprintf("child:%d", child.id),
			map[string]any{"child_id": child.id, "status": child.status}); err != nil {
			return false, err
		}
	}
	state, next := "succeeded", step.Next
	if succeeded < step.Successes {
		if succeeded+pending >= step.Successes {
			return len(newOutcomes) > 0, nil
		}
		state, next = "failed", step.OnFailure
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_child_groups SET status=?,finished_at=UTC_TIMESTAMP(6)
		WHERE workspace_id=? AND parent_run_id=? AND iteration=?`, state, run.workspace, run.ID, run.Transition)
	if err != nil {
		return false, err
	}
	if step.CancelWaiting {
		if err = requestChildCancellations(ctx, tx, *run, "scheduler", run.Transition); err != nil {
			return false, err
		}
	}
	key := fmt.Sprintf("join:%d:%d", run.ID, run.Transition)
	if err = processEvent(ctx, tx, *run, "scheduler", "children_joined", key, map[string]any{
		"state": state, "successes": succeeded, "pending": pending, "successor": next,
	}); err != nil {
		return false, err
	}
	run.Step = next
	run.Transition++
	return true, enterProcessStep(ctx, tx, run, definition)
}

func requestChildCancellations(ctx context.Context, tx *sql.Tx, run ProcessRun, actor string, group uint64) error {
	query := `UPDATE process_child_links SET cancel_actor=?,cancel_requested_at=UTC_TIMESTAMP(6)
		WHERE workspace_id=? AND parent_run_id=? AND observed_status IS NULL AND cancel_requested_at IS NULL`
	args := []any{actor, run.workspace, run.ID}
	if group != 0 {
		query += " AND iteration=?"
		args = append(args, group)
	}
	_, err := tx.ExecContext(ctx, query, args...)
	return err
}

func (s *Store) deliverChildCancellations(ctx context.Context, limit int) (int, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT workspace_id,child_run_id FROM process_child_links
		WHERE pending_cancel IS NOT NULL ORDER BY pending_cancel LIMIT ?`, limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	type notice struct{ workspace, child int64 }
	var pending []notice
	for rows.Next() {
		var item notice
		if err = rows.Scan(&item.workspace, &item.child); err != nil {
			return 0, err
		}
		pending = append(pending, item)
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	if err = rows.Close(); err != nil {
		return 0, err
	}
	delivered := 0
	for _, item := range pending {
		changed, err := s.deliverChildCancellation(ctx, item.workspace, item.child)
		if err != nil {
			return delivered, err
		}
		if changed {
			delivered++
		}
	}
	return delivered, nil
}

func (s *Store) deliverChildCancellation(ctx context.Context, workspace, child int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	run, err := loadProcessRun(ctx, tx, workspace, child, true)
	if err != nil {
		return false, err
	}
	var actor string
	var parent int64
	err = tx.QueryRowContext(ctx, `SELECT parent_run_id,cancel_actor FROM process_child_links
		WHERE workspace_id=? AND child_run_id=? AND pending_cancel IS NOT NULL`, workspace, child).Scan(&parent, &actor)
	if errors.Is(err, sql.ErrNoRows) {
		return false, tx.Commit()
	}
	if err != nil {
		return false, err
	}
	request := fmt.Sprintf("parent-cancel:%d:%d", parent, child)
	if err = cancelProcessInTx(ctx, tx, &run, actor, request, false); err != nil {
		return false, err
	}
	_, err = tx.ExecContext(ctx, `UPDATE process_child_links SET cancel_delivered_at=UTC_TIMESTAMP(6)
		WHERE workspace_id=? AND child_run_id=?`, workspace, child)
	if err != nil {
		return false, err
	}
	return true, tx.Commit()
}
