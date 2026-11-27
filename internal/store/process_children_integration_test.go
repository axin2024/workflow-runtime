package store

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/axin2024/workflow-runtime/internal/process"
)

func TestMySQLProcessChildrenVersionQuorumAndCancellation(t *testing.T) {
	db, dsn := openTestStore(t)
	ctx := t.Context()
	scope := newTestWorkspace(t, db)
	child := process.Definition{Start: "respond", Steps: []process.Step{
		{Key: "respond", Kind: process.Signal, SignalKind: "bid", Next: "won", OnTimeout: "lost", AfterSeconds: 3600},
		{Key: "won", Kind: process.End, Result: "succeeded"}, {Key: "lost", Kind: process.End, Result: "failed"},
	}}
	if _, err := db.PublishProcess(ctx, scope, "supplier", 0, child); err != nil {
		t.Fatal(err)
	}
	parent, err := db.PublishProcess(ctx, scope, "choose-suppliers", 0, process.Definition{Start: "collect", Steps: []process.Step{
		{Key: "collect", Kind: process.Children, Successes: 2, CancelWaiting: true, Next: "quote", OnFailure: "rejected",
			Children: []process.Child{{Process: "supplier", Revision: 1}, {Process: "supplier", Revision: 1}, {Process: "supplier", Revision: 1}}},
		{Key: "quote", Kind: process.End, Result: "succeeded"}, {Key: "rejected", Kind: process.End, Result: "failed"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	child.Steps[0].SignalKind = "bid_v2"
	if updated, err := db.PublishProcess(ctx, scope, "supplier", 1, child); err != nil || updated.Revision != 2 {
		t.Fatalf("updated child: %+v %v", updated, err)
	}
	read := func(run ProcessRun) []ProcessChild {
		t.Helper()
		links, err := processChildrenForRun(ctx, db.db, run)
		if err != nil {
			t.Fatal(err)
		}
		return links
	}
	scan := func(db *Store) {
		t.Helper()
		if _, err := db.ScanProcessChildren(ctx, 128); err != nil {
			t.Fatal(err)
		}
	}
	start := func(business string) ProcessRun {
		t.Helper()
		run, err := db.StartProcess(ctx, scope, parent.Key, NewProcessRun{Business: business, Input: json.RawMessage(`{"warehouse":"west"}`)})
		if err != nil || run.Status != "waiting" || run.Transition != 1 {
			t.Fatalf("parent start: %+v %v", run, err)
		}
		scan(db)
		links := read(run)
		if len(links) != 3 {
			t.Fatalf("links: %+v", links)
		}
		for _, link := range links {
			if link.PinnedRevision != 1 || link.JoinState != "waiting" || link.Status != "waiting" {
				t.Fatalf("pinned child: %+v", link)
			}
		}
		return run
	}
	respond := func(id int64) {
		t.Helper()
		run, err := db.ProcessRun(ctx, scope, id)
		if err != nil || run.StartRevision != 1 || run.Revision != 1 {
			t.Fatalf("actual child revision: %+v %v", run, err)
		}
		result, err := db.SubmitProcessSignal(ctx, scope, run.ID, "bid-response", ProcessSignalInput{
			Event: "response-1", Correlation: run.Business, Kind: "bid", Payload: json.RawMessage(`{"amount":981}`),
		})
		if err != nil || result.Outcome != "consumed" {
			t.Fatalf("pinned child bid: %+v %v", result, err)
		}
	}
	restarted, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	quorum := start("PO-quorum")
	links := read(quorum)
	respond(links[0].Run)
	scan(db)
	if found, err := db.ProcessRun(ctx, scope, quorum.ID); err != nil || found.Status != "waiting" || found.Transition != 1 {
		t.Fatalf("joined before threshold: %+v %v", found, err)
	}
	respond(links[1].Run)
	results := make(chan error, 2)
	for _, worker := range []*Store{db, restarted} {
		go func() {
			_, err := worker.ScanProcessChildren(ctx, 128)
			results <- err
		}()
	}
	for range 2 {
		if err = <-results; err != nil {
			t.Fatal(err)
		}
	}
	scan(restarted)
	if found, err := db.ProcessRun(ctx, scope, quorum.ID); err != nil || found.Status != "succeeded" || found.Transition != 2 {
		t.Fatalf("failed to meet exactly once: %+v %v", found, err)
	}
	var joins int
	err = db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM process_events WHERE run_id=? AND kind='children_joined'`, quorum.ID).Scan(&joins)
	links = read(quorum)
	if err != nil || joins != 1 || links[2].Status != "cancelled" || links[2].CancelDeliveredAt == nil {
		t.Fatalf("quorum receipt: joins=%d links=%+v err=%v", joins, links, err)
	}
	stop := start("PO-cancel")
	links = read(stop)
	respond(links[0].Run)
	scan(db)
	if final, err := db.CancelProcess(ctx, scope, stop.ID, "stop-purchase"); err != nil || final.Status != "cancelling" {
		t.Fatalf("cancel parent: %+v %v", final, err)
	}
	if count, err := restarted.ScanProcessCompensations(ctx, 128); err != nil || count != 3 {
		t.Fatalf("durable child delivery: %d %v", count, err)
	}
	if err := restarted.scanProcessCancellation(ctx, stop.workspace, stop.ID); err != nil {
		t.Fatal(err)
	}
	if final, err := db.ProcessRun(ctx, scope, stop.ID); err != nil || final.Status != "cancelled" {
		t.Fatalf("finish parent after children: %+v %v", final, err)
	}
	links = read(stop)
	if links[0].Status != "succeeded" || links[0].CancelRequestedAt != nil {
		t.Fatalf("completed record modified: %+v", links[0])
	}
	for _, link := range links[1:] {
		var actor string
		err := db.db.QueryRowContext(ctx, `SELECT actor FROM process_events WHERE run_id=? AND kind='cancel_requested' AND request_key=?`,
			link.Run, fmt.Sprintf("parent-cancel:%d:%d", stop.ID, link.Run)).Scan(&actor)
		if err != nil || actor != scope.Actor || link.Status != "cancelled" || link.JoinState != "cancelled" || link.ReverseDeliveredAt == nil {
			t.Fatalf("unfinished child cancel: %+v, actor=%q %v", link, actor, err)
		}
	}
	failed := start("PO-failure")
	for _, link := range read(failed)[:2] {
		run, err := db.ProcessRun(ctx, scope, link.Run)
		if err != nil {
			t.Fatal(err)
		}
		wait := processTestWait(t, db, run)
		if changed, err := db.observeProcessTimer(ctx, timerCandidate{run.workspace, run.ID, wait.ID}, wait.Deadline); err != nil || !changed {
			t.Fatalf("child failed outcome: %v %v", changed, err)
		}
	}
	scan(db)
	if final, err := db.ProcessRun(ctx, scope, failed.ID); err != nil || final.Status != "failed" || final.Step != "rejected" {
		t.Fatalf("impossible quorum did not route to failure: %+v %v", final, err)
	}
	t.Logf("parent=%d join_events=%d, cancelled_parent=%d preserved_child=%d cancelled_children=%d,%d, failed_parent=%d",
		quorum.ID, joins, stop.ID, links[0].Run, links[1].Run, links[2].Run, failed.ID)
}
