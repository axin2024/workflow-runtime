package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	samples "github.com/axin2024/workflow-runtime/examples"
	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/worker"
	"github.com/axin2024/workflow-runtime/internal/workerapi"
	"github.com/axin2024/workflow-runtime/internal/workflow"
	"github.com/go-sql-driver/mysql"
)

const demoWorkspace = "workflow-demo"

func runDemoInit(ctx context.Context, args []string, out, diagnostic io.Writer) error {
	if len(args) != 0 {
		return errors.New("usage: workflow demo-init")
	}
	dsn := os.Getenv("WORKFLOW_MYSQL_DSN")
	target, err := mysql.ParseDSN(dsn)
	if err != nil || target.DBName == "" {
		return errors.New("set WORKFLOW_MYSQL_DSN to the demo database")
	}
	if _, err := fmt.Fprintf(diagnostic, "Demo target: %s %q, database %q, workspace %q\n", target.Net, target.Addr, target.DBName, demoWorkspace); err != nil {
		return err
	}
	definition, inputs := samples.LeaseFailover()
	graph, err := workflow.Parse(definition)
	if err != nil {
		return err
	}
	if _, err := store.Migrate(dsn); err != nil {
		return err
	}
	db, err := store.Open(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	scope := store.Scope{Workspace: demoWorkspace, Actor: "demo-init"}
	if err := db.CreateWorkspace(ctx, scope.Workspace); err != nil {
		return err
	}
	version, err := db.GetVersion(ctx, scope, graph.Definition.Key, 1)
	if errors.Is(err, store.ErrNotFound) {
		version, err = db.Publish(ctx, scope, 0, definition)
		var conflict *store.RevisionConflict
		if errors.As(err, &conflict) {
			version, err = db.GetVersion(ctx, scope, graph.Definition.Key, 1)
		}
	}
	if err != nil {
		return err
	}
	if version.Graph.Digest != graph.Digest {
		return errors.New("the existing lease-failover revision 1 differs from the bundled example")
	}
	logicalAt := time.Date(2026, time.November, 1, 1, 0, 0, 0, time.UTC)
	result, err := db.Trigger(ctx, scope, store.TriggerRequest{DAG: graph.Definition.Key, Revision: version.Revision,
		Kind: "manual", Key: "first-dispatch", LogicalAt: &logicalAt, Inputs: inputs})
	return writeResult(out, result, err)
}

type demoWorkerEvent struct {
	store.LeaseRef
	Phase      string                   `json:"phase"`
	LeaseUntil *time.Time               `json:"lease_until,omitempty"`
	Outcome    string                   `json:"outcome,omitempty"`
	Completion *store.CompletionReceipt `json:"completion,omitempty"`
}

func runDemoWorker(ctx context.Context, args []string, in io.Reader, out, diagnostic io.Writer) error {
	flags := flag.NewFlagSet("demo-worker", flag.ContinueOnError)
	flags.SetOutput(diagnostic)
	endpoint := flags.String("endpoint", "http://127.0.0.1:8080", "worker API origin")
	seconds := flags.Int("lease-seconds", 10, "lease before the example can be taken over")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *seconds < 1 || *seconds > 60 {
		return errors.New("demo-worker requires a lease from 1 to 60 seconds")
	}
	client, err := workerapi.NewClient(*endpoint, os.Getenv("WORKFLOW_WORKER_TOKEN"))
	if err != nil {
		return err
	}
	task, err := client.Claim(ctx, store.ClaimRequest{Tasks: []string{"aggregate"}, LeaseSeconds: *seconds})
	if err != nil {
		return err
	}
	if task == nil || task.Queue != "failover" || task.Node != "aggregate" {
		return errors.New("no demo aggregate task available; start with the demo credentials and execute extract first")
	}
	encode := json.NewEncoder(out)
	if err := encode.Encode(demoWorkerEvent{LeaseRef: task.LeaseRef, Phase: "paused", LeaseUntil: &task.LeaseUntil}); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(diagnostic, "Worker A paused without heartbeats. Press Enter after Worker B completes aggregate."); err != nil {
		return err
	}
	resume := make(chan error, 1)
	go func() {
		_, err := bufio.NewReader(in).ReadString('\n')
		resume <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-resume:
		if err != nil {
			return fmt.Errorf("read resume signal: %w", err)
		}
	}
	result := worker.DispatchHandlers()["aggregate"](ctx, *task)
	receipt, err := client.Complete(ctx, store.CompletionRequest{Lease: task.LeaseRef, Success: result.FailureCode == "",
		Outputs: result.Outputs, FailureCode: result.FailureCode})
	event := demoWorkerEvent{LeaseRef: task.LeaseRef, Phase: "resumed"}
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		event.Outcome = "lease_lost"
	case err != nil:
		return err
	default:
		event.Outcome, event.Completion = "accepted", &receipt
	}
	return encode.Encode(event)
}
