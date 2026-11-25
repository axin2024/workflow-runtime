package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 || !longRunningCommand(os.Args[1]) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func longRunningCommand(name string) bool {
	return name == "scheduler" || name == "worker-api" || name == "worker" || name == "console"
}

const usage = `Usage:
  workflow validate DEFINITION.json [INPUTS.json]
  workflow migrate
  workflow workspace-create WORKSPACE
  workflow publish WORKSPACE EXPECTED_REVISION DEFINITION.json
  workflow revision WORKSPACE DAG [REVISION]
  workflow trigger [--kind manual|external] [--revision N] [--logical-at RFC3339] WORKSPACE DAG KEY INPUTS.json
  workflow run WORKSPACE ID
  workflow run-cancel [--grace-seconds N] WORKSPACE ID KEY REASON
  workflow run-rerun-preview WORKSPACE ID NODE[,NODE...]
  workflow run-rerun WORKSPACE ID KEY NODE[,NODE...]
  workflow run-export WORKSPACE ID
  workflow run-resolve WORKSPACE ID NODE RESOLUTION.json
  workflow execution-sweep [LIMIT]
  workflow schedule-put WORKSPACE DAG EXPECTED_GENERATION SCHEDULE.json
  workflow schedule-get WORKSPACE DAG SCHEDULE
  workflow scheduler [--once] [--at RFC3339] [--interval DURATION] [--limit N]
  workflow backfill-preview WORKSPACE DAG SCHEDULE REVISION FROM THROUGH [INPUTS.json]
  workflow backfill-confirm WORKSPACE BATCH ORDINAL[,ORDINAL...]
  workflow backfill-resume WORKSPACE BATCH
  workflow backfill-get WORKSPACE BATCH
  workflow worker-register WORKSPACE NAME MAX_ACTIVE QUEUE:TASK [...]
  workflow pool-put POOL EXPECTED_GENERATION SLOTS
  workflow pool-get POOL
  workflow workspace-quota-put WORKSPACE EXPECTED_GENERATION SLOTS|unlimited
  workflow workspace-quota-get WORKSPACE
  workflow queue-quota-put WORKSPACE QUEUE EXPECTED_GENERATION SLOTS|unlimited [POOL]
  workflow queue-quota-get WORKSPACE QUEUE
  workflow console-grant WORKSPACE ACTOR author|operator|viewer
  workflow console-revoke WORKSPACE ACTOR
  workflow console [--listen 127.0.0.1:8081] [--secure-cookie]
  workflow worker-api [--listen 127.0.0.1:8080]
  workflow worker [--once] [--endpoint URL] [--interval DURATION] [--lease-seconds N]`

func run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "--help" || args[0] == "-h")) {
		_, err := fmt.Fprintln(out, usage)
		return err
	}
	switch args[0] {
	case "worker":
		return runWorker(ctx, args[1:], out, os.Stderr)
	case "worker-api":
		return runWorkerAPI(ctx, args[1:])
	case "console":
		return runConsole(ctx, args[1:])
	case "validate":
	default:
		return runDatabaseCommand(ctx, args, out)
	}
	if len(args) < 2 || len(args) > 3 {
		return errors.New("usage: workflow validate DEFINITION.json [INPUTS.json]")
	}
	data, err := readSample(args[1])
	if err != nil {
		return err
	}
	compiled, err := workflow.Parse(data)
	if err != nil {
		return err
	}
	if len(args) == 3 {
		inputs, err := readSample(args[2])
		if err != nil {
			return err
		}
		var values map[string]json.RawMessage
		if err := json.Unmarshal(inputs, &values); err != nil || values == nil {
			return errors.New("inputs must be a JSON object")
		}
		if err := workflow.ValidateValues("inputs", compiled.Definition.Inputs, values); err != nil {
			return err
		}
	}
	return writeJSON(out, compiled)
}

func writeJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func readSample(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, workflow.MaxDefinitionBytes+1))
	if err == nil && len(data) > workflow.MaxDefinitionBytes {
		err = fmt.Errorf("%s exceeds %d bytes", path, workflow.MaxDefinitionBytes)
	}
	return data, err
}
