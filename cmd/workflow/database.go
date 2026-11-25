package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
)

func runDatabaseCommand(ctx context.Context, args []string, out io.Writer) error {
	dsn := os.Getenv("WORKFLOW_MYSQL_DSN")
	if args[0] == "migrate" {
		if len(args) != 1 {
			return errors.New("usage: workflow migrate")
		}
		version, err := store.Migrate(dsn)
		return writeResult(out, map[string]uint{"schema_version": version}, err)
	}
	var operation func(*store.Store) (any, error)
	actor := os.Getenv("WORKFLOW_ACTOR")
	if actor == "" {
		actor = "local-operator"
	}
	scope := store.Scope{Actor: actor}
	if args[0] == "scheduler" {
		return runScheduler(ctx, args[1:], dsn, actor, out, os.Stderr)
	}
	if len(args) > 1 {
		scope.Workspace = args[1]
	}
	switch args[0] {
	case "workspace-create":
		if len(args) != 2 {
			return errors.New("usage: workflow workspace-create WORKSPACE")
		}
		operation = func(db *store.Store) (any, error) {
			return map[string]string{"workspace": scope.Workspace}, db.CreateWorkspace(ctx, scope.Workspace)
		}
	case "publish":
		if len(args) != 4 {
			return errors.New("usage: workflow publish WORKSPACE EXPECTED_REVISION DEFINITION.json")
		}
		expected, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			return errors.New("expected revision must be a nonnegative integer")
		}
		definition, err := readSample(args[3])
		if err != nil {
			return err
		}
		operation = func(db *store.Store) (any, error) {
			return db.Publish(ctx, scope, expected, definition)
		}
	case "revision":
		if len(args) < 3 || len(args) > 4 {
			return errors.New("usage: workflow revision WORKSPACE DAG [REVISION]")
		}
		var revision uint64
		if len(args) == 4 {
			var err error
			revision, err = strconv.ParseUint(args[3], 10, 64)
			if err != nil {
				return errors.New("revision must be a nonnegative integer")
			}
		}
		operation = func(db *store.Store) (any, error) {
			return db.GetVersion(ctx, scope, args[2], revision)
		}
	case "trigger":
		workspace, trigger, err := parseTrigger(args[1:])
		if err != nil {
			return err
		}
		scope.Workspace = workspace
		operation = func(db *store.Store) (any, error) {
			return db.Trigger(ctx, scope, trigger)
		}
	case "run":
		if len(args) != 3 {
			return errors.New("usage: workflow run WORKSPACE ID")
		}
		id, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil || id <= 0 {
			return errors.New("run ID must be a positive integer")
		}
		operation = func(db *store.Store) (any, error) {
			return db.GetRun(ctx, scope, id)
		}
	case "schedule-put":
		if len(args) != 5 {
			return errors.New("usage: workflow schedule-put WORKSPACE DAG EXPECTED_GENERATION SCHEDULE.json")
		}
		generation, spec, err := parseSchedule(args[3], args[4])
		if err != nil {
			return err
		}
		operation = func(db *store.Store) (any, error) {
			return db.PutSchedule(ctx, scope, args[2], generation, spec)
		}
	case "schedule-get":
		if len(args) != 4 {
			return errors.New("usage: workflow schedule-get WORKSPACE DAG SCHEDULE")
		}
		operation = func(db *store.Store) (any, error) {
			return db.GetSchedule(ctx, scope, args[2], args[3])
		}
	case "backfill-preview", "backfill-confirm", "backfill-resume", "backfill-get":
		var err error
		operation, err = prepareBackfillCommand(ctx, scope, args)
		if err != nil {
			return err
		}
	default:
		return errors.New(usage)
	}
	db, err := store.Open(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Ready(ctx); err != nil {
		return err
	}
	value, err := operation(db)
	return writeResult(out, value, err)
}

func parseTrigger(args []string) (string, store.TriggerRequest, error) {
	flags := flag.NewFlagSet("trigger", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	kind := flags.String("kind", "manual", "trigger source")
	revision := flags.Uint64("revision", 0, "definition revision")
	logicalAt := flags.String("logical-at", "", "logical instant in RFC3339")
	if err := flags.Parse(args); err != nil {
		return "", store.TriggerRequest{}, err
	}
	if flags.NArg() != 4 {
		return "", store.TriggerRequest{}, errors.New("usage: workflow trigger [--kind manual|external] [--revision N] [--logical-at RFC3339] WORKSPACE DAG KEY INPUTS.json")
	}
	request := store.TriggerRequest{DAG: flags.Arg(1), Key: flags.Arg(2), Kind: *kind, Revision: *revision}
	if *logicalAt != "" {
		instant, err := time.Parse(time.RFC3339Nano, *logicalAt)
		if err != nil {
			return "", store.TriggerRequest{}, errors.New("logical time must be RFC3339")
		}
		request.LogicalAt = &instant
	}
	inputs, err := readSample(flags.Arg(3))
	request.Inputs = inputs
	return flags.Arg(0), request, err
}

func writeResult(out io.Writer, value any, err error) error {
	if err != nil {
		return err
	}
	return writeJSON(out, value)
}
