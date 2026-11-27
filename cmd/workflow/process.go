package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/axin2024/workflow-runtime/internal/process"
	"github.com/axin2024/workflow-runtime/internal/store"
)

func runProcessCommand(ctx context.Context, args []string, dsn string, scope store.Scope, output io.Writer) error {
	if len(args) < 2 {
		return errors.New("specify a workspace; run workflow --help for process commands")
	}
	db, err := store.Open(dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ready(ctx); err != nil {
		return err
	}
	result, err := processCommand(ctx, db, args, scope)
	return writeResult(output, result, err)
}

func processCommand(ctx context.Context, db *store.Store, args []string, scope store.Scope) (any, error) {
	switch args[0] {
	case "process-check":
		if len(args) != 3 {
			break
		}
		definition, err := readProcessDefinition(args[2])
		if err != nil {
			return nil, err
		}
		diagnostics, err := db.CheckProcessDefinition(ctx, scope, definition)
		if err != nil {
			return nil, err
		}
		if len(diagnostics) != 0 {
			return nil, fmt.Errorf("process definition: %s (%s)", diagnostics[0].Path, diagnostics[0].Code)
		}
		return definition, nil
	case "process-publish":
		if len(args) != 5 {
			break
		}
		expected, err := nonnegativeRevision(args[3])
		if err != nil {
			return nil, err
		}
		definition, err := readProcessDefinition(args[4])
		if err != nil {
			return nil, err
		}
		return db.PublishProcess(ctx, scope, args[2], expected, definition)
	case "process-start":
		if len(args) != 6 {
			break
		}
		revision, err := nonnegativeRevision(args[3])
		if err != nil {
			return nil, err
		}
		input, err := readSample(args[5])
		if err != nil {
			return nil, err
		}
		return db.StartProcess(ctx, scope, args[2], store.NewProcessRun{Revision: revision, Business: args[4], Input: input})
	case "process-show", "process-cancel", "process-migration-apply":
		if len(args) < 3 || len(args) > 4 {
			break
		}
		id, err := positiveRunID(args[2])
		if err != nil {
			return nil, err
		}
		if args[0] == "process-show" {
			var before int64
			if len(args) == 4 {
				before, err = positiveRunID(args[3])
				if err != nil {
					return nil, err
				}
			}
			return db.ProcessDetail(ctx, scope, id, before)
		}
		if args[0] == "process-cancel" && len(args) == 4 {
			return db.CancelProcess(ctx, scope, id, args[3])
		}
		if args[0] == "process-migration-apply" && len(args) == 3 {
			return db.ApplyProcessMigration(ctx, scope, id)
		}
	case "process-duty-grant":
		if len(args) != 4 {
			break
		}
		return map[string]string{"workspace": scope.Workspace, "actor": args[2], "duty_role": args[3]}, db.SetProcessDuty(ctx, scope, args[2], args[3])
	case "process-connector-grant", "process-connector-revoke":
		if len(args) < 4 || len(args) > 5 {
			break
		}
		identity := store.ProcessConnector{Scope: store.Scope{Workspace: scope.Workspace, Actor: args[3]}, Process: args[2]}
		if len(args) == 5 {
			identity.Operation = args[4]
			if identity.Operation == "" {
				return nil, store.ErrInvalidProcess
			}
		}
		if args[0] == "process-connector-revoke" {
			return identity, db.RevokeProcessConnector(ctx, identity)
		}
		return db.GrantProcessConnector(ctx, identity)
	case "process-migration-preview":
		if len(args) != 4 {
			break
		}
		raw, err := readSample(args[3])
		if err != nil {
			return nil, err
		}
		var manifest store.ProcessMigrationSpec
		if err = json.Unmarshal(raw, &manifest); err != nil {
			return nil, errors.New("invalid process migration manifest")
		}
		return db.PreviewProcessMigration(ctx, scope, args[2], manifest)
	case "process-intake":
		if len(args) != 3 || (args[2] != "open" && args[2] != "paused") {
			break
		}
		return db.SetProcessIntake(ctx, scope, args[2] == "paused")
	case "process-drain":
		if len(args) > 3 {
			break
		}
		var after int64
		if len(args) == 3 {
			var err error
			after, err = positiveRunID(args[2])
			if err != nil {
				return nil, err
			}
		}
		return db.ProcessDrain(ctx, scope, after)
	}
	return nil, fmt.Errorf("invalid arguments for %s; run workflow --help for usage", args[0])
}

func readProcessDefinition(path string) (process.Definition, error) {
	raw, err := readSample(path)
	if err != nil {
		return process.Definition{}, err
	}
	return process.Decode(raw)
}

func nonnegativeRevision(value string) (int, error) {
	revision, err := strconv.Atoi(value)
	if err != nil || revision < 0 {
		return 0, errors.New("definition revision must be a nonnegative integer")
	}
	return revision, nil
}
