package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"

	"github.com/axin2024/workflow-runtime/internal/store"
)

func prepareRunControl(ctx context.Context, scope store.Scope, args []string) (func(*store.Store) (any, error), error) {
	switch args[0] {
	case "run-cancel":
		flags := flag.NewFlagSet("run-cancel", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		grace := flags.Int("grace-seconds", 15, "worker cancellation grace in seconds")
		if err := flags.Parse(args[1:]); err != nil {
			return nil, err
		}
		if flags.NArg() != 4 {
			return nil, errors.New("usage: workflow run-cancel [--grace-seconds N] WORKSPACE ID KEY REASON")
		}
		scope.Workspace = flags.Arg(0)
		id, err := positiveRunID(flags.Arg(1))
		if err != nil {
			return nil, err
		}
		request := store.CancelRequest{Key: flags.Arg(2), Reason: flags.Arg(3), GraceSeconds: *grace}
		return func(db *store.Store) (any, error) { return db.CancelRun(ctx, scope, id, request) }, nil
	case "run-resolve":
		if len(args) != 5 {
			return nil, errors.New("usage: workflow run-resolve WORKSPACE ID NODE RESOLUTION.json")
		}
		id, err := positiveRunID(args[2])
		if err != nil {
			return nil, err
		}
		raw, err := readSample(args[4])
		if err != nil {
			return nil, err
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		var resolution store.ResolveRequest
		if err := decoder.Decode(&resolution); err != nil {
			return nil, fmt.Errorf("decode external resolution: %w", err)
		}
		if decoder.Decode(new(json.RawMessage)) != io.EOF {
			return nil, errors.New("expected one resolution object")
		}
		return func(db *store.Store) (any, error) { return db.ResolveExternal(ctx, scope, id, args[3], resolution) }, nil
	default:
		limit := 128
		if len(args) == 2 {
			var err error
			limit, err = strconv.Atoi(args[1])
			if err != nil {
				return nil, errors.New("sweep limit must be an integer")
			}
		} else if len(args) != 1 {
			return nil, errors.New("usage: workflow execution-sweep [LIMIT]")
		}
		return func(db *store.Store) (any, error) { return db.SweepExecution(ctx, limit) }, nil
	}
}

func positiveRunID(value string) (int64, error) {
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return 0, errors.New("run ID must be a positive integer")
	}
	return id, nil
}
