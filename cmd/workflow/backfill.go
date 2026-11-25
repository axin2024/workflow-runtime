package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
)

func prepareBackfillCommand(ctx context.Context, scope store.Scope, args []string) (func(*store.Store) (any, error), error) {
	if args[0] == "backfill-preview" {
		if len(args) != 7 && len(args) != 8 {
			return nil, errors.New("usage: workflow backfill-preview WORKSPACE DAG SCHEDULE REVISION FROM THROUGH [INPUTS.json]")
		}
		revision, err := strconv.ParseUint(args[4], 10, 64)
		if err != nil || revision == 0 {
			return nil, errors.New("backfill revision must be a positive integer")
		}
		from, err := time.Parse(time.RFC3339Nano, args[5])
		if err != nil {
			return nil, errors.New("FROM must be an RFC3339 time")
		}
		through, err := time.Parse(time.RFC3339Nano, args[6])
		if err != nil {
			return nil, errors.New("THROUGH must be an RFC3339 time")
		}
		spec := store.BackfillSpec{Schedule: args[3], Revision: revision, From: from, Through: through}
		if len(args) == 8 {
			spec.Inputs, err = readSample(args[7])
			if err != nil {
				return nil, err
			}
			if len(spec.Inputs) == 0 {
				return nil, errors.New("backfill inputs must be a JSON object")
			}
		}
		return func(db *store.Store) (any, error) { return db.PreviewBackfill(ctx, scope, args[2], spec) }, nil
	}
	if args[0] == "backfill-confirm" && len(args) != 4 {
		return nil, errors.New("usage: workflow backfill-confirm WORKSPACE BATCH ORDINAL[,ORDINAL...]")
	}
	if args[0] != "backfill-confirm" && len(args) != 3 {
		return nil, errors.New("usage: workflow " + args[0] + " WORKSPACE BATCH")
	}
	id, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || id <= 0 {
		return nil, errors.New("batch ID must be a positive integer")
	}
	switch args[0] {
	case "backfill-get":
		return func(db *store.Store) (any, error) { return db.GetBackfill(ctx, scope, id) }, nil
	case "backfill-resume":
		return func(db *store.Store) (any, error) { return db.ResumeBackfill(ctx, scope, id) }, nil
	case "backfill-confirm":
		parts := strings.Split(args[3], ",")
		ordinals := make([]int, 0, len(parts))
		for _, part := range parts {
			ordinal, err := strconv.Atoi(part)
			if err != nil || ordinal <= 0 {
				return nil, errors.New("candidate ordinals must be positive integers separated by commas")
			}
			ordinals = append(ordinals, ordinal)
		}
		return func(db *store.Store) (any, error) { return db.ConfirmBackfill(ctx, scope, id, ordinals) }, nil
	default:
		return nil, errors.New("unknown backfill command")
	}
}
