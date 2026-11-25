package main

import (
	"context"
	"errors"
	"strconv"

	"github.com/axin2024/workflow-runtime/internal/store"
)

func prepareCapacity(ctx context.Context, scope store.Scope, args []string) (func(*store.Store) (any, error), error) {
	switch args[0] {
	case "pool-get":
		if len(args) != 2 {
			return nil, errors.New("usage: workflow pool-get POOL")
		}
		return func(db *store.Store) (any, error) { return db.GetPoolCapacity(ctx, args[1]) }, nil
	case "pool-put":
		if len(args) != 4 {
			return nil, errors.New("usage: workflow pool-put POOL EXPECTED_GENERATION SLOTS")
		}
		generation, limit, err := parseCapacity(args[2], args[3])
		if err != nil {
			return nil, err
		}
		if limit == nil {
			return nil, errors.New("a shared pool requires a numeric slot count")
		}
		return func(db *store.Store) (any, error) {
			return db.PutPoolCapacity(ctx, scope.Actor, args[1], generation, *limit)
		}, nil
	case "workspace-quota-get":
		if len(args) != 2 {
			return nil, errors.New("usage: workflow workspace-quota-get WORKSPACE")
		}
		return func(db *store.Store) (any, error) { return db.GetWorkspaceCapacity(ctx, scope) }, nil
	case "workspace-quota-put":
		if len(args) != 4 {
			return nil, errors.New("usage: workflow workspace-quota-put WORKSPACE EXPECTED_GENERATION SLOTS|unlimited")
		}
		generation, limit, err := parseCapacity(args[2], args[3])
		if err != nil {
			return nil, err
		}
		return func(db *store.Store) (any, error) { return db.PutWorkspaceCapacity(ctx, scope, generation, limit) }, nil
	case "queue-quota-get":
		if len(args) != 3 {
			return nil, errors.New("usage: workflow queue-quota-get WORKSPACE QUEUE")
		}
		return func(db *store.Store) (any, error) { return db.GetQueueCapacity(ctx, scope, args[2]) }, nil
	case "queue-quota-put":
		if len(args) < 5 || len(args) > 6 {
			return nil, errors.New("usage: workflow queue-quota-put WORKSPACE QUEUE EXPECTED_GENERATION SLOTS|unlimited [POOL]")
		}
		generation, limit, err := parseCapacity(args[3], args[4])
		if err != nil {
			return nil, err
		}
		spec := store.QueueCapacitySpec{MaxActive: limit}
		if len(args) == 6 {
			spec.Pool = args[5]
		}
		return func(db *store.Store) (any, error) {
			return db.PutQueueCapacity(ctx, scope, args[2], generation, spec)
		}, nil
	default:
		return nil, errors.New("unknown capacity command")
	}
}

func parseCapacity(generation, limit string) (uint64, *int, error) {
	expected, err := strconv.ParseUint(generation, 10, 64)
	if err != nil {
		return 0, nil, errors.New("expected generation must be a nonnegative integer")
	}
	if limit == "unlimited" {
		return expected, nil, nil
	}
	maxActive, err := strconv.Atoi(limit)
	if err != nil || maxActive < 0 || maxActive > 10000 {
		return 0, nil, errors.New("slots must be between 0 and 10000")
	}
	return expected, &maxActive, nil
}
