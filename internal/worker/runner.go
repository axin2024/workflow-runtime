package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workerapi"
)

type Result struct {
	Outputs     json.RawMessage
	Branch      string
	FailureCode string
}

type Handler func(context.Context, store.ClaimedTask) Result

type Runner struct {
	api          API
	handlers     map[string]Handler
	tasks        []string
	leaseSeconds int
}

// API is the client-side transport used by an executor.
type API interface {
	Claim(context.Context, store.ClaimRequest) (*store.ClaimedTask, error)
	Renew(context.Context, store.RenewRequest) (store.LeaseRenewal, error)
	Complete(context.Context, store.CompletionRequest) (store.CompletionReceipt, error)
}

func New(api API, handlers map[string]Handler, leaseSeconds int) (*Runner, error) {
	if len(handlers) == 0 || len(handlers) > 128 || leaseSeconds < 1 || leaseSeconds > 120 {
		return nil, errors.New("worker needs 1 to 128 handlers and a lease of 1 to 120 seconds")
	}
	tasks := make([]string, 0, len(handlers))
	for task, handler := range handlers {
		if handler == nil {
			return nil, fmt.Errorf("missing handler for %s", task)
		}
		tasks = append(tasks, task)
	}
	slices.Sort(tasks)
	return &Runner{api: api, handlers: handlers, tasks: tasks, leaseSeconds: leaseSeconds}, nil
}

func (r *Runner) RunOne(ctx context.Context) (*store.CompletionReceipt, error) {
	claimStarted := time.Now()
	task, err := r.api.Claim(ctx, store.ClaimRequest{Tasks: r.tasks, LeaseSeconds: r.leaseSeconds})
	if err != nil || task == nil {
		return nil, err
	}
	handler := r.handlers[task.Task]
	if handler == nil {
		return nil, fmt.Errorf("worker received unsupported task %s", task.Task)
	}
	// Convert server durations to conservative local deadlines. Using the
	// request start also accounts for time spent transferring the response.
	deadline := claimStarted.Add(task.Deadline.Sub(task.ClaimedAt))
	leaseUntil := claimStarted.Add(task.LeaseUntil.Sub(task.ClaimedAt))
	if !leaseUntil.After(time.Now()) {
		return nil, store.ErrLeaseLost
	}
	runCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	heartbeat := make(chan error, 1)
	go func() {
		err := r.keepLease(runCtx, task.LeaseRef, leaseUntil, task.Deadline)
		if err != nil {
			cancel()
		}
		heartbeat <- err
	}()

	result := handler(runCtx, *task)
	var receipt store.CompletionReceipt
	err = runCtx.Err()
	if err == nil {
		completeCtx, stopCompletion := context.WithDeadline(ctx, deadline)
		defer stopCompletion()
		request := store.CompletionRequest{Lease: task.LeaseRef, Success: result.FailureCode == "",
			Outputs: result.Outputs, Branch: result.Branch, FailureCode: result.FailureCode}
		receipt, err = r.api.Complete(completeCtx, request)
		if errors.Is(err, workerapi.ErrTemporary) && completeCtx.Err() == nil {
			receipt, err = r.api.Complete(completeCtx, request)
		}
	}
	cancel()
	if heartbeatErr := <-heartbeat; err != nil {
		return nil, errors.Join(err, heartbeatErr)
	}
	return &receipt, nil
}

func (r *Runner) keepLease(ctx context.Context, ref store.LeaseRef, until, serverDeadline time.Time) error {
	for ctx.Err() == nil {
		remaining := time.Until(until)
		if remaining <= 0 {
			return store.ErrLeaseLost
		}
		timer := time.NewTimer(remaining / 3)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		started := time.Now()
		renewCtx, cancel := context.WithDeadline(ctx, until)
		result, err := r.api.Renew(renewCtx, store.RenewRequest{Lease: ref, LeaseSeconds: r.leaseSeconds})
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		until = started.Add(result.LeaseUntil.Sub(result.RenewedAt))
		if !result.LeaseUntil.Before(serverDeadline) {
			// The task deadline capped the lease. Further renewal cannot extend it.
			timer := time.NewTimer(time.Until(until))
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil
			case <-timer.C:
				return store.ErrLeaseLost
			}
		}
	}
	return nil
}
