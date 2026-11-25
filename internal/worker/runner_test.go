package worker

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workerapi"
	"github.com/axin2024/workflow-runtime/internal/workflow"
)

type testAPI struct {
	claim    func(store.ClaimRequest) (*store.ClaimedTask, error)
	renew    func(store.RenewRequest) (store.LeaseRenewal, error)
	complete func(store.CompletionRequest) (store.CompletionReceipt, error)
	ack      func(store.LeaseRef) (store.CompletionReceipt, error)
}

func (f testAPI) Claim(_ context.Context, request store.ClaimRequest) (*store.ClaimedTask, error) {
	return f.claim(request)
}

func (f testAPI) Renew(_ context.Context, request store.RenewRequest) (store.LeaseRenewal, error) {
	return f.renew(request)
}

func (f testAPI) Complete(_ context.Context, request store.CompletionRequest) (store.CompletionReceipt, error) {
	return f.complete(request)
}

func (f testAPI) AcknowledgeCancel(_ context.Context, ref store.LeaseRef) (store.CompletionReceipt, error) {
	return f.ack(ref)
}
func TestRunnerRenewsAndResendsCompletion(t *testing.T) {
	ref := store.LeaseRef{RunID: 10, Node: "send", Epoch: 1, OperationKey: strings.Repeat("a", 64)}
	renewed := make(chan struct{}, 1)
	completions := 0
	api := testAPI{
		claim: func(request store.ClaimRequest) (*store.ClaimedTask, error) {
			if len(request.Tasks) != 1 || request.Tasks[0] != "send" || request.LeaseSeconds != 1 {
				t.Fatalf("claim request: %+v", request)
			}
			now := time.Now()
			return &store.ClaimedTask{LeaseRef: ref, Task: "send", ClaimedAt: now, LeaseUntil: now.Add(time.Second), Deadline: now.Add(3 * time.Second)}, nil
		},
		renew: func(request store.RenewRequest) (store.LeaseRenewal, error) {
			if request.Lease != ref || request.LeaseSeconds != 1 {
				t.Errorf("renew request: %+v", request)
			}
			select {
			case renewed <- struct{}{}:
			default:
			}
			now := time.Now()
			return store.LeaseRenewal{RenewedAt: now, LeaseUntil: now.Add(time.Second)}, nil
		},
		complete: func(request store.CompletionRequest) (store.CompletionReceipt, error) {
			if request.Lease != ref || !request.Success || string(request.Outputs) != `{"message":"sent"}` {
				t.Fatalf("completion request: %+v", request)
			}
			completions++
			if completions == 1 {
				return store.CompletionReceipt{}, workerapi.ErrTemporary
			}
			return store.CompletionReceipt{LeaseRef: ref, State: workflow.NodeSucceeded, Replayed: true}, nil
		},
	}
	runner, err := New(api, map[string]Handler{"send": func(ctx context.Context, _ store.ClaimedTask) Result {
		select {
		case <-ctx.Done():
			return Result{FailureCode: "interrupted"}
		case <-renewed:
			return output(map[string]string{"message": "sent"})
		}
	}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := runner.RunOne(t.Context())
	if err != nil || receipt == nil || !receipt.Replayed || completions != 2 {
		t.Fatalf("receipt: %+v, error=%v, completion requests=%d", receipt, err, completions)
	}
}

func TestRunnerInterruptsHandlerOnLostLease(t *testing.T) {
	api := testAPI{
		claim: func(store.ClaimRequest) (*store.ClaimedTask, error) {
			now := time.Now()
			return &store.ClaimedTask{Task: "wait", ClaimedAt: now, LeaseUntil: now.Add(time.Second), Deadline: now.Add(3 * time.Second)}, nil
		},
		renew: func(store.RenewRequest) (store.LeaseRenewal, error) {
			return store.LeaseRenewal{}, store.ErrLeaseLost
		},
		complete: func(store.CompletionRequest) (store.CompletionReceipt, error) {
			t.Fatal("result submitted after lease loss")
			return store.CompletionReceipt{}, nil
		},
	}
	runner, err := New(api, map[string]Handler{"wait": func(ctx context.Context, _ store.ClaimedTask) Result {
		<-ctx.Done()
		return Result{FailureCode: "interrupted"}
	}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := runner.RunOne(t.Context()); receipt != nil || !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("lease loss: %+v, %v", receipt, err)
	}
}

func TestRunnerAcknowledgesCancellationAfterHandlerStops(t *testing.T) {
	ref := store.LeaseRef{RunID: 10, Node: "wait", Epoch: 1, OperationKey: strings.Repeat("a", 64)}
	stopped := false
	api := testAPI{
		claim: func(store.ClaimRequest) (*store.ClaimedTask, error) {
			now := time.Now()
			return &store.ClaimedTask{LeaseRef: ref, Task: "wait", ClaimedAt: now,
				LeaseUntil: now.Add(time.Second), Deadline: now.Add(3 * time.Second)}, nil
		},
		renew: func(store.RenewRequest) (store.LeaseRenewal, error) {
			deadline := time.Now().Add(time.Second)
			return store.LeaseRenewal{CancelDeadline: &deadline}, nil
		},
		complete: func(store.CompletionRequest) (store.CompletionReceipt, error) {
			t.Fatal("regular completion submitted after cancellation")
			return store.CompletionReceipt{}, nil
		},
		ack: func(lease store.LeaseRef) (store.CompletionReceipt, error) {
			if !stopped || lease != ref {
				t.Fatalf("unexpected acknowledgement before handler stop: %+v", lease)
			}
			return store.CompletionReceipt{LeaseRef: lease, State: workflow.NodeCanceled}, nil
		},
	}
	runner, err := New(api, map[string]Handler{"wait": func(ctx context.Context, _ store.ClaimedTask) Result {
		<-ctx.Done()
		stopped = true
		return Result{FailureCode: "interrupted"}
	}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, err := runner.RunOne(t.Context()); err != nil || receipt == nil || receipt.State != workflow.NodeCanceled {
		t.Fatalf("cancellation acknowledgement: %+v, %v", receipt, err)
	}
}
