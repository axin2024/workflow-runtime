package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/axin2024/workflow-runtime/internal/store"
	"github.com/axin2024/workflow-runtime/internal/workflow"
)

// DispatchHandlers process synthetic dispatch data for examples/diamond.json.
func DispatchHandlers() map[string]Handler {
	return map[string]Handler{"extract": extract, "aggregate": aggregate, "inspect": inspect, "report": report, "cleanup": cleanup}
}

type record struct {
	Source string `json:"source"`
	Index  int    `json:"index"`
}

type batch struct {
	Records []record `json:"records"`
}

func extract(ctx context.Context, task store.ClaimedTask) Result {
	var input struct {
		Rows   int    `json:"rows"`
		Source string `json:"source"`
	}
	if task.Effect != workflow.NoEffect || json.Unmarshal(task.Inputs, &input) != nil ||
		input.Rows < 0 || input.Rows > 1000 || input.Source == "" || len(input.Source) > 64 {
		return Result{FailureCode: "invalid_input"}
	}
	records := make([]record, 0, input.Rows)
	for i := range input.Rows {
		if ctx.Err() != nil {
			return Result{FailureCode: "interrupted"}
		}
		records = append(records, record{Source: input.Source, Index: i + 1})
	}
	return output(batch{Records: records})
}

func aggregate(_ context.Context, task store.ClaimedTask) Result {
	var input batch
	if task.Effect != workflow.NoEffect || json.Unmarshal(task.Inputs, &input) != nil || input.Records == nil {
		return Result{FailureCode: "invalid_input"}
	}
	return output(map[string]int{"count": len(input.Records)})
}

func inspect(ctx context.Context, task store.ClaimedTask) Result {
	var input batch
	if task.Effect != workflow.NoEffect || json.Unmarshal(task.Inputs, &input) != nil || input.Records == nil {
		return Result{FailureCode: "invalid_input"}
	}
	for i, record := range input.Records {
		if ctx.Err() != nil {
			return Result{FailureCode: "interrupted"}
		}
		if record.Source == "" || record.Index != i+1 {
			return Result{FailureCode: "invalid_record"}
		}
	}
	branch, grade := "report", "passed"
	if len(input.Records) == 0 {
		branch, grade = "manual", "empty"
	}
	result := output(map[string]string{"grade": grade})
	result.Branch = branch
	return result
}

func report(_ context.Context, task store.ClaimedTask) Result {
	var input struct {
		Count int    `json:"count"`
		Grade string `json:"grade"`
	}
	if task.Effect != workflow.IdempotentEffect || json.Unmarshal(task.Inputs, &input) != nil || input.Count < 0 || input.Grade != "passed" {
		return Result{FailureCode: "invalid_input"}
	}
	return output(map[string]string{"summary": fmt.Sprintf("%d dispatch records (%s)", input.Count, input.Grade)})
}

func cleanup(_ context.Context, task store.ClaimedTask) Result {
	if task.Effect != workflow.IdempotentEffect {
		return Result{FailureCode: "invalid_input"}
	}
	return Result{}
}

func output(value any) Result {
	data, err := json.Marshal(value)
	if err != nil {
		return Result{FailureCode: "invalid_output"}
	}
	return Result{Outputs: data}
}
