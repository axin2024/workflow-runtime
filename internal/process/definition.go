package process

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
)

const MaxDefinitionBytes = 256 << 10
const MaxSteps = 128
const MaxChildren = 16
const MaxWaitSeconds = 365 * 24 * 60 * 60

const (
	Signal   = "signal"
	Approval = "approval"
	Timer    = "timer"
	Action   = "action"
	Children = "children"
	End      = "end"
)

var key = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

func ValidKey(value string) bool { return key.MatchString(value) }

type Definition struct {
	Start      string   `json:"start"`
	Operations []string `json:"operations"`
	Steps      []Step   `json:"steps"`
}

type Step struct {
	Key           string  `json:"key"`
	Kind          string  `json:"kind"`
	Next          string  `json:"next,omitempty"`
	OnTimeout     string  `json:"on_timeout,omitempty"`
	OnFailure     string  `json:"on_failure,omitempty"`
	AfterSeconds  int     `json:"after_seconds,omitempty"`
	SignalKind    string  `json:"signal_kind,omitempty"`
	Role          string  `json:"role,omitempty"`
	Operation     string  `json:"operation,omitempty"`
	Compensation  string  `json:"compensation,omitempty"`
	Children      []Child `json:"children,omitempty"`
	Successes     int     `json:"successes,omitempty"`
	CancelWaiting bool    `json:"cancel_waiting_children,omitempty"`
	Result        string  `json:"result,omitempty"`
}

type Child struct {
	Process  string `json:"process"`
	Revision int    `json:"revision"`
}

type Diagnostic struct {
	Path string `json:"path"`
	Code string `json:"code"`
}

func Decode(raw []byte) (Definition, error) {
	var def Definition
	if len(raw) == 0 || len(raw) > MaxDefinitionBytes {
		return def, errors.New("process definition must be between 1 and 262144 bytes")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&def); err != nil || d.Decode(new(any)) != io.EOF {
		return Definition{}, errors.New("process definition must be one supported JSON object")
	}
	return def, nil
}

func (d Definition) Step(key string) (Step, bool) {
	for _, step := range d.Steps {
		if step.Key == key {
			return step, true
		}
	}
	return Step{}, false
}

func (d Definition) ReachesSignal(start, kind string) bool {
	seen := map[string]bool{}
	var reaches func(string) bool
	reaches = func(key string) bool {
		if seen[key] {
			return false
		}
		seen[key] = true
		step, ok := d.Step(key)
		if !ok {
			return false
		}
		if step.Kind == Signal && step.SignalKind == kind {
			return true
		}
		for _, exit := range step.exits() {
			if reaches(exit) {
				return true
			}
		}
		return false
	}
	return reaches(start)
}

func (d Definition) Validate() []Diagnostic {
	diagnostics := []Diagnostic{}
	report := func(path, code string) {
		diagnostics = append(diagnostics, Diagnostic{Path: path, Code: code})
	}
	if len(d.Steps) < 1 || len(d.Steps) > MaxSteps {
		report("steps", "invalid_step_count")
		return diagnostics
	}
	operations := make(map[string]bool, len(d.Operations))
	for i, operation := range d.Operations {
		if !ValidKey(operation) || operations[operation] {
			report(fmt.Sprintf("operations[%d]", i), "invalid_or_duplicate_operation")
		}
		operations[operation] = true
	}
	if len(operations) > MaxSteps {
		report("operations", "too_many_operations")
	}
	steps := make(map[string]int, len(d.Steps))
	for i, step := range d.Steps {
		if _, duplicate := steps[step.Key]; !ValidKey(step.Key) || duplicate {
			report(fmt.Sprintf("steps[%d].key", i), "invalid_or_duplicate_step")
		}
		steps[step.Key] = i
	}
	if _, ok := steps[d.Start]; !ok {
		report("start", "unknown_step")
	}
	for i, step := range d.Steps {
		path := fmt.Sprintf("steps[%d]", i)
		for _, exit := range step.exits() {
			if _, ok := steps[exit]; !ok {
				report(path, "unknown_exit")
			}
		}
		validWait := step.AfterSeconds > 0 && step.AfterSeconds <= MaxWaitSeconds
		switch step.Kind {
		case Signal, Approval:
			if step.Next == "" || step.OnTimeout == "" || !validWait {
				report(path, "wait_requires_success_and_timeout")
			}
			if step.Kind == Signal && !ValidKey(step.SignalKind) {
				report(path+".signal_kind", "invalid_signal_kind")
			}
			if step.Kind == Approval && (!ValidKey(step.Role) || step.OnFailure == "") {
				report(path, "approval_requires_role_and_rejection_exit")
			}
		case Timer:
			if step.Next == "" || !validWait {
				report(path, "timer_requires_deadline_and_exit")
			}
		case Action:
			if step.Next == "" || step.OnFailure == "" {
				report(path, "action_requires_success_and_failure_exits")
			}
			if !operations[step.Operation] {
				report(path+".operation", "unbound_operation")
			}
			if step.Compensation != "" && !operations[step.Compensation] {
				report(path+".compensation", "unbound_compensation")
			}
		case Children:
			if step.Next == "" || step.OnFailure == "" || len(step.Children) > MaxChildren ||
				step.Successes < 1 || step.Successes > len(step.Children) {
				report(path, "invalid_child_join")
			}
			for j, child := range step.Children {
				if !ValidKey(child.Process) || child.Revision < 1 {
					report(fmt.Sprintf("%s.children[%d]", path, j), "child_revision_required")
				}
			}
		case End:
			if len(step.exits()) != 0 || (step.Result != "succeeded" && step.Result != "failed") {
				report(path, "invalid_terminal_result")
			}
		default:
			report(path+".kind", "unsupported_step_kind")
		}
	}
	if len(diagnostics) != 0 {
		return diagnostics
	}
	colors := make(map[string]uint8, len(steps))
	var visit func(string)
	visit = func(name string) {
		switch colors[name] {
		case 1:
			report(fmt.Sprintf("steps[%d]", steps[name]), "cycle")
			return
		case 2:
			return
		}
		colors[name] = 1
		for _, exit := range d.Steps[steps[name]].exits() {
			visit(exit)
		}
		colors[name] = 2
	}
	visit(d.Start)
	for i, step := range d.Steps {
		if colors[step.Key] == 0 {
			report(fmt.Sprintf("steps[%d]", i), "unreachable_step")
		}
	}
	return diagnostics
}

func (s Step) exits() []string {
	exits := make([]string, 0, 3)
	for _, exit := range []string{s.Next, s.OnTimeout, s.OnFailure} {
		if exit != "" {
			exits = append(exits, exit)
		}
	}
	return exits
}
