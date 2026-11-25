package workflow

type NodeState string

const (
	NodeWaiting        NodeState = "waiting"
	NodeReady          NodeState = "ready"
	NodeRunning        NodeState = "running"
	NodeRetryWait      NodeState = "retry_wait"
	NodeSucceeded      NodeState = "succeeded"
	NodeFailed         NodeState = "failed"
	NodeSkipped        NodeState = "skipped"
	NodeCanceled       NodeState = "canceled"
	NodeActionRequired NodeState = "action_required"
)

func (state NodeState) Terminal() bool {
	switch state {
	case NodeSucceeded, NodeFailed, NodeSkipped, NodeCanceled:
		return true
	default:
		return false
	}
}

type Predecessor struct {
	State  NodeState
	Branch string
}

type EdgeDecision struct {
	Decided   bool
	Satisfied bool
	Reason    string
}

func EvaluateEdge(edge Edge, upstream Predecessor) EdgeDecision {
	if !upstream.State.Terminal() {
		return EdgeDecision{Reason: "upstream_unfinished"}
	}
	decision := EdgeDecision{Decided: true, Reason: "upstream_" + string(upstream.State)}
	switch edge.When {
	case Succeeded:
		decision.Satisfied = upstream.State == NodeSucceeded
	case Finished:
		decision.Satisfied = true
	case Failed:
		decision.Satisfied = upstream.State == NodeFailed
	case Branched:
		if upstream.State == NodeSucceeded {
			decision.Satisfied = edge.Branch == upstream.Branch
			decision.Reason = "branch_not_selected"
			if decision.Satisfied {
				decision.Reason = "branch_selected"
			}
		}
	}
	return decision
}

// ResolveDependencies waits for every predecessor, then applies all edge conditions.
func ResolveDependencies(dependencies []Edge, predecessors map[string]Predecessor) NodeState {
	state := NodeReady
	for _, edge := range dependencies {
		decision := EvaluateEdge(edge, predecessors[edge.Node])
		if !decision.Decided {
			return NodeWaiting
		}
		if !decision.Satisfied {
			state = NodeSkipped
		}
	}
	return state
}
