package workflow

import (
	"bytes"
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strings"
)

const MaxDefinitionBytes = 1 << 20

type ValueType string

const (
	String  ValueType = "string"
	Integer ValueType = "integer"
	Number  ValueType = "number"
	Boolean ValueType = "boolean"
	Object  ValueType = "object"
	Array   ValueType = "array"
)

type Condition string

const (
	Succeeded Condition = "succeeded"
	Finished  Condition = "finished"
	Failed    Condition = "failed"
	Branched  Condition = "branch"
)

type Effect string

const (
	NoEffect         Effect = "none"
	IdempotentEffect Effect = "idempotent"
	ExternalEffect   Effect = "external"
)

type Definition struct {
	Key    string               `json:"key"`
	Inputs map[string]ValueType `json:"inputs,omitempty"`
	Nodes  []Node               `json:"nodes"`
}

type Node struct {
	Key            string               `json:"key"`
	Task           string               `json:"task"`
	Queue          string               `json:"queue"`
	TimeoutSeconds int                  `json:"timeout_seconds"`
	DependsOn      []Edge               `json:"depends_on,omitempty"`
	Input          map[string]Binding   `json:"input,omitempty"`
	Output         map[string]ValueType `json:"output,omitempty"`
	Branches       []string             `json:"branches,omitempty"`
	Retry          RetryPolicy          `json:"retry"`
	Effect         Effect               `json:"effect"`
}

type Edge struct {
	Node   string    `json:"node"`
	When   Condition `json:"when"`
	Branch string    `json:"branch,omitempty"`
}

type Binding struct {
	Type  ValueType `json:"type"`
	Input string    `json:"input,omitempty"`
	Node  string    `json:"node,omitempty"`
	Field string    `json:"field,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts    int      `json:"max_attempts"`
	BackoffSeconds int      `json:"backoff_seconds,omitempty"`
	On             []string `json:"on,omitempty"`
}

type Compiled struct {
	Digest     string     `json:"digest"`
	Order      []string   `json:"order"`
	Definition Definition `json:"definition"`
}

type Issue struct {
	Path    string `json:"path"`
	Message string `json:"message"`
}

func (i *Issue) Error() string { return i.Path + ": " + i.Message }

var keyPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

func issue(path, message string) error { return &Issue{Path: path, Message: message} }

func Parse(raw []byte) (Compiled, error) {
	if len(raw) == 0 || len(raw) > MaxDefinitionBytes {
		return Compiled{}, issue("definition", "expected 1 to 1048576 bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var definition Definition
	if err := decoder.Decode(&definition); err != nil {
		return Compiled{}, issue("definition", err.Error())
	}
	if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
		return Compiled{}, issue("definition", "expected one JSON value")
	}
	return Compile(definition)
}

func Compile(source Definition) (Compiled, error) {
	if !keyPattern.MatchString(source.Key) {
		return Compiled{}, issue("key", "use a lowercase identifier of up to 64 characters")
	}
	if err := validateFields("inputs", source.Inputs); err != nil {
		return Compiled{}, err
	}
	if len(source.Nodes) == 0 || len(source.Nodes) > 256 {
		return Compiled{}, issue("nodes", "expected 1 to 256 nodes")
	}
	definition := Definition{Key: source.Key, Inputs: maps.Clone(source.Inputs), Nodes: make([]Node, len(source.Nodes))}
	indices := make(map[string]int, len(definition.Nodes))
	for index, node := range source.Nodes {
		path := fmt.Sprintf("nodes[%d]", index)
		if err := validateNode(path, node); err != nil {
			return Compiled{}, err
		}
		if _, exists := indices[node.Key]; exists {
			return Compiled{}, issue(path+".key", "duplicate node "+node.Key)
		}
		indices[node.Key] = index
		definition.Nodes[index] = node
	}
	for index, node := range definition.Nodes {
		path := fmt.Sprintf("nodes[%d]", index)
		if err := validateReferences(path, definition, indices, node); err != nil {
			return Compiled{}, err
		}
		definition.Nodes[index] = normalizeNode(node)
	}
	keys := slices.Sorted(maps.Keys(indices))
	order, err := topologicalOrder(definition, indices, keys)
	if err != nil {
		return Compiled{}, err
	}
	slices.SortFunc(definition.Nodes, func(a, b Node) int { return cmp.Compare(a.Key, b.Key) })
	canonical, err := json.Marshal(definition)
	if err != nil {
		return Compiled{}, fmt.Errorf("encode definition: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return Compiled{Digest: hex.EncodeToString(digest[:]), Order: order, Definition: definition}, nil
}

func validateNode(path string, node Node) error {
	for _, item := range []struct{ field, value string }{{"key", node.Key}, {"task", node.Task}, {"queue", node.Queue}} {
		if !keyPattern.MatchString(item.value) {
			return issue(path+"."+item.field, "use a lowercase identifier of up to 64 characters")
		}
	}
	if node.TimeoutSeconds < 1 || node.TimeoutSeconds > 86400 {
		return issue(path+".timeout_seconds", "expected 1 to 86400 seconds")
	}
	if node.Retry.MaxAttempts < 1 || node.Retry.MaxAttempts > 10 {
		return issue(path+".retry.max_attempts", "expected 1 to 10 attempts")
	}
	if node.Retry.BackoffSeconds < 0 || node.Retry.BackoffSeconds > 86400 {
		return issue(path+".retry.backoff_seconds", "expected 0 to 86400 seconds")
	}
	if err := validateNames(path+".retry.on", node.Retry.On); err != nil {
		return err
	}
	if err := validateNames(path+".branches", node.Branches); err != nil {
		return err
	}
	if node.Effect != NoEffect && node.Effect != IdempotentEffect && node.Effect != ExternalEffect {
		return issue(path+".effect", "expected none, idempotent, or external")
	}
	return validateFields(path+".output", node.Output)
}

func validateNames(path string, names []string) error {
	seen := make(map[string]bool, len(names))
	for index, name := range names {
		if !keyPattern.MatchString(name) || seen[name] {
			return issue(fmt.Sprintf("%s[%d]", path, index), "expected a unique lowercase identifier")
		}
		seen[name] = true
	}
	return nil
}

func validateFields(path string, fields map[string]ValueType) error {
	for _, key := range slices.Sorted(maps.Keys(fields)) {
		if !keyPattern.MatchString(key) || !validType(fields[key]) {
			return issue(path+"."+key, "invalid field name or type")
		}
	}
	return nil
}

func validType(kind ValueType) bool {
	switch kind {
	case String, Integer, Number, Boolean, Object, Array:
		return true
	}
	return false
}

func validateReferences(path string, definition Definition, indices map[string]int, node Node) error {
	dependencies := make(map[string]Condition, len(node.DependsOn))
	for index, edge := range node.DependsOn {
		edgePath := fmt.Sprintf("%s.depends_on[%d]", path, index)
		upstream, exists := indices[edge.Node]
		if !exists || dependencies[edge.Node] != "" {
			return issue(edgePath+".node", "unknown or duplicate dependency "+edge.Node)
		}
		switch edge.When {
		case Succeeded, Finished, Failed:
			if edge.Branch != "" {
				return issue(edgePath+".branch", "branch is only used with the branch condition")
			}
		case Branched:
			if !slices.Contains(definition.Nodes[upstream].Branches, edge.Branch) {
				return issue(edgePath+".branch", "branch is absent from the upstream node")
			}
		default:
			return issue(edgePath+".when", "unknown dependency condition")
		}
		dependencies[edge.Node] = edge.When
	}
	for _, key := range slices.Sorted(maps.Keys(node.Input)) {
		binding, fieldPath := node.Input[key], path+".input."+key
		if !keyPattern.MatchString(key) || !validType(binding.Type) {
			return issue(fieldPath, "invalid field name or type")
		}
		var declared ValueType
		switch {
		case binding.Input != "" && binding.Node == "" && binding.Field == "":
			declared = definition.Inputs[binding.Input]
		case binding.Input == "" && binding.Node != "" && binding.Field != "":
			condition := dependencies[binding.Node]
			if condition != Succeeded && condition != Branched {
				return issue(fieldPath, "output requires a successful upstream dependency")
			}
			declared = definition.Nodes[indices[binding.Node]].Output[binding.Field]
		default:
			return issue(fieldPath, "choose one workflow input or upstream output")
		}
		if declared == "" || (declared != binding.Type && !(declared == Integer && binding.Type == Number)) {
			return issue(fieldPath, "source field is missing or has an incompatible type")
		}
	}
	return nil
}

func normalizeNode(node Node) Node {
	node.DependsOn = slices.Clone(node.DependsOn)
	slices.SortFunc(node.DependsOn, func(a, b Edge) int { return cmp.Compare(a.Node, b.Node) })
	node.Branches = slices.Clone(node.Branches)
	slices.Sort(node.Branches)
	node.Retry.On = slices.Clone(node.Retry.On)
	slices.Sort(node.Retry.On)
	node.Input = maps.Clone(node.Input)
	node.Output = maps.Clone(node.Output)
	return node
}

func topologicalOrder(definition Definition, indices map[string]int, keys []string) ([]string, error) {
	visiting := make(map[string]bool, len(keys))
	visited := make(map[string]bool, len(keys))
	order := make([]string, 0, len(keys))
	stack := make([]string, 0, len(keys))
	var visit func(string) error
	visit = func(key string) error {
		if visited[key] {
			return nil
		}
		if visiting[key] {
			cycle := append(slices.Clone(stack[slices.Index(stack, key):]), key)
			return issue("nodes", "dependency cycle "+strings.Join(cycle, " -> "))
		}
		visiting[key] = true
		stack = append(stack, key)
		for _, dep := range definition.Nodes[indices[key]].DependsOn {
			if err := visit(dep.Node); err != nil {
				return err
			}
		}
		stack = stack[:len(stack)-1]
		delete(visiting, key)
		visited[key] = true
		order = append(order, key)
		return nil
	}
	for _, key := range keys {
		if err := visit(key); err != nil {
			return nil, err
		}
	}
	return order, nil
}
