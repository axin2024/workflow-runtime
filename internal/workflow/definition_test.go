package workflow

import (
	"encoding/json"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"
)

func loadDiamond(t *testing.T) Definition {
	t.Helper()
	raw, err := os.ReadFile("../../examples/diamond.json")
	if err != nil {
		t.Fatal(err)
	}
	var source Definition
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	return source
}

func TestDefinitionHasStableDigestAndDoesNotMutateItsSource(t *testing.T) {
	source := loadDiamond(t)
	compiled, err := Compile(source)
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{"extract", "aggregate", "inspect", "cleanup", "report"}
	if !slices.Equal(compiled.Order, wantOrder) || len(compiled.Digest) != 64 {
		t.Fatalf("unexpected compiled graph: order=%v digest=%s", compiled.Order, compiled.Digest)
	}
	if source.Nodes[3].DependsOn[0].Node != "inspect" || source.Nodes[2].Branches[0] != "report" {
		t.Fatal("compilation reordered the source definition")
	}
	slices.Reverse(source.Nodes)
	slices.Reverse(source.Nodes[1].DependsOn)
	reordered, err := Compile(source)
	if err != nil || reordered.Digest != compiled.Digest || !slices.Equal(reordered.Order, compiled.Order) {
		t.Fatalf("reordering changed compilation: %+v %v", reordered, err)
	}
	source.Nodes[4].TimeoutSeconds++
	source.Inputs["source"] = Boolean
	if compiled.Definition.Inputs["source"] != String || compiled.Definition.Nodes[2].TimeoutSeconds != 30 {
		t.Fatal("source edits changed the previously compiled graph")
	}
}

func TestDefinitionRejectsCyclesAndInvalidContracts(t *testing.T) {
	for _, test := range []struct {
		name, path, message string
		change              func(*Definition)
	}{
		{"cycle", "nodes", "extract -> inspect -> extract", func(d *Definition) {
			d.Nodes[0].DependsOn = []Edge{{Node: "inspect", When: Succeeded}}
		}},
		{"unknown dependency", "nodes[1].depends_on[0].node", "absent", func(d *Definition) {
			d.Nodes[1].DependsOn[0].Node = "absent"
		}},
		{"unknown branch", "nodes[3].depends_on[0].branch", "branch is absent", func(d *Definition) {
			d.Nodes[3].DependsOn[0].Branch = "absent"
		}},
		{"invalid timeout", "nodes[0].timeout_seconds", "86400", func(d *Definition) {
			d.Nodes[0].TimeoutSeconds = 0
		}},
		{"upstream output type", "nodes[3].input.count", "incompatible", func(d *Definition) {
			d.Nodes[1].Output["count"] = String
		}},
		{"output on failed dependency", "nodes[4].input.grade", "successful upstream", func(d *Definition) {
			d.Nodes[4].Input["grade"] = Binding{Type: String, Node: "inspect", Field: "grade"}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			definition := loadDiamond(t)
			test.change(&definition)
			_, err := Compile(definition)
			var detail *Issue
			if !errors.As(err, &detail) || detail.Path != test.path || !strings.Contains(detail.Message, test.message) {
				t.Fatalf("unexpected issue: %v", err)
			}
		})
	}
}

func TestValidateValuesPreservesIntegerPrecision(t *testing.T) {
	contract := map[string]ValueType{"rows": Integer, "source": String}
	values := map[string]json.RawMessage{"rows": json.RawMessage("9223372036854775807"), "source": json.RawMessage(`"test"`)}
	if err := ValidateValues("inputs", contract, values); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"9223372036854775808", "1.2", `"12"`, "null", "1 2"} {
		values["rows"] = json.RawMessage(invalid)
		if err := ValidateValues("inputs", contract, values); err == nil || !strings.HasPrefix(err.Error(), "inputs.rows:") {
			t.Fatalf("accepted invalid integer %s: %v", invalid, err)
		}
	}
}
