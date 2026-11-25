package workflow

import (
	"bytes"
	"encoding/json"
	"maps"
	"slices"
	"strconv"
)

// ValidateValues checks a complete workflow input or task output against its fixed contract.
func ValidateValues(path string, schema map[string]ValueType, values map[string]json.RawMessage) error {
	for _, key := range slices.Sorted(maps.Keys(schema)) {
		value, exists := values[key]
		if !exists {
			return issue(path+"."+key, "missing required value")
		}
		if !matchesType(value, schema[key]) {
			return issue(path+"."+key, "expected "+string(schema[key]))
		}
	}
	for _, key := range slices.Sorted(maps.Keys(values)) {
		if _, exists := schema[key]; !exists {
			return issue(path+"."+key, "field is absent from the contract")
		}
	}
	return nil
}

func matchesType(value json.RawMessage, kind ValueType) bool {
	if !json.Valid(value) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return false
	}
	switch typed := decoded.(type) {
	case string:
		return kind == String
	case json.Number:
		if kind == Number {
			return true
		}
		_, err := strconv.ParseInt(typed.String(), 10, 64)
		return kind == Integer && err == nil
	case bool:
		return kind == Boolean
	case map[string]any:
		return kind == Object
	case []any:
		return kind == Array
	}
	return false
}
