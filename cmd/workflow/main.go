package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/axin2024/workflow-runtime/internal/workflow"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	if len(args) == 0 || (len(args) == 1 && (args[0] == "--help" || args[0] == "-h")) {
		_, err := fmt.Fprintln(out, "Usage: workflow validate DEFINITION.json [INPUTS.json]")
		return err
	}
	if args[0] != "validate" || len(args) < 2 || len(args) > 3 {
		return errors.New("usage: workflow validate DEFINITION.json [INPUTS.json]")
	}
	data, err := readSample(args[1])
	if err != nil {
		return err
	}
	compiled, err := workflow.Parse(data)
	if err != nil {
		return err
	}
	if len(args) == 3 {
		inputs, err := readSample(args[2])
		if err != nil {
			return err
		}
		var values map[string]json.RawMessage
		if err := json.Unmarshal(inputs, &values); err != nil || values == nil {
			return errors.New("inputs must be a JSON object")
		}
		if err := workflow.ValidateValues("inputs", compiled.Definition.Inputs, values); err != nil {
			return err
		}
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(compiled)
}

func readSample(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, workflow.MaxDefinitionBytes+1))
	if err == nil && len(data) > workflow.MaxDefinitionBytes {
		err = fmt.Errorf("%s exceeds %d bytes", path, workflow.MaxDefinitionBytes)
	}
	return data, err
}
