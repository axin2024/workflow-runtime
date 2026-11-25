// Package samples contains the data shipped with the self-hosted walkthrough.
package samples

import _ "embed"

//go:embed lease-failover.json
var leaseDefinition string

//go:embed diamond-inputs.json
var dispatchInputs string

func LeaseFailover() (definition, inputs []byte) {
	return []byte(leaseDefinition), []byte(dispatchInputs)
}
