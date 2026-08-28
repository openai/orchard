package v1

import "fmt"

type ConnectionTarget struct {
	VM *ConnectionTargetVM `json:"vm,omitempty"`
}

type ConnectionTargetVM struct {
	Port uint16 `json:"port"`
}

//nolint:err113,perfsprint // preserve validation errors exposed to API clients
func (target ConnectionTarget) Validate() error {
	if target.VM == nil || target.VM.Port == 0 {
		return fmt.Errorf("a VM connection target with a non-zero port is required")
	}

	return nil
}
