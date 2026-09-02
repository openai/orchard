//nolint:err113 // Preserve host-process validation messages.
package v1

import (
	"fmt"

	"github.com/cirruslabs/orchard/internal/simplename"
	mapset "github.com/deckarep/golang-set/v2"
)

// HostProcess describes a process run by the worker alongside a VM. Program is
// an executable path or a name resolved using the worker's PATH.
type HostProcess struct {
	Name    string            `json:"name"`
	Program string            `json:"program"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
}

func ValidateHostProcesses(processes []HostProcess) error {
	seenNames := mapset.NewSetWithSize[string](len(processes))

	for _, process := range processes {
		if process.Name == "" || simplename.Validate(process.Name) != nil {
			return fmt.Errorf("host process %q is invalid", process.Name)
		}
		if !seenNames.Add(process.Name) {
			return fmt.Errorf("host process %q is duplicated", process.Name)
		}
		if process.Program == "" {
			return fmt.Errorf("host process %q has an empty program", process.Name)
		}
	}

	return nil
}
