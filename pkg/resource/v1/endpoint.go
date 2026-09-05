//nolint:err113,perfsprint // preserve the original endpoint validation errors
package v1

import "fmt"

type EndpointSpec struct {
	Name            string           `json:"name"`
	Target          ConnectionTarget `json:"target"`
	WorkerPortRange *PortRange       `json:"workerPortRange,omitempty"`
}

func (endpoint EndpointSpec) Validate() error {
	if endpoint.Name == "" {
		return fmt.Errorf("endpoint name cannot be empty")
	}

	if err := endpoint.Target.Validate(); err != nil {
		return fmt.Errorf("endpoint %q: %w", endpoint.Name, err)
	}

	if portRange := endpoint.WorkerPortRange; portRange != nil {
		if err := portRange.Validate(); err != nil {
			return fmt.Errorf("endpoint %q has invalid worker port range: %w", endpoint.Name, err)
		}
	}

	return nil
}

type EndpointStatus struct {
	Name       string        `json:"name"`
	WorkerPort uint16        `json:"workerPort,omitempty"`
	State      EndpointState `json:"state"`
	Message    string        `json:"message,omitempty"`
}

type PortRange struct {
	Min uint16 `json:"min"`
	Max uint16 `json:"max"`
}

func (portRange PortRange) Validate() error {
	if portRange.Min == 0 {
		return fmt.Errorf("minimum port must be greater than zero")
	}

	if portRange.Min > portRange.Max {
		return fmt.Errorf(
			"minimum port %d exceeds maximum port %d",
			portRange.Min,
			portRange.Max,
		)
	}

	return nil
}

// FreePort returns the lowest port of the range that is not in taken.
func (portRange PortRange) FreePort(taken map[uint16]struct{}) (uint16, bool) {
	for port := int(portRange.Min); port <= int(portRange.Max); port++ {
		if _, ok := taken[uint16(port)]; !ok {
			return uint16(port), true
		}
	}

	return 0, false
}

type EndpointState string

const (
	EndpointStateListening EndpointState = "listening"
	EndpointStateError     EndpointState = "error"
)

func ValidateEndpoints(endpoints []EndpointSpec) error {
	seenNames := make(map[string]struct{}, len(endpoints))

	for _, endpoint := range endpoints {
		if err := endpoint.Validate(); err != nil {
			return err
		}
		if _, exists := seenNames[endpoint.Name]; exists {
			return fmt.Errorf("endpoint %q is duplicated", endpoint.Name)
		}

		seenNames[endpoint.Name] = struct{}{}
	}

	return nil
}
