//nolint:err113,perfsprint // preserve the original endpoint validation errors
package v1

import "fmt"

type EndpointSpec struct {
	Name            string           `json:"name"`
	Protocol        EndpointProtocol `json:"protocol"`
	Target          ConnectionTarget `json:"target"`
	WorkerPortRange *PortRange       `json:"workerPortRange,omitempty"`
}

func (endpoint EndpointSpec) Validate() error {
	if endpoint.Name == "" {
		return fmt.Errorf("endpoint name cannot be empty")
	}

	if err := endpoint.Protocol.Validate(); err != nil {
		return fmt.Errorf("endpoint %q: %w", endpoint.Name, err)
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
	Name       string           `json:"name"`
	Protocol   EndpointProtocol `json:"protocol"`
	WorkerPort uint16           `json:"workerPort,omitempty"`
	State      EndpointState    `json:"state"`
	Message    string           `json:"message,omitempty"`
}

type EndpointProtocol string

const (
	EndpointProtocolTCP EndpointProtocol = "tcp"
	EndpointProtocolUDP EndpointProtocol = "udp"
)

func (protocol EndpointProtocol) Validate() error {
	switch protocol {
	case EndpointProtocolTCP, EndpointProtocolUDP:
		return nil
	default:
		return fmt.Errorf(
			"unsupported endpoint protocol %q: expected %s or %s",
			protocol,
			EndpointProtocolTCP,
			EndpointProtocolUDP,
		)
	}
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
