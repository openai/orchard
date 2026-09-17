package v1

import "time"

type Worker struct {
	// LastSeen is set by the Worker and is used by the Controller
	// to track unhealthy Workers.
	LastSeen time.Time `json:"last_seen,omitempty"`

	MachineID string `json:"machine_id,omitempty"`

	SchedulingPaused bool `json:"scheduling_paused,omitempty"`

	// Resources available on this Worker.
	Resources Resources `json:"resources,omitempty"`

	// Labels that this Worker supports.
	Labels Labels `json:"labels,omitempty"`

	// DefaultCPU is the amount of CPUs to assign to a VM
	// when it doesn't explicitly request a specific amount.
	DefaultCPU uint64 `json:"defaultCPU,omitempty"`
	// DefaultMemory is the amount of memory to assign to a VM
	// when it doesn't explicitly request a specific amount.
	DefaultMemory uint64 `json:"defaultMemory,omitempty"`

	// Arch defines worker's hardware architecture.
	Arch Architecture `json:"arch,omitempty"`

	// Runtime defines a runtime provided by this worker.
	Runtime Runtime `json:"runtime,omitempty"`

	Capabilities WorkerCapabilities `json:"capabilities,omitempty"`

	Meta
}

func (worker Worker) Offline(workerOfflineTimeout time.Duration) bool {
	return time.Since(worker.LastSeen) > workerOfflineTimeout
}

func (worker *Worker) SetVersion(_ uint64) {}

type WorkerCapability string

const (
	WorkerCapabilityVMEndpoints    WorkerCapability = "vm-endpoints"
	WorkerCapabilityVMExposedPorts WorkerCapability = "vm-exposed-ports"
)

type WorkerCapabilities []WorkerCapability

//nolint:modernize // preserve the original capability membership helper
func (workerCapabilities WorkerCapabilities) Has(capability WorkerCapability) bool {
	for _, workerCapability := range workerCapabilities {
		if workerCapability == capability {
			return true
		}
	}

	return false
}

func (worker *Worker) Match(filter Filter) bool {
	return false
}
