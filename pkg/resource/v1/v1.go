package v1

import (
	"encoding/json"
	"fmt"
	"slices"
	"time"
)

// Meta is a common set of fields that apply to all resources managed by the Controller.
type Meta struct {
	// Name is a human-readable resource identifier populated by the Worker or Client.
	//
	// There can't be multiple resources with the same Name in the DB at any given time.
	Name string `json:"name,omitempty"`

	// CreatedAt is a useful field for scheduler prioritization.
	//
	// It is populated by the Controller with the current time
	// when receiving a POST request.
	CreatedAt time.Time `json:"createdAt,omitempty"`

	// Version is a resource version used internally to implement WatchVM().
	Version uint64 `json:"version,omitempty"`
}

type VM struct {
	Image           string          `json:"image,omitempty"`
	ImagePullPolicy ImagePullPolicy `json:"imagePullPolicy,omitempty"`
	CPU             uint64          `json:"cpu,omitempty"`
	Memory          uint64          `json:"memory,omitempty"`
	DiskSize        uint64          `json:"diskSize,omitempty"`
	NetBridged      string          `json:"net-bridged,omitempty"`
	Headless        bool            `json:"headless,omitempty"`
	Nested          bool            `json:"nested,omitempty"`
	NoAudio         bool            `json:"noAudio,omitempty"`
	NoClipboard     bool            `json:"noClipboard,omitempty"`

	VMSpec
	VMSpecReadOnly
	VMState

	// Status field is used to track the lifecycle of the VM associated with this resource.
	Status        VMStatus `json:"status,omitempty"`
	StatusMessage string   `json:"status_message,omitempty"`

	// Worker field is set by the Controller to assign this VM to a specific Worker.
	Worker string `json:"worker,omitempty"`

	// AssignedCPU is set by the Controller when the VM is scheduled.
	//
	// It's set to CPU when CPU non-zero, otherwise the value is taken from
	// Worker's DefaultCPU field. If Worker's DefaultCPU field is zero, it defaults
	// to 4.
	AssignedCPU uint64 `json:"assignedCPU,omitempty"`
	// AssignedMemory is set by the Controller
	//
	// It's set to Memory when Memory non-zero, otherwise the value is taken from
	// Worker's DefaultCPU field. If Worker's DefaultCPU field is zero, it defaults
	// to 8192.
	AssignedMemory uint64 `json:"assignedMemory,omitempty"`

	Username      string    `json:"username,omitempty"`
	Password      string    `json:"password,omitempty"`
	StartupScript *VMScript `json:"startup_script,omitempty"`

	RestartPolicy RestartPolicy `json:"restart_policy,omitempty"`
	RestartedAt   time.Time     `json:"restarted_at,omitempty"`
	RestartCount  uint64        `json:"restart_count,omitempty"`

	// RandomSerial controls whether the worker will run the
	// "tart set --random-serial" when instantiating this VM.
	RandomSerial bool `json:"randomSerial,omitempty"`

	// UID is a useful field for avoiding data races within a single Name.
	//
	// It is populated by the Controller when receiving a POST request.
	UID string `json:"uid,omitempty"`

	// Resources required by this VM.
	Resources Resources `json:"resources,omitempty"`

	// Labels required by this VM.
	Labels Labels `json:"labels,omitempty"`

	// HostDir is a list of host directories to be mounted to the VM.
	HostDirs []HostDir `json:"hostDirs,omitempty"`

	// ImageFQN is a fully qualified name of the Image that it is populated
	// by the worker using "tart fqn" command after it had pulled the image.
	ImageFQN string `json:"image_fqn,omitempty"`

	ScheduledAt time.Time `json:"scheduled_at,omitempty"`
	StartedAt   time.Time `json:"started_at,omitempty"`

	// Generation is incremented by the controller each time
	// the resource's specification is changed.
	//
	// At some point we'll move Generation field to the Metadata
	// structure as it can be useful for other resources too.
	Generation uint64 `json:"generation"`

	Meta
}

func (vm *VM) SetVersion(version uint64) {
	vm.Version = version
}

func (vm *VM) Match(filter Filter) bool {
	switch filter.Path {
	case "worker":
		return vm.Worker == filter.Value
	default:
		return false
	}
}

func (vm *VM) SSHUsername() string {
	if vm.Username != "" {
		return vm.Username
	}

	return "admin"
}

func (vm *VM) SSHPassword() string {
	if vm.Password != "" {
		return vm.Password
	}

	return "admin"
}

func (vm *VM) IsScheduled() bool {
	if ConditionExists(vm.Conditions, ConditionTypeScheduled) {
		return ConditionIsTrue(vm.Conditions, ConditionTypeScheduled)
	} else {
		return vm.Worker != ""
	}
}

func (vm *VM) Validate() error {
	unsupportedFieldError := func(field string) error {
		return fmt.Errorf("runtime %q does not support field %q", vm.Runtime, field)
	}

	switch vm.Runtime {
	case RuntimeVetu:
		if vm.NetSoftnetDeprecated || vm.NetSoftnet {
			return unsupportedFieldError("netSoftnet")
		}
		if len(vm.NetSoftnetAllow) != 0 {
			return unsupportedFieldError("netSoftnetAllow")
		}
		if len(vm.NetSoftnetBlock) != 0 {
			return unsupportedFieldError("netSoftnetBlock")
		}
		if len(vm.HostDirs) != 0 {
			return unsupportedFieldError("hostDirs")
		}
		if vm.Suspendable {
			return unsupportedFieldError("suspendable")
		}
	}

	return nil
}

type VMSpec struct {
	// OS defines the operating system used by a VM.
	//
	// Previously only Darwin was supported,
	// so this field defaults to that when not set.
	OS OS `json:"os,omitempty"`

	// Arch defines the hardware architecture to use for a VM.
	//
	// Previously, only Tart on arm64 was supported,
	// so this field defaults to that when not set.
	Arch Architecture `json:"arch,omitempty"`

	// Runtime defines the runtime to use for a VM.
	//
	// Previously, only Tart on arm64 was supported,
	// so this field defaults to that when not set.
	Runtime Runtime `json:"runtime,omitempty"`

	NetSoftnetDeprecated bool       `json:"net-softnet,omitempty"`
	NetSoftnet           bool       `json:"netSoftnet,omitempty"`
	NetSoftnetAllow      []string   `json:"netSoftnetAllow,omitempty"`
	NetSoftnetBlock      []string   `json:"netSoftnetBlock,omitempty"`
	Suspendable          bool       `json:"suspendable,omitempty"`
	PowerState           PowerState `json:"powerState,omitempty"`
}

func (vm VMSpec) SoftnetEnabled() bool {
	return vm.NetSoftnetDeprecated || vm.NetSoftnet ||
		len(vm.NetSoftnetAllow) != 0 || len(vm.NetSoftnetBlock) != 0
}

func (vm VMSpec) SoftnetPolicyChanged(other VMSpec) bool {
	return !slices.Equal(vm.NetSoftnetAllow, other.NetSoftnetAllow) ||
		!slices.Equal(vm.NetSoftnetBlock, other.NetSoftnetBlock)
}

type VMSpecReadOnly struct {
	LocalName string `json:"localName,omitempty"`

	// Deprecated: use LocalName instead.
	TartName string `json:"tartName,omitempty"`
}

type VMState struct {
	// ObservedGeneration corresponds to the Generation of VM specification
	// on which the worker had acted upon.
	ObservedGeneration uint64 `json:"observedGeneration"`

	Conditions []Condition `json:"conditions,omitempty"`
}

type OS string

const (
	OSDarwin OS = "darwin"
	OSLinux  OS = "linux"
)

func NewOSFromString(osRaw string) (OS, error) {
	switch osRaw {
	case "", string(OSDarwin):
		return OSDarwin, nil
	case string(OSLinux):
		return OSLinux, nil
	default:
		return "", fmt.Errorf("unsupported OS: %q", osRaw)
	}
}

func (os *OS) UnmarshalJSON(data []byte) error {
	var osRaw string

	if err := json.Unmarshal(data, &osRaw); err != nil {
		return err
	}

	parsedOS, err := NewOSFromString(osRaw)
	if err != nil {
		return err
	}

	*os = parsedOS

	return nil
}

type Architecture string

const (
	ArchitectureARM64 Architecture = "arm64"
	ArchitectureAMD64 Architecture = "amd64"
)

func NewArchitectureFromString(rawArch string) (Architecture, error) {
	switch rawArch {
	case "", string(ArchitectureARM64):
		return ArchitectureARM64, nil
	case string(ArchitectureAMD64):
		return ArchitectureAMD64, nil
	default:
		return "", fmt.Errorf("unsupported architecture: %q", rawArch)
	}
}

func (arch *Architecture) UnmarshalJSON(data []byte) error {
	var rawArch string

	if err := json.Unmarshal(data, &rawArch); err != nil {
		return err
	}

	parsedArch, err := NewArchitectureFromString(rawArch)
	if err != nil {
		return err
	}

	*arch = parsedArch

	return nil
}

type Runtime string

const (
	RuntimeTart Runtime = "tart"
	RuntimeVetu Runtime = "vetu"
)

func NewRuntimeFromString(rawRuntime string) (Runtime, error) {
	switch rawRuntime {
	case "", string(RuntimeTart):
		return RuntimeTart, nil
	case string(RuntimeVetu):
		return RuntimeVetu, nil
	default:
		return "", fmt.Errorf("unsupported runtime: %q", rawRuntime)
	}
}

func (runtime *Runtime) UnmarshalJSON(data []byte) error {
	var rawRuntime string

	if err := json.Unmarshal(data, &rawRuntime); err != nil {
		return err
	}

	parsedRuntime, err := NewRuntimeFromString(rawRuntime)
	if err != nil {
		return err
	}

	*runtime = parsedRuntime

	return nil
}

type PowerState string

const (
	PowerStateRunning   PowerState = "running"
	PowerStateStopped   PowerState = "stopped"
	PowerStateSuspended PowerState = "suspended"
)

func (powerState PowerState) Valid() bool {
	switch powerState {
	case PowerStateRunning, PowerStateStopped, PowerStateSuspended:
		return true
	default:
		return false
	}
}

func (powerState PowerState) TerminalState() bool {
	return powerState != PowerStateRunning
}

type Event struct {
	Kind      EventKind `json:"kind,omitempty"`
	Timestamp int64     `json:"timestamp,omitempty"`
	Payload   string    `json:"payload,omitempty"`
}

type EventKind string

const (
	EventKindLogLine EventKind = "log_line"
)

type VMScript struct {
	ScriptContent string            `json:"script_content,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
}

func (vm VM) TerminalState() bool {
	return vm.Status == VMStatusFailed
}

type VMStatus string

func (vmStatus VMStatus) String() string {
	return string(vmStatus)
}

const (
	// VMStatusPending is set by the Controller for all newly-created VM resources.
	VMStatusPending VMStatus = "pending"

	// VMStatusRunning is set by the Worker once it starts running
	// the Virtual Machine associated with this VM resource.
	VMStatusRunning VMStatus = "running"

	// VMStatusFailed is set by both the Controller and the Worker to indicate a failure
	// that prevented the VM resource from reaching the VMStatusRunning state.
	VMStatusFailed VMStatus = "failed"
)

type ControllerCapability string

const (
	ControllerCapabilityRPCV1           ControllerCapability = "rpc-v1"
	ControllerCapabilityRPCV2           ControllerCapability = "rpc-v2"
	ControllerCapabilityVMStateEndpoint ControllerCapability = "vm-state-endpoint"
)

type ControllerCapabilities []ControllerCapability

func (controllerCapabilities ControllerCapabilities) Has(capability ControllerCapability) bool {
	for _, controllerCapability := range controllerCapabilities {
		if controllerCapability == capability {
			return true
		}
	}

	return false
}

type ControllerInfo struct {
	Version      string                 `json:"version,omitempty"`
	Commit       string                 `json:"commit,omitempty"`
	Capabilities ControllerCapabilities `json:"capabilities,omitempty"`
}
