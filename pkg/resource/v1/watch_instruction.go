//nolint:tagliatelle // Preserve the original vmUID keys for wire compatibility.
package v1

type WatchInstruction struct {
	PortForwardAction *PortForwardAction `json:"portForwardAction,omitempty"`
	SyncVMsAction     *SyncVMsAction     `json:"syncVMsAction,omitempty"`
	ResolveIPAction   *ResolveIPAction   `json:"resolveIPAction,omitempty"`
}

type PortForwardAction struct {
	Session string             `json:"session"`
	VMUID   string             `json:"vmUID"`
	Port    uint16             `json:"port"`
	Target  *PortForwardTarget `json:"target,omitempty"`
}

type PortForwardTarget struct {
	HostProcess *PortForwardTargetHostProcess `json:"hostProcess,omitempty"`
}

type PortForwardTargetHostProcess struct {
	VMUID string `json:"vmUID"`
	Name  string `json:"name"`
}

type SyncVMsAction struct {
	// nothing for now
}

type ResolveIPAction struct {
	Session string `json:"session"`
	VMUID   string `json:"vmUID"`
}
