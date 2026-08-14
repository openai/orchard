package vmmanager

import (
	"context"

	"github.com/cirruslabs/orchard/internal/worker/endpoint"
	"github.com/cirruslabs/orchard/internal/worker/hostprocess"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/puzpuzpuz/xsync/v4"
)

type VM interface {
	Resource() v1.VM
	SetResource(vmResource v1.VM)
	UpdateSoftnetPolicy(ctx context.Context, allow []string, block []string) error
	OnDiskName() ondiskname.OnDiskName
	ImageFQN() *string
	EndpointSet() *endpoint.Set
	HostProcessSet() *hostprocess.Set
	Started() bool
	Status() v1.VMStatus
	StatusMessage() string
	Err() error
	Conditions() []v1.Condition

	Start(eventStreamer *client.EventStreamer)
	Suspend() <-chan error
	IP(ctx context.Context) (string, error)
	Stop() <-chan error
	Delete() error
}

//nolint:tagliatelle // The Tart and Vetu CLI JSON fields are capitalized.
type VMInfo struct {
	Name    string `json:"Name"`
	Source  string `json:"Source"`
	State   string `json:"State"`
	Running bool   `json:"Running"`
}

type VMManager struct {
	vms *xsync.Map[ondiskname.OnDiskName, VM]
}

func New() *VMManager {
	return &VMManager{
		vms: xsync.NewMap[ondiskname.OnDiskName, VM](),
	}
}

func (vmm *VMManager) Exists(key ondiskname.OnDiskName) bool {
	_, ok := vmm.vms.Load(key)

	return ok
}

func (vmm *VMManager) Get(key ondiskname.OnDiskName) (VM, bool) {
	vm, ok := vmm.vms.Load(key)

	return vm, ok
}

func (vmm *VMManager) Put(key ondiskname.OnDiskName, vm VM) {
	vmm.vms.Store(key, vm)
}

func (vmm *VMManager) Delete(key ondiskname.OnDiskName) {
	vmm.vms.Delete(key)
}

func (vmm *VMManager) Len() int {
	return vmm.vms.Size()
}

func (vmm *VMManager) List() []VM {
	var vms []VM

	vmm.vms.Range(func(odn ondiskname.OnDiskName, vm VM) bool {
		vms = append(vms, vm)

		return true
	})

	return vms
}
