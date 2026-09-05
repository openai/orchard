package scheduler

import (
	"cmp"
	"maps"
	"slices"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
)

type WorkerInfo struct {
	ResourcesUsed v1.Resources
	NumRunningVMs int
	// UsedPorts is a set of worker ports the VMs scheduled on this worker hold
	// outright, through Softnet or through an endpoint pinned to a single port.
	UsedPorts map[uint16]struct{}
	// ListenerClaims is what the endpoints drawing from a range of ports lay claim to.
	ListenerClaims []ListenerClaim
}

// ListenerClaim is the port range of one endpoint listener, together with the port it
// holds today, zero when it holds none. A listener scans its range from the start every
// time the worker creates it, so the range stays with it either way; the port only says
// which one of the range it is on for now, and that it needs no second one.
type ListenerClaim struct {
	Range v1.PortRange
	Port  uint16
}

type WorkerInfos map[string]WorkerInfo

func newWorkerInfo() WorkerInfo {
	return WorkerInfo{
		ResourcesUsed: v1.Resources{},
		UsedPorts:     map[uint16]struct{}{},
	}
}

func (workerInfos WorkerInfos) AddVM(name string, vm v1.VM) {
	workerInfo, ok := workerInfos[name]
	if !ok {
		workerInfo = newWorkerInfo()
	}

	workerInfo.ResourcesUsed.Add(vm.Resources)
	workerInfo.NumRunningVMs++

	for _, port := range vmPorts(vm) {
		workerInfo.UsedPorts[port] = struct{}{}
	}

	workerInfo.ListenerClaims = append(workerInfo.ListenerClaims, listenerClaims(vm)...)

	workerInfos[name] = workerInfo
}

func (workerInfos WorkerInfos) Get(name string) WorkerInfo {
	workerInfo, ok := workerInfos[name]
	if !ok {
		workerInfo = newWorkerInfo()

		workerInfos[name] = workerInfo
	}

	return workerInfo
}

// PortConflict reports whether the given VM clashes with the VMs already scheduled on
// the given worker: a worker port it holds or pins is taken, or lies in a range an
// endpoint listener draws from, or the listeners, its own and those of the VMs placed
// before it, cannot be given a port each.
func (workerInfos WorkerInfos) PortConflict(name string, vm v1.VM) bool {
	workerInfo := workerInfos[name]
	ports := vmPorts(vm)

	for _, port := range ports {
		if _, ok := workerInfo.UsedPorts[port]; ok {
			return true
		}

		// a listener takes the lowest port of its range it manages to bind, again from
		// the start every time the worker creates it anew, and never learns which port
		// the scheduler had in mind, so no port of a range is one this VM can hold
		for _, claim := range workerInfo.ListenerClaims {
			if rangeContains(claim.Range, port) {
				return true
			}
		}
	}

	if !workerInfo.exhaustsListenerClaims(listenerClaims(vm), ports) {
		return false
	}

	// the listeners already on the worker can outnumber the ports of their ranges,
	// which endpoints added to a VM that is already scheduled do without passing here
	// at all; that is not this VM's doing and not something it mends by staying away
	return !workerInfo.exhaustsListenerClaims(nil, nil)
}

// exhaustsListenerClaims reports whether the endpoint listeners of this worker,
// together with those of a VM about to join it, are left without a port each once that
// VM takes the given ones. A listener that is on a port keeps it; the rest have to be
// found one, and since one range can hold the port another needs, they are matched
// rather than weighed one by one: serving the range that ends first finds a port for
// every one of them whenever the ports allow it. The question is whether the worker has
// the ports, not which one each listener takes, as two listeners settle between
// themselves: the one that loses a bind moves on.
func (workerInfo WorkerInfo) exhaustsListenerClaims(joining []ListenerClaim, ports []uint16) bool {
	claims := slices.Concat(joining, workerInfo.ListenerClaims)
	if len(claims) == 0 {
		return false
	}

	takenPorts := make(map[uint16]struct{}, len(workerInfo.UsedPorts)+len(ports)+len(claims))
	maps.Copy(takenPorts, workerInfo.UsedPorts)

	for _, port := range ports {
		takenPorts[port] = struct{}{}
	}

	var waiting []v1.PortRange

	for _, claim := range claims {
		if claim.Port == 0 {
			waiting = append(waiting, claim.Range)

			continue
		}

		takenPorts[claim.Port] = struct{}{}
	}

	if len(waiting) == 0 {
		return false
	}

	// a range wider than every taken port and every other range together cannot be
	// left empty by them, which is the ordinary case and needs no matching at all
	narrowest := slices.MinFunc(waiting, func(a, b v1.PortRange) int {
		return cmp.Compare(a.Max-a.Min, b.Max-b.Min)
	})
	if int(narrowest.Max)-int(narrowest.Min)+1 > len(takenPorts)+len(waiting) {
		return false
	}

	slices.SortFunc(waiting, func(a, b v1.PortRange) int {
		return cmp.Compare(a.Max, b.Max)
	})

	for _, portRange := range waiting {
		port, ok := portRange.FreePort(takenPorts)
		if !ok {
			return true
		}

		takenPorts[port] = struct{}{}
	}

	return false
}

// WorkerPortConflict reports whether any worker port the given VM needs is
// already taken by a VM from vms that is scheduled on the given worker,
// ignoring the VM with the given name.
//
// This is the same check that the scheduling loop performs through the
// WorkerInfos it builds with ProcessVMs, so that the pre-filter and the
// re-check inside the scheduling transaction agree on conflicts.
func WorkerPortConflict(vms []v1.VM, workerName, ignoredVMName string, vm v1.VM) bool {
	var otherVMs []v1.VM

	for _, other := range vms {
		if other.Worker == workerName && other.Name != ignoredVMName {
			otherVMs = append(otherVMs, other)
		}
	}

	_, workerInfos := ProcessVMs(otherVMs)

	return workerInfos.PortConflict(workerName, vm)
}

// vmPorts returns the worker ports a VM holds outright, or asks for when it is not
// scheduled yet: the external ports it exposes through Softnet, the ports single-port
// ranges pin its endpoints to, and the port a listener is on outside the range its
// endpoint now asks for, which the worker keeps until it applies the new generation. A
// listener on a port of the range it draws from is left to listenerClaims, which counts
// it once, as a range with a port rather than a port and a range.
func vmPorts(vm v1.VM) []uint16 {
	seen := map[uint16]struct{}{}

	var result []uint16

	add := func(port uint16) {
		if port == 0 {
			return
		}

		if _, ok := seen[port]; ok {
			return
		}

		seen[port] = struct{}{}
		result = append(result, port)
	}

	for _, port := range externalPorts(vm.NetSoftnetExpose) {
		add(port)
	}

	for _, observed := range vm.ObservedEndpoints {
		portRange := endpointRange(vm, observed.Name)

		if portRange != nil && portRange.Min != portRange.Max &&
			rangeContains(*portRange, observed.WorkerPort) {
			continue
		}

		add(observed.WorkerPort)
	}

	for _, endpoint := range vm.Endpoints {
		if portRange := endpoint.WorkerPortRange; portRange != nil && portRange.Min == portRange.Max {
			add(portRange.Min)
		}
	}

	return result
}

// listenerClaims returns what the VM's endpoints lay claim to. The port a listener is
// on is no substitute for its range: the worker tears every listener of a VM down when
// its endpoints change or the VM restarts, and the one it creates in their place scans
// the range again from the start. A range narrowed down to a single port is left out,
// since vmPorts holds that port outright.
func listenerClaims(vm v1.VM) []ListenerClaim {
	var result []ListenerClaim

	for _, endpoint := range vm.Endpoints {
		portRange := endpoint.WorkerPortRange

		if portRange == nil || portRange.Min == portRange.Max {
			continue
		}

		claim := ListenerClaim{Range: *portRange}

		if port, ok := observedPort(vm, endpoint.Name); ok && rangeContains(*portRange, port) {
			claim.Port = port
		}

		result = append(result, claim)
	}

	return result
}

// endpointRange returns the range the VM's endpoint of the given name draws from.
func endpointRange(vm v1.VM, name string) *v1.PortRange {
	for _, endpoint := range vm.Endpoints {
		if endpoint.Name == name {
			return endpoint.WorkerPortRange
		}
	}

	return nil
}

// observedPort returns the port the VM's endpoint of the given name is on.
func observedPort(vm v1.VM, name string) (uint16, bool) {
	for _, observed := range vm.ObservedEndpoints {
		if observed.Name == name && observed.WorkerPort != 0 {
			return observed.WorkerPort, true
		}
	}

	return 0, false
}

func rangeContains(portRange v1.PortRange, port uint16) bool {
	return port >= portRange.Min && port <= portRange.Max
}

// externalPorts extracts the external ports from a list of
// EXTERNAL:INTERNAL entries, skipping the ones that fail to parse
// (the API validates them, so this only guards data written by other means).
func externalPorts(netSoftnetExpose []string) []uint16 {
	var result []uint16

	for _, entry := range netSoftnetExpose {
		exposedPort, err := v1.NewExposedPortFromString(entry)
		if err != nil {
			continue
		}

		result = append(result, exposedPort.External)
	}

	return result
}
