package scheduler_test

import (
	"github.com/cirruslabs/orchard/internal/controller/scheduler"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWorkerInfos(t *testing.T) {
	workerInfos := make(scheduler.WorkerInfos)
	require.Len(t, workerInfos, 0)

	var firstVM v1.VM
	firstVM.Resources = v1.Resources{
		"tart-vms": 1,
	}
	firstVM.NetSoftnetExpose = []string{"2222:22"}

	workerInfos.AddVM("worker-name", firstVM)
	require.Len(t, workerInfos, 1)

	var secondVM v1.VM
	secondVM.Resources = v1.Resources{
		"tart-vms": 1,
	}
	secondVM.NetSoftnetExpose = []string{"2223:22", "08080:80", "bogus"}

	workerInfos.AddVM("worker-name", secondVM)
	require.Len(t, workerInfos, 1)
	require.Equal(t, scheduler.WorkerInfo{
		ResourcesUsed: map[string]uint64{
			"tart-vms": 2,
		},
		NumRunningVMs: 2,
		UsedPorts: map[uint16]struct{}{
			2222: {},
			2223: {},
			8080: {},
		},
	}, workerInfos.Get("worker-name"))

	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2222:80")))
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2224:22", "8080:80")))
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("002222:80")))
	require.False(t, workerInfos.PortConflict("worker-name", exposingVM("2224:22", "22", "")))
	require.False(t, workerInfos.PortConflict("worker-name", v1.VM{}))
	require.False(t, workerInfos.PortConflict("other-worker-name", exposingVM("2222:22")))
}

func exposingVM(netSoftnetExpose ...string) v1.VM {
	var vm v1.VM
	vm.NetSoftnetExpose = netSoftnetExpose

	return vm
}

func endpointVM(name string, min uint16, max uint16) v1.VM {
	var vm v1.VM
	vm.Endpoints = []v1.EndpointSpec{
		{
			Name:            name,
			Target:          v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 22}},
			WorkerPortRange: &v1.PortRange{Min: min, Max: max},
		},
	}

	return vm
}

// Endpoint listeners bind worker ports too, so a Softnet-exposing VM must not be
// scheduled onto a port an endpoint already holds, or is pinned to by a single-port range.
func TestWorkerInfosEndpointPorts(t *testing.T) {
	workerInfos := make(scheduler.WorkerInfos)

	var boundVM v1.VM
	boundVM.Endpoints = []v1.EndpointSpec{
		{Name: "ssh", Target: v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 22}},
			WorkerPortRange: &v1.PortRange{Min: 2200, Max: 2299}},
	}
	boundVM.ObservedEndpoints = []v1.EndpointStatus{
		{Name: "ssh", WorkerPort: 2240, State: v1.EndpointStateListening},
	}

	var pinnedVM v1.VM
	pinnedVM.Endpoints = []v1.EndpointSpec{
		// not bound yet, but the range leaves the worker no choice
		{Name: "ssh", Target: v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 22}},
			WorkerPortRange: &v1.PortRange{Min: 2245, Max: 2245}},
		// a wider range picks a port only at bind time, so nothing can be reserved for it
		{Name: "http", Target: v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 80}},
			WorkerPortRange: &v1.PortRange{Min: 2250, Max: 2260}},
	}

	workerInfos.AddVM("worker-name", boundVM)
	workerInfos.AddVM("worker-name", pinnedVM)

	// the listener on 2240 is counted by its claim, not as a port of its own
	require.Equal(t, map[uint16]struct{}{2245: {}}, workerInfos.Get("worker-name").UsedPorts)

	// a Softnet VM must not be placed on a port an endpoint holds, and neither must a VM
	// whose own endpoint is pinned to it
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2240:22")))
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2245:22")))
	require.True(t, workerInfos.PortConflict("worker-name", endpointVM("ssh", 2240, 2240)))
	// the ports of the range the other endpoint is still waiting for are spoken for too
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2250:22")))
	require.False(t, workerInfos.PortConflict("worker-name", exposingVM("2300:22")))
	require.False(t, workerInfos.PortConflict("worker-name", endpointVM("ssh", 2250, 2260)))
}

// A listener binds any port of its range, so only a worker that leaves the range no
// port at all is a conflict.
func TestWorkerInfosEndpointRangeTaken(t *testing.T) {
	workerInfos := make(scheduler.WorkerInfos)

	workerInfos.AddVM("worker-name", exposingVM("2222:22", "2223:23"))

	require.True(t, workerInfos.PortConflict("worker-name", endpointVM("ssh", 2222, 2223)))
	require.False(t, workerInfos.PortConflict("worker-name", endpointVM("ssh", 2222, 2224)))
	require.False(t, workerInfos.PortConflict("worker-name", endpointVM("ssh", 2224, 2225)))
}

// A listener scans its range from the start every time the worker creates it, so the
// whole range stays with the listeners whether one has bound or not.
func TestWorkerInfosEndpointRangeIsReserved(t *testing.T) {
	workerInfos := make(scheduler.WorkerInfos)

	boundVM := endpointVM("ssh", 2222, 2224)
	boundVM.ObservedEndpoints = []v1.EndpointStatus{
		{Name: "ssh", WorkerPort: 2224, State: v1.EndpointStateListening},
	}

	workerInfos.AddVM("worker-name", boundVM)

	require.Equal(t, []scheduler.ListenerClaim{{Range: v1.PortRange{Min: 2222, Max: 2224}, Port: 2224}},
		workerInfos.Get("worker-name").ListenerClaims)

	// the port the listener holds now, and the ones it would scan again after a
	// specification change or a restart
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2224:22")))
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2222:22")))
	require.False(t, workerInfos.PortConflict("worker-name", exposingVM("2225:22")))

	// another listener can still join, since the two settle between themselves
	require.False(t, workerInfos.PortConflict("worker-name", endpointVM("http", 2222, 2224)))
}

// A listener that is on a port of its range needs no second one, so an endpoint with
// the same range still fits beside it.
func TestWorkerInfosEndpointRangeCountsOnce(t *testing.T) {
	workerInfos := make(scheduler.WorkerInfos)

	boundVM := endpointVM("ssh", 2222, 2223)
	boundVM.ObservedEndpoints = []v1.EndpointStatus{
		{Name: "ssh", WorkerPort: 2222, State: v1.EndpointStateListening},
	}

	workerInfos.AddVM("worker-name", boundVM)

	require.Empty(t, workerInfos.Get("worker-name").UsedPorts)
	require.False(t, workerInfos.PortConflict("worker-name", endpointVM("http", 2222, 2223)))

	// the two of them fill the range, and a third has nowhere to go
	workerInfos.AddVM("worker-name", endpointVM("http", 2222, 2223))
	require.True(t, workerInfos.PortConflict("worker-name", endpointVM("other", 2222, 2223)))
}

// Listeners already on a worker can outnumber the ports of their ranges, since
// endpoints added to a scheduled VM never pass the scheduler. The VMs that come after
// them are not the cause and are let through.
func TestWorkerInfosEndpointRangesAlreadyExhausted(t *testing.T) {
	workerInfos := make(scheduler.WorkerInfos)

	workerInfos.AddVM("worker-name", endpointVM("ssh", 2222, 2223))
	workerInfos.AddVM("worker-name", endpointVM("ssh", 2222, 2223))
	workerInfos.AddVM("worker-name", endpointVM("ssh", 2222, 2223))

	require.False(t, workerInfos.PortConflict("worker-name", v1.VM{}))
	require.False(t, workerInfos.PortConflict("worker-name", exposingVM("2224:22")))
	require.False(t, workerInfos.PortConflict("worker-name", endpointVM("http", 2222, 2223)))

	// the ports those listeners draw from are still theirs
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2223:22")))
}

// Endpoints need a port each, and the range that ends first has to be
// served first, or a wider one takes the single port it could have had.
func TestWorkerInfosEndpointRangesCompete(t *testing.T) {
	workerInfos := make(scheduler.WorkerInfos)

	workerInfos.AddVM("worker-name", endpointVM("ssh", 2228, 2229))
	workerInfos.AddVM("worker-name", endpointVM("ssh", 2229, 2230))

	// the three of them fit in 2228-2230
	require.False(t, workerInfos.PortConflict("worker-name", endpointVM("ssh", 2228, 2230)))

	// a third listener waiting leaves the fourth nowhere to go
	workerInfos.AddVM("worker-name", endpointVM("ssh", 2228, 2230))
	require.True(t, workerInfos.PortConflict("worker-name", endpointVM("ssh", 2229, 2230)))
}

// An endpoint moved from one fixed port to another keeps the port it bound until the
// worker applies the new generation, so both are taken meanwhile.
func TestWorkerInfosEndpointUpdatePending(t *testing.T) {
	workerInfos := make(scheduler.WorkerInfos)

	movedVM := endpointVM("ssh", 3333, 3333)
	movedVM.ObservedEndpoints = []v1.EndpointStatus{
		{Name: "ssh", WorkerPort: 2222, State: v1.EndpointStateListening},
	}

	workerInfos.AddVM("worker-name", movedVM)

	require.Equal(t, map[uint16]struct{}{2222: {}, 3333: {}}, workerInfos.Get("worker-name").UsedPorts)
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("2222:22")))
	require.True(t, workerInfos.PortConflict("worker-name", exposingVM("3333:22")))
}

func TestWorkerPortConflict(t *testing.T) {
	newVM := func(name string, worker string, scheduled v1.ConditionState, netSoftnetExpose ...string) v1.VM {
		var vm v1.VM
		vm.Name = name
		vm.Worker = worker
		vm.NetSoftnetExpose = netSoftnetExpose
		vm.Conditions = []v1.Condition{{
			Type:  v1.ConditionTypeScheduled,
			State: scheduled,
		}}

		return vm
	}

	vms := []v1.VM{
		newVM("first", "worker-a", v1.ConditionStateTrue, "2222:22", "8080:80"),
		newVM("second", "worker-b", v1.ConditionStateTrue, "2223:22"),
		// De-scheduled after a failure, so its port is free again
		newVM("third", "worker-a", v1.ConditionStateFalse, "9090:90"),
	}

	for _, test := range []struct {
		name             string
		worker           string
		ignoredVM        string
		netSoftnetExpose []string
		conflict         bool
	}{
		{
			name:             "port held by another VM on the worker",
			worker:           "worker-a",
			ignoredVM:        "new",
			netSoftnetExpose: []string{"2222:80"},
			conflict:         true,
		},
		{
			name:             "one of the ports held by another VM on the worker",
			worker:           "worker-a",
			ignoredVM:        "new",
			netSoftnetExpose: []string{"2224:22", "8080:80"},
			conflict:         true,
		},
		{
			name:             "ports held by the ignored VM itself",
			worker:           "worker-a",
			ignoredVM:        "first",
			netSoftnetExpose: []string{"2222:80", "8080:80"},
			conflict:         false,
		},
		{
			name:             "port held on another worker",
			worker:           "worker-a",
			ignoredVM:        "new",
			netSoftnetExpose: []string{"2223:22"},
			conflict:         false,
		},
		{
			name:             "port held by a de-scheduled VM",
			worker:           "worker-a",
			ignoredVM:        "new",
			netSoftnetExpose: []string{"9090:90"},
			conflict:         false,
		},
		{
			name:      "nothing exposed",
			worker:    "worker-a",
			ignoredVM: "new",
			conflict:  false,
		},
		{
			name:             "unknown worker",
			worker:           "worker-c",
			ignoredVM:        "new",
			netSoftnetExpose: []string{"2222:22"},
			conflict:         false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.conflict, scheduler.WorkerPortConflict(vms, test.worker, test.ignoredVM,
				exposingVM(test.netSoftnetExpose...)))
		})
	}
}
