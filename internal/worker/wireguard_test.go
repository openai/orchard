package worker //nolint:testpackage // The regression tests exercise unexported worker reconciliation.

import (
	"context"
	"testing"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/runtime"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/base"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/synthetic"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type wireGuardTestVM struct {
	delayedStopVM

	updateErr  error
	wireGuard  *v1.WireGuardSpec
	clearCalls int
}

func (vm *wireGuardTestVM) SetWireGuard(
	_ context.Context, wireGuard v1.WireGuardSpec,
) (*v1.WireGuardStatus, error) {
	if vm.updateErr != nil {
		return nil, vm.updateErr
	}

	vm.wireGuard = &wireGuard

	return &v1.WireGuardStatus{
		ListenPort: 21000, PublicKey: "3p7bfXt9wbTTW2HC7OQ1Nz+DQ8hbeGdNrfx+FG+IK08=",
	}, nil
}

func (vm *wireGuardTestVM) ClearWireGuard(context.Context) error {
	vm.clearCalls++
	if vm.updateErr != nil {
		return vm.updateErr
	}

	vm.wireGuard = nil

	return nil
}

func newWireGuardTestVM(t *testing.T) (*Worker, *wireGuardTestVM) {
	t.Helper()
	logger := zap.NewNop().Sugar()
	worker := &Worker{logger: logger, runtime: runtime.NewSynthetic()}
	vm := &wireGuardTestVM{
		VM: &synthetic.VM{VM: base.NewVM(v1.VM{}, ondiskname.OnDiskName{}, logger)},
		resource: v1.VM{
			Meta: v1.Meta{Name: "wireguard"}, Generation: 7, ObservedGeneration: 7,
			VMSpec: v1.VMSpec{
				PowerState: v1.PowerStateRunning, NetSoftnet: true,
				WireGuard: &v1.WireGuardSpec{
					Address: "10.77.0.2",
					Peers:   []v1.WireGuardPeer{},
					Routes:  []string{"10.77.0.0/16"},
				},
			},
		},
		conditions:  []v1.Condition{{Type: v1.ConditionTypeRunning, State: v1.ConditionStateTrue}},
		stopStarted: make(chan struct{}),
	}

	return worker, vm
}

func monitorWireGuard(t *testing.T, worker *Worker, vm *wireGuardTestVM, desired *v1.VM) {
	t.Helper()
	require.NoError(t, worker.monitorRunningVM(t.Context(), desired, vm, func(context.Context, v1.VM) error {
		return nil
	}))
}

func TestWireGuardRetriesWithoutGenerationChanges(t *testing.T) {
	worker, vm := newWireGuardTestVM(t)
	desired := vm.Resource()
	monitorWireGuard(t, worker, vm, &desired)
	require.NotNil(t, desired.ObservedWireGuard)

	// A failed update clears readiness even after the generic generation was acknowledged
	desired.Generation++
	wireGuard := *desired.WireGuard
	wireGuard.Routes = []string{"10.77.0.0/24"}
	desired.WireGuard = &wireGuard
	vm.updateErr = context.DeadlineExceeded
	monitorWireGuard(t, worker, vm, &desired)
	require.Nil(t, desired.ObservedWireGuard)
	require.Equal(t, desired.Generation, desired.ObservedGeneration)
	require.True(t, vm.Running())

	vm.updateErr = nil
	monitorWireGuard(t, worker, vm, &desired)
	require.NotNil(t, desired.ObservedWireGuard)
	require.Equal(t, desired.WireGuard, vm.wireGuard)
}

func TestWireGuardRemovalWhileRunning(t *testing.T) {
	for name, reattach := range map[string]bool{"retry clear": false, "reattach after lost reply": true} {
		t.Run(name, func(t *testing.T) {
			worker, vm := newWireGuardTestVM(t)
			desired := vm.Resource()
			wireGuard := desired.WireGuard
			monitorWireGuard(t, worker, vm, &desired)
			previousGeneration := desired.Generation

			// An endpoint change in the same PUT must not acknowledge a failed clear
			vm.resource.Endpoints = []v1.EndpointSpec{{Name: "old"}}
			desired.Generation++
			desired.WireGuard = nil
			vm.updateErr = context.DeadlineExceeded
			monitorWireGuard(t, worker, vm, &desired)
			require.Equal(t, previousGeneration, desired.ObservedGeneration)
			require.NotNil(t, desired.ObservedWireGuard)
			require.NotNil(t, vm.Resource().WireGuard)
			require.True(t, vm.Running())

			// A lost clear reply can leave Softnet empty when the original config is requested again
			if reattach {
				vm.wireGuard = nil
				desired.Generation++
				desired.WireGuard = wireGuard
			}

			vm.updateErr = nil
			monitorWireGuard(t, worker, vm, &desired)
			require.Equal(t, desired.Generation, desired.ObservedGeneration)
			require.Equal(t, reattach, desired.ObservedWireGuard != nil)
			require.Equal(t, desired.WireGuard, vm.wireGuard)
			require.Equal(t, desired.VMSpec, vm.Resource().VMSpec)
			require.True(t, vm.Running())
			require.Zero(t, vm.starts)

			clearCalls := vm.clearCalls
			monitorWireGuard(t, worker, vm, &desired)
			require.Equal(t, clearCalls, vm.clearCalls)
		})
	}
}
