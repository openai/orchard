package tests_test

import (
	"testing"

	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	"github.com/cirruslabs/orchard/internal/worker"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestUSBAccessoriesSpecUpdate(t *testing.T) {
	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t, false, []controller.Option{controller.WithSynthetic()},
		true, []worker.Option{worker.WithSynthetic()},
	)

	for _, test := range []struct {
		name           string
		usbAccessories bool
		suspendable    bool
	}{
		{name: "enable"},
		{name: "disable", usbAccessories: true},
		{name: "suspendable-enable", suspendable: true},
		{name: "suspendable-disable", usbAccessories: true, suspendable: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.NoError(t, devClient.VMs().Create(t.Context(), &v1.VM{
				Name:           test.name,
				Image:          "example.com/test:latest",
				USBAccessories: test.usbAccessories,
				Suspendable:    test.suspendable,
			}))
			vm, err := devClient.VMs().Get(t.Context(), test.name)
			require.NoError(t, err)
			generation := vm.Generation
			vm.USBAccessories = !vm.USBAccessories

			updated, err := devClient.VMs().Update(t.Context(), *vm)
			if test.suspendable {
				require.ErrorContains(t, err, `"usbAccessories" cannot be toggled for suspendable VMs`)
				unchanged, getErr := devClient.VMs().Get(t.Context(), test.name)
				require.NoError(t, getErr)
				require.Equal(t, generation, unchanged.Generation)
				require.Equal(t, test.usbAccessories, unchanged.USBAccessories)
				return
			}

			require.NoError(t, err)
			require.Equal(t, generation+1, updated.Generation)
			require.Equal(t, !test.usbAccessories, updated.USBAccessories)
			require.False(t, updated.Suspendable)
		})
	}
}
