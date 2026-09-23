package v1_test

import (
	"encoding/json"
	"testing"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestUSBAccessoriesSpecification(t *testing.T) {
	var vm v1.VM
	require.NoError(t, json.Unmarshal([]byte(`{"runtime":"tart"}`), &vm))
	require.False(t, vm.USBAccessories)
	require.False(t, vm.Suspendable)
	require.NoError(t, vm.Validate())

	previous := vm.VMSpec
	vm.USBAccessories = true
	require.False(t, v1.SemanticallyEqual(previous, vm.VMSpec))

	encoded, err := json.Marshal(vm.VMSpec)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"usbAccessories":true`)

	vm.Runtime = v1.RuntimeVetu
	require.ErrorContains(t, vm.Validate(), `does not support field "usbAccessories"`)
	vm.USBAccessories = false
	require.NoError(t, vm.Validate())
}
