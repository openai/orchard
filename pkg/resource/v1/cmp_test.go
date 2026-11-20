package v1_test

import (
	"testing"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

// TestVM ensures that v1.VM and its embedded structs can be compared
// using github.com/google/go-cmp/cmp without causing panics.
func TestVM(t *testing.T) {
	cmp.Equal(v1.VM{}, v1.VM{})
}

func TestSemanticallyEqualEquatesEmptySlices(t *testing.T) {
	nilSlicesSpec := v1.VMSpec{}
	emptySlicesSpec := v1.VMSpec{
		NetSoftnetAllow:      []string{},
		NetSoftnetBlock:      []string{},
		NetSoftnetSubnetPool: []string{},
	}
	require.True(t, v1.SemanticallyEqual(nilSlicesSpec, emptySlicesSpec))
}

func TestVMSpecHostProcessesEqualEquatesEmptySlices(t *testing.T) {
	nilHostProcessesSpec := v1.VMSpec{}
	emptyHostProcessesSpec := v1.VMSpec{
		HostProcesses: []v1.HostProcess{},
	}

	require.True(t, nilHostProcessesSpec.HostProcessesEqual(emptyHostProcessesSpec))
}

func TestSubnetPoolEnablesSoftnet(t *testing.T) {
	spec := v1.VMSpec{NetSoftnetSubnetPool: []string{"192.168.64.0/18"}}

	require.True(t, spec.SoftnetEnabled())
}

func TestVetuRejectsSoftnetSubnetPool(t *testing.T) {
	vm := v1.VM{VMSpec: v1.VMSpec{
		Runtime:              v1.RuntimeVetu,
		NetSoftnetSubnetPool: []string{"192.168.64.0/18"},
	}}

	require.EqualError(t, vm.Validate(), `runtime "vetu" does not support field "netSoftnetSubnetPool"`)
}
