package v1_test

import (
	"encoding/json"
	"testing"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestRootDiskOptsSpecification(t *testing.T) {
	var vm v1.VM
	require.NoError(t, json.Unmarshal([]byte(`{"runtime":"tart","rootDiskOpts":"sync=fsync"}`), &vm))
	require.Equal(t, "sync=fsync", vm.RootDiskOpts)
	require.NoError(t, vm.Validate())

	vm.Runtime = v1.RuntimeVetu
	require.ErrorContains(t, vm.Validate(), `does not support field "rootDiskOpts"`)
	vm.RootDiskOpts = ""
	require.NoError(t, vm.Validate())
}
