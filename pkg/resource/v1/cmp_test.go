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
		NetSoftnetAllow: []string{},
		NetSoftnetBlock: []string{},
	}
	require.True(t, v1.SemanticallyEqual(nilSlicesSpec, emptySlicesSpec))
}
