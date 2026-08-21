package v1_test

import (
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"math"
	"testing"
)

func TestResourcesAdd(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs: 1,
	}

	resources.Add(v1.Resources{v1.ResourceTartVMs: 3})

	require.Equal(t, v1.Resources{v1.ResourceTartVMs: 4}, resources)
}

func TestResourcesAdded(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs: 1,
	}

	resources = resources.Added(v1.Resources{v1.ResourceTartVMs: 3})

	require.Equal(t, v1.Resources{v1.ResourceTartVMs: 4}, resources)
}

func TestResourcesSubtract(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs: 2,
	}

	resources.Subtract(v1.Resources{
		v1.ResourceTartVMs: 1,
	})

	require.False(t, resources.CanFit(v1.Resources{
		v1.ResourceTartVMs: 2,
	}))
}

func TestResourcesSubtracted(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs: 2,
	}

	resources = resources.Subtracted(v1.Resources{
		v1.ResourceTartVMs: 1,
	})

	require.False(t, resources.CanFit(v1.Resources{
		v1.ResourceTartVMs: 2,
	}))
}

func TestResourcesSubtractSaturatesAtZero(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs:      2,
		v1.ResourceLogicalCores: 8,
	}

	resources.Subtract(v1.Resources{
		v1.ResourceTartVMs:      3,
		v1.ResourceLogicalCores: 4,
		v1.ResourceMemoryMiB:    1,
	})

	require.Equal(t, v1.Resources{
		v1.ResourceTartVMs:      0,
		v1.ResourceLogicalCores: 4,
		v1.ResourceMemoryMiB:    0,
	}, resources)
	require.False(t, resources.CanFit(v1.Resources{v1.ResourceTartVMs: 1}))
}

func TestResourcesSubtractedSaturatesAtZeroWithoutMutatingOriginal(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs:      2,
		v1.ResourceLogicalCores: 8,
	}

	remaining := resources.Subtracted(v1.Resources{
		v1.ResourceTartVMs:      3,
		v1.ResourceLogicalCores: 4,
		v1.ResourceMemoryMiB:    1,
	})

	require.Equal(t, v1.Resources{
		v1.ResourceTartVMs:      0,
		v1.ResourceLogicalCores: 4,
		v1.ResourceMemoryMiB:    0,
	}, remaining)
	require.Equal(t, v1.Resources{
		v1.ResourceTartVMs:      2,
		v1.ResourceLogicalCores: 8,
	}, resources)
	require.False(t, remaining.CanFit(v1.Resources{v1.ResourceTartVMs: 1}))
}

func TestResourcesCanFit(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs: 2,
	}

	require.True(t, resources.CanFit(v1.Resources{
		v1.ResourceTartVMs: 0,
	}))
	require.True(t, resources.CanFit(v1.Resources{
		v1.ResourceTartVMs: 1,
	}))
	require.True(t, resources.CanFit(v1.Resources{
		v1.ResourceTartVMs: 2,
	}))

	require.False(t, resources.CanFit(v1.Resources{
		v1.ResourceTartVMs: 3,
	}))
	require.False(t, resources.CanFit(v1.Resources{
		v1.ResourceTartVMs: math.MaxUint64,
	}))
}

func TestResourcesMerge(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs: 2,
		"some-other":       5,
	}
	resources.Merge(v1.Resources{
		v1.ResourceTartVMs: 4,
	})
	require.Equal(t, v1.Resources{
		v1.ResourceTartVMs: 4,
		"some-other":       5,
	}, resources)
}

func TestResourcesMerged(t *testing.T) {
	resources := v1.Resources{
		v1.ResourceTartVMs: 2,
		"some-other":       5,
	}
	require.Equal(t, v1.Resources{
		v1.ResourceTartVMs: 4,
		"some-other":       5,
	}, resources.Merged(v1.Resources{
		v1.ResourceTartVMs: 4,
	}))
}

func TestEqual(t *testing.T) {
	//nolint:gocritic // "dupArg: suspicious method call with the same argument and receiver" // it's not suspicious at all
	require.True(t, v1.Resources{}.Equal(v1.Resources{}))

	require.True(t, v1.Resources{"a": 10.0}.Equal(v1.Resources{"a": 10.0}))

	require.False(t, v1.Resources{"a": 10.0}.Equal(v1.Resources{"a": 10.0, "b": 15.0}))

	require.False(t, v1.Resources{"a": 10.0, "b": 15.0}.Equal(v1.Resources{"a": 10.0}))

	require.False(t, v1.Resources{"a": 0.0}.Equal(v1.Resources{"b": 0.0}))
}
