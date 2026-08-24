package v1_test

import (
	"testing"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

func TestLabelsMatch(t *testing.T) {
	// Two nil labels
	a := v1.Labels(nil)
	b := v1.Labels(nil)
	require.True(t, a.Contains(b))
	require.True(t, b.Contains(a))

	// Two empty labels
	a = map[string]string{}
	b = map[string]string{}
	require.True(t, a.Contains(b))
	require.True(t, b.Contains(a))

	// Two identical labels
	a = map[string]string{"foo": "bar"}
	b = map[string]string{"foo": "bar"}
	require.True(t, a.Contains(b))
	require.True(t, b.Contains(a))

	// Supersets against nil labels
	a = v1.Labels(nil)
	b = map[string]string{"baz": "qux", "foo": "bar"}
	require.False(t, a.Contains(b))
	require.True(t, b.Contains(a))

	// Superset against empty labels
	a = map[string]string{}
	b = map[string]string{"baz": "qux", "foo": "bar"}
	require.False(t, a.Contains(b))
	require.True(t, b.Contains(a))

	// Superset against subset labels
	a = map[string]string{"foo": "bar"}
	b = map[string]string{"baz": "qux", "foo": "bar"}
	require.False(t, a.Contains(b))
	require.True(t, b.Contains(a))
}

func TestLabelsCopy(t *testing.T) {
	original := v1.Labels{"foo": "bar"}
	copied := original.Copy()
	copied["foo"] = "changed"

	require.Equal(t, v1.Labels{"foo": "bar"}, original)
	require.Equal(t, v1.Labels{"foo": "changed"}, copied)
	require.NotNil(t, v1.Labels(nil).Copy())
}

func TestLabelsMerged(t *testing.T) {
	original := v1.Labels{
		"preserved":  "original",
		"overridden": "original",
	}
	overrides := v1.Labels{
		"added":      "override",
		"overridden": "override",
	}

	require.Equal(t, v1.Labels{
		"added":      "override",
		"overridden": "override",
		"preserved":  "original",
	}, original.Merged(overrides))
	require.Equal(t, "original", original["overridden"])
	require.Equal(t, "override", overrides["overridden"])
}
