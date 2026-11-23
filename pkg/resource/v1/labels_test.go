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

func TestLabelsMatchEmptyValue(t *testing.T) {
	const label = "dedicated"
	required := v1.Labels{label: ""}

	require.False(t, v1.Labels(nil).Contains(required))
	require.False(t, v1.Labels{}.Contains(required))
	require.False(t, v1.Labels{"other": ""}.Contains(required))
	require.False(t, v1.Labels{label: "build"}.Contains(required))
	require.True(t, v1.Labels{label: ""}.Contains(required))
	require.True(t, v1.Labels{label: "", "region": "west"}.Contains(required))
}

func TestLabelsMerged(t *testing.T) {
	const (
		originalValue = "original"
		overriddenKey = "overridden"
		overrideValue = "override"
	)

	original := v1.Labels{
		"preserved":   originalValue,
		overriddenKey: originalValue,
	}
	overrides := v1.Labels{
		"added":       overrideValue,
		overriddenKey: overrideValue,
	}

	require.Equal(t, v1.Labels{
		"added":       overrideValue,
		overriddenKey: overrideValue,
		"preserved":   originalValue,
	}, original.Merged(overrides))
	require.Equal(t, originalValue, original[overriddenKey])
	require.Equal(t, overrideValue, overrides[overriddenKey])
}
