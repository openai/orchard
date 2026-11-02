package structpath_test

import (
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/structpath"
	"github.com/stretchr/testify/require"
)

type EmbeddedRecord struct {
	Name string
}

func TestInvalidPathsReturnNoMatch(t *testing.T) {
	const missingField = "field"
	const nameField = "name"
	record := struct {
		Name      string
		Count     int
		Labels    map[string]string
		Aliases   []string
		CreatedAt time.Time
		hidden    string
	}{Name: "synthetic", Count: 2, CreatedAt: time.Unix(0, 0), hidden: "private"}
	cases := []struct {
		name   string
		target any
		path   []string
	}{
		{name: "string continuation", target: record, path: []string{nameField, missingField}},
		{name: "number continuation", target: record, path: []string{"count", missingField}},
		{name: "map continuation", target: record, path: []string{"labels", missingField}},
		{name: "slice continuation", target: record, path: []string{"aliases", missingField}},
		{name: "trailing separator", target: record, path: []string{nameField, ""}},
		{name: "private field", target: record, path: []string{"hidden"}},
		{name: "private nested field", target: record, path: []string{"createdat", "wall"}},
		{name: "nil target", target: nil, path: []string{nameField}},
		{name: "nil empty path", target: nil},
		{name: "scalar target", target: 1, path: []string{nameField}},
		{name: "nil promoted field", target: struct{ *EmbeddedRecord }{}, path: []string{nameField}},
		{name: "nil stringer", target: struct{ Time *time.Time }{}, path: []string{"time"}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var value string
			var ok bool
			require.NotPanics(t, func() { value, ok = structpath.Lookup(test.target, test.path) })
			require.False(t, ok)
			require.Empty(t, value)
		})
	}
}

func TestValidPathsKeepExistingLookupBehavior(t *testing.T) {
	const nestedValue = "nested"
	timestamp := time.Unix(0, 0).UTC()
	record := struct {
		*EmbeddedRecord

		Nested    struct{ Name string }
		CreatedAt time.Time
	}{EmbeddedRecord: &EmbeddedRecord{Name: "promoted"}, CreatedAt: timestamp}
	record.Nested.Name = nestedValue
	for _, test := range []struct {
		path []string
		want string
	}{
		{path: []string{"NAME"}, want: "promoted"},
		{path: []string{"nested", "Name"}, want: nestedValue},
		{path: []string{"createdat"}, want: timestamp.String()},
	} {
		t.Run(test.want, func(t *testing.T) {
			value, ok := structpath.Lookup(record, test.path)
			require.True(t, ok)
			require.Equal(t, test.want, value)
		})
	}
	value, ok := structpath.Lookup("synthetic", nil)
	require.True(t, ok)
	require.Equal(t, "synthetic", value)
}
