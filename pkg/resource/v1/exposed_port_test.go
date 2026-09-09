package v1_test

import (
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNewExposedPortFromString(t *testing.T) {
	valid := map[string]v1.ExposedPort{
		"1:1":         {External: 1, Internal: 1},
		"2222:22":     {External: 2222, Internal: 22},
		"08080:80":    {External: 8080, Internal: 80},
		"65535:1":     {External: 65535, Internal: 1},
		"65535:65535": {External: 65535, Internal: 65535},
	}

	for entry, expected := range valid {
		exposedPort, err := v1.NewExposedPortFromString(entry)
		require.NoError(t, err, entry)
		require.Equal(t, expected, exposedPort, entry)
	}

	invalid := []string{
		"",
		"2222",
		"2222:22:1",
		"2222:",
		":22",
		"a:22",
		"0:22",
		"22:0",
		"65536:22",
		"-1:22",
		"+1:22",
		" 2222:22",
		"2_222:22",
		"0x8ae:22",
	}

	for _, entry := range invalid {
		_, err := v1.NewExposedPortFromString(entry)
		require.ErrorIs(t, err, v1.ErrInvalidExposedPort, entry)
	}
}
