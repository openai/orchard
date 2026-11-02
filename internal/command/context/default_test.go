package context

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDefaultCommandAliases(t *testing.T) {
	cmd := newDefaultCommand()
	require.Equal(t, "default NAME", cmd.Use)
	require.Contains(t, cmd.Aliases, "set")
	require.Contains(t, cmd.Aliases, "use")
}

func TestContextCommandFindSubcommand(t *testing.T) {
	rootCmd := NewCommand()

	defaultCmd, _, err := rootCmd.Find([]string{"default"})
	require.NoError(t, err)
	require.Equal(t, "default NAME", defaultCmd.Use)

	setCmd, _, err := rootCmd.Find([]string{"set"})
	require.NoError(t, err)
	require.Equal(t, "default NAME", setCmd.Use)

	useCmd, _, err := rootCmd.Find([]string{"use"})
	require.NoError(t, err)
	require.Equal(t, "default NAME", useCmd.Use)
}
