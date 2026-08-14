//nolint:noctx,testpackage,usetesting // Preserve the socket-path tests and their required short /var/tmp paths.
package hostprocess

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShortenControlSocketPath(t *testing.T) {
	// Create a control socket path that exceeds macOS's 104-byte limit
	const controlSocketName = "control.sock"

	vmDir := filepath.Join(t.TempDir(), strings.Repeat("v", 104))
	require.NoError(t, os.MkdirAll(vmDir, 0o700))

	controlSocketPath := filepath.Join(vmDir, controlSocketName)
	require.Greater(t, len(controlSocketPath), 104)

	// Create Orchard's short per-process runtime directory
	runtimeDir, err := os.MkdirTemp("/var/tmp", "orchard-hp-test-")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(runtimeDir))
	})

	// Shorten the control socket path and verify the resulting alias
	shortControlSocketPath, err := shortenControlSocketPath(runtimeDir, controlSocketPath)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(runtimeDir, "vm.sock"), shortControlSocketPath)
	require.Less(t, len(shortControlSocketPath), 104)

	// Model Tart binding the socket relative to the long VM directory
	t.Chdir(vmDir)

	listener, err := net.Listen("unix", controlSocketName)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
	})

	// Model the host process running from Orchard's runtime directory
	t.Chdir(runtimeDir)

	// Verify connecting through the long absolute path fails
	connection, err := net.Dial("unix", controlSocketPath)
	require.Error(t, err)
	require.Nil(t, connection)

	// Connect to Tart's control socket through Orchard's short alias
	connection, err = net.Dial("unix", shortControlSocketPath)
	require.NoError(t, err)
	require.NoError(t, connection.Close())
}
