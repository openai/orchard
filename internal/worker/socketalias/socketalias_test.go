package socketalias_test

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cirruslabs/orchard/internal/worker/socketalias"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestDialContextWithLongPath(t *testing.T) {
	const (
		socketName                = "control.sock"
		unixSocketPathLimit       = len(unix.RawSockaddrUnix{}.Path)
		doubleUnixSocketPathLimit = 2 * unixSocketPathLimit
	)

	// Create the socket at an overlong absolute path without passing that path to bind
	originalWorkingDirectory, err := os.Getwd()
	require.NoError(t, err)

	socketDir := filepath.Join(t.TempDir(), strings.Repeat("x", doubleUnixSocketPathLimit))
	require.NoError(t, os.MkdirAll(socketDir, 0o700))
	socketPath := filepath.Join(socketDir, socketName)

	t.Chdir(socketDir)

	listener, err := (&net.ListenConfig{}).Listen(t.Context(), "unix", socketName)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, listener.Close())
	})

	t.Chdir(originalWorkingDirectory)

	// Verify that connecting through the long absolute path fails
	connection, err := (&net.Dialer{}).DialContext(t.Context(), "unix", socketPath)
	require.Error(t, err)
	require.Nil(t, connection)

	// Verify that connecting through a short transient alias succeeds
	connection, err = socketalias.DialContext(t.Context(), socketPath)
	require.NoError(t, err)
	require.NoError(t, connection.Close())
}
