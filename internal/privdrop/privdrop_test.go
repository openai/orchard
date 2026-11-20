//go:build unix

package privdrop_test

import (
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"testing"

	"github.com/cirruslabs/orchard/internal/privdrop"
	"github.com/stretchr/testify/require"
)

const testDropUserEnv = "ORCHARD_TEST_DROP_USER"

func TestDrop(t *testing.T) {
	if name := os.Getenv(testDropUserEnv); name != "" {
		target, err := user.Lookup(name)
		require.NoError(t, err)

		err = privdrop.Drop(name)
		require.NoError(t, err)

		require.Equal(t, target.Uid, strconv.Itoa(os.Getuid()))
		require.Equal(t, target.Uid, strconv.Itoa(os.Geteuid()))
		require.Equal(t, target.Gid, strconv.Itoa(os.Getgid()))
		require.Equal(t, target.Gid, strconv.Itoa(os.Getegid()))
		require.Equal(t, target.HomeDir, os.Getenv("HOME"))
		return
	}

	target, err := user.Current()
	require.NoError(t, err)

	if os.Geteuid() == 0 {
		// Exercise a real UID and GID transition when the test runs as root.
		target, err = user.Lookup("daemon")
		require.NoError(t, err)
	} else if target.Gid != strconv.Itoa(os.Getgid()) && target.Gid != strconv.Itoa(os.Getegid()) {
		t.Skip("an unprivileged process cannot switch to the account's primary group")
	}

	executable, err := os.Executable()
	require.NoError(t, err)

	// Change credentials only in the child so the test runner can clean up its files.
	// #nosec G204 -- The executable is this test binary, with a fixed test selection.
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestDrop$")
	cmd.Env = append(os.Environ(), testDropUserEnv+"="+target.Username, "HOME="+t.TempDir())

	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}
