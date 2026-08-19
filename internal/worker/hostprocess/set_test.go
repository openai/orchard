//nolint:goconst,noctx,testpackage // Preserve the original helper-process fixtures.
package hostprocess

import (
	"net"
	"os"
	"testing"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

const (
	testHelperArg          = "--orchard-host-process-test-helper"
	testHelperEnv          = "ORCHARD_HOST_PROCESS_TEST_ENV"
	testHelperInheritedEnv = "ORCHARD_HOST_PROCESS_TEST_INHERITED_ENV"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == testHelperArg {
		os.Exit(runTestHelper())
	}

	os.Exit(m.Run())
}

func TestSetStartAndStop(t *testing.T) {
	t.Setenv(testHelperInheritedEnv, "must-not-be-inherited")

	// Use this test binary as a host process
	executable, err := os.Executable()
	require.NoError(t, err)

	// Create an empty host process set
	set := NewSet("", "", ondiskname.OnDiskName{})
	t.Cleanup(set.Stop)

	// Ensure that a new set is not ready until started
	require.False(t, set.Ready())

	// Start the first host process
	require.NoError(t, set.Start(t.Context(), []v1.HostProcess{{
		Name:    "first",
		Program: executable,
		Args:    []string{testHelperArg},
		Env: map[string]string{
			testHelperEnv:            "set",
			"PATH":                   "must-not-win",
			"ORCHARD_PROCESS_SOCKET": "/must-not-win",
		},
	}}))
	require.True(t, set.Ready())

	// Ensure that the first host process is reachable
	connection, err := set.Dial(t.Context(), "first")
	require.NoError(t, err)
	require.NoError(t, connection.Close())

	// A failed replacement leaves the set not ready
	require.Error(t, set.Replace(t.Context(), []v1.HostProcess{{
		Name:    "invalid",
		Program: "/does/not/exist",
	}}))
	require.False(t, set.Ready())
	_, err = set.Dial(t.Context(), "first")
	require.Error(t, err)

	// Replace the first host process
	require.NoError(t, set.Replace(t.Context(), []v1.HostProcess{{
		Name:    "replacement",
		Program: executable,
		Args:    []string{testHelperArg},
		Env: map[string]string{
			testHelperEnv:            "set",
			"PATH":                   "must-not-win",
			"ORCHARD_PROCESS_SOCKET": "/must-not-win",
		},
	}}))
	require.True(t, set.Ready())

	// Ensure that only the replacement host process is reachable
	_, err = set.Dial(t.Context(), "first")
	require.Error(t, err)

	connection, err = set.Dial(t.Context(), "replacement")
	require.NoError(t, err)
	require.NoError(t, connection.Close())

	// Stop all host processes
	set.Stop()
	require.False(t, set.Ready())

	// Ensure that the stopped host process is no longer reachable
	_, err = set.Dial(t.Context(), "replacement")
	require.Error(t, err)

	// Ensure that an explicitly started empty set is ready
	require.NoError(t, set.Start(t.Context(), nil))
	require.True(t, set.Ready())
}

func TestSetStartReplacesExistingProcesses(t *testing.T) {
	// Use this test binary as a host process
	executable, err := os.Executable()
	require.NoError(t, err)

	// Create a host process set and start a new host process
	set := NewSet("", "", ondiskname.OnDiskName{})
	defer set.Stop()

	hostProcesses := []v1.HostProcess{{
		Name:    "process",
		Program: executable,
		Args:    []string{testHelperArg},
		Env: map[string]string{
			testHelperEnv: "set",
		},
	}}

	require.NoError(t, set.Start(t.Context(), hostProcesses))

	// Keep track of the original process to verify it is stopped
	original := set.Lookup("process")
	require.NotNil(t, original)
	defer original.Close()

	// Start a new host process
	require.NoError(t, set.Start(t.Context(), hostProcesses))

	// Ensure that the original process was stopped
	select {
	case <-original.done:
		// It was stopped, nice
	default:
		require.FailNow(t, "the original process is still running")
	}

	// Ensure that a new ready process replaced the original
	replacement := set.Lookup("process")
	require.NotNil(t, replacement)
	require.NotSame(t, original, replacement)
	require.True(t, set.Ready())
}

func runTestHelper() int {
	if os.Getenv(testHelperEnv) != "set" {
		return 3
	}
	if os.Getenv(testHelperInheritedEnv) != "" {
		return 4
	}
	if os.Getenv("PATH") == "must-not-win" {
		return 5
	}

	listener, err := net.Listen("unix", os.Getenv("ORCHARD_PROCESS_SOCKET"))
	if err != nil {
		return 2
	}
	defer listener.Close()

	for {
		connection, err := listener.Accept()
		if err != nil {
			return 0
		}
		_ = connection.Close()
	}
}
