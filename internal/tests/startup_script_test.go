//go:build darwin

package tests_test

import (
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	"github.com/cirruslabs/orchard/internal/tests/platformdependent"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTartGuestAgentStartupScript(t *testing.T) {
	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	// Create a VM whose startup script runs through Tart Guest Agent
	vm := platformdependent.VM("test-agent-startup-" + uuid.NewString())

	// Agent execution must work without valid SSH credentials.
	vm.Username = "invalid-ssh-user"
	vm.Password = "invalid-ssh-password"
	vm.StartupScript = &v1.VMScript{
		Transport: v1.VMScriptTransportTartGuestAgent,
		ScriptContent: "printf 'Hello, %s!\\n' \"$FOO\"\n" +
			"printf 'startup stderr\\n' >&2\nexit 123",
		Env: map[string]string{"FOO": "Tart Guest Agent"},
	}
	require.NoError(t, devClient.VMs().Create(t.Context(), vm))

	// Ensure that the script's non-zero exit status is reported as a VM failure
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		current, err := devClient.VMs().Get(t.Context(), vm.Name)
		require.NoError(collect, err)
		require.Equal(collect, v1.VMStatusFailed, current.Status)
		require.Contains(collect, current.StatusMessage, "failed to run startup script: process exited with status 123")

		// Ensure that the script received FOO and both stdout and stderr were captured
		logs, err := devClient.VMs().Logs(t.Context(), vm.Name)
		require.NoError(collect, err)
		require.Contains(collect, logs, "Hello, Tart Guest Agent!")
		require.Contains(collect, logs, "startup stderr")
	}, 2*time.Minute, time.Second)
}
