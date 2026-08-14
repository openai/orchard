//nolint:modernize,noctx,testpackage // Preserve the original integration-test setup and process helper.
package tests

import (
	"context"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	"github.com/cirruslabs/orchard/internal/tests/wait"
	"github.com/cirruslabs/orchard/internal/worker"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
)

const integrationHelperArg = "--orchard-host-process-integration-test-helper"

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == integrationHelperArg {
		os.Exit(runIntegrationEchoHelper())
	}

	os.Exit(m.Run())
}

func TestHostProcessIntegration(t *testing.T) {
	// Use this test binary as an echo host process
	executable, err := os.Executable()
	require.NoError(t, err)

	const startupMarker = "synthetic VM started canary (from startup script)"

	// Create a development environment with a synthetic Controller and Worker
	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t,
		false,
		[]controller.Option{controller.WithSynthetic()},
		false,
		[]worker.Option{worker.WithSynthetic()},
	)

	// Advertise the original test's Tart platform even on a Linux synthetic worker.
	workers, err := devClient.Workers().List(t.Context())
	require.NoError(t, err)
	require.Len(t, workers, 1)
	workers[0].Arch = v1.ArchitectureARM64
	workers[0].Runtime = v1.RuntimeTart
	workers[0].Resources[v1.ResourceTartVMs] = 1
	_, err = devClient.Workers().Create(t.Context(), workers[0])
	require.NoError(t, err)

	// Create a VM without any host processes
	const vmName = "test-vm"

	err = devClient.VMs().Create(t.Context(), &v1.VM{
		Meta:          v1.Meta{Name: vmName},
		Image:         "synthetic",
		StartupScript: &v1.VMScript{ScriptContent: startupMarker},
	})
	require.NoError(t, err)

	// Wait for the VM to start
	var vm *v1.VM

	require.True(t, wait.Wait(time.Minute, func() bool {
		vm, err = devClient.VMs().Get(t.Context(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)

		return vm.Status == v1.VMStatusRunning
	}), "failed to wait for the VM to start")

	// Add an echo host process to the running VM
	const hostProcessName = "echo"

	vm.HostProcesses = []v1.HostProcess{{
		Name:    hostProcessName,
		Program: executable,
		Args:    []string{integrationHelperArg},
	}}

	vm, err = devClient.VMs().Update(t.Context(), *vm)
	require.NoError(t, err)
	require.EqualValues(t, 1, vm.Generation)
	require.EqualValues(t, 0, vm.ObservedGeneration)

	// Wait for the Worker to start the host process without restarting the VM
	require.True(t, wait.Wait(time.Minute, func() bool {
		vm, err = devClient.VMs().Get(t.Context(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the host process to start. Current observed generation: %d", vm.ObservedGeneration)

		return vm.Status == v1.VMStatusRunning &&
			vm.ObservedGeneration == 1 &&
			v1.ConditionIsTrue(vm.Conditions, v1.ConditionTypeHostProcessesReady)
	}), "failed to wait for the host process to start")

	// Connect to the host process through the Controller
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	connection, err := devClient.VMs().PortForwardHostProcess(ctx, vmName, hostProcessName, 30)
	require.NoError(t, err)
	defer connection.Close()

	require.NoError(t, connection.SetDeadline(time.Now().Add(10*time.Second)))

	message := []byte("Hello, World!")

	_, err = connection.Write(message)
	require.NoError(t, err)

	response := make([]byte, len(message))

	_, err = io.ReadFull(connection, response)
	require.NoError(t, err)
	require.Equal(t, message, response)

	// The startup script would run again if applying the update restarted the VM
	logLines, err := devClient.VMs().Logs(t.Context(), vmName)
	require.NoError(t, err)
	require.Equal(t, []string{startupMarker}, logLines)
}

func runIntegrationEchoHelper() int {
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
		go func() {
			defer connection.Close()
			_, _ = io.Copy(connection, connection)
		}()
	}
}
