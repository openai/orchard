package tests_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/tests/platformdependent"
	"github.com/cirruslabs/orchard/internal/tests/wait"
	"github.com/cirruslabs/orchard/internal/worker"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	controllerRestartHelperEnv                = "ORCHARD_CONTROLLER_RESTART_TEST_HELPER"
	controllerRestartHelperDataDirEnv         = "ORCHARD_CONTROLLER_RESTART_TEST_DATA_DIR"
	controllerRestartHelperListenAddrEnv      = "ORCHARD_CONTROLLER_RESTART_TEST_LISTEN_ADDR"
	controllerRestartHelperOfflineTimeoutEnv  = "ORCHARD_CONTROLLER_RESTART_TEST_OFFLINE_TIMEOUT"
	controllerRestartWorkerOfflineTimeout     = 20 * time.Second
	controllerRestartWorkerDisconnectDuration = 21 * time.Second
)

func TestControllerRestartDoesNotFailRunningVMs(t *testing.T) {
	ctx := t.Context()
	listenAddr := unusedTCPAddress(t)
	dataDir := t.TempDir()

	devClient, err := client.New(client.WithAddress("http://" + listenAddr))
	require.NoError(t, err)

	controllerProcess := startControllerProcess(t, devClient, dataDir, listenAddr)
	devWorker := startSyntheticWorker(t, devClient)

	var workerName string
	require.True(t, wait.Wait(30*time.Second, func() bool {
		workers, err := devClient.Workers().List(ctx)
		if err != nil || len(workers) != 1 {
			return false
		}

		workerName = workers[0].Name

		return true
	}), "failed to wait for the worker to register")

	const vmName = "controller-restart-vm"

	require.NoError(t, devClient.VMs().Create(ctx, platformdependent.VM(vmName)))
	require.True(t, wait.Wait(30*time.Second, func() bool {
		vm, err := devClient.VMs().Get(ctx, vmName)
		if err != nil {
			return false
		}

		t.Logf("Waiting for the synthetic VM to start. Current status: %s", vm.Status)

		return vm.Status == v1.VMStatusRunning
	}), "failed to wait for the synthetic VM to start")

	vmBeforeRestart, err := devClient.VMs().Get(ctx, vmName)
	require.NoError(t, err)
	require.Equal(t, workerName, vmBeforeRestart.Worker)

	// Stop the worker's controller session without stopping its running VMs, then
	// keep the controller down until the persisted worker heartbeat is stale.
	devWorker.stop(t)
	require.NoError(t, controllerProcess.stop())
	time.Sleep(controllerRestartWorkerDisconnectDuration)

	restartedAt := time.Now()
	startControllerProcess(t, devClient, dataDir, listenAddr)

	// Keep the worker disconnected beyond the first post-restart scheduler
	// health check. The VM should remain running during the startup grace period.
	observationDeadline := time.Now().Add(7 * time.Second)
	for time.Now().Before(observationDeadline) {
		vm, err := devClient.VMs().Get(ctx, vmName)
		require.NoError(t, err)
		require.Equal(t, v1.VMStatusRunning, vm.Status)
		require.Equal(t, vmBeforeRestart.UID, vm.UID)
		require.Equal(t, workerName, vm.Worker)

		time.Sleep(250 * time.Millisecond)
	}

	devWorker.start(t)
	require.True(t, wait.Wait(30*time.Second, func() bool {
		workerResource, err := devClient.Workers().Get(ctx, workerName)
		if err != nil || !workerResource.LastSeen.After(restartedAt) {
			return false
		}

		vm, err := devClient.VMs().Get(ctx, vmName)

		return err == nil && vm.Status == v1.VMStatusRunning
	}), "worker did not reconnect with its VM still running after the controller restart")
}

// TestControllerRestartHelperProcess runs an Orchard Controller in a separate
// process so that its Badger database is released and can be reopened during a
// realistic controller restart.
func TestControllerRestartHelperProcess(t *testing.T) {
	if os.Getenv(controllerRestartHelperEnv) != "1" {
		return
	}

	dataDirPath := requireEnv(t, controllerRestartHelperDataDirEnv)
	listenAddr := requireEnv(t, controllerRestartHelperListenAddrEnv)
	offlineTimeout, err := time.ParseDuration(requireEnv(t, controllerRestartHelperOfflineTimeoutEnv))
	require.NoError(t, err)

	dataDir, err := controller.NewDataDir(dataDirPath)
	require.NoError(t, err)

	devController, err := controller.New(
		controller.WithDataDir(dataDir),
		controller.WithListenAddr(listenAddr),
		controller.WithInsecureAuthDisabled(),
		controller.WithExperimentalRPCV2(),
		controller.WithWorkerOfflineTimeout(offlineTimeout),
		controller.WithLogger(zap.NewNop()),
	)
	require.NoError(t, err)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	err = devController.Run(ctx)
	require.True(t, err == nil || errors.Is(err, context.Canceled) || errors.Is(err, http.ErrServerClosed),
		"controller failed: %v", err)
}

type controllerTestProcess struct {
	cmd     *exec.Cmd
	output  bytes.Buffer
	stopped bool
}

func startControllerProcess(
	t *testing.T,
	devClient *client.Client,
	dataDir string,
	listenAddr string,
) *controllerTestProcess {
	t.Helper()

	process := &controllerTestProcess{}
	process.cmd = exec.Command(os.Args[0], "-test.run=^TestControllerRestartHelperProcess$", "-test.v")
	process.cmd.Env = append(os.Environ(),
		controllerRestartHelperEnv+"=1",
		controllerRestartHelperDataDirEnv+"="+dataDir,
		controllerRestartHelperListenAddrEnv+"="+listenAddr,
		controllerRestartHelperOfflineTimeoutEnv+"="+controllerRestartWorkerOfflineTimeout.String(),
	)
	process.cmd.Stdout = &process.output
	process.cmd.Stderr = &process.output

	require.NoError(t, process.cmd.Start())
	t.Cleanup(func() {
		require.NoError(t, process.stop())
	})

	if wait.Wait(10*time.Second, func() bool {
		requestCtx, requestCancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer requestCancel()

		_, err := devClient.Controller().Info(requestCtx)

		return err == nil
	}) {
		return process
	}

	stopErr := process.stop()
	t.Fatalf("controller failed to become ready: %v\n%s", stopErr, process.output.String())

	return nil
}

func (process *controllerTestProcess) stop() error {
	if process.stopped {
		return nil
	}
	process.stopped = true

	signalErr := process.cmd.Process.Signal(os.Interrupt)
	waitCh := make(chan error, 1)
	go func() {
		waitCh <- process.cmd.Wait()
	}()

	select {
	case waitErr := <-waitCh:
		if signalErr != nil && !errors.Is(signalErr, os.ErrProcessDone) {
			return fmt.Errorf("failed to interrupt controller process: %w", signalErr)
		}
		if waitErr != nil {
			return fmt.Errorf("controller process failed: %w\n%s", waitErr, process.output.String())
		}

		return nil
	case <-time.After(10 * time.Second):
		_ = process.cmd.Process.Kill()
		<-waitCh

		return fmt.Errorf("timed out waiting for controller process to stop\n%s", process.output.String())
	}
}

type syntheticTestWorker struct {
	worker *worker.Worker
	cancel context.CancelFunc
	done   chan error
}

func startSyntheticWorker(t *testing.T, devClient *client.Client) *syntheticTestWorker {
	t.Helper()

	devWorker, err := worker.New(devClient, worker.WithSynthetic(), worker.WithLogger(zap.NewNop()))
	require.NoError(t, err)

	testWorker := &syntheticTestWorker{
		worker: devWorker,
	}
	testWorker.start(t)
	t.Cleanup(func() {
		testWorker.stop(t)
		require.NoError(t, testWorker.worker.Close())
	})

	return testWorker
}

func (worker *syntheticTestWorker) start(t *testing.T) {
	t.Helper()
	require.Nil(t, worker.cancel, "synthetic test worker is already running")

	workerCtx, cancelWorker := context.WithCancel(context.Background())
	workerDone := make(chan error, 1)
	worker.cancel = cancelWorker
	worker.done = workerDone

	go func() {
		workerDone <- worker.worker.Run(workerCtx)
	}()
}

func (worker *syntheticTestWorker) stop(t *testing.T) {
	t.Helper()
	if worker.cancel == nil {
		return
	}

	worker.cancel()

	select {
	case err := <-worker.done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("dev worker failed: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for dev worker to stop")
	}

	worker.cancel = nil
	worker.done = nil
}

func unusedTCPAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	listenAddr := listener.Addr().String()
	require.NoError(t, listener.Close())

	return listenAddr
}

func requireEnv(t *testing.T, name string) string {
	t.Helper()

	value := os.Getenv(name)
	require.NotEmpty(t, value, "%s must be set", name)

	return value
}
