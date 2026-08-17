package tests_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/imageconstant"
	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	"github.com/cirruslabs/orchard/internal/tests/platformdependent"
	"github.com/cirruslabs/orchard/internal/tests/wait"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
	"golang.org/x/exp/slices"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

func TestSingleVM(t *testing.T) {
	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	workers, err := devClient.Workers().List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(workers))

	vm := platformdependent.VM("test-vm")
	vm.StartupScript = &v1.VMScript{
		ScriptContent: "echo \"Hello, $FOO!\"\nfor i in $(seq 1 1000); do echo \"$i\"; done",
		Env:           map[string]string{"FOO": "Bar"},
	}

	err = devClient.VMs().Create(context.Background(), vm)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err := devClient.VMs().Get(context.Background(), "test-vm")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)
		return vm.Status == v1.VMStatusRunning || vm.Status == v1.VMStatusFailed
	}), "failed to start a VM")
	runningVM, err := devClient.VMs().Get(context.Background(), "test-vm")
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, v1.VMStatusRunning, runningVM.Status)
	assert.True(t, wait.Wait(2*time.Minute, func() bool {
		logLines, err := devClient.VMs().Logs(context.Background(), "test-vm")
		if err != nil {
			t.Fatal(err)
		}
		return len(logLines) >= 1000
	}), "failed to wait for logs to become available")
	logLines, err := devClient.VMs().Logs(context.Background(), "test-vm")
	if err != nil {
		t.Fatal(err)
	}
	expectedLogs := []string{"Hello, Bar!"}
	for i := 1; i <= 1000; i++ {
		expectedLogs = append(expectedLogs, strconv.Itoa(i))
	}
	assert.Contains(t, strings.Join(logLines, "\n"), strings.Join(expectedLogs, "\n"))

	// Ensure that the VM exists on disk before deleting it
	require.True(t, hasVMByPredicate(t, func(info vmmanager.VMInfo) bool {
		return strings.Contains(info.Name, runningVM.UID)
	}, nil))

	// Delete the VM from the controller
	require.NoError(t, devClient.VMs().Delete(context.Background(), "test-vm"))

	// Ensure that the worker has deleted this VM from disk
	assert.True(t, wait.Wait(2*time.Minute, func() bool {
		t.Logf("Waiting for the VM to be garbage collected...")

		return !hasVMByPredicate(t, func(info vmmanager.VMInfo) bool {
			return strings.Contains(info.Name, runningVM.UID)
		}, nil)
	}), "VM was not garbage collected in a timely manner")
}

func TestFailedStartupScript(t *testing.T) {
	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	workers, err := devClient.Workers().List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 1, len(workers))

	vm := platformdependent.VM("test-vm")
	vm.StartupScript = &v1.VMScript{
		ScriptContent: "set +e && exit 123",
	}

	err = devClient.VMs().Create(context.Background(), vm)
	if err != nil {
		t.Fatal(err)
	}
	assert.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err := devClient.VMs().Get(context.Background(), "test-vm")
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)
		return vm.Status == v1.VMStatusFailed
	}), "failed to start a VM")
	runningVM, err := devClient.VMs().Get(context.Background(), "test-vm")
	if err != nil {
		t.Fatal(err)
	}
	assert.Contains(t, runningVM.StatusMessage,
		"failed to run startup script: Process exited with status 123")
}

func TestPortForwarding(t *testing.T) {
	ctx := context.Background()

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	// Create a generic macOS VM
	err := devClient.VMs().Create(ctx, platformdependent.VM("test-vm"))
	require.NoError(t, err)

	// Establish port forwarding to VMs SSH port
	wsConn, err := devClient.VMs().PortForward(ctx, "test-vm", 22, 120)
	require.NoError(t, err)

	vm, err := devClient.VMs().Get(ctx, "test-vm")
	require.NoError(t, err)
	require.Equal(t, v1.VMStatusRunning, vm.Status)

	t.Logf("Waiting for the VM to start, current status: %s", vm.Status)

	// Make sure we can connect to the VM over SSH via the forwarded port
	sshConfig := &ssh.ClientConfig{
		HostKeyCallback: func(hostname string, remote net.Addr, key ssh.PublicKey) error {
			return nil
		},
		User: "admin",
		Auth: []ssh.AuthMethod{
			ssh.Password("admin"),
		},
	}

	sshConn, chans, reqs, err := ssh.NewClientConn(wsConn, "", sshConfig)
	require.NoError(t, err)

	sshClient := ssh.NewClient(sshConn, chans, reqs)

	sshSession, err := sshClient.NewSession()
	require.NoError(t, err)

	unameOutput, err := sshSession.Output("uname -a")
	require.NoError(t, err)
	require.Contains(t, string(unameOutput), cases.Title(language.English).String(runtime.GOOS))
}

func TestSparseWorkerUpdatePreservesLastSeen(t *testing.T) {
	ctx := context.Background()

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t,
		false,
		nil,
		true,
		nil,
	)

	lastSeen := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	_, err := devClient.Workers().Create(ctx, v1.Worker{
		Meta: v1.Meta{
			Name: "sparse-update-worker",
		},
		LastSeen:  lastSeen,
		MachineID: "sparse-update-machine",
	})
	require.NoError(t, err)

	_, err = devClient.Workers().Update(ctx, v1.Worker{
		Meta: v1.Meta{
			Name: "sparse-update-worker",
		},
		SchedulingPaused: true,
	})
	require.NoError(t, err)

	worker, err := devClient.Workers().Get(ctx, "sparse-update-worker")
	require.NoError(t, err)
	require.Equal(t, lastSeen, worker.LastSeen)
	require.True(t, worker.SchedulingPaused)
}

// TestSchedulerHealthCheckingNonExistentWorker ensures that scheduler
// will eventually fail VMs that are scheduled on a worker that was
// deleted from the API.
func TestSchedulerHealthCheckingNonExistentWorker(t *testing.T) {
	ctx := context.Background()

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	const (
		dummyWorkerName = "dummy-worker"
		dummyVMName     = "dummy-vm"
	)

	// Prepare a dummy VM
	vm := platformdependent.VM(dummyVMName)
	vm.Resources = map[string]uint64{
		"unique-resource": 1,
	}

	// Create a dummy worker that won't update it's LastSeen
	// timestamp, which will result in scheduler failing VMs
	// scheduled on that worker.
	//
	// We use a special resource "unique-resource" to prevent
	// our dummy VM (see below) from scheduling on any worker
	// other than this one.
	_, err := devClient.Workers().Create(ctx, v1.Worker{
		Meta: v1.Meta{
			Name: dummyWorkerName,
		},
		LastSeen:  time.Now(),
		MachineID: uuid.New().String(),
		Resources: map[string]uint64{
			v1.ResourceTartVMs: 1,
			"unique-resource":  1,
		},
		Arch:    vm.Arch,
		Runtime: vm.Runtime,
	})
	require.NoError(t, err)

	// Create a dummy VM
	err = devClient.VMs().Create(context.Background(), vm)
	require.NoError(t, err)

	// Wait for the dummy VM to get scheduled to a dummy worker
	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err := devClient.VMs().Get(context.Background(), dummyVMName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to be assigned to a dummy worker, current worker: %q", vm.Worker)

		return vm.Worker == dummyWorkerName
	}), "failed to wait for the dummy VM to be assigned to a dummy worker")

	// Delete the dummy worker
	err = devClient.Workers().Delete(ctx, dummyWorkerName)
	require.NoError(t, err)

	// Wait for the scheduler to change the dummy VM's status to "failed"
	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err := devClient.VMs().Get(context.Background(), dummyVMName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to be failed by the scheduler")

		return vm.Status == v1.VMStatusFailed
	}), "VM was not marked as failed in time")

	// Double check VM's status and status message
	vm, err = devClient.VMs().Get(context.Background(), dummyVMName)
	require.NoError(t, err)
	require.Equal(t, v1.VMStatusFailed, vm.Status)
	require.Equal(t, "VM is assigned to a worker that doesn't exist anymore", vm.StatusMessage)
}

// TestSchedulerHealthCheckingOfflineWorker ensures that scheduler
// will eventually fail VMs that are scheduled on a worker that had
// gone offline for a long time.
func TestSchedulerHealthCheckingOfflineWorker(t *testing.T) {
	ctx := context.Background()

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(t,
		false, []controller.Option{controller.WithWorkerOfflineTimeout(1 * time.Minute)},
		false, nil,
	)

	const (
		dummyWorkerName = "dummy-worker"
		dummyVMName     = "dummy-vm"
	)

	// Prepare a dummy VM that will be assigned to our dummy worker
	vm := platformdependent.VM(dummyVMName)
	vm.Resources = map[string]uint64{
		"unique-resource": 1,
	}

	// Create a dummy worker that will be eventually marked as offline
	// because we won't update the LastSeen field
	_, err := devClient.Workers().Create(ctx, v1.Worker{
		Meta: v1.Meta{
			Name: dummyWorkerName,
		},
		LastSeen:  time.Now(),
		MachineID: uuid.New().String(),
		Resources: map[string]uint64{
			v1.ResourceTartVMs: 1,
			"unique-resource":  1,
		},
		Arch:    vm.Arch,
		Runtime: vm.Runtime,
	})
	require.NoError(t, err)

	// Create a dummy VM
	err = devClient.VMs().Create(context.Background(), vm)
	require.NoError(t, err)

	// Wait for the VM to be marked as failed
	assert.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err := devClient.VMs().Get(context.Background(), dummyVMName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to be marked as failed, current status: %s", vm.Status)

		return vm.Status == v1.VMStatusFailed
	}), "VM wasn't marked as failed in a reasonable time")

	// Double-check the VM's status message
	runningVM, err := devClient.VMs().Get(context.Background(), dummyVMName)
	require.NoError(t, err)
	require.Equal(t, v1.VMStatusFailed, runningVM.Status)
	require.Equal(t, "VM is assigned to a worker that lost connection with the controller",
		runningVM.StatusMessage)
}

// TestVMGarbageCollection ensures that on-disk Tart VMs that are managed by Orchard
// and are not present in the API anymore are garbage-collected by the Orchard Worker
// at startup.
func TestVMGarbageCollection(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Create on-disk Tart VM that looks like it's managed by Orchard
	vmName := ondiskname.New("test", uuid.New().String(), 0).String()
	err = platformdependent.CloneDefaultImage(t.Context(), logger.Sugar(), vmName)
	require.NoError(t, err)

	// Make sure that this VM exists
	require.True(t, hasVM(t, vmName, logger))

	// Start the Orchard Worker
	_, _, _ = devcontroller.StartIntegrationTestEnvironment(t)

	// Wait for the Orchard Worker to garbage-collect this VM
	require.True(t, wait.Wait(2*time.Minute, func() bool {
		t.Logf("Waiting for the on-disk VM to be cleaned up by the worker")

		return !hasVM(t, vmName, logger)
	}), "failed to wait for the VM %s to be garbage-collected", vmName)
}

func TestHostDirsDisabledByDefault(t *testing.T) {
	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	err := devClient.VMs().Create(context.Background(), &v1.VM{
		Meta:     v1.Meta{Name: "test-host-dirs-disabled"},
		Image:    imageconstant.DefaultMacosImage,
		HostDirs: []v1.HostDir{{Name: "src", Path: "/Users/ci/src"}},
	})
	require.Error(t, err)
}

func TestHostDirs(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("HostDirs is only supported on macOS with Tart")
	}

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(t,
		false, []controller.Option{controller.WithInsecureAllowHostDirs()},
		false, nil,
	)

	dirToMount := t.TempDir()

	vmName := "test-host-dirs-" + uuid.NewString()

	err := devClient.ClusterSettings().Set(context.Background(), &v1.ClusterSettings{
		HostDirPolicies: []v1.HostDirPolicy{{PathPrefix: dirToMount}},
	})
	require.NoError(t, err)

	scriptContent, err := os.ReadFile(filepath.Join("testdata", "host-dirs.sh"))
	require.NoError(t, err)

	err = devClient.VMs().Create(context.Background(), &v1.VM{
		Meta: v1.Meta{
			Name: vmName,
		},
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
		Status:   v1.VMStatusPending,
		StartupScript: &v1.VMScript{
			ScriptContent: string(scriptContent),
		},
		HostDirs: []v1.HostDir{
			{Name: "readwrite", Path: dirToMount},
			{Name: "readonly", Path: dirToMount, ReadOnly: true},
		},
	})
	require.NoError(t, err)

	var vm *v1.VM

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)

		return vm.Status == v1.VMStatusRunning || vm.Status == v1.VMStatusFailed
	}), "failed to start a VM")

	require.Equal(t, v1.VMStatusRunning, vm.Status)

	var logLines []string

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		logLines, err = devClient.VMs().Logs(context.Background(), vmName)
		require.NoError(t, err)

		return len(logLines) >= 4
	}), "failed to wait for logs to become available")

	fmt.Println(logLines)

	require.Contains(t, strings.Join(logLines, "\n"), strings.Join([]string{
		"Read-write mount exists",
		"Read-only mount exists",
		"Failed to create a file in read-only mount",
		"Successfully created a file in read-write mount",
	}, "\n"))
	require.FileExists(t, filepath.Join(dirToMount, "test-rw.txt"))
	require.NoFileExists(t, filepath.Join(dirToMount, "test-ro.txt"))
}

func TestHostDirsInvalidPolicy(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("HostDirs is only supported on macOS with Tart")
	}

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(t,
		false, []controller.Option{controller.WithInsecureAllowHostDirs()},
		false, nil,
	)

	dirToMount := t.TempDir()

	vmName := "test-host-dirs-" + uuid.NewString()

	// Create a VM without creating any directory policies
	// and make sure we get an error
	vmSpec := &v1.VM{
		Meta: v1.Meta{
			Name: vmName,
		},
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
		Status:   v1.VMStatusPending,
		HostDirs: []v1.HostDir{
			{Name: "test" + uuid.NewString(), Path: dirToMount},
		},
	}

	err := devClient.VMs().Create(context.Background(), vmSpec)
	require.Error(t, err)

	// Create a policy for our directory, but do not allow for writing
	err = devClient.ClusterSettings().Set(context.Background(), &v1.ClusterSettings{
		HostDirPolicies: []v1.HostDirPolicy{{PathPrefix: dirToMount, ReadOnly: true}},
	})
	require.NoError(t, err)

	// Make sure we get error with the same spec
	err = devClient.VMs().Create(context.Background(), vmSpec)
	require.Error(t, err)
}

func hasVM(t *testing.T, name string, logger *zap.Logger) bool {
	return hasVMByPredicate(t, func(vmInfo vmmanager.VMInfo) bool {
		return vmInfo.Name == name
	}, logger)
}

func hasVMByPredicate(t *testing.T, predicate func(vmmanager.VMInfo) bool, logger *zap.Logger) bool {
	if logger == nil {
		logger = zap.Must(zap.NewDevelopment())
	}

	vmInfos, err := platformdependent.ListVMs(context.Background(), logger.Sugar())
	require.NoError(t, err)

	return slices.ContainsFunc(vmInfos, predicate)
}
