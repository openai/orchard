package tests

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	commandssh "github.com/cirruslabs/orchard/internal/command/ssh"
	"github.com/cirruslabs/orchard/internal/imageconstant"
	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	"github.com/cirruslabs/orchard/internal/tests/wait"
	"github.com/cirruslabs/orchard/internal/worker"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/tart"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/samber/lo"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
)

func TestSpecUpdateSoftnet(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Softnet is only supported on macOS with Tart")
	}

	if os.Getenv("ORCHARD_SKIP_SOFTNET_TESTS") != "" {
		t.Skip("softnet tests require root")
	}

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t,
		false,
		nil,
		false,
		[]worker.Option{worker.WithSoftnetPolicyUpdates(false)},
	)

	// Create a VM
	vmName := "test"

	err := devClient.VMs().Create(t.Context(), &v1.VM{
		Meta: v1.Meta{
			Name: vmName,
		},
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
	})
	require.NoError(t, err)

	// Wait for the VM to start
	var vm *v1.VM

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)

		return vm.Status == v1.VMStatusRunning
	}), "failed to start a VM")

	// Ensure that Softnet is not enabled for a VM
	tartVMName := ondiskname.New(vmName, vm.UID, vm.RestartCount).String()

	tartRunCmdline, err := tartRunProcessCmdline(tartVMName)
	require.NoError(t, err)
	require.NotContains(t, tartRunCmdline, "--net-softnet")
	require.NotContains(t, tartRunCmdline, "--net-softnet-allow")
	require.NotContains(t, tartRunCmdline, "--net-softnet-block")

	// Update the VM's specification and enable Softnet
	vm.NetSoftnetAllow = []string{"10.0.0.0/16"}
	vm.NetSoftnetBlock = []string{"0.0.0.0/0"}

	vm, err = devClient.VMs().Update(t.Context(), *vm)
	require.NoError(t, err)
	require.EqualValues(t, 1, vm.Generation)
	require.EqualValues(t, 0, vm.ObservedGeneration)

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM's observed generation to be updated...")

		return vm.ObservedGeneration == 1
	}), "failed to wait for the VM's observed generation to be updated")

	tartRunCmdline, err = tartRunProcessCmdline(tartVMName)
	require.NoError(t, err)
	require.Contains(t, tartRunCmdline, "--net-softnet")
	require.True(t, sliceContainsAnotherSlice(tartRunCmdline, []string{"--net-softnet-allow", "10.0.0.0/16"}))
	require.True(t, sliceContainsAnotherSlice(tartRunCmdline, []string{"--net-softnet-block", "0.0.0.0/0"}))
}

func TestSpecUpdateSoftnetSuspendable(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Softnet is only supported on macOS with Tart")
	}

	if os.Getenv("ORCHARD_SKIP_SOFTNET_TESTS") != "" {
		t.Skip("softnet tests require root")
	}

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t,
		false,
		nil,
		false,
		[]worker.Option{worker.WithSoftnetPolicyUpdates(false)},
	)

	// Create a suspendable VM with Softnet enabled
	vmName := "test"

	err := devClient.VMs().Create(t.Context(), &v1.VM{
		Meta: v1.Meta{
			Name: vmName,
		},
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
		VMSpec: v1.VMSpec{
			Suspendable: true,
			NetSoftnet:  true,
		},
	})
	require.NoError(t, err)

	// Wait for the VM to start
	var vm *v1.VM

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)

		return vm.Status == v1.VMStatusRunning
	}), "failed to start a VM")

	// Ensure that the VM is using "--suspendable" and "--net-softnet"
	tartVMName := ondiskname.New(vmName, vm.UID, vm.RestartCount).String()

	tartRunCmdline, err := tartRunProcessCmdline(tartVMName)
	require.NoError(t, err)
	require.Contains(t, tartRunCmdline, "--suspendable")
	require.Contains(t, tartRunCmdline, "--net-softnet")

	// Update the VM's specification and tighten the Softnet restrictions
	vm.NetSoftnetAllow = []string{"10.0.0.0/16"}
	vm.NetSoftnetBlock = []string{"0.0.0.0/0"}

	vm, err = devClient.VMs().Update(t.Context(), *vm)
	require.NoError(t, err)
	require.EqualValues(t, 1, vm.Generation)
	require.EqualValues(t, 0, vm.ObservedGeneration)

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM's observed generation to be updated...")

		return vm.ObservedGeneration == 1
	}), "failed to wait for the VM's observed generation to be updated")

	// Ensure that the VM is using "--suspendable", "--net-softnet" and "--net-softnet-{allow,block}"
	tartRunCmdline, err = tartRunProcessCmdline(tartVMName)
	require.NoError(t, err)
	require.Contains(t, tartRunCmdline, "--suspendable")
	require.Contains(t, tartRunCmdline, "--net-softnet")
	require.True(t, sliceContainsAnotherSlice(tartRunCmdline, []string{"--net-softnet-allow", "10.0.0.0/16"}))
	require.True(t, sliceContainsAnotherSlice(tartRunCmdline, []string{"--net-softnet-block", "0.0.0.0/0"}))
}

//nolint:gosec,modernize,perfsprint,staticcheck // preserve the original integration test
func TestSpecUpdateSoftnetPolicy(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Softnet is only supported on macOS with Tart")
	}

	if os.Getenv("ORCHARD_SKIP_SOFTNET_TESTS") != "" {
		t.Skip("softnet tests require root")
	}

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t,
		false,
		nil,
		false,
		[]worker.Option{worker.WithSoftnetPolicyUpdates(true)},
	)

	// Create a VM with Softnet enabled
	vmName := "test"

	err := devClient.VMs().Create(t.Context(), &v1.VM{
		Meta: v1.Meta{
			Name: vmName,
		},
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
		VMSpec: v1.VMSpec{
			NetSoftnet: true,
		},
	})
	require.NoError(t, err)

	// Wait for the VM to start
	var vm *v1.VM

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)

		return vm.Status == v1.VMStatusRunning
	}), "failed to start a VM")

	// Ensure that the VM is using "--net-softnet"
	tartVMName := ondiskname.New(vmName, vm.UID, vm.RestartCount).String()

	tartRunCmdline, err := tartRunProcessCmdline(tartVMName)
	require.NoError(t, err)
	require.Contains(t, tartRunCmdline, "--net-softnet")

	// Connect to the VM over SSH
	var netConn net.Conn

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		netConn, err = devClient.VMs().PortForward(t.Context(), vmName, 22, 120)
		if err != nil {
			t.Logf("Waiting for SSH to become available: %v", err)
		}

		return err == nil
	}), "failed to connect to the VM over SSH")
	defer netConn.Close()

	username, password := commandssh.ChooseUsernameAndPassword(t.Context(), devClient, vmName, "", "")

	sshConn, chans, reqs, err := ssh.NewClientConn(netConn, "", &ssh.ClientConfig{
		User:            username,
		Auth:            []ssh.AuthMethod{ssh.Password(password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	})
	require.NoError(t, err)

	sshClient := ssh.NewClient(sshConn, chans, reqs)
	defer sshClient.Close()

	curl := func(address string) error {
		session, err := sshClient.NewSession()
		require.NoError(t, err)
		defer session.Close()

		return session.Run(fmt.Sprintf(
			"/usr/bin/curl -4 -k -sS -o /dev/null --connect-timeout 5 --max-time 10 https://%s",
			address,
		))
	}

	// Ensure that the address is reachable before blocking it
	require.NoError(t, curl("1.1.1.1"))

	// Update the Softnet policy
	restartCountBeforePolicyUpdate := vm.RestartCount
	vm.NetSoftnetBlock = []string{"1.1.1.1/32"}

	vm, err = devClient.VMs().Update(t.Context(), *vm)
	require.NoError(t, err)
	require.EqualValues(t, 1, vm.Generation)
	require.EqualValues(t, 0, vm.ObservedGeneration)

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM's observed generation to be updated...")

		return vm.ObservedGeneration == 1
	}), "failed to wait for the VM's observed generation to be updated")

	// Ensure that the policy was updated without restarting the VM
	require.Equal(t, restartCountBeforePolicyUpdate, vm.RestartCount)
	require.Equal(t, tartVMName, vm.TartName)

	updatedTartRunCmdline, err := tartRunProcessCmdline(tartVMName)
	require.NoError(t, err)
	require.Equal(t, tartRunCmdline, updatedTartRunCmdline)

	// Ensure that the new policy is applied without disrupting other traffic
	require.Error(t, curl("1.1.1.1"))
	require.NoError(t, curl("1.0.0.1"))
}

func TestSpecUpdatePowerStateSuspend(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("VM suspension is only supported on macOS with Tart")
	}

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	// Create a suspendable VM with Softnet enabled
	vmName := "test"

	err := devClient.VMs().Create(t.Context(), &v1.VM{
		Meta: v1.Meta{
			Name: vmName,
		},
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
		VMSpec: v1.VMSpec{
			Suspendable: true,
			NetSoftnet:  true,
		},
	})
	require.NoError(t, err)

	// Wait for the VM to start
	var vm *v1.VM

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)

		return vm.Status == v1.VMStatusRunning
	}), "failed to start a VM")

	// Ensure that the VM is running
	tartVMName := ondiskname.New(vmName, vm.UID, vm.RestartCount).String()

	_, err = tartRunProcessCmdline(tartVMName)
	require.NoError(t, err)

	// Update the VM's specification and change it's power state
	vm.PowerState = v1.PowerStateSuspended

	vm, err = devClient.VMs().Update(t.Context(), *vm)
	require.NoError(t, err)
	require.EqualValues(t, 1, vm.Generation)
	require.EqualValues(t, 0, vm.ObservedGeneration)

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM's observed generation to be updated...")

		return vm.ObservedGeneration == 1
	}), "failed to wait for the VM's observed generation to be updated")

	// Ensure that the VM is not running
	_, err = tartRunProcessCmdline(tartVMName)
	require.Error(t, err)

	// Ensure that the VM is present and is suspended
	tartVMs, err := tart.List(t.Context(), zap.NewNop().Sugar())
	require.NoError(t, err)
	require.Contains(t, tartVMs, vmmanager.VMInfo{
		Name:    vm.LocalName,
		Source:  "local",
		State:   "suspended",
		Running: false,
	})
}

func TestSpecUpdatePowerStateStopped(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("VM suspension and Softnet is only supported on macOS with Tart")
	}

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	// Create a suspendable VM with Softnet enabled
	vmName := "test"

	err := devClient.VMs().Create(t.Context(), &v1.VM{
		Meta: v1.Meta{
			Name: vmName,
		},
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
		VMSpec: v1.VMSpec{
			Suspendable: true,
			NetSoftnet:  true,
		},
	})
	require.NoError(t, err)

	// Wait for the VM to start
	var vm *v1.VM

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM to start. Current status: %s", vm.Status)

		return vm.Status == v1.VMStatusRunning
	}), "failed to start a VM")

	// Ensure that the VM is running
	tartVMName := ondiskname.New(vmName, vm.UID, vm.RestartCount).String()

	_, err = tartRunProcessCmdline(tartVMName)
	require.NoError(t, err)

	// Update the VM's specification and change it's power state
	vm.PowerState = v1.PowerStateStopped

	vm, err = devClient.VMs().Update(t.Context(), *vm)
	require.NoError(t, err)
	require.EqualValues(t, 1, vm.Generation)
	require.EqualValues(t, 0, vm.ObservedGeneration)

	require.True(t, wait.Wait(2*time.Minute, func() bool {
		vm, err = devClient.VMs().Get(context.Background(), vmName)
		require.NoError(t, err)

		t.Logf("Waiting for the VM's observed generation to be updated...")

		return vm.ObservedGeneration == 1
	}), "failed to wait for the VM's observed generation to be updated")

	// Ensure that the VM is not running
	_, err = tartRunProcessCmdline(tartVMName)
	require.Error(t, err)

	// Ensure that the VM is present and is suspended
	tartVMs, err := tart.List(t.Context(), zap.NewNop().Sugar())
	require.NoError(t, err)
	require.Contains(t, tartVMs, vmmanager.VMInfo{
		Name:    vm.LocalName,
		Source:  "local",
		State:   "stopped",
		Running: false,
	})
}

func tartRunProcessCmdline(vmName string) ([]string, error) {
	processes, err := process.Processes()
	if err != nil {
		return nil, err
	}

	for _, process := range processes {
		name, err := process.Name()
		if err != nil {
			// On macOS, process.Name() returns "invalid argument" for most
			// of the processes likely due to permissions, so just ignore it
			continue
		}

		if name != "tart" {
			continue
		}

		cmdline, err := process.CmdlineSlice()
		if err != nil {
			return nil, err
		}

		if len(cmdline) < 3 {
			continue
		}

		if cmdline[1] != "run" {
			continue
		}

		if lo.Contains(cmdline[2:], vmName) {
			return cmdline, nil
		}
	}

	return nil, fmt.Errorf("failed to find a \"tart run\" process for VM %q", vmName)
}

func sliceContainsAnotherSlice(haystack []string, needle []string) bool {
	if len(needle) == 0 {
		return true
	}

	var needleIdx int

	for _, haystackItem := range haystack {
		if haystackItem == needle[needleIdx] {
			needleIdx++

			if needleIdx == len(needle) {
				return true
			}
		}
	}

	return false
}
