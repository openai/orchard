package tests

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	commandssh "github.com/cirruslabs/orchard/internal/command/ssh"
	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/imageconstant"
	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	"github.com/cirruslabs/orchard/internal/tests/wait"
	"github.com/cirruslabs/orchard/internal/worker"
	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/tart"
	"github.com/cirruslabs/orchard/pkg/client"
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

const testBridgedInterface = "en0"

func TestWireGuardConfigurationLifecycle(t *testing.T) {
	api, devController, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t, false, []controller.Option{controller.WithSynthetic(), controller.WithListenAddr("127.0.0.1:0")},
		true, []worker.Option{worker.WithSynthetic()},
	)
	vms, ctx := api.VMs(), t.Context()
	mtu := uint16(1380)
	wireGuard := &v1.WireGuardSpec{
		Address: "10.77.0.2", Routes: []string{"10.77.0.0/24"},
		MTU: &mtu,
		Peers: []v1.WireGuardPeer{{
			PublicKey: "3p7bfXt9wbTTW2HC7OQ1Nz+DQ8hbeGdNrfx+FG+IK08=", Endpoint: "192.0.2.20:23018",
			AllowedIPs: []string{"10.77.0.0/24"},
		}},
	}

	// Creation records the requested tunnel before a worker applies its generation
	require.NoError(t, vms.Create(ctx, &v1.VM{
		Name: "wireguard", Image: "example.com/test:latest", Worker: "worker",
		NetSoftnet: true, WireGuard: wireGuard,
	}))
	initial, err := vms.Get(ctx, "wireguard")
	require.NoError(t, err)
	require.Equal(t, wireGuard, initial.WireGuard)
	require.Zero(t, initial.Generation)
	require.Zero(t, initial.ObservedGeneration)
	require.Nil(t, initial.ObservedWireGuard)

	// Every Softnet option supports enrollment without a peer
	for name, spec := range map[string]v1.VMSpec{
		"without-peers":   {NetSoftnet: true},
		"deprecated-flag": {NetSoftnetDeprecated: true},
		"allow-policy":    {NetSoftnetAllow: []string{"192.0.2.0/24"}},
		"block-policy":    {NetSoftnetBlock: []string{"192.0.2.0/24"}},
	} {
		t.Run(name, func(t *testing.T) {
			spec.WireGuard = &v1.WireGuardSpec{Address: wireGuard.Address, Routes: wireGuard.Routes}
			require.NoError(t, vms.Create(ctx, &v1.VM{
				Name: name, Image: initial.Image, VMSpec: spec,
			}))
			created, err := vms.Get(ctx, name)
			require.NoError(t, err)
			require.True(t, created.NetSoftnet)
			require.Equal(t, spec.WireGuard, created.WireGuard)
		})
	}

	initial.ObservedWireGuard = &v1.WireGuardStatus{PublicKey: wireGuard.Peers[0].PublicKey, ListenPort: 51820}
	_, err = vms.UpdateState(ctx, *initial)
	require.NoError(t, err)

	endpoint, err := url.JoinPath(devController.Address(), "v1", "vms", initial.Name)
	require.NoError(t, err)
	putSpec := func(fields string, wantStatus int) {
		t.Helper()
		body := fmt.Sprintf(`{"os":%q,"arch":%q,"runtime":%q,%s}`, initial.OS, initial.Arch, initial.Runtime, fields)
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, wantStatus, resp.StatusCode)
	}

	// A generation without a UID guards the full replacement, including at generation zero
	putSpec(`"generation":0,"powerState":"running"`, http.StatusOK)
	putSpec(`"generation":0,"powerState":"running","usbAccessories":true`, http.StatusPreconditionFailed)
	current, err := vms.Get(ctx, initial.Name)
	require.NoError(t, err)
	require.Equal(t, initial.Generation+1, current.Generation)
	require.Nil(t, current.WireGuard)
	require.False(t, current.USBAccessories)

	// Removal accepts the previous observation until a current-generation report clears it
	_, err = vms.UpdateState(ctx, *current)
	require.NoError(t, err)
	changed := *current.ObservedWireGuard
	changed.ListenPort++
	current.ObservedWireGuard = &changed
	_, err = vms.UpdateState(ctx, *current)
	require.ErrorContains(t, err, "VM has no WireGuard configuration")

	current.ObservedWireGuard = nil
	current.Generation = initial.Generation
	_, err = vms.UpdateState(ctx, *current)
	require.ErrorContains(t, err, "generation")
	current.Generation++
	_, err = vms.UpdateState(ctx, *current)
	require.NoError(t, err)

	// A UID still guards identity when generation is omitted or null
	putSpec(`"uid":"previous","powerState":"running"`, http.StatusPreconditionFailed)
	putSpec(fmt.Sprintf(`"uid":%q,"powerState":"running"`, initial.UID), http.StatusOK)
	putSpec(fmt.Sprintf(`"uid":%q,"generation":null,"powerState":"running"`, initial.UID), http.StatusOK)

	const bridgedSoftnetError = "cannot combine Softnet and bridged networking"

	// Invalid network configurations are rejected before a VM is stored
	for name, tc := range map[string]struct {
		spec      v1.VMSpec
		bridged   string
		wantError string
	}{
		"missing-address": {
			spec:      v1.VMSpec{NetSoftnet: true, WireGuard: &v1.WireGuardSpec{Routes: wireGuard.Routes}},
			wantError: "WireGuard address is required",
		},
		"missing-route": {
			spec:      v1.VMSpec{NetSoftnet: true, WireGuard: &v1.WireGuardSpec{Address: wireGuard.Address}},
			wantError: "routes must contain exactly one IPv4 prefix",
		},
		"without-softnet": {
			spec:      v1.VMSpec{WireGuard: wireGuard},
			wantError: "WireGuard configuration requires Softnet",
		},
		"bridged-tunnel": {
			spec:      v1.VMSpec{NetSoftnet: true, WireGuard: wireGuard},
			bridged:   testBridgedInterface,
			wantError: bridgedSoftnetError,
		},
		"bridged-softnet": {
			spec:      v1.VMSpec{NetSoftnet: true},
			bridged:   testBridgedInterface,
			wantError: bridgedSoftnetError,
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := vms.Create(ctx, &v1.VM{
				Name: name, Image: initial.Image, VMSpec: tc.spec, NetBridged: tc.bridged,
			})
			var apiErr *client.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)
			require.ErrorContains(t, err, tc.wantError)
		})
	}

	// Attaching WireGuard requires the stored VM to already use Softnet
	for name, tc := range map[string]struct {
		netBridged string
		wantError  string
	}{
		"attach-without-softnet": {wantError: "WireGuard configuration requires a running or pending Softnet VM"},
		"attach-to-bridge":       {netBridged: testBridgedInterface, wantError: bridgedSoftnetError},
	} {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, vms.Create(ctx, &v1.VM{
				Name: name, Image: initial.Image, NetBridged: tc.netBridged,
			}))
			current, err := vms.Get(ctx, name)
			require.NoError(t, err)

			current.NetBridged = ""
			current.NetSoftnet = true
			current.WireGuard = wireGuard
			_, err = vms.Update(ctx, *current)

			var apiErr *client.APIError
			require.ErrorAs(t, err, &apiErr)
			require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)
			require.ErrorContains(t, err, tc.wantError)
		})
	}

	checkWireGuardVMReplacement(t, vms, initial, wireGuard, putSpec)
}

func checkWireGuardVMReplacement(
	t *testing.T,
	vms *client.VMsService,
	initial *v1.VM,
	wireGuard *v1.WireGuardSpec,
	putSpec func(string, int),
) {
	t.Helper()
	ctx := t.Context()
	var apiErr *client.APIError

	// Reusing a name must not let a delayed allocation attach to its replacement
	require.NoError(t, vms.Delete(ctx, initial.Name))
	require.NoError(t, vms.Create(ctx, &v1.VM{
		Name: initial.Name, Image: initial.Image,
		NetSoftnet: true,
	}))
	replacement, err := vms.Get(ctx, initial.Name)
	require.NoError(t, err)
	require.NotEqual(t, initial.UID, replacement.UID)

	// A delayed failure from the deleted VM must not mark its replacement as failed
	stale := *initial
	stale.Status = v1.VMStatusFailed
	_, err = vms.UpdateState(ctx, stale)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)
	require.ErrorContains(t, err, "UID")

	current, err := vms.Get(ctx, replacement.Name)
	require.NoError(t, err)
	require.Equal(t, replacement.Status, current.Status)

	// A VM without WireGuard accepts a legacy report with no UID
	legacy := *current
	legacy.UID = ""
	reported, err := vms.UpdateState(ctx, legacy)
	require.NoError(t, err)
	require.Equal(t, replacement.UID, reported.UID)

	report := *reported
	report.ObservedWireGuard = &v1.WireGuardStatus{PublicKey: wireGuard.Peers[0].PublicKey, ListenPort: 51820}
	_, err = vms.UpdateState(ctx, report)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)
	require.ErrorContains(t, err, "VM has no WireGuard configuration")

	// Always build a new snapshot for this incarnation
	currentVM := func(t *testing.T) *v1.VM {
		t.Helper()
		vm, err := vms.Get(ctx, replacement.Name)
		require.NoError(t, err)
		require.Equal(t, replacement.UID, vm.UID)

		return vm
	}

	_, err = vms.Update(ctx, *initial)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)

	// The allocation applies its policy and peer in one generation
	desired := currentVM(t)
	desired.NetBridged = testBridgedInterface // PUT ignores this create-time field
	desired.NetSoftnetBlock = []string{"0.0.0.0/0"}
	desired.WireGuard = wireGuard

	attached, err := vms.Update(ctx, *desired)
	require.NoError(t, err)
	require.Empty(t, attached.NetBridged)
	require.Equal(t, wireGuard, attached.WireGuard)
	require.Equal(t, desired.NetSoftnetBlock, attached.NetSoftnetBlock)
	require.Equal(t, replacement.Generation+1, attached.Generation)

	unchanged, err := vms.Update(ctx, *currentVM(t))
	require.NoError(t, err)
	require.Equal(t, attached.Generation, unchanged.Generation)

	// A full replacement without peers withdraws the peer and keeps the reserved route
	putSpec(`
		"powerState":"running", "netSoftnet":true, "netSoftnetBlock":["0.0.0.0/0"],
		"wireGuard":{"address":"10.77.0.2","routes":["10.77.0.0/24"]}
	`, http.StatusOK)
	withdrawn := currentVM(t)
	require.Equal(t, &v1.WireGuardSpec{
		Address: wireGuard.Address, Routes: wireGuard.Routes,
	}, withdrawn.WireGuard)
	require.Equal(t, attached.Generation+1, withdrawn.Generation)

	// Sending the explicit empty list represents the same configuration
	explicit := currentVM(t)
	explicit.WireGuard.Peers = []v1.WireGuardPeer{}
	unchanged, err = vms.Update(ctx, *explicit)
	require.NoError(t, err)
	require.Equal(t, withdrawn.Generation, unchanged.Generation)

	// Rejecting a second peer must also discard the unrelated USB change
	request := currentVM(t)
	request.WireGuard.Peers = []v1.WireGuardPeer{wireGuard.Peers[0], wireGuard.Peers[0]}
	request.USBAccessories = !request.USBAccessories
	_, err = vms.Update(ctx, *request)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)
	require.ErrorContains(t, err, "at most one WireGuard peer")

	stored := currentVM(t)
	require.Equal(t, withdrawn.VMSpec, stored.VMSpec)
	require.Equal(t, withdrawn.Generation, stored.Generation)

	// An enrolled VM still requires Softnet after peer withdrawal
	request = currentVM(t)
	request.NetSoftnet = false
	request.NetSoftnetDeprecated = false
	request.NetSoftnetBlock = nil

	_, err = vms.Update(ctx, *request)
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)

	// A legacy PUT can stop the VM and clear its tunnel without sending metadata
	putSpec(`"powerState":"stopped"`, http.StatusOK)

	stopped := currentVM(t)
	require.Equal(t, v1.PowerStateStopped, stopped.PowerState)
	require.Nil(t, stopped.WireGuard)
	require.Equal(t, withdrawn.Generation+1, stopped.Generation)
}

func TestWireGuardRuntimeConfiguration(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Softnet is only supported on macOS with Tart")
	}

	if os.Getenv("ORCHARD_SKIP_SOFTNET_TESTS") != "" {
		t.Skip("softnet tests require root")
	}

	api, _, _ := devcontroller.StartIntegrationTestEnvironment(t)
	vms, ctx := api.VMs(), t.Context()
	const (
		name  = "wireguard"
		route = "10.77.0.0/23"
	)
	wireGuard := &v1.WireGuardSpec{Address: "10.77.0.67", Routes: []string{route}}

	require.NoError(t, vms.Create(ctx, &v1.VM{
		Name: name, Image: imageconstant.DefaultMacosImage,
		CPU: 4, Memory: 8 * 1024, Headless: true,
		NetSoftnet: true, WireGuard: wireGuard,
	}))
	defer func() { require.NoError(t, vms.Delete(ctx, name)) }()

	awaitVM := func(ready func(*v1.VM) bool) *v1.VM {
		t.Helper()
		var vm *v1.VM
		require.True(t, wait.Wait(2*time.Minute, func() bool {
			var err error
			vm, err = vms.Get(ctx, name)
			require.NoError(t, err)
			require.NotEqual(t, v1.VMStatusFailed, vm.Status, vm.StatusMessage)

			return ready(vm)
		}), "VM did not reach the expected WireGuard state: %+v", vm)

		return vm
	}
	vm := awaitVM(func(vm *v1.VM) bool {
		return vm.Status == v1.VMStatusRunning && v1.ConditionIsTrue(vm.Conditions, v1.ConditionTypeRunning) &&
			vm.ObservedGeneration == vm.Generation && vm.ObservedWireGuard != nil
	})
	uid, restarts := vm.UID, vm.RestartCount

	apply := func(peers []v1.WireGuardPeer) *v1.VM {
		t.Helper()
		wireGuard := &v1.WireGuardSpec{
			Peers: peers, Address: "10.77.0.67",
			Routes: []string{route},
		}
		current, err := vms.Get(ctx, name)
		require.NoError(t, err)
		require.Equal(t, uid, current.UID)
		current.WireGuard = wireGuard

		updated, err := vms.Update(ctx, *current)
		require.NoError(t, err)

		vm := awaitVM(func(vm *v1.VM) bool {
			return vm.ObservedGeneration == updated.Generation && vm.ObservedWireGuard != nil
		})
		require.Equal(t, uid, vm.UID)
		require.Equal(t, restarts, vm.RestartCount)
		require.Equal(t, wireGuard, vm.WireGuard)
		require.NotNil(t, vm.ObservedWireGuard)
		require.NoError(t, vm.ObservedWireGuard.Validate())

		return vm
	}

	enrolled := vm
	attached := apply([]v1.WireGuardPeer{{
		PublicKey: "3p7bfXt9wbTTW2HC7OQ1Nz+DQ8hbeGdNrfx+FG+IK08=", Endpoint: "192.0.2.20:23018",
		AllowedIPs: []string{route}, PersistentKeepaliveSeconds: 15,
	}})

	withdrawn := apply([]v1.WireGuardPeer{})
	require.Equal(t, enrolled.ObservedWireGuard, attached.ObservedWireGuard)
	require.Equal(t, enrolled.ObservedWireGuard, withdrawn.ObservedWireGuard)

	// The worker must report observations for the current VM and generation
	for _, change := range []func(*v1.VM){
		func(vm *v1.VM) { vm.UID = "previous" },
		func(vm *v1.VM) { vm.UID = "" },
		func(vm *v1.VM) { vm.Worker += "-previous" },
		func(vm *v1.VM) { vm.RestartCount++ },
		func(vm *v1.VM) { vm.Generation = attached.Generation },
	} {
		stale := *withdrawn
		stale.ObservedWireGuard = nil
		change(&stale)
		_, err := vms.UpdateState(ctx, stale)

		var apiErr *client.APIError
		require.ErrorAs(t, err, &apiErr)
		require.Equal(t, http.StatusPreconditionFailed, apiErr.StatusCode)
	}
	current, err := vms.Get(ctx, name)
	require.NoError(t, err)
	require.Equal(t, withdrawn.WireGuard, current.WireGuard)
	require.Equal(t, withdrawn.ObservedWireGuard, current.ObservedWireGuard)

	// Clearing and re-enrolling replaces the gateway inside the same Tart process
	tartName := ondiskname.NewFromResource(*current).String()
	pid, _, err := findTartRunProcess(tartName)
	require.NoError(t, err)
	current.WireGuard = nil
	removed, err := vms.Update(ctx, *current)
	require.NoError(t, err)
	awaitVM(func(vm *v1.VM) bool {
		return vm.ObservedGeneration == removed.Generation && vm.ObservedWireGuard == nil &&
			v1.ConditionIsTrue(vm.Conditions, v1.ConditionTypeRunning)
	})

	reenrolled := apply([]v1.WireGuardPeer{})
	require.NotEqual(t, withdrawn.ObservedWireGuard.PublicKey, reenrolled.ObservedWireGuard.PublicKey)
	currentPID, _, err := findTartRunProcess(tartName)
	require.NoError(t, err)
	require.Equal(t, pid, currentPID)

	// A configured VM withdraws readiness while stopped and confirms a fresh gateway after restart
	reenrolled.PowerState = v1.PowerStateStopped
	stopped, err := vms.Update(ctx, *reenrolled)
	require.NoError(t, err)
	stopped = awaitVM(func(vm *v1.VM) bool {
		return vm.ObservedGeneration == stopped.Generation && vm.ObservedWireGuard == nil &&
			v1.ConditionIsFalse(vm.Conditions, v1.ConditionTypeRunning)
	})
	require.Equal(t, reenrolled.WireGuard, stopped.WireGuard)

	stopped.PowerState = v1.PowerStateRunning
	restarted, err := vms.Update(ctx, *stopped)
	require.NoError(t, err)
	restarted = awaitVM(func(vm *v1.VM) bool {
		return vm.ObservedGeneration == restarted.Generation && vm.ObservedWireGuard != nil &&
			v1.ConditionIsTrue(vm.Conditions, v1.ConditionTypeRunning)
	})
	require.Equal(t, uid, restarted.UID)
	require.NotEqual(t, reenrolled.ObservedWireGuard.PublicKey, restarted.ObservedWireGuard.PublicKey)
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

	// Include a host process that would fail to start to ensure that host-process
	// reconciliation cannot block the terminal power-state transition.
	vm.HostProcesses = []v1.HostProcess{{
		Name:    "unavailable",
		Program: "/does/not/exist",
	}}

	// Update the VM's specification and change its power state
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

	// Include a host process that would fail to start to ensure that host-process
	// reconciliation cannot block the terminal power-state transition.
	vm.HostProcesses = []v1.HostProcess{{
		Name:    "unavailable",
		Program: "/does/not/exist",
	}}

	// Update the VM's specification and change its power state
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
	_, cmdline, err := findTartRunProcess(vmName)

	return cmdline, err
}

func findTartRunProcess(vmName string) (int32, []string, error) {
	processes, err := process.Processes()
	if err != nil {
		return 0, nil, err
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
			return 0, nil, err
		}

		if len(cmdline) < 3 {
			continue
		}

		if cmdline[1] != "run" {
			continue
		}

		if lo.Contains(cmdline[2:], vmName) {
			return process.Pid, cmdline, nil
		}
	}

	return 0, nil, fmt.Errorf("failed to find a \"tart run\" process for VM %q", vmName)
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
