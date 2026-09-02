//go:build darwin

package tests_test

import (
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/imageconstant"
	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

func TestEndpoint(t *testing.T) {
	ctx := t.Context()

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	// Create a VM that exposes its SSH service through the worker endpoint
	const (
		vmName       = "endpoint-test-vm"
		endpointName = "ssh"
	)

	require.NoError(t, devClient.VMs().Create(ctx, &v1.VM{
		Name:     vmName,
		Image:    imageconstant.DefaultMacosImage,
		CPU:      4,
		Memory:   8 * 1024,
		Headless: true,
		Endpoints: []v1.EndpointSpec{
			{
				Name: endpointName,
				Target: v1.ConnectionTarget{
					VM: &v1.ConnectionTargetVM{Port: 22},
				},
			},
		},
	}))

	// Wait for the Worker to expose a listening endpoint on the running VM
	var (
		vm  *v1.VM
		err error
	)

	require.Eventually(t, func() bool {
		vm, err = devClient.VMs().Get(ctx, vmName)

		return err == nil &&
			vm.Status == v1.VMStatusRunning &&
			len(vm.ObservedEndpoints) == 1 &&
			vm.ObservedEndpoints[0].State == v1.EndpointStateListening
	}, 2*time.Minute, time.Second, "failed to wait for the endpoint")

	// Verify the endpoint uses the expected name and a dynamically assigned Worker port
	endpointStatus := vm.ObservedEndpoints[0]
	require.Equal(t, endpointName, endpointStatus.Name)
	require.NotZero(t, endpointStatus.WorkerPort)

	// Wait for SSH to accept connections through the worker port
	var sshClient *ssh.Client
	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(int(endpointStatus.WorkerPort)))
	sshConfig := &ssh.ClientConfig{
		User:            "admin",
		Auth:            []ssh.AuthMethod{ssh.Password("admin")},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         10 * time.Second,
	}

	require.Eventually(t, func() bool {
		sshClient, err = ssh.Dial("tcp", address, sshConfig)
		return err == nil
	}, 2*time.Minute, time.Second, "failed to connect to the endpoint over SSH")
	defer sshClient.Close()

	// Run a command to verify the forwarded connection reaches the VM
	sshSession, err := sshClient.NewSession()
	require.NoError(t, err)
	defer sshSession.Close()

	unameOutput, err := sshSession.Output("uname -a")
	require.NoError(t, err)
	require.Contains(t, string(unameOutput), "Darwin")
}
