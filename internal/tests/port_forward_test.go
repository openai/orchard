//go:build darwin

package tests_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	"github.com/cirruslabs/orchard/internal/tests/platformdependent"
	"github.com/cirruslabs/orchard/pkg/client"
	guestagent "github.com/cirruslabs/tart-guest-agent/pkg/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestTartGuestAgentPortForward(t *testing.T) {
	devClient, _, _ := devcontroller.StartIntegrationTestEnvironment(t)

	// Create a VM with Tart Guest Agent pre-installed
	vmName := "test-guest-agent-" + uuid.NewString()
	require.NoError(t, devClient.VMs().Create(t.Context(), platformdependent.VM(vmName)))

	// Configure the Tart Guest Agent client to connect through Orchard's port-forwarding API
	agentConn, err := grpc.NewClient("passthrough:///tart-guest-agent",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			// Keep the stream alive beyond gRPC's temporary dial context
			return devClient.VMs().PortForwardTarget(t.Context(), vmName,
				client.PortForwardTargetTartGuestAgent, 120)
		}),
	)
	require.NoError(t, err)
	defer agentConn.Close()

	// Perform IP resolution through the forwarded connection
	agentClient := guestagent.NewAgentClient(agentConn)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		rpcContext, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		response, err := agentClient.ResolveIP(rpcContext, &guestagent.ResolveIPRequest{},
			grpc.WaitForReady(true))
		require.NoError(collect, err)
		require.NotNil(collect, net.ParseIP(response.GetIp()))
	}, 2*time.Minute, time.Second)
}
