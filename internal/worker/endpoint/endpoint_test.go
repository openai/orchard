//nolint:testpackage // exercises private listener lifecycle and relay behavior
package endpoint

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

//nolint:forcetypeassert,noctx // the test owns its TCP listener and closes it through cleanup
func TestEndpointPropagatesTCPHalfClose(t *testing.T) {
	// Listen for the endpoint's target connection
	backendListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backendListener.Close()) })

	// Consume one request through EOF before sending the backend response
	backendResult := make(chan error, 1)

	const (
		request  = "request that ends at EOF"
		response = "response after request EOF"
	)

	go func() {
		backendConnection, err := backendListener.Accept()
		if err != nil {
			backendResult <- err
			return
		}
		defer backendConnection.Close()

		_, err = io.Copy(io.Discard, backendConnection)
		if err == nil {
			_, err = io.WriteString(backendConnection, response)
		}

		backendResult <- err
	}()

	// Create an endpoint that forwards accepted connections to the backend
	bindTarget := func(v1.ConnectionTarget) (Dial, error) {
		return func(ctx context.Context) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", backendListener.Addr().String())
		}, nil
	}

	ep, err := newEndpoint(v1.EndpointSpec{Name: "half-close"}, bindTarget, zap.NewNop().Sugar())
	require.NoError(t, err)
	t.Cleanup(ep.close)

	// Connect a client using the address family selected by the endpoint listener
	listenerAddress := ep.listener.Addr().(*net.TCPAddr)
	clientAddress := &net.TCPAddr{IP: net.IPv6loopback, Port: listenerAddress.Port}
	if listenerAddress.IP.To4() != nil {
		clientAddress.IP = net.IPv4(127, 0, 0, 1)
	}

	client, err := net.DialTCP("tcp", nil, clientAddress)
	require.NoError(t, err)
	defer client.Close()
	require.NoError(t, client.SetDeadline(time.Now().Add(5*time.Second)))

	// End the request with a half-close while keeping the client read side open
	_, err = io.WriteString(client, request)
	require.NoError(t, err)
	require.NoError(t, client.CloseWrite())

	// Verify the backend response crosses the still-open reverse direction
	received, err := io.ReadAll(client)
	require.NoError(t, err)
	require.Equal(t, response, string(received))
	require.NoError(t, <-backendResult)
}

func TestSetRecreatesFailedEndpoint(t *testing.T) {
	endpointSet := NewSet(zap.NewNop().Sugar())
	endpointSet.Start()
	t.Cleanup(endpointSet.Stop)

	const endpointName = "ssh"

	// Create a listening endpoint
	endpointSpecs := []v1.EndpointSpec{{Name: endpointName}}

	statuses := endpointSet.Reconcile(endpointSpecs, testBindTarget)
	require.Len(t, statuses, 1)
	require.Equal(t, v1.EndpointStateListening, statuses[0].State)

	// Simulate an unexpected listener failure
	originalEndpoint := endpointSet.endpoints[endpointName]
	require.NotNil(t, originalEndpoint)
	require.NoError(t, originalEndpoint.listener.Close())

	// Wait for the endpoint to report the listener failure
	require.Eventually(t, func() bool {
		return originalEndpoint.status().State == v1.EndpointStateError
	}, time.Second, 10*time.Millisecond)

	failedStatus := originalEndpoint.status()
	require.Zero(t, failedStatus.WorkerPort)
	require.Contains(t, failedStatus.Message, "failed to accept connections")

	// Reconcile again and verify the failed endpoint is replaced
	statuses = endpointSet.Reconcile(endpointSpecs, testBindTarget)
	require.Len(t, statuses, 1)
	require.Equal(t, v1.EndpointStateListening, statuses[0].State)
	require.NotSame(t, originalEndpoint, endpointSet.endpoints[endpointName])
}

func TestSetRecreatesEndpointsWhenDesiredSetChanges(t *testing.T) {
	endpointSet := NewSet(zap.NewNop().Sugar())
	endpointSet.Start()
	t.Cleanup(endpointSet.Stop)

	// Start with one endpoint whose worker port can move
	flexibleSpec := v1.EndpointSpec{Name: "flexible"}
	statuses := endpointSet.Reconcile([]v1.EndpointSpec{flexibleSpec}, testBindTarget)
	require.Len(t, statuses, 1)
	require.Equal(t, v1.EndpointStateListening, statuses[0].State)

	claimedPort := statuses[0].WorkerPort
	originalFlexible := endpointSet.endpoints[flexibleSpec.Name]

	// Add a fixed-port endpoint first, forcing the whole set to be reallocated
	statuses = endpointSet.Reconcile(
		[]v1.EndpointSpec{
			{
				Name:            "fixed",
				WorkerPortRange: &v1.PortRange{Min: claimedPort, Max: claimedPort},
			},
			flexibleSpec,
		},
		testBindTarget,
	)

	// Verify the fixed endpoint takes the old port and the flexible endpoint moves
	require.Len(t, statuses, 2)
	require.Equal(t, v1.EndpointStateListening, statuses[0].State)
	require.Equal(t, claimedPort, statuses[0].WorkerPort)
	require.Equal(t, v1.EndpointStateListening, statuses[1].State)
	require.NotEqual(t, claimedPort, statuses[1].WorkerPort)
	require.NotSame(t, originalFlexible, endpointSet.endpoints[flexibleSpec.Name])
}

func TestSetDoesNotRepeatFullResetAfterPartialFailure(t *testing.T) {
	// Occupy a worker port so one desired endpoint cannot start
	blocker, blockedPort, err := listen(nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, blocker.Close()) })

	endpointSet := NewSet(zap.NewNop().Sugar())
	endpointSet.Start()
	t.Cleanup(endpointSet.Stop)

	desired := []v1.EndpointSpec{
		{
			Name:            "blocked",
			WorkerPortRange: &v1.PortRange{Min: blockedPort, Max: blockedPort},
		},
		{Name: "healthy"},
	}

	// Apply the desired set once and remember its healthy listener
	endpointSet.Reconcile(desired, testBindTarget)
	require.NotContains(t, endpointSet.endpoints, "blocked")
	healthyEndpoint := endpointSet.endpoints["healthy"]
	require.NotNil(t, healthyEndpoint)

	// Reconcile unchanged desired state and verify the healthy listener is preserved
	endpointSet.Reconcile(desired, testBindTarget)
	require.Same(t, healthyEndpoint, endpointSet.endpoints["healthy"])
}

func testBindTarget(v1.ConnectionTarget) (Dial, error) {
	return func(context.Context) (net.Conn, error) {
		return nil, net.ErrClosed
	}, nil
}
