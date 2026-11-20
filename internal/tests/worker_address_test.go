//nolint:testpackage // Exercise worker registration end-to-end through the dev environment.
package tests

import (
	"net"
	"testing"

	"github.com/cirruslabs/orchard/internal/controller"
	"github.com/cirruslabs/orchard/internal/tests/devcontroller"
	"github.com/cirruslabs/orchard/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestWorkerAddressObserved(t *testing.T) {
	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t,
		false, []controller.Option{controller.WithSynthetic()},
		false, []worker.Option{worker.WithSynthetic()},
	)

	workers, err := devClient.Workers().List(t.Context())
	require.NoError(t, err)
	require.Len(t, workers, 1)

	address := net.ParseIP(workers[0].Address)
	require.NotNil(t, address, "expected an IP address, got %q", workers[0].Address)
	require.True(t, address.IsLoopback())
}

func TestWorkerAddressAdvertised(t *testing.T) {
	const advertiseAddress = "worker.example.test"

	devClient, _, _ := devcontroller.StartIntegrationTestEnvironmentWithAdditionalOpts(
		t,
		false, []controller.Option{controller.WithSynthetic()},
		false, []worker.Option{worker.WithSynthetic(), worker.WithAdvertiseAddress(advertiseAddress)},
	)

	workers, err := devClient.Workers().List(t.Context())
	require.NoError(t, err)
	require.Len(t, workers, 1)
	require.Equal(t, advertiseAddress, workers[0].Address)
}
