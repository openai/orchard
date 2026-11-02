//nolint:testpackage // Exercise the real HTTP routes with the production store.
package controller

import (
	"net/http/httptest"
	"testing"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/controller/store/badger"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestCreateWorkerSchedulingPaused(t *testing.T) {
	logger := zap.NewNop().Sugar()

	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	account := v1.ServiceAccount{
		Name:  "test-admin",
		Token: "test-token",
		Roles: []v1.ServiceAccountRole{
			v1.ServiceAccountRoleComputeRead,
			v1.ServiceAccountRoleComputeWrite,
		},
	}

	require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
		return txn.SetServiceAccount(&account)
	}))

	var server *httptest.Server

	startController := func(startPaused bool) *client.Client {
		t.Helper()

		if server != nil {
			server.Close()
		}

		controller := &Controller{
			store:  store,
			logger: logger,
		}

		if startPaused {
			WithWorkersStartPaused()(controller)
		}

		server = httptest.NewServer(controller.initAPI())
		t.Cleanup(server.Close)

		apiClient, err := client.New(
			client.WithAddress(server.URL),
			client.WithCredentials(account.Name, account.Token),
			client.WithHTTPClient(server.Client()),
		)
		require.NoError(t, err)

		return apiClient
	}

	ctx := t.Context()
	apiClient := startController(false)

	register := func(name string, paused bool) *v1.Worker {
		t.Helper()

		_, err := apiClient.Workers().Create(ctx, v1.Worker{
			Name:             name,
			MachineID:        name + "-machine",
			SchedulingPaused: paused,
		})
		require.NoError(t, err)

		worker, err := apiClient.Workers().Get(ctx, name)
		require.NoError(t, err)

		return worker
	}

	require.False(t, register("existing-worker", false).SchedulingPaused)
	require.True(t, register("explicitly-paused-worker", true).SchedulingPaused)

	apiClient = startController(true)

	// Enabling the policy must not pause an existing worker on reconnect.
	require.False(t, register("existing-worker", true).SchedulingPaused)

	worker := register("new-worker", false)
	require.True(t, worker.SchedulingPaused)
	require.True(t, register("new-worker", false).SchedulingPaused)

	// Explicitly resumed workers must stay resumed after reconnecting.
	worker.SchedulingPaused = false
	_, err = apiClient.Workers().Update(ctx, *worker)
	require.NoError(t, err)
	require.False(t, register("new-worker", false).SchedulingPaused)

	// Disabling the policy affects future registrations, not paused workers.
	apiClient = startController(false)

	require.False(t, register("later-worker", false).SchedulingPaused)
	require.True(t, register("explicitly-paused-worker", false).SchedulingPaused)
}
