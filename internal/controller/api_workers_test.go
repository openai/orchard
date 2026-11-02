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
			v1.ServiceAccountRoleAdminRead,
			v1.ServiceAccountRoleAdminWrite,
			v1.ServiceAccountRoleComputeRead,
			v1.ServiceAccountRoleComputeWrite,
		},
	}
	require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
		return txn.SetServiceAccount(&account)
	}))

	controller := &Controller{
		store:  store,
		logger: logger,
	}
	server := httptest.NewServer(controller.initAPI())
	t.Cleanup(server.Close)

	apiClient, err := client.New(
		client.WithAddress(server.URL),
		client.WithCredentials(account.Name, account.Token),
		client.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)

	ctx := t.Context()
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

	require.NoError(t, apiClient.ClusterSettings().Set(ctx, &v1.ClusterSettings{}))
	require.False(t, register("existing-worker", false).SchedulingPaused)
	require.True(t, register("explicitly-paused-worker", true).SchedulingPaused)

	require.NoError(t, apiClient.ClusterSettings().Set(ctx, &v1.ClusterSettings{
		WorkersStartPaused: true,
	}))

	settings, err := apiClient.ClusterSettings().Get(ctx)
	require.NoError(t, err)
	require.True(t, settings.WorkersStartPaused)

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
	require.NoError(t, apiClient.ClusterSettings().Set(ctx, &v1.ClusterSettings{}))
	require.False(t, register("later-worker", false).SchedulingPaused)
	require.True(t, register("explicitly-paused-worker", false).SchedulingPaused)
}
