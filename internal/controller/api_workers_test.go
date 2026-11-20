//nolint:testpackage // Exercise the real HTTP routes with the production store.
package controller

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/controller/store/badger"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func addTestServiceAccount(t *testing.T, store storepkg.Store) v1.ServiceAccount {
	t.Helper()

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

	return account
}

func TestCreateWorkerSchedulingPaused(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	account := addTestServiceAccount(t, store)

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

		apiServer, err := controller.initAPI()
		require.NoError(t, err)

		server = httptest.NewServer(apiServer)
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

type headerTransport struct {
	base    http.RoundTripper
	headers map[string]string
}

func (transport *headerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())

	for key, value := range transport.headers {
		request.Header.Set(key, value)
	}

	return transport.base.RoundTrip(request)
}

func newWorkerAddressTestClient(t *testing.T, trustedProxies []string, headers map[string]string) *client.Client {
	t.Helper()

	logger := zap.NewNop().Sugar()
	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	account := addTestServiceAccount(t, store)

	controller := &Controller{
		store:  store,
		logger: logger,
	}
	WithTrustedProxies(trustedProxies)(controller)

	apiServer, err := controller.initAPI()
	require.NoError(t, err)

	server := httptest.NewServer(apiServer)
	t.Cleanup(server.Close)

	httpClient := server.Client()
	httpClient.Transport = &headerTransport{base: httpClient.Transport, headers: headers}

	apiClient, err := client.New(
		client.WithAddress(server.URL),
		client.WithCredentials(account.Name, account.Token),
		client.WithHTTPClient(httpClient),
	)
	require.NoError(t, err)

	return apiClient
}

func requireLoopback(t *testing.T, address string) {
	t.Helper()

	ip := net.ParseIP(address)
	require.NotNil(t, ip, "expected an IP address, got %q", address)
	require.True(t, ip.IsLoopback(), "expected a loopback address, got %q", address)
}

func registerWorker(t *testing.T, apiClient *client.Client, name string, address string) *v1.Worker {
	t.Helper()

	_, err := apiClient.Workers().Create(t.Context(), v1.Worker{
		Name:      name,
		MachineID: name + "-machine",
		Address:   address,
	})
	require.NoError(t, err)

	worker, err := apiClient.Workers().Get(t.Context(), name)
	require.NoError(t, err)

	return worker
}

func TestCreateWorkerAddress(t *testing.T) {
	apiClient := newWorkerAddressTestClient(t, nil, nil)

	requireLoopback(t, registerWorker(t, apiClient, "observed", "").Address)

	worker := registerWorker(t, apiClient, "advertised", "worker.example.test")
	require.Equal(t, "worker.example.test", worker.Address)

	worker.Address = "10.0.0.1"
	worker.SchedulingPaused = true
	_, err := apiClient.Workers().Update(t.Context(), *worker)
	require.NoError(t, err)

	worker, err = apiClient.Workers().Get(t.Context(), "advertised")
	require.NoError(t, err)
	require.True(t, worker.SchedulingPaused)
	require.Equal(t, "worker.example.test", worker.Address)

	requireLoopback(t, registerWorker(t, apiClient, "advertised", "").Address)
}

func TestCreateWorkerAddressTrustedProxies(t *testing.T) {
	headers := map[string]string{"X-Forwarded-For": "192.0.2.10"}

	untrusted := newWorkerAddressTestClient(t, nil, headers)
	requireLoopback(t, registerWorker(t, untrusted, "worker", "").Address)

	otherProxy := newWorkerAddressTestClient(t, []string{"192.0.2.0/24"}, headers)
	requireLoopback(t, registerWorker(t, otherProxy, "worker", "").Address)

	trusted := newWorkerAddressTestClient(t, []string{"127.0.0.0/8", "::1/128"}, headers)
	require.Equal(t, "192.0.2.10", registerWorker(t, trusted, "worker", "").Address)
}

func TestCreateWorkerInvalidAddress(t *testing.T) {
	apiClient := newWorkerAddressTestClient(t, nil, nil)

	_, err := apiClient.Workers().Create(t.Context(), v1.Worker{
		Name:      "worker",
		MachineID: "worker-machine",
		Address:   "192.0.2.10:22",
	})
	require.Error(t, err)
}

func TestNewInvalidTrustedProxiesReleasesResources(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listenAddr := listener.Addr().String()
	require.NoError(t, listener.Close())

	dataDir, err := NewDataDir(t.TempDir())
	require.NoError(t, err)

	_, err = New(
		WithDataDir(dataDir),
		WithListenAddr(listenAddr),
		WithTrustedProxies([]string{"not-an-ip"}),
	)
	require.ErrorIs(t, err, ErrInitFailed)

	listener, err = net.Listen("tcp", listenAddr)
	require.NoError(t, err)
	require.NoError(t, listener.Close())
}

func TestNewListenFailureReleasesStore(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = occupied.Close() })

	dataDir, err := NewDataDir(t.TempDir())
	require.NoError(t, err)

	_, err = New(WithDataDir(dataDir), WithListenAddr(occupied.Addr().String()))
	require.Error(t, err)

	controller, err := New(WithDataDir(dataDir), WithListenAddr("127.0.0.1:0"))
	require.NoError(t, err)
	require.NoError(t, controller.listener.Close())
	require.NoError(t, controller.store.Close())
}
