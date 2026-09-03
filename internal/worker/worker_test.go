package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/runtime"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager/tart"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/cirruslabs/orchard/rpc"
	"github.com/coder/websocket"
	"github.com/samber/lo"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	recoveryTestWorkerName = "worker-a"
	recoveryTestVMUID      = "running-vm-uid"
	recoveryTestWorkerPath = "/v1/workers/" + recoveryTestWorkerName
	recoveryTestVMsPath    = "/v1/vms"
	recoveryTestWatchPath  = "/v1/rpc/watch"
	recoveryTestInfoPath   = "/v1/controller/info"
	startupTestWorkersPath = "/v1/workers"
	startupTestVMUID       = "11111111-2222-4333-8444-555555555555"
)

func TestSyncOnDiskVMsCancelsStuckTartList(t *testing.T) {
	worker := newWorkerWithFakeTart(t, "#!/bin/sh\nexec /bin/sleep 60\n")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	started := time.Now()
	err := worker.syncOnDiskVMs(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "timed out listing on-disk VMs")
	require.Less(t, time.Since(started), time.Second)
}

func TestSyncOnDiskVMsPreservesTartPermissionError(t *testing.T) {
	worker := newWorkerWithFakeTart(t,
		"#!/bin/sh\necho 'Failed to perform garbage collection: NSCocoaErrorDomain Code=257' >&2\nexit 1\n",
	)

	err := worker.syncOnDiskVMs(context.Background())

	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "Code=257")
}

func TestRunNewSessionDoesNotRegisterWorkerWhenTartFails(t *testing.T) {
	var registrations atomic.Int32
	worker := newWorkerWithFakeTart(t,
		"#!/bin/sh\necho 'Failed to perform garbage collection: NSCocoaErrorDomain Code=257' >&2\nexit 1\n",
		func(_ http.ResponseWriter, request *http.Request) bool {
			if request.Method == http.MethodPost && request.URL.Path == startupTestWorkersPath {
				registrations.Add(1)
			}

			return false
		},
	)

	require.NoError(t, worker.runNewSession(context.Background(), func() {}))
	require.Zero(t, registrations.Load(), "workers with unusable Tart storage must not appear healthy")
}

func TestRunNewSessionDoesNotDeleteVMsWhenWorkerIdentityConflicts(t *testing.T) {
	onDiskName := ondiskname.New("protected-vm", startupTestVMUID, 0).String()
	script, commandsPath := fakeTartInventoryScript(t, onDiskName)
	var registrations atomic.Int32

	worker := newWorkerWithFakeTart(t, script,
		func(writer http.ResponseWriter, request *http.Request) bool {
			if request.Method != http.MethodPost || request.URL.Path != startupTestWorkersPath {
				return false
			}

			registrations.Add(1)
			writer.WriteHeader(http.StatusConflict)

			return true
		},
	)

	require.NoError(t, worker.runNewSession(context.Background(), func() {}))
	require.Equal(t, int32(1), registrations.Load())

	commands, err := readFakeTartCommands(commandsPath)
	require.NoError(t, err)
	require.Equal(t, "list\n", string(commands),
		"registration conflicts must not stop or delete local VMs")
}

func TestRunNewSessionReconcilesVMsOnlyAfterWorkerRegistration(t *testing.T) {
	onDiskName := ondiskname.New("orphaned-vm", startupTestVMUID, 0).String()
	script, commandsPath := fakeTartInventoryScript(t, onDiskName)
	var commandsAtRegistration atomic.Value

	worker := newWorkerWithFakeTart(t, script,
		func(writer http.ResponseWriter, request *http.Request) bool {
			if request.Method != http.MethodPost || request.URL.Path != startupTestWorkersPath {
				return false
			}

			commands, err := readFakeTartCommands(commandsPath)
			if err != nil {
				t.Errorf("failed to inspect Tart commands at worker registration: %v", err)
				writer.WriteHeader(http.StatusInternalServerError)

				return true
			}
			commandsAtRegistration.Store(string(commands))

			var workerResource v1.Worker
			if err := json.NewDecoder(request.Body).Decode(&workerResource); err != nil {
				t.Errorf("failed to decode worker registration: %v", err)
				writer.WriteHeader(http.StatusBadRequest)

				return true
			}

			writeRecoveryTestJSON(t, writer, workerResource)

			return true
		},
	)

	require.NoError(t, worker.runNewSession(context.Background(), func() {}))
	require.Equal(t, "list\n", commandsAtRegistration.Load(),
		"local VMs must not be reconciled until worker registration succeeds")

	commands, err := readFakeTartCommands(commandsPath)
	require.NoError(t, err)
	require.Equal(t, "list\nstop\ndelete\n", string(commands),
		"successful registration should reconcile the original inventory without listing twice")
}

func fakeTartInventoryScript(t *testing.T, onDiskName string) (string, string) {
	t.Helper()

	commandsPath := filepath.Join(t.TempDir(), "tart-commands")
	inventory, err := json.Marshal([]struct {
		Name    string `json:"name"`
		Running bool   `json:"running"`
	}{{Name: onDiskName, Running: true}})
	require.NoError(t, err)

	script := fmt.Sprintf("#!/bin/sh\nprintf '%%s\\n' \"$1\" >> %q\n"+
		"if [ \"$1\" = list ]; then\nprintf '%%s\\n' '%s'\nfi\n", commandsPath, inventory)

	return script, commandsPath
}

func readFakeTartCommands(commandsPath string) ([]byte, error) {
	return os.ReadFile(filepath.Clean(commandsPath))
}

func newWorkerWithFakeTart(
	t *testing.T,
	script string,
	observeRequests ...func(http.ResponseWriter, *http.Request) bool,
) *Worker {
	t.Helper()

	binDir := t.TempDir()
	fakeTartPath := filepath.Join(binDir, "tart")
	require.NoError(t, os.WriteFile(fakeTartPath, []byte(script), 0o600))
	require.NoError(t, os.Chmod(fakeTartPath, 0o700)) //nolint:gosec // Fake Tart must be executable.
	t.Setenv("PATH", binDir)

	controller := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		for _, observeRequest := range observeRequests {
			if observeRequest(writer, request) {
				return
			}
		}

		switch request.URL.Path {
		case recoveryTestInfoPath:
			writeRecoveryTestJSON(t, writer, v1.ControllerInfo{
				Capabilities: v1.ControllerCapabilities{v1.ControllerCapabilityRPCV2},
			})
		case recoveryTestWatchPath:
			connection, err := websocket.Accept(writer, request, nil)
			if err != nil {
				t.Errorf("failed to accept RPC watch: %v", err)

				return
			}
			defer connection.CloseNow()

			<-request.Context().Done()
		case recoveryTestVMsPath:
			writeRecoveryTestJSON(t, writer, []v1.VM{})
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(controller.Close)

	controllerClient, err := client.New(client.WithAddress(controller.URL))
	require.NoError(t, err)

	pollTicker := time.NewTicker(pollInterval)
	t.Cleanup(pollTicker.Stop)

	return &Worker{
		name:          "worker-a",
		client:        controllerClient,
		vmm:           vmmanager.New(),
		pollTicker:    pollTicker,
		syncRequested: make(chan bool, 1),
		runtime:       runtime.NewTart(),
		logger:        zap.NewNop().Sugar(),
	}
}

func TestWorkerNameLabel(t *testing.T) {
	tests := []struct {
		name               string
		configuredLabels   v1.Labels
		expectedWorkerName string
	}{
		{
			name:               "automatic worker name",
			configuredLabels:   v1.Labels{"custom-label": "custom-value"},
			expectedWorkerName: recoveryTestWorkerName,
		},
		{
			name: "explicit override",
			configuredLabels: v1.Labels{
				"custom-label":     "custom-value",
				v1.LabelWorkerName: "custom-worker-name",
			},
			expectedWorkerName: "custom-worker-name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			worker, err := New(
				nil,
				WithName(recoveryTestWorkerName),
				WithLabels(test.configuredLabels),
				WithSynthetic(),
				WithLogger(zap.NewNop()),
			)
			require.NoError(t, err)
			t.Cleanup(worker.pollTicker.Stop)

			require.Equal(t, "custom-value", worker.labels["custom-label"])
			require.Equal(t, test.expectedWorkerName, worker.labels[v1.LabelWorkerName])
		})
	}
}

func TestWorkerRecoversControllerSessionWithoutDeletingRunningVM(t *testing.T) {
	firstHeartbeat := make(chan struct{})
	reregistered := make(chan struct{})
	var firstHeartbeatOnce sync.Once
	var registrations atomic.Int32
	var watches atomic.Int32

	vmResource := v1.VM{
		Meta:   v1.Meta{Name: "running-vm"},
		UID:    recoveryTestVMUID,
		Worker: recoveryTestWorkerName,
		Status: v1.VMStatusRunning,
	}
	recoveredVM := &recoveryTestVM{
		resource:       vmResource,
		conditionsSeen: make(chan struct{}),
	}

	controller := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/workers":
			var workerResource v1.Worker
			if err := json.NewDecoder(request.Body).Decode(&workerResource); err != nil {
				t.Errorf("failed to decode worker registration: %v", err)
				return
			}

			if registrations.Add(1) == 2 {
				close(reregistered)
			}

			writeRecoveryTestJSON(t, writer, workerResource)
		case request.Method == http.MethodGet && request.URL.Path == recoveryTestInfoPath:
			writeRecoveryTestJSON(t, writer, v1.ControllerInfo{
				Capabilities: v1.ControllerCapabilities{v1.ControllerCapabilityRPCV2},
			})
		case request.Method == http.MethodGet && request.URL.Path == recoveryTestWorkerPath:
			writeRecoveryTestJSON(t, writer, v1.Worker{Meta: v1.Meta{Name: recoveryTestWorkerName}})
		case request.Method == http.MethodPut && request.URL.Path == recoveryTestWorkerPath:
			var workerResource v1.Worker
			if err := json.NewDecoder(request.Body).Decode(&workerResource); err != nil {
				t.Errorf("failed to decode worker heartbeat: %v", err)
				return
			}

			firstHeartbeatOnce.Do(func() { close(firstHeartbeat) })
			writeRecoveryTestJSON(t, writer, workerResource)
		case request.Method == http.MethodGet && request.URL.Path == recoveryTestVMsPath:
			writeRecoveryTestJSON(t, writer, []v1.VM{})
		case request.URL.Path == recoveryTestWatchPath:
			handleRecoveryTestWatch(t, writer, request, &watches, firstHeartbeat, recoveredVM.conditionsSeen)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(controller.Close)

	controllerClient, err := client.New(client.WithAddress(controller.URL))
	require.NoError(t, err)

	worker, err := New(controllerClient, WithName(recoveryTestWorkerName), WithSynthetic(), WithLogger(zap.NewNop()))
	require.NoError(t, err)

	t.Cleanup(worker.pollTicker.Stop)
	worker.vmm.Put(ondiskname.NewFromResource(vmResource), recoveredVM)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	runResult := make(chan error, 1)
	go func() {
		runResult <- worker.Run(ctx)
	}()

	select {
	case <-reregistered:
	case <-time.After(2 * time.Second):
		t.Fatalf("worker did not re-register after its controller RPC watch disconnected (registrations=%d, watches=%d)",
			registrations.Load(), watches.Load())
	}

	require.True(t, worker.vmm.Exists(ondiskname.NewFromResource(vmResource)))
	require.False(t, recoveredVM.stopped.Load(), "controller recovery must not stop a running VM")
	require.False(t, recoveredVM.deleted.Load(), "controller recovery must not delete a running VM")

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

func handleRecoveryTestWatch(
	t *testing.T,
	writer http.ResponseWriter,
	request *http.Request,
	watches *atomic.Int32,
	firstHeartbeat <-chan struct{},
	conditionsSeen <-chan struct{},
) {
	t.Helper()

	connection, err := websocket.Accept(writer, request, nil)
	if err != nil {
		t.Errorf("failed to accept RPC watch: %v", err)
		return
	}
	defer connection.CloseNow()

	if watches.Add(1) == 1 {
		select {
		case <-firstHeartbeat:
		case <-request.Context().Done():
			return
		}

		select {
		case <-conditionsSeen:
		case <-request.Context().Done():
			return
		}

		_ = connection.Close(websocket.StatusGoingAway, "controller rollout")
		return
	}

	<-request.Context().Done()
}

func TestRPCWatchReconnectBackoff(t *testing.T) {
	var backoff rpcWatchReconnectBackoff
	backoff.reset()

	expected := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		3200 * time.Millisecond,
		5 * time.Second,
		5 * time.Second,
	}
	for _, interval := range expected {
		require.Equal(t, interval, backoff.next())
	}

	backoff.reset()
	require.Equal(t, 100*time.Millisecond, backoff.next(),
		"a successfully connected RPC watch should restore the fast initial retry")
}

func TestMonitorRPCWatchHealth(t *testing.T) {
	t.Run("closed watch is not healthy", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		established := make(chan struct{})
		close(established)
		var markedHealthy atomic.Bool
		result := make(chan error, 1)

		go func() {
			result <- monitorRPCWatchHealth(ctx, established, time.Second, func() {
				markedHealthy.Store(true)
			})
		}()

		cancel()
		require.ErrorIs(t, <-result, context.Canceled)
		require.False(t, markedHealthy.Load())
	})

	t.Run("stable watch resets backoff", func(t *testing.T) {
		established := make(chan struct{})
		close(established)
		var backoff rpcWatchReconnectBackoff
		backoff.nextInterval = rpcWatchReconnectMaxInterval

		require.NoError(t, monitorRPCWatchHealth(context.Background(), established,
			10*time.Millisecond, backoff.reset))
		require.Equal(t, rpcWatchReconnectInterval, backoff.next())
	})
}

func TestWorkerBacksOffPersistentRPCWatchFailures(t *testing.T) {
	testCases := []struct {
		name       string
		useRPCV2   bool
		closeAfter bool
	}{
		{name: "HTTP rejection", useRPCV2: true},
		{name: "WebSocket closes immediately", useRPCV2: true, closeAfter: true},
		{name: "gRPC first receive fails"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testWorkerBacksOffPersistentRPCWatchFailure(t, testCase.useRPCV2, testCase.closeAfter)
		})
	}
}

func testWorkerBacksOffPersistentRPCWatchFailure(t *testing.T, useRPCV2 bool, closeAfter bool) {
	t.Helper()

	watchAttempts := make(chan time.Time, 4)
	grpcServer := grpc.NewServer()
	rpc.RegisterControllerServer(grpcServer, &failingRecoveryRPCServer{watchAttempts: watchAttempts})
	t.Cleanup(grpcServer.Stop)

	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Content-Type") == "application/grpc" {
			grpcServer.ServeHTTP(writer, request)
			return
		}

		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/v1/workers":
			var workerResource v1.Worker
			if err := json.NewDecoder(request.Body).Decode(&workerResource); err != nil {
				t.Errorf("failed to decode worker registration: %v", err)
				return
			}
			writeRecoveryTestJSON(t, writer, workerResource)
		case request.Method == http.MethodGet && request.URL.Path == recoveryTestInfoPath:
			info := v1.ControllerInfo{}
			if useRPCV2 {
				info.Capabilities = v1.ControllerCapabilities{v1.ControllerCapabilityRPCV2}
			}
			writeRecoveryTestJSON(t, writer, info)
		case request.Method == http.MethodGet && request.URL.Path == recoveryTestWorkerPath:
			writeRecoveryTestJSON(t, writer, v1.Worker{Meta: v1.Meta{Name: recoveryTestWorkerName}})
		case request.Method == http.MethodPut && request.URL.Path == recoveryTestWorkerPath:
			writeRecoveryTestJSON(t, writer, v1.Worker{Meta: v1.Meta{Name: recoveryTestWorkerName}})
		case request.Method == http.MethodGet && request.URL.Path == recoveryTestVMsPath:
			writeRecoveryTestJSON(t, writer, []v1.VM{})
		case request.URL.Path == recoveryTestWatchPath:
			select {
			case watchAttempts <- time.Now():
			default:
			}

			if closeAfter {
				connection, err := websocket.Accept(writer, request, nil)
				if err != nil {
					t.Errorf("failed to accept rapidly closing RPC watch: %v", err)
					return
				}

				_ = connection.Close(websocket.StatusGoingAway, "upstream unavailable")
				return
			}

			writer.WriteHeader(http.StatusForbidden)
		default:
			http.NotFound(writer, request)
		}
	})
	controller := httptest.NewServer(h2c.NewHandler(handler, &http2.Server{}))
	t.Cleanup(controller.Close)

	controllerClient, err := client.New(client.WithAddress(controller.URL))
	require.NoError(t, err)
	worker, err := New(controllerClient, WithName(recoveryTestWorkerName), WithSynthetic(), WithLogger(zap.NewNop()))
	require.NoError(t, err)
	t.Cleanup(worker.pollTicker.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runResult := make(chan error, 1)
	go func() {
		runResult <- worker.Run(ctx)
	}()

	attempts := make([]time.Time, 0, 4)
	for len(attempts) < 4 {
		select {
		case attemptedAt := <-watchAttempts:
			attempts = append(attempts, attemptedAt)
		case <-time.After(3 * time.Second):
			t.Fatalf("worker made only %d RPC watch attempts", len(attempts))
		}
	}

	expectedMinimums := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond}
	for index, minimum := range expectedMinimums {
		actual := attempts[index+1].Sub(attempts[index])
		require.GreaterOrEqual(t, actual, minimum-20*time.Millisecond,
			"RPC watch retry %d did not increase its backoff", index+1)
	}

	cancel()
	require.ErrorIs(t, <-runResult, context.Canceled)
}

type failingRecoveryRPCServer struct {
	rpc.UnimplementedControllerServer

	watchAttempts chan time.Time
}

func (server *failingRecoveryRPCServer) Watch(
	_ *emptypb.Empty,
	_ rpc.Controller_WatchServer,
) error {
	select {
	case server.watchAttempts <- time.Now():
	default:
	}

	return status.Error(codes.Unavailable, "RPC watch upstream unavailable")
}

func TestShouldPreserveRecoveredVM(t *testing.T) {
	onDiskName := ondiskname.New("running-vm", recoveryTestVMUID, 0)
	now := time.Unix(1_000, 0)
	deadline := now.Add(recoveredVMProtectionPeriod)

	t.Run("missing running VM stays protected", func(t *testing.T) {
		recoveredVMs := map[ondiskname.OnDiskName]time.Time{onDiskName: deadline}

		require.True(t, shouldPreserveRecoveredVM(recoveredVMs, onDiskName, nil,
			mo.Some(v1.VMStatusRunning), now))
		require.Contains(t, recoveredVMs, onDiskName)
	})

	t.Run("missing pending VM stays protected", func(t *testing.T) {
		recoveredVMs := map[ondiskname.OnDiskName]time.Time{onDiskName: deadline}

		require.True(t, shouldPreserveRecoveredVM(recoveredVMs, onDiskName, nil,
			mo.Some(v1.VMStatusPending), now))
		require.Contains(t, recoveredVMs, onDiskName)
	})

	t.Run("missing running VM loses protection after recovery deadline", func(t *testing.T) {
		recoveredVMs := map[ondiskname.OnDiskName]time.Time{onDiskName: deadline}

		require.False(t, shouldPreserveRecoveredVM(recoveredVMs, onDiskName, nil,
			mo.Some(v1.VMStatusRunning), deadline))
		require.NotContains(t, recoveredVMs, onDiskName)
	})

	t.Run("missing pending VM loses protection after recovery deadline", func(t *testing.T) {
		recoveredVMs := map[ondiskname.OnDiskName]time.Time{onDiskName: deadline}

		require.False(t, shouldPreserveRecoveredVM(recoveredVMs, onDiskName, nil,
			mo.Some(v1.VMStatusPending), deadline))
		require.NotContains(t, recoveredVMs, onDiskName)
	})

	t.Run("recognized VM returns to normal deletion behavior", func(t *testing.T) {
		recoveredVMs := map[ondiskname.OnDiskName]time.Time{onDiskName: deadline}

		require.False(t, shouldPreserveRecoveredVM(recoveredVMs, onDiskName,
			&v1.VM{Status: v1.VMStatusRunning}, mo.Some(v1.VMStatusRunning), now))
		require.NotContains(t, recoveredVMs, onDiskName)
		require.False(t, shouldPreserveRecoveredVM(recoveredVMs, onDiskName, nil,
			mo.Some(v1.VMStatusRunning), now))
	})

	t.Run("failed recovered VM is not protected", func(t *testing.T) {
		recoveredVMs := map[ondiskname.OnDiskName]time.Time{onDiskName: deadline}

		require.False(t, shouldPreserveRecoveredVM(recoveredVMs, onDiskName, nil,
			mo.Some(v1.VMStatusFailed), now))
		require.NotContains(t, recoveredVMs, onDiskName)
	})

	t.Run("new VM is not protected", func(t *testing.T) {
		recoveredVMs := map[ondiskname.OnDiskName]time.Time{}

		require.False(t, shouldPreserveRecoveredVM(recoveredVMs, onDiskName, nil,
			mo.Some(v1.VMStatusRunning), now))
	})
}

func TestTrackRecoveredVMsPreservesDeadlinesAcrossSessions(t *testing.T) {
	firstVMResource := v1.VM{
		Meta:   v1.Meta{Name: "first-running-vm"},
		UID:    "first-running-vm-uid",
		Worker: recoveryTestWorkerName,
		Status: v1.VMStatusRunning,
	}
	secondVMResource := v1.VM{
		Meta:   v1.Meta{Name: "second-pending-vm"},
		UID:    "second-pending-vm-uid",
		Worker: recoveryTestWorkerName,
		Status: v1.VMStatusPending,
	}

	worker := &Worker{vmm: vmmanager.New()}
	firstOnDiskName := ondiskname.NewFromResource(firstVMResource)
	worker.vmm.Put(firstOnDiskName, &recoveryTestVM{resource: firstVMResource})

	firstSession := time.Unix(1_000, 0)
	firstDeadline := firstSession.Add(recoveredVMProtectionPeriod)
	require.Equal(t, firstDeadline, worker.trackRecoveredVMs(firstSession)[firstOnDiskName])

	secondSession := firstSession.Add(20 * time.Second)
	secondOnDiskName := ondiskname.NewFromResource(secondVMResource)
	worker.vmm.Put(secondOnDiskName, &recoveryTestVM{resource: secondVMResource})
	recoveredVMs := worker.trackRecoveredVMs(secondSession)

	require.Equal(t, firstDeadline, recoveredVMs[firstOnDiskName],
		"controller reconnects must not extend an existing VM's protection")
	require.Equal(t, secondSession.Add(recoveredVMProtectionPeriod), recoveredVMs[secondOnDiskName],
		"newly recovered pending VMs receive their own bounded protection window")

	worker.vmm.Delete(secondOnDiskName)
	require.NotContains(t, worker.trackRecoveredVMs(secondSession.Add(time.Second)), secondOnDiskName)
}

func TestSyncVMsDeletesRecoveredVMAfterProtectionExpires(t *testing.T) {
	controller := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != recoveryTestVMsPath {
			http.NotFound(writer, request)
			return
		}

		writeRecoveryTestJSON(t, writer, []v1.VM{})
	}))
	t.Cleanup(controller.Close)

	controllerClient, err := client.New(client.WithAddress(controller.URL))
	require.NoError(t, err)

	worker, err := New(controllerClient, WithName(recoveryTestWorkerName), WithSynthetic(), WithLogger(zap.NewNop()))
	require.NoError(t, err)
	t.Cleanup(worker.pollTicker.Stop)

	vmResource := v1.VM{
		Meta:   v1.Meta{Name: "deleted-vm"},
		UID:    "deleted-vm-uid",
		Worker: recoveryTestWorkerName,
		Status: v1.VMStatusRunning,
	}
	deletedVM := &recoveryTestVM{
		resource:       vmResource,
		conditionsSeen: make(chan struct{}),
	}
	onDiskName := ondiskname.NewFromResource(vmResource)
	worker.vmm.Put(onDiskName, deletedVM)

	recoveredVMs := map[ondiskname.OnDiskName]time.Time{
		onDiskName: time.Now().Add(-time.Second),
	}
	err = worker.syncVMs(context.Background(), func(context.Context, v1.VM) error {
		return nil
	}, recoveredVMs)

	require.NoError(t, err)
	require.True(t, deletedVM.stopped.Load(), "expired recovery protection must not prevent VM shutdown")
	require.True(t, deletedVM.deleted.Load(), "expired recovery protection must not prevent VM deletion")
	require.False(t, worker.vmm.Exists(onDiskName))
	require.NotContains(t, recoveredVMs, onDiskName)
}

func TestSyncVMsDefersNewVMWhileRecoveredVMIsUnaccounted(t *testing.T) {
	for _, existingStatus := range []v1.VMStatus{v1.VMStatusPending, v1.VMStatusRunning} {
		t.Run(string(existingStatus), func(t *testing.T) {
			existing := v1.VM{
				Meta:      v1.Meta{Name: "missing-vm"},
				UID:       "missing-vm-uid",
				Worker:    recoveryTestWorkerName,
				Status:    existingStatus,
				Resources: v1.Resources{v1.ResourceTartVMs: 1},
			}
			replacement := v1.VM{
				Meta:      v1.Meta{Name: "replacement-vm"},
				UID:       "replacement-vm-uid",
				Worker:    recoveryTestWorkerName,
				Status:    v1.VMStatusPending,
				Resources: v1.Resources{v1.ResourceTartVMs: 1},
			}

			controller := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path != recoveryTestVMsPath {
					http.NotFound(writer, request)
					return
				}

				writeRecoveryTestJSON(t, writer, []v1.VM{replacement})
			}))
			t.Cleanup(controller.Close)

			controllerClient, err := client.New(client.WithAddress(controller.URL))
			require.NoError(t, err)
			worker, err := New(controllerClient,
				WithName(recoveryTestWorkerName),
				WithSynthetic(),
				WithResources(v1.Resources{v1.ResourceTartVMs: 1}),
				WithLogger(zap.NewNop()),
			)
			require.NoError(t, err)
			t.Cleanup(worker.pollTicker.Stop)

			existingOnDiskName := ondiskname.NewFromResource(existing)
			existingVM := &recoveryTestVM{
				resource:       existing,
				conditionsSeen: make(chan struct{}),
			}
			worker.vmm.Put(existingOnDiskName, existingVM)
			recoveredVMs := map[ondiskname.OnDiskName]time.Time{
				existingOnDiskName: time.Now().Add(recoveredVMProtectionPeriod),
			}
			updateVM := func(context.Context, v1.VM) error { return nil }

			require.NoError(t, worker.syncVMs(context.Background(), updateVM, recoveredVMs))
			require.True(t, worker.vmm.Exists(existingOnDiskName))
			require.False(t, worker.vmm.Exists(ondiskname.NewFromResource(replacement)),
				"a one-slot worker must not start another VM while an unaccounted VM is active")
			require.Len(t, worker.vmm.List(), 1)

			recoveredVMs[existingOnDiskName] = time.Now().Add(-time.Second)
			require.NoError(t, worker.syncVMs(context.Background(), updateVM, recoveredVMs))
			require.True(t, existingVM.stopped.Load())
			require.True(t, existingVM.deleted.Load())
			require.True(t, worker.vmm.Exists(ondiskname.NewFromResource(replacement)),
				"the queued VM should start after the expired recovered VM is removed")
			t.Cleanup(func() { require.NoError(t, worker.Close()) })
		})
	}
}

func TestWatchRPCV2PreservesActiveOperationsAfterWatchCloses(t *testing.T) {
	operationStarted := make(chan struct{})
	releaseOperation := make(chan struct{})
	resolvedIP := make(chan string, 1)

	controller := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case recoveryTestWatchPath:
			connection, err := websocket.Accept(writer, request, nil)
			if err != nil {
				t.Errorf("failed to accept RPC watch: %v", err)
				return
			}
			defer connection.CloseNow()

			instruction := v1.WatchInstruction{ResolveIPAction: &v1.ResolveIPAction{
				Session: "ip-session",
				VMUID:   recoveryTestVMUID,
			}}
			payload, err := json.Marshal(instruction)
			if err != nil {
				t.Errorf("failed to encode RPC instruction: %v", err)
				return
			}
			payload = append(payload, '\n')

			if err := connection.Write(request.Context(), websocket.MessageBinary, payload); err != nil {
				t.Errorf("failed to send RPC instruction: %v", err)
				return
			}

			<-request.Context().Done()
		case "/v1/rpc/resolve-ip":
			resolvedIP <- request.URL.Query().Get("ip")
			writer.WriteHeader(http.StatusOK)
		default:
			http.NotFound(writer, request)
		}
	}))
	t.Cleanup(controller.Close)

	controllerClient, err := client.New(client.WithAddress(controller.URL))
	require.NoError(t, err)

	worker, err := New(controllerClient, WithName(recoveryTestWorkerName), WithSynthetic(), WithLogger(zap.NewNop()))
	require.NoError(t, err)
	t.Cleanup(worker.pollTicker.Stop)

	vmResource := v1.VM{Meta: v1.Meta{Name: "running-vm"}, UID: recoveryTestVMUID}
	worker.vmm.Put(ondiskname.NewFromResource(vmResource), &recoveryIPTestVM{
		recoveryTestVM: recoveryTestVM{resource: vmResource},
		started:        operationStarted,
		release:        releaseOperation,
	})

	operationCtx, cancelOperation := context.WithCancel(context.Background())
	t.Cleanup(cancelOperation)
	watchCtx, cancelWatch := context.WithCancel(operationCtx)
	t.Cleanup(cancelWatch)

	watchResult := make(chan error, 1)
	go func() {
		watchResult <- worker.watchRPCV2(watchCtx, operationCtx, func() {})
	}()

	select {
	case <-operationStarted:
	case err := <-watchResult:
		t.Fatalf("RPC watch terminated before its operation started: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("RPC operation did not start")
	}

	cancelWatch()
	require.ErrorIs(t, <-watchResult, context.Canceled)
	close(releaseOperation)

	select {
	case ip := <-resolvedIP:
		require.Equal(t, "192.0.2.10", ip)
	case <-time.After(2 * time.Second):
		t.Fatal("active RPC operation was canceled with its watch session")
	}
}

type recoveryTestVM struct {
	vmmanager.VM

	resource       v1.VM
	conditionsSeen chan struct{}
	conditionsOnce sync.Once
	stopped        atomic.Bool
	deleted        atomic.Bool
}

type recoveryIPTestVM struct {
	recoveryTestVM

	started chan struct{}
	release chan struct{}
}

func (vm *recoveryIPTestVM) IP(ctx context.Context) (string, error) {
	close(vm.started)

	select {
	case <-vm.release:
		return "192.0.2.10", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (vm *recoveryTestVM) Resource() v1.VM {
	return vm.resource
}

func (vm *recoveryTestVM) OnDiskName() ondiskname.OnDiskName {
	return ondiskname.NewFromResource(vm.resource)
}

func (vm *recoveryTestVM) Status() v1.VMStatus {
	return vm.resource.Status
}

func (vm *recoveryTestVM) Conditions() []v1.Condition {
	vm.conditionsOnce.Do(func() { close(vm.conditionsSeen) })

	return nil
}

func (vm *recoveryTestVM) Running() bool { return false }

func (vm *recoveryTestVM) Stop() <-chan error {
	vm.stopped.Store(true)

	result := make(chan error, 1)
	result <- nil

	return result
}

func (vm *recoveryTestVM) Delete() error {
	vm.deleted.Store(true)

	return nil
}

func writeRecoveryTestJSON(t *testing.T, writer http.ResponseWriter, value any) {
	t.Helper()

	writer.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		t.Errorf("failed to encode controller response: %v", err)
	}
}

func TestSortNonExistentAndFailedFirst(t *testing.T) {
	newVMTuple := func(name string, vmResource *v1.VM) lo.Tuple3[ondiskname.OnDiskName, *v1.VM, vmmanager.VM] {
		return lo.T3[ondiskname.OnDiskName, *v1.VM, vmmanager.VM](
			ondiskname.New(name, name, 0),
			vmResource,
			&tart.VM{},
		)
	}

	target := []lo.Tuple3[ondiskname.OnDiskName, *v1.VM, vmmanager.VM]{
		newVMTuple("test1", &v1.VM{Status: v1.VMStatusFailed}),
		newVMTuple("test2", &v1.VM{Status: v1.VMStatusPending}),
		newVMTuple("test3", &v1.VM{Status: v1.VMStatusRunning}),
		newVMTuple("test5", nil),
		newVMTuple("test4", &v1.VM{Status: v1.VMStatusFailed}),
	}

	sortNonExistentAndFailedFirst(target)

	expected := []lo.Tuple3[ondiskname.OnDiskName, *v1.VM, vmmanager.VM]{
		newVMTuple("test5", nil),
		newVMTuple("test1", &v1.VM{Status: v1.VMStatusFailed}),
		newVMTuple("test4", &v1.VM{Status: v1.VMStatusFailed}),
		newVMTuple("test2", &v1.VM{Status: v1.VMStatusPending}),
		newVMTuple("test3", &v1.VM{Status: v1.VMStatusRunning}),
	}

	require.Equal(t, expected, target)
}
