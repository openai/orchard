package worker //nolint:testpackage // The regression test exercises unexported worker reconciliation.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/worker/ondiskname"
	"github.com/cirruslabs/orchard/internal/worker/vmmanager"
	"github.com/cirruslabs/orchard/pkg/client"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var errLocalVMFailed = errors.New("local VM failed")

type delayedStopVM struct {
	vmmanager.VM

	resource    v1.VM
	status      v1.VMStatus
	stopStarted chan struct{}
	stopResult  chan error
}

func (vm *delayedStopVM) OnDiskName() ondiskname.OnDiskName {
	return ondiskname.NewFromResource(vm.resource)
}

func (vm *delayedStopVM) Status() v1.VMStatus { return vm.status }

func (vm *delayedStopVM) Conditions() []v1.Condition { return nil }

func (vm *delayedStopVM) Err() error { return errLocalVMFailed }

func (vm *delayedStopVM) Stop() <-chan error {
	close(vm.stopStarted)
	return vm.stopResult
}

func TestSyncVMsWaitsForVMShutdown(t *testing.T) {
	tests := []struct {
		name         string
		remoteStatus v1.VMStatus
		localStatus  v1.VMStatus
		update       bool
	}{
		{
			name:         "remote failed VM stops before reconciliation continues",
			remoteStatus: v1.VMStatusFailed,
			localStatus:  v1.VMStatusRunning,
		},
		{
			name:         "local failed VM stops before reporting failure",
			remoteStatus: v1.VMStatusRunning,
			localStatus:  v1.VMStatusFailed,
			update:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resource := v1.VM{
				Meta:   v1.Meta{Name: "test-vm"},
				UID:    "00112233-4455-6677-8899-aabbccddeeff",
				Worker: "test-worker",
				Status: test.remoteStatus,
			}

			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.Method != http.MethodGet || request.URL.Path != "/v1/vms" {
					t.Errorf("unexpected request: %s %s", request.Method, request.URL.Path)
					return
				}

				response.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(response).Encode([]map[string]any{{
					"name":   resource.Name,
					"uid":    resource.UID,
					"worker": resource.Worker,
					"status": resource.Status,
				}}); err != nil {
					t.Errorf("encode VM response: %v", err)
				}
			}))
			defer server.Close()

			apiClient, err := client.New(client.WithAddress(server.URL))
			require.NoError(t, err)
			vm := &delayedStopVM{
				resource:    resource,
				status:      test.localStatus,
				stopStarted: make(chan struct{}),
				stopResult:  make(chan error, 1),
			}
			manager := vmmanager.New()
			manager.Put(vm.OnDiskName(), vm)
			worker := &Worker{name: "test-worker", client: apiClient, vmm: manager, logger: zap.NewNop().Sugar()}
			updated := make(chan v1.VM, 1)
			finished := make(chan error, 1)

			go func() {
				finished <- worker.syncVMs(context.Background(), func(_ context.Context, updatedVM v1.VM) error {
					updated <- updatedVM
					return nil
				}, nil)
			}()

			select {
			case <-vm.stopStarted:
			case <-time.After(time.Second):
				t.Fatal("VM shutdown was not requested")
			}

			select {
			case updatedVM := <-updated:
				t.Fatalf("VM was reported %q before shutdown completed", updatedVM.Status)
			case err := <-finished:
				t.Fatalf("reconciliation continued before VM shutdown completed: %v", err)
			case <-time.After(30 * time.Millisecond):
			}

			vm.stopResult <- nil
			require.NoError(t, <-finished)
			if test.update {
				require.Equal(t, v1.VMStatusFailed, (<-updated).Status)
			}
		})
	}
}
