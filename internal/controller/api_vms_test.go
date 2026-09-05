//nolint:testpackage // The handler under test is unexported.
package controller

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cirruslabs/orchard/internal/controller/notifier"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/controller/store/badger"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newScheduledVM(name string, worker string, expose []string) v1.VM {
	var vm v1.VM
	vm.Name = name
	vm.UID = name + "-uid"
	vm.Worker = worker
	vm.Status = v1.VMStatusRunning
	vm.PowerState = v1.PowerStateRunning
	vm.OS = v1.OSDarwin
	vm.Arch = v1.ArchitectureARM64
	vm.Runtime = v1.RuntimeTart
	vm.Image = "ghcr.io/cirruslabs/macos-sequoia-base:latest"
	vm.NetSoftnetExpose = expose
	vm.Conditions = []v1.Condition{{Type: v1.ConditionTypeScheduled, State: v1.ConditionStateTrue}}

	return vm
}

func endpointOn(port uint16) []v1.EndpointSpec {
	return []v1.EndpointSpec{
		{
			Name:            "ssh",
			Target:          v1.ConnectionTarget{VM: &v1.ConnectionTargetVM{Port: 22}},
			WorkerPortRange: &v1.PortRange{Min: port, Max: port},
		},
	}
}

// Endpoints may be added to a VM that is already scheduled, so the update is the only
// place where they can be held against the other VMs on its worker.
func TestUpdateVMSpecEndpointPortConflict(t *testing.T) {
	for _, test := range []struct {
		name        string
		otherExpose []string
		port        uint16
		status      int
	}{
		{name: "port another VM exposes", port: 2222, status: http.StatusPreconditionFailed},
		{name: "free port", port: 2224, status: http.StatusOK},
		// ports of its own that clash already are no licence to add an endpoint that does
		{name: "clash on top of a clash", otherExpose: []string{"2222:22", "2223:22"},
			port: 2222, status: http.StatusPreconditionFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := badger.NewBadgerStore(t.TempDir(), true, zap.NewNop().Sugar())
			require.NoError(t, err)

			otherExpose := test.otherExpose
			if otherExpose == nil {
				otherExpose = []string{"2222:22"}
			}

			other := newScheduledVM("other-vm", "worker-a", otherExpose)
			updated := newScheduledVM("updated-vm", "worker-a", []string{"2223:22"})

			require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
				if err := txn.SetVM(other); err != nil {
					return err
				}

				return txn.SetVM(updated)
			}))

			logger := zap.NewNop().Sugar()
			workerNotifier := notifier.NewNotifier(logger)

			_, unregister := workerNotifier.Register(t.Context(), updated.Worker)
			defer unregister()

			controller := &Controller{
				insecureAuthDisabled: true,
				store:                store,
				workerNotifier:       workerNotifier,
				logger:               logger,
			}

			body := updated
			body.Endpoints = endpointOn(test.port)

			payload, err := json.Marshal(body)
			require.NoError(t, err)

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Params = gin.Params{{Key: "name", Value: updated.Name}}
			ctx.Request = httptest.NewRequest(http.MethodPut, "/v1/vms/"+updated.Name, bytes.NewReader(payload))
			ctx.Request.Header.Set("Content-Type", "application/json")

			controller.updateVMSpec(ctx).Respond(ctx)
			require.Equal(t, test.status, recorder.Code, recorder.Body.String())

			var dbVM *v1.VM

			require.NoError(t, store.View(func(txn storepkg.Transaction) (err error) {
				dbVM, err = txn.GetVM(updated.Name)

				return
			}))

			if test.status == http.StatusOK {
				require.Equal(t, body.Endpoints, dbVM.Endpoints)
			} else {
				require.Empty(t, dbVM.Endpoints)
			}
		})
	}
}

func TestUpdateVMSpecExposedPorts(t *testing.T) {
	for _, test := range []struct {
		name      string
		scheduled bool
		expose    []string
		status    int
	}{
		// A scheduled VM keeps its ports bound on the worker until it is stopped to apply a new
		// generation, so they cannot be changed, whether or not the new ones are free
		{name: "conflicting port", scheduled: true, expose: []string{"02222:80"},
			status: http.StatusPreconditionFailed},
		{name: "free port", scheduled: true, expose: []string{"2224:22"}, status: http.StatusPreconditionFailed},
		{name: "unscheduled", scheduled: false, expose: []string{"2224:22"}, status: http.StatusOK},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := badger.NewBadgerStore(t.TempDir(), true, zap.NewNop().Sugar())
			require.NoError(t, err)

			other := newScheduledVM("other-vm", "worker-a", []string{"2222:22"})
			updated := newScheduledVM("updated-vm", "worker-a", []string{"2223:22"})

			if !test.scheduled {
				updated.Worker = ""
				updated.Status = v1.VMStatusPending
				updated.Conditions = []v1.Condition{
					{Type: v1.ConditionTypeScheduled, State: v1.ConditionStateFalse},
				}
			}

			require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
				if err := txn.SetVM(other); err != nil {
					return err
				}

				return txn.SetVM(updated)
			}))

			logger := zap.NewNop().Sugar()
			workerNotifier := notifier.NewNotifier(logger)

			// A successful update notifies the VM's worker; register it, otherwise the
			// notification waits for the worker to connect before giving up
			_, unregister := workerNotifier.Register(t.Context(), updated.Worker)
			defer unregister()

			controller := &Controller{
				insecureAuthDisabled: true,
				store:                store,
				workerNotifier:       workerNotifier,
				logger:               logger,
			}

			body := updated
			body.NetSoftnetExpose = test.expose

			payload, err := json.Marshal(body)
			require.NoError(t, err)

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Params = gin.Params{{Key: "name", Value: updated.Name}}
			ctx.Request = httptest.NewRequest(http.MethodPut, "/v1/vms/"+updated.Name, bytes.NewReader(payload))
			ctx.Request.Header.Set("Content-Type", "application/json")

			controller.updateVMSpec(ctx).Respond(ctx)
			require.Equal(t, test.status, recorder.Code, recorder.Body.String())

			var dbVM *v1.VM

			require.NoError(t, store.View(func(txn storepkg.Transaction) (err error) {
				dbVM, err = txn.GetVM(updated.Name)

				return
			}))

			if test.status == http.StatusOK {
				require.Equal(t, test.expose, dbVM.NetSoftnetExpose)
				require.Equal(t, uint64(1), dbVM.Generation)
			} else {
				require.Contains(t, recorder.Body.String(), "cannot be changed once the VM is scheduled")
				require.Equal(t, []string{"2223:22"}, dbVM.NetSoftnetExpose)
				require.Equal(t, uint64(0), dbVM.Generation)
			}
		})
	}
}
