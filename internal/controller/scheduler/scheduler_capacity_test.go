//nolint:testpackage // The regression exercises the unexported scheduler reconciliation loop.
package scheduler

import (
	"testing"
	"time"

	"github.com/cirruslabs/orchard/internal/controller/notifier"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/internal/controller/store/badger"
	v1 "github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestWorker(name string) v1.Worker {
	var worker v1.Worker
	worker.Name = name
	worker.LastSeen = time.Now()
	worker.MachineID = name + "-machine"
	worker.Resources = v1.Resources{v1.ResourceTartVMs: 2}
	worker.Arch = v1.ArchitectureARM64
	worker.Runtime = v1.RuntimeTart
	worker.Capabilities = v1.WorkerCapabilities{
		v1.WorkerCapabilityVMEndpoints,
		v1.WorkerCapabilityVMExposedPorts,
	}

	return worker
}

func newPendingVM(name string) v1.VM {
	var vm v1.VM
	vm.Name = name
	vm.CreatedAt = time.Now()
	vm.UID = name + "-uid"
	vm.Status = v1.VMStatusPending
	vm.Resources = v1.Resources{v1.ResourceTartVMs: 1}
	vm.Arch = v1.ArchitectureARM64
	vm.Runtime = v1.RuntimeTart
	vm.PowerState = v1.PowerStateRunning
	vm.Conditions = []v1.Condition{{
		Type:  v1.ConditionTypeScheduled,
		State: v1.ConditionStateFalse,
	}}

	return vm
}

func newAssignedVM(name string, workerName string, status v1.VMStatus) v1.VM {
	vm := newPendingVM(name)
	vm.Worker = workerName
	vm.Status = status
	vm.Conditions[0].State = v1.ConditionStateTrue

	return vm
}

func newTestStore(t *testing.T, profile v1.SchedulerProfile, workers []v1.Worker, vms []v1.VM) storepkg.Store {
	store, err := badger.NewBadgerStore(t.TempDir(), true, zap.NewNop().Sugar())
	require.NoError(t, err)

	var settings v1.ClusterSettings
	settings.SchedulerProfile = profile

	err = store.Update(func(txn storepkg.Transaction) error {
		if err := txn.SetClusterSettings(settings); err != nil {
			return err
		}

		for _, worker := range workers {
			if err := txn.SetWorker(worker); err != nil {
				return err
			}
		}

		for _, vm := range vms {
			if err := txn.SetVM(vm); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(t, err)

	return store
}

func newTestScheduler(t *testing.T, store storepkg.Store) *Scheduler {
	logger := zap.NewNop().Sugar()
	workerNotifier := notifier.NewNotifier(logger)

	// Register the workers with the notifier, otherwise each placement
	// waits a second for its worker to connect before giving up
	var workers []v1.Worker

	err := store.View(func(txn storepkg.Transaction) (err error) {
		workers, err = txn.ListWorkers()

		return
	})
	require.NoError(t, err)

	for _, worker := range workers {
		instructionCh, cancel := workerNotifier.Register(t.Context(), worker.Name)
		t.Cleanup(cancel)

		go func() {
			for {
				select {
				case <-instructionCh:
				case <-t.Context().Done():
					return
				}
			}
		}()
	}

	scheduler, err := NewScheduler(store, workerNotifier, time.Minute, logger)
	require.NoError(t, err)

	return scheduler
}

func getVM(t *testing.T, store storepkg.Store, name string) v1.VM {
	var vm *v1.VM

	err := store.View(func(txn storepkg.Transaction) (err error) {
		vm, err = txn.GetVM(name)

		return
	})
	require.NoError(t, err)

	return *vm
}

func TestSchedulingLoopSkipsOvercommittedWorker(t *testing.T) {
	worker := newTestWorker("worker-a")
	pending := newPendingVM("pending-vm")

	store := newTestStore(t, v1.SchedulerProfileOptimizeUtilization, []v1.Worker{worker}, []v1.VM{
		newAssignedVM("running-first", worker.Name, v1.VMStatusRunning),
		newAssignedVM("running-second", worker.Name, v1.VMStatusRunning),
		newAssignedVM("failed-third", worker.Name, v1.VMStatusFailed),
		pending,
	})

	numWorkers, numVMs, err := newTestScheduler(t, store).schedulingLoopIteration()
	require.NoError(t, err)
	require.Equal(t, 1, numWorkers)
	require.Equal(t, 4, numVMs)

	currentVM := getVM(t, store, pending.Name)
	require.False(t, currentVM.IsScheduled())
	require.Empty(t, currentVM.Worker)
}
