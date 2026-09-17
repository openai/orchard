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

func TestSchedulingLoopSkipsWorkerWithExposedPortConflict(t *testing.T) {
	worker := newTestWorker("worker-a")
	// Enough capacity for all VMs, so that an exposed
	// port conflict is the only thing preventing scheduling
	worker.Resources = v1.Resources{v1.ResourceTartVMs: 3}

	running := newAssignedVM("running-vm", worker.Name, v1.VMStatusRunning)
	running.NetSoftnetExpose = []string{"2222:22"}

	conflicting := newPendingVM("conflicting-vm")
	conflicting.NetSoftnetExpose = []string{"2222:80"}

	nonConflicting := newPendingVM("non-conflicting-vm")
	nonConflicting.NetSoftnetExpose = []string{"2223:22"}

	store := newTestStore(t, v1.SchedulerProfileOptimizeUtilization, []v1.Worker{worker},
		[]v1.VM{running, conflicting, nonConflicting})

	numWorkers, numVMs, err := newTestScheduler(t, store).schedulingLoopIteration()
	require.NoError(t, err)
	require.Equal(t, 1, numWorkers)
	require.Equal(t, 3, numVMs)

	currentConflicting := getVM(t, store, conflicting.Name)
	require.False(t, currentConflicting.IsScheduled())
	require.Empty(t, currentConflicting.Worker)

	currentNonConflicting := getVM(t, store, nonConflicting.Name)
	require.True(t, currentNonConflicting.IsScheduled())
	require.Equal(t, worker.Name, currentNonConflicting.Worker)
}

func TestSchedulingLoopSkipsWorkerWithoutExposedPortSupport(t *testing.T) {
	stale := newTestWorker("worker-a")
	// A worker from before the field existed advertises everything but the ports
	stale.Capabilities = v1.WorkerCapabilities{v1.WorkerCapabilityVMEndpoints}

	capable := newTestWorker("worker-b")

	pending := newPendingVM("pending-vm")
	pending.NetSoftnetExpose = []string{"2222:22"}

	store := newTestStore(t, v1.SchedulerProfileOptimizeUtilization,
		[]v1.Worker{stale, capable}, []v1.VM{pending})

	numWorkers, numVMs, err := newTestScheduler(t, store).schedulingLoopIteration()
	require.NoError(t, err)
	require.Equal(t, 2, numWorkers)
	require.Equal(t, 1, numVMs)

	currentPending := getVM(t, store, pending.Name)
	require.True(t, currentPending.IsScheduled())
	require.Equal(t, capable.Name, currentPending.Worker)
}

func TestSchedulingLoopPlacesVMWithExposedPortConflictOnAnotherWorker(t *testing.T) {
	for _, profile := range []v1.SchedulerProfile{
		v1.SchedulerProfileOptimizeUtilization,
		v1.SchedulerProfileDistributeLoad,
	} {
		t.Run(string(profile), func(t *testing.T) {
			workerA := newTestWorker("worker-a")
			workerB := newTestWorker("worker-b")

			running := newAssignedVM("running-vm", workerA.Name, v1.VMStatusRunning)
			running.NetSoftnetExpose = []string{"2222:22"}

			pending := newPendingVM("pending-vm")
			pending.NetSoftnetExpose = []string{"2222:80"}

			store := newTestStore(t, profile, []v1.Worker{workerA, workerB}, []v1.VM{running, pending})

			numWorkers, numVMs, err := newTestScheduler(t, store).schedulingLoopIteration()
			require.NoError(t, err)
			require.Equal(t, 2, numWorkers)
			require.Equal(t, 2, numVMs)

			currentPending := getVM(t, store, pending.Name)
			require.True(t, currentPending.IsScheduled())
			require.Equal(t, workerB.Name, currentPending.Worker)
		})
	}
}

func TestSchedulingLoopSchedulesOlderVMWithSharedExposedPortFirst(t *testing.T) {
	worker := newTestWorker("worker-a")

	// The store lists VMs by name, which puts the newer VM first,
	// so the scheduler has to order them by creation time itself
	older := newPendingVM("older-vm")
	older.NetSoftnetExpose = []string{"2222:22"}

	newer := newPendingVM("newer-vm")
	newer.CreatedAt = older.CreatedAt.Add(time.Second)
	newer.NetSoftnetExpose = []string{"2222:22"}

	store := newTestStore(t, v1.SchedulerProfileOptimizeUtilization, []v1.Worker{worker}, []v1.VM{newer, older})

	numWorkers, numVMs, err := newTestScheduler(t, store).schedulingLoopIteration()
	require.NoError(t, err)
	require.Equal(t, 1, numWorkers)
	require.Equal(t, 2, numVMs)

	currentOlder := getVM(t, store, older.Name)
	require.True(t, currentOlder.IsScheduled())
	require.Equal(t, worker.Name, currentOlder.Worker)

	currentNewer := getVM(t, store, newer.Name)
	require.False(t, currentNewer.IsScheduled())
	require.Empty(t, currentNewer.Worker)
}

func TestSchedulingLoopFailedScheduledVMHoldsExposedPort(t *testing.T) {
	worker := newTestWorker("worker-a")

	// Failed, but not yet de-scheduled by the health-checking loop
	failed := newAssignedVM("failed-vm", worker.Name, v1.VMStatusFailed)
	failed.NetSoftnetExpose = []string{"2222:22"}

	pending := newPendingVM("pending-vm")
	pending.NetSoftnetExpose = []string{"2222:80"}

	store := newTestStore(t, v1.SchedulerProfileOptimizeUtilization, []v1.Worker{worker}, []v1.VM{failed, pending})

	numWorkers, numVMs, err := newTestScheduler(t, store).schedulingLoopIteration()
	require.NoError(t, err)
	require.Equal(t, 1, numWorkers)
	require.Equal(t, 2, numVMs)

	currentPending := getVM(t, store, pending.Name)
	require.False(t, currentPending.IsScheduled())
	require.Empty(t, currentPending.Worker)
}

// hookedStore runs a callback before the first Update, standing in for
// a specification update that lands between the scheduler's snapshot of
// the VMs and its scheduling transaction.
type hookedStore struct {
	storepkg.Store
	beforeUpdate func()
}

func (store *hookedStore) Update(cb func(txn storepkg.Transaction) error) error {
	if store.beforeUpdate != nil {
		beforeUpdate := store.beforeUpdate
		store.beforeUpdate = nil
		beforeUpdate()
	}

	return store.Store.Update(cb)
}

func TestSchedulingLoopRechecksExposedPortInTransaction(t *testing.T) {
	worker := newTestWorker("worker-a")
	worker.Resources = v1.Resources{v1.ResourceTartVMs: 3}

	running := newAssignedVM("running-vm", worker.Name, v1.VMStatusRunning)

	pending := newPendingVM("pending-vm")
	pending.NetSoftnetExpose = []string{"2222:22"}

	store := newTestStore(t, v1.SchedulerProfileOptimizeUtilization, []v1.Worker{worker},
		[]v1.VM{running, pending})

	hooked := &hookedStore{Store: store}
	hooked.beforeUpdate = func() {
		// Expose the pending VM's port on the worker after the scheduler took its snapshot
		err := store.Update(func(txn storepkg.Transaction) error {
			vm, err := txn.GetVM(running.Name)
			if err != nil {
				return err
			}

			vm.NetSoftnetExpose = []string{"2222:80"}

			return txn.SetVM(*vm)
		})
		require.NoError(t, err)
	}

	numWorkers, numVMs, err := newTestScheduler(t, hooked).schedulingLoopIteration()
	require.NoError(t, err)
	require.Equal(t, 1, numWorkers)
	require.Equal(t, 2, numVMs)

	currentPending := getVM(t, store, pending.Name)
	require.False(t, currentPending.IsScheduled())
	require.Empty(t, currentPending.Worker)
}

func TestSchedulingLoopSkipsVMUpdatedSinceSnapshot(t *testing.T) {
	worker := newTestWorker("worker-a")

	pending := newPendingVM("pending-vm")
	pending.NetSoftnetExpose = []string{"2222:22"}

	store := newTestStore(t, v1.SchedulerProfileOptimizeUtilization, []v1.Worker{worker}, []v1.VM{pending})

	hooked := &hookedStore{Store: store}
	hooked.beforeUpdate = func() {
		// Change the pending VM's specification after the scheduler took its snapshot
		err := store.Update(func(txn storepkg.Transaction) error {
			vm, err := txn.GetVM(pending.Name)
			if err != nil {
				return err
			}

			vm.NetSoftnetExpose = []string{"2223:22"}
			vm.Generation++

			return txn.SetVM(*vm)
		})
		require.NoError(t, err)
	}

	numWorkers, numVMs, err := newTestScheduler(t, hooked).schedulingLoopIteration()
	require.NoError(t, err)
	require.Equal(t, 1, numWorkers)
	require.Equal(t, 1, numVMs)

	// The update survived and the VM waits for the next iteration
	currentPending := getVM(t, store, pending.Name)
	require.False(t, currentPending.IsScheduled())
	require.Equal(t, []string{"2223:22"}, currentPending.NetSoftnetExpose)
}
