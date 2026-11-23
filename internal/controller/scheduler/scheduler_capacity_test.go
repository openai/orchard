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

func TestSchedulingLoopSkipsOvercommittedWorker(t *testing.T) {
	logger := zap.NewNop().Sugar()

	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	var worker v1.Worker
	worker.Name = "worker-a"
	worker.LastSeen = time.Now()
	worker.MachineID = "machine-a"
	worker.Resources = v1.Resources{v1.ResourceTartVMs: 2}
	worker.Arch = v1.ArchitectureARM64
	worker.Runtime = v1.RuntimeTart

	newPendingVM := func(name string) v1.VM {
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

	assignedVM := func(name string, status v1.VMStatus) v1.VM {
		vm := newPendingVM(name)
		vm.Worker = worker.Name
		vm.Status = status
		vm.Conditions[0].State = v1.ConditionStateTrue

		return vm
	}

	pending := newPendingVM("pending-vm")

	var settings v1.ClusterSettings
	settings.SchedulerProfile = v1.SchedulerProfileOptimizeUtilization

	err = store.Update(func(txn storepkg.Transaction) error {
		if err := txn.SetClusterSettings(settings); err != nil {
			return err
		}

		if err := txn.SetWorker(worker); err != nil {
			return err
		}

		vms := []v1.VM{
			assignedVM("running-first", v1.VMStatusRunning),
			assignedVM("running-second", v1.VMStatusRunning),
			assignedVM("failed-third", v1.VMStatusFailed),
			pending,
		}

		for _, vm := range vms {
			if err := txn.SetVM(vm); err != nil {
				return err
			}
		}

		return nil
	})
	require.NoError(t, err)

	scheduler, err := NewScheduler(store, notifier.NewNotifier(logger), time.Minute, logger)
	require.NoError(t, err)

	numWorkers, numVMs, err := scheduler.schedulingLoopIteration()
	require.NoError(t, err)
	require.Equal(t, 1, numWorkers)
	require.Equal(t, 4, numVMs)

	err = store.View(func(txn storepkg.Transaction) error {
		currentVM, err := txn.GetVM(pending.Name)
		require.NoError(t, err)
		require.False(t, currentVM.IsScheduled())
		require.Empty(t, currentVM.Worker)

		return nil
	})
	require.NoError(t, err)
}

func TestSchedulingRequiresEmptyValuedLabel(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	var worker v1.Worker
	worker.Name = "worker-a"
	worker.LastSeen = time.Now()
	worker.MachineID = "machine-a"
	worker.Resources = v1.Resources{v1.ResourceTartVMs: 2}
	worker.Arch = v1.ArchitectureARM64
	worker.Runtime = v1.RuntimeTart

	var vm v1.VM
	vm.Name = "dedicated-vm"
	vm.UID = "dedicated-vm-uid"
	vm.CreatedAt = time.Now()
	vm.Status = v1.VMStatusPending
	vm.PowerState = v1.PowerStateRunning
	vm.Resources = v1.Resources{v1.ResourceTartVMs: 1}
	vm.Arch = v1.ArchitectureARM64
	vm.Runtime = v1.RuntimeTart
	vm.Labels = v1.Labels{"dedicated": ""}
	vm.Conditions = []v1.Condition{{Type: v1.ConditionTypeScheduled, State: v1.ConditionStateFalse}}

	settings := v1.ClusterSettings{SchedulerProfile: v1.SchedulerProfileOptimizeUtilization}
	require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
		if err := txn.SetClusterSettings(settings); err != nil {
			return err
		}
		if err := txn.SetWorker(worker); err != nil {
			return err
		}
		return txn.SetVM(vm)
	}))
	scheduler, err := NewScheduler(store, notifier.NewNotifier(logger), time.Minute, logger)
	require.NoError(t, err)

	_, _, err = scheduler.schedulingLoopIteration()
	require.NoError(t, err)
	require.NoError(t, store.View(func(txn storepkg.Transaction) error {
		pending, err := txn.GetVM(vm.Name)
		require.NoError(t, err)
		require.False(t, pending.IsScheduled())
		require.Empty(t, pending.Worker)
		return nil
	}))

	worker.Labels = v1.Labels{"dedicated": ""}
	require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
		return txn.SetWorker(worker)
	}))
	_, _, err = scheduler.schedulingLoopIteration()
	require.NoError(t, err)
	require.NoError(t, store.View(func(txn storepkg.Transaction) error {
		scheduled, err := txn.GetVM(vm.Name)
		require.NoError(t, err)
		require.True(t, scheduled.IsScheduled())
		require.Equal(t, worker.Name, scheduled.Worker)
		return nil
	}))
}

type replaceAfterViewStore struct {
	storepkg.Store

	replacement v1.VM
}

func (store *replaceAfterViewStore) View(cb func(storepkg.Transaction) error) error {
	if err := store.Store.View(cb); err != nil {
		return err
	}
	return store.Update(func(txn storepkg.Transaction) error {
		return txn.SetVM(store.replacement)
	})
}

func TestHealthCheckSkipsUnscheduledReplacement(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	var replacement v1.VM
	replacement.Name = "replaced-vm"
	replacement.UID = "replacement-uid"
	replacement.Status = v1.VMStatusPending

	original := replacement
	original.UID = "original-uid"
	original.Status = v1.VMStatusRunning
	original.Worker = "worker-a"
	original.Conditions = []v1.Condition{{
		Type:  v1.ConditionTypeScheduled,
		State: v1.ConditionStateTrue,
	}}
	require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
		return txn.SetVM(original)
	}))

	scheduler, err := NewScheduler(&replaceAfterViewStore{Store: store, replacement: replacement},
		notifier.NewNotifier(logger), time.Minute, logger)
	require.NoError(t, err)
	_, err = scheduler.healthCheckingLoopIteration()
	require.NoError(t, err)

	require.NoError(t, store.View(func(txn storepkg.Transaction) error {
		current, err := txn.GetVM(replacement.Name)
		require.NoError(t, err)
		require.Equal(t, replacement.UID, current.UID)
		require.Equal(t, v1.VMStatusPending, current.Status)
		require.Empty(t, current.StatusMessage)
		return nil
	}))
}
