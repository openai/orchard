//nolint:testpackage // Exercise the scheduler reconciliation loop against the store.
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

const (
	pendingTestWorkerName = "worker-a"
	pendingTestFailed     = "failed"
	pendingTestStopped    = "stopped"
	pendingTestScheduled  = "scheduled"
	pendingTestDeleted    = "deleted"
)

func TestSchedulingLoopExplainsPendingVM(t *testing.T) {
	for _, unavailable := range []string{"missing", "paused", "offline", "capacity"} {
		t.Run(unavailable, func(t *testing.T) {
			scheduler, vm := newPendingScheduler(t)
			store := scheduler.store

			var worker v1.Worker
			worker.Name = pendingTestWorkerName
			worker.MachineID = "machine-a"
			worker.Arch = vm.Arch
			worker.Runtime = vm.Runtime
			worker.LastSeen = time.Now()
			worker.Resources = v1.Resources{v1.ResourceTartVMs: 1}

			switch unavailable {
			case "paused":
				worker.SchedulingPaused = true
			case "offline":
				worker.LastSeen = time.Now().Add(-time.Hour)
			case "capacity":
				worker.Resources[v1.ResourceTartVMs] = 0
			}

			if unavailable != "missing" {
				require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
					return txn.SetWorker(worker)
				}))
			}

			// Repeated reconciliation must keep the VM pending with an explanation.
			var waitingVersion uint64
			for iteration := range 2 {
				_, _, err := scheduler.schedulingLoopIteration()
				require.NoError(t, err)
				require.NoError(t, store.View(func(txn storepkg.Transaction) error {
					current, err := txn.GetVM(vm.Name)
					require.NoError(t, err)
					require.Equal(t, v1.VMStatusPending, current.Status)
					require.Equal(t, "Waiting for an available worker", current.StatusMessage)
					require.False(t, current.IsScheduled())
					require.Empty(t, current.Worker)
					if iteration == 0 {
						waitingVersion = current.Version
					} else {
						require.Equal(t, waitingVersion, current.Version, "unchanged status should not be rewritten")
					}
					return nil
				}))
			}

			worker.SchedulingPaused = false
			worker.LastSeen = time.Now()
			worker.Resources[v1.ResourceTartVMs] = 1
			require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
				return txn.SetWorker(worker)
			}))
			_, _, err := scheduler.schedulingLoopIteration()
			require.NoError(t, err)
			require.NoError(t, store.View(func(txn storepkg.Transaction) error {
				current, err := txn.GetVM(vm.Name)
				require.NoError(t, err)
				require.True(t, current.IsScheduled())
				require.Equal(t, worker.Name, current.Worker)
				require.Empty(t, current.StatusMessage)
				return nil
			}))
		})
	}
}

func TestSchedulingLoopPreservesInactiveVMMessage(t *testing.T) {
	for _, state := range []string{pendingTestFailed, pendingTestStopped, "suspended", pendingTestScheduled} {
		t.Run(state, func(t *testing.T) {
			scheduler, vm := newPendingScheduler(t)
			store := scheduler.store
			vm.StatusMessage = "Existing status message"
			switch state {
			case pendingTestFailed:
				vm.Status = v1.VMStatusFailed
			case pendingTestStopped:
				vm.PowerState = v1.PowerStateStopped
			case "suspended":
				vm.PowerState = v1.PowerStateSuspended
			case pendingTestScheduled:
				vm.Worker = pendingTestWorkerName
				vm.Conditions[0].State = v1.ConditionStateTrue
			}
			require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
				return txn.SetVM(vm)
			}))
			require.NoError(t, store.View(func(txn storepkg.Transaction) error {
				current, err := txn.GetVM(vm.Name)
				require.NoError(t, err)
				vm = *current
				return nil
			}))
			_, _, err := scheduler.schedulingLoopIteration()
			require.NoError(t, err)
			require.NoError(t, store.View(func(txn storepkg.Transaction) error {
				current, err := txn.GetVM(vm.Name)
				require.NoError(t, err)
				require.Equal(t, vm, *current)
				return nil
			}))
		})
	}
}

func TestWaitingForWorkerSkipsChangedVM(t *testing.T) {
	for _, change := range []string{
		pendingTestDeleted, "replaced", "updated", pendingTestScheduled, pendingTestFailed, pendingTestStopped,
	} {
		t.Run(change, func(t *testing.T) {
			scheduler, staleVM := newPendingScheduler(t)
			store := scheduler.store
			currentVM := staleVM
			currentVM.StatusMessage = "New status message"
			require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
				switch change {
				case pendingTestDeleted:
					return txn.DeleteVM(staleVM.Name)
				case "replaced":
					currentVM.UID = "replacement-uid"
				case "updated":
					currentVM.Generation++
				case pendingTestScheduled:
					currentVM.Worker = pendingTestWorkerName
					currentVM.Conditions = []v1.Condition{{Type: v1.ConditionTypeScheduled, State: v1.ConditionStateTrue}}
				case pendingTestFailed:
					currentVM.Status = v1.VMStatusFailed
				case pendingTestStopped:
					currentVM.PowerState = v1.PowerStateStopped
				}
				return txn.SetVM(currentVM)
			}))

			require.NoError(t, scheduler.setWaitingForWorker(staleVM))
			require.NoError(t, store.View(func(txn storepkg.Transaction) error {
				current, err := txn.GetVM(staleVM.Name)
				if change == pendingTestDeleted {
					require.ErrorIs(t, err, storepkg.ErrNotFound)
				} else {
					require.NoError(t, err)
					require.Equal(t, currentVM.StatusMessage, current.StatusMessage)
				}
				return nil
			}))
		})
	}
}

func newPendingScheduler(t *testing.T) (*Scheduler, v1.VM) {
	logger := zap.NewNop().Sugar()
	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	var vm v1.VM
	vm.Name = "pending-vm"
	vm.UID = "pending-vm-uid"
	vm.Status = v1.VMStatusPending
	vm.Resources = v1.Resources{v1.ResourceTartVMs: 1}
	vm.Arch = v1.ArchitectureARM64
	vm.Runtime = v1.RuntimeTart
	vm.PowerState = v1.PowerStateRunning
	vm.Conditions = []v1.Condition{{Type: v1.ConditionTypeScheduled, State: v1.ConditionStateFalse}}
	require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
		if err := txn.SetClusterSettings(v1.ClusterSettings{}); err != nil {
			return err
		}
		return txn.SetVM(vm)
	}))
	scheduler, err := NewScheduler(store, notifier.NewNotifier(logger), time.Minute, logger)
	require.NoError(t, err)
	return scheduler, vm
}
