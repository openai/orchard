//nolint:testpackage // Exercise the scheduler's placement and restart loops.
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

func TestWorkerNameAffinitySurvivesRestart(t *testing.T) {
	logger := zap.NewNop().Sugar()
	store, err := badger.NewBadgerStore(t.TempDir(), true, logger)
	require.NoError(t, err)

	var owner v1.Worker
	owner.Name = "worker-a"
	owner.MachineID = "machine-a"
	owner.LastSeen = time.Now().Add(-time.Hour)
	owner.Arch = v1.ArchitectureARM64
	owner.Runtime = v1.RuntimeTart
	owner.Resources = v1.Resources{v1.ResourceTartVMs: 2}
	owner.Labels = v1.Labels{v1.LabelWorkerName: owner.Name}

	other := owner
	other.Name = "worker-b"
	other.MachineID = "machine-b"
	other.LastSeen = time.Now()
	other.Labels = v1.Labels{v1.LabelWorkerName: other.Name}

	var vm v1.VM
	vm.Name = "workspace"
	vm.UID = "workspace-uid"
	vm.CreatedAt = time.Now()
	vm.Status = v1.VMStatusFailed
	vm.Worker = owner.Name
	vm.Arch = v1.ArchitectureARM64
	vm.Runtime = v1.RuntimeTart
	vm.Resources = v1.Resources{v1.ResourceTartVMs: 1}
	vm.Labels = v1.Labels{v1.LabelWorkerName: owner.Name}
	vm.HostDirs = []v1.HostDir{{Name: "workspace", Path: "/path/on/worker"}}
	vm.RestartPolicy = v1.RestartPolicyOnFailure
	vm.PowerState = v1.PowerStateRunning
	vm.Conditions = []v1.Condition{{Type: v1.ConditionTypeScheduled, State: v1.ConditionStateTrue}}

	require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
		if err := txn.SetClusterSettings(v1.ClusterSettings{}); err != nil {
			return err
		}
		if err := txn.SetWorker(owner); err != nil {
			return err
		}
		if err := txn.SetWorker(other); err != nil {
			return err
		}
		return txn.SetVM(vm)
	}))

	scheduler, err := NewScheduler(store, notifier.NewNotifier(logger), time.Minute, logger)
	require.NoError(t, err)
	readVM := func() v1.VM {
		var current *v1.VM
		require.NoError(t, store.View(func(txn storepkg.Transaction) error {
			var err error
			current, err = txn.GetVM(vm.Name)
			return err
		}))
		return *current
	}

	_, err = scheduler.healthCheckingLoopIteration()
	require.NoError(t, err)
	current := readVM()
	require.Equal(t, uint64(1), current.RestartCount)
	require.Equal(t, vm.Labels, current.Labels)
	require.Equal(t, vm.HostDirs, current.HostDirs)

	_, _, err = scheduler.schedulingLoopIteration()
	require.NoError(t, err)
	current = readVM()
	require.False(t, current.IsScheduled())
	require.Empty(t, current.Worker)

	owner.LastSeen = time.Now()
	require.NoError(t, store.Update(func(txn storepkg.Transaction) error {
		return txn.SetWorker(owner)
	}))
	_, _, err = scheduler.schedulingLoopIteration()
	require.NoError(t, err)
	current = readVM()
	require.True(t, current.IsScheduled())
	require.Equal(t, owner.Name, current.Worker)
}
