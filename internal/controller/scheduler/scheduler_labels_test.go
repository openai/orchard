//nolint:testpackage // Exercise scheduler reconciliation across its snapshot and update transaction.
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

type beforeUpdateStore struct {
	storepkg.Store

	beforeUpdate func() error
}

func (store *beforeUpdateStore) Update(callback func(storepkg.Transaction) error) error {
	if hook := store.beforeUpdate; hook != nil {
		store.beforeUpdate = nil
		if err := hook(); err != nil {
			return err
		}
	}

	return store.Store.Update(callback)
}

func TestSchedulingLoopRechecksWorkerLabels(t *testing.T) {
	const labelKey = "pool"
	const labelValue = "required"

	for _, test := range []struct {
		name      string
		labels    v1.Labels
		scheduled bool
	}{
		{name: "changed", labels: v1.Labels{labelKey: "other"}},
		{name: "removed", labels: nil},
		{name: "unchanged", labels: v1.Labels{labelKey: labelValue}, scheduled: true},
		{name: "additional", labels: v1.Labels{labelKey: labelValue, "extra": "value"}, scheduled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			logger := zap.NewNop().Sugar()
			base, err := badger.NewBadgerStore(t.TempDir(), true, logger)
			require.NoError(t, err)

			var worker v1.Worker
			worker.Name = "worker-a"
			worker.LastSeen = time.Now()
			worker.MachineID = "machine-a"
			worker.Resources = v1.Resources{v1.ResourceTartVMs: 2}
			worker.Arch = v1.ArchitectureARM64
			worker.Runtime = v1.RuntimeTart
			worker.Labels = v1.Labels{labelKey: labelValue}

			var pending v1.VM
			pending.Name = "pending-vm"
			pending.UID = "pending-uid"
			pending.CreatedAt = time.Now()
			pending.Status = v1.VMStatusPending
			pending.Resources = v1.Resources{v1.ResourceTartVMs: 1}
			pending.Arch = worker.Arch
			pending.Runtime = worker.Runtime
			pending.Labels = v1.Labels{labelKey: labelValue}
			pending.PowerState = v1.PowerStateRunning

			require.NoError(t, base.Update(func(txn storepkg.Transaction) error {
				if err := txn.SetClusterSettings(v1.ClusterSettings{}); err != nil {
					return err
				}
				if err := txn.SetWorker(worker); err != nil {
					return err
				}
				return txn.SetVM(pending)
			}))

			store := &beforeUpdateStore{Store: base}
			store.beforeUpdate = func() error {
				// Change only labels after the scheduler snapshot was read.
				worker.Labels = test.labels
				return base.Update(func(txn storepkg.Transaction) error {
					return txn.SetWorker(worker)
				})
			}
			scheduler, err := NewScheduler(store, notifier.NewNotifier(logger), time.Minute, logger)
			require.NoError(t, err)
			_, _, err = scheduler.schedulingLoopIteration()
			require.NoError(t, err)

			require.NoError(t, base.View(func(txn storepkg.Transaction) error {
				current, err := txn.GetVM(pending.Name)
				require.NoError(t, err)
				require.Equal(t, test.scheduled, current.IsScheduled())
				if test.scheduled {
					require.Equal(t, worker.Name, current.Worker)
				} else {
					require.Empty(t, current.Worker)
				}
				return nil
			}))
		})
	}
}
