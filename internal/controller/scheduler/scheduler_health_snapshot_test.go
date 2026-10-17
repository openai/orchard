//nolint:testpackage // Exercise reconciliation between the snapshot and update transaction.
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

type beforeHealthUpdateStore struct {
	storepkg.Store

	beforeUpdate func() error
}

func (store *beforeHealthUpdateStore) Update(callback func(storepkg.Transaction) error) error {
	if hook := store.beforeUpdate; hook != nil {
		store.beforeUpdate = nil
		if err := hook(); err != nil {
			return err
		}
	}
	return store.Store.Update(callback)
}

func TestHealthCheckingRechecksCurrentVMScheduling(t *testing.T) {
	const deleted = "deleted"
	const replaced = "replaced"

	for _, mode := range []string{replaced, "descheduled", "still-scheduled", deleted} {
		t.Run(mode, func(t *testing.T) {
			logger := zap.NewNop().Sugar()
			base, err := badger.NewBadgerStore(t.TempDir(), true, logger)
			require.NoError(t, err)

			var original v1.VM
			original.Name = "synthetic-vm"
			original.UID = "original-uid"
			original.Status = v1.VMStatusRunning
			original.Worker = "missing-worker"
			original.Conditions = []v1.Condition{{Type: v1.ConditionTypeScheduled, State: v1.ConditionStateTrue}}
			require.NoError(t, base.Update(func(txn storepkg.Transaction) error { return txn.SetVM(original) }))

			current := original
			if mode == replaced || mode == "descheduled" {
				current.Status = v1.VMStatusPending
				current.Worker = ""
				current.Conditions = []v1.Condition{{Type: v1.ConditionTypeScheduled, State: v1.ConditionStateFalse}}
				if mode == replaced {
					current.UID = "replacement-uid"
				}
			}
			store := &beforeHealthUpdateStore{Store: base}
			store.beforeUpdate = func() error {
				if err := base.Update(func(txn storepkg.Transaction) error {
					if mode == deleted {
						return txn.DeleteVM(original.Name)
					}
					return txn.SetVM(current)
				}); err != nil {
					return err
				}
				if mode == deleted {
					return nil
				}
				return base.View(func(txn storepkg.Transaction) error {
					saved, err := txn.GetVM(current.Name)
					if err == nil {
						current = *saved
					}
					return err
				})
			}
			scheduler, err := NewScheduler(store, notifier.NewNotifier(logger), time.Minute, logger)
			require.NoError(t, err)
			count, err := scheduler.healthCheckingLoopIteration()
			require.NoError(t, err)
			require.Equal(t, 1, count)
			assertHealthSnapshot(t, base, current, mode == deleted, mode == "still-scheduled")
		})
	}
}

func assertHealthSnapshot(t *testing.T, store storepkg.Store, expected v1.VM, deleted, failed bool) {
	t.Helper()

	require.NoError(t, store.View(func(txn storepkg.Transaction) error {
		actual, err := txn.GetVM(expected.Name)
		if deleted {
			require.ErrorIs(t, err, storepkg.ErrNotFound)
			return nil
		}
		require.NoError(t, err)
		if failed {
			require.Equal(t, v1.VMStatusFailed, actual.Status)
			require.Contains(t, actual.StatusMessage, "doesn't exist")
		} else {
			require.Equal(t, expected, *actual, "unscheduled current VMs must remain unchanged")
		}
		return nil
	}))
}
