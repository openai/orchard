package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWatchVM(t *testing.T) {
	logger := zap.Must(zap.NewDevelopment())

	testCases := []struct {
		Name string
		Run  func(t *testing.T, store storepkg.Store)
	}{
		{
			Name: "simple-vm-already-exists",
			Run: func(t *testing.T, store storepkg.Store) {
				// Create a VM
				const vmName = "test"

				vm := v1.VM{
					Meta: v1.Meta{
						Name: vmName,
					},
				}

				err := store.Update(func(txn storepkg.Transaction) error {
					return txn.SetVM(vm)
				})
				require.NoError(t, err)

				// Start watching a VM
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()

				watchCh, errCh, err := store.WatchVM(ctx, vmName)
				require.NoError(t, err)

				// Ensure that a synthetic VM creation event is emitted
				select {
				case item := <-watchCh:
					require.Equal(t, item.Type, storepkg.WatchMessageTypeAdded)
				case err := <-errCh:
					require.NoError(t, err)
				case <-ctx.Done():
					require.FailNow(t, "timed out waiting for ADDED watch event")
				}

				// Update the VM and ensure that a modification event is emitted
				err = store.Update(func(txn storepkg.Transaction) error {
					return txn.SetVM(vm)
				})
				require.NoError(t, err)

				select {
				case item := <-watchCh:
					require.Equal(t, item.Type, storepkg.WatchMessageTypeModified)
				case err := <-errCh:
					require.NoError(t, err)
				case <-ctx.Done():
					require.FailNow(t, "timed out waiting for MODIFIED watch event")
				}

				// Delete the VM and ensure that a deletion event is emitted
				err = store.Update(func(txn storepkg.Transaction) error {
					return txn.DeleteVM(vmName)
				})
				require.NoError(t, err)

				select {
				case item := <-watchCh:
					require.Equal(t, item.Type, storepkg.WatchMessageTypeDeleted)
				case err := <-errCh:
					require.NoError(t, err)
				case <-ctx.Done():
					require.FailNow(t, "timed out waiting for DELETED watch event")
				}
			},
		},
		{
			Name: "simple-vm-not-yet-exists",
			Run: func(t *testing.T, store storepkg.Store) {
				// Start watching a VM
				const vmName = "test"

				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()

				watchCh, errCh, err := store.WatchVM(ctx, vmName)
				require.NoError(t, err)

				// Create a VM
				vm := v1.VM{
					Meta: v1.Meta{
						Name: vmName,
					},
				}

				err = store.Update(func(txn storepkg.Transaction) error {
					return txn.SetVM(vm)
				})
				require.NoError(t, err)

				// Ensure that a VM creation event is emitted
				select {
				case item := <-watchCh:
					require.Equal(t, item.Type, storepkg.WatchMessageTypeAdded)
				case err := <-errCh:
					require.NoError(t, err)
				case <-ctx.Done():
					require.FailNow(t, "timed out waiting for ADDED watch event")
				}

				// Update the VM and ensure that a modification event is emitted
				err = store.Update(func(txn storepkg.Transaction) error {
					return txn.SetVM(vm)
				})
				require.NoError(t, err)

				select {
				case item := <-watchCh:
					require.Equal(t, item.Type, storepkg.WatchMessageTypeModified)
				case err := <-errCh:
					require.NoError(t, err)
				case <-ctx.Done():
					require.FailNow(t, "timed out waiting for MODIFIED watch event")
				}

				// Delete the VM and ensure that a deletion event is emitted
				err = store.Update(func(txn storepkg.Transaction) error {
					return txn.DeleteVM(vmName)
				})
				require.NoError(t, err)

				select {
				case item := <-watchCh:
					require.Equal(t, item.Type, storepkg.WatchMessageTypeDeleted)
				case err := <-errCh:
					require.NoError(t, err)
				case <-ctx.Done():
					require.FailNow(t, "timed out waiting for DELETED watch event")
				}
			},
		},
	}

	for _, testCase := range testCases {
		for _, storeImpl := range testStores(logger.Sugar()) {
			name := fmt.Sprintf("%s-%s", testCase.Name, storeImpl.Name)

			t.Run(name, func(t *testing.T) {
				store := storeImpl.Init(t)
				testCase.Run(t, store)
			})
		}
	}
}
