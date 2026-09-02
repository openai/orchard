package store_test

import (
	"testing"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestListEventsPage(t *testing.T) {
	logger := zap.NewNop().Sugar()

	for _, storeImpl := range testStores(logger) {
		t.Run(storeImpl.Name, func(t *testing.T) {
			store := storeImpl.Init(t)
			testListEventsPage(t, store)
		})
	}
}

func testListEventsPage(t *testing.T, store storepkg.Store) {
	events := []v1.Event{
		{Kind: v1.EventKindLogLine, Timestamp: 1, Payload: "one"},
		{Kind: v1.EventKindLogLine, Timestamp: 2, Payload: "two"},
		{Kind: v1.EventKindLogLine, Timestamp: 3, Payload: "three"},
		{Kind: v1.EventKindLogLine, Timestamp: 4, Payload: "four"},
	}

	err := store.Update(func(txn storepkg.Transaction) error {
		return txn.AppendEvents(events, "vms", "vm-uid")
	})
	require.NoError(t, err)

	var page storepkg.Page[v1.Event]
	err = store.View(func(txn storepkg.Transaction) error {
		page, err = txn.ListEventsPage(storepkg.ListOptions{
			Limit: 2,
		}, "vms", "vm-uid")
		return err
	})
	require.NoError(t, err)
	require.Equal(t, events[:2], page.Items)
	require.NotEmpty(t, page.NextCursor)

	var page2 storepkg.Page[v1.Event]
	err = store.View(func(txn storepkg.Transaction) error {
		page2, err = txn.ListEventsPage(storepkg.ListOptions{
			Limit:  2,
			Cursor: page.NextCursor,
		}, "vms", "vm-uid")
		return err
	})
	require.NoError(t, err)
	require.Equal(t, events[2:], page2.Items)
	require.Empty(t, page2.NextCursor)

	var descPage storepkg.Page[v1.Event]
	err = store.View(func(txn storepkg.Transaction) error {
		descPage, err = txn.ListEventsPage(storepkg.ListOptions{
			Limit: 2,
			Order: storepkg.ListOrderDesc,
		}, "vms", "vm-uid")
		return err
	})
	require.NoError(t, err)
	require.Equal(t, []v1.Event{events[3], events[2]}, descPage.Items)
	require.NotEmpty(t, descPage.NextCursor)

	var descPage2 storepkg.Page[v1.Event]
	err = store.View(func(txn storepkg.Transaction) error {
		descPage2, err = txn.ListEventsPage(storepkg.ListOptions{
			Limit:  2,
			Cursor: descPage.NextCursor,
			Order:  storepkg.ListOrderDesc,
		}, "vms", "vm-uid")
		return err
	})
	require.NoError(t, err)
	require.Equal(t, []v1.Event{events[1], events[0]}, descPage2.Items)
	require.Empty(t, descPage2.NextCursor)
}

func TestDeleteManyEvents(t *testing.T) {
	logger := zap.NewNop().Sugar()

	for _, storeImpl := range testStores(logger) {
		t.Run(storeImpl.Name, func(t *testing.T) {
			store := storeImpl.Init(t)
			testDeleteManyEvents(t, store)
		})
	}
}

func testDeleteManyEvents(t *testing.T, store storepkg.Store) {
	events := make([]v1.Event, 200)
	for i := range events {
		events[i] = v1.Event{Kind: v1.EventKindLogLine, Timestamp: int64(i), Payload: "log line"}
	}

	for _, eventBatch := range [][]v1.Event{events[:100], events[100:]} {
		err := store.Update(func(txn storepkg.Transaction) error {
			return txn.AppendEvents(eventBatch, "vms", "vm-with-many-events")
		})
		require.NoError(t, err)
	}

	err := store.Update(func(txn storepkg.Transaction) error {
		return txn.DeleteEvents("vms", "vm-with-many-events")
	})
	require.NoError(t, err)

	var remaining []v1.Event
	err = store.View(func(txn storepkg.Transaction) error {
		var err error
		remaining, err = txn.ListEvents("vms", "vm-with-many-events")

		return err
	})
	require.NoError(t, err)
	require.Empty(t, remaining)
}
