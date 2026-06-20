package etcd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"time"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const SpaceEvents = "/events"

func scopePrefix(scope []string) string {
	keyParts := []string{SpaceEvents}
	keyParts = append(keyParts, scope...)

	return path.Join(keyParts...)
}

func (txn *Transaction) AppendEvents(events []v1.Event, scope ...string) error {
	injectionTime := time.Now().UnixNano()

	for index, event := range events {
		valueBytes, err := json.Marshal(event)
		if err != nil {
			return err
		}

		eventUID := fmt.Sprintf("/%d-%d-%06d",
			event.Timestamp,
			injectionTime,
			index,
		)
		eventKey := txn.store.key(scopePrefix(scope) + eventUID)

		txn.puts[eventKey] = string(valueBytes)
		delete(txn.deletes, eventKey)
	}

	return nil
}

func (txn *Transaction) ListEvents(scope ...string) ([]v1.Event, error) {
	page, err := txn.ListEventsPage(storepkg.ListOptions{}, scope...)
	if err != nil {
		return nil, err
	}

	return page.Items, nil
}

func (txn *Transaction) ListEventsPage(options storepkg.ListOptions, scope ...string) (
	storepkg.Page[v1.Event],
	error,
) {
	var result storepkg.Page[v1.Event]
	result.Items = []v1.Event{}

	logicalPrefix := scopePrefix(scope)
	physicalPrefix := txn.store.keyPrefix(logicalPrefix)
	response, err := txn.store.client.Get(txn.ctx, physicalPrefix, clientv3.WithPrefix(), clientv3.WithSort(
		clientv3.SortByKey, clientv3.SortAscend,
	))
	if err != nil {
		return result, mapErr(err)
	}
	txn.prefixReadRevisions[physicalPrefix] = response.Header.Revision

	type keyedEvent struct {
		key   string
		event v1.Event
	}

	keyedEventsByKey := map[string]v1.Event{}
	for _, kv := range response.Kvs {
		var event v1.Event
		if err := json.Unmarshal(kv.Value, &event); err != nil {
			return result, err
		}

		keyedEventsByKey[string(kv.Key)] = event
	}

	for key, value := range txn.puts {
		if !hasPrefix(key, physicalPrefix) {
			continue
		}

		var event v1.Event
		if err := json.Unmarshal([]byte(value), &event); err != nil {
			return result, err
		}
		keyedEventsByKey[key] = event
	}

	keyedEvents := make([]keyedEvent, 0, len(keyedEventsByKey))
	for key, event := range keyedEventsByKey {
		if _, deleted := txn.deletes[key]; deleted {
			continue
		}

		keyedEvents = append(keyedEvents, keyedEvent{key: key, event: event})
	}

	sort.Slice(keyedEvents, func(i, j int) bool {
		if options.Order == storepkg.ListOrderDesc {
			return keyedEvents[i].key > keyedEvents[j].key
		}

		return keyedEvents[i].key < keyedEvents[j].key
	})

	startIndex := 0
	if len(options.Cursor) > 0 {
		cursor := eventCursor(physicalPrefix, logicalPrefix, options.Cursor)
		for index, keyedEvent := range keyedEvents {
			if keyedEvent.key == cursor {
				startIndex = index + 1
				break
			}
		}
	}

	for index := startIndex; index < len(keyedEvents); index++ {
		result.Items = append(result.Items, keyedEvents[index].event)

		if options.Limit > 0 && len(result.Items) >= options.Limit {
			if index+1 < len(keyedEvents) {
				result.NextCursor = bytes.TrimPrefix([]byte(keyedEvents[index].key), []byte(physicalPrefix))
			}

			break
		}
	}

	return result, nil
}

func (txn *Transaction) DeleteEvents(scope ...string) error {
	physicalPrefix := txn.store.keyPrefix(scopePrefix(scope))
	response, err := txn.store.client.Get(txn.ctx, physicalPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly())
	if err != nil {
		return mapErr(err)
	}
	txn.prefixReadRevisions[physicalPrefix] = response.Header.Revision

	for _, kv := range response.Kvs {
		txn.deletes[string(kv.Key)] = struct{}{}
		delete(txn.puts, string(kv.Key))
	}

	return nil
}

func eventCursor(physicalPrefix, logicalPrefix string, cursor []byte) string {
	if bytes.HasPrefix(cursor, []byte(physicalPrefix)) {
		return string(cursor)
	}
	if bytes.HasPrefix(cursor, []byte(logicalPrefix)) {
		return path.Join(physicalPrefix, string(bytes.TrimPrefix(cursor, []byte(logicalPrefix))))
	}

	return physicalPrefix + string(cursor)
}
