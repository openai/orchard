package etcd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path"
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
	getKey, getOptions := txn.listEventsPageQueryOptions(physicalPrefix, logicalPrefix, options)
	response, err := txn.store.client.Get(txn.ctx, getKey, getOptions...)
	if err != nil {
		return result, mapErr(err)
	}
	txn.prefixReadRevisions[physicalPrefix] = response.Header.Revision

	limit := options.Limit
	for _, kv := range response.Kvs {
		key := string(kv.Key)
		if txn.isDeleted(key) {
			continue
		}

		var event v1.Event
		if err := json.Unmarshal(kv.Value, &event); err != nil {
			return result, err
		}
		if limit > 0 && len(result.Items) >= limit {
			break
		}

		result.Items = append(result.Items, event)

		if limit > 0 && len(result.Items) == limit && len(response.Kvs) > limit {
			result.NextCursor = bytes.TrimPrefix([]byte(key), []byte(physicalPrefix))
		}
	}

	return result, nil
}

func (txn *Transaction) DeleteEvents(scope ...string) error {
	physicalPrefix := txn.store.keyPrefix(scopePrefix(scope))
	response, err := txn.store.client.Get(txn.ctx, physicalPrefix, clientv3.WithPrefix(), clientv3.WithKeysOnly(),
		clientv3.WithLimit(1))
	if err != nil {
		return mapErr(err)
	}
	txn.prefixReadRevisions[physicalPrefix] = response.Header.Revision

	txn.prefixDeletes[physicalPrefix] = struct{}{}
	for key := range txn.puts {
		if hasPrefix(key, physicalPrefix) {
			delete(txn.puts, key)
		}
	}

	return nil
}

func (txn *Transaction) listEventsPageQueryOptions(
	physicalPrefix string,
	logicalPrefix string,
	options storepkg.ListOptions,
) (string, []clientv3.OpOption) {
	rangeEnd := clientv3.GetPrefixRangeEnd(physicalPrefix)
	getKey := physicalPrefix
	getRangeEnd := rangeEnd
	if len(options.Cursor) > 0 {
		cursor := eventCursor(physicalPrefix, logicalPrefix, options.Cursor)
		if options.Order == storepkg.ListOrderDesc {
			getRangeEnd = cursor
		} else {
			getKey = cursor + "\x00"
		}
	}

	getOptions := []clientv3.OpOption{
		clientv3.WithRange(getRangeEnd),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	}
	if options.Order == storepkg.ListOrderDesc {
		getOptions[1] = clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend)
	}
	if options.Limit > 0 {
		getOptions = append(getOptions, clientv3.WithLimit(int64(options.Limit+1)))
	}

	return getKey, getOptions
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
