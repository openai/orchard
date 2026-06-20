package etcd

import (
	"encoding/json"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func genericSet[T any](txn *Transaction, key string, obj T) error {
	valueBytes, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	txn.puts[key] = string(valueBytes)
	delete(txn.deletes, key)

	return nil
}

func genericGet[T any, PT interface {
	SetVersion(uint64)
	*T
}](txn *Transaction, key string) (*T, error) {
	if _, deleted := txn.deletes[key]; deleted {
		return nil, storepkg.ErrNotFound
	}
	if value, ok := txn.puts[key]; ok {
		var obj T
		if err := json.Unmarshal([]byte(value), &obj); err != nil {
			return nil, err
		}

		return &obj, nil
	}

	response, err := txn.store.client.Get(txn.ctx, key)
	if err != nil {
		return nil, mapErr(err)
	}
	if len(response.Kvs) == 0 {
		txn.readRevisions[key] = 0

		return nil, storepkg.ErrNotFound
	}

	kv := response.Kvs[0]
	txn.readRevisions[key] = kv.ModRevision

	var obj T
	if err := json.Unmarshal(kv.Value, &obj); err != nil {
		return nil, err
	}
	PT(&obj).SetVersion(uint64(kv.ModRevision))

	return &obj, nil
}

func genericList[T any, PT interface {
	SetVersion(uint64)
	*T
}](txn *Transaction, logicalPrefix string) ([]T, error) {
	physicalPrefix := txn.store.keyPrefix(logicalPrefix)
	response, err := txn.store.client.Get(txn.ctx, physicalPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, mapErr(err)
	}
	txn.prefixReadRevisions[physicalPrefix] = response.Header.Revision

	itemsByKey := map[string]T{}
	keys := make([]string, 0, len(response.Kvs)+len(txn.puts))

	for _, kv := range response.Kvs {
		var obj T
		if err := json.Unmarshal(kv.Value, &obj); err != nil {
			return nil, err
		}
		PT(&obj).SetVersion(uint64(kv.ModRevision))

		key := string(kv.Key)
		itemsByKey[key] = obj
		keys = append(keys, key)
	}

	for key, value := range txn.puts {
		if !hasPrefix(key, physicalPrefix) {
			continue
		}

		var obj T
		if err := json.Unmarshal([]byte(value), &obj); err != nil {
			return nil, err
		}

		if _, ok := itemsByKey[key]; !ok {
			keys = append(keys, key)
		}
		itemsByKey[key] = obj
	}

	result := []T{}
	for _, key := range keys {
		if _, deleted := txn.deletes[key]; deleted {
			continue
		}

		result = append(result, itemsByKey[key])
	}

	return result, nil
}

func genericDelete(txn *Transaction, key string) error {
	delete(txn.puts, key)
	txn.deletes[key] = struct{}{}

	return nil
}

func hasPrefix(value, prefix string) bool {
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}
