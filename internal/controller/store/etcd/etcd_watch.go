package etcd

import (
	"context"
	"encoding/json"

	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/cirruslabs/orchard/pkg/resource/v1"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (store *Store) WatchVM(ctx context.Context, vmName string) (chan storepkg.WatchMessage[v1.VM], chan error, error) {
	watchCh := make(chan storepkg.WatchMessage[v1.VM], 1)
	errCh := make(chan error, 1)
	key := store.VMKey(vmName)

	response, err := store.client.Get(ctx, key)
	if err != nil {
		return nil, nil, mapErr(err)
	}

	exists := len(response.Kvs) != 0
	watchRevision := response.Header.Revision + 1
	if exists {
		var vm v1.VM
		if err := json.Unmarshal(response.Kvs[0].Value, &vm); err != nil {
			return nil, nil, err
		}
		vm.Version = uint64(response.Kvs[0].ModRevision)

		watchCh <- storepkg.WatchMessage[v1.VM]{
			Type:   storepkg.WatchMessageTypeAdded,
			Object: vm,
		}
	}

	go func() {
		defer close(watchCh)
		defer close(errCh)

		watchResponses := store.client.Watch(ctx, key, clientv3.WithRev(watchRevision))
		for watchResponse := range watchResponses {
			if err := watchResponse.Err(); err != nil {
				errCh <- mapErr(err)

				return
			}

			for _, event := range watchResponse.Events {
				switch event.Type {
				case clientv3.EventTypeDelete:
					if !exists {
						continue
					}

					exists = false
					select {
					case watchCh <- storepkg.WatchMessage[v1.VM]{Type: storepkg.WatchMessageTypeDeleted}:
					case <-ctx.Done():
						return
					}
				case clientv3.EventTypePut:
					var vm v1.VM
					if err := json.Unmarshal(event.Kv.Value, &vm); err != nil {
						errCh <- err

						return
					}
					vm.Version = uint64(event.Kv.ModRevision)

					messageType := storepkg.WatchMessageTypeAdded
					if exists {
						messageType = storepkg.WatchMessageTypeModified
					}
					exists = true

					select {
					case watchCh <- storepkg.WatchMessage[v1.VM]{Type: messageType, Object: vm}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return watchCh, errCh, nil
}
