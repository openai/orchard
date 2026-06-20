package etcd

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/avast/retry-go/v4"
	storepkg "github.com/cirruslabs/orchard/internal/controller/store"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const defaultDialTimeout = 5 * time.Second

type Store struct {
	client *clientv3.Client
	prefix string
}

type Transaction struct {
	ctx                 context.Context
	store               *Store
	readRevisions       map[string]int64
	prefixReadRevisions map[string]int64
	puts                map[string]string
	deletes             map[string]struct{}
}

func NewEtcdStore(endpoints []string, keyPrefix string, logger *zap.SugaredLogger) (storepkg.Store, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("%w: at least one etcd endpoint is required", storepkg.ErrStoreFailed)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: defaultDialTimeout,
		Logger:      logger.Desugar(),
	})
	if err != nil {
		return nil, mapErr(err)
	}

	return &Store{
		client: client,
		prefix: normalizePrefix(keyPrefix),
	}, nil
}

func (store *Store) View(cb func(txn storepkg.Transaction) error) error {
	txn := store.newTransaction(context.Background())

	return cb(txn)
}

func (store *Store) Update(cb func(txn storepkg.Transaction) error) error {
	return retry.Do(func() error {
		txn := store.newTransaction(context.Background())
		if err := cb(txn); err != nil {
			return err
		}

		return txn.commit()
	}, retry.RetryIf(func(err error) bool {
		return errors.Is(err, storepkg.ErrConflict)
	}), retry.Attempts(3), retry.LastErrorOnly(true))
}

func (store *Store) newTransaction(ctx context.Context) *Transaction {
	return &Transaction{
		ctx:                 ctx,
		store:               store,
		readRevisions:       map[string]int64{},
		prefixReadRevisions: map[string]int64{},
		puts:                map[string]string{},
		deletes:             map[string]struct{}{},
	}
}

func (store *Store) key(parts ...string) string {
	keyParts := []string{store.prefix}
	keyParts = append(keyParts, parts...)

	return path.Join(keyParts...)
}

func (store *Store) keyPrefix(logicalPrefix string) string {
	return store.key(logicalPrefix)
}

func (txn *Transaction) commit() error {
	if len(txn.puts) == 0 && len(txn.deletes) == 0 {
		return nil
	}

	comparisons := make([]clientv3.Cmp, 0, len(txn.readRevisions)+len(txn.prefixReadRevisions))
	for key, revision := range txn.readRevisions {
		if revision == 0 {
			comparisons = append(comparisons, clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
		} else {
			comparisons = append(comparisons, clientv3.Compare(clientv3.ModRevision(key), "=", revision))
		}
	}
	for prefix, revision := range txn.prefixReadRevisions {
		comparisons = append(comparisons, clientv3.Compare(clientv3.ModRevision(prefix).WithPrefix(), "<", revision+1))
	}

	operations := make([]clientv3.Op, 0, len(txn.puts)+len(txn.deletes))
	for key, value := range txn.puts {
		if _, deleted := txn.deletes[key]; deleted {
			continue
		}

		operations = append(operations, clientv3.OpPut(key, value))
	}
	for key := range txn.deletes {
		operations = append(operations, clientv3.OpDelete(key))
	}

	response, err := txn.store.client.Txn(txn.ctx).If(comparisons...).Then(operations...).Commit()
	if err != nil {
		return mapErr(err)
	}
	if !response.Succeeded {
		return storepkg.ErrConflict
	}

	return nil
}

func normalizePrefix(prefix string) string {
	prefix = strings.TrimSpace(prefix)
	if prefix == "" || prefix == "/" {
		return "/"
	}

	return "/" + strings.Trim(prefix, "/")
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}

	return fmt.Errorf("%w: %v", storepkg.ErrStoreFailed, err)
}
