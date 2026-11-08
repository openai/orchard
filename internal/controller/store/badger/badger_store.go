package badger

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/cirruslabs/orchard/internal/controller/store"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"go.uber.org/zap"
)

type Store struct {
	db       *badger.DB
	gcStop   chan struct{}
	gcDone   sync.WaitGroup
	closeErr error
	close    sync.Once
	store.Store
}

type Transaction struct {
	badgerTxn *badger.Txn
	store.Transaction
}

func NewBadgerStore(dbPath string, noCompression bool, logger *zap.SugaredLogger) (store.Store, error) {
	opts := badger.DefaultOptions(dbPath).
		WithLogger(newBadgerLogger(logger)).
		WithLoggingLevel(badger.INFO)

	opts.SyncWrites = true

	if noCompression {
		opts.Compression = options.None
		opts.BlockCacheSize = 0
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:     db,
		gcStop: make(chan struct{}),
	}

	// Perform garbage collection periodically, as recommended in the documentation[1]
	//
	// [1]: https://docs.hypermode.com/badger/quickstart#garbage-collection
	store.gcDone.Add(1)
	go func() {
		defer store.gcDone.Done()

		for {
			if err := store.performGarbageCollection(); err != nil {
				logger.Errorf("garbage collection failed: %v", err)
			}

			select {
			case <-store.gcStop:
				return
			case <-time.After(1 * time.Hour):
			}
		}
	}()

	return store, nil
}

// Close stops the periodic garbage collection and closes the database.
func (store *Store) Close() error {
	store.close.Do(func() {
		close(store.gcStop)
		store.gcDone.Wait()
		store.closeErr = store.db.Close()
	})

	return store.closeErr
}

func (store *Store) performGarbageCollection() error {
	// RunValueLogGC() needs to be invoked multiple times
	for {
		if err := store.db.RunValueLogGC(0.5); err != nil {
			// Nothing to rewrite, stop
			if errors.Is(err, badger.ErrNoRewrite) {
				return nil
			}

			return err
		}
	}
}

func (store *Store) View(cb func(txn store.Transaction) error) error {
	return retry.Do(func() error {
		return store.db.View(func(txn *badger.Txn) error {
			return cb(&Transaction{
				badgerTxn: txn,
			})
		})
	}, retry.RetryIf(func(err error) bool {
		return errors.Is(err, badger.ErrConflict)
	}), retry.Attempts(3), retry.LastErrorOnly(true))
}

func (store *Store) Update(cb func(txn store.Transaction) error) error {
	return retry.Do(func() error {
		return store.db.Update(func(txn *badger.Txn) error {
			return cb(&Transaction{
				badgerTxn: txn,
			})
		})
	}, retry.RetryIf(func(err error) bool {
		return errors.Is(err, badger.ErrConflict)
	}), retry.Attempts(3), retry.LastErrorOnly(true))
}

func mapErr(err error) error {
	if err != nil {
		if errors.Is(err, badger.ErrKeyNotFound) {
			return store.ErrNotFound
		}

		return fmt.Errorf("%w: %v", store.ErrStoreFailed, err)
	}

	return err
}
