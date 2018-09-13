package ffldb

import (
	"sync"
	"time"

	"github.com/hawkit/btcd-demo/database/internal/treap"

	"github.com/hawkit/goleveldb/leveldb"
)

// dbCacheSnapshot defines a snapshot of the database cache and underlying
// database at a particular point in time.
type dbCacheSnapshot struct {
	dbSnapshot    *leveldb.Snapshot
	pendingKeys   *treap.Immutable
	pendingRemove *treap.Immutable
}

func (snap *dbCacheSnapshot) Has(key []byte) bool {
	// Check the cached entries first.
	if snap.pendingRemove.Has(key) {
		return false
	}
	if snap.pendingKeys.Has(key) {
		return true
	}
	// Consult the database
	hasKey, _ := snap.dbSnapshot.Has(key, nil)
	return hasKey
}

func (snap *dbCacheSnapshot) Get(key []byte) []byte {
	// Check the cached entries first.
	if snap.pendingRemove.Has(key) {
		return nil
	}

	if value := snap.pendingKeys.Get(key); value != nil {
		return value
	}

	// Consult the database.
	value, err := snap.dbSnapshot.Get(key, nil)
	if err != nil {
		return nil
	}
	return value
}

// dbCache provides a database cache layer backed by an underlying database. It
// allows a maximum cache size and flush interval to be specified such that the
// cache is flushed to the database when the cache size exceeds the maximum
// configured value or it has been longer than the configured interval since  the
// last flush. This effectively provides transaction batching so that callers
// can commit transactions at will without incurring large performance hits due to
// frequent disk syncs.
type dbCache struct {
	// ldb is the underlying leveldb DB for metadata.
	ldb *leveldb.DB

	// store is used to sync blocks to flat files
	store *blockStore

	// The following fields are related to flushing the cache to persistent
	// storage. Note that all flushing is performed in an opportunistic
	// fashion. This means that it is only flushed during a transaction or
	// when the database cache is closed.
	//
	// maxSize is the maximum size threshold the cache can grow to before
	// it is flushed.
	//
	// flushInterval is the threshold interval of time that is allowed to
	// pass before the cache is flushed.
	//
	// lastFlush is the time the cache was last flushed. It is used in
	// conjunction with the current time and the flush interval.
	//
	// NOTE: These flush related fields are protected by the database write
	// lock.
	maxSize       uint64
	flushInterval time.Duration
	lastFlush     time.Time

	// The following fields hold the keys that need to be stored or deleted
	// from the underlying database once the cache is full, enough time has
	// passed, or when the database is shutting down. Note that these are
	// stored using immutable treap to support O(1) MVCC snapshots against
	// the cached data. The cacheLock is used to protect concurrent access
	// for cache updates and snapshots.
	cacheLock    sync.RWMutex
	cachedKeys   *treap.Immutable
	cachedRemove *treap.Immutable
}

func (c *dbCache) Snapshot() (*dbCacheSnapshot, error) {
	dbSnapshot, err := c.ldb.GetSnapshot()
	if err != nil {
		str := "failed to open transaction"
		return nil, convertErr(str, err)
	}

	c.cacheLock.RLock()
	cacheSnapshot := &dbCacheSnapshot{
		dbSnapshot:    dbSnapshot,
		pendingKeys:   c.cachedKeys,
		pendingRemove: c.cachedRemove,
	}
	c.cacheLock.Unlock()
	return cacheSnapshot, nil
}
