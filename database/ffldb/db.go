package ffldb

import (
	"btcd-demo/database"
	"btcd-demo/wire"
	"sync"

	"btcd-demo/chaincfg/chainhash"

	"btcd-demo/database/internal/treap"

	"github.com/hawkit/btcutil-demo"
	"github.com/hawkit/goleveldb/leveldb"
	ldberrors "github.com/hawkit/goleveldb/leveldb/errors"
)

const (
	// ErrDbNotOpenStr is the text to use for the database.ErrDbNotOpen
	// error code
	ErrDbNotOpenStr = "database is not open"

	// ErrTxClosedStr is the text to use for the database.ErrTxClosed
	// error code
	ErrTxClosedStr = "database tx is closed"
)

var (
	// metadataBucketID is the ID of the top-level metadata bucket.
	// It is the value 0 encoded as an unsigned big-endian uint32
	metadataBucketID = [4]byte{}

	// blockIdxBucketID is the ID of the internal block metadata bucket.
	// It is the value 1 encoded as an unsigned big-endian uint32
	blockIdxBucketID = [4]byte{0x00, 0x00, 0x00, 0x01}
)

// makeDbErr creates a database.Error given a set of arguments.
func makeDbErr(c database.ErrorCode, desc string, err error) database.Error {
	return database.Error{ErrorCode: c, Description: desc, Err: err}
}

// convertErr converts the passed leveldb error into a database error with an
// equivalent error code and the passed description. It also sets the passed
// error as the underlying error.
func convertErr(desc string, ldbErr error) database.Error {
	// Use the driver-specific error code by default. The code below will
	// update this with the converted error if it's recognized.
	var code = database.ErrDriverSpecific
	switch {
	case ldberrors.IsCorrupted(ldbErr):
		code = database.ErrCorruption
	case ldbErr == leveldb.ErrClosed:
		code = database.ErrDbNotOpen
	case ldbErr == leveldb.ErrSnapshotReleased:
		code = database.ErrTxClosed
	case ldbErr == leveldb.ErrIterReleased:
		code = database.ErrTxClosed
	}
	return database.Error{ErrorCode: code, Description: desc, Err: ldbErr}
}

// bucket is an internal type used to represent a collection of key/value pairs
// and implements the database.Bucket interface.
type bucket struct {
	tx *transaction
	id [4]byte
}

func (b *bucket) Bucket(key []byte) database.Bucket {
	panic("implement me")
}

func (b *bucket) CreateBucket(key []byte) (database.Bucket, error) {
	panic("implement me")
}

func (b *bucket) CreateBucketIfNotExists(key []byte) (database.Bucket, error) {
	panic("implement me")
}

func (b *bucket) DeleteBucket(key []byte) error {
	panic("implement me")
}

func (b *bucket) ForEach(func(k, v []byte) error) error {
	panic("implement me")
}

func (b *bucket) ForEachBucket(func(k []byte) error) error {
	panic("implement me")
}

func (b *bucket) Cursor() database.Cursor {
	panic("implement me")
}

func (b *bucket) Writable() bool {
	panic("implement me")
}

func (b *bucket) Put(key, value []byte) error {
	panic("implement me")
}

func (b *bucket) Get(key []byte) []byte {
	panic("implement me")
}

func (b *bucket) Delete(key []byte) error {
	panic("implement me")
}

// Enforce bucket implements the database.Bucket interface
var _ database.Bucket = (*bucket)(nil)

// pendingBlocks houses a block that will be written to disk when the database
// transaction is committed.
type pendingBlock struct {
	hash  *chainhash.Hash
	bytes []byte
}

// transaction represents a database transaction. It can either be read-only or
// read-write and implements the database.Bucket interface. The transaction
// provides a root bucket against which all read and writes occur.
type transaction struct {
	managed        bool             // Is the transaction managed?
	closed         bool             // Is the transaction closed?
	writable       bool             // Is the transaction writable?
	db             *db              // DB instance the tx was created from.
	snapshot       *dbCacheSnapshot // Underlying snapshot for txns.
	metaBucket     *bucket          // The root metadata bucket.
	blockIdxBucket *bucket          // The block index bucket.

	// Blocks that need to be stored on commit. The pendingBlocks map is
	// kept to allow quick lookups of pending data by block hash.
	pendingBlocks    map[chainhash.Hash]int
	pendingBlockData []pendingBlock

	// Keys that need to be stored or deleted on commit.
	pendingKeys   *treap.Mutable
	pendingRemove *treap.Mutable

	// Active iterators that need to be notified when the pending keys have
	// been updated so the cursors can properly handle updates to the
	// transaction state.
	activeIterLock sync.RWMutex
	activeIters    []*treap.Iterator
}

// Enforce transaction implements the database.Tx interface
var _ database.Tx = (*transaction)(nil)

func (tx *transaction) Metadata() database.Bucket {
	return tx.metaBucket
}

// StoreBlock stores the provided block into the database. There are no checks
// to ensure the block connects to a previous block, contains double spends, or
// any additional functionality such as transaction indexing. It simply stores
// the block in the database.
//
// Returns the following errors as required by the interface contract:
// - ErrBlockExists when the block hash already exists
// - ErrNotWritable if attempted against a read-only transaction
// - ErrTxClosed if the transaction has already been closed
func (tx *transaction) StoreBlock(block *btcutil.Block) error {

	//todo
	return nil
}

func (tx *transaction) HasBlock(hash *chainhash.Hash) (bool, error) {
	panic("implement me")
}

func (tx *transaction) HasBlocks(hashes []*chainhash.Hash) ([]bool, error) {
	panic("implement me")
}

func (tx *transaction) FetchBlockHeader(hash *chainhash.Hash) ([]byte, error) {
	panic("implement me")
}

func (tx *transaction) FetchBlockHeaders(hashes []*chainhash.Hash) ([][]byte, error) {
	panic("implement me")
}

func (tx *transaction) FetchBlock(hash *chainhash.Hash) ([]byte, error) {
	panic("implement me")
}

func (tx *transaction) FetchBlocks(hashes []*chainhash.Hash) ([][]byte, error) {
	panic("implement me")
}

func (tx *transaction) FetchBlockRegion(region *database.BlockRegion) ([]byte, error) {
	panic("implement me")
}

func (tx *transaction) FetchBlockRegions(regions []*database.BlockRegion) ([][]byte, error) {
	panic("implement me")
}

func (tx *transaction) Commit() error {
	panic("implement me")
}

func (tx *transaction) Rollback() error {
	panic("implement me")
}

// db represents a collection of namespaces which are persisted and implements
// the database.DB interface. All database access is performed through
// transactions which are obtained through the specific Namespace.
type db struct {
	writeLock sync.Mutex   // Limit to one write transaction at a time.
	closeLock sync.RWMutex // Make database close block while txns active.
	closed    bool         // Is the database closed?
	store     *blockStore  // Handles read/writing blocks to flat files.
	cache     *dbCache     // Cache layer which wraps underlying leveldb DB.
}

// Enfore db implements the database.DB interface
var _ database.DB = (*db)(nil)

// Type returns the database driver type the current database instance was
// created with.
//
// This function is part of the database.DB interface implementation.
func (db *db) Type() string {
	return dbType
}

// begin is the implementation function for the Begin database method.  See its
// documentation for more details.
//
// This function is only separate because it returns the internal transaction
// which is used by the managed transaction code while the database method
// returns the interface.
func (db *db) begin(writable bool) (*transaction, error) {
	// Whenever a new writable transaction is started, grab the write lock
	// to ensure only a single write transaction can be active at the same
	// time. This lock will not be released until the transaction is closed
	// (via Rollback or Commit)
	if writable {
		db.writeLock.Lock()
	}

	// Whenever a new transaction is started, grab a read lock against the
	// database to ensure Close will wait for the transaction to finish.
	// This lock will not be released until the transaction is closed( via
	// Rollback or Commit).
	db.closeLock.RLock()

	if db.closed {
		db.closeLock.RUnlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, makeDbErr(database.ErrDbNotOpen, ErrDbNotOpenStr, nil)
	}

	// Grab a snapshot of the database cache (which in turn also handles the
	// underlying database)
	snapshot, err := db.cache.Snapshot()
	if err != nil {
		db.closeLock.Unlock()
		if writable {
			db.writeLock.Unlock()
		}
		return nil, err
	}

	// The metadata and block index buckets are internal-only buckets, so
	// they have defined IDs
	tx := &transaction{
		writable:      writable,
		db:            db,
		snapshot:      snapshot,
		pendingKeys:   treap.NewMutable(),
		pendingRemove: treap.NewMutable(),
	}

	tx.metaBucket = &bucket{tx: tx, id: metadataBucketID}
	tx.blockIdxBucket = &bucket{tx: tx, id: blockIdxBucketID}

	return tx, nil
}

func (db *db) Begin(writable bool) (database.Tx, error) {
	return db.begin(writable)
}

func (db *db) Close() error {
	panic("implement me")
}

func (db *db) Update(fn func(tx database.Tx) error) error {
	panic("implement me")
}

func (db *db) View(fn func(tx database.Tx) error) error {
	panic("implement me")
}

// openDB opens the database at the provided path.  database.ErrDbDoesNotExist
// is returned if the database doesn't exist and the create flag is not set.
func openDB(dbPath string, network wire.BitcoinNet, create bool) (database.DB, error) {
	return nil, nil // todo opendb
}
