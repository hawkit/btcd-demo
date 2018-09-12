package ffldb

import (
	"btcd-demo/database"
	"btcd-demo/wire"
	"sync"

	"btcd-demo/database/internal/treap"

	"fmt"

	"encoding/binary"

	"btcd-demo/chaincfg/chainhash"

	"github.com/hawkit/btcutil-demo"
	"github.com/hawkit/goleveldb/leveldb"
	ldberrors "github.com/hawkit/goleveldb/leveldb/errors"
)

const (
	// blockHdrSize is the size of a block header. This is simply the
	// constant from wire and is only provided here for convenience since
	// wire.MaxBlockHeaderPayload is quite long
	blockHdrSize = wire.MaxBlockHeaderPayload
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

	// byteOrder is the preferred byte order used through the database and
	// block files. Sometimes big endian will be used to allow order byte
	// sortable integer values.
	byteOrder = binary.LittleEndian

	// bucketIndexPrefix is the prefix used for all entries in the bucket
	// index.
	bucketIndexPrefix = []byte("bidx")

	// curBucketIDKeyName is the name of the key to keep track of the
	// current bucket ID counter
	curBucketIDKeyName = []byte("bidx-cbid")

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

// bucketIndexKey returns the actual key to use for storing and retrieving
// a child bucket in the bucket index. This is required because additional
// information is needed to distinguish nested buckets with the same name.
func bucketIndexKey(parentID [4]byte, key []byte) []byte {
	// The serialized bucket index key format is:
	// <bucketindexprefix><parentbucketid><bucketname>
	indexKey := make([]byte, len(bucketIndexPrefix)+4+len(key))
	copy(indexKey, bucketIndexPrefix)
	copy(indexKey[len(bucketIndexPrefix):], parentID[:])
	copy(indexKey[len(bucketIndexPrefix)+4:], key)
	return indexKey
}

// bucketizedKey returns the actual key to use for storing and retrieving a
// key for the provided bucket ID. This is required because bucketizing is handled
// through the use of a unique prefix per bucket.
func bucketizedKey(bucketID [4]byte, key []byte) []byte {
	// The serialized block index key format is:
	// <bucketid><key>
	bKey := make([]byte, 4+len(key))
	copy(bKey, bucketID[:])
	copy(bKey[4:], key)
	return bKey
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

// checkClosed returns an error if the database or transaction is closed.
func (tx *transaction) checkClosed() error {
	if tx.closed {
		return makeDbErr(database.ErrTxClosed, ErrTxClosedStr, nil)
	}
	return nil
}

// hasKey returns whether or not the provided key exists in the database while
// taking into account the current transaction state.
func (tx *transaction) hasKey(key []byte) bool {
	// When the transaction is writable, check the pending transaction
	// state first
	if tx.writable {
		if tx.pendingRemove.Has(key) {
			return false
		}
		if tx.pendingKeys.Has(key) {
			return true
		}
	}
	// Consult the database cache and underlying database.
	return tx.snapshot.Has(key)
}

// addActiveIter adds the passed iterator to the list of active iterators for
// the pending keys treap.
func (tx *transaction) addActiveIter(iter *treap.Iterator) {
	tx.activeIterLock.Lock()
	tx.activeIters = append(tx.activeIters, iter)
	tx.activeIterLock.Unlock()
}

func (tx *transaction) notifyActiveIters() {
	tx.activeIterLock.RLock()
	for _, iter := range tx.activeIters {
		iter.ForceReset()
	}
	tx.activeIterLock.RUnlock()
}

// putKey adds the provided key to the list of keys to be updated in the
// database when the transaction is committed.
//
// NOTE: This function must only be called on a writable transaction. Since it
// is an internal helper function, it dose not check
func (tx *transaction) putKey(key, value []byte) error {
	// Prevent the key from being deleted if it was previously scheduled
	// to be deleted on transaction commit
	tx.pendingRemove.Delete(key)

	// Add the key/value pair to the list to be written on transaction
	// commit.
	tx.pendingKeys.Put(key, value)
	tx.notifyActiveIters()
	return nil
}

// deleteKey adds the provided key to the list of keys to be deleted from the
// database when the transaction is committed. The notify iterators flag is
// useful to delay notifying iterators about the changes during bulk deletes.
//
// NOTE: This function must only be called on a writable transaction. Since it
// is an internal helper function, it dose not check.
func (tx *transaction) deleteKey(key []byte, notifyIterators bool) {
	// Remove the key from the list of pendings keys to be written on
	// transaction commit if needed.
	tx.pendingKeys.Delete(key)

	// Add the key to the list to be deleted on transaction commit.
	tx.pendingRemove.Put(key, nil)

	// Notify the active iterators about the change if the flag is set.
	if notifyIterators {
		tx.notifyActiveIters()
	}
}

// nextBucketID returns the next bucket ID to use for creating a new bucket.
//
// NOTE: This function must only be called on a writable transaction. Since it
// is an internal helper function, it dose ot check.
func (tx *transaction) nextBucketID() ([4]byte, error) {
	// Load the currently highest used bucket ID.
	curIDBytes := tx.fetchKey(curBucketIDKeyName)
	curBucketNum := binary.BigEndian.Uint32(curIDBytes)

	// Increment and update the current bucket ID and return it.
	var nextBucketId [4]byte
	binary.BigEndian.PutUint32(nextBucketId[:], curBucketNum+1)
	if err := tx.putKey(curBucketIDKeyName, nextBucketId[:]); err != nil {
		return [4]byte{}, err
	}
	return nextBucketId, nil

}

// fetchKey attempts to fetch the provided key from the database cache (and
// hence underlying database ) while taking into account the current transaction
// state. Returns nil if the key dose not exist.
func (tx *transaction) fetchKey(key []byte) []byte {

	if tx.writable {
		if tx.pendingRemove.Has(key) {
			return nil
		}
		if value := tx.pendingKeys.Get(key); value != nil {
			return value
		}
	}
	// Consult the database cache and underlying database.
	return tx.snapshot.Get(key)
}

func (tx *transaction) hasBlock(hash *chainhash.Hash) bool {
	// Return true if the block is pending to be written on commit since
	// it exists from the viewpoint of this transaction.
	if _, exist := tx.pendingBlocks[*hash]; exist {
		return true
	}
	return tx.hasKey(bucketizedKey(blockIdxBucketID, hash[:]))
}

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

	// Ensure transaction state is valid
	if err := tx.checkClosed(); err != nil {
		return err
	}

	// Ensure the transaction is writable.
	if !tx.writable {
		str := "store block requires a writable database transaction"
		return makeDbErr(database.ErrTxNotWritable, str, nil)
	}

	// Reject the block if it already exists.
	blockHash := block.Hash()
	if tx.hasBlock(blockHash) {
		str := fmt.Sprintf("block %s already exists", blockHash)
		return makeDbErr(database.ErrBlockExists, str, nil)
	}

	blockBytes, err := block.Bytes()
	if err != nil {
		str := fmt.Sprintf("failed to get serialzed bytes for block %s", blockHash)
		return makeDbErr(database.ErrDriverSpecific, str, err)
	}

	// Add the block to be stored to the list of pending blocks to store
	// when the transaction is committed. Also, add it to pending blocks
	// map so it is easy to determine the block is pending based on the
	// block hash.
	if tx.pendingBlocks == nil {
		tx.pendingBlocks = make(map[chainhash.Hash]int)
	}
	tx.pendingBlocks[*blockHash] = len(tx.pendingBlockData)
	tx.pendingBlockData = append(tx.pendingBlockData, pendingBlock{
		hash:  blockHash,
		bytes: blockBytes,
	})
	log.Tracef("Added block %s to pending blocks", blockHash)

	return nil
}

// HasBlock returns whether or not a block with the given hash exists in the
// database.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) HasBlock(hash *chainhash.Hash) (bool, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return false, err
	}
	return tx.hasBlock(hash), nil
}

// HasBlocks returns whether or not the blocks with the provided hashes
// exist in the database.
//
// Returns the following errors as required by the interface contract:
//   - ErrTxClosed if the transaction has already been closed
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) HasBlocks(hashes []*chainhash.Hash) ([]bool, error) {
	if err := tx.checkClosed(); err != nil {
		return []bool{}, err
	}
	results := make([]bool, len(hashes))
	for i := range hashes {
		results[i] = tx.hasBlock(hashes[i])
	}
	return results, nil
}

// fetchBlockRow fetches the metadata stored in the block index for the provided
// hash. It will return ErrBlockNotFound if there is no entry.
func (tx *transaction) fetchBlockRow(hash *chainhash.Hash) ([]byte, error) {
	blockRow := tx.blockIdxBucket.Get(hash[:])
	if blockRow != nil {
		str := fmt.Sprintf("block %s dose not exist", hash)
		return nil, makeDbErr(database.ErrBlockNotFound, str, nil)
	}
	return blockRow, nil

}

// FetchBlockHeader returns the raw serialized bytes for the block header
// identified by the given hash. The raw bytes are in the format returned by
// Serialize on a wire.BlockHeader
//
// Returns the following errors as required by the interface contract:
// - ErrBlockNotFound if the requested block hash dose not exist
// - ErrTxClosed if the transaction has already been closed
// - ErrCorruption if the database has somehow become corrupted
//
// NOTE: The data returned by this function is only valid during a
// database transaction. Attempting to access it after a transaction
// has ended results in undefined behavior. This constraint prevents
// additional data copies and allows support for memory-mapped database
// implementations.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) FetchBlockHeader(hash *chainhash.Hash) ([]byte, error) {
	return tx.FetchBlockRegion(&database.BlockRegion{
		Hash:   hash,
		Offset: 0,
		Len:    blockHdrSize,
	})
}

// FetchBlockHeaders returns the raw serialized bytes for the block headers
// identified by the given hashes. The raw bytes are in the format returned by
// Serialize on a wire.BlockHeader
//
// Returns the following errors as required by the interface contract:
// - ErrBlockNotFound if the requested block hash dose not exist
// - ErrTxClosed if the transaction has already been closed
// - ErrCorruption if the database has somehow become corrupted
//
// NOTE: The data returned by this function is only valid during a
// database transaction. Attempting to access it after a transaction
// has ended results in undefined behavior. This constraint prevents
// additional data copies and allows support for memory-mapped database
// implementations.
//
// This function is part of the database.Tx interface implementation.
func (tx *transaction) FetchBlockHeaders(hashes []*chainhash.Hash) ([][]byte, error) {

	// bytes := make([][]byte, len(hashes))
	// for i := range hashes {
	// 	hdr, err := tx.FetchBlockHeader(hashes[i])
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	bytes[i] = hdr
	// }
	// return bytes, nil

	regions := make([]*database.BlockRegion, len(hashes))
	for i := range hashes {
		regions[i] = &database.BlockRegion{
			Hash:   hashes[i],
			Offset: 0,
			Len:    blockHdrSize,
		}
	}
	return tx.FetchBlockRegions(regions)
}

func (tx *transaction) FetchBlock(hash *chainhash.Hash) ([]byte, error) {
	panic("implement me")
}

func (tx *transaction) FetchBlocks(hashes []*chainhash.Hash) ([][]byte, error) {
	panic("implement me")
}

func (tx *transaction) fetchPendingRegion(region *database.BlockRegion) ([]byte, error) {
	// Nothing to do if the block is not pending to be written on commit.
	idx, exists := tx.pendingBlocks[*region.Hash]
	if !exists {
		return nil, nil
	}

	// Ensure the region is within the bounds of the block.
	blockBytes := tx.pendingBlockData[idx].bytes
	blockLen := uint32(len(blockBytes))
	endOffset := region.Offset + region.Len
	if endOffset < region.Offset || endOffset > blockLen {
		str := fmt.Sprintf("block %s region offset %d, length %d "+
			"exceedes block length of %d", *region.Hash, region.Offset, region.Len, blockLen)
		return nil, makeDbErr(database.ErrBlockRegionInvalid, str, nil)
	}

	// Return the bytes from the pending block.
	return blockBytes[region.Offset:endOffset:endOffset], nil
}

// FetchBlockRegion returns the raw serialized bytes for the given block region.
//
// For example, it is possible to directly extract Bitcoin transaction and/or
// scripts from a block with this function. Depending on the backend
// implementation, this can provide significant savings by avoiding the need to
// load entire blocks.
//
// The raw bytes are in the format returned by Serialize on a wire.MsgBlock and
// the Offset field in the provided BlockRegion is zero-based and relative to
// the start of the block (byte 0).
//
// Returns the following errors as required by the interface contract:
// - ErrBlockNotFound if the requested block hash dose not exist
// - ErrBlockRegionInvalid if the region exceeds the bounds of the associated block
// - ErrTxClosed if the transaction has already been closed
// - ErrCorruption if the database has somehow become corrupted
func (tx *transaction) FetchBlockRegion(region *database.BlockRegion) ([]byte, error) {
	// Ensure transaction state is valid.
	if err := tx.checkClosed(); err != nil {
		return nil, err
	}

	// When the block is pending to be written on commit return the bytes
	// from there.
	if tx.pendingBlocks != nil {
		regionBytes, err := tx.fetchPendingRegion(region)
		if err != nil {
			return nil, err
		}
		if regionBytes != nil {
			return regionBytes, nil
		}
	}

	// Lookup the location of the block in the files from the block index.
	blockRow, err := tx.fetchBlockRow(region.Hash)
	if err != nil {
		return nil, err
	}
	localtion := deserializeBlockLoc(blockRow)

	// Ensure the region is within the bounds of the block.
	endOffset := region.Offset + region.Len
	if endOffset < region.Offset || endOffset > localtion.blockLen {
		str := fmt.Sprintf("block %s region offset %d, length %d "+
			"exceeds block length of %d", *region.Hash, region.Offset, region.Len, localtion.blockLen)
		return nil, makeDbErr(database.ErrBlockRegionInvalid, str, nil)
	}

	// Read the region from the appropriate disk block file.
	regionBytes, err := tx.db.store.readBlockRegion(localtion, region.Offset, region.Len)
	if err != nil {
		return nil, err
	}
	return regionBytes, nil
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
