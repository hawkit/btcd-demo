package database

import "btcd-demo/chaincfg/chainhash"

// Cursor represents a cursor over key/value pairs and nested buckets of a
// bucket.
//
// Note that open cursors are not tracked on bucket changes and any
// modifications to the bucket, with the exception of Cursor.Delete,
// invalidates the cursor. After invalidation, the cursor must be
// repositioned, or the keys and values returned may be unpredictable.
type Cursor interface {

	// Bucket returns the bucket the cursor was created for.
	Bucket() Bucket

	// Delete removes the current key/value pair the cursor is at without
	// invalidating the cursor.
	Delete() error

	// First positions the cursor at the first key/value pair and returns
	// whether or not the pair exists.
	First() bool

	// Last positions the cursor at the last key/value pair and returns
	// whether or not the pair exists.
	Last() bool

	// Next moves the cursor one key/value pair forward and returns whether
	// or not the pair exists.
	Next() bool

	// Prev moves the cursor one key/value pair backward and return whether
	// or not the pair exists.
	Prev() bool

	// Seek positions the cursor at the first key/value pair that is greater
	// than or equal to the passed seek key. Returns whether or not the pair
	// exists.
	Seek(seek []byte) bool

	// Key returns the current key the cursor is pointing to.
	Key() []byte

	// Value returns the current value the cursor is pointing to. This will
	// be nil for nested buckets.
	Value() []byte
}

// Bucket represents a collection of key/value pairs.
type Bucket interface {
	// Bucket retrieves a nested bucket with the given key. Returns nil if
	// the bucket does not exist.
	Bucket(key []byte) Bucket

	// CreateBucket creates and returns a new nested bucket with the given
	// key.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	// - ErrBucketExists if the bucket already exists
	// - ErrBucketNameRequired if the key is empty
	// - ErrIncompatibleValue if the key is otherwise invalid for the
	//   particular implementation
	// - ErrNotWritable if attempted against a read-only transaction
	// - ErrTxClosed if the transaction has already been closed
	CreateBucket(key []byte) (Bucket, error)

	// CreateBucketIfNotExists creates and returns a new nested bucket with
	// the given key if it does not already exist.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	// - ErrBucketExists if the bucket already exists
	// - ErrBucketNameRequired if the key is empty
	// - ErrIncompatibleValue if the key is otherwise invalid for the
	//   particular implementation
	// - ErrNotWritable if attempted against a read-only transaction
	// - ErrTxClosed if the transaction has already been closed
	CreateBucketIfNotExists(key []byte) (Bucket, error)

	// DeleteBucket removes a nested bucket with the given key. This also
	// includes removing all nested buckets and keys under the bucket being
	// deleted.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrBucketNotFound if the specified bucket does not exist
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	DeleteBucket(key []byte) error

	// ForEach invokes the passed function with every key/value pair in the
	// bucket. This dose not include nested buckets or the key/value pairs
	// with those nested buckets.
	//
	// WARNING: It is not safe to mutate data while iterating with this method.
	// Doing so may cause the underlying cursor to be invalidated and return
	// unexpected keys and/or values
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//  - ErrTxClose if the transaction has already been closed
	//
	// NOTE: The slices returned by this function are only valid during a
	// transaction. Attempting to access them after a transaction has ended
	// results in undefined behavior. Additionally, the slices must NOT
	// modified by the caller. These constraints prevent additional data
	// copies and allows support for memory-mapped database implementations.
	ForEach(func(k, v []byte) error) error

	// ForEachBucket invokes the passed function with the key of every
	// nested bucket in the current bucket.  This does not include any
	// nested buckets within those nested buckets.
	//
	// WARNING: It is not safe to mutate data while iterating with this
	// method.  Doing so may cause the underlying cursor to be invalidated
	// and return unexpected keys and/or values.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrTxClosed if the transaction has already been closed
	//
	// NOTE: The keys returned by this function are only valid during a
	// transaction.  Attempting to access them after a transaction has ended
	// results in undefined behavior.  This constraint prevents additional
	// data copies and allows support for memory-mapped database
	// implementations.
	ForEachBucket(func(k []byte) error) error

	// Cursor returns a new cursor, allowing for iteration over the bucket's
	// key/value pairs and nested buckets in forward or backward order.
	//
	// You must seed to a position using the First, Last, or Seek functions
	// before calling the Next, Prev, Key, or Value functions. Failure to do
	// so will result in the same return values as an exhausted cursor, which
	// is false for the Prev and Next functions and nil for Key and Value
	// functions.
	Cursor() Cursor

	// Writable returns whether or not the bucket is writable
	Writable() bool

	// Put saves the specified key/value pair to the bucket. Keys that do
	// not already exist are added and keys that already exist are
	// overwritten.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrKeyRequired if the key is empty
	//   - ErrIncompatibleValue if the key is the same as an existing bucket
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	Put(key, value []byte) error

	// Get returns the value for the given key. Returns nil if the key dose
	// not exist in this bucket. An empty slice is returned for keys that
	// exist but have no value assigned.
	//
	// NOTE: The value returned by this function is only valid during a
	// transaction.  Attempting to access it after a transaction has ended
	// results in undefined behavior.  Additionally, the value must NOT
	// be modified by the caller.  These constraints prevent additional data
	// copies and allows support for memory-mapped database implementations.
	Get(key []byte) []byte

	// Delete removes the specified key from the bucket.  Deleting a key
	// that does not exist does not return an error.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	//   - ErrKeyRequired if the key is empty
	//   - ErrIncompatibleValue if the key is the same as an existing bucket
	//   - ErrTxNotWritable if attempted against a read-only transaction
	//   - ErrTxClosed if the transaction has already been closed
	Delete(key []byte) error
}

// BlockRegion specifies a particular region of a block identified by the
// specified hash, given an offset and length.
type BlockRegion struct {
	Hash   *chainhash.Hash
	Offset uint32
	Len    uint32
}

// Tx represents a database transaction. It can either by read-only or
// read-write. The transaction provides a metadata bucket against which all
// read and writes occur.
//
// As would be expected with a transaction, no changes will be saved to the
// database until it has been committed. The transaction will only provide a
// view of the database at the time it was created. Transactions should not be
// long running operations.
type Tx interface {
	// Metadata returns the top-most bucket for all metadata storage.
	Metadata() Bucket

	// StoreBlock stores the provided block into the database. There are no
	// checks to ensure the block connects to a previous block, contains
	// double spends, or any additional functionality such as transaction
	// indexing. It simply stores the block in the database.
	//
	// The interface contract guarantees at least the following errors will
	// be returned (other implementation-specific errors are possible):
	// - ErrBlockExists when the block hash already exists
	// - ErrTxNotWritable if attempted against a read-only transaction
	// - ErrExClosed if the transaction has already been closed
	//StoreBlock(block *btcu)
}

type DB interface {
	Type() string

	Close() error
}
