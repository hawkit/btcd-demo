package treap

// Immutable represents a treap data struct which is used to hold ordered
// key/value pairs using a combination of binary search tree and heap semantics.
// It is a self-organizing and randomized data struct that doesn't require
// complex operations to maintain balance. Search, insert, and delete operations
// all O(log n). In addition, it provides O(1) snapshots for multi-version
// concurrency control (MVCC)
//
// All operations which result in modifying the treap return a new version of
// the treap with only the modified nodes updated. All unmodified nodes are
// shared with previous version. This is extremely useful in concurrent
// applications since the caller only has to atomically replace the treap
// pointer with the newly returned version after performing any mutations. All
// readers can simply use their existing pointer as a snapshot since the treap
// it points to is immutable. This effectively provides O(1) snapshot
// capability with efficient memory usage characteristics since the old nodes
// only remain allocated until there are no longer any references to them.
type Immutable struct {
	root  *treapNode
	count int

	// totalSize is the best estimate of the total size of all data in
	// the treap including the keys, values, and node sizes.
	totalSize uint64
}
