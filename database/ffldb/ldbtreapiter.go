package ffldb

import (
	"github.com/hawkit/btcd-demo/database/internal/treap"
	"github.com/hawkit/goleveldb/leveldb/iterator"
	"github.com/hawkit/goleveldb/leveldb/util"
)

// ldbTreapIter wraps a treap iterator to provide the additional functionality
// needed to satisfy the leveldb iterator.Iterator interface.
type ldbTreapIter struct {
	*treap.Iterator
	tx       *transaction
	released bool
}

// Enforce ldbTreapIter implements the leveldb iterator.Iterator interface.
var _ iterator.Iterator = (*ldbTreapIter)(nil)

// Error is only provided to satisfy the iterator interface as there are no
// errors for this memory-only structure.
//
// This is part of the leveldb iterator.Iterator interface implementation.
func (iter *ldbTreapIter) Error() error {
	return nil
}

// SetReleaser is only provided to satisfy the iterator interface as there is no
// need to override it.
//
// This is part of the leveldb iterator.Iterator interface implementation.
func (iter *ldbTreapIter) SetReleaser(releaser util.Releaser) {
}

// Release releases the iterator by removing the underlying treap iterator from
// the list of active iterators against the pending keys treap.
//
// This is part of the leveldb iterator.Iterator interface implementation.
func (iter *ldbTreapIter) Release() {
	if !iter.released {
		iter.tx.removeActiveIter(iter.Iterator)
		iter.released = true
	}
}

func newLdbTreapIter(tx *transaction, slice *util.Range) *ldbTreapIter {
	iter := tx.pendingKeys.Iterator(slice.Start, slice.Limit)
	tx.addActiveIter(iter)
	return &ldbTreapIter{Iterator: iter, tx: tx}
}
