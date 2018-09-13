package treap

// Iterator represents an iterator for forwards and backwards iteration over
// the contents of a treap (mutable or immutable)
type Iterator struct {
	t        *Mutable    // Mutable treap iterator is associated with or nil
	root     *treapNode  // Root node of treap iterator is associated with
	node     *treapNode  // The node the iterator is positioned at
	parents  parentStack // The stack of parents needed to iterate
	isNew    bool        // Whether the iterator has been positioned
	seekKey  []byte      // Used to handle dynamic updates for mutable treap
	startKey []byte      // Used to limit the iterator to a range
	limitKey []byte      // Used to limit the iterator to a range
}

// ForceReset notifies the iterator that the underlying mutable treap has been
// updated, so the next call to Prev or Next needs to reseek in order to allow
// the iterator to continue working properly.
//
// NOTE: Calling this function when the iterator is associated with an immutable
// treap has no effect as you would expect.
func (iter *Iterator) ForceReset() {
	// Nothing to do when the iterator is associated with an immutable treap.
	if iter.t == nil {
		return
	}

	// Update the iterator root to the mutable treap root in case it changed.
	iter.root = iter.t.root

	// Set the seek key to the current node. This will force the Next/Prev
	// functions to reseek, and thus properly reconstruct the iterator, on
	// their next call.
	if iter.node == nil {
		iter.seekKey = nil
		return
	}
	iter.seekKey = iter.node.key
}
