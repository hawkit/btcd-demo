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
