package mempool

const (
	// DefaultBlockPrioritySize is the dfault size in bytes for high -
	// priority / low-fee transactions. It is used to help determine which
	// are allowed into the mempool and consequently affects their relay and
	// inclusion when generating block templates.
	DefaultBlockPrioritySize = 50000
)