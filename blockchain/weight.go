package blockchain

const (

	// MaxBlockWeight defines the maximum block weight, where "block
	// weigght" is interpreted as defined in BIP0141. A block's weight
	// is calculated as the sum of bytes in the existing transactions
	// and header, plus the weight of each byte within a transaction. The
	// weight of a "base" byte is 4, while the weight of a witness byte is
	// 1. As a result, for a block to be valid, the BlockWeight MUST be
	// less than , or equal to MaxBlockWeight
	MaxBlockWeight = 4000000

	// MaxBlockBaseSize is the maximum number of bytes with a block
	// which can be allocated to not-witness data.
	MaxBlockBaseSize = 1000000

	// WitnessScaleFactor determines the level of "discount" witness data
	// receives compared to "base" data. A scale factor of 4, denotes that
	// witness data is 1/4 as cheap as regular non-witness data.
	WitnessScaleFactor = 4
)
