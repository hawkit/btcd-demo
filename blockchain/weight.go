package blockchain

const (
	// MaxBlockBaseSize is the maximum number of bytes with a block
	// which can be allocated to not-witness data.
	MaxBlockBaseSize = 1000000

	// WitnessScaleFactor determines the level of "discount" witness data
	// receives compared to "base" data. A scale factor of 4, denotes that
	// witness data is 1/4 as cheap as regular non-witness data.
	WitnessScaleFactor = 4
)