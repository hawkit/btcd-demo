package chaincfg

import (
	"btcd-demo/chaincfg/chainhash"
	"btcd-demo/wire"
	"strings"
	"errors"
)

type Params struct {

	// Name defines a human-readable identifier for the network
	Name string

	// Mempool parameters
	RelayNonStdTxs bool

	// Net defines the magic bytes used to identify the network
	Net wire.BitcoinNet

	// DefaultPort defines the default peer-to-peer port for the network
	DefaultPort string

	// Human-readable part for Bech32 encoded segwit addresses, as defined
	// in BIP 173.
	Bech32HRPSegwit string

}

type Checkpoint struct {
	Height int32
	Hash *chainhash.Hash
}

var (
	// ErrDuplicatedNet describes an error where the parameters for a Bitcoin
	// network could not be set due to the network already being a standard
	// network or previously-registered into this package.
	ErrDuplicatedNet = errors.New("duplicated Bitcoin network")

)
var (
	registeredNets = make(map[wire.BitcoinNet]struct{})
	bech32SegwitPrefixes = make(map[string]struct{})
)

// IsBech32SegwitPrefix returns whether the prefix is a known prefix for segwit
// addresses on any default or registered network. This is used when decoding
// an address string into a specific address type
func IsBech32SegwitPrefix(prefix string) bool  {
	prefix = strings.ToLower(prefix)
	_, ok := bech32SegwitPrefixes[prefix]
	return ok
}


// Register registers the network parameters for a Bitcoin network.  This may
// error with ErrDuplicateNet if the network is already registered (either
// due to a previous Register call, or the network being one of the default
// networks).
//
// Network parameters should be registered into this package by a main package
// as early as possible.  Then, library packages may lookup networks or network
// parameters based on inputs and work regardless of the network being standard
// or not.
func Register(params *Params) error {
	if _, ok := registeredNets[params.Net]; ok {
		return ErrDuplicatedNet
	}
	registeredNets[params.Net] = struct{}{}

	// A valid Bech32 encoded segwit address always has as prefix the
	// human-readable part for the given net followed by '1'
	bech32SegwitPrefixes[params.Bech32HRPSegwit+"1"] = struct{}{}
	return nil
}


