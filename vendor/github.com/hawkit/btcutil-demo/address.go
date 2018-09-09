package btcutil

import (
	"strings"
	"github.com/pkg/errors"
	"btcd-demo/chaincfg"
)

// Address is an interface type for any type of destination a transaction
// output may spend to. This includes pay-to-pubkey (P2PK), pay-to-pubkey-hash
// (P2PKH), and pay-to-script-hash (P2SH). Address is designed to be generic
// enough that other kinds of addresses may be added in the future without
// changing the decoding and encoding API.
type Address interface {

	// String returns the string encoding of the transaction output
	// destination
	//
	// Please note that String differs subtly from EncodeAddress: String
	// will return the value as a string without any conversion, while
	// EncodeAddress may convert destination types (for example,
	// converting pubkeys to P2PKH addresses) before encoding as a payment
	// address string.
	String() string

	// EncodeAddress returns the string encoding of the payment address
	// associated with the Address value. See the comment on String
	// for how this method differs from String.
	EncodeAddress() string

	// ScriptAddress returns the raw bytes of the address to be used
	// when inserting the address into a txout's script.
	ScriptAddress() []byte

	// IsForNet returns whether or not the address is associated with the
	// passed bitcion network.
	IsForNet(params * chaincfg.Params)bool

}

// DecodeAddress decodes the string encoding of an address and returns
// the Address if addr is a valide encoding for a known address type.
//
// The bitcoin network the address is associated with is extracted if possible.
// When the address does not encode the network, such as in the case of a raw
// public key, the address will be associcated with the passed defaultNet
func DecodeAddress(addr string, defaultNet *chaincfg.Params) (Address, error) {
	// Bech32 encoded segwit address start with a human-readable part
	// (hrp) followed by '1'. For Bitcoin mainet the hrp is "bc", and for
	// testnet it is "tb". If the address string has a prefix that matches
	// one of the prefixes for the known networks, we try to decode it as
	// a segwit address.
	oneIndex := strings.LastIndexByte(addr, '1')
	if oneIndex > 1 {
		prefix := addr[:oneIndex+1]
		if chaincfg.IsBech32SegwitPrefix(prefix) {

		}

	}

	return nil, errors.New("decoded address is of unknown size")


}