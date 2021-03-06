package wire

import (
	"io"
	"time"

	"github.com/hawkit/btcd-demo/chaincfg/chainhash"

	"bytes"
)

// MaxBlockHeaderPayload is the maximum block number of bytes a block header can be.
// Version 4 bytes + Timestamp 4 bytes + Bits 4 bytes + Nonce 4 bytes +
// PrevBlock and MerkleRoot hashes.
const MaxBlockHeaderPayload = 16 + (chainhash.HashSize * 2)

// BlockHeader defines information about a block and is used to in the bitcoin
// block (MsgBlock) and headers (MsgHeaders) messages.
type BlockHeader struct {
	// Version of the block. This is not the same as the protocol version.
	Version int32

	// Hash of the previous block header in the block chain
	PrevBlock chainhash.Hash

	// Merkle tree reference to hash of all transactions for the block.
	MerkelRoot chainhash.Hash

	// Time the block was created. This is, unfortunately, encoded as a
	// uint32 on the wire and therefore is limited to 2106
	Timestamp time.Time

	// Difficulty target for the block.
	Bits uint32

	// Nonce used to generate the block.
	Nonce uint32
}

const blockHeaderLen = 80

func (h *BlockHeader) BtcEncode(w io.Writer, pver uint32, enc MessageEncoding) error {
	return writeBlockHeader(w, pver, h)
}

// BlockHash computes the block identifier hash for the given block header.
func (h *BlockHeader) BlockHash() chainhash.Hash {
	// Encode the header and double sha256 everything prior to the number of
	// transactions. Ignore the error returns since there is no way the
	// encode could fail except being out of memory which would cause a
	// run-time panic.
	buf := bytes.NewBuffer(make([]byte, 0, MaxBlockHeaderPayload))
	_ = writeBlockHeader(buf, 0, h)
	return chainhash.DoubleHashH(buf.Bytes())
}
