package wire

import (
	"io"
	"time"

	"github.com/hawkit/btcd-demo/chaincfg/chainhash"
)

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
