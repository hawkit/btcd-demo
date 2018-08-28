package chaincfg

import "btcd-demo/chaincfg/chainhash"

type Params struct {

}

type Checkpoint struct {
	Height int32
	Hash *chainhash.Hash
}
