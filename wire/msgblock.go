package wire

type MsgBlock struct {
	Header       BlockHeader
	Transactions []*MsgTx
}

func (msg *MsgBlock) SerializeSize() int {
	// Block header bytes + Serialized varint size for the number of
	// transactions.
	// n := blockHeaderLen + VarIntSerializeSize(uint64(len(msg.Transactions)))
	//
	// for _, tx := range msg.Transactions {
	// 	//n += tx.Se
	// }
	// todo
	return 0
}
