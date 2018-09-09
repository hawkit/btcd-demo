package wire

type MsgBlock struct {
	Header       BlockHeader
	Transactions []*MsgTx
}
