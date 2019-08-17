package meles

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type applyTransactionRequest struct {
	NodeID      raft.ServerID
	Transaction *transactionStorage
}

func (applyTransactionRequest) Client() {}

func (applyTransactionRequest) RPC() {}

func (i *applyTransactionRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(string(i.NodeID))
	buf.Append(i.Transaction.Encode()...)
	return buf.Bytes()
}

func (i *applyTransactionRequest) Decode(src []byte) error {
	*i = applyTransactionRequest{
		Transaction: &transactionStorage{},
	}
	buf := buffers.NewBytesReader(src)
	i.NodeID = raft.ServerID(buf.NextString())
	return i.Transaction.Decode(buf.NextBytes())
}
