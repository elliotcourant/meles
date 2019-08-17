package meles

import (
	"github.com/elliotcourant/buffers"
)

type joinRequest struct {
	NodeID         string
	Address        string
	GenerateNodeId bool
}

func (joinRequest) Client() {}

func (joinRequest) RPC() {}

func (i *joinRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(i.NodeID)
	buf.AppendString(i.Address)
	buf.AppendBool(i.GenerateNodeId)
	return buf.Bytes()
}

func (i *joinRequest) Decode(src []byte) error {
	*i = joinRequest{}
	buf := buffers.NewBytesReader(src)
	i.NodeID = buf.NextString()
	i.Address = buf.NextString()
	i.GenerateNodeId = buf.NextBool()
	return nil
}
