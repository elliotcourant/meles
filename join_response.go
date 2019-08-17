package meles

import (
	"github.com/elliotcourant/buffers"
)

type joinResponse struct {
	NodeID          string
	NodeIDGenerated bool
	AlreadyJoined   bool
}

func (joinResponse) Server() {}

func (joinResponse) RPC() {}

func (i *joinResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(i.NodeID)
	buf.AppendBool(i.NodeIDGenerated)
	buf.AppendBool(i.AlreadyJoined)
	return buf.Bytes()
}

func (i *joinResponse) Decode(src []byte) error {
	*i = joinResponse{}
	buf := buffers.NewBytesReader(src)
	i.NodeID = buf.NextString()
	i.NodeIDGenerated = buf.NextBool()
	i.AlreadyJoined = buf.NextBool()
	return nil
}
