package meles

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type discoveryRequest struct {
	NodeID raft.ServerID
}

func (discoveryRequest) Client() {}

func (discoveryRequest) RPC() {}

func (i *discoveryRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(string(i.NodeID))
	return buf.Bytes()
}

func (i *discoveryRequest) Decode(src []byte) error {
	*i = discoveryRequest{}
	buf := buffers.NewBytesReader(src)
	i.NodeID = raft.ServerID(buf.NextString())
	return nil
}
