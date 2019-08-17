package meles

import (
	"github.com/elliotcourant/buffers"
)

type handshakeIntention = int32

const (
	raftIntention handshakeIntention = 1 << iota
	rpcIntention
)

type handshakeRequest struct {
	Intention handshakeIntention
}

func (handshakeRequest) Client() {}

func (i *handshakeRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(i.Intention)
	return buf.Bytes()
}

func (i *handshakeRequest) Decode(src []byte) error {
	*i = handshakeRequest{}
	buf := buffers.NewBytesReader(src)
	i.Intention = buf.NextInt32()
	return nil
}
