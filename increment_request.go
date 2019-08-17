package meles

import (
	"github.com/elliotcourant/buffers"
)

type incrementRequest struct {
	ObjectPath []byte
}

func (incrementRequest) Client() {}

func (incrementRequest) RPC() {}

func (i *incrementRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.Append(i.ObjectPath...)
	return buf.Bytes()
}

func (i *incrementRequest) Decode(src []byte) error {
	*i = incrementRequest{}
	buf := buffers.NewBytesReader(src)
	i.ObjectPath = buf.NextBytes()
	return nil
}
