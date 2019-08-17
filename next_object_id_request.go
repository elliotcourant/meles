package meles

import (
	"github.com/elliotcourant/buffers"
)

type nextObjectIdRequest struct {
	ObjectPath []byte
}

func (nextObjectIdRequest) Client() {}

func (nextObjectIdRequest) RPC() {}

func (i *nextObjectIdRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.Append(i.ObjectPath...)
	return buf.Bytes()
}

func (i *nextObjectIdRequest) Decode(src []byte) error {
	*i = nextObjectIdRequest{}
	buf := buffers.NewBytesReader(src)
	i.ObjectPath = buf.NextBytes()
	return nil
}
