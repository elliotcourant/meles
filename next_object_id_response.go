package meles

import (
	"github.com/elliotcourant/buffers"
)

type nextObjectIdResponse struct {
	Identity uint8
}

func (nextObjectIdResponse) Server() {}

func (nextObjectIdResponse) RPC() {}

func (i *nextObjectIdResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(i.Identity)
	return buf.Bytes()
}

func (i *nextObjectIdResponse) Decode(src []byte) error {
	*i = nextObjectIdResponse{}
	buf := buffers.NewBytesReader(src)
	i.Identity = buf.NextUint8()
	return nil
}
