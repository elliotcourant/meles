package meles

import (
	"github.com/elliotcourant/buffers"
)

type incrementResponse struct {
	Identity uint64
}

func (incrementResponse) Server() {}

func (incrementResponse) RPC() {}

func (i *incrementResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(i.Identity)
	return buf.Bytes()
}

func (i *incrementResponse) Decode(src []byte) error {
	*i = incrementResponse{}
	buf := buffers.NewBytesReader(src)
	i.Identity = buf.NextUint64()
	return nil
}
