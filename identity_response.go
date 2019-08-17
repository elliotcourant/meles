package meles

import (
	"github.com/elliotcourant/buffers"
)

type identityResponse struct {
	Id uint64
}

func (identityResponse) Server() {}

func (identityResponse) RPC() {}

func (i *identityResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(i.Id)
	return buf.Bytes()
}

func (i *identityResponse) Decode(src []byte) error {
	*i = identityResponse{}
	buf := buffers.NewBytesReader(src)
	i.Id = buf.NextUint64()
	return nil
}
