package meles

import (
	"github.com/elliotcourant/buffers"
)

type errorResponse struct {
	Error error
}

func (errorResponse) Server() {}

func (errorResponse) Raft() {}

func (errorResponse) RPC() {}

func (i *errorResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendError(i.Error)
	return buf.Bytes()
}

func (i *errorResponse) Decode(src []byte) error {
	*i = errorResponse{}
	buf := buffers.NewBytesReader(src)
	i.Error = buf.NextError()
	return nil
}
