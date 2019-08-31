package meles

import (
	"github.com/elliotcourant/buffers"
)

type distSequenceRequest struct {
	SequenceName string
}

func (distSequenceRequest) Client() {}

func (distSequenceRequest) RPC() {}

func (i *distSequenceRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(i.SequenceName)
	return buf.Bytes()
}

func (i *distSequenceRequest) Decode(src []byte) error {
	*i = distSequenceRequest{}
	buf := buffers.NewBytesReader(src)
	i.SequenceName = buf.NextString()
	return nil
}
