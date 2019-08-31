package meles

import (
	"github.com/elliotcourant/buffers"
)

type distSequenceResponse struct {
	distSequenceChunk
}

func (distSequenceResponse) Server() {}

func (distSequenceResponse) RPC() {}

func (i *distSequenceResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendString(i.sequenceName)
	buf.AppendUint64(i.offset)
	buf.AppendUint64(i.count)
	buf.AppendUint64(i.start)
	buf.AppendUint64(i.end)
	return buf.Bytes()
}

func (i *distSequenceResponse) Decode(src []byte) error {
	*i = distSequenceResponse{}
	buf := buffers.NewBytesReader(src)
	i.sequenceName = buf.NextString()
	i.offset = buf.NextUint64()
	i.count = buf.NextUint64()
	i.start = buf.NextUint64()
	i.end = buf.NextUint64()
	return nil
}
