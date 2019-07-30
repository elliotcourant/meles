package wire

import (
	"github.com/elliotcourant/meles/buffers"
)

type HandshakeResponse struct {
	ID uint64
}

func (item *HandshakeResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(item.ID)
	return buf.Bytes()
}

func (item *HandshakeResponse) EncodeMessage() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(MsgHandshakeResponse)
	buf.Append(item.Encode()...)
	return buf.Bytes()
}

func (item *HandshakeResponse) Decode(src []byte) {
	*item = HandshakeResponse{}
	buf := buffers.NewBytesReader(src)
	item.ID = buf.NextUint64()
}