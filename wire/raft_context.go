package wire

import (
	"github.com/elliotcourant/meles/buffers"
)

type RaftContext struct {
	Id         uint64
	Group      uint32
	Addr       string
	SnapshotTs uint64
}

func (item *RaftContext) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(item.Id)
	buf.AppendUint32(item.Group)
	buf.AppendString(item.Addr)
	buf.AppendUint64(item.SnapshotTs)
	return buf.Bytes()
}

func (item *RaftContext) Decode(src []byte) {
	*item = RaftContext{}
	buf := buffers.NewBytesReader(src)
	item.Id = buf.NextUint64()
	item.Group = buf.NextUint32()
	item.Addr = buf.NextString()
	item.SnapshotTs = buf.NextUint64()
}
