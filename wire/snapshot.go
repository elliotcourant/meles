package wire

import (
	"github.com/elliotcourant/meles/buffers"
)

type Snapshot struct {
	Context *RaftContext
	Index   uint64
	ReadTs  uint64
	// done is used to indicate that snapshot stream was a success.
	Done bool
	// since_ts stores the ts of the last snapshot to support diff snap updates.
	SinceTs uint64
}

func (item *Snapshot) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.Append(item.Context.Encode()...)
	buf.AppendUint64(item.Index)
	buf.AppendUint64(item.ReadTs)
	buf.AppendBool(item.Done)
	buf.AppendUint64(item.SinceTs)
	return buf.Bytes()
}

func (item *Snapshot) Decode(src []byte) {
	*item = Snapshot{}
	buf := buffers.NewBytesReader(src)
	ctx := &RaftContext{}
	c := buf.NextBytes()
	ctx.Decode(c)
	item.Context = ctx
	item.Index = buf.NextUint64()
	item.ReadTs = buf.NextUint64()
	item.Done = buf.NextBool()
	item.SinceTs = buf.NextUint64()
}
