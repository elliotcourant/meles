package meles

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type log struct {
	raft.Log
}

func newLogFromRaft(l *raft.Log) *log {
	return &log{
		Log: *l,
	}
}

func (i log) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(metaPrefix_Log)
	buf.AppendUint64(i.Index)
	return buf.Bytes()
}

func (i *log) Decode(src []byte) error {
	*i = log{}
	buf := buffers.NewBytesReader(src)
	i.Index = buf.NextUint64()
	i.Term = buf.NextUint64()
	i.Type = raft.LogType(buf.NextUint8())
	i.Data = buf.NextBytes()
	i.Extensions = buf.NextBytes()
	return nil
}

func (i *log) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(i.Index)
	buf.AppendUint64(i.Term)
	buf.AppendUint8(uint8(i.Type))
	buf.Append(i.Data...)
	buf.Append(i.Extensions...)
	return buf.Bytes()
}
