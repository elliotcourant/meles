package storage

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type Log struct {
	raft.Log
}

func NewLogFromRaft(log *raft.Log) *Log {
	return &Log{
		Log: *log,
	}
}

func (i Log) Path() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendByte(MetaPrefix_Log)
	buf.AppendUint64(i.Index)
	return buf.Bytes()
}

func (i *Log) Decode(src []byte) error {
	*i = Log{}
	buf := buffers.NewBytesReader(src)
	i.Index = buf.NextUint64()
	i.Term = buf.NextUint64()
	i.Type = raft.LogType(buf.NextUint8())
	i.Data = buf.NextBytes()
	i.Extensions = buf.NextBytes()
	return nil
}

func (i *Log) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(i.Index)
	buf.AppendUint64(i.Term)
	buf.AppendUint8(uint8(i.Type))
	buf.Append(i.Data...)
	buf.Append(i.Extensions...)
	return buf.Bytes()
}
