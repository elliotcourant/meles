package meles

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type appendEntriesRequest struct {
	raft.AppendEntriesRequest
}

func (appendEntriesRequest) Client() {}

func (appendEntriesRequest) Raft() {}

func (i *appendEntriesRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(int32(i.ProtocolVersion))
	buf.AppendUint64(i.Term)
	buf.Append(i.Leader...)
	buf.AppendUint64(i.PrevLogEntry)
	buf.AppendUint64(i.PrevLogTerm)
	entryCount := 0
	for _, entry := range i.Entries {
		if entry != nil {
			entryCount++
		}
	}
	buf.AppendUint32(uint32(entryCount))
	for _, entry := range i.Entries {
		if entry != nil {
			b := buffers.NewBytesBuffer()
			b.AppendUint64(entry.Index)
			b.AppendUint64(entry.Term)
			b.AppendUint8(uint8(entry.Type))
			b.Append(entry.Data...)
			b.Append(entry.Extensions...)
			buf.Append(b.Bytes()...)
		}
	}
	buf.AppendUint64(i.LeaderCommitIndex)
	return buf.Bytes()
}

func (i *appendEntriesRequest) Decode(src []byte) error {
	*i = appendEntriesRequest{}
	buf := buffers.NewBytesReader(src)
	i.ProtocolVersion = raft.ProtocolVersion(buf.NextInt32())
	i.Term = buf.NextUint64()
	i.Leader = buf.NextBytes()
	i.PrevLogEntry = buf.NextUint64()
	i.PrevLogTerm = buf.NextUint64()
	length := buf.NextUint32()
	i.Entries = make([]*raft.Log, length)
	for x := range i.Entries {
		b := buffers.NewBytesReader(buf.NextBytes())
		log := &raft.Log{}
		log.Index = b.NextUint64()
		log.Term = b.NextUint64()
		log.Type = raft.LogType(b.NextUint8())
		log.Data = b.NextBytes()
		log.Extensions = b.NextBytes()
		i.Entries[x] = log
	}
	i.LeaderCommitIndex = buf.NextUint64()
	return nil
}
