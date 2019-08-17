package meles

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type requestVoteRequest struct {
	raft.RequestVoteRequest
}

func (requestVoteRequest) Client() {}

func (requestVoteRequest) Raft() {}

func (i *requestVoteRequest) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(int32(i.ProtocolVersion))
	buf.AppendUint64(i.Term)
	buf.Append(i.Candidate...)
	buf.AppendUint64(i.LastLogIndex)
	buf.AppendUint64(i.LastLogTerm)
	buf.AppendBool(i.LeadershipTransfer)
	return buf.Bytes()
}

func (i *requestVoteRequest) Decode(src []byte) error {
	*i = requestVoteRequest{}
	buf := buffers.NewBytesReader(src)
	i.ProtocolVersion = raft.ProtocolVersion(buf.NextInt32())
	i.Term = buf.NextUint64()
	i.Candidate = buf.NextBytes()
	i.LastLogIndex = buf.NextUint64()
	i.LastLogTerm = buf.NextUint64()
	i.LeadershipTransfer = buf.NextBool()
	return nil
}
