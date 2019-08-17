package meles

import (
	"github.com/elliotcourant/buffers"
	"github.com/hashicorp/raft"
)

type installSnapshotResponse struct {
	raft.InstallSnapshotResponse
	Error error
}

func (installSnapshotResponse) Server() {}

func (installSnapshotResponse) Raft() {}

func (i *installSnapshotResponse) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendInt32(int32(i.ProtocolVersion))
	buf.AppendUint64(i.Term)
	buf.AppendBool(i.Success)
	buf.AppendError(i.Error)
	return buf.Bytes()
}

func (i *installSnapshotResponse) Decode(src []byte) error {
	*i = installSnapshotResponse{}
	buf := buffers.NewBytesReader(src)
	i.ProtocolVersion = raft.ProtocolVersion(buf.NextInt32())
	i.Term = buf.NextUint64()
	i.Success = buf.NextBool()
	i.Error = buf.NextError()
	return nil
}
