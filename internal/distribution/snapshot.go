package distribution

import (
	"github.com/hashicorp/raft"
)

type raftSnapshot struct {
	data []byte
}

func (r *raftSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := func() error {
		if _, err := sink.Write(r.data); err != nil {
			return err
		}
		return sink.Close()
	}(); err != nil {
		return sink.Cancel()
	}
	return nil
}

func (r *raftSnapshot) Release() {}
