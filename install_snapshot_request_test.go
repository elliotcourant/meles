package meles

import (
	"encoding/hex"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInstallSnapshotRequest_Client(t *testing.T) {
	installSnapshotRequest{}.Client()
}

func TestInstallSnapshotRequest(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		i := installSnapshotRequest{
			InstallSnapshotRequest: raft.InstallSnapshotRequest{
				RPCHeader: raft.RPCHeader{
					ProtocolVersion: raft.ProtocolVersionMax,
				},
				SnapshotVersion:    raft.SnapshotVersionMax,
				Term:               5326423643,
				Leader:             []byte("123.123.123.123:1234"),
				LastLogIndex:       1532523,
				LastLogTerm:        532432,
				Peers:              nil,
				Configuration:      make([]byte, 0),
				ConfigurationIndex: 5436534,
				Size:               432432,
			},
			Snapshot: []byte("this is a snapshot thing"),
		}
		encoded := i.Encode()
		fmt.Println(hex.Dump(encoded))
		d := installSnapshotRequest{}
		err := d.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, i, d)
	})
}
