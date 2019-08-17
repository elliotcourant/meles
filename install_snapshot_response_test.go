package meles

import (
	"encoding/hex"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInstallSnapshotResponse_Server(t *testing.T) {
	installSnapshotResponse{}.Server()
}

func TestInstallSnapshotResponse(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		i := installSnapshotResponse{
			InstallSnapshotResponse: raft.InstallSnapshotResponse{
				RPCHeader: raft.RPCHeader{
					ProtocolVersion: raft.ProtocolVersionMax,
				},
				Term:    153252,
				Success: true,
			},
			Error: nil,
		}
		encoded := i.Encode()
		fmt.Println(hex.Dump(encoded))
		d := installSnapshotResponse{}
		err := d.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, i, d)
	})
}
