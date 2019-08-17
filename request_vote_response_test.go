package meles

import (
	"encoding/hex"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRequestVoteResponse_Server(t *testing.T) {
	requestVoteResponse{}.Server()
}

func TestRequestVoteResponse(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		i := requestVoteResponse{
			RequestVoteResponse: raft.RequestVoteResponse{
				RPCHeader: raft.RPCHeader{
					ProtocolVersion: raft.ProtocolVersionMax,
				},
				Term:    4162534672,
				Peers:   nil,
				Granted: false,
			},
		}
		encoded := i.Encode()
		fmt.Println(hex.Dump(encoded))
		d := requestVoteResponse{}
		err := d.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, i, d)
	})
}
