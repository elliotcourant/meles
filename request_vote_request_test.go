package meles

import (
	"encoding/hex"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRequestVoteRequest_Client(t *testing.T) {
	requestVoteRequest{}.Client()
}

func TestRequestVoteRequest(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		i := requestVoteRequest{
			RequestVoteRequest: raft.RequestVoteRequest{
				RPCHeader: raft.RPCHeader{
					ProtocolVersion: raft.ProtocolVersionMax,
				},
				Term:               4162534672,
				Candidate:          []byte("dgsagdsa"),
				LastLogIndex:       12453215,
				LastLogTerm:        5432513,
				LeadershipTransfer: false,
			},
		}
		encoded := i.Encode()
		fmt.Println(hex.Dump(encoded))
		d := requestVoteRequest{}
		err := d.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, i, d)
	})
}
