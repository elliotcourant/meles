package meles

import (
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJoinRequest_Client(t *testing.T) {
	joinRequest{}.Client()
}

func TestJoinRequest_RPC(t *testing.T) {
	joinRequest{}.RPC()
}

func TestJoinRequest(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		i := joinRequest{
			NodeID:         "1",
			Address:        "127.0.0.1:5433",
			GenerateNodeId: true,
		}
		encoded := i.Encode()
		fmt.Println(hex.Dump(encoded))
		d := joinRequest{}
		err := d.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, i, d)
	})
}
