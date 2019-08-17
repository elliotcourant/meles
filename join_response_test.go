package meles

import (
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestJoinResponse_Server(t *testing.T) {
	joinResponse{}.Server()
}

func TestJoinResponse_RPC(t *testing.T) {
	joinResponse{}.RPC()
}

func TestJoinResponse(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		i := joinResponse{
			NodeID:          "5",
			NodeIDGenerated: true,
		}
		encoded := i.Encode()
		fmt.Println(hex.Dump(encoded))
		d := joinResponse{}
		err := d.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, i, d)
	})
}
