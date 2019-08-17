package meles

import (
	"encoding/hex"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestApplyTransactionRequest(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		appendEntry := applyTransactionRequest{
			NodeID: "1234",
			Transaction: &transactionStorage{
				Actions: []action{
					{
						Type:  actionTypeSet,
						Key:   []byte("test"),
						Value: []byte("value"),
					},
					{
						Type:  actionTypeDelete,
						Key:   []byte("test2"),
						Value: nil,
					},
				},
			},
		}
		encoded := appendEntry.Encode()
		fmt.Println(hex.Dump(encoded))
		decodeEntry := applyTransactionRequest{}
		err := decodeEntry.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, appendEntry, decodeEntry)
	})
}
