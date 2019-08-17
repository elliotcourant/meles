package meles

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestTransaction_Encode(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		j1, _ := json.Marshal(struct {
			FieldOne string
			FieldTwo int
		}{
			FieldOne: "hjkdgashjkdsaghjkdsaga",
			FieldTwo: 1234,
		})
		item := transactionStorage{
			Actions: []action{
				{
					Type:  actionTypeSet,
					Key:   []byte("test"),
					Value: j1,
				},
				{
					Type: actionTypeDelete,
					Key:  []byte("test2"),
				},
				{
					Type:  actionTypeSet,
					Key:   []byte("test3"),
					Value: j1,
				},
			},
		}
		encoded := item.Encode()
		assert.NotEmpty(t, encoded)
		itemDecoder := &transactionStorage{}
		err := itemDecoder.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, item, *itemDecoder)
	})
}
