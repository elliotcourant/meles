package storage

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLog_Encode(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		j1, _ := json.Marshal(struct {
			FieldOne string
			FieldTwo int
		}{
			FieldOne: "hjkdgashjkdsaghjkdsaga",
			FieldTwo: 1234,
		})
		j2, _ := json.Marshal(struct {
			StuffOne string
			StuffTwo bool
		}{
			StuffOne: "fadshfds",
			StuffTwo: true,
		})
		item := Log{
			Log: raft.Log{
				Index:      125543634,
				Term:       6543,
				Type:       raft.LogCommand,
				Data:       j1,
				Extensions: j2,
			},
		}
		encoded := item.Encode()
		assert.NotEmpty(t, encoded)
		itemDecoder := &Log{}
		err := itemDecoder.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, item, *itemDecoder)
	})
}
