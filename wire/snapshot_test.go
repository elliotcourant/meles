package wire_test

import (
	"github.com/elliotcourant/meles/wire"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSnapshot_EncodeDecode(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		original := wire.Snapshot{
			Context: &wire.RaftContext{
				Id:         1234,
				Group:      125423,
				Addr:       "test:1235",
				SnapshotTs: 85392,
			},
			Index:   1231616,
			ReadTs:  8953295892,
			Done:    true,
			SinceTs: 38953892,
		}
		encoded := original.Encode()
		result := wire.Snapshot{}
		result.Decode(encoded)
		assert.Equal(t, original, result)
	})
}
