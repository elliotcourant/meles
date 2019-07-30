package wire_test

import (
	"github.com/elliotcourant/meles/wire"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHandshakeResponse_EncodeDecode(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		original := wire.HandshakeResponse{
			ID: 1231616,
		}
		encoded := original.Encode()
		result := wire.HandshakeResponse{
			ID: 0,
		}
		result.Decode(encoded)
		assert.Equal(t, original, result)
	})
}
