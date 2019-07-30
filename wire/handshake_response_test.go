package wire_test

import (
	"github.com/elliotcourant/meles"
	"github.com/elliotcourant/meles/wire"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHandshakeResponse_EncodeDecode(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		original := wire.HandshakeResponse{
			ID: meles.GetShortID(),
		}
		encoded := original.Encode()
		result := wire.HandshakeResponse{
			ID: "",
		}
		result.Decode(encoded)
		assert.Equal(t, original, result)
	})
}
