package meles

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestResolveAddress(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		addr, err := resolveAddress(":5432")
		assert.NoError(t, err)
		assert.NotEmpty(t, addr)
	})

	t.Run("allocated", func(t *testing.T) {
		listener, err := net.Listen("tcp", ":")
		assert.NoError(t, err)
		assert.NotNil(t, listener)
		defer listener.Close()

		addr, err := resolveAddress(listener.Addr().String())
		assert.NoError(t, err)
		assert.NotEmpty(t, addr)
	})
}
