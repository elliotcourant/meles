package meles

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	t.Run("simple", func(t *testing.T) {

		numberOfStores := 3

		listeners := make([]net.Listener, numberOfStores)
		peers := make([]string, numberOfStores)
		dirs := make([]string, numberOfStores)
		cleanups := make([]func(), numberOfStores)
		for i := 0; i < numberOfStores; i++ {
			listener, err := net.Listen("tcp", ":")
			if !assert.NoError(t, err) {
				panic(err)
			}
			listeners[i] = listener

			peer, err := resolveAddress(listener.Addr().String())
			if !assert.NoError(t, err) {
				panic(err)
			}
			peers[i] = peer

			dir, cleanup := TempDirectory(t)
			dirs[i] = dir

			cleanups[i] = func() {
				defer cleanup()
				if err := listener.Close(); !assert.NoError(t, err) {
					panic(err)
				}
			}
		}
		defer func() {
			for _, clean := range cleanups {
				clean()
			}
		}()

		stores := make([]Store, numberOfStores)
		for i := 0; i < numberOfStores; i++ {
			go func(i int) {
				stores[i] = NewStore(listeners[i], peers, dirs[i], true)
			}(i)
		}

		time.Sleep(5 * time.Second)

		for i := 0; i < numberOfStores; i++ {
			stores[i].Close()
		}

		assert.NotEmpty(t, stores)
	})
}
