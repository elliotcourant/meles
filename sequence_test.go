package meles

import (
	"github.com/elliotcourant/meles/testutils"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestBoat_GetObjectSequence(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		tempDir, cleanup := testutils.NewTempDirectory(t)

		ln, err := newTransport(":")
		assert.NoError(t, err)

		defer cleanup()
		d, err := newDistributor(ln, &distOptions{
			Directory: tempDir,
			Peers:     []string{ln.Addr().String()},
		}, timber.With(timber.Keys{
			"test": t.Name(),
		}))
		assert.NoError(t, err)
		assert.NotNil(t, d)

		err = d.Start()
		assert.NoError(t, err)

		VerifyLeader(t, d)

		numberOfThreads := 4
		numberOfIds := 30
		ids := make(chan uint64, numberOfThreads*numberOfIds)
		var wg sync.WaitGroup
		wg.Add(numberOfThreads)
		for y := 0; y < numberOfThreads; y++ {
			go func(d barge) {
				defer wg.Done()
				for x := 0; x < numberOfIds; x++ {
					id, err := d.NextIncrementId([]byte("table"))
					assert.NoError(t, err)
					ids <- id
				}
			}(d)
		}
		wg.Wait()
		assert.NotEmpty(t, ids)
		distinct := map[uint64]interface{}{}
		for i := 0; i < (numberOfThreads * numberOfIds); i++ {
			id := <-ids
			_, ok := distinct[id]
			assert.False(t, ok, "duplicate ID found: %d", id)
			distinct[id] = nil
		}
	})

	t.Run("multiple nodes", func(t *testing.T) {
		numberOfNodes := 5
		numberOfIds := 10

		listeners := make([]transportInterface, numberOfNodes)
		peers := make([]string, numberOfNodes)
		for i := range listeners {
			ln, err := newTransport(":")
			assert.NoError(t, err)
			listeners[i] = ln
			peers[i] = ln.Addr().String()
		}

		cleanups := make([]func(), numberOfNodes)
		nodes := make([]barge, numberOfNodes)

		for i := 0; i < numberOfNodes; i++ {
			func() {
				tempDir, cleanup := testutils.NewTempDirectory(t)
				cleanups[i] = cleanup

				d, err := newDistributor(listeners[i], &distOptions{
					Directory: tempDir,
					Peers:     peers,
				}, timber.With(timber.Keys{
					"test": t.Name(),
				}))
				assert.NoError(t, err)
				assert.NotNil(t, d)

				nodes[i] = d
			}()
		}

		defer func(cleanups []func()) {
			for _, cleanup := range cleanups {
				cleanup()
			}
		}(cleanups)

		timber.Debugf("created %d node(s), starting now", numberOfNodes)

		for _, node := range nodes {
			go func(node barge) {
				err := node.Start()
				assert.NoError(t, err)
			}(node)
		}

		// Make sure all of the nodes have the same leader
		VerifyLeader(t, nodes...)

		ids := make(chan uint64, numberOfNodes*numberOfIds)

		var wg sync.WaitGroup
		wg.Add(numberOfNodes)
		for _, node := range nodes {
			go func(node barge) {
				defer wg.Done()
				for x := 0; x < numberOfIds; x++ {
					id, err := node.NextIncrementId([]byte("table"))
					assert.NoError(t, err)
					ids <- id
				}
			}(node)
		}
		wg.Wait()
		assert.NotEmpty(t, ids)
		distinct := map[uint64]interface{}{}
		for i := 0; i < (numberOfNodes * numberOfIds); i++ {
			id := <-ids
			_, ok := distinct[id]
			assert.False(t, ok, "duplicate ID found: %d", id)
			distinct[id] = nil
		}
	})
}
