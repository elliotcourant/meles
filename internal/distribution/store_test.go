package distribution

import (
	"fmt"
	"github.com/elliotcourant/arkdb/internal/testutils"
	"github.com/elliotcourant/arkdb/pkg/transport"
	"github.com/elliotcourant/meles/internal/storage"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func VerifyLeader(t *testing.T, nodes []Barge) {
	maxRetry := 3
	retries := 0
	leaderAddr := ""
TryAgain:
	for _, node := range nodes {
		if IsStopped() {
			continue
		}
		addr, _, err := WaitForLeader(time.Second * 10)
		assert.NoError(t, err)
		if leaderAddr == "" {
			leaderAddr = addr
		} else if leaderAddr != addr {
			if retries < maxRetry {
				retries++
				time.Sleep(5 * time.Second)
				goto TryAgain
			}
		}
		assert.Equal(t, leaderAddr, addr)
	}
	timber.Infof("current leader: %s", leaderAddr)
}

func TestNewDistributor(t *testing.T) {
	t.Run("single", func(t *testing.T) {
		tempDir, cleanup := testutils.NewTempDirectory(t)

		ln, err := transport.NewTransport(":")
		assert.NoError(t, err)

		defer cleanup()
		d, err := NewDistributor(ln, &Options{
			Directory:     tempDir,
			ListenAddress: ln.Addr().String(),
			Peers:         []string{ln.Addr().String()},
			Join:          false,
		}, timber.With(timber.Keys{
			"test": t.Name(),
		}))
		assert.NoError(t, err)
		assert.NotNil(t, d)

		err = Start()
		assert.NoError(t, err)

		time.Sleep(time.Second * 5)
	})

	t.Run("multiple", func(t *testing.T) {
		numberOfNodes := 3

		listeners := make([]transport.Transport, numberOfNodes)
		peers := make([]string, numberOfNodes)
		for i := range listeners {
			ln, err := transport.NewTransport(":")
			assert.NoError(t, err)
			listeners[i] = ln
			peers[i] = ln.Addr().String()
		}

		cleanups := make([]func(), numberOfNodes)
		nodes := make([]Barge, numberOfNodes)

		for i := 0; i < numberOfNodes; i++ {
			func() {
				tempDir, cleanup := testutils.NewTempDirectory(t)
				cleanups[i] = cleanup

				d, err := NewDistributor(listeners[i], &Options{
					Directory:     tempDir,
					ListenAddress: listeners[i].Addr().String(),
					Peers:         peers,
					Join:          false,
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
			go func(node Barge) {
				err := Start()
				assert.NoError(t, err)
			}(node)
		}

		// Make sure all of the nodes have the same leader
		VerifyLeader(t, nodes)
	})

	t.Run("non-leader write", func(t *testing.T) {
		numberOfNodes := 9

		listeners := make([]transport.Transport, numberOfNodes)
		peers := make([]string, numberOfNodes)
		for i := range listeners {
			ln, err := transport.NewTransport(":")
			assert.NoError(t, err)
			listeners[i] = ln
			peers[i] = ln.Addr().String()
		}

		cleanups := make([]func(), numberOfNodes)
		nodes := make([]Barge, numberOfNodes)

		for i := 0; i < numberOfNodes; i++ {
			func() {
				tempDir, cleanup := testutils.NewTempDirectory(t)
				cleanups[i] = cleanup

				d, err := NewDistributor(listeners[i], &Options{
					Directory:     tempDir,
					ListenAddress: listeners[i].Addr().String(),
					Peers:         peers,
					Join:          false,
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
			go func(node Barge) {
				err := Start()
				assert.NoError(t, err)
			}(node)
		}

		// Make sure all of the nodes have the same leader
		VerifyLeader(t, nodes)

		for i, node := range nodes {
			if IsLeader() {
				continue
			}

			tx, err := Begin()
			assert.NoError(t, err)

			table := &storage.Table{
				TableID:   uint8(i + 1),
				TableName: fmt.Sprintf("table_%d", i),
			}

			err = Set(table.Path(), table)
			assert.NoError(t, err)

			err = Commit()
			assert.NoError(t, err)

			tx, err = Begin()
			assert.NoError(t, err)

			tableRead := &storage.Table{}
			err = Get(table.Path(), tableRead)
			assert.NoError(t, err)
			assert.Equal(t, table, tableRead)
		}
	})

	t.Run("single shutdown", func(t *testing.T) {
		tempDir, cleanup := testutils.NewTempDirectory(t)

		ln, err := transport.NewTransport(":")
		assert.NoError(t, err)

		defer cleanup()
		d, err := NewDistributor(ln, &Options{
			Directory:     tempDir,
			ListenAddress: ln.Addr().String(),
			Peers:         []string{ln.Addr().String()},
			Join:          false,
		}, timber.With(timber.Keys{
			"test": t.Name(),
		}))
		assert.NoError(t, err)
		assert.NotNil(t, d)

		err = Start()
		assert.NoError(t, err)

		time.Sleep(time.Second * 5)

		err = Stop()
		assert.NoError(t, err)

		tx, err := Begin()
		assert.EqualError(t, err, ErrBargeStopped.Error())
		assert.Nil(t, tx)
	})

	t.Run("fail over", func(t *testing.T) {
		numberOfNodes := 9

		listeners := make([]transport.Transport, numberOfNodes)
		peers := make([]string, numberOfNodes)
		for i := range listeners {
			ln, err := transport.NewTransport(":")
			assert.NoError(t, err)
			listeners[i] = ln
			peers[i] = ln.Addr().String()
		}

		cleanups := make([]func(), numberOfNodes)
		nodes := make([]Barge, numberOfNodes)

		for i := 0; i < numberOfNodes; i++ {
			func() {
				tempDir, cleanup := testutils.NewTempDirectory(t)
				cleanups[i] = cleanup

				d, err := NewDistributor(listeners[i], &Options{
					Directory:     tempDir,
					ListenAddress: listeners[i].Addr().String(),
					Peers:         peers,
					Join:          false,
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
			go func(node Barge) {
				err := Start()
				assert.NoError(t, err)
			}(node)
		}

		// Make sure all of the nodes have the same leader
		VerifyLeader(t, nodes)

		// Kill the leader node to force an election.
		for _, node := range nodes {
			if !IsLeader() {
				continue
			}
			timber.Warningf("killing leader node [%s]", NodeID())
			err := Stop()
			assert.NoError(t, err)
			break
		}

		// Wait a short amount of time for a new leader
		time.Sleep(5 * time.Second)

		// Make sure that the remaining nodes all have the same leader.
		VerifyLeader(t, nodes)
	})

	t.Run("leader write", func(t *testing.T) {
		numberOfNodes := 9

		listeners := make([]transport.Transport, numberOfNodes)
		peers := make([]string, numberOfNodes)
		for i := range listeners {
			ln, err := transport.NewTransport(":")
			assert.NoError(t, err)
			listeners[i] = ln
			peers[i] = ln.Addr().String()
		}

		cleanups := make([]func(), numberOfNodes)
		nodes := make([]Barge, numberOfNodes)

		for i := 0; i < numberOfNodes; i++ {
			func() {
				tempDir, cleanup := testutils.NewTempDirectory(t)
				cleanups[i] = cleanup

				d, err := NewDistributor(listeners[i], &Options{
					Directory:     tempDir,
					ListenAddress: listeners[i].Addr().String(),
					Peers:         peers,
					Join:          false,
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
			go func(node Barge) {
				err := Start()
				assert.NoError(t, err)
			}(node)
		}

		// Make sure all of the nodes have the same leader
		VerifyLeader(t, nodes)

		for i, node := range nodes {
			if !IsLeader() {
				continue
			}

			tx, err := Begin()
			assert.NoError(t, err)

			table := &storage.Table{
				TableID:   uint8(i + 1),
				TableName: fmt.Sprintf("table_%d", i),
			}

			err = Set(table.Path(), table)
			assert.NoError(t, err)

			err = Commit()
			assert.NoError(t, err)

			tx, err = Begin()
			assert.NoError(t, err)

			tableRead := &storage.Table{}
			err = Get(table.Path(), tableRead)
			assert.NoError(t, err)
			assert.Equal(t, table, tableRead)
		}
	})
}
