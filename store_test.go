package meles

import (
	"fmt"
	"github.com/elliotcourant/meles/testutils"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
)

func VerifyLeader(t *testing.T, nodes ...barge) {
	timber.Infof("verifying leader for %d node(s)", len(nodes))
	start := time.Now()
	maxRetry := 5
	retries := 0
TryAgain:
	leaderAddr := ""
	for _, node := range nodes {
		if node.IsStopped() {
			continue
		}
		addr, _, err := node.WaitForLeader(time.Second * 10)
		assert.NoError(t, err)
		if leaderAddr == "" {
			leaderAddr = addr
		} else if leaderAddr != addr {
			if retries < maxRetry {
				retries++
				time.Sleep(10 * time.Second)
				goto TryAgain
			}
		}
		assert.Equal(t, leaderAddr, addr, "node [%s] does not match expected leader", node.NodeID())
	}
	timber.Infof("current leader [%s] verification time: %s retries: %d", leaderAddr, time.Since(start), retries)
}

func TestNewDistributor(t *testing.T) {
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

		time.Sleep(time.Second * 5)
	})

	t.Run("multiple", func(t *testing.T) {
		numberOfNodes := 3

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
	})

	t.Run("non-leader write", func(t *testing.T) {
		numberOfNodes := 9

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

		for i, node := range nodes {
			if node.IsLeader() {
				continue
			}

			tx, err := node.Begin()
			assert.NoError(t, err)

			key, value := []byte(fmt.Sprintf("test_%d", i)), []byte("value")
			err = tx.Set(key, value)
			assert.NoError(t, err)

			err = tx.Commit()
			assert.NoError(t, err)

			tx, err = node.Begin()
			assert.NoError(t, err)

			readValue, err := tx.Get(key)
			assert.NoError(t, err)
			assert.NotEmpty(t, readValue)
			assert.Equal(t, value, readValue)
		}

		// Make sure all of the nodes have the same leader
		VerifyLeader(t, nodes...)
	})

	t.Run("single shutdown", func(t *testing.T) {
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

		time.Sleep(time.Second * 5)

		err = d.Stop()
		assert.NoError(t, err)

		tx, err := d.Begin()
		assert.EqualError(t, err, ErrStopped.Error())
		assert.Nil(t, tx)
	})

	t.Run("fail over", func(t *testing.T) {
		numberOfNodes := 9

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

		// Kill the leader node to force an election.
		for _, node := range nodes {
			if !node.IsLeader() {
				continue
			}
			timber.Warningf("killing leader node [%s]", node.NodeID())
			err := node.Stop()
			assert.NoError(t, err)
			break
		}

		// Wait a short amount of time for a new leader
		time.Sleep(5 * time.Second)

		// Make sure that the remaining nodes all have the same leader.
		VerifyLeader(t, nodes...)
	})

	t.Run("majority failure", func(t *testing.T) {
		numberOfNodes := 9

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

		majority := int(math.Round(float64(numberOfNodes)/2) + 1)
		timber.Debugf("killing %d nodes", majority)
		killed := 0
		// Kill the leader node to force an election.
		for _, node := range nodes {
			if killed >= majority {
				break
			}
			timber.Warningf("killing node [%s]", node.NodeID())
			err := node.Stop()
			assert.NoError(t, err)
			killed++
		}

		// Wait a short amount of time for a new leader
		time.Sleep(5 * time.Second)

		var n barge
		for _, node := range nodes {
			if node.IsStopped() {
				continue
			}
			n = node
			break
		}

		//noinspection GoNilness
		txn, err := n.Begin()
		assert.NoError(t, err)

		err = txn.Set([]byte("test"), []byte("value"))
		assert.NoError(t, err)
		err = txn.Commit()
		assert.Error(t, err)
	})

	t.Run("minority failure", func(t *testing.T) {
		numberOfNodes := 9

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

		minority := int(math.Round(float64(numberOfNodes)/2) - 1)
		timber.Debugf("killing %d nodes", minority)
		killed := 0
		// Kill the leader node to force an election.
		for _, node := range nodes {
			if killed >= minority {
				break
			}
			timber.Warningf("killing node [%s]", node.NodeID())
			err := node.Stop()
			assert.NoError(t, err)
			killed++
		}

		// Wait a short amount of time for a new leader
		time.Sleep(5 * time.Second)

		var n barge
		for _, node := range nodes {
			if node.IsStopped() {
				continue
			}
			n = node
			break
		}

		// time.Sleep(5 * time.Second)

		VerifyLeader(t, nodes...)

		//noinspection GoNilness
		txn, err := n.Begin()
		assert.NoError(t, err)

		err = txn.Set([]byte("test"), []byte("value"))
		assert.NoError(t, err)
		err = txn.Commit()
		assert.NoError(t, err)
	})

	t.Run("leader write", func(t *testing.T) {
		numberOfNodes := 9

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

		for i, node := range nodes {
			if !node.IsLeader() {
				continue
			}

			tx, err := node.Begin()
			assert.NoError(t, err)

			key, value := []byte(fmt.Sprintf("test_%d", i)), []byte("value")
			err = tx.Set(key, value)
			assert.NoError(t, err)

			err = tx.Commit()
			assert.NoError(t, err)

			tx, err = node.Begin()
			assert.NoError(t, err)

			readValue, err := tx.Get(key)
			assert.NoError(t, err)
			assert.NotEmpty(t, readValue)
			assert.Equal(t, value, readValue)
		}
	})

	t.Run("join", func(t *testing.T) {
		tempDir1, cleanup1 := testutils.NewTempDirectory(t)

		ln1, err := newTransport(":")
		assert.NoError(t, err)

		defer cleanup1()
		d1, err := newDistributor(ln1, &distOptions{
			Directory: tempDir1,
			Peers:     []string{ln1.Addr().String()},
		}, timber.With(timber.Keys{
			"test": t.Name(),
		}))
		assert.NoError(t, err)
		assert.NotNil(t, d1)

		err = d1.Start()
		assert.NoError(t, err)

		time.Sleep(time.Second * 5)

		tempDir2, cleanup2 := testutils.NewTempDirectory(t)

		ln2, err := newTransport(":")
		assert.NoError(t, err)

		defer cleanup2()
		d2, err := newDistributor(ln2, &distOptions{
			Directory: tempDir2,
			Peers:     []string{ln1.Addr().String(), ln2.Addr().String()},
		}, timber.With(timber.Keys{
			"test": t.Name(),
		}))
		assert.NoError(t, err)
		assert.NotNil(t, d1)

		err = d2.Start()
		assert.NoError(t, err)

		time.Sleep(time.Second * 5)

		VerifyLeader(t, d1, d2)
	})
}
