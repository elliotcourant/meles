package meles_test

import (
	"github.com/elliotcourant/meles"
	"github.com/elliotcourant/meles/testutils"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	c := m.Run()
	os.Exit(c)
}

type TestCluster []*meles.Store

func (c TestCluster) RandomNode() *meles.Store {
	node := c[rand.Int()%len(c)]
	for node.IsStopped() {
		node = c[rand.Int()%len(c)]
	}

	return node
}

func (c TestCluster) Set(t *testing.T, key, value string) {
	node := c.RandomNode()

	txn, err := node.Begin()
	if !assert.NoError(t, err, "could not start transaction on node [%s]", node.NodeID()) {
		panic(err)
	}

	if err = txn.Set([]byte(key), []byte(value)); !assert.NoError(t, err, "could not set key-value on node [%s]", node.NodeID()) {
		panic(err)
	}

	if err = txn.Commit(); !assert.NoError(t, err, "could not commit on node [%s]", node.NodeID()) {
		panic(err)
	}
}

func (c TestCluster) VerifyLeader(t *testing.T) {
	time.Sleep(2 * time.Second)
	timber.Infof("verifying leader for %d node(s)", len(c))
	start := time.Now()
	maxRetry := 5
	retries := 0
TryAgain:
	leaderAddr := ""
	for _, node := range c {
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

func (c TestCluster) VerifyValue(t *testing.T, key, value string) {
	timber.Infof("verifying value for [%s] for %d node(s)", string(key), len(c))
	start := time.Now()
	maxRetry := 5
	retries := 0
TryAgain:
	for _, node := range c {
		if node.IsStopped() {
			continue
		}
		txn, err := node.Begin()
		assert.NoError(t, err)

		v, ok, err := txn.Get([]byte(key))
		assert.NoError(t, err)
		if (!ok || string(v) != value) && retries < maxRetry {
			time.Sleep(5 * time.Second)
			retries++
			goto TryAgain
		} else {
			assert.True(t, ok, "expected to find key [%s] on node [%s]", key, node.NodeID())
			assert.Equal(t, value, string(v), "expected value to match for key [%s] on node [%s]", key, node.NodeID())
		}
	}
	timber.Infof("value verification for [%s] time: %s retries: %d", key, time.Since(start), retries)
}

func NewStore(t *testing.T, peers ...string) (*meles.Store, func()) {
	tempDir, cleanup := testutils.NewTempDirectory(t)
	log := timber.With(timber.Keys{
		"test": t.Name(),
	})

	listener, err := net.Listen("tcp", ":")
	assert.NoError(t, err)

	store, err := meles.NewStore(listener, log, meles.Options{
		Directory: tempDir,
		Peers:     peers,
	})
	assert.NoError(t, err)

	err = store.Start()
	assert.NoError(t, err)

	assert.False(t, store.IsStopped())

	if len(peers) == 0 {
		assert.True(t, store.IsLeader())
	}

	return store, func() {
		store.Stop()
		cleanup()
		listener.Close()
	}
}

func NewCluster(t *testing.T, numberOfPeers int) (TestCluster, func()) {
	logger := timber.With(timber.Keys{
		"test": t.Name(),
	})

	listeners := make([]net.Listener, numberOfPeers)
	peers := make([]string, numberOfPeers)
	dirs := make([]string, numberOfPeers)

	for i := 0; i < numberOfPeers; i++ {
		{ // Listener
			listener, err := net.Listen("tcp", ":")
			assert.NoError(t, err)
			assert.NotNil(t, listener)
			listeners[i] = listener
			peers[i] = listener.Addr().String()
		}

		{ // Temp directory
			tmpdir, err := ioutil.TempDir("", "meles")
			assert.NoError(t, err)
			dirs[i] = tmpdir
		}
	}

	nodes := make(TestCluster, numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		log := logger.With(timber.Keys{
			"peer": i,
		})
		store, err := meles.NewStore(
			listeners[i],
			log,
			meles.Options{
				Directory: dirs[i],
				Peers:     peers,
			},
		)
		assert.NoError(t, err)

		nodes[i] = store
	}

	wg := sync.WaitGroup{}
	wg.Add(numberOfPeers)
	for i := 0; i < numberOfPeers; i++ {
		go func(i int) {
			defer wg.Done()
			err := nodes[i].Start()
			assert.NoError(t, err, "failed to start node [%d]", i)
		}(i)
	}
	wg.Wait()

	nodes.VerifyLeader(t)

	return nodes, func() {
		for i := 0; i < numberOfPeers; i++ {
			if nodes[i].IsStopped() {
				continue
			}
			nodes[i].Stop()
			listeners[i].Close()
			os.RemoveAll(dirs[i])
		}
	}
}
