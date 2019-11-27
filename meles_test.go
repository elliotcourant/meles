package meles_test

import (
	"github.com/ahmetb/go-linq/v3"
	"github.com/elliotcourant/meles"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func TestNewStore(t *testing.T) {
	store, cleanup := NewStore(t)
	defer cleanup()

	assert.NotEmpty(t, store.NodeID())
}

func TestConflict(t *testing.T) {
	t.Run("local conflict", func(t *testing.T) {
		store, cleanup := NewStore(t)
		defer cleanup()

		txn1, err := store.Begin()
		assert.NoError(t, err)

		txn2, err := store.Begin()
		assert.NoError(t, err)

		txn1.MustGet([]byte("test"))
		err = txn1.Set([]byte("test"), []byte("value one"))
		assert.NoError(t, err)

		txn2.MustGet([]byte("test"))
		err = txn2.Set([]byte("test"), []byte("value two"))
		assert.NoError(t, err)

		err = txn1.Commit()
		assert.NoError(t, err)

		err = txn2.Commit()
		assert.Error(t, err)

		txn, err := store.Begin()
		assert.NoError(t, err)

		value, _, _ := txn.Get([]byte("test"))
		assert.Equal(t, []byte("value one"), value)
	})

	t.Run("distributed conflict", func(t *testing.T) {
		cluster, cleanup := NewCluster(t, 9)
		defer cleanup()

		txn1, err := cluster[0].Begin()
		assert.NoError(t, err)

		txn2, err := cluster[1].Begin()
		assert.NoError(t, err)

		txn3, err := cluster[2].Begin()
		assert.NoError(t, err)

		txn1.MustGet([]byte("test"))
		err = txn1.Set([]byte("test"), []byte("value one"))
		assert.NoError(t, err)

		txn2.MustGet([]byte("test"))
		err = txn2.Set([]byte("test"), []byte("value two"))
		assert.NoError(t, err)

		txn3.MustGet([]byte("test"))
		err = txn3.Set([]byte("test"), []byte("value three"))
		assert.NoError(t, err)

		err = txn1.Commit()
		assert.NoError(t, err)

		// Instigate a conflict on multiple nodes
		err = txn2.Commit()
		assert.Error(t, err)

		err = txn3.Commit()
		assert.Error(t, err)

		cluster.VerifyValue(t, "test", "value one")
	})
}

func TestClustering(t *testing.T) {
	t.Run("small", func(t *testing.T) {
		cluster, cleanup := NewCluster(t, 3)
		defer cleanup()

		cluster.Set(t, "test", "value")
		cluster.VerifyValue(t, "test", "value")
	})

	t.Run("medium", func(t *testing.T) {
		cluster, cleanup := NewCluster(t, 9)
		defer cleanup()

		cluster.Set(t, "test", "value")
		cluster.VerifyValue(t, "test", "value")
	})

	t.Run("large", func(t *testing.T) {
		cluster, cleanup := NewCluster(t, 32)
		defer cleanup()

		cluster.Set(t, "test", "value")
		cluster.VerifyValue(t, "test", "value")
	})
}

func TestClustering_NonLeaderWrite(t *testing.T) {
	cluster, cleanup := NewCluster(t, 3)
	defer cleanup()

	nonLeader := linq.From(cluster).FirstWith(func(i interface{}) bool {
		node, ok := i.(*meles.Store)
		return ok && !node.IsLeader()
	}).(*meles.Store)

	txn, err := nonLeader.Begin()
	assert.NoError(t, err)

	err = txn.Set([]byte("test"), []byte("value"))
	assert.NoError(t, err)

	err = txn.Commit()
	assert.NoError(t, err)

	cluster.VerifyValue(t, "test", "value")
}

func TestClustering_Shutdown(t *testing.T) {
	t.Run("shutdown follower", func(t *testing.T) {
		cluster, cleanup := NewCluster(t, 5)
		defer cleanup()

		// Get a node that is not the leader
		randomNode := cluster[rand.Int()%len(cluster)]
		for randomNode.IsLeader() {
			randomNode = cluster[rand.Int()%len(cluster)]
		}

		err := randomNode.Stop()
		assert.NoError(t, err)

		cluster.VerifyLeader(t)
	})

	t.Run("shutdown leader", func(t *testing.T) {
		cluster, cleanup := NewCluster(t, 5)
		defer cleanup()

		// Get a node that is not the leader
		randomNode := cluster[rand.Int()%len(cluster)]
		for !randomNode.IsLeader() {
			randomNode = cluster[rand.Int()%len(cluster)]
		}

		err := randomNode.Stop()
		assert.NoError(t, err)

		cluster.VerifyLeader(t)
	})
}

func TestFailure(t *testing.T) {
	t.Run("minority failure", func(t *testing.T) {
		cluster, cleanup := NewCluster(t, 5)
		defer cleanup()

		// Write something to the cluster
		cluster.Set(t, "test", "initial value")
		cluster.VerifyValue(t, "test", "initial value")

		// Kill a minority of the nodes in the cluster.
		minority := len(cluster) / 2
		for i := 0; i < minority; i++ {
			err := cluster[i].Stop()
			assert.NoError(t, err)
		}

		// Make sure a new leader is elected.
		cluster.VerifyLeader(t)

		// Update the initial value
		cluster.Set(t, "test", "new value")
		cluster.VerifyValue(t, "test", "new value")
	})

	t.Run("majority failure", func(t *testing.T) {
		cluster, cleanup := NewCluster(t, 5)
		defer cleanup()

		// Write something to the cluster
		cluster.Set(t, "test", "initial value")
		cluster.VerifyValue(t, "test", "initial value")

		// Kill a majority of the nodes in the cluster.
		minority := (len(cluster) / 2) + 1
		for i := 0; i < minority; i++ {
			err := cluster[i].Stop()
			assert.NoError(t, err)
		}

		{
			// Try to update the value
			randomLiveNode := cluster.RandomNode()

			txn, err := randomLiveNode.Begin()
			assert.NoError(t, err)

			err = txn.Set([]byte("test"), []byte("new value"))
			assert.NoError(t, err)

			err = txn.Commit()
			assert.Error(t, err)
		}

		cluster.VerifyValue(t, "test", "initial value")
	})
}

func TestJoin(t *testing.T) {
	t.Run("join one node", func(t *testing.T) {
		node1, cleanup1 := NewStore(t)
		defer cleanup1()

		// Start the second node and give it the first node
		// address as a peer.
		node2, cleanup2 := NewStore(t, node1.Address())
		defer cleanup2()

		// Turn the two nodes into a cluster.
		cluster := TestCluster{node1, node2}

		// Make sure that the two nodes agree on a leader
		cluster.VerifyLeader(t)

		// Set a value somewhere in the cluster.
		cluster.Set(t, "test", "value")

		// Make sure that both nodes have the same value
		cluster.VerifyValue(t, "test", "value")
	})

	t.Run("join cluster", func(t *testing.T) {
		initialCluster, cleanup1 := NewCluster(t, 3)
		defer cleanup1()

		// Join using a random node in the cluster.
		newNode, cleanupNew := NewStore(t, initialCluster.RandomNode().Address())
		defer cleanupNew()

		cluster := append(initialCluster, newNode)

		// Make sure that the two nodes agree on a leader
		cluster.VerifyLeader(t)

		// Set a value somewhere in the cluster.
		cluster.Set(t, "test", "value")

		// Make sure that both nodes have the same value
		cluster.VerifyValue(t, "test", "value")
	})
}
