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
