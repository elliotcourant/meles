package meles

import (
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/meles/logger"
	"github.com/elliotcourant/meles/testutils"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

// TestPointInTimeTransaction is a proof of concept that
// we can use a unix timestamp as a transaction Id.
func TestPointInTimeTransaction(t *testing.T) {
	dir, cleanup := testutils.NewTempDirectory(t)
	defer cleanup()
	opts := badger.DefaultOptions(dir).WithLogger(logger.NewBadgerLogger(timber.With(timber.Keys{
		"test": t.Name(),
	})))
	db, err := badger.OpenManaged(opts)
	assert.NoError(t, err)
	defer db.Close()

	t.Run("snapshot", func(t *testing.T) {
		ts := uint64(time.Now().UnixNano())
		key := []byte("test")

		txn := db.NewTransactionAt(ts, true)
		err = txn.Set(key, []byte("value"))
		assert.NoError(t, err)

		err = txn.CommitAt(ts, nil)
		assert.NoError(t, err)

		txn = db.NewTransactionAt(ts, true)
		err = txn.Set(key, []byte("new value"))
		assert.NoError(t, err)

		err = txn.CommitAt(ts, nil)
		assert.NoError(t, err)

		txn = db.NewTransactionAt(uint64(time.Now().UnixNano()), true)
		item, err := txn.Get(key)
		assert.NoError(t, err)
		err = item.Value(func(val []byte) error {
			assert.Equal(t, []byte("new value"), val)
			return nil
		})
		assert.NoError(t, err)
		txn.Discard()
	})
}
