package testutils

import (
	"github.com/dgraph-io/badger"
	"github.com/stretchr/testify/assert"
)

func NewDB(t assert.TestingT) (*badger.DB, CleanupFunction) {
	tempDir, cleanup := NewTempDirectory(t)
	// Open the Badger database located in the /tmp/badger directory.
	// It will be created if it doesn't exist.
	db, err := badger.Open(badger.DefaultOptions(tempDir))
	if !assert.NoError(t, err) {
		panic(err)
	}
	return db, func() {
		defer cleanup()
		if err := db.Close(); !assert.NoError(t, err) {
			panic(err)
		}
	}
}
