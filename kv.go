package meles

import (
	"bytes"
	"github.com/dgraph-io/badger"
)

type kvStore interface {
	Begin() kvTransaction
	GetSnapshot() ([]byte, error)
}

type kvTransaction interface {
}

type kv struct {
	db *badger.DB
}

type tx struct {
	tx *badger.Txn
}

func newKeyValueStore(dir string, logger badger.Logger) (kvStore, error) {
	options := badger.DefaultOptions(dir)
	options.Logger = logger
	db, err := badger.OpenManaged(options)
	if err != nil {
		return nil, err
	}

	return &kv{
		db: db,
	}, nil
}

func (k *kv) Begin() kvTransaction {
	return &tx{
		tx: k.db.NewTransaction(true),
	}
}

func (k *kv) GetSnapshot() ([]byte, error) {
	backupWriter := bytes.NewBuffer(nil)
	_, err := k.db.Backup(backupWriter, 0)
	return backupWriter.Bytes(), err
}

func (t *tx) Set(key, value []byte) error {
	return nil
}
