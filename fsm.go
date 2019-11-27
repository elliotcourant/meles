package meles

import (
	"bytes"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"io"
	"time"
)

type raftFsmStore struct {
	db     *badger.DB
	logger timber.Logger
}

func (r *raftFsmStore) Apply(log *raft.Log) interface{} {
	var transaction transactionStorage
	if err := transaction.Decode(log.Data); err != nil {
		return err
	}
	delay := time.Unix(0, int64(transaction.Timestamp))
	now := time.Now()
	defer r.logger.Verbosef("apply transactionBase time: %s", time.Since(now))
	r.logger.Verbosef("applying transactionBase, delayed: %s", time.Since(delay))
	txn := r.db.NewTransactionAt(transaction.Timestamp, true)
	defer txn.Discard()

	for i := 0; i < 2; i++ {
		for _, action := range transaction.Actions {
			switch action.Type {
			case actionTypeSet:
				if i == 0 {
					continue
				}
				if err := txn.Set(action.Key, action.Value); err != nil {
					return err
				}
			case actionTypeGet:
				if i > 0 {
					continue
				}
				if _, err := txn.Get(action.Key); err != nil && err != badger.ErrKeyNotFound {
					return err
				}
			case actionTypeDelete:
				if i == 0 {
					continue
				}
				if err := txn.Delete(action.Key); err != nil {
					return err
				}
			}
		}
	}

	return txn.CommitAt(transaction.CommitTimestamp, nil)
}

func (r *raftFsmStore) Restore(rc io.ReadCloser) error {
	return r.db.Load(rc, 8)
}

func (r *raftFsmStore) Snapshot() (raft.FSMSnapshot, error) {
	w := bytes.NewBuffer(nil)
	if _, err := r.db.Backup(w, 0); err != nil {
		return nil, err
	}
	return &raftSnapshot{data: w.Bytes()}, nil
}
