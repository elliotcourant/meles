package meles

import (
	"encoding/binary"
	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
	"time"
)

type raftLogStore struct {
	db *badger.DB
}

func (r *raftLogStore) FirstIndex() (uint64, error) {
	return r.index(false)
}

func (r *raftLogStore) LastIndex() (uint64, error) {
	return r.index(true)
}

func (r *raftLogStore) GetLog(index uint64, rl *raft.Log) error {
	l := &log{}
	err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(r.getKeyForIndex(index))
		if err != nil {
			return raft.ErrLogNotFound
		}
		v := make([]byte, 0)
		_, err = item.ValueCopy(v)
		if err != nil {
			return err
		}
		return l.Decode(v)
	})
	*rl = l.Log
	return err
}

func (r *raftLogStore) StoreLog(log *raft.Log) error {
	return r.StoreLogs([]*raft.Log{log})
}

func (r *raftLogStore) StoreLogs(logs []*raft.Log) error {
	txn := r.db.NewTransactionAt(uint64(time.Now().UnixNano()), true)
	defer txn.Discard()
	for _, raftLog := range logs {
		log := newLogFromRaft(raftLog)
		if err := txn.Set(log.Path(), log.Encode()); err != nil {
			return err
		}
	}
	return txn.CommitAt(uint64(time.Now().UnixNano()), nil)
}

func (r *raftLogStore) DeleteRange(min, max uint64) error {
	return r.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
		})
		defer it.Close()
		start := r.getKeyForIndex(min)
		keys := make([][]byte, 0)

		for it.Seek(start); it.Valid(); it.Next() {
			k := make([]byte, 0)
			it.Item().KeyCopy(k)
			index := r.getIndexForKey(k)
			if index > max {
				break
			}
			keys = append(keys, k)
		}

		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *raftLogStore) index(reverse bool) (val uint64, err error) {
	val = 0
	err = r.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: false,
			Reverse:        reverse,
		})
		defer it.Close()
		k := make([]byte, r.getKeySize())
		for it.Seek(r.getPrefix()); it.ValidForPrefix(r.getPrefix()); {
			it.Item().KeyCopy(k)
			val = r.getIndexForKey(k)
			return nil
		}
		return nil
	})
	return val, err
}

func (r raftLogStore) getPrefix() []byte {
	return []byte{metaPrefix_Log}
}

func (r raftLogStore) getIndexForKey(key []byte) uint64 {
	k := key[len(r.getPrefix()):]
	if len(k) == 8 {
		return binary.BigEndian.Uint64(k)
	}
	return 0
}

func (r raftLogStore) getKeyForIndex(index uint64) []byte {
	k := r.getPrefix()
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, index)
	k = append(k, b...)
	return k
}

func (r raftLogStore) getKeySize() int {
	return len(r.getPrefix()) + 8
}
