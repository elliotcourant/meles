package distribution

import (
	"encoding/binary"
	"github.com/dgraph-io/badger"
)

type raftStableStore struct {
	db *badger.DB
}

func (r raftStableStore) getKey(key []byte) []byte {
	return append([]byte{StableStorePrefix}, key...)
}

func (r *raftStableStore) Set(key []byte, val []byte) error {
	return r.db.Update(func(txn *badger.Txn) error {
		return txn.Set(r.getKey(key), val)
	})
}

func (r *raftStableStore) Get(key []byte) (dst []byte, err error) {
	err = r.db.View(func(txn *badger.Txn) error {
		v, e := txn.Get(r.getKey(key))
		if e == badger.ErrKeyNotFound {
			return ErrNotFound
		} else if e != nil {
			return e
		}
		_, e = v.ValueCopy(dst)
		return e
	})
	return dst, err
}

func (r *raftStableStore) SetUint64(key []byte, val uint64) error {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, val)
	return r.Set(key, v)
}

func (r *raftStableStore) GetUint64(key []byte) (uint64, error) {
	v, err := r.Get(key)
	if err != nil || v == nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(v), nil
}
