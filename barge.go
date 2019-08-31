package meles

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/hashicorp/raft"
	"sync"
	"time"
)

var (
	ErrStopped = fmt.Errorf("barge has been stopped")
)

type Encoder interface {
	Encode() []byte
}

type Decoder interface {
	Decode(src []byte) error
}

type barge interface {
	Start() error
	WaitForLeader(timeout time.Duration) (string, bool, error)
	IsLeader() bool
	Begin() (transaction, error)
	BeginAt(timestamp time.Time) (transaction, error)
	Stop() error
	IsStopped() bool
	NodeID() raft.ServerID
	NextIncrementId(objectPath []byte) (uint64, error)
	NextSequenceValueById(sequenceName string) (uint64, error)
}

func (r *boat) Begin() (transaction, error) {
	return r.BeginAt(time.Now())
}

func (r *boat) BeginAt(timestamp time.Time) (transaction, error) {
	if r.IsStopped() {
		return nil, ErrStopped
	}
	ts := uint64(timestamp.UnixNano())
	return &transactionBase{
		timestamp:     ts,
		txn:           r.db.NewTransactionAt(ts, true),
		boat:          r,
		pendingWrites: map[string][]byte{},
	}, nil
}

type iterator interface {
	Close()
	Item() *badger.Item
	Next()
	Valid() bool
	Seek(key []byte)
	ValidForPrefix(prefix []byte) bool
	Rewind()
}

type iteratorBase struct {
	closed     bool
	closedSync sync.RWMutex
	itr        *badger.Iterator
}

func (i *iteratorBase) Close() {
	i.closedSync.Lock()
	defer i.closedSync.Unlock()
	i.closed = true
	i.itr.Close()
}

func (i *iteratorBase) Item() *badger.Item {
	return i.itr.Item()
}

func (i *iteratorBase) Next() {
	i.itr.Next()
}

func (i *iteratorBase) Valid() bool {
	return i.itr.Valid()
}

func (i *iteratorBase) Seek(key []byte) {
	i.itr.Seek(key)
}

func (i *iteratorBase) ValidForPrefix(prefix []byte) bool {
	return i.itr.ValidForPrefix(prefix)
}

func (i *iteratorBase) Rewind() {
	i.itr.Rewind()
}

type transaction interface {
	Get(key []byte) ([]byte, error)
	GetEx(key []byte, value Decoder) error
	Set(key, value []byte) error
	Delete(key []byte) error

	GetKeyIterator(prefix []byte, keyOnly bool, reverse bool) iterator

	NextIncrementId(objectPath []byte) (uint64, error)

	Rollback() error
	Commit() error
	Discard()
}

type transactionBase struct {
	timestamp         uint64
	closed            bool
	closedSync        sync.RWMutex
	boat              *boat
	txn               *badger.Txn
	pendingWrites     map[string][]byte
	pendingWritesSync sync.RWMutex
}

func (t *transactionBase) Delete(key []byte) error {
	return t.Set(key, nil)
}

func (t *transactionBase) NextIncrementId(objectPath []byte) (uint64, error) {
	return t.boat.NextIncrementId(objectPath)
}

func (t *transactionBase) GetKeyIterator(prefix []byte, keyOnly bool, reverse bool) iterator {
	itr := t.txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: !keyOnly,
		Reverse:        reverse,
		Prefix:         prefix,
	})

	return &iteratorBase{
		itr: itr,
	}
}

func (t *transactionBase) Get(key []byte) ([]byte, error) {
	if t.isFinished() {
		return nil, fmt.Errorf("transactionBase closed")
	}
	item, err := t.txn.Get(key)
	if err != nil {
		return nil, err
	}
	val := make([]byte, item.ValueSize())
	return item.ValueCopy(val)
}

func (t *transactionBase) GetEx(key []byte, value Decoder) error {
	v, err := t.Get(key)
	if err != nil {
		return err
	}
	return value.Decode(v)
}

func (t *transactionBase) Set(key, val []byte) error {
	if t.isFinished() {
		return fmt.Errorf("transactionBase closed")
	}
	if val == nil {
		if err := t.txn.Delete(key); err != nil {
			return err
		}
	} else {
		if err := t.txn.Set(key, val); err != nil {
			return err
		}
	}
	t.addPendingWrite(key, val)
	return nil
}

func (t *transactionBase) Rollback() error {
	if t.isFinished() {
		return fmt.Errorf("transactionBase closed")
	}
	defer t.finishTransaction()
	t.txn.Discard()
	return nil
}

func (t *transactionBase) Commit() error {
	if t.boat.IsStopped() {
		return ErrStopped
	}
	startTime := time.Now()
	defer t.boat.logger.Verbosef("time to commit: %s", time.Since(startTime))
	if t.isFinished() {
		return fmt.Errorf("transactionBase closed")
	}
	defer t.finishTransaction()
	if t.getNumberOfPendingWrites() == 0 {
		return nil
	}
	rtx := transactionStorage{
		Timestamp: t.timestamp,
		Actions:   make([]action, 0),
	}
	t.pendingWritesSync.RLock()
	defer t.pendingWritesSync.RUnlock()
	t.boat.logger.Verbosef("preparing to commit %d pending write(s)", len(t.pendingWrites))
	for k, v := range t.pendingWrites {
		actionType := actionTypeSet
		if v == nil {
			actionType = actionTypeDelete
		}
		rtx.Actions = append(rtx.Actions, action{
			Type:  actionType,
			Key:   []byte(k),
			Value: v,
		})
	}
	rtx.Timestamp = uint64(time.Now().UnixNano())
	return t.boat.apply(rtx, t.txn)
}

func (t *transactionBase) Discard() {
	defer t.finishTransaction()
	t.txn.Discard()
}

func (t *transactionBase) addPendingWrite(key []byte, value []byte) {
	t.pendingWritesSync.Lock()
	defer t.pendingWritesSync.Unlock()
	t.pendingWrites[string(key)] = value
}

func (t *transactionBase) getNumberOfPendingWrites() uint32 {
	t.pendingWritesSync.RLock()
	defer t.pendingWritesSync.RUnlock()
	return uint32(len(t.pendingWrites))
}

func (t *transactionBase) finishTransaction() {
	t.closedSync.Lock()
	defer t.closedSync.Unlock()
	t.closed = true
	t.txn.Discard()
}

func (t *transactionBase) isFinished() bool {
	t.closedSync.RLock()
	defer t.closedSync.RUnlock()
	return t.closed
}
