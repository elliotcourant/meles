package distribution

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/meles/internal/storage"
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"sync"
	"time"
)

var (
	ErrBargeStopped = fmt.Errorf("barge has been stopped")
)

type Encoder interface {
	Encode() []byte
}

type Decoder interface {
	Decode(src []byte) error
}

type Barge interface {
	Start() error
	WaitForLeader(timeout time.Duration) (string, bool, error)
	IsLeader() bool
	Begin() (Transaction, error)
	Stop() error
	IsStopped() bool
	NodeID() raft.ServerID
	NextObjectID(objectPath []byte) (uint8, error)
}

func (r *boat) Begin() (Transaction, error) {
	if IsStopped() {
		return nil, ErrBargeStopped
	}
	return &transaction{
		txn:           r.db.NewTransaction(true),
		boat:          r,
		pendingWrites: map[string][]byte{},
	}, nil
}

type Iterator interface {
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

type Transaction interface {
	Get(key []byte, value Decoder) error
	Set(key []byte, value Encoder) error
	SetRaw(key []byte, value []byte) error
	Delete(key []byte) error

	GetKeyIterator(prefix []byte, keyOnly bool, reverse bool) Iterator

	NextObjectID(objectPath []byte) (uint8, error)

	Rollback() error
	Commit() error
}

type transaction struct {
	closed            bool
	closedSync        sync.RWMutex
	boat              *boat
	txn               *badger.Txn
	pendingWrites     map[string][]byte
	pendingWritesSync sync.RWMutex
}

func (t *transaction) Delete(key []byte) error {
	return t.SetRaw(key, nil)
}

func (t *transaction) NextObjectID(objectPath []byte) (uint8, error) {
	return NextObjectID(objectPath)
}

func (t *transaction) GetKeyIterator(prefix []byte, keyOnly bool, reverse bool) Iterator {
	itr := t.txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: !keyOnly,
		Reverse:        reverse,
		Prefix:         prefix,
	})

	return &iteratorBase{
		itr: itr,
	}
}

func (t *transaction) Get(key []byte, value Decoder) error {
	if t.isFinished() {
		return fmt.Errorf("transaction closed")
	}
	item, err := t.txn.Get(key)
	if err != nil {
		return err
	}
	val := make([]byte, item.ValueSize())
	val, err = item.ValueCopy(val)
	if err != nil {
		return err
	}
	return value.Decode(val)
}

func (t *transaction) Set(key []byte, value Encoder) error {
	return t.SetRaw(key, value.Encode())
}

func (t *transaction) SetRaw(key, val []byte) error {
	if t.isFinished() {
		return fmt.Errorf("transaction closed")
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

func (t *transaction) Rollback() error {
	if t.isFinished() {
		return fmt.Errorf("transaction closed")
	}
	defer t.finishTransaction()
	t.txn.Discard()
	return nil
}

func (t *transaction) Commit() error {
	if IsStopped() {
		return ErrBargeStopped
	}
	startTime := time.Now()
	defer timber.Verbosef("time to commit: %s", time.Since(startTime))
	if t.isFinished() {
		return fmt.Errorf("transaction closed")
	}
	defer t.finishTransaction()
	if t.getNumberOfPendingWrites() == 0 {
		return nil
	}
	rtx := storage.Transaction{
		Timestamp: uint64(time.Now().UTC().UnixNano()),
		Actions:   make([]storage.Action, 0),
	}
	t.pendingWritesSync.RLock()
	defer t.pendingWritesSync.RUnlock()
	timber.Verbosef("preparing to commit %d pending write(s)", len(t.pendingWrites))
	for k, v := range t.pendingWrites {
		actionType := storage.ActionTypeSet
		if v == nil {
			actionType = storage.ActionTypeDelete
		}
		rtx.Actions = append(rtx.Actions, storage.Action{
			Type:  actionType,
			Key:   []byte(k),
			Value: v,
		})
	}
	return apply(rtx, t.txn)
}

func (t *transaction) addPendingWrite(key []byte, value []byte) {
	t.pendingWritesSync.Lock()
	defer t.pendingWritesSync.Unlock()
	t.pendingWrites[string(key)] = value
}

func (t *transaction) getNumberOfPendingWrites() uint32 {
	t.pendingWritesSync.RLock()
	defer t.pendingWritesSync.RUnlock()
	return uint32(len(t.pendingWrites))
}

func (t *transaction) finishTransaction() {
	t.closedSync.Lock()
	defer t.closedSync.Unlock()
	t.closed = true
	t.txn.Discard()
}

func (t *transaction) isFinished() bool {
	t.closedSync.RLock()
	defer t.closedSync.RUnlock()
	return t.closed
}
