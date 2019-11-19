package meles

import (
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"net"
	"time"
)

type Options struct {
	Directory string
	Peers     []string
}

type Store struct {
	db barge
}

func NewStore(listener net.Listener, logger timber.Logger, options Options) (*Store, error) {
	opt := &distOptions{
		Directory:      options.Directory,
		Peers:          options.Peers,
		NumericNodeIds: true,
	}
	db, err := newDistributor(listener, opt, logger)
	if err != nil {
		return nil, err
	}
	return &Store{
		db: db,
	}, nil
}

func (s *Store) Start() error {
	return s.db.Start()
}

func (s *Store) Stop() error {
	return s.db.Stop()
}

func (s *Store) NodeID() raft.ServerID {
	return s.db.NodeID()
}

func (s *Store) IsLeader() bool {
	return s.db.IsLeader()
}

func (s *Store) WaitForLeader(timeout time.Duration) (leaderAddress string, hasLeader bool, err error) {
	return s.db.WaitForLeader(timeout)
}

func (s *Store) IsStopped() bool {
	return s.db.IsStopped()
}

func (s *Store) NextIncrementId(objectPath []byte) (uint64, error) {
	return s.db.NextIncrementId(objectPath)
}

func (s *Store) NextSequenceId(sequenceName string) (uint64, error) {
	return s.db.NextSequenceValueById(sequenceName)
}

func (s *Store) Begin() (*Transaction, error) {
	return s.BeginAt(time.Now())
}

func (s *Store) BeginAt(timestamp time.Time) (*Transaction, error) {
	txn, err := s.db.BeginAt(timestamp)
	if err != nil {
		return nil, err
	}
	return &Transaction{
		db:  s.db,
		txn: txn,
	}, nil
}

type Transaction struct {
	db  barge
	txn transaction
}

func (s *Transaction) Get(key []byte) ([]byte, bool, error) {
	val, err := s.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (s *Transaction) MustGet(key []byte) ([]byte, bool, error) {
	val, err := s.txn.MustGet(key)
	if err == badger.ErrKeyNotFound {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (s *Transaction) Delete(key []byte) error {
	return s.txn.Set(key, nil)
}

func (s *Transaction) Set(key, value []byte) error {
	return s.txn.Set(key, value)
}

func (s *Transaction) Commit() error {
	return s.txn.Commit()
}

func (s *Transaction) Rollback() error {
	return s.txn.Rollback()
}

func (s *Transaction) NextIncrementId(objectPath []byte) (uint64, error) {
	return s.txn.NextIncrementId(objectPath)
}

func (s *Transaction) NextSequenceId(sequenceName string) (uint64, error) {
	return s.db.NextSequenceValueById(sequenceName)
}

func (s *Transaction) GetIterator(prefix []byte, keyOnly bool, reverse bool) *Iterator {
	return &Iterator{
		i: s.txn.GetKeyIterator(prefix, keyOnly, reverse),
	}
}

func (s *Transaction) NodeID() raft.ServerID {
	return s.db.NodeID()
}

func (s *Transaction) IsLeader() bool {
	return s.db.IsLeader()
}

func (s *Transaction) WaitForLeader(timeout time.Duration) (leaderAddress string, hasLeader bool, err error) {
	return s.db.WaitForLeader(timeout)
}

func (s *Transaction) IsStopped() bool {
	return s.db.IsStopped()
}

type Iterator struct {
	i iterator
}

func (i *Iterator) Close() {
	i.i.Close()
}

func (i *Iterator) Item() *badger.Item {
	return i.i.Item()
}

func (i *Iterator) Next() {
	i.i.Next()
}

func (i *Iterator) Valid() bool {
	return i.i.Valid()
}

func (i *Iterator) Seek(key []byte) {
	i.i.Seek(key)
}

func (i *Iterator) ValidForPrefix(prefix []byte) bool {
	return i.i.ValidForPrefix(prefix)
}

func (i *Iterator) Rewind() {
	i.i.Rewind()
}
