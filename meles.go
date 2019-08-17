package meles

import (
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/timber"
	"net"
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

func (s *Store) Begin() (*Transaction, error) {
	txn, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	return &Transaction{
		txn: txn,
	}, nil
}

type Transaction struct {
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
