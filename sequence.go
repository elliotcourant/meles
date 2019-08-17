package meles

import (
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/timber"
	"sync"
)

type incrementSequence struct {
	sync.Mutex
	barge     barge
	objSeq    *incrementSequenceStorage
	key       []byte
	leased    uint8
	bandwidth uint8
}

func (r *boat) GetObjectSequence(key []byte, bandwidth uint8) (*incrementSequence, error) {
	switch {
	case len(key) == 0:
		return nil, badger.ErrEmptyKey
	case bandwidth == 0:
		return nil, badger.ErrZeroBandwidth
	}

	seq := &incrementSequence{
		barge: r,
		key:   key,
		objSeq: &incrementSequenceStorage{
			Key: key,
		},
		leased:    0,
		bandwidth: bandwidth,
	}
	err := seq.updateLease()
	return seq, err
}

func (s *incrementSequence) updateLease() error {
	tx, err := s.barge.Begin()
	if err != nil {
		return err
	}

	err = tx.GetEx(s.objSeq.Path(), s.objSeq)
	if err == badger.ErrKeyNotFound {
		if s.objSeq.Next > 0 {
			timber.Warningf("sequence [%s] is missing, current val [%d] could not find stored val", s.key, s.objSeq.Next)
		}
		s.objSeq.Next = 0
	} else if err != nil {
		return err
	}

	s.objSeq.Key = s.key

	lease := s.objSeq.Next + s.bandwidth

	storeSeq := &incrementSequenceStorage{
		Key:  s.key,
		Next: lease,
	}

	err = tx.Set(storeSeq.Path(), storeSeq.Encode())
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	s.leased = lease
	return nil
}

func (s *incrementSequence) Next() (uint8, error) {
	s.Lock()
	defer s.Unlock()
	if s.objSeq.Next >= s.leased {
		if err := s.updateLease(); err != nil {
			return 0, err
		}
	}
	s.objSeq.Next++
	val := s.objSeq.Next
	return val, nil
}
