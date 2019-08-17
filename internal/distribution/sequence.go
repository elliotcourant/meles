package distribution

import (
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/meles/internal/storage"
	"github.com/elliotcourant/timber"
	"sync"
)

type ObjectSequence struct {
	sync.Mutex
	barge     Barge
	objSeq    *storage.ObjectSequence
	key       []byte
	leased    uint8
	bandwidth uint8
}

func (r *boat) GetObjectSequence(key []byte, bandwidth uint8) (*ObjectSequence, error) {
	switch {
	case len(key) == 0:
		return nil, badger.ErrEmptyKey
	case bandwidth == 0:
		return nil, badger.ErrZeroBandwidth
	}

	seq := &ObjectSequence{
		barge: r,
		key:   key,
		objSeq: &storage.ObjectSequence{
			Key: key,
		},
		leased:    0,
		bandwidth: bandwidth,
	}
	err := seq.updateLease()
	return seq, err
}

func (s *ObjectSequence) updateLease() error {
	tx, err := Begin()
	if err != nil {
		return err
	}

	err = Get(s.objSeq.Path(), s.objSeq)
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

	storeSeq := &storage.ObjectSequence{
		Key:  s.key,
		Next: lease,
	}

	err = Set(storeSeq.Path(), storeSeq)
	if err != nil {
		return err
	}
	err = Commit()
	if err != nil {
		return err
	}

	s.leased = lease
	return nil
}

func (s *ObjectSequence) Next() (uint8, error) {
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
