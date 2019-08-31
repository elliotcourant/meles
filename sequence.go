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
	leased    uint64
	bandwidth uint64
}

func (r *boat) GetObjectSequence(key []byte, bandwidth uint64) (*incrementSequence, error) {
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

func (s *incrementSequence) Next() (uint64, error) {
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

const (
	SequenceRangeSize   = 1000
	SequencePartitions  = 5
	SequencePreretrieve = 50
)

type distSequenceChunk struct {
	sequenceName string
	start        uint64
	end          uint64
	offset       uint64
	count        uint64
}

type distSequence struct {
	db           *boat
	current      *distSequenceChunk
	next         *distSequenceChunk
	index        uint64
	sync         sync.Mutex
	sequenceName string
}

func (r *boat) NextSequenceValueById(sequenceName string) (uint64, error) {
	r.distSequenceSync.Lock()
	defer r.distSequenceSync.Unlock()
	sequence, ok := r.distSequences[sequenceName]
	if !ok {
		sequence = &distSequence{
			current:      nil,
			next:         nil,
			index:        1,
			sequenceName: sequenceName,
			db:           r,
		}
		r.distSequences[sequenceName] = sequence
	}
	return sequence.Next()
}

func (r *boat) SequenceIndexById(sequenceName string) (uint64, error) {
	r.distSequenceSync.Lock()
	defer r.distSequenceSync.Unlock()
	sequence, ok := r.distSequences[sequenceName]
	if !ok {
		sequence = &distSequence{
			current:      nil,
			next:         nil,
			index:        1,
			sequenceName: sequenceName,
			db:           r,
		}
		r.distSequences[sequenceName] = sequence
	}
	return sequence.GetSequenceIndex(), nil
}

func (r *boat) GetSequenceChunk(sequenceName string) (*distSequenceChunk, error) {
	leaderAddr, amLeader, err := r.waitForAmILeader(leaderWaitTimeout)
	if err != nil {
		return nil, err
	}
	if !amLeader {
		r.logger.Verbosef("redirecting dist sequence command to leader [%s]", leaderAddr)
		c, err := r.newRpcConnectionTo(leaderAddr)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		return c.NextSequenceChunk(sequenceName)
	}

	path := append([]byte{metaPrefix_DistributedSequence}, sequenceName...)

	txn, err := r.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Discard()

	r.distCacheSync.Lock()
	defer r.distCacheSync.Unlock()
	cache, ok := r.distCache[sequenceName]
	if !ok {
		seqVal, err := txn.Get(path)
		if err == badger.ErrKeyNotFound {
			cache = &distSequenceCache{
				currentValue:       0,
				lastPartitionIndex: 0,
				maxPartitionIndex:  SequencePartitions - 1,
				partitions:         SequencePartitions,
			}
		} else if err != nil {
			return nil, err
		} else {
			if err := cache.Decode(seqVal); err != nil {
				return nil, err
			}
		}
		r.distCache[sequenceName] = cache
	}
	if cache.lastPartitionIndex >= cache.maxPartitionIndex {
		cache.currentValue += SequenceRangeSize
		cache.lastPartitionIndex = 0
		cache.maxPartitionIndex = SequencePartitions - 1
	}
	index := cache.lastPartitionIndex
	cache.lastPartitionIndex++
	err = txn.Set(path, cache.Encode())
	if err != nil {
		return nil, err
	}
	return &distSequenceChunk{
		start:  cache.currentValue,
		end:    cache.currentValue + SequenceRangeSize,
		offset: index,
		count:  cache.maxPartitionIndex,
	}, txn.Commit()
}

func (s *distSequence) GetSequenceIndex() uint64 {
	return s.index
}

func (s *distSequence) Next() (uint64, error) {
	s.sync.Lock()
	defer s.sync.Unlock()
	if s.current == nil {
		chunk, err := s.db.GetSequenceChunk(s.sequenceName)
		if err != nil {
			return 0, err
		}
		s.current = chunk
		s.next = nil
		s.index = 1
	}
NewId:
	nextId := s.current.start + s.current.offset + (s.current.count * s.index) - (s.current.count - 1)
	if nextId > s.current.end {
		s.db.logger.Verbosef("moving next chunk into current sequence [%s]", s.sequenceName)
		if s.next != nil {
			s.current = s.next
		} else {
			chunk, err := s.db.GetSequenceChunk(s.sequenceName)
			if err != nil {
				return 0, err
			}
			s.current = chunk
		}
		s.next = nil
		s.index = 1
		goto NewId
	}
	if s.next == nil && float64(s.index*s.current.count)/float64(s.current.end-s.current.start) > (float64(SequencePreretrieve)/100) {
		s.db.logger.Verbosef("requesting next chunk in sequence [%s] preemptive", s.sequenceName)
		chunk, err := s.db.GetSequenceChunk(s.sequenceName)
		if err != nil {
			return 0, err
		}
		s.next = chunk
	}
	s.index++
	return nextId, nil
}
