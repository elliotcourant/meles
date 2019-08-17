package meles

import (
	"github.com/elliotcourant/buffers"
)

type incrementSequenceStorage struct {
	Key  []byte
	Next uint64
}

func (i incrementSequenceStorage) Path() []byte {
	return append([]byte{metaPrefix_IncrementSequence}, i.Key...)
}

func (i incrementSequenceStorage) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(i.Next)
	return buf.Bytes()
}

func (i *incrementSequenceStorage) Decode(src []byte) error {
	*i = incrementSequenceStorage{}
	buf := buffers.NewBytesReader(src)
	i.Next = buf.NextUint64()
	return nil
}
