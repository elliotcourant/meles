package meles

import (
	"github.com/elliotcourant/buffers"
)

type distSequenceCache struct {
	currentValue       uint64
	lastPartitionIndex uint64
	maxPartitionIndex  uint64
	partitions         uint64
}

func (i *distSequenceCache) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(i.currentValue)
	buf.AppendUint64(i.lastPartitionIndex)
	buf.AppendUint64(i.maxPartitionIndex)
	buf.AppendUint64(i.partitions)
	return buf.Bytes()
}

func (i *distSequenceCache) Decode(src []byte) error {
	*i = distSequenceCache{}
	buf := buffers.NewBytesReader(src)
	i.currentValue = buf.NextUint64()
	i.lastPartitionIndex = buf.NextUint64()
	i.maxPartitionIndex = buf.NextUint64()
	i.partitions = buf.NextUint64()
	return nil
}
