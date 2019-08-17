package meles

import (
	"github.com/elliotcourant/buffers"
)

type transactionStorage struct {
	Timestamp uint64
	Actions   []action
}

func (i *transactionStorage) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(i.Timestamp)
	buf.AppendUint32(uint32(len(i.Actions)))
	for _, action := range i.Actions {
		buf.Append(action.Encode()...)
	}
	return buf.Bytes()
}

func (i *transactionStorage) Decode(src []byte) error {
	*i = transactionStorage{}
	buf := buffers.NewBytesReader(src)
	i.Timestamp = buf.NextUint64()
	length := buf.NextUint32()
	i.Actions = make([]action, length)
	for index := range i.Actions {
		actionBytes := buf.NextBytes()
		action := &action{}
		if err := action.Decode(actionBytes); err != nil {
			return err
		}
		i.Actions[index] = *action
	}
	return nil
}

type actionType uint8

const (
	actionTypeNone   actionType = 0
	actionTypeSet    actionType = 1
	actionTypeDelete actionType = 2
)

type action struct {
	Type  actionType
	Key   []byte
	Value []byte
}

func (i *action) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(uint8(i.Type))
	buf.Append(i.Key...)
	switch i.Type {
	case actionTypeSet:
		buf.Append(i.Value...)
	}
	return buf.Bytes()
}

func (i *action) Decode(src []byte) error {
	*i = action{}
	buf := buffers.NewBytesReader(src)
	i.Type = actionType(buf.NextUint8())
	i.Key = buf.NextBytes()
	switch i.Type {
	case actionTypeSet:
		i.Value = buf.NextBytes()
	}
	return nil
}
