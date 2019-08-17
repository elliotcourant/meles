package storage

import (
	"github.com/elliotcourant/buffers"
)

type Transaction struct {
	Timestamp uint64
	Actions   []Action
}

func (i *Transaction) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint64(i.Timestamp)
	buf.AppendUint32(uint32(len(i.Actions)))
	for _, action := range i.Actions {
		buf.Append(action.Encode()...)
	}
	return buf.Bytes()
}

func (i *Transaction) Decode(src []byte) error {
	*i = Transaction{}
	buf := buffers.NewBytesReader(src)
	i.Timestamp = buf.NextUint64()
	length := buf.NextUint32()
	i.Actions = make([]Action, length)
	for index := range i.Actions {
		actionBytes := buf.NextBytes()
		action := &Action{}
		if err := action.Decode(actionBytes); err != nil {
			return err
		}
		i.Actions[index] = *action
	}
	return nil
}

type ActionType uint8

const (
	ActionTypeNone   ActionType = 0
	ActionTypeSet    ActionType = 1
	ActionTypeDelete ActionType = 2
)

type Action struct {
	Type  ActionType
	Key   []byte
	Value []byte
}

func (i *Action) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(uint8(i.Type))
	buf.Append(i.Key...)
	switch i.Type {
	case ActionTypeSet:
		buf.Append(i.Value...)
	}
	return buf.Bytes()
}

func (i *Action) Decode(src []byte) error {
	*i = Action{}
	buf := buffers.NewBytesReader(src)
	i.Type = ActionType(buf.NextUint8())
	i.Key = buf.NextBytes()
	switch i.Type {
	case ActionTypeSet:
		i.Value = buf.NextBytes()
	}
	return nil
}
