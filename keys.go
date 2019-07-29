package meles

type KeyPrefix byte

const (
	KeyPrefix_Internal KeyPrefix = 'i'
	KeyPrefix_External           = 'e'
)

type InternalItem byte

const (
	InternalItem_NodeID InternalItem = 'i'
)

func (item InternalItem) GetPath() []byte {
	return append([]byte{}, byte(KeyPrefix_Internal), byte(item))
}
