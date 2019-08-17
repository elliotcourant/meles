package storage

func GetMyNodeIdPath() []byte {
	return []byte{MetaPrefix_MyId}
}

type MyNodeId struct {
	NodeId uint64
}

type Node struct {
}
