package meles

func getMyNodeIdPath() []byte {
	return []byte{metaPrefix_MyId}
}

type myNodeId struct {
	NodeId uint64
}
