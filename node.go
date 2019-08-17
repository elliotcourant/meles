package meles

func getMyNodeIdPath() []byte {
	return []byte{metaPrefix_MyId}
}

func getNodeIncrementNodeIdPath() []byte {
	return []byte("node_ids")
}

type myNodeId struct {
	NodeId uint64
}
