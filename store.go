package meles

import (
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/timber"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"net"
)

type Store interface {
}

type store struct {
	proposeChannel             <-chan *proposition
	configurationChangeChannel <-chan raftpb.ConfChange

	node raft.Node
}

func NewStore(listener net.Listener, peers []string, directory string, join bool) Store {
	shouldJoin, id := startupCheck(directory)

	// If we are told that we need to join the cluster, this could be a false
	// positive. If the store already exists then we do not need to join the
	// cluster. So make the shouldJoin pessimistic here.
	shouldJoin = shouldJoin && join

	log := timber.New().Prefix(id)

	log.Infof("starting raft store")

	return nil
}

func startupCheck(directory string) (join bool, id string) {
	options := badger.DefaultOptions(directory)
	options.Logger = timber.New().Prefix("N/A").SetDepth(1)
	db, err := badger.Open(options)
	if err != nil {
		panic(err)
	}

	defer db.Close()

	var nodeId []byte
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(InternalItem_NodeID.GetPath())
		if err == badger.ErrKeyNotFound {
			return nil
		} else if err != nil {
			panic(err)
		}
		if _, err := item.ValueCopy(nodeId); err != nil {
			panic(err)
		}
		return nil
	}); err != nil {
		panic(err)
	}

	if len(nodeId) == 0 {
		id = GetShortID()
		return true, id
	}

	return false, string(nodeId)
}
