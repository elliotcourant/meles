package meles

import (
	"encoding/binary"
	"github.com/ahmetb/go-linq"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/meles/driver"
	"github.com/elliotcourant/timber"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"net"
	"sort"
	"strconv"
	"time"
)

type Store interface {
}

type store struct {
	proposeChannel             <-chan *proposition
	configurationChangeChannel <-chan raftpb.ConfChange

	node raft.Node
	id   uint64
	log  timber.Logger
}

func NewStore(listener net.Listener, peers []string, directory string, join bool) Store {
	address, err := resolveAddress(listener.Addr().String())
	if err != nil {
		panic(err)
	}

	log := timber.New().Prefix("-")

	log.Infof("initializing raft store at [%s]", address)

	shouldJoin, id := startupCheck(directory)

	// If we are told that we need to join the cluster, this could be a false
	// positive. If the store already exists then we do not need to join the
	// cluster. So make the shouldJoin pessimistic here.
	shouldJoin = shouldJoin && join
	if shouldJoin && id == 0 {
		sortedPeers := make([]string, 0)
		linq.From(append(peers, address)).Distinct().ToSlice(&sortedPeers)
		sort.Strings(sortedPeers)
		id = uint64(linq.From(sortedPeers).IndexOf(func(i interface{}) bool {
			addr, ok := i.(string)
			return ok && addr == address
		}) + 1)
	}

	if id == 0 {
		panic("could not determine ID")
	}

	log = log.Prefix(strconv.FormatUint(id, 10))

	log.Infof("starting raft store")

	str := &store{
		id:                         id,
		log:                        log,
		proposeChannel:             make(chan *proposition),
		configurationChangeChannel: make(chan raftpb.ConfChange),
	}

	go func(str *store, listener net.Listener) {
		if err := str.listen(listener); err != nil {
			str.log.Errorf("failed to listen: %v", err)
		}
	}(str, listener)

	str.checkPeers(peers)

	return nil
}

func startupCheck(directory string) (join bool, id uint64) {
	options := badger.DefaultOptions(directory)
	options.Logger = timber.New().Prefix("-").SetDepth(1)
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
		return true, 0
	}

	return false, binary.BigEndian.Uint64(nodeId)
}

func (s *store) checkPeers(peers []string) []raft.Peer {
	for _, peer := range peers {
		peer, err := resolveAddress(peer)
		if err != nil {
			s.log.Errorf("could not parse peer address [%s]: %v", peer, err)
			continue
		}

		addr, err := net.ResolveTCPAddr("tcp", peer)
		if err != nil {
			s.log.Errorf("could not resolve peer address [%s]: %v", peer, err)
			continue
		}

		log := s.log.With(timber.Keys{
			"r-peer": addr.String(),
		})

		retries := 0
	Retry:
		conn, err := net.Dial(addr.Network(), addr.String())
		if err != nil {
			log.Errorf("could not connect to peer: %v", err)
			if retries < 3 {
				time.Sleep(5 * time.Second)
				retries++
				goto Retry
			}
			continue
		}

		peerDriver := driver.NewDriver(conn, log)

		// Send the handshake to the peer to get their ID.
		handshakeResponse, err := peerDriver.Handshake(s.id)
		if err != nil {
			log.Errorf("could not shake hands with peer: %v", err)
		}

		log.Infof("greeted peer with ID [%s]", handshakeResponse.ID)
	}
	return nil
}
