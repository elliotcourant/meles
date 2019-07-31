package meles

import (
	"github.com/ahmetb/go-linq"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/meles/driver"
	"github.com/elliotcourant/timber"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Store interface {
	Close()
}

type store struct {
	db *badger.DB

	proposeChannel             <-chan *proposition
	configurationChangeChannel <-chan raftpb.ConfChange

	node    raft.Node
	config  *raft.Config
	storage raft.Storage
	id      uint64
	log     timber.Logger
	addr    string

	shutdownSync sync.RWMutex
	shutdown     bool
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
		addr:                       address,
	}

	options := badger.DefaultOptions(directory)
	options.Logger = log.With(timber.Keys{}).SetDepth(1)
	db, err := badger.Open(options)
	if err != nil {
		panic(err)
	}

	c := &raft.Config{
		ID:                        id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   nil,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	str.config = c

	str.storage = Init(db, id, 0, log)

	go func(str *store, listener net.Listener) {
		if err := str.listen(listener); err != nil {
			str.log.Errorf("failed to listen: %v", err)
		}
	}(str, listener)

	str.checkPeers(peers)

	return str
}

func startupCheck(directory string) (join bool, id uint64) {
	options := badger.DefaultOptions(directory)
	options.Logger = timber.New().Prefix("-").SetDepth(1)
	db, err := badger.Open(options)
	if err != nil {
		panic(err)
	}

	defer db.Close()

	id, _ = RaftId(db)
	return id == 0, id
}

func (s *store) checkPeers(peers []string) []raft.Peer {
	neighbors := make([]raft.Peer, 0)
	for _, peer := range peers {
		if peer == s.addr {
			continue
		}
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

		log.Debugf("trying to shake hands with peer")

		// Send the handshake to the peer to get their ID.
		handshakeResponse, err := peerDriver.Handshake(s.id)
		if err != nil {
			log.Errorf("could not shake hands with peer: %v", err)
		}

		log.Infof("greeted peer with ID [%d]", handshakeResponse.ID)

		neighbors = append(neighbors, raft.Peer{
			ID: handshakeResponse.ID,
		})
	}
	return neighbors
}

func (s *store) Close() {
	s.shutdownSync.Lock()
	defer s.shutdownSync.Unlock()
	s.shutdown = true
}

func (s *store) IsShutdown() bool {
	s.shutdownSync.RLock()
	defer s.shutdownSync.RUnlock()
	return s.shutdown
}
