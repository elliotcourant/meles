package meles

import (
	"encoding/binary"
	"fmt"
	"github.com/ahmetb/go-linq/v3"
	"github.com/dgraph-io/badger"
	"github.com/elliotcourant/meles/logger"
	"github.com/elliotcourant/meles/network"
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

type storeState int

const (
	stateNone storeState = 0 << iota
	stateCreated
	stateStarting
	stateEstablished
)

const (
	leaderWaitDelay   = 50 * time.Millisecond
	leaderWaitTimeout = 500 * time.Millisecond
	applyTimeout      = 1000 * time.Millisecond
)

var (
	ErrNotFound = fmt.Errorf("not found")
)

type nodeId uint64

func (n nodeId) RaftID() raft.ServerID {
	return raft.ServerID(fmt.Sprintf("%d", n))
}

type distOptions struct {
	Directory       string
	Peers           []string
	LeaderWaitDelay time.Duration
	NumericNodeIds  bool
}

type boat struct {
	id     raft.ServerID
	idSync sync.RWMutex

	listenAddress string
	db            *badger.DB
	options       *distOptions
	logger        timber.Logger
	raftSync      sync.RWMutex
	raft          *raft.Raft
	ln            transportWrapper
	peers         []raft.Server

	closed     bool
	closedSync sync.RWMutex

	objectSequences     map[string]*incrementSequence
	objectSequencesSync sync.Mutex

	distSequences    map[string]*distSequence
	distSequenceSync sync.Mutex

	distCache     map[string]*distSequenceCache
	distCacheSync sync.Mutex

	peerPool map[raft.ServerAddress]sync.Pool
	peerSync sync.Mutex

	newNodeLock sync.Mutex
	newNode     bool
}

func newDistributor(listener net.Listener, options *distOptions, l timber.Logger) (barge, error) {
	options.NumericNodeIds = true
	ln := newTransportWrapperFromListener(listener, l)
	addr, err := network.ResolveAddress(ln.Addr().String())
	if err != nil {
		return nil, err
	}
	dbOptions := badger.DefaultOptions(options.Directory)
	dbOptions.Logger = logger.NewBadgerLogger(l)
	db, err := badger.OpenManaged(dbOptions)
	if err != nil {
		return nil, err
	}
	r := &boat{
		options:         options,
		db:              db,
		logger:          l,
		ln:              ln,
		listenAddress:   addr,
		objectSequences: map[string]*incrementSequence{},
		distCache:       map[string]*distSequenceCache{},
		distSequences:   map[string]*distSequence{},
	}
	r.id, r.peers, r.newNode, err = r.determineNodeId()
	return r, err
}

func (r *boat) Start() error {
	r.runMasterServer()
	r.runBoatServer()

	r.logger.Infof("trying to discover peers")
	shouldJoin, shouldJoinAddr, err := r.discoverPeers()
	if err != nil {
		return err
	}

	if shouldJoin && len(shouldJoinAddr) > 0 && r.options.NumericNodeIds {
		wire, err := r.newRpcConnectionTo(shouldJoinAddr)
		if err != nil {
			return err
		}
		id, err := wire.GetIdentity()
		if err != nil {
			return err
		}
		r.id = nodeId(id).RaftID()
	}

	r.logger.Prefix(string(r.id))

	r.logger.Infof("starting node at address [%s]", r.listenAddress)

	raftTransport := newMelesTransportWithLogger(
		r.ln.RaftTransport(),
		4,
		time.Second*5,
		r.logger)

	config := &raft.Config{
		ProtocolVersion:    raft.ProtocolVersionMax,
		HeartbeatTimeout:   time.Millisecond * 500,
		ElectionTimeout:    time.Millisecond * 500,
		CommitTimeout:      time.Millisecond * 500,
		MaxAppendEntries:   64,
		ShutdownOnRemove:   true,
		TrailingLogs:       128,
		SnapshotInterval:   time.Minute * 5,
		SnapshotThreshold:  512,
		LeaderLeaseTimeout: time.Millisecond * 500,
		LocalID:            r.id,
		NotifyCh:           nil,
		Logger:             logger.NewLogger(r.logger),
	}

	r.ln.SetNodeID(config.LocalID)

	snapshots, err := raft.NewFileSnapshotStore(r.options.Directory, 8, os.Stderr)
	if err != nil {
		return fmt.Errorf("could not create snapshot store: %v", err)
	}

	stableStore := r.stableStore()

	raftLog, err := raft.NewLogCache(64, r.logStore())
	if err != nil {
		return fmt.Errorf("could not create raft log store: %v", err)
	}

	rft, err := raft.NewRaft(
		config,
		r.fsmStore(),
		raftLog,
		stableStore,
		snapshots,
		raftTransport)
	if err != nil {
		return err
	}
	r.raftSync.Lock()
	r.raft = rft
	r.raftSync.Unlock()
	if r.newNode {
		if len(r.peers) == 1 && !shouldJoin {
			r.logger.Infof("bootstrapping")
			configuration := raft.Configuration{
				Servers: []raft.Server{
					{
						ID:      config.LocalID,
						Address: raftTransport.LocalAddr(),
					},
				},
			}
			r.raft.BootstrapCluster(configuration)
		} else if len(r.peers) > 1 && !shouldJoin {
			r.logger.Infof("bootstrapping")
			configuration := raft.Configuration{
				Servers: r.peers,
			}
			bootstrapResult := r.raft.BootstrapCluster(configuration)
			if err := bootstrapResult.Error(); err != nil {
				r.logger.Errorf("failed to bootstrap: %v", err)
			} else {
				r.logger.Infof("successfully bootstrapped node")
			}
		} else if len(r.peers) > 1 && shouldJoin && len(shouldJoinAddr) > 0 {
			r.logger.Infof("attempting to join [%s]", shouldJoinAddr)
			wire, err := r.newRpcConnectionTo(shouldJoinAddr)
			if err != nil {
				return fmt.Errorf("failed to join cluster: %v", err)
			}
			if err := wire.Join(); err != nil {
				return err
			}
		}
	}

	r.logger.Info("raft started")
	r.setNewNode(false)
	r.idSync.Lock()
	r.id = config.LocalID
	r.idSync.Unlock()

	r.WaitForLeader(time.Second * 10)
	return nil
}

func (r *boat) setNewNode(val bool) {
	r.newNodeLock.Lock()
	defer r.newNodeLock.Unlock()
	r.newNode = val
}

func (r *boat) getIsNewNode() bool {
	r.newNodeLock.Lock()
	r.raftSync.RLock()
	defer r.newNodeLock.Unlock()
	defer r.raftSync.RUnlock()
	return r.newNode && r.raft == nil
}

// discoverPeers will return a boolean, string and error
// the bool will be true if we should join a cluster
// rather than bootstrapping a new one, and the string
// will be the address where we should send the join request.
func (r *boat) discoverPeers() (bool, string, error) {
	peers := map[string]rpcDriver{}
	maxRetries := 3
	for _, peer := range r.peers {
		if string(peer.Address) == r.listenAddress {
			continue
		}
		retries := 0
	RetryConnection:
		wire, err := r.newRpcConnectionTo(string(peer.Address))
		if err != nil {
			if retries < maxRetries {
				retries++
				r.logger.Warningf("failed to connect to peer [%s] trying again: %v", peer.Address, err)
				goto RetryConnection
			}
			return false, "", err
		}
		peers[string(peer.Address)] = wire
	}

	if len(peers) == 0 {
		return false, "", nil
	}

	shouldJoin := false
	shouldJoinAddr := ""
	for addr, peer := range peers {
		discovery, err := peer.Discover()
		if err != nil {
			r.logger.Warningf("could not discover peer [%s]: %v", addr, err)
			continue
		}

		if len(discovery.Leader) > 0 {
			if shouldJoin && shouldJoinAddr != string(discovery.Leader) {
				r.logger.Errorf(
					"peer [%s - %s] has leader [%s], but a leader has already been found [%s], using original leader",
					discovery.NodeID,
					addr,
					discovery.Leader,
					shouldJoinAddr)
				return false, "", fmt.Errorf("multiple leaders found")
			} else {
				r.logger.Infof(
					"peer [%s - %s] has leader [%s], setting join address",
					discovery.NodeID,
					addr,
					discovery.Leader)
				shouldJoin = true
				shouldJoinAddr = string(discovery.Leader)
			}
		}
	}
	return shouldJoin, shouldJoinAddr, nil
}

func (r *boat) NextIncrementId(incrementPath []byte) (uint64, error) {
	leaderAddr, amLeader, err := r.waitForAmILeader(leaderWaitTimeout)
	if err != nil {
		return 0, fmt.Errorf("could not apply transactionBase: %v", err)
	}
	// If I am not the leader then we need to forward this request to the leader.
	if !amLeader {
		r.logger.Verbosef("redirecting increment request to leader [%s]", leaderAddr)
		c, err := r.newRpcConnectionTo(leaderAddr)
		if err != nil {
			return 0, err
		}
		defer c.Close()
		return c.NextObjectID(incrementPath)
	}

	r.objectSequencesSync.Lock()
	defer r.objectSequencesSync.Unlock()

	seq, ok := r.objectSequences[string(incrementPath)]
	if !ok {
		seq, err = r.GetObjectSequence(incrementPath, 10)
		if err != nil {
			return 0, err
		}
		r.objectSequences[string(incrementPath)] = seq
	}

	return seq.Next()
}

func (r *boat) NodeID() raft.ServerID {
	r.idSync.Lock()
	defer r.idSync.Unlock()
	return r.id
}

func (r *boat) IsStopped() bool {
	r.closedSync.RLock()
	defer r.closedSync.RUnlock()
	return r.closed
}

func (r *boat) Stop() error {
	r.closedSync.Lock()
	defer r.closedSync.Unlock()
	r.closed = true
	ftr := r.raft.Shutdown()
	if err := ftr.Error(); err != nil {
		return err
	}
	r.ln.Close()
	r.db.Close()
	return nil
}

func (r *boat) Leader() raft.ServerAddress {
	r.raftSync.RLock()
	defer r.raftSync.RUnlock()
	if r.raft == nil {
		return raft.ServerAddress("")
	}
	return r.raft.Leader()
}

func (r *boat) WaitForLeader(timeout time.Duration) (string, bool, error) {
	address, err := func(timeout time.Duration) (string, error) {
		ld := r.Leader()
		l := string(ld)
		if len(l) > 0 {
			return l, nil
		}

		ticker := time.NewTicker(leaderWaitDelay)
		defer ticker.Stop()
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-ticker.C:
				l := string(r.Leader())
				if len(l) > 0 {
					return l, nil
				}
			case <-timer.C:
				return "", fmt.Errorf("wait for leader timeout expired")
			}
		}
	}(timeout)
	if err != nil {
		return "", false, err
	}
	address, err = network.ResolveAddress(address)
	return address, true, err
}

func (r *boat) IsLeader() bool {
	_, ok, _ := r.WaitForLeader(leaderWaitTimeout)
	return ok && r.raft.State() == raft.Leader
}

func (r *boat) waitForAmILeader(timeout time.Duration) (string, bool, error) {
	addr, ok, err := r.WaitForLeader(timeout)
	if !ok || err != nil {
		return "", false, fmt.Errorf("failed to wait to find out if this is the leader: %v", err)
	}
	return addr, r.raft.State() == raft.Leader, nil
}

func (r *boat) apply(tx transactionStorage, badgerTxn *badger.Txn) error {
	leaderAddr, amLeader, err := r.waitForAmILeader(leaderWaitTimeout)
	if err != nil {
		return fmt.Errorf("could not apply transactionBase: %v", err)
	}
	// If I am not the leader then we need to forward this transactionBase to the leader.
	if !amLeader {
		r.logger.Verbosef("redirecting apply command to leader [%s]", leaderAddr)
		c, err := r.newRpcConnectionTo(leaderAddr)
		if err != nil {
			return err
		}
		defer c.Close()
		err = c.ApplyTransaction(tx)
		if err != nil {
			return err
		}

		if badgerTxn != nil {
			r.logger.Verbosef("apply was successful, applying locally")
			for _, action := range tx.Actions {
				switch action.Type {
				case actionTypeSet:
					if err := badgerTxn.Set(action.Key, action.Value); err != nil {
						return err
					}
				case actionTypeDelete:
					if err := badgerTxn.Delete(action.Key); err != nil {
						return err
					}
				default:
					return fmt.Errorf("invalid action type [%d]", action.Type)
				}
			}
			return badgerTxn.CommitAt(tx.Timestamp, nil)
		}
	}
	applyFuture := r.raft.Apply(tx.Encode(), applyTimeout)
	return applyFuture.Error()
}

func (r *boat) stableStore() raft.StableStore {
	return &raftStableStore{
		db: r.db,
	}
}

func (r *boat) logStore() raft.LogStore {
	return &raftLogStore{
		db: r.db,
	}
}

func (r *boat) fsmStore() raft.FSM {
	return &raftFsmStore{
		db:     r.db,
		logger: r.logger,
	}
}

func (r *boat) runMasterServer() {
	ms := &masterServer{
		boat:   r,
		ln:     r.ln,
		logger: r.logger,
	}
	ms.runMasterServer()
}

func (r *boat) runBoatServer() {
	bs := &boatServer{
		boat:   r,
		ln:     r.ln.RpcTransport(),
		logger: r.logger,
	}
	bs.runBoatServer()
}

func (r *boat) determineNodeId() (id raft.ServerID, servers []raft.Server, newNode bool, err error) {
	if !r.options.NumericNodeIds {
		panic("non numeric node Ids are not yet supported")
	}

	var val []byte
	if err := r.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(getMyNodeIdPath())
		if err == badger.ErrKeyNotFound {
			val = make([]byte, 0)
			return nil
		} else if err != nil {
			return err
		}
		_, err = item.ValueCopy(val)
		return err
	}); err != nil {
		return "", nil, true, err
	}

	if len(val) == 8 {
		id = nodeId(binary.BigEndian.Uint64(val)).RaftID()
		return id, nil, false, nil
	}

	myParsedAddress, err := network.ResolveAddress(r.listenAddress)
	if err != nil {
		return "", nil, true, err
	}
	peers := make([]string, len(r.options.Peers))
	for i, peer := range r.options.Peers {
		addr, err := network.ResolveAddress(peer)
		if err != nil {
			return "", nil, true, err
		}
		peers[i] = addr
	}
	peers = append(peers, myParsedAddress)

	linq.From(peers).Distinct().ToSlice(&peers)

	sort.Strings(peers)

	servers = make([]raft.Server, len(peers))
	for i, peer := range peers {
		servers[i] = raft.Server{
			ID:       raft.ServerID(fmt.Sprintf("%d", i+1)),
			Address:  raft.ServerAddress(peer),
			Suffrage: raft.Voter,
		}
	}

	id = nodeId(linq.From(peers).IndexOf(func(i interface{}) bool {
		addr, ok := i.(string)
		return ok && addr == myParsedAddress
	}) + 1).RaftID()
	return id, servers, true, nil
}
