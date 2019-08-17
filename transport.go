package meles

import (
	"bytes"
	"context"
	"fmt"
	"github.com/elliotcourant/meles/network"
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"io"
	"net"
	"os"
	"sync"
	"time"
)

func newTransport(address string) (transportInterface, error) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return newTransportFromListener(l), nil
}

func newTransportFromListener(listener net.Listener) transportInterface {
	return &transport{listener}
}

type transport struct {
	net.Listener
}

func (transport) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

type transportInterface interface {
	Accept() (net.Conn, error)
	Close() error
	Addr() net.Addr
	Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error)
}

const (
	// rpcMaxPipeline controls the maximum number of outstanding
	// AppendEntries RPC calls.
	rpcMaxPipeline = 128
)

// deferError can be embedded to allow a future
// to provide an error in the future.
type deferError struct {
	err       error
	errCh     chan error
	responded bool
}

func (d *deferError) init() {
	d.errCh = make(chan error, 1)
}

func (d *deferError) Error() error {
	if d.err != nil {
		// Note that when we've received a nil error, this
		// won't trigger, but the channel is closed after
		// send so we'll still return nil below.
		return d.err
	}
	if d.errCh == nil {
		panic("waiting for response on nil channel")
	}
	d.err = <-d.errCh
	return d.err
}

func (d *deferError) respond(err error) {
	if d.errCh == nil {
		return
	}
	if d.responded {
		return
	}
	d.errCh <- err
	close(d.errCh)
	d.responded = true
}

// serverAddressProvider just provides us a potential implementation
// to allow us to lookup an address with whatever ID we are provided.
// While it is default behavior most of the time to use the listen
// address as the server ID in a raft implementation, this is a dumb
// idea and we should absolutely not depend on it.
type serverAddressProvider interface {
	ServerAddr(id raft.ServerID) (raft.ServerAddress, error)
}

// streamLayer is just a local interface definition for our net stuff
// essentially what will actually be passed here is from the core.Wrapper
// stuff that we built as a net code hack.
type streamLayer interface {
	net.Listener

	// Dial is used to create a new outgoing connection
	Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error)
}

// melesTransport is an improved TCP transport for
// raft that uses a net code similar to Postgres.
type melesTransport struct {
	connPool     map[raft.ServerAddress][]*melesConn
	connPoolLock sync.Mutex

	consumeChannel chan raft.RPC

	hearbeatCallback      func(raft.RPC)
	heartbeatCallbackLock sync.Mutex

	// In the other TCP transport we use a different
	// logger, but to be consistent with what hashicorp's
	// raft library uses, we should use this.
	logger timber.Logger

	maxPool int

	serverAddressProvider serverAddressProvider

	shutdown        bool
	shutdownChannel chan struct{}
	shutdownLock    sync.RWMutex

	stream streamLayer

	streamContext     context.Context
	streamCancel      context.CancelFunc
	streamContextLock sync.RWMutex

	timeout      time.Duration
	timeoutScale int
}

func (p *melesTransport) TimeoutNow(id raft.ServerID, target raft.ServerAddress, args *raft.TimeoutNowRequest, resp *raft.TimeoutNowResponse) error {
	panic("implement me")
}

// melesTransportConfig exposes just a few ways to tweak the
// internal behavior of the pg transport.
type melesTransportConfig struct {
	ServerAddressProvider serverAddressProvider
	Logger                timber.Logger
	Stream                streamLayer
	MaxPool               int
	Timeout               time.Duration
}

// appendFuture is used for waiting on a pipelined append
// entries RPC.
type appendFuture struct {
	deferError
	start time.Time
	args  *raft.AppendEntriesRequest
	resp  *raft.AppendEntriesResponse
}

func (a *appendFuture) Start() time.Time {
	return a.start
}

func (a *appendFuture) Request() *raft.AppendEntriesRequest {
	return a.args
}

func (a *appendFuture) Response() *raft.AppendEntriesResponse {
	return a.resp
}

type melesConn struct {
	target raft.ServerAddress
	conn   net.Conn
	wire   raftClientWire
}

func (p *melesConn) Release() error {
	return p.conn.Close()
}

type melesPipeline struct {
	conn      *melesConn
	transport *melesTransport

	doneChannel       chan raft.AppendFuture
	inProgressChannel chan *appendFuture

	shutdown        bool
	shutdownChannel chan struct{}
	shutdownLock    sync.Mutex

	logger timber.Logger

	wire raftClientWire
}

func newMelesTransportWithConfig(
	config *melesTransportConfig,
) *melesTransport {
	if config.Logger == nil {
		config.Logger = timber.New()
	}
	trans := &melesTransport{
		connPool:              make(map[raft.ServerAddress][]*melesConn),
		consumeChannel:        make(chan raft.RPC),
		logger:                config.Logger,
		maxPool:               config.MaxPool,
		shutdownChannel:       make(chan struct{}),
		stream:                config.Stream,
		timeout:               config.Timeout, // I'm leary of this at the moment
		serverAddressProvider: config.ServerAddressProvider,
	}

	trans.setupStreamContext()
	go trans.listen()
	return trans
}

// newMelesTransport creates a new network transport with the given dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func newMelesTransport(
	stream streamLayer,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) *melesTransport {
	if logOutput == nil {
		logOutput = os.Stderr
	}
	config := &melesTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: timber.New()}
	return newMelesTransportWithConfig(config)
}

// newMelesTransportWithLogger creates a new network transport with the given logger, dialer
// and listener. The maxPool controls how many connections we will pool. The
// timeout is used to apply I/O deadlines. For InstallSnapshot, we multiply
// the timeout by (SnapshotSize / TimeoutScale).
func newMelesTransportWithLogger(
	stream streamLayer,
	maxPool int,
	timeout time.Duration,
	logger timber.Logger,
) *melesTransport {
	config := &melesTransportConfig{Stream: stream, MaxPool: maxPool, Timeout: timeout, Logger: logger}
	return newMelesTransportWithConfig(config)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
func (p *melesTransport) SetHeartbeatHandler(callback func(rpc raft.RPC)) {
	p.heartbeatCallbackLock.Lock()
	defer p.heartbeatCallbackLock.Unlock()
	p.hearbeatCallback = callback
}

// LocalAddr implements the transportInterface interface.
func (p *melesTransport) LocalAddr() raft.ServerAddress {
	addr, err := network.ResolveAddress(p.stream.Addr().String())
	if err != nil {
		return raft.ServerAddress(p.stream.Addr().String())
	}
	return raft.ServerAddress(addr)
}

func (p *melesTransport) IsShutdown() bool {
	select {
	case <-p.shutdownChannel:
		return true
	default:
		return false
	}
}

// CloseStreams closes the current streams.
func (p *melesTransport) CloseStreams() {
	p.connPoolLock.Lock()
	defer p.connPoolLock.Unlock()

	for k, e := range p.connPool {
		for _, conn := range e {
			conn.Release()
		}

		delete(p.connPool, k)
	}

	// Cancel the existing connections and create a new context. Both these
	// operations must always be done with the lock held otherwise we can create
	// connection handlers that are holding a context that will never be
	// cancelable.
	p.streamContextLock.Lock()
	defer p.streamContextLock.Unlock()
	p.streamCancel()
	p.setupStreamContext()
}

func (p *melesTransport) Close() error {
	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()

	if !p.shutdown {
		close(p.shutdownChannel)
		p.stream.Close()
		p.shutdown = true
	}
	return nil
}

// Consumer implements the transportInterface interface.
func (p *melesTransport) Consumer() <-chan raft.RPC {
	return p.consumeChannel
}

func (p *melesTransport) AppendEntriesPipeline(
	id raft.ServerID,
	target raft.ServerAddress,
) (raft.AppendPipeline, error) {
	conn, err := p.getConnFromAddressProvider(id, target)
	if err != nil {
		return nil, err
	}

	return newPgPipeline(p, conn)
}

func (p *melesTransport) AppendEntries(
	id raft.ServerID,
	target raft.ServerAddress,
	args *raft.AppendEntriesRequest,
	resp *raft.AppendEntriesResponse,
) error {
	return p.genericRPC(id, target, &appendEntriesRequest{
		AppendEntriesRequest: *args,
	}, resp)
}

func (p *melesTransport) RequestVote(
	id raft.ServerID,
	target raft.ServerAddress,
	args *raft.RequestVoteRequest,
	resp *raft.RequestVoteResponse,
) error {
	return p.genericRPC(id, target, &requestVoteRequest{
		RequestVoteRequest: *args,
	}, resp)
}

func (p *melesTransport) InstallSnapshot(
	id raft.ServerID,
	target raft.ServerAddress,
	args *raft.InstallSnapshotRequest,
	resp *raft.InstallSnapshotResponse,
	data io.Reader,
) error {
	snapshot := make([]byte, 0)
	writer := bytes.NewBuffer(snapshot)

	if _, err := io.Copy(writer, data); err != nil {
		p.logger.Errorf("failed to copy snapshot data to bytes: %v", err)
		return err
	}

	// Get a conn, always close for InstallSnapshot
	conn, err := p.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}
	defer conn.Release()

	// Set a deadline, scaled by request size
	if p.timeout > 0 {
		timeout := p.timeout * time.Duration(args.Size/int64(p.timeoutScale))
		if timeout < p.timeout {
			timeout = p.timeout
		}
		conn.conn.SetDeadline(time.Now().Add(timeout))
	}

	if err := conn.wire.Send(&installSnapshotRequest{
		InstallSnapshotRequest: *args,
		Snapshot:               snapshot,
	}); err != nil {
		p.logger.Errorf("failed sending snapshot to [%v]: %v", conn.conn.RemoteAddr(), err)
		return err
	}

	return p.receiveResponse(conn, resp)
}

// EncodePeer implements the transportInterface interface.
func (p *melesTransport) EncodePeer(id raft.ServerID, a raft.ServerAddress) []byte {
	address := p.getProviderAddressOrFallback(id, a)
	return []byte(address)
}

// DecodePeer implements the transportInterface interface.
func (p *melesTransport) DecodePeer(buf []byte) raft.ServerAddress {
	return raft.ServerAddress(buf)
}

func (p *melesTransport) genericRPC(id raft.ServerID, target raft.ServerAddress, args raftClientMessage, response interface{}) error {
	conn, err := p.getConnFromAddressProvider(id, target)
	if err != nil {
		return err
	}

	if p.timeout > 0 {
		conn.conn.SetDeadline(time.Now().Add(p.timeout))
	}

	if err := conn.wire.Send(args); err != nil {
		p.logger.Errorf("when sending RPC to [%v]: %v", conn.conn.RemoteAddr(), err)
		return err
	}

	return p.receiveResponse(conn, response)
}

func (p *melesTransport) receiveResponse(conn *melesConn, response interface{}) error {
	message, err := conn.wire.Receive()
	if err != nil {
		if err == io.EOF || p.IsShutdown() {
			return nil
		}
		p.logger.Errorf("could not receive message from [%v]: %v", conn.conn.RemoteAddr(), err)
		return err
	}

	defer p.returnConn(conn)

	return func(message serverMessage, response interface{}) (err error) {
		switch msg := message.(type) {
		case *appendEntriesResponse:
			err = msg.Error
			if r, ok := response.(*raft.AppendEntriesResponse); ok {
				*r = msg.AppendEntriesResponse
			} else {
				p.logger.Warningf("received %T but was expecting to received %T", msg, response)
			}
		case *requestVoteResponse:
			err = msg.Error
			if r, ok := response.(*raft.RequestVoteResponse); ok {
				*r = msg.RequestVoteResponse
			} else {
				p.logger.Warningf("received %T but was expecting to received %T", msg, response)
			}
			response = msg.RequestVoteResponse
		case *installSnapshotResponse:
			err = msg.Error
			if r, ok := response.(*raft.InstallSnapshotResponse); ok {
				*r = msg.InstallSnapshotResponse
			} else {
				p.logger.Warningf("received %T but was expecting to received %T", msg, response)
			}
			response = msg.InstallSnapshotResponse
		case *errorResponse:
			err = msg.Error
			response = nil
		default:
			err = fmt.Errorf("could handle response message type [%v]", msg)
			response = nil
		}
		return err
	}(message, response)
}

func (p *melesTransport) getPooledConn(target raft.ServerAddress) *melesConn {
	p.connPoolLock.Lock()
	defer p.connPoolLock.Unlock()

	conns, ok := p.connPool[target]
	if !ok || len(conns) == 0 {
		return nil
	}

	var conn *melesConn
	num := len(conns)
	conn, conns[num-1] = conns[num-1], nil
	p.connPool[target] = conns[:num-1]
	return conn
}

func (p *melesTransport) getConnFromAddressProvider(
	id raft.ServerID,
	target raft.ServerAddress,
) (*melesConn, error) {
	address := p.getProviderAddressOrFallback(id, target)
	return p.getConn(address)
}

func (p *melesTransport) getProviderAddressOrFallback(
	id raft.ServerID,
	target raft.ServerAddress,
) raft.ServerAddress {
	if p.serverAddressProvider != nil {
		serverAddressOverride, err := p.serverAddressProvider.ServerAddr(id)
		if err != nil {
			p.logger.Warningf("unable to get address for server id %v, using fallback address %v: %v", id, target, err)
		} else {
			return serverAddressOverride
		}
	}
	return target
}

func (p *melesTransport) getConn(target raft.ServerAddress) (*melesConn, error) {
	// Check for a pooled conn
	if conn := p.getPooledConn(target); conn != nil {
		return conn, nil
	}

	// Dial a new connection
	conn, err := p.stream.Dial(target, p.timeout)
	if err != nil {
		p.logger.Errorf("could not dial connection for [%v]: %v", target, err)
		return nil, err
	}

	w, err := newRaftClientWire(conn, conn)
	if err != nil {
		return nil, err
	}

	pgConn := &melesConn{
		target: target,
		conn:   conn,
		wire:   w,
	}

	return pgConn, nil
}

func (p *melesTransport) returnConn(conn *melesConn) {
	p.connPoolLock.Lock()
	defer p.connPoolLock.Unlock()

	key := conn.target
	conns, _ := p.connPool[key]

	if !p.IsShutdown() && len(conns) < p.maxPool {
		p.connPool[key] = append(conns, conn)
	} else {
		conn.Release()
	}
}

func (p *melesTransport) setupStreamContext() {
	p.streamContext, p.streamCancel = context.WithCancel(context.Background())
}

// getStreamContext is used retrieve the current stream context.
func (p *melesTransport) getStreamContext() context.Context {
	p.streamContextLock.RLock()
	defer p.streamContextLock.RUnlock()
	return p.streamContext
}

func (p *melesTransport) listen() {
	for {
		conn, err := p.stream.Accept()
		if err != nil {
			if p.IsShutdown() {
				return
			}
			p.logger.Errorf("failed to accept connection: %v", err)
			continue
		}
		// p.logger.Debugf("%v accepted connection from: %v", p.LocalAddr(), conn.RemoteAddr())

		go p.handleConnection(p.getStreamContext(), conn)
	}
}

func (p *melesTransport) handleConnection(connectionContext context.Context, conn net.Conn) {
	defer conn.Close()
	wr := newRaftServerWire(conn, conn)

	for {
		select {
		case <-connectionContext.Done():
			p.logger.Debug("stream layer is closed")
			return
		default:
		}

		request, err := wr.Receive()
		if err != nil {
			if err != io.EOF {
				p.logger.Errorf("failed to receive message from [%v]: %v", conn.RemoteAddr(), err)
			}
			return
		}

		responseChannel := make(chan raft.RPCResponse, 1)
		rpc := raft.RPC{
			RespChan: responseChannel,
		}

		isHeartbeat := false
		switch req := request.(type) {
		case *appendEntriesRequest:
			rpc.Command = &req.AppendEntriesRequest

			// Check if this is a heartbeat
			if req.Term != 0 && req.Leader != nil &&
				req.PrevLogEntry == 0 && req.PrevLogTerm == 0 &&
				len(req.Entries) == 0 && req.LeaderCommitIndex == 0 {
				isHeartbeat = true
			}
		case *requestVoteRequest:
			rpc.Command = &req.RequestVoteRequest
		case *installSnapshotRequest:
			rpc.Command = &req.InstallSnapshotRequest
			rpc.Reader = req.Reader()
		default:
			p.logger.Errorf("did not recognize request type [%v] from [%v]: %v", req, conn.RemoteAddr(), err)
			return
		}

		// Check for heartbeat fast-path
		if isHeartbeat {
			p.heartbeatCallbackLock.Lock()
			callback := p.hearbeatCallback
			p.heartbeatCallbackLock.Unlock()
			if callback != nil {
				callback(rpc)
				goto RESPONSE
			}
		}

		// Dispatch the RPC to this raft node.
		select {
		case p.consumeChannel <- rpc:
		case <-p.shutdownChannel:
			p.logger.Error("transport is shutdown")
			return
		}

		// Wait for response
	RESPONSE:
		select {
		case response := <-responseChannel:
			var msg raftServerMessage
			switch rsp := response.Response.(type) {
			case *raft.AppendEntriesResponse:
				msg = &appendEntriesResponse{
					Error:                 response.Error,
					AppendEntriesResponse: *rsp,
				}
			case *raft.RequestVoteResponse:
				msg = &requestVoteResponse{
					Error:               response.Error,
					RequestVoteResponse: *rsp,
				}
			case *raft.InstallSnapshotResponse:
				msg = &installSnapshotResponse{
					Error:                   response.Error,
					InstallSnapshotResponse: *rsp,
				}
			case nil:
				msg = &errorResponse{
					Error: response.Error,
				}
			}

			if err := wr.Send(msg); err != nil {
				p.logger.Errorf("failed to send response to [%v]: %v", conn.RemoteAddr(), err)
				return
			}
		case <-p.shutdownChannel:
			p.logger.Warningf("closing transport due to shutdown")
			return
		}
	}
}

func newPgPipeline(trans *melesTransport, conn *melesConn) (*melesPipeline, error) {
	wr := conn.wire

	p := &melesPipeline{
		conn:              conn,
		transport:         trans,
		doneChannel:       make(chan raft.AppendFuture, rpcMaxPipeline),
		inProgressChannel: make(chan *appendFuture, rpcMaxPipeline),
		shutdownChannel:   make(chan struct{}),
		logger:            timber.New(),
		wire:              wr,
	}

	go p.processResponses()

	return p, nil
}

// AppendEntries is used to pipeline a new append entries request.
func (p *melesPipeline) AppendEntries(
	args *raft.AppendEntriesRequest,
	resp *raft.AppendEntriesResponse,
) (raft.AppendFuture, error) {
	// Create a new future
	future := &appendFuture{
		start: time.Now(),
		args:  args,
		resp:  resp,
	}
	future.init()

	// Add a send timeout
	if timeout := p.transport.timeout; timeout > 0 {
		p.conn.conn.SetWriteDeadline(time.Now().Add(timeout))
	}

	if err := p.wire.Send(&appendEntriesRequest{
		AppendEntriesRequest: *args,
	}); err != nil {
		return nil, err
	}

	select {
	case p.inProgressChannel <- future:
		return future, nil
	case <-p.shutdownChannel:
		return nil, fmt.Errorf("pipeline shutting down")
	}
}

// Consumer returns a channel that can be used to consume complete futures.
func (p *melesPipeline) Consumer() <-chan raft.AppendFuture {
	return p.doneChannel
}

// Close is used to shutdown the pipeline connection.
func (p *melesPipeline) Close() error {
	p.shutdownLock.Lock()
	defer p.shutdownLock.Unlock()
	if p.shutdown {
		return nil
	}

	// Release the connection
	p.conn.Release()

	p.shutdown = true
	close(p.shutdownChannel)
	return nil
}

func (p *melesPipeline) processResponses() {
	timeout := p.transport.timeout
	for {
		select {
		case future := <-p.inProgressChannel:
			func(future *appendFuture) {
				defer func(future *appendFuture) {
					p.doneChannel <- future
				}(future)

				if timeout > 0 {
					p.conn.conn.SetReadDeadline(time.Now().Add(timeout))
				}

				response, err := p.wire.Receive()
				if err != nil {
					p.logger.Errorf("could not process response from [%v]: %v", p.conn.conn.RemoteAddr(), err)
					future.respond(err)
					return
				}

				switch msg := response.(type) {
				case *appendEntriesResponse:
					future.resp = &msg.AppendEntriesResponse
					future.respond(msg.Error)
				case *errorResponse:
					future.resp = nil
					future.respond(msg.Error)
				default:
					err = fmt.Errorf("received an invalid response from [%v]: %v", p.conn.conn.RemoteAddr(), msg)
					p.logger.Error(err.Error())
					future.respond(err)
				}
			}(future)
		case <-p.shutdownChannel:
			return
		}
	}
}
