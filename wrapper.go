package meles

import (
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"net"
	"time"
)

type transportWrapper interface {
	NormalTransport() net.Listener
	ForwardToRaft(net.Conn, error)
	ForwardToRpc(net.Conn, error)
	RaftTransport() transportInterface
	RpcTransport() transportInterface
	Port() int
	Addr() net.Addr
	Close()
	SetNodeID(id raft.ServerID)
}

type accept struct {
	conn net.Conn
	err  error
}
type transportWrapperItem struct {
	listener      transportInterface
	acceptChannel chan accept
	logger        timber.Logger

	closeCallback func()
}

func (t *transportWrapperItem) SendAccept(conn net.Conn, err error) {
	t.acceptChannel <- accept{conn, err}
}

func (t *transportWrapperItem) Accept() (net.Conn, error) {
	a := <-t.acceptChannel
	return a.conn, a.err
}

func (t *transportWrapperItem) Close() error {
	t.logger.Warningf("closing transport wrapper item")
	// close(t.acceptChannel)
	t.closeCallback()
	return t.listener.Close()
}

func (t *transportWrapperItem) Addr() net.Addr {
	return t.listener.Addr()
}

func (t *transportWrapperItem) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	t.logger.Warningf("dialing [%s] via transport wrapper item", address)
	return t.listener.Dial(address, timeout)
}

type transportWrapperBase struct {
	transport     transportInterface
	raftTransport *transportWrapperItem
	rpcTransport  *transportWrapperItem
}

func (wrapper *transportWrapperBase) SetNodeID(id raft.ServerID) {
	wrapper.raftTransport.logger = wrapper.raftTransport.logger.Prefix(string(id))
	wrapper.rpcTransport.logger = wrapper.rpcTransport.logger.Prefix(string(id))
}

func newTransportWrapperFromListener(listener net.Listener) transportWrapper {
	ln := newTransportFromListener(listener)
	return newTransportWrapperEx(ln)
}

func newTransportWrapper(addr string) (transportWrapper, error) {
	ln, err := newTransport(addr)
	if err != nil {
		return nil, err
	}
	return newTransportWrapperEx(ln), nil
}

func newTransportWrapperEx(listener transportInterface) transportWrapper {
	wrapper := &transportWrapperBase{
		transport: listener,
		raftTransport: &transportWrapperItem{
			acceptChannel: make(chan accept, 0),
			logger:        timber.New(),
		},
		rpcTransport: &transportWrapperItem{
			acceptChannel: make(chan accept, 0),
			logger:        timber.New(),
		},
	}

	{
		wrapper.raftTransport.closeCallback = wrapper.closeCallback
		wrapper.raftTransport.listener = wrapper.transport
	}

	{
		wrapper.rpcTransport.closeCallback = wrapper.closeCallback
		wrapper.rpcTransport.listener = wrapper.transport
	}

	return wrapper
}

func (wrapper *transportWrapperBase) ForwardToRaft(conn net.Conn, err error) {
	wrapper.raftTransport.SendAccept(conn, err)
}

func (wrapper *transportWrapperBase) ForwardToRpc(conn net.Conn, err error) {
	wrapper.rpcTransport.SendAccept(conn, err)
}

func (wrapper *transportWrapperBase) closeCallback() {
	wrapper.rpcTransport.logger.Verbosef("received close callback")
}

func (wrapper *transportWrapperBase) RaftTransport() transportInterface {
	return wrapper.raftTransport
}

func (wrapper *transportWrapperBase) RpcTransport() transportInterface {
	return wrapper.rpcTransport
}

func (wrapper *transportWrapperBase) NormalTransport() net.Listener {
	return wrapper.transport
}

func (wrapper *transportWrapperBase) Port() int {
	addr, _ := net.ResolveTCPAddr("tcp", wrapper.Addr().String())
	return addr.Port
}

func (wrapper *transportWrapperBase) Addr() net.Addr {
	return wrapper.transport.Addr()
}

func (wrapper *transportWrapperBase) Close() {
	wrapper.raftTransport.Close()
}
