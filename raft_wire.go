package meles

import (
	"fmt"
	"github.com/elliotcourant/buffers"
	"github.com/jackc/pgx/chunkreader"
	"io"
)

func newRpcClientWire(r io.ReadCloser, w io.WriteCloser) (rpcClientWire, error) {
	wr := newClientWire(r, w)

	err := wr.HandshakeRpc()
	if err != nil {
		return nil, err
	}

	return &rpcClientWireBase{
		clientWire: wr,
	}, nil
}

func newRpcServerWire(r io.ReadCloser, w io.WriteCloser) rpcServerWire {
	return &rpcServerWireBase{
		serverWire: newServerWire(r, w),
	}
}

type rpcClientWireBase struct {
	clientWire
}

func (r *rpcClientWireBase) Send(msg rpcClientMessage) error {
	return r.clientWire.Send(msg)
}

func (r *rpcClientWireBase) Receive() (rpcServerMessage, error) {
	msg, err := r.clientWire.Receive()
	if err != nil {
		return nil, err
	}
	raftMsg, ok := msg.(rpcServerMessage)
	if !ok {
		return nil, fmt.Errorf("expected rpc server message, received [%T]", msg)
	}
	return raftMsg, nil
}

type rpcServerWireBase struct {
	serverWire
}

func (r *rpcServerWireBase) Send(msg rpcServerMessage) error {
	return r.serverWire.Send(msg)
}

func (r *rpcServerWireBase) Receive() (rpcClientMessage, error) {
	msg, err := r.serverWire.Receive()
	if err != nil {
		return nil, err
	}
	raftMsg, ok := msg.(rpcClientMessage)
	if !ok {
		return nil, fmt.Errorf("expected rpc client message, received [%T]", msg)
	}
	return raftMsg, nil
}

func newRaftClientWire(r io.ReadCloser, w io.WriteCloser) (raftClientWire, error) {
	wr := newClientWire(r, w)

	err := wr.HandshakeRaft()
	if err != nil {
		return nil, err
	}

	return &raftClientWireBase{
		clientWire: wr,
	}, nil
}

func newRaftServerWire(r io.ReadCloser, w io.WriteCloser) raftServerWire {
	return &raftServerWireBase{
		serverWire: newServerWire(r, w),
	}
}

type raftClientWireBase struct {
	clientWire
}

func (r *raftClientWireBase) Send(msg raftClientMessage) error {
	return r.clientWire.Send(msg)
}

func (r *raftClientWireBase) Receive() (raftServerMessage, error) {
	msg, err := r.clientWire.Receive()
	if err != nil {
		return nil, err
	}
	raftMsg, ok := msg.(raftServerMessage)
	if !ok {
		return nil, fmt.Errorf("expected raft server message, received [%T]", msg)
	}
	return raftMsg, nil
}

type raftServerWireBase struct {
	serverWire
}

func (r *raftServerWireBase) Send(msg raftServerMessage) error {
	return r.serverWire.Send(msg)
}

func (r *raftServerWireBase) Receive() (raftClientMessage, error) {
	msg, err := r.serverWire.Receive()
	if err != nil {
		return nil, err
	}
	raftMsg, ok := msg.(raftClientMessage)
	if !ok {
		return nil, fmt.Errorf("expected raft client message, received [%T]", msg)
	}
	return raftMsg, nil
}

func newClientWire(r io.ReadCloser, w io.WriteCloser) clientWire {
	cr := chunkreader.NewChunkReader(r)
	return &clientWireBase{
		cr: cr,
		r:  r,
		w:  w,
	}
}

func newServerWire(r io.ReadCloser, w io.WriteCloser) serverWire {
	cr := chunkreader.NewChunkReader(r)
	return &serverWireBase{
		cr: cr,
		r:  r,
		w:  w,
	}
}

type serverWireBase struct {
	cr         *chunkreader.ChunkReader
	r          io.ReadCloser
	w          io.WriteCloser
	bodyLen    int32
	msgType    clientMessageType
	partialMsg bool
}

func (r *serverWireBase) Send(msg serverMessage) error {
	_, err := r.w.Write(writeWireMessage(msg))
	return err
}

func (r *serverWireBase) Receive() (clientMessage, error) {
	if !r.partialMsg {
		header, err := r.cr.Next(5)
		if err != nil {
			return nil, err
		}

		buf := buffers.NewBytesReader(header)
		r.msgType = buf.NextUint8()
		r.bodyLen = buf.NextInt32() - 4
	}

	msgBody, err := r.cr.Next(int(r.bodyLen))
	if err != nil {
		return nil, err
	}

	var msg clientMessage
	switch r.msgType {
	case msgAppendEntriesRequest:
		msg = &appendEntriesRequest{}
	case msgRequestVoteRequest:
		msg = &requestVoteRequest{}
	case msgInstallSnapshotRequest:
		msg = &installSnapshotRequest{}
	case msgHandshakeRequest:
		msg = &handshakeRequest{}
	case msgDiscoveryRequest:
		msg = &discoveryRequest{}
	case msgApplyTransactionRequest:
		msg = &applyTransactionRequest{}
	case msgIncrementRequest:
		msg = &incrementRequest{}
	case msgJoinRequest:
		msg = &joinRequest{}
	case msgIdentityRequest:
		msg = &identityRequest{}
	case msgDistIdentityRequest:
		msg = &distSequenceRequest{}

	default:
		return nil, fmt.Errorf("failed to handle client message of with header [%s]", string(r.msgType))
	}

	r.partialMsg = false

	err = msg.Decode(msgBody)

	return msg, err
}

type clientWireBase struct {
	cr         *chunkreader.ChunkReader
	r          io.ReadCloser
	w          io.WriteCloser
	bodyLen    int32
	msgType    serverMessageType
	partialMsg bool
}

func (r *clientWireBase) HandshakeRaft() error {
	return r.handshake(raftIntention)
}

func (r *clientWireBase) HandshakeRpc() error {
	return r.handshake(rpcIntention)
}

func (r *clientWireBase) handshake(intention handshakeIntention) error {
	if err := r.Send(&handshakeRequest{
		Intention: intention,
	}); err != nil {
		return err
	}

	for {
		receivedMsg, err := r.Receive()
		if err != nil {
			return err
		}

		switch msg := receivedMsg.(type) {
		case *handshakeResponse:
			return nil
		case *errorResponse:
			return msg.Error
		default:
			return fmt.Errorf("expected handshake response received [%T]", msg)
		}
	}
}

func (r *clientWireBase) Send(msg clientMessage) error {
	_, err := r.w.Write(writeWireMessage(msg))
	return err
}

func (r *clientWireBase) Receive() (serverMessage, error) {
	if !r.partialMsg {
		header, err := r.cr.Next(5)
		if err != nil {
			return nil, err
		}

		buf := buffers.NewBytesReader(header)
		r.msgType = buf.NextUint8()
		r.bodyLen = buf.NextInt32() - 4
	}

	msgBody, err := r.cr.Next(int(r.bodyLen))
	if err != nil {
		return nil, err
	}

	var msg serverMessage
	switch r.msgType {
	case msgAppendEntriesResponse:
		msg = &appendEntriesResponse{}
	case msgRequestVoteResponse:
		msg = &requestVoteResponse{}
	case msgInstallSnapshotResponse:
		msg = &installSnapshotResponse{}
	case msgErrorResponse:
		msg = &errorResponse{}
	case msgHandshakeResponse:
		msg = &handshakeResponse{}
	case msgDiscoveryResponse:
		msg = &discoveryResponse{}
	case msgApplyTransactionResponse:
		msg = &applyTransactionResponse{}
	case msgIncrementResponse:
		msg = &incrementResponse{}
	case msgJoinResponse:
		msg = &joinResponse{}
	case msgIdentityResponse:
		msg = &identityResponse{}
	case msgDistIdentityResponse:
		msg = &distSequenceResponse{}

	default:
		return nil, fmt.Errorf("failed to handle server message of with header [%s]", string(r.msgType))
	}

	r.partialMsg = false
	err = msg.Decode(msgBody)

	return msg, err
}
