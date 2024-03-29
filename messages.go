package meles

import (
	"fmt"
	"github.com/elliotcourant/buffers"
)

type (
	clientMessageType = byte
	serverMessageType = byte
)

// Client message Types
const (
	msgAppendEntriesRequest    clientMessageType = 'a'
	msgRequestVoteRequest      clientMessageType = 'v'
	msgInstallSnapshotRequest  clientMessageType = 'i'
	msgDiscoveryRequest        clientMessageType = 'd'
	msgHandshakeRequest        clientMessageType = 'h'
	msgApplyTransactionRequest clientMessageType = 't'
	msgIncrementRequest        clientMessageType = 'n'
	msgJoinRequest             clientMessageType = 'j'
	msgIdentityRequest         clientMessageType = '1'
	msgDistIdentityRequest     clientMessageType = '2'
)

const (
	msgAppendEntriesResponse    serverMessageType = 'A'
	msgRequestVoteResponse      serverMessageType = 'V'
	msgInstallSnapshotResponse  serverMessageType = 'I'
	msgDiscoveryResponse        serverMessageType = 'D'
	msgHandshakeResponse        serverMessageType = 'H'
	msgApplyTransactionResponse serverMessageType = 'T'
	msgIncrementResponse        serverMessageType = 'N'
	msgJoinResponse             serverMessageType = 'J'
	msgIdentityResponse         serverMessageType = '!'
	msgDistIdentityResponse     serverMessageType = '@'

	msgErrorResponse serverMessageType = 'E'
)

type message interface {
	Encode() []byte
	Decode(src []byte) error
}

type clientMessage interface {
	message
	Client()
}

type raftClientMessage interface {
	clientMessage
	Raft()
}

type rpcClientMessage interface {
	clientMessage
	RPC()
}

type serverMessage interface {
	message
	Server()
}

type raftServerMessage interface {
	serverMessage
	Raft()
}

type rpcServerMessage interface {
	serverMessage
	RPC()
}

type clientWire interface {
	Send(msg clientMessage) error
	Receive() (serverMessage, error)
	HandshakeRaft() error
	HandshakeRpc() error
}

type serverWire interface {
	Send(msg serverMessage) error
	Receive() (clientMessage, error)
}

type raftClientWire interface {
	Send(msg raftClientMessage) error
	Receive() (raftServerMessage, error)
}

type raftServerWire interface {
	Send(msg raftServerMessage) error
	Receive() (raftClientMessage, error)
}

type rpcClientWire interface {
	Send(msg rpcClientMessage) error
	Receive() (rpcServerMessage, error)
}

type rpcServerWire interface {
	Send(msg rpcServerMessage) error
	Receive() (rpcClientMessage, error)
}

func writeWireMessage(msg message) []byte {
	buf := buffers.NewBytesBuffer()
	switch msg.(type) {
	case *appendEntriesRequest:
		buf.AppendByte(msgAppendEntriesRequest)
	case *requestVoteRequest:
		buf.AppendByte(msgRequestVoteRequest)
	case *installSnapshotRequest:
		buf.AppendByte(msgInstallSnapshotRequest)
	case *discoveryRequest:
		buf.AppendByte(msgDiscoveryRequest)
	case *handshakeRequest:
		buf.AppendByte(msgHandshakeRequest)
	case *applyTransactionRequest:
		buf.AppendByte(msgApplyTransactionRequest)
	case *incrementRequest:
		buf.AppendByte(msgIncrementRequest)
	case *joinRequest:
		buf.AppendByte(msgJoinRequest)
	case *identityRequest:
		buf.AppendByte(msgIdentityRequest)
	case *distSequenceRequest:
		buf.AppendByte(msgDistIdentityRequest)

	case *appendEntriesResponse:
		buf.AppendByte(msgAppendEntriesResponse)
	case *requestVoteResponse:
		buf.AppendByte(msgRequestVoteResponse)
	case *installSnapshotResponse:
		buf.AppendByte(msgInstallSnapshotResponse)
	case *discoveryResponse:
		buf.AppendByte(msgDiscoveryResponse)
	case *handshakeResponse:
		buf.AppendByte(msgHandshakeResponse)
	case *applyTransactionResponse:
		buf.AppendByte(msgApplyTransactionResponse)
	case *incrementResponse:
		buf.AppendByte(msgIncrementResponse)
	case *joinResponse:
		buf.AppendByte(msgJoinResponse)
	case *identityResponse:
		buf.AppendByte(msgIdentityResponse)
	case *distSequenceResponse:
		buf.AppendByte(msgDistIdentityResponse)

	case *errorResponse:
		buf.AppendByte(msgErrorResponse)
	default:
		panic(fmt.Sprintf("unrecognized message type for wire [%T]", msg))
	}

	buf.Append(msg.Encode()...)
	return buf.Bytes()
}
