package meles

import (
	"fmt"
	"github.com/elliotcourant/timber"
	"io"
	"net"
)

type masterServer struct {
	boat   *boat
	logger timber.Logger
	ln     transportWrapper
}

func (i *masterServer) runMasterServer() {
	go func(i *masterServer) {
		t := i.ln.NormalTransport()
		for {
			conn, err := t.Accept()
			if err != nil && !i.boat.IsStopped() {
				i.logger.Errorf("failed to accept connection: %v", err)
			}

			if !i.boat.IsStopped() {
				go func(i *masterServer, conn net.Conn) {
					if err := i.handleMasterConn(conn); err != nil {
						i.logger.Errorf("failed to handle connection: %v", err)
					}
				}(i, conn)
			} else {
				return
			}
		}
	}(i)
}

func (i *masterServer) handleMasterConn(conn net.Conn) error {
	w := newServerWire(conn, conn)

	receivedMsg, err := w.Receive()
	if err != nil {
		return err
	}

	switch msg := receivedMsg.(type) {
	case *handshakeRequest:
		switch msg.Intention {
		case raftIntention:
			if err := w.Send(&handshakeResponse{}); err != nil {
				return err
			}
			i.ln.ForwardToRaft(conn, nil)
		case rpcIntention:
			if err := w.Send(&handshakeResponse{}); err != nil {
				return err
			}
			i.ln.ForwardToRpc(conn, nil)
		default:
			e := fmt.Errorf("invalid intention received [%d]", msg.Intention)
			if err := w.Send(&errorResponse{
				Error: e,
			}); err != nil {
				return err
			}
			return e
		}
	default:
		return fmt.Errorf("invalid startup message received [%T]", msg)
	}

	return nil
}

type boatServer struct {
	boat   *boat
	logger timber.Logger
	ln     transportInterface
}

func (i *boatServer) runBoatServer() {
	go func(i *boatServer) {
		for {
			conn, err := i.ln.Accept()
			if err != nil {
				i.logger.Errorf("failed to accept connection: %v", err)
			}

			go func(i *boatServer, conn net.Conn) {
				if err := i.handleConn(conn); err != nil && err != io.EOF {
					i.logger.Errorf("failed to handle connection: %v", err)
				}
			}(i, conn)
		}
	}(i)
}

func (i *boatServer) handleConn(conn net.Conn) error {
	r := newRpcServerWire(conn, conn)

	for {
		receivedMsg, err := r.Receive()
		if err != nil {
			return err
		}

		switch msg := receivedMsg.(type) {
		case *applyTransactionRequest:
			i.logger.Verbosef("received apply transactionBase request from [%s]", msg.NodeID)
			e := i.boat.apply(*msg.Transaction, nil)
			if e != nil {
				if err := r.Send(&errorResponse{
					Error: e,
				}); err != nil {
					return err
				}
			} else {
				if err := r.Send(&applyTransactionResponse{}); err != nil {
					return err
				}
			}
		case *discoveryRequest:
			myNodeId, newNode := i.boat.nodeId, i.boat.newNode
			response := &discoveryResponse{
				NodeID:    myNodeId.RaftID(),
				IsNewNode: newNode,
			}
			if err := r.Send(response); err != nil {
				return err
			}
		case *nextObjectIdRequest:
			val, e := i.boat.NextObjectID(msg.ObjectPath)
			if e != nil {
				if err := r.Send(&errorResponse{
					Error: e,
				}); err != nil {
					return err
				}
			} else {
				if err := r.Send(&nextObjectIdResponse{
					Identity: val,
				}); err != nil {
					return err
				}
			}
		default:
			e := fmt.Errorf("invalid rpc message received [%d]", msg)
			if err := r.Send(&errorResponse{
				Error: e,
			}); err != nil {
				return err
			}
			return e
		}
	}
}
