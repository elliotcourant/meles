package meles

import (
	"fmt"
	"github.com/elliotcourant/timber"
	"net"
)

type rpcDriver interface {
	ApplyTransaction(tx transactionStorage) error
	NextObjectID(objectPath []byte) (uint8, error)
	Discover() (*discoveryResponse, error)
	Join() error
	Close() error
}

type rpcDriverBase struct {
	boat   *boat
	logger timber.Logger
	w      rpcClientWire
	c      net.Conn
}

func (r *rpcDriverBase) Join() error {
	if err := r.w.Send(&joinRequest{
		NodeID:         string(r.boat.nodeId.RaftID()),
		Address:        r.boat.listenAddress,
		GenerateNodeId: r.boat.options.NumericNodeIds,
	}); err != nil {
		return err
	}

	for {
		receivedMsg, err := r.w.Receive()
		if err != nil {
			return err
		}

		switch msg := receivedMsg.(type) {
		case *joinResponse:
			return nil
		case *errorResponse:
			return msg.Error
		default:
			return fmt.Errorf("expected join response, received [%T]", msg)
		}
	}
}

func (r *rpcDriverBase) NextObjectID(objectPath []byte) (uint8, error) {
	if err := r.w.Send(&nextObjectIdRequest{
		ObjectPath: objectPath,
	}); err != nil {
		return 0, err
	}

	for {
		receivedMsg, err := r.w.Receive()
		if err != nil {
			return 0, err
		}

		switch msg := receivedMsg.(type) {
		case *nextObjectIdResponse:
			return msg.Identity, nil
		case *errorResponse:
			return 0, msg.Error
		default:
			return 0, fmt.Errorf("expected apply transactionBase response, received [%T]", msg)
		}
	}
}

func (r *rpcDriverBase) ApplyTransaction(tx transactionStorage) error {
	if err := r.w.Send(&applyTransactionRequest{
		NodeID:      r.boat.id,
		Transaction: &tx,
	}); err != nil {
		return err
	}

	for {
		receivedMsg, err := r.w.Receive()
		if err != nil {
			return err
		}

		switch msg := receivedMsg.(type) {
		case *applyTransactionResponse:
			return nil
		case *errorResponse:
			return msg.Error
		default:
			return fmt.Errorf("expected apply transactionBase response, received [%T]", msg)
		}
	}
}

func (r *rpcDriverBase) Discover() (*discoveryResponse, error) {
	if err := r.w.Send(&discoveryRequest{
		NodeID: r.boat.nodeId.RaftID(),
	}); err != nil {
		return nil, err
	}

	for {
		receivedMsg, err := r.w.Receive()
		if err != nil {
			return nil, err
		}

		switch msg := receivedMsg.(type) {
		case *discoveryResponse:
			return msg, nil
		case *errorResponse:
			return nil, msg.Error
		default:
			return nil, fmt.Errorf("expected discovery response, received [%T]", msg)
		}
	}
}

func (r *rpcDriverBase) Close() error {
	return r.c.Close()
}

func (r *boat) newRpcConnectionTo(addr string) (rpcDriver, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	w, err := newRpcClientWire(conn, conn)
	if err != nil {
		return nil, err
	}
	return &rpcDriverBase{
		boat:   r,
		logger: r.logger,
		w:      w,
		c:      conn,
	}, nil
}
