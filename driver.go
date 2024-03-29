package meles

import (
	"fmt"
	"github.com/elliotcourant/timber"
	"net"
)

type rpcDriver interface {
	ApplyTransaction(tx transactionStorage) error
	NextObjectID(objectPath []byte) (uint64, error)
	NextSequenceChunk(sequenceName string) (*distSequenceChunk, error)
	Discover() (*discoveryResponse, error)
	GetIdentity() (uint64, error)
	Join() error
	Close() error
}

type rpcDriverBase struct {
	boat   *boat
	logger timber.Logger
	w      rpcClientWire
	c      net.Conn
}

func (r *rpcDriverBase) NextSequenceChunk(sequenceName string) (*distSequenceChunk, error) {
	if err := r.w.Send(&distSequenceRequest{
		SequenceName: sequenceName,
	}); err != nil {
		return nil, err
	}

	for {
		receivedMsg, err := r.w.Receive()
		if err != nil {
			return nil, err
		}

		switch msg := receivedMsg.(type) {
		case *distSequenceResponse:
			return &msg.distSequenceChunk, nil
		case *errorResponse:
			return nil, msg.Error
		default:
			return nil, fmt.Errorf("expected identity response, received [%T]", msg)
		}
	}
}

func (r *rpcDriverBase) Join() error {
	if err := r.w.Send(&joinRequest{
		NodeID:         string(r.boat.id),
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

func (r *rpcDriverBase) GetIdentity() (uint64, error) {
	if err := r.w.Send(&identityRequest{}); err != nil {
		return 0, err
	}

	for {
		receivedMsg, err := r.w.Receive()
		if err != nil {
			return 0, err
		}

		switch msg := receivedMsg.(type) {
		case *identityResponse:
			return msg.Id, nil
		case *errorResponse:
			return 0, msg.Error
		default:
			return 0, fmt.Errorf("expected identity response, received [%T]", msg)
		}
	}
}

func (r *rpcDriverBase) NextObjectID(objectPath []byte) (uint64, error) {
	if err := r.w.Send(&incrementRequest{
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
		case *incrementResponse:
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
		NodeID: r.boat.id,
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
