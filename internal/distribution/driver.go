package distribution

import (
	"fmt"
	"github.com/elliotcourant/arkdb/pkg/wire"
	"github.com/elliotcourant/meles/internal/storage"
	"github.com/elliotcourant/timber"
	"net"
)

type rpcDriver interface {
	ApplyTransaction(tx storage.Transaction) error
	NextObjectID(objectPath []byte) (uint8, error)
	Close() error
}

type rpcDriverBase struct {
	boat   *boat
	logger timber.Logger
	w      wire.RpcClientWire
	c      net.Conn
}

func (r *rpcDriverBase) NextObjectID(objectPath []byte) (uint8, error) {
	if err := r.w.Send(&wire.NextObjectIdRequest{
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
		case *wire.NextObjectIdResponse:
			return msg.Identity, nil
		case *wire.ErrorResponse:
			return 0, msg.Error
		default:
			return 0, fmt.Errorf("expected apply transaction response, received [%T]", msg)
		}
	}
}

func (r *rpcDriverBase) ApplyTransaction(tx storage.Transaction) error {
	if err := r.w.Send(&wire.ApplyTransactionRequest{
		NodeID:      id,
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
		case *wire.ApplyTransactionResponse:
			return nil
		case *wire.ErrorResponse:
			return msg.Error
		default:
			return fmt.Errorf("expected apply transaction response, received [%T]", msg)
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
	w, err := wire.NewRpcClientWire(conn, conn)
	if err != nil {
		return nil, err
	}
	return &rpcDriverBase{
		boat:   r,
		logger: logger,
		w:      w,
		c:      conn,
	}, nil
}
