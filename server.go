package meles

import (
	"fmt"
	"github.com/elliotcourant/timber"
	"github.com/hashicorp/raft"
	"io"
	"net"
	"time"
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

		err = func(receivedMsg rpcClientMessage) error {
			switch msg := receivedMsg.(type) {
			case *applyTransactionRequest:
				i.logger.Verbosef("received apply transactionBase request from [%s]", msg.NodeID)

				if err := i.boat.apply(*msg.Transaction, nil); err != nil {
					return r.Send(&errorResponse{
						Error: err,
					})
				}
				return r.Send(&applyTransactionResponse{})
			case *discoveryRequest:
				myNodeId, newNode := i.boat.nodeId, i.boat.getIsNewNode()
				leaderAddr := raft.ServerAddress("")
				if !newNode {
					leaderAddr = i.boat.raft.Leader()
				}
				return r.Send(&discoveryResponse{
					NodeID:    myNodeId.RaftID(),
					IsNewNode: newNode,
					Leader:    leaderAddr,
				})
			case *incrementRequest:
				val, err := i.boat.NextIncrementId(msg.ObjectPath)
				if err != nil {
					return r.Send(&errorResponse{
						Error: err,
					})
				}

				return r.Send(&incrementResponse{
					Identity: val,
				})
			case *identityRequest:
				confFuture := i.boat.raft.GetConfiguration()
				if err := confFuture.Error(); err != nil {
					return err
				}
				maxId := uint64(len(confFuture.Configuration().Servers) + 1)

				val, err := i.boat.NextIncrementId(getNodeIncrementNodeIdPath())
				if err != nil {
					return r.Send(&errorResponse{
						Error: err,
					})
				}

				if maxId > val {
					for x := uint64(0); x < maxId; x++ {
						val, err = i.boat.NextIncrementId(getNodeIncrementNodeIdPath())
						if err != nil {
							return err
						}
						if val > maxId {
							break
						}
					}
				}

				return r.Send(&identityResponse{
					Id: val,
				})
			case *joinRequest:
				if !i.boat.IsLeader() {
					return r.Send(&errorResponse{
						Error: raft.ErrNotLeader,
					})
				}

				if err := i.boat.raft.VerifyLeader().Error(); err != nil {
					return r.Send(&errorResponse{
						Error: err,
					})
				}

				future := i.boat.raft.GetConfiguration()
				if err := future.Error(); err != nil {
					return r.Send(&errorResponse{
						Error: err,
					})
				}
				servers := future.Configuration().Servers
				if !i.boat.options.NumericNodeIds && msg.GenerateNodeId {
					return r.Send(&errorResponse{
						Error: fmt.Errorf("cannot generate numeric node Ids yet"),
					})
				}

				for _, peer := range servers {
					if string(peer.Address) == msg.Address {
						i.logger.Warningf("received join request from an existing peer [%s - %s]", peer.ID, peer.Address)
						return r.Send(&joinResponse{
							NodeID:        msg.NodeID,
							AlreadyJoined: true,
						})
					}

					if string(peer.ID) == msg.NodeID && !msg.GenerateNodeId {
						i.logger.Warningf("potential peer [%s] has duplicate nodeId [%s]", msg.Address, peer.ID)
						return r.Send(&errorResponse{
							Error: fmt.Errorf("a node with the provided Id [%s] already exists", msg.NodeID),
						})
					}
				}

				result := i.boat.raft.AddVoter(raft.ServerID(msg.NodeID), raft.ServerAddress(msg.Address), 0, time.Second*10)

				if err := result.Error(); err != nil {
					return r.Send(&errorResponse{
						Error: err,
					})
				}
				return r.Send(&joinResponse{NodeID: msg.NodeID})
			default:
				return r.Send(&errorResponse{
					Error: fmt.Errorf("invalid rpc message received [%d]", msg),
				})
			}
		}(receivedMsg)
		if err != nil {
			i.logger.Errorf("failed to handle message [%T]: %v", receivedMsg, err)
		}
	}
}
