package driver

import (
	"encoding/binary"
	"fmt"
	"github.com/elliotcourant/meles/buffers"
	"github.com/elliotcourant/meles/wire"
	"github.com/elliotcourant/timber"
	"github.com/kataras/go-errors"
	"net"
)

type MelesDriver interface {
	Handshake(id uint64) (*wire.HandshakeResponse, error)
}

type driver struct {
	conn net.Conn
	log  timber.Logger
}

func NewDriver(conn net.Conn, log timber.Logger) MelesDriver {
	return &driver{
		conn: conn,
		log:  log,
	}
}

func (d *driver) Handshake(id uint64) (*wire.HandshakeResponse, error) {
	b := buffers.NewBytesBuffer()
	b.AppendUint64(id)
	buf := buffers.NewBytesBuffer()
	buf.AppendUint8(wire.MsgHandshakeRequest)
	buf.Append(b.Bytes()...)
	if _, err := d.conn.Write(buf.Bytes()); err != nil {
		return nil, err
	}
	result, err := d.getResponse()
	if err != nil {
		return nil, err
	}
	response, ok := result.(*wire.HandshakeResponse)
	if !ok {
		return nil, fmt.Errorf("received invalid response [%T]", result)
	}
	return response, nil
}

func (d *driver) getResponse() (wire.Message, error) {
	header := make([]byte, 5)
	if _, err := d.conn.Read(header); err != nil {
		return nil, err
	}

	msgType := header[0]
	size := binary.BigEndian.Uint32(header[1:])

	body := make([]byte, size)
	if _, err := d.conn.Read(body); err != nil {
		return nil, err
	}

	var msg wire.Message

	switch msgType {
	case wire.MsgHandshakeResponse:
		msg = &wire.HandshakeResponse{}
	default:
		e := fmt.Sprintf("received unknown message type [%v] with body: %s", msgType, string(body))
		d.log.Errorf(e)
		return nil, errors.New(e)
	}

	msg.Decode(body)
	return msg, nil
}
