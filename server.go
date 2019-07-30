package meles

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/elliotcourant/meles/wire"
	"github.com/elliotcourant/timber"
	"io"
	"net"
)

func closedError(listener net.Listener) string {
	return fmt.Sprintf(
		"close tcp %s: use of closed network connection",
		listener.Addr().String())
}

func (s *store) listen(listener net.Listener) error {
	defer func() {
		if err := listener.Close(); err != nil {
			e := err.Error()
			c := closedError(listener)
			if s.IsShutdown() && e == c {
				// This will happen when the store is closed.
				return
			}
			s.log.Errorf("failed to close listener: %v", err)
		}
	}()

	for !s.IsShutdown() {
		conn, err := listener.Accept()
		if err != nil {
			// If the server is shutdown then we do not need to throw an error, the error is
			// almost certainly due to stuff being closed.
			if s.IsShutdown() {
				continue
			}

			s.log.Errorf("failed to accept connection: %v", err)
			return err
		}

		go func(conn net.Conn) {
			defer func() {
				if err := conn.Close(); err != nil {
					s.log.Errorf("failed to close listener: %v", err)
				}
			}()

			remoteAddr := conn.RemoteAddr().String()
			friendlyAddress, err := resolveAddress(remoteAddr)
			if err != nil {
				s.log.Warningf("could not make address [%s] friendly: %v", remoteAddr, err)
				friendlyAddress = remoteAddr
			}

			log := s.log.With(timber.Keys{
				"c-peer": friendlyAddress,
			})

			log.Tracef("serving connection from [%s]", friendlyAddress)

			if err := s.serveConnection(conn, conn, log); err != nil {
				if err == io.EOF {
					return
				}
				log.Errorf("failed to serve connection: %v", err)
				return
			}
		}(conn)
	}

	return nil
}

func (s *store) serveConnection(reader io.Reader, writer io.Writer, log timber.Logger) error {
	for {
		header := make([]byte, 5)
		if _, err := reader.Read(header); err != nil {
			return err
		}

		msgType := header[0]
		size := binary.BigEndian.Uint32(header[1:])

		body := make([]byte, size)
		if _, err := reader.Read(body); err != nil {
			return err
		}

		var err error

		switch msgType {
		case wire.MsgHandshakeRequest:
			log.Debugf("received handshake request from [%d]", binary.BigEndian.Uint64(body))
			_, err = writer.Write((&wire.HandshakeResponse{
				ID: s.id,
			}).EncodeMessage())
		default:
			log.Errorf("received unknown message type [%v] with body: %s", msgType, string(body))
		}

		if err != nil {
			log.Errorf("could not send response: %v", err)
		}
	}
}

// ExternalIP returns the first outward facing address available.
func externalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", fmt.Errorf("are you connected to the network")
}

func resolveAddress(input string) (string, error) {
	inputParsed, err := net.ResolveTCPAddr("tcp", input)
	if err != nil {
		return "", err
	}
	// If they have specified an IP address to do the things, then prefer that
	// But if there is no IP address then default to our primary external IP.
	if inputParsed.IP != nil && !bytes.Equal(inputParsed.IP, make([]byte, 16)) {
		return inputParsed.String(), nil
	}

	// But if they haven't specified an IP address then we want to assume.
	assumedAddr, err := externalIP()
	if err != nil {
		return "", err
	}
	inputParsed, _ = net.ResolveTCPAddr("tcp", fmt.Sprintf("%s:%d", assumedAddr, inputParsed.Port))
	return inputParsed.String(), nil
}
