package meles

type handshakeResponse struct{}

func (handshakeResponse) Server() {}

func (handshakeResponse) Encode() []byte {
	return make([]byte, 0)
}

func (handshakeResponse) Decode(src []byte) error {
	return nil
}
