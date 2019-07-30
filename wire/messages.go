package wire

type Message interface {
	Decode(src []byte)
	Encode() []byte
	EncodeMessage() []byte
}

type MessageType = byte

const (
	MsgHandshakeRequest  MessageType = 'h'
	MsgHandshakeResponse             = 'H'
)
