package meles

type identityRequest struct {
}

func (identityRequest) Client() {}

func (identityRequest) RPC() {}

func (i *identityRequest) Encode() []byte {
	return make([]byte, 0)
}

func (i *identityRequest) Decode(src []byte) error {
	return nil
}
