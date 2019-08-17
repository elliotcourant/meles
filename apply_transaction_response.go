package meles

type applyTransactionResponse struct{}

func (applyTransactionResponse) Server() {}

func (applyTransactionResponse) RPC() {}

func (applyTransactionResponse) Encode() []byte {
	return make([]byte, 0)
}

func (applyTransactionResponse) Decode(src []byte) error {
	return nil
}
