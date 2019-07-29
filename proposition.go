package meles

type proposition struct {
	Key    []byte
	Value  []byte
	Result chan<- error
}

func (p *proposition) Finished() {
	p.Result <- nil
}

func (p *proposition) FinishedWithError(err error) {
	p.Result <- err
}
