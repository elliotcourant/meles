package meles

type Options struct {
	Directory string
	Peers     []string
}

type Store struct {
	db barge
}

func NewStore(options *Options) *Store {
	return nil
}
