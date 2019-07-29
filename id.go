package meles

import (
	"github.com/teris-io/shortid"
)

const defaultAbc = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ@-"

var (
	shortIdentifier *shortid.Shortid
)

func init() {
	shortIdentifier = shortid.MustNew(0, defaultAbc, 1)
}

func GetShortID() string {
	id, _ := shortIdentifier.Generate()
	return id
}
