package meles

import (
	"github.com/elliotcourant/timber"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	timber.SetLevel(timber.Level_Verbose)
	os.Exit(m.Run())
}
