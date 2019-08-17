package distribution_test

import (
	"github.com/elliotcourant/timber"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	c := m.Run()
	os.Exit(c)
}

func TestThing(t *testing.T) {
	timber.Trace("test")
}
