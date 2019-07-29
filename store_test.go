package meles

import (
	"github.com/elliotcourant/meles/internal/testutils"
	"testing"
)

func TestNewStore(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		dir, cleanup := testutils.TempDirectory(t)
		defer cleanup()
		NewStore(nil, nil, dir, false)
	})
}
