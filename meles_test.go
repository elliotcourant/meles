package meles_test

import (
	"github.com/elliotcourant/meles"
	"github.com/elliotcourant/meles/testutils"
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestNewStore(t *testing.T) {
	tempDir, cleanup := testutils.NewTempDirectory(t)
	defer cleanup()

	log := timber.With(timber.Keys{
		"test": t.Name(),
	})

	l1, err := net.Listen("tcp", ":")
	assert.NoError(t, err)

	s1, err := meles.NewStore(l1, log, meles.Options{
		Directory: tempDir,
	})
	assert.NoError(t, err)

	err = s1.Start()
	assert.NoError(t, err)

	assert.False(t, s1.IsStopped())
	assert.True(t, s1.IsLeader())
}
