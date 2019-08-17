package meles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWriteWireMessage(t *testing.T) {
	t.Run("appendEntriesRequest", func(t *testing.T) {
		b := writeWireMessage(&appendEntriesRequest{})
		assert.NotEmpty(t, b)
	})

	t.Run("msgAppendEntriesResponse", func(t *testing.T) {
		b := writeWireMessage(&appendEntriesResponse{})
		assert.NotEmpty(t, b)
	})

	t.Run("msgRequestVoteRequest", func(t *testing.T) {
		b := writeWireMessage(&requestVoteRequest{})
		assert.NotEmpty(t, b)
	})

	t.Run("msgRequestVoteResponse", func(t *testing.T) {
		b := writeWireMessage(&requestVoteResponse{})
		assert.NotEmpty(t, b)
	})

	t.Run("msgInstallSnapshotRequest", func(t *testing.T) {
		b := writeWireMessage(&installSnapshotRequest{})
		assert.NotEmpty(t, b)
	})

	t.Run("msgInstallSnapshotResponse", func(t *testing.T) {
		b := writeWireMessage(&installSnapshotResponse{})
		assert.NotEmpty(t, b)
	})

	t.Run("msgErrorResponse", func(t *testing.T) {
		b := writeWireMessage(&errorResponse{})
		assert.NotEmpty(t, b)
	})

	t.Run("panics", func(t *testing.T) {
		assert.Panics(t, func() {
			var test message
			writeWireMessage(test)
		})
	})
}
