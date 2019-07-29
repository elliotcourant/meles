package meles

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateNodeID(t *testing.T) {
	id := GetShortID()
	assert.NotEmpty(t, id)
}
