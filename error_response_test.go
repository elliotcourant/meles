package meles

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestErrorResponse_Server(t *testing.T) {
	errorResponse{}.Server()
}

func TestErrorResponse(t *testing.T) {
	t.Run("encode and decode", func(t *testing.T) {
		i := errorResponse{
			Error: errors.New("test"),
		}
		encoded := i.Encode()
		fmt.Println(hex.Dump(encoded))
		d := errorResponse{}
		err := d.Decode(encoded)
		assert.NoError(t, err)
		assert.Equal(t, i, d)
	})
}
