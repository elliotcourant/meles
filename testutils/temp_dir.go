package testutils

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
)

func NewTempDirectory(t assert.TestingT) (string, CleanupFunction) {
	dir, err := ioutil.TempDir("", "meles")
	if !assert.NoError(t, err) {
		panic(err)
	}
	return dir, func() {
		if err := os.RemoveAll(dir); !assert.NoError(t, err) {
			panic(err)
		}
	}
}
