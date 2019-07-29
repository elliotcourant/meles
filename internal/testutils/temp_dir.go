package testutils

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func CleanTestName(t *testing.T) string {
	return strings.ReplaceAll(t.Name(), "/", "")
}

func TempDirectory(t *testing.T) (string, func()) {
	dir, err := ioutil.TempDir("", CleanTestName(t))
	if !assert.NoError(t, err) {
		panic(err)
	}
	return dir, func() {
		if err := os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}
}
