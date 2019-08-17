package meles_test

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	c := m.Run()
	os.Exit(c)
}
