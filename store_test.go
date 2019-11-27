package meles

import (
	"github.com/elliotcourant/timber"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func VerifyLeader(t *testing.T, nodes ...barge) {
	timber.Infof("verifying leader for %d node(s)", len(nodes))
	start := time.Now()
	maxRetry := 5
	retries := 0
TryAgain:
	leaderAddr := ""
	for _, node := range nodes {
		if node.IsStopped() {
			continue
		}
		addr, _, err := node.WaitForLeader(time.Second * 10)
		assert.NoError(t, err)
		if leaderAddr == "" {
			leaderAddr = addr
		} else if leaderAddr != addr {
			if retries < maxRetry {
				retries++
				time.Sleep(10 * time.Second)
				goto TryAgain
			}
		}
		assert.Equal(t, leaderAddr, addr, "node [%s] does not match expected leader", node.NodeID())
	}
	timber.Infof("current leader [%s] verification time: %s retries: %d", leaderAddr, time.Since(start), retries)
}
