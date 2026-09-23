package sim

import (
	"testing"
	"time"

	"github.com/lisama569/distributed-consensus-go/pkg/consensus"
)

func newTestCluster(t *testing.T, ids ...string) *Cluster {
	t.Helper()
	cfg := consensus.DefaultConfig()
	c := NewCluster(ids, t.TempDir(), cfg, nil)
	t.Cleanup(c.StopAll)
	c.StartAll()
	return c
}

func TestSmokeElectLeader(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	id, term, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}
	t.Logf("leader=%s term=%d", id, term)
	if term < 1 {
		t.Fatalf("term should be >=1, got %d", term)
	}
}

func TestSmokeReplicateLock(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	id, term, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("no leader: %v", err)
	}
	t.Logf("leader=%s term=%d", id, term)
	resp, leader, err := c.Submit(t.Context(), mustAcquire("resA", "holder-1", "req-1", 10000))
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	t.Logf("acquire resp=%+v leader=%s", resp, leader)
	if !resp.OK || resp.Token != 1 {
		t.Fatalf("expected ok token=1, got %+v", resp)
	}
	time.Sleep(200 * time.Millisecond)
	for _, n := range c.IDs() {
		lk, ok := c.SnapshotLock(n, "resA")
		if !ok {
			t.Fatalf("node %s did not apply lock", n)
		}
		t.Logf("node %s view: holder=%s token=%d until=%d", n, lk.Holder, lk.Token, lk.LeaseUntilMS)
	}
}
