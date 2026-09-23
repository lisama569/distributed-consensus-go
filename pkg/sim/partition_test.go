package sim

import (
	"testing"
	"time"

	"github.com/lisama569/distributed-consensus-go/pkg/coordinator"
)

// TestPartitionLeaderUniqueness is the core split-brain scenario:
//  1. elect a leader in a 5-node cluster;
//  2. isolate the leader with one follower (2 nodes = a minority);
//  3. the majority side must elect a new leader in a higher term;
//  4. the stale minority leader must fail every commit while partitioned;
//  5. after healing there is exactly one leader and fence tokens keep
//     increasing (the minority's uncommitted entries never take effect).
func TestPartitionLeaderUniqueness(t *testing.T) {
	ids := []string{"n1", "n2", "n3", "n4", "n5"}
	c := newTestCluster(t, ids...)
	logf := testingLogger(t)

	oldLeader, oldTerm, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("initial election: %v", err)
	}
	logf("initial leader=%s term=%d", oldLeader, oldTerm)

	// Lock the resource before the split so we can later prove the stale
	// leader cannot mint an additional fence token.
	resp, _, err := c.Submit(t.Context(), acq("r", "clientA", "req-pre", 60_000))
	if err != nil || !resp.OK {
		t.Fatalf("pre-split acquire: resp=%+v err=%v", resp, err)
	}
	tokenBefore := resp.Token
	logf("pre-split acquire: holder=clientA token=%d", tokenBefore)

	// Build a 2-node minority partition containing the leader.
	others := others(ids, oldLeader)
	minority := []string{oldLeader, others[0]}
	majority := others[1:] // 3 nodes

	c.Partition(minority)
	logf("minority partition installed: %v (majority: %v)", minority, majority)

	// The majority side elects a new leader in a strictly higher term.
	newLeader := waitForLeaderIn(t, c, majority, 3*time.Second)
	newTerm := c.Node(newLeader).Raft.Term()
	if newTerm <= oldTerm {
		t.Fatalf("new term must advance: old=%d new=%d", oldTerm, newTerm)
	}
	logf("majority elected new leader=%s term=%d (old leader=%s term=%d isolated)",
		newLeader, newTerm, oldLeader, oldTerm)

	// During the partition, the global view may contain the stale old
	// leader still thinking it is leader; assert it cannot commit.
	stale := c.Node(oldLeader).Raft
	proposeDone := make(chan error, 1)
	go func() {
		_, e := stale.Propose(t.Context(), jsonCmd(acq("r2", "rogue", "req-rogue", 60_000)))
		proposeDone <- e
	}()
	select {
	case e := <-proposeDone:
		if e == nil {
			t.Fatal("stale minority leader committed a proposal during partition")
		}
		logf("stale leader proposal rejected during partition: %v", e)
	case <-time.After(700 * time.Millisecond):
		logf("stale leader proposal blocked (no majority acks) for 700ms as required")
	}

	// Majority keeps serving: acquire must get the next strictly-greater
	// fence token through the new leader.
	resp2, servedBy, err := c.Submit(t.Context(), acq("r2", "clientB", "req-majority", 60_000), majority...)
	if err != nil || !resp2.OK {
		t.Fatalf("majority acquire during split: resp=%+v err=%v", resp2, err)
	}
	if resp2.Token <= tokenBefore {
		t.Fatalf("fence token must strictly increase: before=%d after=%d", tokenBefore, resp2.Token)
	}
	logf("majority acquire served by leader=%s term=%d: holder=clientB token=%d (strictly > %d)",
		servedBy, c.Node(servedBy).Raft.Term(), resp2.Token, tokenBefore)

	// Heal and prove a single leader remains.
	c.Heal()
	time.Sleep(300 * time.Millisecond)
	leaders := 0
	var sole string
	for _, id := range ids {
		if c.Node(id).Raft.IsLeader() {
			leaders++
			sole = id
		}
	}
	if leaders != 1 {
		t.Fatalf("after heal: want exactly 1 leader, got %d", leaders)
	}
	logf("after heal: exactly one leader=%s term=%d — no split brain", sole, c.Node(sole).Raft.Term())

	// The old leader must have stepped down and its rogue command be absent.
	if stale.IsLeader() {
		t.Fatalf("stale leader %s still leader after heal", oldLeader)
	}
	if lk, ok := c.SnapshotLock(oldLeader, "r2"); ok && lk.Holder == "rogue" {
		t.Fatal("rogue lock from minority became visible after heal")
	}
	logf("rogue minority command absent after heal; decision basis: only majority-committed log entries apply")
}

func waitForLeaderIn(t *testing.T, c *Cluster, subset []string, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range subset {
			if c.Node(id).Raft.IsLeader() {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader elected within subset %v", subset)
	return ""
}

func others(ids []string, exclude string) []string {
	out := []string{}
	for _, id := range ids {
		if id != exclude {
			out = append(out, id)
		}
	}
	return out
}

func testingLogger(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		t.Helper()
		t.Logf(format, args...)
	}
}

func jsonCmd(cmd coordinator.Command) []byte {
	b, _ := encodeCmd(cmd)
	return b
}
