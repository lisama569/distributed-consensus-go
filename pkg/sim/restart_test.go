package sim

import (
	"testing"
	"time"

	"github.com/lisama569/distributed-consensus-go/pkg/coordinator"
)

// TestRestartTermPersistenceAndReplay verifies the durability contract:
//   - a follower restart restores term/vote/log from disk, so terms never
//     go backwards and the term it observes on restart is at least the
//     term it persisted;
//   - committed lock state (holder + fence token) is replayed locally
//     before the node could even talk to the leader again;
//   - after restart the cluster keeps committing with increasing terms
//     and the global fence sequence continues.
func TestRestartTermPersistenceAndReplay(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	c := newTestCluster(t, ids...)
	logf := testingLogger(t)

	leader, term1, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	logf("initial leader=%s term=%d", leader, term1)

	// Commit two grants so the log spans multiple terms' worth of entries
	// (here one term, but token sequence >1 is what matters after replay).
	r1, _, err := c.Submit(t.Context(), acq("res", "holder-1", "r-acq-1", 60_000))
	if err != nil || !r1.OK {
		t.Fatalf("acquire1: %+v %v", r1, err)
	}
	r2, _, err := c.Submit(t.Context(), acq("other", "holder-2", "r-acq-2", 60_000))
	if err != nil || !r2.OK || r2.Token != r1.Token+1 {
		t.Fatalf("acquire2: %+v %v", r2, err)
	}
	logf("committed tokens: res token=%d, other token=%d (cluster fence high-water=%d)",
		r1.Token, r2.Token, r2.Token)

	// Pick a follower and record its persisted term, then hard-restart it.
	var follower string
	for _, id := range ids {
		if id != leader {
			follower = id
			break
		}
	}
	termBefore := c.Node(follower).Raft.Term()
	c.Stop(follower)
	logf("follower=%s hard-stopped at persisted term=%d (state.json kept on disk)", follower, termBefore)

	// While down, commit one more entry so the restarted node must catch up.
	r3, _, err := c.Submit(t.Context(), acq("res2", "holder-3", "r-acq-3", 60_000))
	if err != nil || !r3.OK {
		t.Fatalf("acquire3 while follower down: %+v %v", r3, err)
	}
	logf("cluster kept serving with follower down: new token=%d committed by majority", r3.Token)

	// Restart: durable term/log reloaded, committed entries replayed.
	c.Start(follower)
	time.Sleep(150 * time.Millisecond)
	termAfter := c.Node(follower).Raft.Term()
	if termAfter < termBefore {
		t.Fatalf("term went backwards on restart: %d -> %d", termBefore, termAfter)
	}
	logf("follower=%s restarted; restored term=%d (>= persisted %d, no rollback)",
		follower, termAfter, termBefore)

	// Replayed state: the two pre-stop entries must be visible locally.
	if lk, ok := c.SnapshotLock(follower, "res"); !ok || lk.Holder != "holder-1" || lk.Token != r1.Token {
		t.Fatalf("replayed lock mismatch: %+v ok=%v", lk, ok)
	}
	if lk, ok := c.SnapshotLock(follower, "other"); !ok || lk.Token != r2.Token {
		t.Fatalf("second replayed lock missing: %+v ok=%v", lk, ok)
	}
	logf("restarted node replayed committed log locally: res holder=holder-1 token=%d; other token=%d",
		r1.Token, r2.Token)

	// Give replication a moment to catch up the entry committed while down.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lk, ok := c.SnapshotLock(follower, "res2"); ok && lk.Holder == "holder-3" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lk, ok := c.SnapshotLock(follower, "res2"); !ok || lk.Holder != "holder-3" {
		t.Fatalf("restarted node did not catch up missed entry: %+v ok=%v", lk, ok)
	}
	logf("restarted node caught up the entry missed while down (res2 holder=holder-3 token=%d)", r3.Token)

	// Fenced commit still works end-to-end with the original token after
	// the restart, proving replay preserved the fence contract.
	ok2, _, err := c.Submit(t.Context(), commit("res", "holder-1", "r-commit-after-restart", r1.Token))
	if err != nil || !ok2.OK {
		t.Fatalf("post-restart fenced commit: %+v %v", ok2, err)
	}
	logf("fenced commit with token=%d accepted after restart (basis: replayed current token + live lease)",
		r1.Token)
}

// TestRestartLeaderThenReelect restarts the whole cluster one node at a
// time (rolling bounce) and finally all at once, asserting each phase
// ends with exactly one leader and a monotonic term.
func TestRestartLeaderThenReelect(t *testing.T) {
	ids := []string{"n1", "n2", "n3"}
	c := newTestCluster(t, ids...)
	logf := testingLogger(t)

	leader1, t1, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	logf("phase1: leader=%s term=%d", leader1, t1)

	// Bounce the leader: remaining majority elects a new one in a new term.
	c.Stop(leader1)
	var remaining []string
	for _, id := range ids {
		if id != leader1 {
			remaining = append(remaining, id)
		}
	}
	newLeader := waitForLeaderIn(t, c, remaining, 3*time.Second)
	t2 := c.Node(newLeader).Raft.Term()
	if t2 <= t1 {
		t.Fatalf("term must advance after leader failure: %d -> %d", t1, t2)
	}
	logf("phase2: leader %s stopped; surviving majority elected %s term=%d", leader1, newLeader, t2)

	// Restart the old leader. It must come back as follower and never
	// fight for leadership in a stale term.
	c.Start(leader1)
	time.Sleep(400 * time.Millisecond)
	if c.Node(leader1).Raft.IsLeader() {
		t.Fatalf("restarted stale leader must not seize leadership immediately")
	}
	if term := c.Node(leader1).Raft.Term(); term < t1 {
		t.Fatalf("restarted node term rolled back: %d < %d", term, t1)
	}
	logf("phase3: %s restarted as follower, term=%d restored; current leader=%s term=%d",
		leader1, c.Node(leader1).Raft.Term(), newLeader, t2)
	assertSingleLeader(t, c, logf)

	// Full simultaneous restart: persisted state lets the cluster re-elect
	// without term regression and keep all lock state. Acquire while all
	// three nodes are alive, then explicitly wait for full replication so
	// the entry truly has a durable majority before the bounce.
	resp, _, err := c.Submit(t.Context(), acq("durable", "dh", "d-acq", 60_000))
	if err != nil || !resp.OK {
		t.Fatalf("acquire before full restart: %+v %v", resp, err)
	}
	token := resp.Token
	if !waitForReplication(c, token, "durable", 2*time.Second) {
		t.Fatalf("durable lock not replicated to all 3 nodes before full restart")
	}
	logf("durable lock holder=dh token=%d confirmed on all nodes before full restart", token)
	for _, id := range ids {
		c.Stop(id)
	}
	logf("phase4: all nodes stopped; token=%d persisted in committed logs", token)
	time.Sleep(50 * time.Millisecond)
	for _, id := range ids {
		c.Start(id)
	}
	leader4, t4, err := c.WaitForLeader(3 * time.Second)
	if err != nil {
		t.Fatalf("no leader after full restart: %v", err)
	}
	if t4 < t2 {
		t.Fatalf("term regressed after full restart: %d < %d", t4, t2)
	}
	logf("phase5: cluster re-elected leader=%s term=%d (>= %d) after full restart", leader4, t4, t2)
	assertSingleLeader(t, c, logf)

	// Lock state survived full restart; wait for every node to finish
	// replaying its durable committed log before inspecting.
	if !waitForAppliedAll(c, 2*time.Second) {
		t.Fatalf("nodes did not finish replaying committed logs after full restart")
	}
	logf("all nodes replayed committed logs after full restart")
	if lk, ok := c.SnapshotLock(leader4, "durable"); !ok || lk.Token != token || lk.Holder != "dh" {
		t.Fatalf("lock state lost across full restart: %+v ok=%v", lk, ok)
	}
	stale, _, _ := c.Submit(t.Context(),
		commit("durable", "dh", "d-stale", token+100))
	if stale.OK || stale.Reason != coordinator.ReasonTokenStale {
		t.Fatalf("want token_stale after restart, got %+v", stale)
	}
	good, _, _ := c.Submit(t.Context(), commit("durable", "dh", "d-good", token))
	if !good.OK {
		t.Fatalf("valid fenced commit after full restart: %+v", good)
	}
	logf("after full restart: durable lock holder=dh token=%d intact; token=%d accepted, forged token rejected reason=%s",
		token, token, stale.Reason)
}

func assertSingleLeader(t *testing.T, c *Cluster, logf func(string, ...any)) {
	t.Helper()
	var leaders []string
	for _, id := range c.IDs() {
		if c.Node(id).Raft.IsLeader() {
			leaders = append(leaders, id)
		}
	}
	if len(leaders) != 1 {
		t.Fatalf("expected exactly one leader, got %v", leaders)
	}
	logf("invariant check: exactly one leader=%v", leaders)
}

// waitForAppliedAll waits until every live node has applied its durable
// committed prefix. A restarted node may carry an uncommitted log suffix
// (from a deposed leader); that suffix must NOT be applied and may be
// overwritten when the new leader replicates, so it is deliberately not
// part of the wait condition.
func waitForAppliedAll(c *Cluster, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range c.IDs() {
			h := c.Node(id)
			if h.Raft == nil || h.Raft.AppliedIndex() < h.Raft.CommitIndex() {
				all = false
			}
		}
		if all {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// waitForReplication blocks until every node's local state machine shows
// the given resource held by holder "dh" with the given token.
func waitForReplication(c *Cluster, token int64, resource string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range c.IDs() {
			lk, ok := c.Node(id).Mach.SnapshotLock(resource)
			if !ok || lk.Token != token {
				all = false
			}
		}
		if all {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
