package sim

import (
	"testing"
	"time"

	"github.com/lisama569/distributed-consensus-go/pkg/coordinator"
)

// TestLeaseFenceAndExpiry covers the lease contract:
//   - acquire mints a strictly increasing, comparable fence token;
//   - duplicate requests replay the same response (no token jump);
//   - while a live lease exists, a competing holder is rejected as
//     resource_busy and commits by strangers are rejected with distinct
//     reasons;
//   - after lease expiry the old holder's commits/renewals are rejected
//     (lease_expired) and the resource can move to a new holder with a
//     larger token;
//   - a stale token commit after handoff is rejected as token_stale.
func TestLeaseFenceAndExpiry(t *testing.T) {
	c := newTestCluster(t, "n1", "n2", "n3")
	logf := testingLogger(t)
	_, term, err := c.WaitForLeader(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	logf("leader elected term=%d", term)

	ctx := t.Context()
	const res = "ledger/42"

	// 1) first acquire -> token 1.
	r1, _, err := c.Submit(ctx, acq(res, "alpha", "acq-1", 300))
	if err != nil || !r1.OK {
		t.Fatalf("first acquire: %+v %v", r1, err)
	}
	if r1.Token != 1 {
		t.Fatalf("first token should be 1, got %d", r1.Token)
	}
	logf("acquire: holder=alpha resource=%s token=%d leaseUntil=%d (basis: majority commit, new grant)",
		res, r1.Token, r1.LeaseUntilMS)

	// 2) duplicate acquire (same RequestID) replays verbatim: same token.
	r1dup, _, _ := c.Submit(ctx, acq(res, "alpha", "acq-1", 300))
	if !r1dup.Replayed || r1dup.Token != r1.Token || r1dup.LeaseUntilMS != r1.LeaseUntilMS {
		t.Fatalf("duplicate acquire must replay exact response, got %+v", r1dup)
	}
	logf("duplicate acquire request=acq-1 replayed token=%d (no double lock, no token jump)", r1dup.Token)

	// 2b) same holder re-acquiring under a live lease with a NEW request id
	// returns the existing token rather than minting another.
	r1again, _, _ := c.Submit(ctx, acq(res, "alpha", "acq-again", 300))
	if !r1again.OK || r1again.Token != 1 {
		t.Fatalf("holder re-acquire must preserve token, got %+v", r1again)
	}

	// 3) competing holder during the live lease -> resource_busy.
	rBusy, _, _ := c.Submit(ctx, acq(res, "beta", "acq-2", 300))
	if rBusy.OK || rBusy.Reason != coordinator.ReasonResourceBusy {
		t.Fatalf("want resource_busy, got %+v", rBusy)
	}
	logf("competing acquire holder=beta rejected reason=%s (basis: live lease until=%d)",
		rBusy.Reason, r1.LeaseUntilMS)

	// 4) valid fenced commit accepted.
	rCommit, _, _ := c.Submit(ctx, commit(res, "alpha", "commit-ok", 1))
	if !rCommit.OK {
		t.Fatalf("valid commit: %+v", rCommit)
	}
	logf("fenced commit holder=alpha token=1 accepted (basis: token==current && lease live)")

	// 5) stranger with the same token rejected not_holder; forged token 99 token_stale.
	rNH, _, _ := c.Submit(ctx, commit(res, "beta", "commit-stranger", 1))
	if rNH.OK || rNH.Reason != coordinator.ReasonNotHolder {
		t.Fatalf("want not_holder, got %+v", rNH)
	}
	rForge, _, _ := c.Submit(ctx, commit(res, "alpha", "commit-forge", 99))
	if rForge.OK || rForge.Reason != coordinator.ReasonTokenStale {
		t.Fatalf("want token_stale, got %+v", rForge)
	}
	logf("guarded commit rejections carry distinct reasons: stranger=%s forgedToken=%s",
		rNH.Reason, rForge.Reason)

	// 6) duplicate release replays; release with wrong token rejected.
	rRelBad, _, _ := c.Submit(ctx, release(res, "alpha", "rel-bad", 2))
	if rRelBad.OK || rRelBad.Reason != coordinator.ReasonTokenStale {
		t.Fatalf("want token_stale on release, got %+v", rRelBad)
	}
	logf("release with stale token rejected reason=%s", rRelBad.Reason)

	// 7) wait for the 300ms lease to expire, then old holder is dead.
	time.Sleep(450 * time.Millisecond)
	rExp, _, _ := c.Submit(ctx, commit(res, "alpha", "commit-exp", 1))
	if rExp.OK || rExp.Reason != coordinator.ReasonLeaseExpired {
		t.Fatalf("want lease_expired commit, got %+v", rExp)
	}
	rRenew, _, _ := c.Submit(ctx, renew(res, "alpha", "renew-exp", 300))
	if rRenew.OK || rRenew.Reason != coordinator.ReasonLeaseExpired {
		t.Fatalf("want lease_expired renew, got %+v", rRenew)
	}
	logf("after expiry: old holder commit rejected reason=%s, renew rejected reason=%s (basis: leaseUntil<=now)",
		rExp.Reason, rRenew.Reason)

	// 8) new holder acquires and receives a strictly larger token.
	r2, _, _ := c.Submit(ctx, acq(res, "beta", "acq-3", 5_000))
	if !r2.OK || r2.Token <= r1.Token {
		t.Fatalf("new grant must mint larger token, got %+v", r2)
	}
	logf("post-expiry acquire: holder=beta token=%d, strictly greater than prior token=%d", r2.Token, r1.Token)

	// 9) old holder's stale token commit now rejected token_stale, and the
	// duplicate of that rejection also replays with the same reason.
	rStale, _, _ := c.Submit(ctx, commit(res, "alpha", "commit-stale", 1))
	if rStale.OK || rStale.Reason != coordinator.ReasonTokenStale {
		t.Fatalf("want token_stale, got %+v", rStale)
	}
	rStaleDup, _, _ := c.Submit(ctx, commit(res, "alpha", "commit-stale", 1))
	if !rStaleDup.Replayed || rStaleDup.Reason != coordinator.ReasonTokenStale {
		t.Fatalf("rejection must replay identically, got %+v", rStaleDup)
	}
	logf("old token after handoff rejected reason=%s (and replays identically); fence ordering 1 < %d enforced",
		rStale.Reason, r2.Token)

	// 10) second resource's token keeps the global monotonic sequence.
	r3, _, _ := c.Submit(ctx, acq("other", "gamma", "acq-4", 5_000))
	if r3.Token <= r2.Token {
		t.Fatalf("global fence must be monotonic across resources, got %d <= %d", r3.Token, r2.Token)
	}
	logf("different resource acquire token=%d continues global strictly-increasing fence sequence", r3.Token)

	// 11) correct release, then a duplicate release is a harmless replay.
	rRel, _, _ := c.Submit(ctx, release(res, "beta", "rel-ok", r2.Token))
	if !rRel.OK {
		t.Fatalf("release: %+v", rRel)
	}
	rRelDup, _, _ := c.Submit(ctx, release(res, "beta", "rel-ok", r2.Token))
	if !rRelDup.OK || !rRelDup.Replayed {
		t.Fatalf("duplicate release must replay, got %+v", rRelDup)
	}
	logf("release request=rel-ok ok; identical repeat replayed (no double release)")
}
