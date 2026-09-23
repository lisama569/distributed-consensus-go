package coordinator

import "testing"

func cmd(kind, req, res, holder string, token, lease, now int64) Command {
	return Command{Kind: kind, RequestID: req, Resource: res, Holder: holder,
		Token: token, LeaseMS: lease, NowMS: now}
}

func TestMachineContract(t *testing.T) {
	m := NewMachine()

	// Empty RequestID is always rejected with its own reason.
	if r := m.Apply(cmd(CmdAcquire, "", "r", "h", 0, 100, 0)); r.OK || r.Reason != ReasonEmptyRequestID {
		t.Fatalf("empty request id: %+v", r)
	}

	// Fresh grant: token 1.
	r := m.Apply(cmd(CmdAcquire, "q1", "r", "h1", 0, 100, 1000))
	if !r.OK || r.Token != 1 || r.LeaseUntilMS != 1100 {
		t.Fatalf("grant: %+v", r)
	}

	// Duplicate request id replays, even with different-looking fields.
	dup := m.Apply(cmd(CmdAcquire, "q1", "other-res", "h2", 0, 5, 2000))
	if !dup.Replayed || dup.Token != 1 || dup.LeaseUntilMS != 1100 {
		t.Fatalf("idempotent replay: %+v", dup)
	}

	// Busy while live.
	if b := m.Apply(cmd(CmdAcquire, "q2", "r", "h2", 0, 100, 1050)); b.OK || b.Reason != ReasonResourceBusy {
		t.Fatalf("busy: %+v", b)
	}
	// Renew by wrong holder.
	if b := m.Apply(cmd(CmdRenew, "q3", "r", "h2", 0, 100, 1050)); b.OK || b.Reason != ReasonNotHolder {
		t.Fatalf("renew not holder: %+v", b)
	}
	// Valid renew extends.
	if b := m.Apply(cmd(CmdRenew, "q4", "r", "h1", 0, 100, 1050)); !b.OK || b.LeaseUntilMS != 1150 {
		t.Fatalf("renew: %+v", b)
	}
	// Commit stale/future token vs current holder.
	if b := m.Apply(cmd(CmdCommit, "q5", "r", "h1", 2, 0, 1100)); b.OK || b.Reason != ReasonTokenStale {
		t.Fatalf("forge token: %+v", b)
	}
	// Commit valid.
	if b := m.Apply(cmd(CmdCommit, "q6", "r", "h1", 1, 0, 1100)); !b.OK {
		t.Fatalf("valid commit: %+v", b)
	}

	// Expire at 1150: commit/renew rejected lease_expired.
	if b := m.Apply(cmd(CmdCommit, "q7", "r", "h1", 1, 0, 1150)); b.OK || b.Reason != ReasonLeaseExpired {
		t.Fatalf("expired commit: %+v", b)
	}
	if b := m.Apply(cmd(CmdRenew, "q8", "r", "h1", 0, 100, 1150)); b.OK || b.Reason != ReasonLeaseExpired {
		t.Fatalf("expired renew: %+v", b)
	}
	// Old holder cannot release after handoff; first new holder takes over
	// with token 2 (strictly greater).
	nr := m.Apply(cmd(CmdAcquire, "q9", "r", "h2", 0, 100, 1150))
	if !nr.OK || nr.Token != 2 {
		t.Fatalf("regrant: %+v", nr)
	}
	if b := m.Apply(cmd(CmdRelease, "q10", "r", "h1", 1, 0, 1150)); b.OK || b.Reason != ReasonNotHolder {
		t.Fatalf("old owner release: %+v", b)
	}
	// Unknown resource commit.
	if b := m.Apply(cmd(CmdCommit, "q11", "ghost", "h1", 1, 0, 1150)); b.OK || b.Reason != ReasonTokenUnknown {
		t.Fatalf("unknown: %+v", b)
	}
	// Unknown command kind.
	if b := m.Apply(cmd("explode", "q12", "r", "h1", 0, 0, 0)); b.OK || b.Reason != ReasonUnknownCmd {
		t.Fatalf("unknown cmd: %+v", b)
	}
	// Correct release by current holder; duplicate release replays.
	if b := m.Apply(cmd(CmdRelease, "q13", "r", "h2", 2, 0, 1200)); !b.OK {
		t.Fatalf("release: %+v", b)
	}
	if b := m.Apply(cmd(CmdRelease, "q13", "r", "h2", 2, 0, 1200)); !b.OK || !b.Replayed {
		t.Fatalf("release replay: %+v", b)
	}
	if m.FenceSeq() != 2 {
		t.Fatalf("fence seq = %d, want 2", m.FenceSeq())
	}
}
