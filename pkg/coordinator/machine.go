package coordinator

import "sync"

// Lock is the state of one resource as seen by the state machine.
type Lock struct {
	Holder       string
	Token        int64
	LeaseUntilMS int64
}

// Machine is the deterministic lock state machine. It must only ever be
// driven through committed Raft log entries: because every node applies
// the same commands in the same order, all replicas reach identical
// holder/token/expiry conclusions.
//
// Time rule: every Command carries the leader's wall-clock reading
// (NowMS). The FSM never reads the system clock itself, so applying the
// log (including replay after a restart) stays deterministic.
type Machine struct {
	mu        sync.RWMutex
	locks     map[string]*Lock
	responses map[string]Response // request idempotency table
	fenceSeq  int64               // cluster-wide strictly increasing fence token
}

// NewMachine returns an empty lock state machine.
func NewMachine() *Machine {
	return &Machine{
		locks:     map[string]*Lock{},
		responses: map[string]Response{},
	}
}

func reject(reason string) Response { return Response{OK: false, Reason: reason} }

// Apply executes one command deterministically and returns its response.
//
// Idempotency: any non-empty RequestID that has already been applied
// replays the exact recorded response (Replayed=true), so retries can
// never double-lock, double-release or bump the fence token.
func (m *Machine) Apply(cmd Command) Response {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cmd.RequestID != "" {
		if prior, ok := m.responses[cmd.RequestID]; ok {
			prior.Replayed = true
			return prior
		}
	} else {
		return reject(ReasonEmptyRequestID)
	}

	var resp Response
	switch cmd.Kind {
	case CmdAcquire:
		resp = m.applyAcquire(cmd)
	case CmdRenew:
		resp = m.applyRenew(cmd)
	case CmdRelease:
		resp = m.applyRelease(cmd)
	case CmdCommit:
		resp = m.applyCommit(cmd)
	default:
		resp = reject(ReasonUnknownCmd)
	}
	m.responses[cmd.RequestID] = resp
	return resp
}

// holderActive reports whether lk exists, belongs to cmd.Holder and its
// lease has not expired at cmd.NowMS.
func (m *Machine) holderActive(lk *Lock, cmd Command) bool {
	return lk != nil && lk.Holder == cmd.Holder && lk.LeaseUntilMS > cmd.NowMS
}

// applyAcquire grants the lock when it is free or held by the same
// caller's *valid* (non-expired) lease. A conflicting or expired lock
// blocks with an explicit reason. On each fresh grant the global fence
// counter advances by exactly one.
func (m *Machine) applyAcquire(cmd Command) Response {
	if cmd.LeaseMS <= 0 {
		return reject("invalid_lease_duration")
	}
	lk := m.locks[cmd.Resource]
	switch {
	case lk == nil:
		// free resource -> new grant
	case lk.Holder == cmd.Holder && lk.LeaseUntilMS > cmd.NowMS:
		// Re-acquire by the current holder during a live lease is a
		// no-op: return the existing token, never mint a new one.
		return Response{OK: true, Token: lk.Token, LeaseUntilMS: lk.LeaseUntilMS}
	case lk.Holder != cmd.Holder && lk.LeaseUntilMS > cmd.NowMS:
		return reject(ReasonResourceBusy)
	case lk.LeaseUntilMS <= cmd.NowMS:
		// Expired: fall through and grant to the new holder. A stale
		// owner therefore never blocks progress past lease expiry.
	}

	m.fenceSeq++
	token := m.fenceSeq
	until := cmd.NowMS + cmd.LeaseMS
	m.locks[cmd.Resource] = &Lock{
		Holder:       cmd.Holder,
		Token:        token,
		LeaseUntilMS: until,
	}
	return Response{OK: true, Token: token, LeaseUntilMS: until}
}

// applyRenew extends the lease for the current holder. An expired lease
// cannot be revived by renew: the old owner must acquire again (getting a
// new token), which is why expired renewals return ReasonLeaseExpired.
func (m *Machine) applyRenew(cmd Command) Response {
	lk := m.locks[cmd.Resource]
	if lk == nil {
		return reject(ReasonTokenUnknown)
	}
	if lk.Holder != cmd.Holder {
		return reject(ReasonNotHolder)
	}
	if lk.LeaseUntilMS <= cmd.NowMS {
		return reject(ReasonLeaseExpired)
	}
	if cmd.LeaseMS <= 0 {
		return reject("invalid_lease_duration")
	}
	lk.LeaseUntilMS = cmd.NowMS + cmd.LeaseMS
	return Response{OK: true, Token: lk.Token, LeaseUntilMS: lk.LeaseUntilMS}
}

// applyRelease removes the lock. Releases require the caller to present
// the exact token it was granted (or zero, meaning an unconditional
// release by the current holder), so a stale previous owner cannot
// release a lock that already moved to someone else.
func (m *Machine) applyRelease(cmd Command) Response {
	lk := m.locks[cmd.Resource]
	if lk == nil {
		return Response{OK: true} // releasing an absent lock is a no-op
	}
	if lk.Holder != cmd.Holder {
		return reject(ReasonNotHolder)
	}
	if cmd.Token != 0 && cmd.Token != lk.Token {
		return reject(ReasonTokenStale)
	}
	delete(m.locks, cmd.Resource)
	return Response{OK: true}
}

// applyCommit is the fenced write guard: a protected operation is only
// accepted when the presented token equals the resource's *current*
// token and the lease is still live. Each failure mode carries a
// distinct reason.
func (m *Machine) applyCommit(cmd Command) Response {
	lk := m.locks[cmd.Resource]
	switch {
	case lk == nil:
		return reject(ReasonTokenUnknown)
	case lk.Token != cmd.Token:
		// Checked first: a previous owner presenting its old token after
		// the lock has moved (or anyone presenting a forged/future token)
		// is fenced as token_stale — that is the more specific, more
		// actionable reason than merely not holding the resource.
		return reject(ReasonTokenStale)
	case lk.Holder != cmd.Holder:
		return reject(ReasonNotHolder)
	case lk.LeaseUntilMS <= cmd.NowMS:
		return reject(ReasonLeaseExpired)
	default:
		return Response{OK: true, Token: lk.Token, LeaseUntilMS: lk.LeaseUntilMS}
	}
}

// SnapshotLock returns the current state of a resource (for logging).
func (m *Machine) SnapshotLock(resource string) (Lock, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	lk, ok := m.locks[resource]
	if !ok {
		return Lock{}, false
	}
	return *lk, true
}

// FenceSeq returns the high-water fence token (for tests/logging).
func (m *Machine) FenceSeq() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.fenceSeq
}
