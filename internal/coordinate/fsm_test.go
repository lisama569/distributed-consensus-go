package coordinate

import (
	"errors"
	"testing"
	"time"
)

func TestFSM_AcquireMonotonicTokens(t *testing.T) {
	f := NewFSM()
	now := time.Unix(1000, 0)
	ttl := 100 * time.Millisecond

	r1, err := f.Apply(Op{Kind: OpAcquire, RequestID: "a1", Resource: "R", Owner: "A", TTL: ttl, Term: 1}, now)
	if err != nil {
		t.Fatalf("acquire1: %v", err)
	}
	if r1.(LockResult).Token != 1 {
		t.Fatalf("first token = %d, want 1", r1.(LockResult).Token)
	}

	// 同一 owner 在租约内重入：令牌不变。
	r2, err := f.Apply(Op{Kind: OpAcquire, RequestID: "a2", Resource: "R", Owner: "A", TTL: ttl, Term: 1}, now.Add(10*time.Millisecond))
	if err != nil || r2.(LockResult).Token != 1 {
		t.Fatalf("reentrant acquire token=%d err=%v, want 1/nil", r2.(LockResult).Token, err)
	}

	// 其他 owner 在租约内被拒绝。
	if _, err := f.Apply(Op{Kind: OpAcquire, RequestID: "a3", Resource: "R", Owner: "B", TTL: ttl, Term: 1}, now.Add(20*time.Millisecond)); !errors.Is(err, ErrHeldByOther) {
		t.Fatalf("contended acquire err=%v, want ErrHeldByOther", err)
	}

	// 租约过期后 B 获取成功，令牌严格 +1。
	r4, err := f.Apply(Op{Kind: OpAcquire, RequestID: "a4", Resource: "R", Owner: "B", TTL: ttl, Term: 2}, now.Add(200*time.Millisecond))
	if err != nil || r4.(LockResult).Token != 2 {
		t.Fatalf("post-expiry acquire token=%d err=%v, want 2/nil", r4.(LockResult).Token, err)
	}

	// A 的旧令牌提交受保护操作必须被拒绝为令牌陈旧。
	vr, err := f.Apply(Op{Kind: OpValidate, RequestID: "v1", Resource: "R", Owner: "A", Token: 1, Term: 2}, now.Add(210*time.Millisecond))
	if !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("stale token validate err=%v, want ErrFenceTokenStale", err)
	}
	if vr.(ValidateResult).Accepted {
		t.Fatal("stale token must not be accepted")
	}

	// B 的当前令牌通过。
	if vr, err := f.Apply(Op{Kind: OpValidate, RequestID: "v2", Resource: "R", Owner: "B", Token: 2, Term: 2}, now.Add(220*time.Millisecond)); err != nil || !vr.(ValidateResult).Accepted {
		t.Fatalf("current token validate accepted=%v err=%v", vr.(ValidateResult).Accepted, err)
	}
}

func TestFSM_LeaseExpiryRejectsProtectedOp(t *testing.T) {
	f := NewFSM()
	now := time.Unix(2000, 0)
	ttl := 50 * time.Millisecond
	r, _ := f.Apply(Op{Kind: OpAcquire, RequestID: "x1", Resource: "Q", Owner: "A", TTL: ttl, Term: 1}, now)
	token := r.(LockResult).Token

	// 租约过期：续约被拒。
	if _, err := f.Apply(Op{Kind: OpRenew, RequestID: "x2", Resource: "Q", Owner: "A", Token: token, TTL: ttl, Term: 1}, now.Add(ttl+time.Millisecond)); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("renew after expiry err=%v, want ErrLeaseExpired", err)
	}
	// 受保护操作也被拒，原因是租约过期。
	if _, err := f.Apply(Op{Kind: OpValidate, RequestID: "x3", Resource: "Q", Owner: "A", Token: token, Term: 1}, now.Add(ttl+time.Millisecond)); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("validate after expiry err=%v, want ErrLeaseExpired", err)
	}
}

func TestFSM_DedupIdempotent(t *testing.T) {
	f := NewFSM()
	now := time.Unix(3000, 0)
	ttl := 100 * time.Millisecond

	op := Op{Kind: OpAcquire, RequestID: "dup", Resource: "Z", Owner: "A", TTL: ttl, Term: 1}
	r1, e1 := f.Apply(op, now)
	// 相同 requestID 重放，即便时间推进到租约之后：结论必须与首次完全一致。
	r2, e2 := f.Apply(op, now.Add(500*time.Millisecond))
	if (e1 == nil) != (e2 == nil) || r1.(LockResult).Token != r2.(LockResult).Token {
		t.Fatalf("replay changed outcome: %+v/%v vs %+v/%v", r1, e1, r2, e2)
	}

	// 新的 requestID 在过期后获取，只应拿到令牌 2（重放没有制造新令牌）。
	r3, err := f.Apply(Op{Kind: OpAcquire, RequestID: "fresh", Resource: "Z", Owner: "B", TTL: ttl, Term: 2}, now.Add(500*time.Millisecond))
	if err != nil || r3.(LockResult).Token != 2 {
		t.Fatalf("token after replay = %d err=%v, want 2", r3.(LockResult).Token, err)
	}
}

func TestFSM_RejectReplayPreservesDistinctCode(t *testing.T) {
	f := NewFSM()
	now := time.Unix(4000, 0)
	ttl := 100 * time.Millisecond
	f.Apply(Op{Kind: OpAcquire, RequestID: "h1", Resource: "L", Owner: "A", TTL: ttl, Term: 1}, now)

	op := Op{Kind: OpAcquire, RequestID: "h2", Resource: "L", Owner: "B", TTL: ttl, Term: 1}
	_, e1 := f.Apply(op, now.Add(5*time.Millisecond))
	_, e2 := f.Apply(op, now.Add(6*time.Millisecond))
	if !errors.Is(e1, ErrHeldByOther) || !errors.Is(e2, ErrHeldByOther) {
		t.Fatalf("replay of rejection changed: %v / %v", e1, e2)
	}
}
