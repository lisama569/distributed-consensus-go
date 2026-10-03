package coordinate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// reasonOf 把哨兵错误映射为历史事件中的 reason 字符串（与投影器用词一致）。
func reasonOf(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrHeldByOther):
		return "held_by_other"
	case errors.Is(err, ErrLeaseExpired):
		return "lease_expired"
	case errors.Is(err, ErrFenceTokenStale):
		return "fence_token_stale"
	case errors.Is(err, ErrWrongOwner):
		return "wrong_owner"
	case errors.Is(err, ErrEmptyResource):
		return "empty_resource"
	case errors.Is(err, ErrEmptyOwner):
		return "empty_owner"
	default:
		return "unknown"
	}
}

// TestHistorian_ProjectionMatchesFSM 逐条验证：同一串已提交日志，
// 历史投影给出的结论（接受/拒绝及原因）与在线状态机完全一致，
// 且归属段（开始/续约/结束原因）被正确记录。
func TestHistorian_ProjectionMatchesFSM(t *testing.T) {
	f := NewFSM()
	h := NewHistorian()
	base := time.Unix(1_700_000_000, 0)
	ttl := 100 * time.Millisecond

	type step struct {
		op Op
		at time.Time
	}
	mk := func(kind OpKind, id, owner string, token int64, at time.Duration) step {
		return step{op: Op{Kind: kind, RequestID: id, Resource: "R", Owner: owner,
			Token: token, TTL: ttl, Term: 1}, at: base.Add(at)}
	}
	steps := []step{
		mk(OpAcquire, "a1", "A", 0, 0),                                // 成功 token=1
		mk(OpAcquire, "a2", "B", 0, 10*time.Millisecond),              // held_by_other
		mk(OpRenew, "r1", "A", 1, 20*time.Millisecond),                // 续约成功
		mk(OpValidate, "v1", "A", 1, 30*time.Millisecond),             // 接受
		mk(OpValidate, "v2", "A", 9, 40*time.Millisecond),             // fence_token_stale
		mk(OpRelease, "rel1", "A", 1, 50*time.Millisecond),            // 释放 -> span1 released
		mk(OpAcquire, "a3", "B", 0, 60*time.Millisecond),              // 成功 token=2
		mk(OpRenew, "r2", "A", 1, 70*time.Millisecond),                // wrong_owner
		mk(OpValidate, "v3", "B", 2, 80*time.Millisecond),             // 接受
		mk(OpRenew, "r3", "B", 2, time.Second),                        // lease_expired -> span2 expired
		mk(OpAcquire, "a4", "C", 0, time.Second+10*time.Millisecond),  // token=3
		mk(OpValidate, "v4", "B", 2, time.Second+20*time.Millisecond), // fence_token_stale
	}

	for i, s := range steps {
		s.op.Seq = int64(i + 1)
		s.op.At = s.at
		_, ferr := f.Apply(s.op, s.at)
		h.Apply(s.op, s.at)
		ev := h.events[len(h.events)-1]
		if ev.Accepted != (ferr == nil) || ev.Reason != reasonOf(ferr) {
			t.Fatalf("step %d (%s %s): projection=(accepted=%v reason=%q) fsm err=%v",
				i, s.op.Kind, s.op.RequestID, ev.Accepted, ev.Reason, ferr)
		}
		t.Logf("step %02d seq=%d %-8s owner=%s token=%d => accepted=%v reason=%s (fsm err=%v)",
			i, ev.Seq, ev.Kind, s.op.Owner, s.op.Token, ev.Accepted, ev.Reason, ferr)
	}

	// 相同 RequestID 重放：投影记录 replay 事件，结论与首次一致。
	dup := steps[0].op
	dup.Seq = int64(len(steps) + 1)
	dup.At = base.Add(2 * time.Second)
	if _, err := f.Apply(dup, dup.At); err != nil {
		t.Fatalf("fsm replay: %v", err)
	}
	h.Apply(dup, dup.At)
	last := h.events[len(h.events)-1]
	if last.Kind != HistReplay || !last.Accepted || last.Reason != "ok" {
		t.Fatalf("replay event = %+v, want accepted replay of first acquire", last)
	}

	// 归属段：A(released) -> B(expired) -> C(ongoing)，令牌 1/2/3，顺序稳定。
	spans := h.Spans("R")
	if len(spans) != 3 {
		t.Fatalf("spans=%d, want 3: %+v", len(spans), spans)
	}
	want := []struct {
		owner  string
		token  int64
		reason EndReason
	}{
		{"A", 1, EndReleased},
		{"B", 2, EndExpired},
		{"C", 3, EndNone},
	}
	for i, w := range want {
		sp := spans[i]
		if sp.Owner != w.owner || sp.Token != w.token || sp.EndReason != w.reason {
			t.Fatalf("span[%d] = owner=%s token=%d end=%s, want %s/%d/%s",
				i, sp.Owner, sp.Token, sp.EndReason, w.owner, w.token, w.reason)
		}
		if i > 0 && sp.StartSeq <= spans[i-1].StartSeq {
			t.Fatalf("span start order not strictly increasing: %d then %d",
				spans[i-1].StartSeq, sp.StartSeq)
		}
		t.Logf("span[%d] owner=%s token=%d startSeq=%d endSeq=%d end=%s renewals=%d",
			i, sp.Owner, sp.Token, sp.StartSeq, sp.EndSeq, sp.EndReason, len(sp.Renewals))
	}
	if len(spans[0].Renewals) != 1 {
		t.Fatalf("span A renewals=%d, want 1 (r1)", len(spans[0].Renewals))
	}

	// 历史时点重建：B 持锁期间的位置应重建出 B/token=2/未过期。
	st, err := h.StateAt("R", 8) // v3 的位置（B 租约内）
	if err != nil || !st.Found || st.Owner != "B" || st.Token != 2 || st.Expired {
		t.Fatalf("StateAt@8 = %+v err=%v, want B/token=2/not-expired", st, err)
	}
	// B 的租约被观察到期的位置：该位置已无有效持有者。
	st, err = h.StateAt("R", 10) // r3 的位置（续约时发现已过期）
	if err != nil {
		t.Fatal(err)
	}
	if st.Found {
		t.Fatalf("StateAt@10 = %+v, want no live owner (lease observed expired)", st)
	}
	// 把当时的提交放回原位置：结论必须与当时一致。
	vr, err := h.ValidateAt("R", 8, "B", 2)
	if err != nil || !vr.Accepted {
		t.Fatalf("ValidateAt@8(B,2) = %+v err=%v, want accepted", vr, err)
	}
	if vr, err = h.ValidateAt("R", 8, "B", 1); !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("ValidateAt@8(B,1) err=%v, want ErrFenceTokenStale", err)
	}
	if _, err = h.ValidateAt("R", 10, "B", 2); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("ValidateAt@10(B,2) err=%v, want ErrLeaseExpired", err)
	}
	t.Logf("reconstruction: @8 owner=B token=2 valid; @10 expired; validate-at conclusions match")
}

// TestHistory_OwnershipSpansStableOrdered 在真实集群上验证：
// 归属历史顺序稳定、令牌递增、结束原因正确，且已产生的历史不被后续操作改写。
func TestHistory_OwnershipSpansStableOrdered(t *testing.T) {
	c := newTestCluster(t, 3)
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 300*time.Millisecond)
	ctx := context.Background()
	const res = "audit/1"

	r1, err := svc.Acquire(ctx, "h-a1", res, "owner-A", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Renew(ctx, "h-r1", res, "owner-A", r1.Token, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if vr, err := svc.ValidateAndSubmit(ctx, "h-v1", res, "owner-A", r1.Token); err != nil || !vr.Accepted {
		t.Fatalf("validate within lease: %v %+v", err, vr)
	}
	if err := svc.Release(ctx, "h-rel1", res, "owner-A", r1.Token); err != nil {
		t.Fatal(err)
	}
	r2, err := svc.Acquire(ctx, "h-a2", res, "owner-B", 120*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // 让 B 的租约到期
	r3, err := svc.Acquire(ctx, "h-a3", res, "owner-C", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ValidateAndSubmit(ctx, "h-v2", res, "owner-B", r2.Token); !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("old owner validate err=%v, want ErrFenceTokenStale", err)
	}

	spans, err := svc.OwnershipHistory(ctx, res)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 3 {
		t.Fatalf("spans=%d, want 3: %+v", len(spans), spans)
	}
	checks := []struct {
		owner  string
		token  int64
		reason EndReason
	}{
		{"owner-A", r1.Token, EndReleased},
		{"owner-B", r2.Token, EndTakenOver},
		{"owner-C", r3.Token, EndNone},
	}
	for i, w := range checks {
		sp := spans[i]
		if sp.Owner != w.owner || sp.Token != w.token || sp.EndReason != w.reason {
			t.Fatalf("span[%d] = %s/%d/%s, want %s/%d/%s",
				i, sp.Owner, sp.Token, sp.EndReason, w.owner, w.token, w.reason)
		}
		if i > 0 && sp.StartSeq <= spans[i-1].StartSeq {
			t.Fatalf("history order unstable: span[%d].StartSeq=%d <= span[%d].StartSeq=%d",
				i, sp.StartSeq, i-1, spans[i-1].StartSeq)
		}
		t.Logf("span[%d] owner=%s token=%d startSeq=%d endSeq=%d end=%s renewals=%d",
			i, sp.Owner, sp.Token, sp.StartSeq, sp.EndSeq, sp.EndReason, len(sp.Renewals))
	}
	if len(spans[0].Renewals) != 1 || !spans[0].Renewals[0].LeaseUntil.After(spans[0].LeaseUntil) {
		t.Fatalf("renewal not recorded as lease extension: %+v", spans[0].Renewals)
	}

	// 不可改写性：再做一轮操作后——
	// 1) 已关闭的段必须逐字段原样保留；
	// 2) 之前仍进行中的段只允许"追加结束信息"，归属字段不得改变。
	before := spans
	if err := svc.Release(ctx, "h-rel3", res, "owner-C", r3.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Acquire(ctx, "h-a4", res, "owner-D", 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	after, err := svc.OwnershipHistory(ctx, res)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 4 {
		t.Fatalf("spans after more ops = %d, want 4", len(after))
	}
	if !reflect.DeepEqual(after[:2], before[:2]) {
		t.Fatalf("closed spans rewritten by later ops:\nbefore=%+v\nafter=%+v", before[:2], after[:2])
	}
	open, closed := before[2], after[2]
	if closed.EndReason != EndReleased || closed.EndSeq == 0 {
		t.Fatalf("ongoing span should have been closed by release: %+v", closed)
	}
	closed.EndSeq, closed.EndedAt, closed.EndReason = open.EndSeq, open.EndedAt, open.EndReason
	if !reflect.DeepEqual(closed, open) {
		t.Fatalf("closing a span rewrote its ownership fields:\nopen=%+v\nclosed=%+v", open, closed)
	}
	t.Logf("immutability: 2 closed spans byte-identical; ongoing span only gained end info; total spans=%d", len(after))
}

// TestHistory_ValidateAtMatchesLiveDecisions 验证"把携带令牌的提交放回当时位置"，
// 重建判定与集群当时实际给出的结论逐一一致。
func TestHistory_ValidateAtMatchesLiveDecisions(t *testing.T) {
	c := newTestCluster(t, 3)
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 250*time.Millisecond)
	ctx := context.Background()
	const res = "audit/2"

	r1, err := svc.Acquire(ctx, "va-1", res, "A", 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// 当时结论：有效令牌通过。
	if _, err := svc.ValidateAndSubmit(ctx, "va-ok", res, "A", r1.Token); err != nil {
		t.Fatal(err)
	}
	// 当时结论：伪造的更大令牌被拒。
	if _, err := svc.ValidateAndSubmit(ctx, "va-forge", res, "A", r1.Token+100); !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("forged token err=%v", err)
	}
	time.Sleep(350 * time.Millisecond) // 租约到期
	// 当时结论：过期后旧令牌按 lease_expired 被拒。
	if _, err := svc.ValidateAndSubmit(ctx, "va-late", res, "A", r1.Token); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late validate err=%v", err)
	}
	r2, err := svc.Acquire(ctx, "va-2", res, "B", 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	events, err := svc.HistoryEvents(ctx, res)
	if err != nil {
		t.Fatal(err)
	}
	// 每一条 validate 事件：把同样的 owner/token 放回它发生的位置，
	// 重建判定必须与当时记录的结论完全一致。
	validates := 0
	for _, ev := range events {
		if ev.Kind != HistValidate {
			continue
		}
		validates++
		vr, verr := svc.ValidateAt(ctx, res, ev.Seq, ev.Owner, ev.Token)
		if got := reasonOf(verr); vr.Accepted != ev.Accepted || got != ev.Reason {
			t.Fatalf("ValidateAt@%d(%s,%d) = accepted=%v reason=%q, but live decision was accepted=%v reason=%q",
				ev.Seq, ev.Owner, ev.Token, vr.Accepted, got, ev.Accepted, ev.Reason)
		}
		t.Logf("seq=%d validate owner=%s token=%d: live=%v/%s replayed=%v/%s (consistent)",
			ev.Seq, ev.Owner, ev.Token, ev.Accepted, ev.Reason, vr.Accepted, reasonOf(verr))
	}
	if validates != 3 {
		t.Fatalf("validate events=%d, want 3", validates)
	}

	// 时点重建：A 的租约内在位者是 A；过期后、B 接管前的位置无在位者；
	// B 接管后位置在位者是 B 且令牌更新。
	events2, _ := svc.HistoryEvents(ctx, res)
	var acqA, acqB HistoryEvent
	for _, ev := range events2 {
		switch ev.RequestID {
		case "va-1":
			acqA = ev
		case "va-2":
			acqB = ev
		}
	}
	st, err := svc.StateAt(ctx, res, acqA.Seq)
	if err != nil || !st.Found || st.Owner != "A" || st.Token != r1.Token || st.Expired {
		t.Fatalf("StateAt@acquireA = %+v err=%v", st, err)
	}
	st, err = svc.StateAt(ctx, res, acqB.Seq)
	if err != nil || !st.Found || st.Owner != "B" || st.Token != r2.Token || st.Expired {
		t.Fatalf("StateAt@acquireB = %+v err=%v", st, err)
	}
	if r2.Token <= r1.Token {
		t.Fatalf("token not monotonic: %d -> %d", r1.Token, r2.Token)
	}
	t.Logf("reconstruction: @seq%d owner=A token=%d; @seq%d owner=B token=%d",
		acqA.Seq, r1.Token, acqB.Seq, r2.Token)
}

// normState 去掉 time.Time 的单调时钟分量：
// 重启后时间来自 JSON 落盘（无单调分量），与运行中的时间做结构体比较前必须归一。
func normState(st HistoricalState) HistoricalState {
	st.LeaseUntil = st.LeaseUntil.Round(0)
	return st
}

// fmtSpans 压缩打印归属段，便于阅读测试日志。
func fmtSpans(spans []OwnershipSpan) string {
	s := ""
	for i, sp := range spans {
		s += fmt.Sprintf("[%d owner=%s tok=%d seq=%d->%d %s] ",
			i, sp.Owner, sp.Token, sp.StartSeq, sp.EndSeq, sp.EndReason)
	}
	return s
}

// waitConverged 轮询直到所有存活节点导出的历史完全一致（或超时失败）。
func waitConverged(t *testing.T, c *Cluster, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var canon string
	for time.Now().Before(deadline) {
		canon = ""
		ok := true
		for i := 0; i < c.Size(); i++ {
			if c.Network().IsDown(i) {
				continue
			}
			exp, err := c.Node(i).ExportHistory()
			if err != nil {
				ok = false
				break
			}
			s := exp.Canonical()
			if canon == "" {
				canon = s
			} else if s != canon {
				ok = false
				break
			}
		}
		if ok && canon != "" {
			return canon
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("nodes did not converge to identical history")
	return ""
}

// TestHistory_ConsistentAcrossNodesAndRestart 验证：
// 同一份已提交日志在每台节点上投影出完全一致的历史；
// 逐节点滚动重启（磁盘持久化）后，历史与每个历史时点的重建结论都不变。
func TestHistory_ConsistentAcrossNodesAndRestart(t *testing.T) {
	dir := t.TempDir()
	c := newTestCluster(t, 3, WithDataDir(dir))
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 250*time.Millisecond)
	ctx := context.Background()

	// 两个资源上制造完整的归属变迁：释放结束 + 到期被接管。
	r1, err := svc.Acquire(ctx, "rc-1", "rc/a", "A", 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Renew(ctx, "rc-2", "rc/a", "A", r1.Token, 250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := svc.Release(ctx, "rc-3", "rc/a", "A", r1.Token); err != nil {
		t.Fatal(err)
	}
	r2, err := svc.Acquire(ctx, "rc-4", "rc/a", "B", 120*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Acquire(ctx, "rc-5", "rc/b", "X", 250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // rc/a 上 B 的租约到期
	r3, err := svc.Acquire(ctx, "rc-6", "rc/a", "C", 250*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ValidateAndSubmit(ctx, "rc-7", "rc/a", "B", r2.Token); !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("stale token after takeover err=%v", err)
	}

	// 1) 跨节点一致：所有节点导出完全相同的历史。
	canonBefore := waitConverged(t, c, 3*time.Second)
	t.Logf("all %d nodes exported identical history (%d bytes)", c.Size(), len(canonBefore))

	// 记录若干历史位置的重建结论，供重启后比对。
	events, err := svc.HistoryEvents(ctx, "rc/a")
	if err != nil {
		t.Fatal(err)
	}
	type probe struct {
		seq  int64
		want HistoricalState
	}
	var probes []probe
	for _, ev := range events {
		if ev.Kind == HistAcquire && ev.Accepted {
			st, err := svc.StateAt(ctx, "rc/a", ev.Seq)
			if err != nil {
				t.Fatal(err)
			}
			probes = append(probes, probe{seq: ev.Seq, want: st})
		}
	}
	if len(probes) < 3 {
		t.Fatalf("probes=%d, want >= 3", len(probes))
	}

	// 2) 逐节点滚动重启（等价真实进程重启）。
	for i := 0; i < c.Size(); i++ {
		if err := c.Restart(ctx, i); err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
		if _, err := c.WaitForLeader(3 * time.Second); err != nil {
			t.Fatalf("no leader after restart %d: %v", i, err)
		}
	}

	// 3) 重启后：历史仍然跨节点一致，且与重启前逐字节相同。
	canonAfter := waitConverged(t, c, 3*time.Second)
	if canonAfter != canonBefore {
		t.Fatalf("history changed across rolling restart:\nbefore=%s\nafter=%s", canonBefore, canonAfter)
	}
	t.Logf("history identical before/after rolling restart of all %d nodes", c.Size())

	// 4) 重启后同一历史位置：在每台节点上重建，结论与重启前一致。
	for _, p := range probes {
		for i := 0; i < c.Size(); i++ {
			var st HistoricalState
			deadline := time.Now().Add(2 * time.Second)
			for {
				st, err = c.Node(i).StateAt("rc/a", p.seq)
				if !errors.Is(err, ErrPositionUnknown) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("node %d never reached seq %d", i, p.seq)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("node %d StateAt@%d: %v", i, p.seq, err)
			}
			if normState(st) != normState(p.want) {
				t.Fatalf("node %d StateAt@%d = %+v, want %+v (pre-restart)", i, p.seq, st, p.want)
			}
		}
		t.Logf("seq=%d reconstructed identically on all nodes after restart: owner=%s token=%d expired=%v",
			p.seq, p.want.Owner, p.want.Token, p.want.Expired)
	}
	_ = r3
}

// runScriptedScenario 在手动时钟 + 固定种子下跑一份固定脚本，返回导出历史的确定性文本。
// 同一脚本独立跑任意遍，结果必须逐字节相同（RunID 除外）。
func runScriptedScenario(t *testing.T, runID string) string {
	t.Helper()
	start := time.Unix(1_760_000_000, 0)
	clk := NewManualClock(start)
	c := newTestCluster(t, 1, WithClock(clk), WithRandomSeed(7))

	// 推进手动时钟直到选出领导者，随后把时钟归一到固定逻辑时点，
	// 消除"选主花了几次推进"的运行间差异。
	deadline := time.Now().Add(3 * time.Second)
	for c.LeaderID() == -1 {
		clk.Advance(20 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
		if time.Now().After(deadline) {
			t.Fatal("no leader under manual clock")
		}
	}
	clk.Advance(start.Add(2 * time.Second).Sub(clk.Now()))

	svc := NewLockService(c, 200*time.Millisecond)
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	r1, err := svc.Acquire(ctx, "s-1", "scn/x", "A", 0)
	must(err)
	clk.Advance(50 * time.Millisecond)
	_, err = svc.Renew(ctx, "s-2", "scn/x", "A", r1.Token, 0)
	must(err)
	clk.Advance(50 * time.Millisecond)
	_, err = svc.ValidateAndSubmit(ctx, "s-3", "scn/x", "A", r1.Token)
	must(err)
	clk.Advance(300 * time.Millisecond)                 // 租约到期
	r2, err := svc.Acquire(ctx, "s-4", "scn/x", "B", 0) // B 接管，token=2
	must(err)
	if _, err := svc.ValidateAndSubmit(ctx, "s-5", "scn/x", "A", r1.Token); !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("stale token err=%v", err)
	}
	must(svc.Release(ctx, "s-6", "scn/x", "B", r2.Token))
	if _, err := svc.Acquire(ctx, "s-7", "scn/y", "C", 0); err != nil { // 第二个资源
		t.Fatal(err)
	}
	clk.Advance(10 * time.Millisecond)

	exp, err := svc.ExportHistory(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	return exp.Canonical()
}

// TestHistory_DeterministicAcrossRuns 同一套操作场景独立跑两遍：
// 除运行编号外，导出的历史（含归属段、事件、时间戳）必须完全一致。
func TestHistory_DeterministicAcrossRuns(t *testing.T) {
	c1 := runScriptedScenario(t, "run-1")
	c2 := runScriptedScenario(t, "run-2")

	body1 := strings.TrimPrefix(c1, "run run-1\n")
	body2 := strings.TrimPrefix(c2, "run run-2\n")
	if body1 != body2 {
		line1 := strings.Split(body1, "\n")
		line2 := strings.Split(body2, "\n")
		for i := 0; i < len(line1) && i < len(line2); i++ {
			if line1[i] != line2[i] {
				t.Fatalf("run divergence at line %d:\nrun-1: %s\nrun-2: %s", i, line1[i], line2[i])
			}
		}
		t.Fatalf("run divergence: %d vs %d lines", len(line1), len(line2))
	}
	if c1 == c2 {
		t.Fatal("RunID should distinguish the two exports")
	}
	t.Logf("two independent runs exported identical history (%d bytes, %d lines), differing only by RunID",
		len(body1), strings.Count(body1, "\n"))
}

// TestHistory_ConcurrentMultiResource 100 个资源、8 个并发客户端、约 2500 次操作：
// 历史顺序与归属不变量在并发下依然成立，各节点导出一致，时点重建一致，
// 且单次重建查询延迟在可接受范围内。
func TestHistory_ConcurrentMultiResource(t *testing.T) {
	c := newTestCluster(t, 5)
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 300*time.Millisecond)
	ctx := context.Background()

	const (
		resources = 100
		workers   = 8
		iters     = 100
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			owner := fmt.Sprintf("w%d", w)
			for i := 0; i < iters; i++ {
				res := fmt.Sprintf("res/%d", (i*workers+w)%resources)
				id := fmt.Sprintf("w%d-i%d", w, i)
				r, err := svc.Acquire(ctx, id+"-a", res, owner, 300*time.Millisecond)
				if err != nil {
					continue // 被别人持有：拒绝本身也是历史的一部分
				}
				_, _ = svc.ValidateAndSubmit(ctx, id+"-v", res, owner, r.Token)
				_ = svc.Release(ctx, id+"-r", res, owner, r.Token)
			}
		}(w)
	}
	wg.Wait()

	// 1) 全部节点收敛到同一份历史。
	canon := waitConverged(t, c, 5*time.Second)
	t.Logf("converged: %d nodes exported identical history (%d bytes)", c.Size(), len(canon))

	// 2) 每个资源的归属不变量：令牌逐代 +1、段顺序严格递增、互不重叠。
	exp, err := svc.ExportHistory(ctx, "stress")
	if err != nil {
		t.Fatal(err)
	}
	byRes := map[string][]OwnershipSpan{}
	for _, sp := range exp.Spans {
		byRes[sp.Resource] = append(byRes[sp.Resource], sp)
	}
	if len(byRes) != resources {
		t.Fatalf("resources with history=%d, want %d", len(byRes), resources)
	}
	totalSpans := 0
	for res, spans := range byRes {
		for i, sp := range spans {
			if i > 0 {
				prev := spans[i-1]
				if sp.Token != prev.Token+1 {
					t.Fatalf("%s: token jump %d -> %d", res, prev.Token, sp.Token)
				}
				if sp.StartSeq <= prev.StartSeq {
					t.Fatalf("%s: span order unstable %d -> %d", res, prev.StartSeq, sp.StartSeq)
				}
				if prev.EndSeq == 0 || sp.StartSeq < prev.EndSeq {
					t.Fatalf("%s: overlapping spans %+v / %+v", res, prev, sp)
				}
			}
		}
		totalSpans += len(spans)
	}
	t.Logf("invariants hold for %d resources, %d ownership spans (tokens +1, ordered, non-overlapping)",
		len(byRes), totalSpans)

	// 3) 每条 validate 事件放回其位置，重建判定与当时结论一致。
	validates := 0
	for _, ev := range exp.Events {
		if ev.Kind != HistValidate {
			continue
		}
		validates++
		vr, verr := svc.ValidateAt(ctx, ev.Resource, ev.Seq, ev.Owner, ev.Token)
		if got := reasonOf(verr); vr.Accepted != ev.Accepted || got != ev.Reason {
			t.Fatalf("ValidateAt@%d(%s,%s,%d) = %v/%s, live was %v/%s",
				ev.Seq, ev.Resource, ev.Owner, ev.Token, vr.Accepted, got, ev.Accepted, ev.Reason)
		}
	}
	t.Logf("all %d validate events reproduce their live decisions at their positions", validates)

	// 4) 随机历史位置：在每台节点上重建，结论一致；顺带统计查询延迟。
	rng := []int64{}
	for _, ev := range exp.Events {
		if ev.Kind == HistAcquire && ev.Accepted {
			rng = append(rng, ev.Seq)
		}
	}
	start := time.Now()
	queries := 0
	for _, seq := range rng {
		var ref HistoricalState
		for i := 0; i < c.Size(); i++ {
			st, err := c.Node(i).StateAt("res/0", seq)
			if errors.Is(err, ErrPositionUnknown) {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			if i == 0 {
				ref = st
			} else if normState(st) != normState(ref) {
				t.Fatalf("node %d StateAt(res/0,%d) = %+v, node 0 says %+v", i, seq, st, ref)
			}
			queries++
		}
	}
	elapsed := time.Since(start)
	t.Logf("%d StateAt queries across %d nodes in %s (avg %s/query)",
		queries, c.Size(), elapsed, elapsed/time.Duration(max(queries, 1)))
	if queries > 0 && elapsed/time.Duration(queries) > 50*time.Millisecond {
		t.Fatalf("StateAt too slow: avg %s", elapsed/time.Duration(queries))
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
