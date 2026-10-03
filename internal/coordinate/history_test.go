package coordinate

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// scriptedOp 让测试以固定的下标与时间精确重放一段操作过程。
type scriptedOp struct {
	index int64
	at    time.Duration // 相对 base 的时间偏移
	op    Op
}

// runScript 把脚本应用到 FSM，返回每次应用的结果与错误。
func runScript(t *testing.T, f FSM, base time.Time, script []scriptedOp) ([]any, []error) {
	t.Helper()
	var vals []any
	var errs []error
	for _, s := range script {
		op := s.op
		op.Time = base.Add(s.at) // 模拟领导者提议时盖的时间戳
		v, err := f.Apply(op, s.index, base.Add(s.at))
		vals = append(vals, v)
		errs = append(errs, err)
	}
	return vals, errs
}

// ownershipScript 构造一段覆盖三种结束原因的归属过程：
//
//	idx1  A 加锁(token1, ttl100ms)
//	idx2  A 续约            -> 延长至 t0+130ms
//	idx3  A 重入加锁        -> 延长至 t0+140ms
//	idx4  A 主动释放        -> 段1 released
//	idx5  A 重新加锁(token2, ttl100ms)
//	idx6  A 租约到期后再次加锁(token3) -> 段2 expired（未易主）
//	idx7  B 在 A 租约失效后加锁(token4) -> 段3 taken_over（易主），段4 开放
func ownershipScript() (time.Time, []scriptedOp) {
	base := time.Unix(5000, 0)
	ms := time.Millisecond
	return base, []scriptedOp{
		{1, 0 * ms, Op{Kind: OpAcquire, RequestID: "a1", Resource: "R", Owner: "A", TTL: 100 * ms, Term: 1}},
		{2, 30 * ms, Op{Kind: OpRenew, RequestID: "a2", Resource: "R", Owner: "A", Token: 1, TTL: 100 * ms, Term: 1}},
		{3, 40 * ms, Op{Kind: OpAcquire, RequestID: "a3", Resource: "R", Owner: "A", TTL: 100 * ms, Term: 1}},
		{4, 50 * ms, Op{Kind: OpRelease, RequestID: "a4", Resource: "R", Owner: "A", Token: 1, Term: 1}},
		{5, 60 * ms, Op{Kind: OpAcquire, RequestID: "a5", Resource: "R", Owner: "A", TTL: 100 * ms, Term: 1}},
		{6, 200 * ms, Op{Kind: OpAcquire, RequestID: "a6", Resource: "R", Owner: "A", TTL: 100 * ms, Term: 1}},
		{7, 301 * ms, Op{Kind: OpAcquire, RequestID: "a7", Resource: "R", Owner: "B", TTL: 100 * ms, Term: 1}},
	}
}

func TestHistory_SegmentsCoverAllEndReasons(t *testing.T) {
	base, script := ownershipScript()
	f := NewFSM()
	runScript(t, f, base, script)
	ms := time.Millisecond

	h := f.HistoryView().Resource("R")

	// 事件顺序：下标严格递增、内容只增不改。
	if len(h.Events) != 7 {
		t.Fatalf("events=%d, want 7", len(h.Events))
	}
	wantKinds := []EventKind{EvAcquire, EvRenew, EvReenter, EvRelease, EvAcquire, EvAcquire, EvAcquire}
	for i, ev := range h.Events {
		if ev.Index != int64(i+1) {
			t.Fatalf("event %d index=%d, want %d（顺序必须稳定）", i, ev.Index, i+1)
		}
		if ev.Kind != wantKinds[i] {
			t.Fatalf("event %d kind=%s, want %s", i, ev.Kind, wantKinds[i])
		}
	}
	t.Logf("事件流: 7 条事件下标 1..7 严格递增，类型序列 %v", wantKinds)

	// 归属段：4 段，Seq 严格递增，令牌 1..4。
	if len(h.Segments) != 4 {
		t.Fatalf("segments=%d, want 4", len(h.Segments))
	}
	for i, seg := range h.Segments {
		if seg.Seq != int64(i+1) || seg.Token != int64(i+1) {
			t.Fatalf("segment %d seq=%d token=%d, want %d/%d", i, seg.Seq, seg.Token, i+1, i+1)
		}
	}

	s1, s2, s3, s4 := h.Segments[0], h.Segments[1], h.Segments[2], h.Segments[3]

	// 段1：主动释放结束；两次续约/重入延长了租约。
	if !s1.Closed || s1.EndReason != EndReleased || s1.EndIndex != 4 {
		t.Fatalf("seg1 closed=%v reason=%s endIndex=%d, want closed released@4", s1.Closed, s1.EndReason, s1.EndIndex)
	}
	if s1.Renewals != 2 || !s1.LeaseUntil.Equal(base.Add(140*ms).UTC()) {
		t.Fatalf("seg1 renewals=%d leaseUntil=%s, want 2 / t0+140ms", s1.Renewals, s1.LeaseUntil)
	}
	if !s1.EndTime.Equal(base.Add(50 * ms).UTC()) {
		t.Fatalf("seg1 endTime=%s, want release time t0+50ms", s1.EndTime)
	}
	t.Logf("段1: owner=A token=1 续约2次 -> %s 结束于 idx4 (released)", s1.LeaseUntil.Format("15:04:05.000"))

	// 段2：租约自然到期后同一持有者重新获取（未易主 -> expired）。
	if !s2.Closed || s2.EndReason != EndExpired || s2.EndIndex != 6 {
		t.Fatalf("seg2 closed=%v reason=%s endIndex=%d, want closed expired@6", s2.Closed, s2.EndReason, s2.EndIndex)
	}
	if !s2.EndTime.Equal(s2.LeaseUntil) {
		t.Fatalf("seg2 endTime 应等于其最终租约到期时刻: %s vs %s", s2.EndTime, s2.LeaseUntil)
	}
	t.Logf("段2: owner=A token=2 租约到期自然结束 (expired)，同一持有者重新获取")

	// 段3：租约失效后被 B 接管（易主 -> taken_over）。
	if !s3.Closed || s3.EndReason != EndTakenOver || s3.EndIndex != 7 {
		t.Fatalf("seg3 closed=%v reason=%s endIndex=%d, want closed taken_over@7", s3.Closed, s3.EndReason, s3.EndIndex)
	}
	t.Logf("段3: owner=A token=3 被新持有者 B 接管 (taken_over)")

	// 段4：当前开放段，结束原因未定。
	if s4.Closed || s4.EndReason != EndNone || s4.Owner != "B" || s4.Token != 4 {
		t.Fatalf("seg4=%+v, want open segment owner=B token=4", s4)
	}
	t.Logf("段4: owner=B token=4 当前持有中 (open)")
}

func TestHistory_StateAtReconstructsPastMoment(t *testing.T) {
	base, script := ownershipScript()
	f := NewFSM()
	runScript(t, f, base, script)
	ms := time.Millisecond
	h := f.HistoryView()

	checks := []struct {
		name       string
		pos        HistoryPosition
		held       bool
		owner      string
		token      int64
		expired    bool
		leaseUntil time.Duration // 相对 base；held 时有效
	}{
		{"加锁瞬间", HistoryPosition{Index: 1}, true, "A", 1, false, 100 * ms},
		{"续约后", HistoryPosition{Index: 2}, true, "A", 1, false, 130 * ms},
		{"重入后", HistoryPosition{Index: 3}, true, "A", 1, false, 140 * ms},
		{"释放后", HistoryPosition{Index: 4}, false, "", 0, false, 0},
		{"重新加锁", HistoryPosition{Index: 5}, true, "A", 2, false, 160 * ms},
		{"到期后易主前(按时间)", HistoryPosition{Time: base.Add(170 * ms)}, true, "A", 2, true, 160 * ms},
		{"同主再获(段3)", HistoryPosition{Index: 6}, true, "A", 3, false, 300 * ms},
		{"B 接管后", HistoryPosition{Index: 7}, true, "B", 4, false, 401 * ms},
	}
	for _, c := range checks {
		st, err := h.StateAt("R", c.pos)
		if err != nil {
			t.Fatalf("%s: StateAt: %v", c.name, err)
		}
		if st.Held != c.held || st.Owner != c.owner || st.Token != c.token || st.Expired != c.expired {
			t.Fatalf("%s: StateAt=%+v, want held=%v owner=%s token=%d expired=%v",
				c.name, st, c.held, c.owner, c.token, c.expired)
		}
		if c.held && !st.LeaseUntil.Equal(base.Add(c.leaseUntil).UTC()) {
			t.Fatalf("%s: leaseUntil=%s, want t0+%s", c.name, st.LeaseUntil, c.leaseUntil)
		}
		t.Logf("重建[%s]: held=%v owner=%s token=%d expired=%v (pos=%d)",
			c.name, st.Held, st.Owner, st.Token, st.Expired, st.Position)
	}

	// 历史之前的时刻：资源从未被加锁。
	st, err := h.StateAt("R", HistoryPosition{Time: base.Add(-time.Second)})
	if err != nil || st.Held {
		t.Fatalf("before history: %+v err=%v, want not held", st, err)
	}
	// 超出已应用范围的位置：明确报错。
	if _, err := h.StateAt("R", HistoryPosition{Index: 999}); !errors.Is(err, ErrHistoryBeyond) {
		t.Fatalf("beyond position err=%v, want ErrHistoryBeyond", err)
	}
	// 空位置：明确报错。
	if _, err := h.StateAt("R", HistoryPosition{}); !errors.Is(err, ErrHistoryPosition) {
		t.Fatalf("empty position err=%v, want ErrHistoryPosition", err)
	}
}

func TestHistory_ValidateAtMatchesLiveConclusions(t *testing.T) {
	base, script := ownershipScript()
	// 追加两条真实的 validate 日志：一条接受、一条拒绝，
	// 然后用 ValidateAt 在"当时的下标"重放判定，结论必须逐字一致。
	ms := time.Millisecond
	script = append(script,
		scriptedOp{8, 310 * ms, Op{Kind: OpValidate, RequestID: "v8", Resource: "R", Owner: "B", Token: 4, Term: 1}},
		scriptedOp{9, 320 * ms, Op{Kind: OpValidate, RequestID: "v9", Resource: "R", Owner: "A", Token: 1, Term: 1}},
	)
	f := NewFSM()
	vals, errs := runScript(t, f, base, script)
	h := f.HistoryView()

	// 实时结论：idx8 接受（B 持 token4 且租约有效）；idx9 拒绝（token1 陈旧）。
	live8, liveErr8 := vals[7].(ValidateResult), errs[7]
	live9, liveErr9 := vals[8].(ValidateResult), errs[8]
	if !live8.Accepted || liveErr8 != nil {
		t.Fatalf("live validate idx8 should accept: %+v %v", live8, liveErr8)
	}
	if live9.Accepted || !errors.Is(liveErr9, ErrFenceTokenStale) {
		t.Fatalf("live validate idx9 should reject stale: %+v %v", live9, liveErr9)
	}

	// 历史时点重放：同一位置、同一令牌，结论与当时完全一致。
	re8, err8 := h.ValidateAt("R", "B", 4, HistoryPosition{Index: 8})
	re9, err9 := h.ValidateAt("R", "A", 1, HistoryPosition{Index: 9})
	if re8.Accepted != live8.Accepted || re8.Reason != live8.Reason || (err8 == nil) != (liveErr8 == nil) {
		t.Fatalf("idx8 replay=%+v/%v, live=%+v/%v", re8, err8, live8, liveErr8)
	}
	if re9.Accepted != live9.Accepted || re9.Reason != live9.Reason || !errors.Is(err9, ErrFenceTokenStale) {
		t.Fatalf("idx9 replay=%+v/%v, live=%+v/%v", re9, err9, live9, liveErr9)
	}
	t.Logf("idx8 当时=accepted/%s 重放=accepted/%s；idx9 当时=rejected/%s 重放=rejected/%s",
		live8.Reason, re8.Reason, live9.Reason, re9.Reason)

	// 各历史位置上的假设性判定：
	cases := []struct {
		name   string
		owner  string
		token  int64
		pos    HistoryPosition
		accept bool
		err    error
	}{
		{"当时持有者的有效提交", "A", 1, HistoryPosition{Index: 2}, true, nil},
		{"同一时刻的伪造令牌", "A", 99, HistoryPosition{Index: 2}, false, ErrFenceTokenStale},
		{"令牌对但持有者错", "B", 2, HistoryPosition{Index: 5}, false, ErrWrongOwner},
		{"释放后的空档提交", "A", 1, HistoryPosition{Index: 4}, false, ErrLeaseExpired},
		{"租约已到期(按时间)", "A", 2, HistoryPosition{Time: base.Add(170 * ms)}, false, ErrLeaseExpired},
		{"接管后旧令牌", "A", 3, HistoryPosition{Index: 7}, false, ErrFenceTokenStale},
	}
	for _, c := range cases {
		res, err := h.ValidateAt("R", c.owner, c.token, c.pos)
		if res.Accepted != c.accept || !errors.Is(err, c.err) {
			t.Fatalf("%s: accepted=%v err=%v, want %v/%v", c.name, res.Accepted, err, c.accept, c.err)
		}
		t.Logf("假设判定[%s]: owner=%s token=%d => accepted=%v reason=%s",
			c.name, c.owner, c.token, res.Accepted, res.Reason)
	}
}

func TestHistory_ReplayOnFreshFSMIdentical(t *testing.T) {
	base, script := ownershipScript()
	f1 := NewFSM()
	runScript(t, f1, base, script)

	// 把同一段操作过程（同样的下标与时间戳）在全新状态机上原样重放。
	f2 := NewFSM()
	runScript(t, f2, base, script)

	e1 := f1.HistoryView().Export("run-A")
	e2 := f2.HistoryView().Export("run-B")
	e1.Source, e2.Source = "", "" // 运行标识不参与内容比较
	if !reflect.DeepEqual(e1, e2) {
		t.Fatalf("fresh replay produced different history:\n%+v\nvs\n%+v", e1, e2)
	}

	// 每个历史位置的重建结论也一致。
	for i := int64(1); i <= e1.LastIndex; i++ {
		s1, _ := f1.HistoryView().StateAt("R", HistoryPosition{Index: i})
		s2, _ := f2.HistoryView().StateAt("R", HistoryPosition{Index: i})
		if s1 != s2 {
			t.Fatalf("StateAt(%d) differs across replay: %+v vs %+v", i, s1, s2)
		}
	}
	t.Logf("全新节点原样重放：导出历史与 %d 个历史位置的重建结论逐字一致", e1.LastIndex)
}

// ---------------------------------------------------------------------------
// 集群级：跨节点一致、重启一致、跨运行一致、并发多资源
// ---------------------------------------------------------------------------

// waitHistoryConverged 等待所有未宕机节点的已应用下标追平领导者的已应用下标。
func waitHistoryConverged(t *testing.T, c *Cluster) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		target := int64(0)
		for i := 0; i < c.Size(); i++ {
			if a := c.Node(i).AppliedIndex(); a > target {
				target = a
			}
		}
		converged := true
		for i := 0; i < c.Size(); i++ {
			if c.Network().IsDown(i) {
				continue
			}
			if c.Node(i).AppliedIndex() < target {
				converged = false
				break
			}
		}
		if converged {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("nodes did not converge applied index in time")
}

// exportAll 从每个未宕机节点导出历史（Source 置空以便逐字比较）。
func exportAll(t *testing.T, c *Cluster) []ExportedHistory {
	t.Helper()
	var out []ExportedHistory
	for i := 0; i < c.Size(); i++ {
		if c.Network().IsDown(i) {
			continue
		}
		e, err := c.Node(i).ExportHistory()
		if err != nil {
			t.Fatalf("export from node %d: %v", i, err)
		}
		e.Source = ""
		out = append(out, e)
	}
	return out
}

// driveLockOps 在集群上执行一段覆盖 acquire/renew/reenter/release/validate 的操作。
func driveLockOps(t *testing.T, svc *LockService) (tokenA, tokenB int64) {
	t.Helper()
	ctx := context.Background()
	ttl := 30 * time.Second // 长租约：本用例不依赖到期时序

	r1, err := svc.Acquire(ctx, "h-a1", "res/alpha", "owner-A", ttl)
	if err != nil {
		t.Fatalf("acquire A: %v", err)
	}
	tokenA = r1.Token
	if _, err := svc.Renew(ctx, "h-r1", "res/alpha", "owner-A", tokenA, ttl); err != nil {
		t.Fatalf("renew: %v", err)
	}
	// 同主重入：延长租约，令牌不变。
	r2, err := svc.Acquire(ctx, "h-a2", "res/alpha", "owner-A", ttl)
	if err != nil || r2.Token != tokenA {
		t.Fatalf("reenter token=%d err=%v, want %d", r2.Token, err, tokenA)
	}
	if vr, err := svc.ValidateAndSubmit(ctx, "h-v1", "res/alpha", "owner-A", tokenA); err != nil || !vr.Accepted {
		t.Fatalf("validate held token: %v %+v", err, vr)
	}
	if err := svc.Release(ctx, "h-x1", "res/alpha", "owner-A", tokenA); err != nil {
		t.Fatalf("release: %v", err)
	}
	r3, err := svc.Acquire(ctx, "h-a3", "res/alpha", "owner-B", ttl)
	if err != nil {
		t.Fatalf("acquire B: %v", err)
	}
	tokenB = r3.Token
	if tokenB != tokenA+1 {
		t.Fatalf("tokenB=%d, want %d", tokenB, tokenA+1)
	}
	// 旧令牌提交必须被拒（栅栏），该结论也要能在历史时点复现。
	if _, err := svc.ValidateAndSubmit(ctx, "h-v2", "res/alpha", "owner-A", tokenA); !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("stale validate err=%v, want ErrFenceTokenStale", err)
	}
	// 第二个资源：验证多资源互不干扰。
	if _, err := svc.Acquire(ctx, "h-b1", "res/beta", "owner-C", ttl); err != nil {
		t.Fatalf("acquire beta: %v", err)
	}
	return tokenA, tokenB
}

func TestHistory_ClusterConsistentAcrossNodes(t *testing.T) {
	c := newTestCluster(t, 3)
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 30*time.Second)
	tokenA, tokenB := driveLockOps(t, svc)
	waitHistoryConverged(t, c)

	// 1) 全量导出逐节点逐字一致（Source 除外）。
	exports := exportAll(t, c)
	for i := 1; i < len(exports); i++ {
		if !reflect.DeepEqual(exports[0], exports[i]) {
			t.Fatalf("node exports differ:\nnode0=%+v\nnode%d=%+v", exports[0], i, exports[i])
		}
	}
	t.Logf("跨节点一致：%d 个节点导出的归属历史逐字一致（lastIndex=%d, 资源数=%d）",
		len(exports), exports[0].LastIndex, len(exports[0].Resources))

	// 2) 归属历史内容符合操作过程。
	h, err := svc.HistoryOf(context.Background(), "res/alpha")
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Segments) != 2 {
		t.Fatalf("segments=%d, want 2", len(h.Segments))
	}
	seg1, seg2 := h.Segments[0], h.Segments[1]
	if seg1.Owner != "owner-A" || seg1.Token != tokenA || !seg1.Closed || seg1.EndReason != EndReleased {
		t.Fatalf("seg1=%+v, want owner-A released", seg1)
	}
	if seg1.Renewals != 2 {
		t.Fatalf("seg1 renewals=%d, want 2 (renew + reenter)", seg1.Renewals)
	}
	if seg2.Owner != "owner-B" || seg2.Token != tokenB || seg2.Closed {
		t.Fatalf("seg2=%+v, want open owner-B", seg2)
	}
	t.Logf("res/alpha 归属: [owner-A token=%d 续约2次 released] -> [owner-B token=%d open]",
		tokenA, tokenB)

	// 3) 每个事件下标作为历史位置，在所有节点上重建结论一致。
	for _, ev := range h.Events {
		var want LockStateAt
		for i := 0; i < c.Size(); i++ {
			st, err := c.Node(i).StateAt("res/alpha", HistoryPosition{Index: ev.Index})
			if err != nil {
				t.Fatalf("node %d StateAt(%d): %v", i, ev.Index, err)
			}
			if i == 0 {
				want = st
			} else if st != want {
				t.Fatalf("StateAt(%d) differs across nodes: %+v vs %+v", ev.Index, want, st)
			}
		}
	}

	// 4) 历史时点的栅栏判定与当时实时结论一致：
	//    段1 内 owner-A/tokenA 通过；段2 开始后同一令牌被判 fence_token_stale。
	vr1, err := svc.ValidateAt(context.Background(), "res/alpha", "owner-A", tokenA,
		HistoryPosition{Index: seg1.StartIndex})
	if err != nil || !vr1.Accepted {
		t.Fatalf("ValidateAt seg1 start: %+v %v, want accepted", vr1, err)
	}
	vr2, err := svc.ValidateAt(context.Background(), "res/alpha", "owner-A", tokenA,
		HistoryPosition{Index: seg2.StartIndex})
	if !errors.Is(err, ErrFenceTokenStale) || vr2.Accepted {
		t.Fatalf("ValidateAt seg2 start: %+v %v, want fence_token_stale", vr2, err)
	}
	t.Logf("历史时点判定: seg1 内 token=%d => accepted；seg2 开始后同令牌 => %s",
		tokenA, vr2.Reason)
}

func TestHistory_RestartPreservesHistoryAndConclusions(t *testing.T) {
	dir := t.TempDir()
	c := newTestCluster(t, 3, WithDataDir(dir))
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 30*time.Second)
	tokenA, _ := driveLockOps(t, svc)
	waitHistoryConverged(t, c)

	before := exportAll(t, c)[0]
	// 记录若干历史位置的判定结论，重启后必须逐字复现。
	h, err := svc.HistoryOf(context.Background(), "res/alpha")
	if err != nil {
		t.Fatal(err)
	}
	type probe struct {
		pos     HistoryPosition
		state   LockStateAt
		verdict ValidateResult
		verr    error
	}
	var probes []probe
	for _, ev := range h.Events {
		pos := HistoryPosition{Index: ev.Index}
		st, err := svc.StateAt(context.Background(), "res/alpha", pos)
		if err != nil {
			t.Fatal(err)
		}
		vr, verr := svc.ValidateAt(context.Background(), "res/alpha", "owner-A", tokenA, pos)
		probes = append(probes, probe{pos, st, vr, verr})
	}
	t.Logf("重启前: lastIndex=%d, res/alpha 事件=%d, 记录 %d 个历史位置的结论",
		before.LastIndex, len(h.Events), len(probes))

	// 逐节点滚动重启（磁盘持久化，等价真实进程重启）。
	for i := 0; i < c.Size(); i++ {
		if err := c.Restart(context.Background(), i); err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
		if _, err := c.WaitForLeader(3 * time.Second); err != nil {
			t.Fatalf("no leader after restart %d: %v", i, err)
		}
	}
	waitHistoryConverged(t, c)

	// 重启后：资源归属历史逐字一致；时间线前缀一致（重启后的新选举
	// 只会在日志末尾追加 no-op，不改写已有历史）。
	after := exportAll(t, c)
	for i, e := range after {
		if !reflect.DeepEqual(before.Resources, e.Resources) {
			t.Fatalf("node %d resources differ after restart", i)
		}
		if len(e.Times) < len(before.Times) {
			t.Fatalf("node %d times shrank after restart: %d < %d", i, len(e.Times), len(before.Times))
		}
		for j := range before.Times {
			if e.Times[j] != before.Times[j] {
				t.Fatalf("node %d times[%d] changed across restart: %v -> %v", i, j, before.Times[j], e.Times[j])
			}
		}
	}
	t.Logf("重启后: 归属历史逐字一致，时间线前缀 %d 条不变（新条目只追加不改写）", len(before.Times))

	// 每个历史位置的重建状态与栅栏判定，重启后逐字复现。
	for _, p := range probes {
		st, err := svc.StateAt(context.Background(), "res/alpha", p.pos)
		if err != nil {
			t.Fatalf("StateAt(%d) after restart: %v", p.pos.Index, err)
		}
		if st != p.state {
			t.Fatalf("StateAt(%d) changed across restart: %+v -> %+v", p.pos.Index, p.state, st)
		}
		vr, verr := svc.ValidateAt(context.Background(), "res/alpha", "owner-A", tokenA, p.pos)
		if vr != p.verdict || (verr == nil) != (p.verr == nil) {
			t.Fatalf("ValidateAt(%d) changed across restart: %+v/%v -> %+v/%v",
				p.pos.Index, p.verdict, p.verr, vr, verr)
		}
	}
	t.Logf("重启后: %d 个历史位置的重建状态与栅栏判定逐字复现", len(probes))
}

// normExport 把导出历史归一化为"与运行无关"的内容：
// 去掉日志下标、绝对时间戳等运行特定信息，只保留归属/令牌/结束原因/
// 续约次数与租约时长（LeaseUntil-Time，恒定等于 TTL）。
type normExport struct {
	resources map[string]normResource
}

type normResource struct {
	events   []normEvent
	segments []normSegment
}

type normEvent struct {
	kind  EventKind
	owner string
	token int64
	ttl   time.Duration
}

type normSegment struct {
	owner    string
	token    int64
	renewals int
	closed   bool
	reason   EndReason
}

func normalizeExport(e ExportedHistory) normExport {
	n := normExport{resources: map[string]normResource{}}
	for name, rh := range e.Resources {
		nr := normResource{}
		for _, ev := range rh.Events {
			ne := normEvent{kind: ev.Kind, owner: ev.Owner, token: ev.Token}
			if ev.Kind != EvRelease {
				ne.ttl = ev.LeaseUntil.Sub(ev.Time)
			}
			nr.events = append(nr.events, ne)
		}
		for _, seg := range rh.Segments {
			nr.segments = append(nr.segments, normSegment{
				owner: seg.Owner, token: seg.Token, renewals: seg.Renewals,
				closed: seg.Closed, reason: seg.EndReason,
			})
		}
		n.resources[name] = nr
	}
	return n
}

// runScenarioOnce 在全新集群上执行固定脚本并导出历史。
func runScenarioOnce(t *testing.T) (ExportedHistory, *Cluster) {
	t.Helper()
	c := newTestCluster(t, 3)
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 30*time.Second)
	driveLockOps(t, svc)
	waitHistoryConverged(t, c)
	e, err := c.Node(c.LeaderID()).ExportHistory()
	if err != nil {
		t.Fatal(err)
	}
	e.Source = ""
	return e, c
}

func TestHistory_TwoRunsSameScenarioConsistent(t *testing.T) {
	e1, c1 := runScenarioOnce(t)
	e2, c2 := runScenarioOnce(t)

	n1, n2 := normalizeExport(e1), normalizeExport(e2)
	if !reflect.DeepEqual(n1, n2) {
		t.Fatalf("two runs produced different normalized history:\nrun1=%+v\nrun2=%+v", n1, n2)
	}
	t.Logf("跨运行一致：两遍独立运行的归一化历史逐字一致（资源数=%d）", len(n1.resources))

	// 每一遍里，每个资源的每次"新一代"位置，用当时令牌判定都必须通过。
	checkRun := func(tag string, c *Cluster, e ExportedHistory) {
		t.Helper()
		for name, rh := range e.Resources {
			for _, ev := range rh.Events {
				if ev.Kind != EvAcquire {
					continue
				}
				vr, err := c.Node(0).ValidateAt(name, ev.Owner, ev.Token, HistoryPosition{Index: ev.Index})
				if err != nil || !vr.Accepted {
					t.Fatalf("%s: ValidateAt(%s, %s, token=%d, idx=%d) = %+v/%v, want accepted",
						tag, name, ev.Owner, ev.Token, ev.Index, vr, err)
				}
			}
		}
	}
	checkRun("run1", c1, e1)
	checkRun("run2", c2, e2)
	t.Logf("两遍运行中每个新一代位置的栅栏判定均为 accepted（与当时实时结论一致）")
}

func TestHistory_ConcurrentMultiResourceScale(t *testing.T) {
	c := newTestCluster(t, 3)
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 2*time.Second)
	ctx := context.Background()

	const resources = 100
	const workers = 16
	// 每个资源 11 次操作（含 3 代归属），共 1100 次操作。
	var wg sync.WaitGroup
	resCh := make(chan int, resources)
	for i := 0; i < resources; i++ {
		resCh <- i
	}
	close(resCh)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for ri := range resCh {
				res := fmt.Sprintf("scale/res-%03d", ri)
				owner := fmt.Sprintf("owner-%d", ri)
				id := func(k int) string { return fmt.Sprintf("cw-%d-%d", ri, k) }
				ttl := 2 * time.Second

				r1, err := svc.Acquire(ctx, id(1), res, owner, ttl)
				if err != nil {
					t.Errorf("res %d acquire1: %v", ri, err)
					return
				}
				if vr, err := svc.ValidateAndSubmit(ctx, id(2), res, owner, r1.Token); err != nil || !vr.Accepted {
					t.Errorf("res %d validate1: %v %+v", ri, err, vr)
				}
				if _, err := svc.Renew(ctx, id(3), res, owner, r1.Token, ttl); err != nil {
					t.Errorf("res %d renew: %v", ri, err)
				}
				if _, err := svc.ValidateAndSubmit(ctx, id(4), res, owner, r1.Token+100); !errors.Is(err, ErrFenceTokenStale) {
					t.Errorf("res %d forged token err=%v, want stale", ri, err)
				}
				if err := svc.Release(ctx, id(5), res, owner, r1.Token); err != nil {
					t.Errorf("res %d release1: %v", ri, err)
				}
				r2, err := svc.Acquire(ctx, id(6), res, owner, ttl)
				if err != nil || r2.Token != r1.Token+1 {
					t.Errorf("res %d acquire2 token=%d err=%v", ri, r2.Token, err)
					return
				}
				if vr, err := svc.ValidateAndSubmit(ctx, id(7), res, owner, r2.Token); err != nil || !vr.Accepted {
					t.Errorf("res %d validate2: %v %+v", ri, err, vr)
				}
				if err := svc.Release(ctx, id(8), res, owner, r2.Token); err != nil {
					t.Errorf("res %d release2: %v", ri, err)
				}
				r3, err := svc.Acquire(ctx, id(9), res, owner, ttl)
				if err != nil || r3.Token != r2.Token+1 {
					t.Errorf("res %d acquire3 token=%d err=%v", ri, r3.Token, err)
					return
				}
				if _, err := svc.ValidateAndSubmit(ctx, id(10), res, "intruder", r3.Token); !errors.Is(err, ErrWrongOwner) {
					t.Errorf("res %d wrong owner err=%v, want ErrWrongOwner", ri, err)
				}
				if err := svc.Release(ctx, id(11), res, owner, r3.Token); err != nil {
					t.Errorf("res %d release3: %v", ri, err)
				}
			}
		}(w)
	}
	wg.Wait()
	waitHistoryConverged(t, c)

	// 1) 跨节点导出逐字一致。
	exports := exportAll(t, c)
	for i := 1; i < len(exports); i++ {
		if !reflect.DeepEqual(exports[0], exports[i]) {
			t.Fatalf("node %d export differs under concurrency", i)
		}
	}
	ex := exports[0]
	t.Logf("并发规模: %d 资源 x 11 操作 = 1100 次操作, lastIndex=%d, %d 个节点导出逐字一致",
		resources, ex.LastIndex, len(exports))

	// 2) 每个资源的历史结构不变式。
	if len(ex.Resources) != resources {
		t.Fatalf("resources=%d, want %d", len(ex.Resources), resources)
	}
	totalEvents := 0
	for name, rh := range ex.Resources {
		// 事件下标严格递增（顺序稳定可比较）。
		for i := 1; i < len(rh.Events); i++ {
			if rh.Events[i].Index <= rh.Events[i-1].Index {
				t.Fatalf("%s: event indices not strictly increasing: %d -> %d",
					name, rh.Events[i-1].Index, rh.Events[i].Index)
			}
		}
		// 3 代归属，令牌 1/2/3，均主动释放结束。
		if len(rh.Segments) != 3 {
			t.Fatalf("%s: segments=%d, want 3", name, len(rh.Segments))
		}
		for i, seg := range rh.Segments {
			if seg.Token != int64(i+1) {
				t.Fatalf("%s: seg %d token=%d, want %d", name, i, seg.Token, i+1)
			}
			if !seg.Closed || seg.EndReason != EndReleased {
				t.Fatalf("%s: seg %d closed=%v reason=%s, want released", name, i, seg.Closed, seg.EndReason)
			}
		}
		if rh.Segments[0].Renewals != 1 {
			t.Fatalf("%s: seg1 renewals=%d, want 1", name, rh.Segments[0].Renewals)
		}
		totalEvents += len(rh.Events)
	}
	t.Logf("结构不变式: %d 个资源共 %d 条事件，下标全部严格递增；每资源 3 段令牌 1/2/3 均 released",
		resources, totalEvents)

	// 3) 历史时点重建抽样核对 + 查询耗时。
	start := time.Now()
	queries := 0
	for name, rh := range ex.Resources {
		for _, ev := range rh.Events {
			st, err := svc.StateAt(ctx, name, HistoryPosition{Index: ev.Index})
			if err != nil {
				t.Fatalf("StateAt %s@%d: %v", name, ev.Index, err)
			}
			queries++
			switch ev.Kind {
			case EvAcquire:
				if !st.Held || st.Token != ev.Token || st.Expired {
					t.Fatalf("%s@%d acquire: %+v, want held token=%d", name, ev.Index, st, ev.Token)
				}
				vr, verr := svc.ValidateAt(ctx, name, ev.Owner, ev.Token, HistoryPosition{Index: ev.Index})
				if verr != nil || !vr.Accepted {
					t.Fatalf("%s@%d ValidateAt: %+v %v, want accepted", name, ev.Index, vr, verr)
				}
				queries++
			case EvRelease:
				if st.Held {
					t.Fatalf("%s@%d release: %+v, want not held", name, ev.Index, st)
				}
			}
		}
	}
	elapsed := time.Since(start)
	t.Logf("历史时点重建: %d 次 StateAt/ValidateAt 查询全部与事件一致，耗时 %s（平均 %s/次）",
		queries, elapsed, elapsed/time.Duration(queries))
	if elapsed > 30*time.Second {
		t.Fatalf("history queries too slow: %s for %d queries", elapsed, queries)
	}
}
