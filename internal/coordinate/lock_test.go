package coordinate

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newLockService(t *testing.T, n int) (*Cluster, *LockService) {
	t.Helper()
	c := newTestCluster(t, n)
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	return c, NewLockService(c, 150*time.Millisecond)
}

func TestLock_BasicAcquireAndFence(t *testing.T) {
	_, svc := newLockService(t, 3)
	ctx := context.Background()

	r, err := svc.Acquire(ctx, "req-1", "orders/42", "client-A", 150*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if r.Token != 1 {
		t.Fatalf("first token=%d, want 1", r.Token)
	}
	t.Logf("ACQUIRE resource=orders/42 owner=client-A token=%d leaseUntil=%s term=%d",
		r.Token, r.LeaseUntil.Format(time.RFC3339Nano), r.Term)

	// 其他持有者在租约内被明确拒绝，原因可区分。
	if _, err := svc.Acquire(ctx, "req-2", "orders/42", "client-B", 150*time.Millisecond); !errors.Is(err, ErrHeldByOther) {
		t.Fatalf("contended acquire err=%v, want ErrHeldByOther", err)
	}

	// 正确令牌提交受保护操作：接受。
	vr, err := svc.ValidateAndSubmit(ctx, "req-3", "orders/42", "client-A", r.Token)
	if err != nil || !vr.Accepted {
		t.Fatalf("valid fence accepted=%v err=%v", vr.Accepted, err)
	}
	t.Logf("VALIDATE owner=client-A token=%d => accepted (reason=%s, term=%d)", r.Token, vr.Reason, r.Term)

	// 伪造的更大令牌：拒绝。
	if _, err := svc.ValidateAndSubmit(ctx, "req-4", "orders/42", "client-A", r.Token+100); !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("future token err=%v, want ErrFenceTokenStale", err)
	}
}

func TestLock_DuplicateRequestsIdempotent(t *testing.T) {
	_, svc := newLockService(t, 3)
	ctx := context.Background()

	r1, err := svc.Acquire(ctx, "same-id", "res", "A", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// 相同 requestID 重放三次：结论、令牌必须完全一致。
	for i := 0; i < 3; i++ {
		ri, err := svc.Acquire(ctx, "same-id", "res", "A", 300*time.Millisecond)
		if err != nil || ri.Token != r1.Token || !ri.LeaseUntil.Equal(r1.LeaseUntil) {
			t.Fatalf("replay %d changed: %+v err=%v (orig token=%d)", i, ri, err, r1.Token)
		}
	}

	// 释放也幂等：重复提交同一 requestID 不报错（返回首次结论）。
	if err := svc.Release(ctx, "rel-id", "res", "A", r1.Token); err != nil {
		t.Fatalf("release: %v", err)
	}
	if err := svc.Release(ctx, "rel-id", "res", "A", r1.Token); err != nil {
		t.Fatalf("duplicate release must be idempotent, got %v", err)
	}
	// 用全新 requestID 释放一个已不存在的锁：同样是幂等成功。
	if err := svc.Release(ctx, "rel-id-2", "res", "A", r1.Token); err != nil {
		t.Fatalf("release of absent lock must succeed, got %v", err)
	}
	// 释放后新 acquire 只应拿到令牌 2（重放没有制造额外一代）。
	r2, err := svc.Acquire(ctx, "next-id", "res", "B", 300*time.Millisecond)
	if err != nil || r2.Token != 2 {
		t.Fatalf("token after idempotent release = %d err=%v, want 2", r2.Token, err)
	}
}

func TestLock_LeaseExpiryBlocksOldOwner(t *testing.T) {
	_, svc := newLockService(t, 3)
	ctx := context.Background()
	ttl := 80 * time.Millisecond

	r, err := svc.Acquire(ctx, "acq", "db/conn", "A", ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("ACQUIRE owner=A token=%d ttl=%s", r.Token, ttl)

	// 在过期前沿期成功。
	renewed, err := svc.Renew(ctx, "renew-1", "db/conn", "A", r.Token, ttl)
	if err != nil {
		t.Fatalf("renew within lease: %v", err)
	}
	t.Logf("RENEW owner=A token=%d => new leaseUntil=%s", r.Token, renewed.LeaseUntil.Format(time.RFC3339Nano))

	// 等待超过续约后的租约。
	time.Sleep(ttl + 120*time.Millisecond)

	// 旧持有者续约失败：ErrLeaseExpired。
	if _, err := svc.Renew(ctx, "renew-late", "db/conn", "A", r.Token, ttl); !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late renew err=%v, want ErrLeaseExpired", err)
	}
	// 旧持有者携带旧令牌提交受保护操作：明确拒绝。
	vr, verr := svc.ValidateAndSubmit(ctx, "val-late", "db/conn", "A", r.Token)
	if !errors.Is(verr, ErrLeaseExpired) {
		t.Fatalf("late validate err=%v, want ErrLeaseExpired", verr)
	}
	t.Logf("after expiry: owner=A token=%d => REJECTED reason=%q (%v)", r.Token, vr.Reason, verr)

	// 新持有者接管，令牌严格递增。
	r2, err := svc.Acquire(ctx, "acq2", "db/conn", "B", ttl)
	if err != nil || r2.Token <= r.Token {
		t.Fatalf("new owner token=%d err=%v, want > %d", r2.Token, err, r.Token)
	}
	// A 的旧令牌现在必须被判为栅栏陈旧。
	if _, err := svc.ValidateAndSubmit(ctx, "val-stale", "db/conn", "A", r.Token); !errors.Is(err, ErrFenceTokenStale) {
		t.Fatalf("old token after takeover err=%v, want ErrFenceTokenStale", err)
	}
	t.Logf("TAKEOVER owner=B token=%d; old owner=A token=%d => fence_token_stale", r2.Token, r.Token)
}

func TestLock_PartitionedOldLeaderCannotCommit(t *testing.T) {
	c, svc := newLockService(t, 5)
	ctx := context.Background()
	ttl := 400 * time.Millisecond

	old := c.LeaderID()
	r, err := svc.Acquire(ctx, "p1", "shared-x", "owner-majority", ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("before partition: leader=node%d term=%d owner=owner-majority token=%d",
		old, r.Term, r.Token)

	// 把集群切成 {旧主 + 一个追随者}（2，少数派）与其余 3 个节点（多数派）。
	// 多数派一侧包含 3 个节点，必然能凑出自己的多数派，选举结论确定。
	minorityPeer := -1
	for i := 0; i < c.Size(); i++ {
		if i != old {
			minorityPeer = i
			break
		}
	}
	minority := map[int]bool{old: true, minorityPeer: true}
	c.Network().Partition(func(from, to int) bool {
		return minority[from] != minority[to]
	})

	// 多数派一侧选出更高任期新领导者。
	newLeader := -1
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < c.Size(); i++ {
			if !minority[i] && c.Node(i).Role() == Leader && c.Node(i).Term() > r.Term {
				newLeader = i
			}
		}
		if newLeader != -1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if newLeader == -1 {
		t.Fatal("majority side did not elect new leader")
	}
	t.Logf("partition: minority=%v (old leader node%d); new leader node%d term=%d",
		[]int{old, minorityPeer}, old, newLeader, c.Node(newLeader).Term())

	// 分区期间：多数派一侧可以正常提交；旧主所在少数派凑不齐多数派。
	// 旧主对同一资源的加锁/续约即使在本地记录，也不可能复制成功。
	oldSubmitCtx, oldCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer oldCancel()
	if _, err := c.Node(old).Submit(oldSubmitCtx, Op{
		Kind: OpAcquire, RequestID: "p-old", Resource: "shared-stale",
		Owner: "stale-owner", TTL: ttl,
	}); err == nil {
		t.Fatal("partitioned old leader unexpectedly committed without majority")
	} else {
		t.Logf("old leader node%d commit correctly BLOCKED without majority: %v", old, err)
	}

	svcCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	r2, err := svc.Acquire(svcCtx, "p2", "shared-y", "owner-majority2", ttl)
	if err != nil {
		t.Fatalf("acquire via new majority leader failed: %v", err)
	}
	if r2.Term <= r.Term {
		t.Fatalf("op committed under stale term: %d <= %d", r2.Term, r.Term)
	}
	t.Logf("acquire during partition served by new leader term=%d token=%d", r2.Term, r2.Token)

	// 恢复分区，旧主因更高任期退下；再做一次操作，确认全程单主。
	c.Network().Heal()
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(countLeaders(c)) == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if _, err := svc.Acquire(context.Background(), "p3", "shared-z", "owner-majority3", ttl); err != nil {
		t.Fatalf("acquire after heal: %v", err)
	}
	leaders := 0
	for i := 0; i < c.Size(); i++ {
		if c.Node(i).Role() == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("leaders after heal=%d, want 1", leaders)
	}
	t.Logf("partition healed: sole leader node%d term=%d", c.LeaderID(), c.Node(c.LeaderID()).Term())
}

func TestLock_RestartLeaderKeepsTermAndLockState(t *testing.T) {
	dir := t.TempDir()
	c := newTestCluster(t, 3, WithDataDir(dir))
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 300*time.Millisecond)
	ctx := context.Background()

	// 租约时长给足 5s：重启 + 重新选主的窗口内租约仍然有效，
	// 本用例验证的是"状态持久、任期/令牌不回退"，而不是租约到期。
	r, err := svc.Acquire(ctx, "k1", "durable/1", "persistent-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	leader := c.LeaderID()
	t.Logf("acquired on leader node%d term=%d token=%d", leader, r.Term, r.Token)

	// 重启领导者：磁盘上的 currentTerm / votedFor / 日志全部保留。
	if err := c.Restart(ctx, leader); err != nil {
		t.Fatalf("restart: %v", err)
	}
	t.Logf("restarted node%d: term on disk=%d (must not regress)", leader, c.Node(leader).Term())
	if c.Node(leader).Term() < r.Term {
		t.Fatalf("term regressed after restart: %d < %d", c.Node(leader).Term(), r.Term)
	}

	// 等待集群稳定，新（或原）领导者上读回的锁状态必须还在。
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	var st LockState
	for time.Now().Before(deadline) {
		got, lerr := svc.Lookup(ctx, "durable/1")
		if lerr == nil && got.Owner == "persistent-owner" && got.Token == r.Token {
			st = got
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if st.Owner != "persistent-owner" {
		t.Fatalf("lock state lost across restart: %+v", st)
	}
	t.Logf("after restart lookup: owner=%s token=%d term=%d leaseUntil=%s",
		st.Owner, st.Token, st.Term, st.LeaseUntil.Format(time.RFC3339Nano))

	// 重启后再发放的令牌必须继续单调递增（不回退到 1）。
	r2, err := svc.Acquire(ctx, "k2", "durable/2", "another", 300*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Token != 1 {
		t.Fatalf("brand-new resource first token=%d, want 1", r2.Token)
	}
	// 释放 durable/1 后重新抢，令牌必须是 2 而不是回退到 1。
	if err := svc.Release(ctx, "k3", "durable/1", "persistent-owner", r.Token); err != nil {
		t.Fatal(err)
	}
	r3, err := svc.Acquire(ctx, "k4", "durable/1", "persistent-owner", 300*time.Millisecond)
	if err != nil || r3.Token != r.Token+1 {
		t.Fatalf("token after release+reacquire = %d err=%v, want %d", r3.Token, err, r.Token+1)
	}
}
