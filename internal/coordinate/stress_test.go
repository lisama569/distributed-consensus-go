package coordinate

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestStress_ConcurrentAcquireMutualExclusion 并发加锁同一资源：
// 任意时刻只有一个客户端能持有"有效租约"，且所有成功 acquire 的令牌严格递增。
func TestStress_ConcurrentAcquireMutualExclusion(t *testing.T) {
	c := newTestCluster(t, 5)
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 120*time.Millisecond)
	ctx := context.Background()

	const clients = 12
	const rounds = 4
	var (
		mu     sync.Mutex
		tokens []int64
		owners = map[string]int64{}
		wg     sync.WaitGroup
	)

	for cl := 0; cl < clients; cl++ {
		wg.Add(1)
		go func(cl int) {
			defer wg.Done()
			owner := fmt.Sprintf("client-%d", cl)
			for rd := 0; rd < rounds; rd++ {
				// 每次"新的加锁尝试"使用新 requestID：被 ErrHeldByOther 拒绝后
				// 必须换 ID，否则去重表会永久重放那次拒绝（这正是幂等语义）。
				attempt := 0
				for {
					attempt++
					reqID := fmt.Sprintf("c%d-r%d-a%d", cl, rd, attempt)
					r, err := svc.Acquire(ctx, reqID, "hot/resource", owner, 120*time.Millisecond)
					if err == nil {
						mu.Lock()
						tokens = append(tokens, r.Token)
						// 同一 owner 成功拿到的令牌不得回退。
						if prev, ok := owners[owner]; ok && prev > r.Token {
							mu.Unlock()
							t.Errorf("owner %s token went backwards: %d -> %d", owner, prev, r.Token)
							return
						}
						owners[owner] = r.Token
						mu.Unlock()
						// 持锁期间提交一个受保护操作，必须被接受。
						vr, verr := svc.ValidateAndSubmit(ctx, reqID+"-v", "hot/resource", owner, r.Token)
						if verr != nil || !vr.Accepted {
							t.Errorf("just-acquired token rejected: %v accepted=%v", verr, vr.Accepted)
						}
						// 显式释放，使下一轮成为全新代次（新 requestID，幂等安全）。
						if err := svc.Release(ctx, reqID+"-rel", "hot/resource", owner, r.Token); err != nil {
							t.Errorf("release: %v", err)
						}
						break
					}
					if !errors.Is(err, ErrHeldByOther) {
						t.Errorf("unexpected acquire error: %v", err)
						return
					}
					// 被别人持有时等待其租约过期后再用新 ID 尝试。
					select {
					case <-ctx.Done():
						return
					case <-time.After(20 * time.Millisecond):
					}
				}
			}
		}(cl)
	}
	wg.Wait()

	// 全部成功令牌必须严格等于 1..N（每个新代次 +1，不重号、不跳变）。
	mu.Lock()
	defer mu.Unlock()
	if len(tokens) != clients*rounds {
		t.Fatalf("successful acquires=%d, want %d", len(tokens), clients*rounds)
	}
	seen := map[int64]bool{}
	var max int64
	for _, tk := range tokens {
		if seen[tk] {
			t.Fatalf("token %d granted to two different generations", tk)
		}
		seen[tk] = true
		if tk > max {
			max = tk
		}
	}
	if max != int64(clients*rounds) {
		t.Fatalf("max token=%d, want %d (tokens=%v)", max, clients*rounds, tokens)
	}
	t.Logf("concurrency: %d clients x %d rounds => %d strictly ordered tokens, max token=%d",
		clients, rounds, len(tokens), max)
}

// TestStress_RollingRestartPreservesTokens 逐节点滚动重启（磁盘持久化）后，
// 集群仍可服务，且同一资源释放后令牌继续递增、不回退。
func TestStress_RollingRestartPreservesTokens(t *testing.T) {
	dir := t.TempDir()
	c := newTestCluster(t, 3, WithDataDir(dir))
	if _, err := c.WaitForLeader(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	svc := NewLockService(c, 500*time.Millisecond)
	ctx := context.Background()

	r1, err := svc.Acquire(ctx, "s1", "roll/res", "o1", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	// 逐节点重启；每轮等集群重新选出唯一主。
	for i := 0; i < c.Size(); i++ {
		if err := c.Restart(ctx, i); err != nil {
			t.Fatalf("restart %d: %v", i, err)
		}
		if _, err := c.WaitForLeader(3 * time.Second); err != nil {
			t.Fatalf("no leader after restart %d: %v", i, err)
		}
	}
	// 任期不得回退：任一新主 term >= 初次 term（实际更高）。
	l := c.LeaderID()
	if c.Node(l).Term() < r1.Term {
		t.Fatalf("term regressed across rolling restart: %d < %d", c.Node(l).Term(), r1.Term)
	}
	if err := svc.Release(ctx, "s2", "roll/res", "o1", r1.Token); err != nil {
		t.Fatalf("release after rolling restart: %v", err)
	}
	r2, err := svc.Acquire(ctx, "s3", "roll/res", "o2", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if r2.Token != r1.Token+1 {
		t.Fatalf("token after rolling restart = %d, want %d", r2.Token, r1.Token+1)
	}
	t.Logf("rolling restart survived: token %d -> %d, leader term=%d",
		r1.Token, r2.Token, c.Node(l).Term())
}
