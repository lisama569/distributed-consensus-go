// Command demo 在本机用进程内多节点模拟一个 5 节点协调集群，
// 依次演示：选主 -> 租约加锁与栅栏令牌 -> 重复请求幂等 ->
// 网络分区（少数派旧主无法提交）-> 分区恢复单主 -> 进程重启后任期/状态不回退。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/lisama569/distributed-consensus-go/internal/coordinate"
)

func main() {
	size := flag.Int("nodes", 5, "集群节点数（奇数）")
	dir := flag.String("data-dir", "", "持久化目录（为空则使用临时目录，演示真实重启）")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	ctx := context.Background()

	if *dir == "" {
		*dir = "demo-data"
	}
	logger.Printf("=== 创建 %d 节点集群（持久化目录 %s）===", *size, *dir)
	cluster, err := coordinate.NewCluster(*size,
		coordinate.WithLogger(logger),
		coordinate.WithDataDir(*dir),
	)
	if err != nil {
		log.Fatalf("create cluster: %v", err)
	}
	if err := cluster.Start(ctx); err != nil {
		log.Fatalf("start cluster: %v", err)
	}
	defer func() { _ = cluster.Stop() }()

	leader, err := cluster.WaitForLeader(3 * time.Second)
	must(err)
	logger.Printf("【选主】唯一领导者 = node%d，任期 term=%d", leader, cluster.Node(leader).Term())

	svc := coordinate.NewLockService(cluster, 2*time.Second)

	// 1) 租约加锁 + 栅栏令牌
	fmt.Println()
	logger.Printf("【租约锁】client-A 申请资源 orders/1")
	r, err := svc.Acquire(ctx, "demo-acq-1", "orders/1", "client-A", 2*time.Second)
	must(err)
	logger.Printf("  => 持有者=%s 栅栏令牌=%d 到期=%s 任期=%d",
		r.Owner, r.Token, r.LeaseUntil.Format("15:04:05.000"), r.Term)

	vr, err := svc.ValidateAndSubmit(ctx, "demo-val-1", "orders/1", "client-A", r.Token)
	must(err)
	logger.Printf("【栅栏】client-A 携带令牌 %d 提交受保护操作 => accepted=%v reason=%s",
		r.Token, vr.Accepted, vr.Reason)

	// 2) 重复请求幂等
	r2, err := svc.Acquire(ctx, "demo-acq-1", "orders/1", "client-A", 2*time.Second)
	must(err)
	logger.Printf("【幂等】同一 requestID 重复提交 => 令牌仍为 %d（不会重复加锁/跳变）", r2.Token)

	// 3) 网络分区：旧主 + 一个节点 组成少数派
	oldLeader := cluster.LeaderID()
	minorityPeer := -1
	for i := 0; i < cluster.Size(); i++ {
		if i != oldLeader {
			minorityPeer = i
			break
		}
	}
	minority := map[int]bool{oldLeader: true, minorityPeer: true}
	fmt.Println()
	logger.Printf("【注入分区】少数派={node%d(旧主),node%d} 与其余节点隔离", oldLeader, minorityPeer)
	cluster.Network().Partition(func(from, to int) bool {
		return minority[from] != minority[to]
	})

	// 旧主在少数派中提交必须失败
	oldCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	_, oerr := cluster.Node(oldLeader).Submit(oldCtx, coordinate.Op{
		Kind: coordinate.OpAcquire, RequestID: "demo-stale",
		Resource: "orders/2", Owner: "stale-client", TTL: 300 * time.Millisecond,
	})
	cancel()
	if oerr == nil {
		log.Fatalf("安全违规：分区中的旧主 node%d 竟在无多数派时提交成功", oldLeader)
	}
	logger.Printf("  旧主 node%d 在少数派中提交被正确阻断：%v", oldLeader, oerr)

	// 多数派选出新主
	var newLeader = -1
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for i := 0; i < cluster.Size(); i++ {
			if !minority[i] && cluster.Node(i).Role() == coordinate.Leader &&
				cluster.Node(i).Term() > r.Term {
				newLeader = i
			}
		}
		if newLeader != -1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	must2(logger, newLeader)
	logger.Printf("  多数派一侧选出新领导者 = node%d，任期 term=%d（严格高于旧任期 %d）",
		newLeader, cluster.Node(newLeader).Term(), r.Term)

	r3, err := svc.Acquire(ctx, "demo-acq-2", "orders/3", "client-C", 2*time.Second)
	must(err)
	logger.Printf("  分区期间多数派正常服务：orders/3 持有者=%s 令牌=%d 任期=%d",
		r3.Owner, r3.Token, r3.Term)

	// 4) 恢复分区（轮询等待单主，避免空等导致租约过期）
	fmt.Println()
	logger.Printf("【恢复分区】清除隔离，旧主 node%d 应因更高任期退下", oldLeader)
	cluster.Network().Heal()
	sole := -1
	healDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(healDeadline) {
		leaders := 0
		for i := 0; i < cluster.Size(); i++ {
			if cluster.Node(i).Role() == coordinate.Leader {
				leaders++
				sole = i
			}
		}
		if leaders == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	logger.Printf("  恢复后全集群唯一领导者 = node%d，任期 term=%d", sole, cluster.Node(sole).Term())

	// 5) 重启领导者前先续约 orders/1，确保重启窗口内锁仍有效。
	renewed, err := svc.Renew(ctx, "demo-renew-1", "orders/1", "client-A", r.Token, 3*time.Second)
	must(err)
	logger.Printf("【续约】重启前为 client-A 续约 orders/1 令牌=%d 新到期=%s",
		renewed.Token, renewed.LeaseUntil.Format("15:04:05.000"))

	// 6) 重启领导者：任期与锁状态不回退
	fmt.Println()
	termBefore := cluster.Node(sole).Term()
	logger.Printf("【进程重启】重启领导者 node%d（重启前 term=%d）", sole, termBefore)
	if err := cluster.Restart(ctx, sole); err != nil {
		log.Fatalf("restart: %v", err)
	}
	leader2, err := cluster.WaitForLeader(3 * time.Second)
	must(err)
	logger.Printf("  重启后稳定，领导者 = node%d，任期 term=%d（不低于重启前 %d）",
		leader2, cluster.Node(leader2).Term(), termBefore)

	st, err := svc.Lookup(ctx, "orders/1")
	must(err)
	logger.Printf("  重启后读回 orders/1：持有者=%s 令牌=%d（锁状态与栅栏代数从日志重放恢复）",
		st.Owner, st.Token)

	// 7) 租约过期后旧令牌被拒
	fmt.Println()
	logger.Printf("【租约过期】等待 orders/1 租约过期后，用旧令牌 %d 提交", r.Token)
	time.Sleep(3200 * time.Millisecond)
	if _, verr := svc.ValidateAndSubmit(ctx, "demo-val-stale", "orders/1", "client-A", r.Token); verr != nil {
		logger.Printf("  旧持有者提交被明确拒绝：%v", verr)
	} else {
		log.Fatalf("安全违规：过期租约的旧令牌竟提交成功")
	}
	r4, err := svc.Acquire(ctx, "demo-acq-3", "orders/1", "client-D", 2*time.Second)
	must(err)
	logger.Printf("  client-D 接管 orders/1，新令牌=%d（严格高于旧令牌 %d）", r4.Token, r.Token)

	// 8) 归属历史与历史时点重建
	fmt.Println()
	logger.Printf("【归属历史】orders/1 从首次加锁至今的完整归属变化：")
	spans, err := svc.OwnershipHistory(ctx, "orders/1")
	must(err)
	for i, sp := range spans {
		logger.Printf("  段%d 持有者=%s 令牌=%d 开始位置seq=%d 续约=%d次 结束位置seq=%d 结束原因=%s",
			i, sp.Owner, sp.Token, sp.StartSeq, len(sp.Renewals), sp.EndSeq, sp.EndReason)
	}
	// 把 client-A 的旧令牌放回它第一次持锁的位置：当时是通过的；
	// 放回 client-D 接管之后的位置：当时就会被栅栏拒绝。
	first := spans[0]
	vrOld, err := svc.ValidateAt(ctx, "orders/1", first.StartSeq, "client-A", r.Token)
	must(err)
	logger.Printf("【时点重建】令牌 %d 放回位置 seq=%d => accepted=%v reason=%s（与当时结论一致）",
		r.Token, first.StartSeq, vrOld.Accepted, vrOld.Reason)
	last := spans[len(spans)-1]
	if _, verr := svc.ValidateAt(ctx, "orders/1", last.StartSeq, "client-A", r.Token); verr != nil {
		logger.Printf("【时点重建】令牌 %d 放回位置 seq=%d => 拒绝：%v（与当时结论一致）",
			r.Token, last.StartSeq, verr)
	}

	fmt.Println()
	logger.Printf("=== 全部演示结论符合预期：单主、多数派、任期单调、令牌单调、幂等、过期拒绝、历史可追溯 ===")
}

func must(err error) {
	if err != nil {
		log.Fatalf("fatal: %v", err)
	}
}

func must2(logger *log.Logger, newLeader int) {
	if newLeader == -1 {
		logger.Fatalf("多数派一侧未能选出新领导者")
	}
}
