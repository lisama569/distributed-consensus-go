# distributed-consensus-go

一个完全在**单进程内**运行的多节点协调服务：用本地 goroutine 模拟集群节点、
用同步内存网络模拟 RPC 与网络分区，实现了一套教学/验证用途的
**Raft 选主与日志复制 + 租约分布式锁 + 栅栏令牌（fencing token）**。
所有结论都可在测试日志中按“任期 / 持有者 / 判定依据”逐步核对。

## 快速开始

```bash
go run ./cmd/demo          # 5 节点演示：选主 → 加锁 → 分区 → 租约过期 → 滚动重启
go test ./... -v           # 全部用例（含决策日志）
go test -race ./...        # 竞态检测
```

运行环境：Go 1.25+，无任何外部依赖（标准库即可）。

## 目录结构

```
pkg/cluster/       模拟网络：同步投递、双向分区、节点宕机/重注册
pkg/consensus/     Raft：选主、投票、日志复制、提交、崩溃恢复
  raft.go            节点主循环与 RPC 处理（所有状态转换都在一把互斥锁内）
  file_persister.go  term/votedFor/commitIndex/log 的原子落盘（state.json）
  mem_persister.go   非持久实现
pkg/coordinator/  确定性锁状态机：租约、栅栏令牌、幂等去重、拒绝原因
pkg/sim/           本地集群编排（启停、重启、分区、提交封装）+ 全部场景测试
cmd/demo/         可执行演示
```

## 协调协议

### 选主（Raft 论文第 5.2 节）

- 每个节点三种角色 `follower / candidate / leader`，选举超时随机化
  （默认 150–300ms），避免活锁。
- candidate 自增任期、先给自己投票并持久化 `(term, votedFor)`，再向
  `RequestVote` 多数派拉票；**得到多数票才能成为领导者**。
- 投票限制（防止提交记录被覆盖）：候选人日志必须与投票者“同样新”
  （比较最后一条日志的任期与索引）。
- 领导者周期性发 `AppendEntries` 心跳（默认 50ms）维持权威并推进复制。
- 任意 RPC 见到更高任期立即降级为 follower；分区中的旧主即使仍自称
  领导者，也因拿不到多数 ACK 而**无法提交任何新日志**。
- 任期号是单调时钟：落盘在投票/变更任期之前，**进程重启后不回退**。

### 日志复制与提交

- 客户端请求只在领导者处追加本地日志，随后复制到 follower；某索引
  得到多数派匹配后，领导者推进 `commitIndex` 并按序 apply。
- 遵循 Raft 的安全提交规则：领导者只直接提交**本任期**的条目，更早
  任期的条目通过本任期的提交间接生效（日志一致性检查保证）。
- follower 侧合并日志时若发现冲突，截断后缀再追加（冲突索引快速回退）。
- `commitIndex` 同样持久化；重启时只重放 `<= commitIndex` 的已提交
  前缀——被废黜领导者遗留的**未提交后缀不会被执行**，恢复后由新领导
  者的日志覆盖。这正是 Raft “Leader Completeness”性质。

> 说明：本实现为可解释性牺牲了部分性能（整份 JSON 状态原子重写、
> RPC 同步投递），不追求真实网络吞吐，但安全规则与 Raft 一致。

## 租约与栅栏语义

锁状态机只由**已提交的 Raft 日志条目**驱动；所有副本按相同顺序 apply
相同命令，因此结论一致。时间不是状态机自取的：每条命令带领导者的
`now_ms`，保证重放（含重启重放）确定。

| 操作 | 成功条件 | 失败原因（显式、可区分） |
| --- | --- | --- |
| `acquire` | 资源空闲；或当前持有者在**有效租约**内重复获取（返回原令牌，不发新令牌） | `resource_busy`（他人持有且未过期）、`invalid_lease_duration` |
| `renew` | 持有者正确且租约未过期 | `not_holder`、`lease_expired`（过期不能靠续约“复活”，必须重新 acquire 拿新令牌） |
| `release` | 持有者正确且令牌等于当前令牌（token=0 表示不校验） | `not_holder`、`token_stale` |
| `commit`（受保护操作） | 持有者正确 **且** 令牌等于当前令牌 **且** 租约未过期 | `token_unknown`、`token_stale`、`not_holder`、`lease_expired` |

其他原因：`not_leader`、`unknown_command`、`empty_request_id`。

- **同一资源租约有效期内至多一个持有者**：冲突获取返回
  `resource_busy`，不会覆盖现有锁。
- **栅栏令牌全局严格单调递增**：每次“新授权”发一个新令牌（整数
  +1，跨所有资源共享同一序列），数值可直接比较。资源易主后旧持有者
  再拿旧令牌提交会被判定 `token_stale`——存储层可据此拒绝旧主写入，
  这就是 fencing。
- **租约过期**后旧持有者的 `commit/renew` 一律以 `lease_expired`
  拒绝；资源可被新持有者获取并得到**更大**的令牌。

### 幂等与重放

每条命令必须带非空 `RequestID`。状态机保存 `RequestID -> Response`
表：相同请求重复提交时**原样回放**记录的响应（并标记 `replayed=true`），
因此：

- 重复 acquire 不会重复加锁、不会让令牌跳变；
- 重复 release 不会二次释放；
- 被拒绝的请求重放时返回**同一个拒绝原因**。

由于幂等表本身随日志确定性复制，客户端可以在超时后安全重试。

## 故障注入方式

全部故障都通过 `pkg/sim.Cluster` 注入，不需要任何外部工具：

```go
c.Partition([]string{"node-A", "node-B"}) // 双向隔离：成员 vs 其余所有节点
c.Heal()                                  // 消除分区
c.Stop("node-A")                          // 硬故障：取消循环 + 从网络摘除
c.Restart("node-A")                       // 停后再起，从 state.json 恢复
c.StopAll()                               // 整集群同时断电（配合 Start 做全量重启）
```

模拟网络（`cluster.LocalNetwork`）语义：

- 投递是同步且按“有序节点对”有序的（比真实网络更强，换可复现性）；
- 目标节点未注册（宕机）或处于分区两侧时，`Send` 返回 `nil`，
  上层视为丢包/超时，不做重试以外的特殊处理；
- 分区是双向的：A、B 被隔开时两个方向都不通。

## 本地验证的场景

`pkg/sim` 下的测试（`go test -v ./pkg/sim`）覆盖：

1. **`TestPartitionLeaderUniqueness`（5 节点）**：隔离现任领导者到 2 节点
   少数派；多数派在**更高任期**选出新主；旧主在分区期间的提交被阻塞
   （多数 ACK 不可能）；多数派继续服务且新令牌严格变大；恢复后恰好
   一个领导者，少数派未提交的“流氓命令”永不生效。
2. **`TestLeaseFenceAndExpiry`**：令牌严格单调、同请求回放不跳号、
   `resource_busy` / `not_holder` / `token_stale` / `lease_expired`
   各拒绝原因可区分、过期后易主、旧令牌被栅栏拦截、释放幂等。
3. **`TestRestartTermPersistenceAndReplay`**：follower 硬重启后任期不
   回退、已提交日志本地重放出 holder/token、停机期间错过的条目由领导
   者补传、重启后原令牌的受保护提交仍成功。
4. **`TestRestartLeaderThenReelect`**：领导者崩溃后多数派在更高任期
   重选；旧主重启后以 follower 归队；**整集群同时重启**后仍能重选、
   任期不回退、锁与令牌完整、伪造令牌仍被拒绝。
5. **`coordinator.TestMachineContract`**：状态机全部规则的纯确定性
   单元测试。

日志每一步都打印 `term=... holder=... token=... reason=...` 及判定依据，
例如：

```
majority elected new leader=node-E term=2 (> old term 1)
acquire ... token=3 ok=true served by majority leader
stale leader proposal blocked (no majority acks)
old token after handoff rejected reason=token_stale (fence ordering 1 < 2 enforced)
```

## 安全性质与边界

已实现并可验证：

- 任意时刻至多一个**能提交**的领导者；分区时表面可能短暂存在两个
  “自称领导者”，但只有多数派侧能提交，恢复后只剩一个。
- 已提交条目永不丢失、不被覆盖（多数派 + 日志新旧投票限制 + 提交规则）。
- 任期单调、重启不回退；栅栏令牌严格单调、可比较。
- 拒绝都有机器可读原因；重复请求幂等。

有意简化（教学边界，生产系统需另行处理）：

- 整份状态 JSON 原子重写，非增量 WAL / 快照分段；
- 租约依赖墙上时钟（`now_ms` 由领导者注入），不处理时钟漂移；
- 同步内存网络，不模拟消息乱序/重复投递（RPC 可能丢但不重排）；
- 幂等表与日志无界增长，没有压缩/快照 GC；
- 成员变更固定为启动时配置，不支持动态扩缩容。
