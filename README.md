# 多节点协调服务：选主、租约锁与栅栏令牌（本机进程模拟集群）

一个用 Go 编写、可在**单机直接运行**的分布式协调原型。它用同一进程内的多个节点
（goroutine + 内存网络）模拟一个 Raft 集群，在其上实现：

- **集群选主与任期推进**（Raft 选举 + 日志复制的安全子集）
- **基于租约的分布式锁**
- **严格单调的栅栏令牌（fencing token）**
- **重复请求 / 重放的幂等处理**
- **网络分区、租约到期、节点宕机、进程重启**的故障注入与可解释判定

所有“谁是主、锁归谁、令牌是否有效”的结论都只由**复制到多数派的日志驱动的
确定性状态机**产生，因此结论可预期、可重放、可解释。

---

## 1. 目录结构

```
internal/coordinate/
  node.go          Raft 节点：选举、日志复制、提交（单事件循环，无共享可变状态）
  fsm.go           锁状态机：加锁/续约/释放/栅栏校验，全部由已提交日志驱动
  types.go         核心类型：Op / LockResult / ValidateResult / 拒绝码
  errors.go        可区分的哨兵错误（拒绝原因）
  rpc.go           RPC 消息与 Transport 接口
  cluster.go       SimNetwork（故障注入）+ Cluster（创建/启动/重启节点）
  logstore.go      内存日志存储 + 内存任期存储
  persistence.go   FileStore：currentTerm/votedFor/日志 的 JSON 原子落盘
  lock.go          LockService：客户端 API，负责发现并路由到当前领导者
  *_test.go        选举、锁语义、分区、租约、重启测试
cmd/demo/main.go   端到端可运行演示
```

---

## 2. 协调协议（简化 Raft）

### 2.1 任期与选举

- 时间被划分为单调递增的 **term（任期）**。
- 节点三态：`follower / candidate / leader`。跟随者在 **随机选举超时**
  （默认 150–300ms）内收不到合法领导者心跳，就自增 term 成为 candidate 并拉票。
- candidate 获得 **多数派（majority）** 选票才成为 leader，并立刻追加一条
  **no-op 日志**；在该 no-op 被多数派确认前，新任期不对外提供读结论。
- 每个节点每个 term 最多投一票，且**只投给日志至少和自己一样新**的 candidate
  （`LastLogTerm/LastLogIndex` 比较），保证当选者拥有所有已提交日志。
- `currentTerm` 与 `votedFor` 在响应前**同步持久化**，进程重启不回退。

### 2.2 为什么任意时刻至多一个“有效”领导者

- 任何 RPC 携带**更大 term**，接收方必须立刻 `becomeFollower(更大 term)`。
- 因此一个被网络分区隔离的旧主，即使本地仍自认为 leader，也**无法获得多数派**，
  它的任何提交都会挂起并超时；分区恢复后，它一接触到多数派的更高 term 就立即退位。
- **未获多数派认可绝不产生新领导者**：candidate 必须拿到多数派投票。
- **只有“本任期条目”被多数派复制后才推进 commitIndex**（Raft Figure 8 安全规则），
  避免间接提交旧任期日志而得出与原领导者冲突的结论。

### 2.3 日志复制与线性一致提交

- 客户端操作只由 leader 包装成日志条目，复制到多数派后按序 apply 到状态机。
- 每个待提交操作在事件循环里**先登记 waiter 再追加/广播**，避免快速返回的
  复制响应抢先提交导致请求永久等待。
- follower 端按标准 Raft 合并日志：`prevLogIndex/prevLogTerm` 一致性检查，
  下标相同而 term 不同则截断冲突后缀后重追加。

> 实现采用**单事件循环**：所有 Raft 可变状态只在节点的一个 goroutine 中修改，
> RPC 线程通过 channel 投递消息，对外的 term/role/leader 用原子变量暴露，
> 因此 `go test -race` 全程无竞争。

---

## 3. 租约与栅栏令牌语义

### 3.1 租约锁（lease）

- `Acquire(resource, owner, ttl)` 成功后返回一把带 `LeaseUntil` 的锁。
- 同一 `owner` 在租约**有效期内重入**只延长到期时间，**令牌不变**。
- 其他 `owner` 在租约有效期内加锁被明确拒绝：`ErrHeldByOther`。
- 持有者必须周期性 `Renew(...)`；**超过 TTL 未续约，租约自动失效**。
- 租约过期后：
  - 旧持有者 `Renew` → `ErrLeaseExpired`
  - 旧持有者携带旧令牌提交受保护操作 → `ErrLeaseExpired`
  - 任意 `owner` 可重新抢锁，并获得**新一代令牌（token+1）**

### 3.2 栅栏令牌（fencing token）

- 每个资源维护一个**只增不减**的 `maxToken`。每次“新的一代”（从未加锁，
  或旧租约过期后的新持有者，或显式释放后的重新加锁）发放 `maxToken+1`。
- 受保护操作通过 `ValidateAndSubmit(resource, owner, token)` 提交，判定顺序：
  1. 锁不存在或租约过期 → `ErrLeaseExpired`（reason=`lease_expired`）
  2. 令牌不等于当前代（落后 **或** 超前/伪造）→ `ErrFenceTokenStale`
     （reason=`fence_token_stale`）
  3. 令牌正确但 owner 不符 → `ErrWrongOwner`（reason=`wrong_owner`）
  4. 全部匹配 → `Accepted=true`
- 资源存储端可据此实现经典 fencing：拒绝任何令牌小于已见过最大值的写，
  从而在锁易主/租约过期后杜绝“旧持有者的迟到写”。

### 3.3 幂等与重放

- 每个写操作携带 **`RequestID`** 作为幂等键。状态机保存
  `requestID -> 首次结论（含拒绝码）`。
- 相同 `RequestID` 重放**永远返回首次结论**（成功或拒绝），不会：
  - 重复加锁、重复释放
  - 让令牌跳变
  - 把首次的拒绝“翻案”为成功
- 去重表本身在日志里，节点重启重放日志后自动恢复。

### 3.4 可区分的拒绝原因（不是静默失败）

| 哨兵错误 | 含义 |
| --- | --- |
| `ErrNotLeader` | 该节点不是主 / 无法在本次尝试内获得多数派（客户端会换节点重试） |
| `ErrHeldByOther` | 资源正被其他持有者以有效租约占用 |
| `ErrLeaseExpired` | 租约已过期，原持有者已失去保护 |
| `ErrFenceTokenStale` | 栅栏令牌落后、超前或伪造，不属于当前代 |
| `ErrWrongOwner` | 释放/续约/提交的 owner 与当前持有者不符 |
| `ErrUnreachable` / `ErrTimeout` | 模拟网络中对端宕机 / 被分区 / RPC 超时 |

调用方用 `errors.Is(err, coordinate.ErrFenceTokenStale)` 等即可明确区分。

---

## 4. 故障注入方式

`coordinate.SimNetwork` 是所有节点共享的内存网络，提供：

```go
net := cluster.Network()

// 1) 节点宕机 / 恢复（进程停止，丢弃全部消息）
net.SetDown(nodeID, true)
net.SetDown(nodeID, false)

// 2) 网络分区：blocked 返回 true 的链路双向丢消息
net.Partition(func(from, to int) bool {
    return side[from] != side[to]
})
net.Heal() // 恢复

// 3) 固定消息延迟
net.Set.SetDelay(20 * time.Millisecond)
```

**进程重启**由 `Cluster.Restart(ctx, id)` 模拟：停止节点 → 用同一持久身份重建：

- `WithDataDir(dir)` 时，`currentTerm/votedFor/日志` 从 JSON 文件加载，
  **等价于真实进程重启**，term 不回退、日志不丢、FSM 通过重放日志重建；
- 不指定目录时使用内存存储，“重启”仍保留同一内存身份（便于纯内存测试）。

---

## 5. 本地验证方法

要求 Go 1.25+（本模块在 go 1.25/1.26 工具链验证）。

### 5.1 运行全部测试

```bash
go test ./... -count=1                 # 功能测试
go test ./... -race -count=1           # 含竞态检测
go test ./... -count=10                # 多轮重复，验证稳定性
go test ./internal/coordinate/ -run TestElection -v   # 只看选主
go test ./internal/coordinate/ -run TestLock -v       # 只看锁/租约/栅栏
```

测试覆盖：

| 测试 | 场景与结论 |
| --- | --- |
| `TestElection_SingleLeader` | 5 节点稳定选出唯一主 |
| `TestElection_TermMonotonicAcrossTerms` | 杀掉主后新主 term 严格升高 |
| `TestElection_MinorityPartitionCannotElect` | 少数派无法选主；恢复后只剩一个主 |
| `TestFSM_*` | 令牌单调、租约过期拒绝、重放幂等、拒绝码不被翻案 |
| `TestLock_DuplicateRequestsIdempotent` | 相同 RequestID 不重复加锁/释放、不跳令牌 |
| `TestLock_LeaseExpiryBlocksOldOwner` | 续约失败 + 旧令牌受保护操作被拒 + 新主令牌更高 |
| `TestLock_PartitionedOldLeaderCannotCommit` | 分区旧主提交被阻断，多数派正常服务，恢复后单主 |
| `TestLock_RestartLeaderKeepsTermAndLockState` | 磁盘重启 term/锁/令牌不回退，释放后令牌继续递增 |

测试日志会打印 **任期（term）、持有者（owner）、栅栏令牌（token）与判定依据
（accepted/reason）**，便于核对。

### 5.2 运行端到端演示

```bash
go run ./cmd/demo -nodes 5 -data-dir demo-data
```

演示会依次打印：选主 → 加锁/令牌/栅栏接受 → 重复请求幂等 → 注入分区
（少数派旧主提交被阻断、多数派选出 term 更高的新主）→ 恢复分区单主 →
续约 → 重启领导者（term/锁状态不回退）→ 等待租约过期 → 旧令牌被明确拒绝 →
新持有者拿到更高令牌。

落盘内容可直接查看：

```bash
cat demo-data/cluster/node-0.json   # currentTerm / votedFor / 已复制日志
```

---

## 6. 安全性质一览（本实现如何保证）

| 性质 | 保证手段 |
| --- | --- |
| 任意时刻至多一个有效领导者 | 多数派投票 + 更大 term 强制退位 + 无多数派提交挂起超时 |
| 未获多数派不产生新主 | candidate 必须多数票，且只投日志更新者 |
| term 单调、重启不回退 | term 变更前同步持久化，启动从磁盘加载 |
| 分区恢复无双主 | 旧主接触更高 term 立即 `becomeFollower` |
| 同租约内资源独占 | 状态机按 owner + LeaseUntil 判定，冲突返回 `ErrHeldByOther` |
| 续约/提交失败后旧持有者无法再提交 | 过期时间 + 栅栏代次双重校验 |
| 栅栏令牌严格单调可比 | 每资源 `maxToken` 只增不减，新代次 +1，重启经日志重放恢复 |
| 重复请求不产生重复副作用 | `RequestID` 去重表，重放返回首次结论 |
| 失效令牌被明确拒绝 | `lease_expired / fence_token_stale / wrong_owner` 三类可区分原因 |

---

## 7. 已知边界（原型范围）

- 租约到期判定依赖**各节点本地墙钟**（本机模拟中时钟一致）；跨机器部署需用
  有界时钟偏移假设或接入逻辑时钟。
- 未实现日志快照 / 日志压缩（演示与测试规模下全量保留）。
- 网络模拟为“同步 in-process 调用 + 可注入丢包/延迟”，未模拟拜占庭行为与
  消息乱序重排；目标是结论可预期、可解释，而不是完整生产级 Raft。
