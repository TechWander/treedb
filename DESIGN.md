# treedb 设计文档

**定位**：元数据引擎。单进程、epoll 事件循环（前端 worker 默认按核数给
`min(8, 核数/2)`，`--workers 1` 是单线程基线档；但写永远只有一个提交线程在定序）、
自定义二进制线协议、自带 WAL + 快照恢复。要解的问题形状是「万级并发视图 ×
每视图多条活跃连接」，把这件事从 PG 的 process-per-connection 成本模型里解放出来，
同时不牺牲已 ack 写入的持久性。
它**不绑某个客户端**：类型与语义按 POSIX 元数据定，为客户端服务的适配层留在客户端
自己的仓里；本仓唯一与 PostgreSQL 有关的东西是默认关掉的 `postgres-compat` 脚手架，
而这条约束有测试守着（`tests/independence.rs`，详见 §11 第 7 条）。

**状态**：v0.3.1。引擎本体已落地并跑完五轴验收（§7.8 是 2026-11-19 的全轴复跑）；
已建 git 仓推 `origin`(gitee) + `github` 两远端。0.3.0 之后的一批全在**读轴**上：
量具口径对称化（§9.1 第 3 条）、默认前端 worker 按核数（§8 第 1 条）、目录项两层表
+ kind 冗余（§7.10）；**线协议、WAL 与快照的字节格式一个字节没动**（`SNAP_VERSION`
仍是 2，本轮没升），旧数据目录直接升级。tag：`v0.3.0` 是引擎首版（e2bb51d），`v0.3.1` 是本轮
定版。接入 spike 已在 grovefs 实验分支 `exp/treedb-integration` 上**端到端跑通**（§10
阶段 B 末节：31 条 e2e 断言全过，grovefs 默认档逐字节不变；treedb 在 grovefs 里是
**按 tag 的 git 依赖**，不再依赖同级克隆）。

`v0.3.1` 之后又落了一笔**写轴**改动（下一轮定版收入）：`always` 档的按连接串行
fsync 删掉了，与 `group` 共用一个提交点，机械盘 64 并发 1 891 → 10 913 ops/s、
p99 81.5 → 31.1 ms（§7.9 第 3 条，受控 A/B 在 `benches/results/ab_durability.log`）。

test 现在分两档报：默认档（纯引擎）`cargo test --release` = **74 passed**（28 lib +
20 engine_crud + 9 net_protocol + 7 read_scaling + 6 crash_recovery + 3 independence +
1 scale_connections，10k 那条仍 `#[ignore]`）；`--features postgres-compat` =
**105 passed**，多出的 31 条 = 过渡档自己的 30 条单测 + `concurrency.rs` 里那条
不需要 PG 的拨号用例（剩下 6 条要真 PG，默认 `#[ignore]`，不计入 passed）。即 PG 只出现在
`postgres-compat` 后面的 `src/pool`、`src/store`、
`src/persist`、`src/schema`、`src/probe.rs` 里，以及性能对照器 `src/bin/pgbench.rs`
（同样 `required-features`）—— 默认构建的直接依赖只有 `libc` + `thiserror`。

---

## 1. 目标与验收标准

### 1.1 用户目标（复述）

1. **万级活跃视图下的连接瓶颈** — 现有 `pg_store::PgEngine` 每视图 3–12 连接，
   万级视图 = 3 万–12 万 PG backend 进程，结构上不可行。
2. **数据可靠性与持久化** — 密钥、审计、视图定义不能丢；崩溃后已确认的写必须在。
3. **架构选型** — 扩展现有 PG 后端 vs 引入嵌入式 KV vs 自研引擎，要给出判据和数据。

### 1.2 验收结果

| # | 验收项 | 判据 | 实测结果 | 结论 |
|---|--------|------|----------|------|
| 1 | 连接轴 | 单进程 ≥10 000 并发连接，且 1 fd/conn | 10 000 连接：RSS +5 MiB、**0.53 KiB/conn**、`fd/conn = 1.0000`；25 000 连接：0.39 KiB/conn（2026-11-19 复跑：+6 MiB、0.56 KiB/conn，见 §7.8） | **达成** |
| 2 | 持久性轴 | `kill -9` 后已 ack 的写零丢失 | 6 个崩溃用例全绿（`always`/`group` 两档、6 线程混打中途崩、checkpoint 瞬间崩、连崩 3 轮） | **达成** |
| 3 | 吞吐轴 | op QPS/p99 与 PG 同语义对照 | **默认档（本机 `workers=8`）逐格领先**：64 并发纯读 `readdir` 427 092 vs PG 146 203（**2.9×**）、`lookup` 847 297 vs 210 868（**4.0×**）、`ping` 977 771 vs 531 437（**1.8×**）、`create` 10 386 vs 1 837（**5.7×**）、`write` 12 753 vs 1 415（**9.0×**）；p50 62–112 µs vs PG 117–431 µs（写档 PG 33.6 ms）；零旗标默认档独立复跑 5 次的中位更高（`readdir` 4.5×、`lookup` 5.0×、`ping` 1.9×），所以上面这组倍率是下界。本行旧版那句“默认 `workers=1` 下被 PG 反胜 3–8×”**作废**：它是量具分母不对称与显式单前端两件事叠出来的（§9.1 第 3 条、§7.5） | **达成**（默认即领先，代价见 §8 第 1 条） |
| 4 | 设计文档 | 选型判据 + 协议 + 语义边界 + 数据成文 | 本文档 | 达成 |

对照用的 PG 是**私有集群**（`.pgref`，端口 54390，PG 12.22，`fsync=on`、
`synchronous_commit=on`，WAL 与 treedb 落同一块 `/data` 机械盘）。不动 5432/5433 上的共享实例。

---

## 2. 选型定案：路线 A（自研引擎 + 自定义二进制协议）

### 2.1 连接层的四条路线

| 方案 | 每视图连接数 | 判定 |
|------|--------------|------|
| PgBouncer 事务池化 | N/A | 拒：session-scoped prepared 缓存在事务模式下不可用，且只提复用率不提真并发 |
| 弹性池 + TTL 收缩 | warm（2–8） | 已落地在 `grovefs/pg_store.rs`，连接数仍随视图线性，非终态 |
| 实例级共享池（`search_path` 化） | 0（固定/实例） | 过渡档：`src/pool/shared.rs`。池与视图数解耦，但仍受 PG backend 上限（`max_connections`）与每 backend ~14 MiB RSS 约束 |
| **自研单进程事件循环引擎** | **1 fd/conn** | **采纳（本文档）**。连接轴是结构性优势：10 000 连接 = 10 000 fd + 5 MiB，同规模 PG 需要 ≈134 GiB RSS |

**为什么连接轴真的能拉开**：PG 的成本模型是 process-per-connection，每 backend 自带
一份 portalmem/dynahash/输出缓冲，实测 14 MiB/backend；万级活跃连接在这里就是
140 GiB 量级，而且 `max_connections` 默认 100、调到 4050 需要改配置 + 承担每连接
一次 fork。treedb 侧每条连接只有 `Conn{fd, rxbuf, txbuf}`，内核侧一个 fd + 收发缓冲。

### 2.2 持久化的三条路线

| 方案 | 结论 |
|------|------|
| PG 原生流复制 + PITR | 视图树继续用它（过渡档）；连接轴过不去，不能作为终态 |
| RocksDB / FoundationDB / SQLite | 拒。状态是「小对象 + 高频点查 + 目录遍历」，通用 KV 的 MVCC/compaction 只会带来写放大与 tail latency；关系型/树形语义仍要在其上重建一层 |
| **自写 WAL + 全内存状态 + 快照** | **采纳**。状态量算得过账：万级视图 × 千级 node 也只是千万级小对象，128 GiB/755 GiB 内存放得下，崩溃恢复 = 快照 + 尾日志重放 |

### 2.3 关键决策与理由

**A. 单线程事件循环，而不是 thread-per-connection 或 tokio**
- GroveFS 全栈是 `std::thread` + `parking_lot`，客户端 `remote-only` 构建刻意排除 tokio
  运行时；引 tokio 会污染依赖图。
- 元数据 op 的服务端 CPU 成本是百纳秒级（§7.2：单线程 259 万 op/s ⇒ 平摊每条
  不到 0.4 µs），串行化几乎免费：一次 epoll 轮转处理完所有可读 fd，天然获得
  「全局顺序 = ack 顺序」，不需要锁，也不需要 MVCC 快照。
- 代价在 §7.5 / §8 明说：单线程时纯读高并发会被 PG 的多进程摊核打败。但要把话说
  准：**那面墙不是 CPU，是「每连接一次 `read` + 一次 `write` 系统调用」被串行化**。
  64 连接 `lookup` pipeline=32 单线程能跑 47.8 万 op/s，pipeline=1 只剩 6.0 万 ——
  同一份计算量，差的只是 syscall 怎么排。

**A′. 读轴靠「N 条前端 worker + 唯一提交线程」补，不靠计算池**（§2.4 第 3 条）
- 切法：每条 worker 自带一张 `SO_REUSEPORT` 监听 + 自己的 epoll + 属于自己的连接；
  **纯读批次就地算完直接回**（只拿 `RwLock` 读锁），**含写批次整批**交给唯一那条
  提交线程，由它 translate → apply → WAL → 一次 fsync → 原路送回成品帧。
  于是读并行度和 syscall 并行度是同一个 N，不需要再单独搞一层池子。
- 「全局定序」这条不变量一点没被稀释：WAL、`stats`、`apply` 仍然只有提交线程碰得到。
- 唯一新增的共享可变状态是三个原子计数（在册连接数、就地读条数、转交条数）和
  `State` 那把本来就要给 checkpoint 用的 `RwLock`。
- `workers = 1`（默认）根本不走这条路径，事件循环代码与改造前逐字一致 —— 历史上
  所有基线数字仍然是同一段代码跑出来的。

**B. group commit 的提交点是「一轮 epoll 结束」，不是定时器**
- 所有落在同一轮里的写共用一次 fsync。客户端一问一答（pipeline=1）时，等待方越多、
  批次越大，摊薄越狠：实测 create 1 连接 1 702 ops/s → 8 连接 4 378 → 64 连接 9 377，
  纯靠批次变大。
- `--sync-ms`（默认 20 ms）只用于空闲唤醒，不作为提交的延迟上界之外的东西。

**C. ack 严格晚于 fsync**
- 两段式：`apply 到内存 → WAL append → fsync → 写回包`。任何「先回包后台补 fsync」
  的做法在 §6 的崩溃测试里会立刻红。`Durability::Never` 是唯一例外，且必须显式选择。

**D. 手写 codec，不用 serde/bincode**
- 字节布局同时是磁盘格式和线协议，是对外契约；手写让「版本位 + 字段顺序」显式可控，
  也省掉 derive 带来的泛型膨胀。整数一律小端，`Rd` 是零拷贝游标、越界返回 `Err` 不 panic。

**E. 错误码直接用 POSIX `errno`**
- 引擎的语义就是文件系统语义，FUSE 侧要的就是 `errno`。中间再放一层枚举只会让
  「服务端错误」和「客户端该看到的错误」对不上。`err_code()` 系列断言直接锁住这条。

### 2.4 被实测推翻的三条先前结论（记录下来防回退）

1. **「WAL 必须用 `O_APPEND` 打开」— 错了。**
   预分配（§6.3）之后文件长度比逻辑末尾长，`O_APPEND` 会把记录写到空洞**末尾**，
   中间留下一大段零；重启后扫描在那段零上停下，后面的已 ack 写全丢。现在改成
   显式 `seek(SeekFrom::Start(end))` + 自记位置，`Wal::open` 的注释写明了为什么反过来。
2. **「每轮结尾无脑 `sync_now()` 无害」— 错了。**
   `sync_now()` 在 `Never` 档内部就是 fsync，于是 `Never` 与 `Group` 实测完全等价
   （两档都是 ~820 ops/s），「无持久化上限」这根轴整个是废的，测出来的引擎上限其实
   是磁盘上限。现在周期 sync 必须满足 `cfg.sync_ms > 0 && last_sync.elapsed() >= sync_ms`。
3. **「把 `translate` 搬进只读工作池就能补上读轴」— 错了。**
   真做过一版（原 `src/net/pool.rs`，参数 `--readers N`）：`readdir` 这种大回包确实
   快 3.8×，但 `lookup`/`ping` 反而慢 3–7× —— 省下的那点计算抵不过多出来的一次
   线程间递交，而 `read`/`write` 系统调用仍然全压在主线程上。工作池已删除，读轴
   改由前端 worker 补（§2.3 A′）。

---

## 3. 架构与模块结构

```
                       treedb-bench / treedb-pgbench（对照器）
                                  │
  FUSE/GroveFS 客户端 ── TCP ──►  workers=1：src/net/server.rs  单线程 epoll 事件循环
                                  │                            1 fd/conn，per-conn 收发缓冲
                                  ├─ workers>1：src/net/multi.rs  N 条前端 worker 各自收发包，
                                  │            src/net/mail.rs     纯读批次就地算完直接回；
                                  │           （eventfd 信箱）     含写批次整批 ──► 唯一提交线程
                                  │                                 （只有它持 Engine/WAL/stats）
                                  │  Op/Reply（src/engine/model.rs，37 op / 20 reply）
                                  ▼
                                  src/engine/core.rs   Engine：apply → WAL → 持久化 → 回包
                                  │  src/engine/apply.rs  语义实现（rename/锁/账本/分页）
                                  │  src/engine/wal.rs    WAL：帧/CRC/预分配/group commit
                                  │  src/engine/mem.rs    State：HashMap 状态机 + Log
                                  ▼
                            /data/…/treedb.wal + state.snap

  过渡档（PG 后端，不参与引擎路径）：
     src/pool/shared.rs   实例级共享连接池（search_path 化）
     src/store/{keys,audit,defs}.rs   密钥/审计/定义，PG 主 + SQLite 镜像兜底
     src/persist/mirror.rs            本地 SQLite 镜像
     src/schema.rs                    视图名 ↔ PG schema 编解码（与 grovefs 字节一致）
```

| 文件 | 行 | 职责 |
|------|----|------|
| `src/engine/wal.rs` | 656 | WAL 帧格式、CRC32、预分配、group commit、扫描/重放、`reset` |
| `src/engine/core.rs` | 469 | `Engine`：op 入口、durability 分档、checkpoint、`Stats`、只读入口 `exec_read` |
| `src/engine/mem.rs` | 912 | `State`（views/nodes/dentries/locks/refs/keys/audit/defs/place）+ `Log` |
| `src/engine/apply.rs` | 854 | 37 个 op 的具体语义：环检测、nlink、租约过滤、分页游标 |
| `src/engine/model.rs` | 1488 | `Op`/`Reply` 定义与 codec 实现 |
| `src/net/server.rs` | 672 | `workers=1` 的 epoll 循环、连接生命周期、写回背压、周期任务；兼作两条路径共用的 `Conn`/`Slot` 构件 |
| `src/net/multi.rs` | 819 | `workers>1`：N 条前端 worker + 唯一提交线程（路由闸门、按连接号回程、停机收尾） |
| `src/net/mail.rs` | 114 | 两者之间那条能被 `epoll` 唤醒的信箱（`Mutex<VecDeque>` + eventfd） |
| `src/net/client.rs` | 115 | 同步客户端 + pipeline |
| `src/bin/{server,bench,pgbench,demo}.rs` | 1151 | 可执行入口；`pgbench` 是 PG 对照器 |
| `benches/throughput_matrix.sh` | 188 | 五轴吞吐矩阵（§7 的数据来源） |

引擎本体（`src/engine` + `src/net`）的依赖只有 `libc` + `thiserror`。

---

## 4. 线协议

### 4.1 帧

```
请求/响应同构：  len: u32(小端) | payload: len 字节
payload = tag: u16 | 字段…            （Op / Reply 各自的 tag 空间，Reply 从 0x0100 起）
```

- `MAX_FRAME = 8 MiB`（`src/engine/mod.rs`）。超限帧**只杀违规连接**，不影响其他连接
  （`oversized_frame_kills_only_the_offender`）。
- 半帧等待：`partial_frame_waits_for_more`，碎帧跨多次 `read` 也要拼完整
  （`fragmented_request_is_reassembled`）。
- 错误：`Reply::Err { code: u16, msg }`，`code` 就是 POSIX `errno`。

### 4.2 op 分组（37 个）

| 组 | op |
|----|----|
| 运维 | `Ping` `Stats` `Checkpoint` `Shutdown` |
| 视图 | `CreateView` `DropView` `ListViews` |
| 名字/属性 | `Lookup` `Getattr` `Setattr` `ReadDir` |
| 增删 | `Create` `Mkdir` `Symlink` `Readlink` `Unlink` `Rmdir` `Rename` |
| 数据账本 | `RefLookup` `RefUp` `RefDown` |
| 锁 | `LockQuery` `LockSet` |
| 密钥 | `KeyCreate` `KeyGet` `KeyList` `KeyUpdate` `KeyCheck` |
| 审计 | `AuditAppend` `AuditList` |
| 定义/放置 | `DefPut` `DefGet` `DefList` `DefDel` `PlaceSet` `PlaceGet` `PlaceList` |

**只读集（16 个）= worker 可就地执行集**：`Ping` `ListViews` `Lookup` `Getattr`
`Readlink` `ReadDir` `RefLookup` `LockQuery` `KeyGet` `KeyList` `KeyCheck`
`AuditList` `DefGet` `DefList` `PlaceGet` `PlaceList`；剩下的 21 个全部要过 WAL。

- 这份划分不只是优化提示，**它是语义地基**：`Op::is_readonly()` 判错一条，就是把
  一次应当被确认的写丢在 worker 手里。`readonly_classification_matches_translate`
  拿 37 个 op 的语料逐条跑 `translate`，断言「判为只读 ⇒ logs 必空」，同时钉住
  16/21 这两个数 —— 加 op、改分派表，这里和测试必须一起动。
- `Stats` 刻意**不在**只读集里：它不写 WAL，但它读的是 `Engine` 手里的计数器，而那些
  计数器只有提交线程碰得到。worker 就地算的 op 数走一个 `AtomicU64` 折回
  `ops_total`（`Engine::attach_external_ops`），否则运维看到的 QPS 会少一大截
  （`stats_counts_worker_reads`）。

### 4.3 pipeline 语义

一条连接上连发 N 个请求、再一次性收 N 个回包，**保序**（`pipeline_keeps_per_connection_order`）。
这是吞吐轴上的主要杠杆：它把「一次往返」和「一轮 fsync」都摊到 N 个 op 上。
p50/p99 按「批次总耗时 / N」计，也就是每 op 的摊销延迟——报「批次延迟」会让
pipeline=32 看起来像 1 ms 级，那是骗人。

`workers > 1` 时这条保序多一层规矩，它就是前端的路由闸门：**一个批次里只要
有一条写，整批交给提交线程**；这条连接上只要有已转发、未回话的 op，后续批次
也一律转发。否则就地算出来的读会插到前面那条写的答复之前，客户端看到的
就是「我前一次写还没确认，后一次读已经看不到它」。应答按同连接 FIFO 回填第 k 个
`Slot::Await`，**身份用从不复用的连接号而不是 fd**：fd 一关就被内核重新分配给另一
条连接，按 fd 回填等于把上一任的答复塞进新任的队头。
覆盖用例：`read_write_stream_keeps_per_connection_order`、
`concurrent_reads_do_not_obey_stale_or_torn_writes`、`conn_id_reuse_does_not_misroute_acks`。

---

## 5. 数据模型与 POSIX 语义边界

### 5.1 状态

`State` 是 9 张内存表：`views` / `nodes`（ino 为中心）/ `dentries`（parent+name → ino）/
`locks` / `refs` / `keys` / `audit` / `defs` / `place`。
`data_ref` 是一串**不透明字节**（`DataRef = Vec<u8>`，空串 = 没有内容），引擎不存数据块
也不解释它；`refs` 表只记「持有者数 + 字节总量」，不变量是 `bytes == Σ 持有者 size`，
GC 拿它当唯一依据。
（这里曾经是 `u64` 对象 id。接 grovefs 前改掉了：grovefs 侧的键是 `1:4096:dev0`
这种字符串（PG 侧列类型 `TEXT`），`u64` 要接它只有两条路 —— 哈希（碰撞 = 两个文件的
字节互相顶掉）或在 provider 里养一张 `String ↔ u64` 映射表（表本身得持久化，崩溃后
查不到映射就读不到数据）。字节作键零碰撞、零映射表，且与 PG 后端同形。磁盘格式跟
着变了，所以 `SNAP_VERSION` 1 → 2，旧快照会被版本号闸门拒收而不是静默解错。）

### 5.2 已经锁进测试的语义

| 语义 | 测试 |
|------|------|
| `rename` 原子替换同名目标；拒环（目录挂到自家子孙下 → `EINVAL`）与类型错配；自身改自身按 POSIX 是 no-op 成功而不是 `EEXIST`；`ENOTDIR` 优先于 `ENOENT`；`RENAME_NOREPLACE` 位目标存在则 `EEXIST` 且不动现场 | `rename_moves_and_overwrites`、`rename_rejects_cycles_and_type_mismatches`、`unlink_and_rename_report_the_last_holder_key` |
| `unlink`/`rmdir`/`rename` 的应答直接携带 GC 线索：内容对象**刚好**失去最后一个持有者时键原样带回，否则空串。判定必须与删 dentry 在同一个 op 里 —— 客户端自己 `Unlink` 后再 `RefLookup` 之间有窗口，别人重新引用同键就会让 GC 删掉还在用的字节 | `unlink_and_rename_report_the_last_holder_key` |
| `mkdir`/`rmdir` 与 `nlink` 规则（非空目录不能 rmdir；`..` 计入 nlink） | `mkdir_rmdir_and_nlink_rules` |
| 视图根 inode 不可被 lookup/rename/unlink 触达 | `view_root_cannot_be_reached_or_removed` |
| 视图名 ≤ 25 字节校验；视图之间名字空间完全隔离 | `view_name_validation_and_isolated_namespaces` |
| `readdir` 按名字字节序，游标分页稳定（不重不漏） | `readdir_is_byte_ordered_and_pages_stably` |
| `symlink`/`readlink` 与 POSIX 覆盖语义 | `symlink_readlink_and_posix_override` |
| `refs` 账本的字节不变量 `bytes == Σ 持有者 size`；未登记对象查出来是零行而不是 ENOENT（GC 能无脑查）；只改 size 不动 ref 时账本跟着补差 | `data_ref_ledger_keeps_exact_byte_invariant`、`commit_ref` |
| 锁按 owner 生效，租约过期条目被过滤掉 | `locks_are_owner_scoped_and_lease_filtered` |
| 密钥 create/auth/revoke/rotate/expire 全生命周期 | `key_lifecycle_auth_revoke_rotate_expire` |
| 缺失 inode 上 `setattr` → errno；目录 size 规则 | `setattr_on_missing_inode_and_dir_size` |
| 视图 id 单调，drop + checkpoint 后不回绕复用 | `dropped_view_id_is_never_reused_across_reopen` |

### 5.3 明确**不**提供的东西

- **数据面**：没有 `open`/`read`/`write`/`close`，文件内容永远在对象存储侧。
  引擎只回答「这个名字对应哪个 inode、inode 的属性和 data_ref 是什么」。
- **fd 表与 open-file 状态**：因此没有 `O_TRUNC`、没有 POSIX 记录锁的
  `(pid, fd)` 所有权语义 —— 锁的 key 是调用方给的 owner 字符串 + 租约到期时间。
- **跨视图原子操作**：一个 op 只落在一个视图的命名空间里，`Rename` 不跨视图。
- **用户可见多语句事务**：串行执行给的是隐式全序；要给 FUSE 的「一次 rename 覆盖
  两个 dentry + 一个 inode」这种原子性，是靠单 op 内部一次 WAL 提交（多条 `Log`
  打包成一个批次）实现的，不是靠 BEGIN/COMMIT。
- **硬链接**：没有 `Link` op，一个 inode 只有一条目录项路径，`nlink` 只在目录意义上
  活动（2 + 子目录数）。grovefs `MetaProvider::link_node` 接过来只能回 `EPERM`。
- **字节区间锁**：锁的 key 是 `(view, ino, owner)`，没有 `(start, end, pid)`。POSIX 记录锁
  要的是按区间合并/拆分的那套语义，这里给不了 —— provider 只能拒，不能假装支持。
- xattr、ACL、quota、mtime 之外的时间戳精度增强。

---

## 6. 持久化与恢复

### 6.1 WAL 帧

```
seq: u64 | crc: u32 | len: u32 | reserved: u32   (HDR = 20) | payload(len)
```

- `crc32` 是自带查表的 IEEE CRC32，只覆盖 payload（`crc32_matches_known_vector`）。
- 一次提交可以打包多条 `Log`（一个 op 改多处 = 一个原子批次）。

### 6.2 恢复规则（`scan` / `replay`）

| 现象 | 处理 | 理由 |
|------|------|------|
| 头读不满 / payload 读不满 | 停在这里，`truncate` 到 good end | 掉电写一半 |
| 头齐但 CRC 不对 | 同上，丢弃 | 坏尾 |
| `len == 0` | **停在这里，不算记录** | 这是预分配空洞，不是记录；当成记录会撞上下一条的跳号硬错 |
| 完好记录之间 seq 跳号 | **硬错误，拒绝启动** | CRC 都对却跳号 = 已 ack 的记录真没落盘，静默重放会把丢数据说成没丢（`seq_gap_is_a_hard_error_not_a_tail`） |
| `seq <= snap_seq` | 跳过 | 快照已覆盖 |

checkpoint 之后 WAL 被 `reset` 成空文件，`fast_forward(snap_seq)` 把发号水位接回快照
位置——否则新记录从 1 重新发号，下次恢复会被当成「快照已覆盖」静默丢掉
（`reset_keeps_sequence_numbering`、`checkpoint_then_reopen_survives_wal_truncation`）。
`reset` 走 `set_len(0)` + 重新预分配 + `sync_all` + rename + 目录 fsync，把上一代记录
**物理抹掉**，不然预分配空洞里的旧字节会被后续扫描误读。

### 6.3 三档 durability 与预分配（本轮最大的性能结论）

| 档 | 语义 | 什么时候 fsync |
|----|------|----------------|
| `always` | 同一个不变量：ack 即 durable | 每 epoll 轮一次，覆盖本轮全部已 stage 的写 —— **与 `group` 同一个提交点**；单连接下自然退化为「每个写 op 一次」 |
| `group`（默认） | group commit | 每 epoll 轮一次，跨连接合并 |
| `never` | 只 write 不 fsync | 仅按 `sync_ms` 周期刷，崩溃会丢最近窗口 |

`v0.3.1` 之前 `always` 是按连接逐个 fsync，实测证明那只增加排队、不增加已 ack
写的持久性，已删（§7.9 第 3 条）。名字保留是为了让既有启动参数与用例照旧能跑。

**WAL 预分配**：`posix_fallocate` 把文件长度一次性抬到「逻辑末尾 + 4 MiB」，之后倍增
（上限单步 256 MiB），commit 用 `sync_data()`（fdatasync）。原因是这台机器上量出来的：

| 写法（同 1 400 B 一次，`/data` XFS 机械盘） | 单次耗时 |
|---------------------------------------------|----------|
| 文件随写变大 + `fsync` | **6 947 µs** |
| `posix_fallocate` 预分配 + `fdatasync` | **1 076 µs**（6.5×） |
| `ftruncate` 撑长度（sparse）+ `fsync` | 6 464 µs（没用，还要另付分配） |

差在哪：让文件变长的 commit 必须**同步**写出 extent 分配 + inode 大小两项元数据，
机械盘上这就是一次额外寻道。预分配把长度定死，commit 只剩「数据 + 一次 fdatasync」。

引擎侧的自证数据（`treedb-server --stats`，create 8 连接 pipeline=1，WAL 在 `/data`）：

| | 修复前（随写增长） | 修复后（预分配 + fdatasync） |
|---|---|---|
| avg fsync | 6 823 µs | **1 211 µs** |
| syncs / 窗口 | 442 | 2 239 |
| 每批摊到的记录 | 6.1 | 6.5 |
| create ops/s | 856 | **4 756** |

`posix_fallocate` 不可用时（少见文件系统/无空间）自动退回按需增长并置
`prealloc_ok = false`，只是慢，语义不变。

### 6.4 崩溃验收（`tests/crash_recovery.rs`，6 条）

`kill -9` 之后逐条 `Lookup` 核对已 ack 的写：

| 用例 | 场景 |
|------|------|
| `acked_writes_survive_sigkill_always` | `always` 档，写一批、崩、全在 |
| `acked_writes_survive_sigkill_group` | `group` 档同上 |
| `kill_mid_flight_keeps_every_acked_write` | 6 线程 × 200 条混打中途 `kill -9` |
| `crash_right_after_checkpoint` | 快照刚落盘、WAL 刚 `reset` 时崩 |
| `control_plane_survives_sigkill` | 密钥/审计/定义/放置/锁这类无本地缓存兜底的数据 |
| `repeated_kill_cycles_do_not_erode_state` | 连崩 3 轮，状态不腐蚀 |

---

## 7. 性能实测（五轴）

复现：`benches/throughput_matrix.sh [每格秒数=3] [轴…]`，原始逐格输出在
`benches/results/matrix.log`。下面只报表格化的结论。注意脚本默认只跑轴 1–4，
§7.5 那张读并行与写档回退表来自轴 5，要显式传 `5`。

### 7.1 环境与被测条件

| 项 | 值 |
|----|----|
| CPU / 内存 | Intel Xeon Gold 6338 @ 2.00 GHz，**128 核** / 755 GiB |
| `/data`（WAL 与 PG 都在这） | `/dev/sdb1` **161 TB XFS，`ROTA=1`（机械盘阵列）** |
| `/`（对照用根盘） | `ubuntu--vg` ext4，875 GB |
| 裸 fsync，`/data` | avg **3 816–4 620 µs**，max 24–53 ms（`/tmp/fsync_probe`，同一文件反复 1.4 KB 写 + fsync） |
| 裸 fsync，`/` | avg 236 µs |
| treedb | `cargo build --release`，`durability=group`，`auto-checkpoint-mb 0` |
| PG | 12.22 私有集群，`fsync=on`、`synchronous_commit=on`，WAL 同样在 `/data` |
| 负载器 | `treedb-bench` / `treedb-pgbench`，每格 3 s，`conns` = 线程数 = 连接/backend 数 |

**为什么必须分轴**：这台机器的 fsync 要 4 ms 级。任何「WAL 在 `/data` + 开 fsync」
的格子，数字都是被盘决定的；把它和「引擎上限」混在一张表里报，就是让磁盘替引擎背锅
（或者反过来替引擎邀功）。

### 7.2 轴 1：`durability=never` —— 事件循环 + 内存状态的上限

| op | conns/pipeline | ops/s | p50 µs | p99 µs |
|----|----------------|-------|--------|--------|
| `ping` | 1/1 | 47 575 | 19 | 39 |
| `ping` | 8/1 | 137 190 | 47 | 101 |
| `ping` | 8/32 | **2 591 852** | 3 | 4 |
| `ping` | 64/32 | 2 447 561 | 24 | 49 |
| `lookup` | 1/1 | 45 670 | 20 | 32 |
| `lookup` | 8/32 | 981 145 | 6 | 7 |
| `stat` | 8/32 | 2 110 219 | 3 | 5 |
| `create` | 1/1 | 18 712 | 28 | 64 |
| `create` | 64/32 | 92 027 | 260 | 2 177 |
| `readdir` | 1/1 | 21 056 | 43 | 65 |

单线程循环在纯内存路径上跑到 **250 万 op/s**（pipeline 摊薄往返），一问一答的
往返上限约 13 万 op/s。这条轴的作用是给出「持久化这件事到底收了多少税」的分母。

### 7.3 轴 2 + 轴 4：写路径 —— treedb 默认档 vs PG（两边都真 fsync、同一块盘）

| op | conns/pipe | treedb ops/s（p50 / p99 µs） | PG ops/s（p50 / p99 µs） | QPS 比 | p50 比 |
|----|-----------|------------------------------|--------------------------|--------|--------|
| `create` | 1/1 | 1 702（390 / 1 905） | 553（582 / 22 778） | **3.1×** | 1.5× |
| `create` | 8/1 | 4 378（951 / 16 607） | 662（1 565 / 66 863） | **6.6×** | 1.6× |
| `create` | 64/1 | 9 377（3 131 / 32 426） | 1 692（33 584 / 80 769） | **5.5×** | **10.7×** |
| `create` | 8/32 | 17 420（452 / 1 206） | 6 480（1 083 / 2 305） | 2.7× | 2.4× |
| `create` | 64/32 | 51 788（1 075 / 2 122） | 37 489（1 651 / 2 693） | 1.4× | 1.5× |
| `write`（改 inode + 记 refs 账，两条语句一事务） | 1/1 | 1 812（375 / 1 539） | 378（766 / 23 483） | **4.8×** | 2.0× |
| `write` | 8/1 | 4 419（851 / 18 333） | 632（13 900 / 48 376） | **7.0×** | **16.3×** |
| `write` | 64/1 | 9 515（2 945 / 31 421） | 1 395（33 577 / 165 685） | **6.8×** | **11.4×** |
| `lock` | 8/1 | 8 790（506 / 10 781） | 1 364（1 122 / 46 430） | 6.4× | 2.2× |
| `lock` | 64/32 | 125 599（417 / 1 705） | 40 297（173 / 232） | 3.1× | **PG 2.4×**（排在一个回合里） |
| `key` | 8/1 | 6 124（694 / 15 602） | 1 023（1 292 / 37 918） | 6.0× | 1.9× |
| `key` | 64/1 | 13 103（2 432 / 24 395） | 1 729（33 491 / 100 370） | 7.6× | 13.8× |

怎么读这张表：

- **treedb 全程领先，并发越高、p99 越夸张的地方领先越多**。原因是两边的持久化单位
  不同：treedb 一轮一次 fdatasync 且跨连接合并；PG 每个事务一次 `XLogFlush`，
  group commit 只在同一 commit 队列里排上了才合并，队列一长就是 33 ms 级 p50。
- **PG `create` 64/1 的 p50 是 33.6 ms**，treedb 是 3.1 ms。这不是「谁写得快」，
  是「等待 fsync 队列的排布方式」——高并发元数据写正是 GroveFS 的实际形状。
- pipeline=32 时差距收窄到 1.4×：treedb 一次只能收一条连接的一个批次，PG 的
  多 backend 能同时吃满多个核。见 §7.5。
- `lock` 64/32 这一行要说清楚：treedb QPS 3.1×，但单 op p50 反而比 PG 差 2.4×。
  64 条连接同时各丢一个 32 条的批次进来，一个回合就是 2 048 个锁 op，后面的
  连接只能排在同一轮里——这是单线程循环用延迟换吞吐的固有形状。

### 7.4 轴 3：`durability=always` —— 一次 fsync 到底多贵

| WAL 位置 | 裸 fsync | create 8/1 | create 64/1 | write 8/1 |
|----------|---------|-----------|------------|----------|
| `/data`（机械盘） | 3.8–4.6 ms | 1 640 ops/s（p50 3.4 ms，p99 22.9 ms） | 1 315 ops/s（**p50 34 ms，p99 315 ms**） | 1 625 |
| `/`（ext4） | 236 µs | 13 149 ops/s（p50 533 µs） | 13 165 ops/s（p50 4.7 ms） | 14 409 |

同一份代码、同一个 op，**换盘就是 8×**。结论：一次裸 fsync 就是 3.8–4.6 ms，
所以单连接串行写盘的天花板就在 1.6k ops/s 附近，与连接数无关（盘被打满了）；
这类格子只能用来比较引擎，不能用来宣称引擎快慢。本表 `create 64/1` 那一格的
p99 315 ms 是**旧 `always` 实现**（按连接串 fsync）量出来的，该实现已删、现在同一格
与 `group` 同量级（p99 31 ms），见 §7.9 第 3 条。

### 7.5 读路径：先纠正一处不对称的量具，再看一个核的天花板

**账先记在量具上**（本轮改动，详见 §9.1 第 3 条）。`treedb-bench` 过去把 `t0` 取在
`join` 之前，分母因此含「建 64 条连接 + 每线程预热 8 视图 × 500 文件」；`--secs 3`
那一档实测分母被撑到 4.8–6.2 s，readdir/lookup **每一格虚低 1.6–2×**。而
`treedb-pgbench` 的预热在父线程做完，同一处只撑到 3.0–3.05 s —— 两边口径不一样，
旧对照表里 PG 的领先**有一半是量具给的**。现在两个客户端都改成「每线程自记窗口、
汇总取最大窗口当分母」，本节下面所有数字是修完之后同一天、同一台机器、同一块盘
重跑的（`benches/results/matrix.log` 里 `2026-11-19 07:25 UTC` 那一段）。

**同格对照**（`dur=group`、WAL 在 `/data`、conns = threads = 64、8 视图 × 500 文件
预热；格式 ops/s（p50 µs）；`workers=8` 就是本机默认档 —— 128 核 →`min(8, 核/2)`，
见 §8 第 1 条）：

| op | pipe | PG | treedb w=1 | treedb w=8（默认） | treedb w=16 |
|----|------|-----|-----------|---------------------|-------------|
| `ping`〔never〕 | 1 | 531 437（117） | 136 496（450） | **977 771（62）** = 1.8× | 1 734 166（31） |
| `lookup` | 1 | 210 868（304） | 101 098（605） | **847 297（62）** = 4.0× | 1 428 739（35） |
| `lookup` | 32 | 303 607（209） | 780 206（73） | **3 262 879（20）** = 10.7× | 3 301 541（15） |
| `readdir` | 1 | 146 203（431） | 50 435（1 198） | **427 092（112）** = 2.9× | 869 637（69） |
| `readdir` | 32 | 182 029（350） | 117 881（588） | **847 799（41）** = 4.7× | 1 742 453（30） |
| `create` | 1 | 1 837（33 563） | 11 116 | **10 386** = 5.7× | 11 558 |
| `write` | 1 | 1 415（33 685） | 10 854 | **12 753** = 9.0× | 12 762 |

没列进来的那一格要说清楚：`write` 64/32 的 PG 窗口尾部溢出到 4.20 s（它最后一轮
32 条语句跑了 1.2 s），按 4.2 s 摊出来的 6 780 ops/s 对 PG 不利，不当对照数据。

低并发那一档 treedb 本来就全面领先（一次往返就够，纯内存查找）；一到 64 并发，
`workers=1` 就被反超。根因**不是 CPU 频率、不是协议、不是内存布局**，是「每连接
一次 `read` + 一次 `write`」全挤在一条线程上：同一条 `lookup` 64/1 只有 10.1 万，
改成 64/32 立刻 78.0 万，计算量没变，变的只是 syscall 怎么排。PG 每个 backend
一个进程，天然摊在 64 个核上。

**补法**：`--workers N` 起 N 条前端 worker（§2.3 A′）。本机默认已经是
`min(8, 核数/2)`（§8 第 1 条）；`workers=1` 是历史基线档，也是下面那列「w=1」。

**读并行实测**（轴 5 全部重跑，口径同上张表；格式 ops/s（p50 µs））：

| op | pipe | w=1 | w=2 | w=4 | w=8 | w=16 | PG 同格 |
|----|------|------|------|------|------|------|---------|
| `lookup` | 1 | 101 098（605） | 205 750（320） | 412 155（137） | **847 297（62）** | 1 428 739（35） | 210 868（304） |
| `lookup` | 32 | 780 206（73） | 1 361 453（38） | 2 417 118（23） | 3 262 879（20） | **3 301 541（15）** | 303 607（209） |
| `readdir` | 1 | 50 435（1 198） | 121 156（509） | 257 566（233） | **427 092（112）** | 869 637（69） | 146 203（431） |
| `readdir` | 32 | 117 881（588） | 280 281（226） | 384 935（91） | 847 799（41） | **1 742 453（30）** | 182 029（350） |
| `ping`〔never〕 | 1 | 136 496（450） | 261 293（235） | 505 525（114） | **977 771（62）** | 1 734 166（31） | 531 437（117） |
| `ping`〔never〕 | 32 | 2 247 492（27） | 3 552 242（18） | 4 336 814（11） | 6 546 672（9） | 6 665 805（8） | —（libpq 做不到真合并往返） |

怎么读这张表：

- **§11 那条判据「64 并发 `readdir` 不被 PG 甩开」在默认档就达标**：`workers=8`
  是 PG 的 **2.9×**（427 092 vs 146 203），`workers=16` 是 **5.9×**，p50 从 431 µs
  降到 112 µs。`lookup` 在 `workers=4` 过 PG（412 155 vs 210 868），`workers=8` 是
  4.0×；`ping` 1.8×。尾延迟是同一条线上一起反过来的：PG 那三格 64/1 的 p50 是
  117–431 µs，这边默认档是 62–112 µs。
- **扩展接近线性**：1 → 8 前端换来 readdir 8.5×、lookup 8.4×、ping 7.2×；8 → 16
  还有 1.6–2.0×，之后进平台期（`lookup` 64/32 那格 3.26 M → 3.30 M）。
  封顶 8 的判据就是这一段：8 → 16 只换来不到 2×，却要把 §8 第 1 条那三个代价
  再摊一倍。
- **`workers=1` 那一列仍然输给 PG（0.26–0.48×：`ping` 0.26、`readdir` 0.34、`lookup` 0.48），而它才是真实的单核成绩**
  （独立复跑对得上：0.25–0.49×，见下一小节）。
  把这一列往上推的是计算侧布局优化（§7.10）：它的收益**只能在受控 A/B 里引**，
  拿两次不同日期的矩阵格子相减会得到完全错误的结论（本轮就这么错了一次，
  §7.10 末条）。
- **group commit 没被并行化弄丢**：提交线程每一轮把**所有**上行信箱一次排干再做
  一次 `commit_batch`，只排干被唤醒那一条等于把摊薄比例砍到 1/N。下面写档那一堆
  格子没变慢，就是这条还在生效。

**零旗标默认档单独钉一次**（`benches/default_check.sh`，输出
`benches/results/default_check.log`）。上面两张表的 treedb 数字都是矩阵显式传
`--workers N` 跑出来的，而用户不传 —— 他抄的是 README 那两行命令。这一档每格
都是全新服务端 + 全新 `/data` 目录，跑 5 次取中位（区间是最小到最大）：

| 臂（服务端 banner） | `readdir` | `lookup` | `ping` |
|---|---|---|---|
| 不写 `--workers`（banner `workers=8`） | **654 983**（490 714–792 903） | **1 062 564**（1 021 964–1 103 529） | **1 016 868**（991 711–1 039 153） |
| `--workers 8`（banner `workers=8`） | 572 638（501 893–720 462） | 1 057 844（874 370–1 115 528） | 1 007 756（985 636–1 013 921） |
| `--workers 1`（banner `workers=1`） | 50 156（49 470–51 436） | 103 507（101 198–104 988） | 132 340（131 201–136 097） |
| PG 64/1 同格 | 146 203 | 210 868 | 531 437 |

三条读出来的东西不一样：

- **默认值真的生效**：不写旗标时 banner 就是 `workers=8`，与显式那臂同格（中位差
  1–14 %，而同一臂自己 5 次的散度就有 1.4–1.6×）。“默认持平/碾压 PG”这句话
  现在是零旗标成立的，不靠调用方传参。
- **矩阵连跑会压制多前端那一列**：`readdir`/`lookup` 的 w=8 格子独立复跑比矩阵高
  25–53 %（而 `w=1` 那一列两边只差 ±3 %，见下条）。矩阵里 PG 与 treedb 的格子是
  交替跑的，残留负载对 8 条 worker 的干扰比对单核大。所以上面两张表里的
  **2.9× / 4.0× / 1.8× 要当成下界读**，同格中位是 **4.5× / 5.0× / 1.9×**。
- **单核那一列是实的**：独立复跑与矩阵逐格咬合（0.99 / 1.02 / 0.97），换算成对 PG
  的比值是 0.25–0.49×。这不但是同一台机器上的两次测量，还是两个独立跑的
  脚本 —— “一个核打不过 64 个 PG backend”不是量具问题。

**这笔账的另一半 —— 写档有没有被拖下水**（同轴、同机器、conns=64、`group`、
WAL 在 `/data`；格式 ops/s（p50 µs））：

| op | pipe | w=1 | w=8 | w=16 | PG 同格 |
|----|------|------|------|------|---------|
| `create` | 1 | 11 116（2 819） | 10 386（2 056） | 11 558（1 990） | 1 837（33 563） |
| `create` | 32 | 52 385（1 094） | 45 053（1 107） | 55 624（1 053） | 38 533（1 573） |
| `write` | 1 | 10 854（2 905） | 12 753（1 816） | 12 762（1 811） | 1 415（33 685） |
| `write` | 32 | 25 658（1 296） | 53 036（1 047） | 51 572（1 066） | —（窗口尾溢出，见上） |

没有系统性回退：`create` 在 ±8 % 内来回，`write` 反而随 workers 涨。道理在 §7.9
的计数器上：64 连接的写在 `group` 档就是 **fsync-bound**，含写批次多出来的两次
eventfd 递交是 µs 级，在这块机械盘上它没出现在任何一格里。但要把话说全：写延迟的
**下界**确实抬高了半个线程间往返，只是这台机器上 fsync 把它淹掉了。

另一个测量口径差异，别被表格骗到：treedb 的 `pipeline=32` 是**真的一次往返发 32 个
请求**；`treedb-pgbench` 的 `pipeline=32` 只能是一个事务里跑 32 条语句（libpq 不做
请求合并），省的是 commit、不省往返。所以读档上 PG 的 pipeline=32 是**被低估**的。

### 7.6 连接轴

| 系统 | 规模 | 成本 |
|------|------|------|
| treedb | 10 000 并发连接（单进程） | RSS **+5 MiB**，**0.53 KiB/conn**，`fd/conn = 1.0000` |
| treedb | 25 000 并发连接 | 0.39 KiB/conn |
| PG | 单 backend 实测 | ≈14 MiB → 10 000 连接 ≈ **134 GiB**，且要 `max_connections≥10000` |

`fd/conn = 1.0000` 是关键断言：没有每连接线程、没有 dup fd、没有 eventfd 配对。
`tests/scale_connections.rs` 里 `thousand_connections_cost_one_fd_and_a_sliver_of_rss`
每次 `cargo test` 都跑；万级那条 `ten_thousand_connections_in_one_process` 是
`#[ignore]` 的重压用例，手动 `--ignored` 触发（连接数由 `TREEDB_CONNS` 控制，
2.5 万那个数字就是 `TREEDB_CONNS=25000` 跑出来的）。临时端口和 `somaxconn=4096`
是这一档的真实上限，不是引擎。

上面两行都是**默认 `workers=1`** 的数字。开多前端 worker 时 fd 成本是
**+3 fd/worker**（1 张 `SO_REUSEPORT` 监听 + 上/下行各 1 个 eventfd），**与连接数无关**：
`workers=16` 就是多 48 个 fd，1 000 连接那格从 1.0000 变成 1.048 fd/conn（两条断言
的阈值是 1.05/1.02，跑的都是默认档，所以不会被前端 worker 撑爆）。

### 7.7 一句话结论

写路径与连接轴上 treedb 是**结构性**领先（fsync 摊薄方式 + 1 fd/conn）；纯读高并发
以前看着结构性落后，拆开来是两件事：**一半是量具不对称**（§9.1 第 3 条），另一半是
**并行度**（`workers=1` 把 64 条连接的 read/exec/write 排在一个核上，而 PG 是 64 个
进程）。默认档现在按核数给 worker（本机 8 个，零旗标实测 banner 就是
`workers=8`，§7.5 末小节），64 并发纯读**默认就领先** PG 1.8–4.0×（同格独立
复跑的中位是 1.9–5.0×），`workers=16` 是 1.8–5.9×；再叠加计算侧的目录布局改造
（§7.10），单核那一列也抬了 1.6–1.7×。GroveFS 的元数据负载以写和长连接为主，这条取舍仍然
划得来；写扩展不在这条轴上 —— 它永远只有一个提交线程（§8 第 1 条③）。

### 7.8 全五轴复跑（2026-11-19）

为什么要有这一节：§7.2–§7.5 的数字是一次性跑出来的，而「独立数据库」的主张要能
被别人复算。本轮在同一台机器上把轴 1、2、3、4、5 全部重跑（160 格，逐格原始输出
追加在 `benches/results/matrix.log`），并把连接轴与并发正确性用例一起重跑。

**环境同一性先确认**（否则复跑没有意义）：`Xeon Gold 6338` / 128 核 / 755.284 GiB /
`/dev/sdb1` 161 TB XFS `ROTA=1` —— 与 §7.1 那台机器逐字相同，PG 仍是 `.pgref` 私有
集群、`fsync=on` + `synchronous_commit=on`（脚本会读回这两项，不是 on 就整轴作废）。

关键格子的历史值 vs 本次值（**注意：这一段里 `lookup`/`readdir` 的 treedb 侧数字
全部是 §9.1 第 3 条那个分母修正之前的，虚低 1.6–2×**；表格保留原样是为了记下当时
确实看到了“PG 反胜 3.6–7.5×”，修正后的同格数字在 §7.5）：

| 格子 | 历史 ops/s | 本次 ops/s | 本次 PG | 判读 |
|------|-----------|-----------|---------|------|
| `create` 64/1 `group` | 9 377 | 7 844 | 1 441 | 比 5.4×；机械盘写回方差，±19 % |
| `create` 64/1 `always`（**旧实现**） | 1 315 | 1 586 | 1 441 | ~~**1.1×**~~ → 合批后 7.4×，见判读 2 与 §7.9 第 3 条 |
| `write` 64/32 `group` | （表里没有） | 52 317 | 5 957 | **8.8×**，本轮新增可报格子 |
| `lookup` 64/1 `group` w=1 | 66 229 | 57 403 | 206 559 | PG 仍反胜 **3.6×** |
| `lookup` 64/1 w=16 | 861 465 | 792 243 | 206 559 | 反超 **3.8×**，p50 37 µs |
| `readdir` 64/1 w=1 | 18 769 | 18 930 | 142 507 | PG 反胜 **7.5×**（历史 7.9×） |
| `readdir` 64/1 w=16 | 316 780 | 288 747 | 142 507 | 反超 **2.0×** |
| `readdir` 64/32 w=16 | 505 400 | 343 512 | 178 484 | **摆动 1.5×**，正是 §7.5 已经预告的高阶格噪声 |
| `ping` 64/1 w=16〔never〕 | 1 715 136 | 1 733 155 | 509 099 | 吻合，**3.4×** |

三条判读：

1. **PG 那一侧极可复现**（`lookup` 208 239 → 206 559，`readdir` 142 815 → 142 507，
   不到 1 %），而 treedb 落在机械盘上的写格与 `pipeline=32` 高 worker 格摆动
   10–50 %。所以对外报数应当报**倍率区间**和 p50，不该报单格小数的第六位。
2. **`always` 那一格把「领先从哪来」变成了实测，不是推断**：每 op 一次 fsync 时
   treedb 1 586 vs PG 1 441 = 1.1×（两边都撞在同一块盘的 fsync 上限），而默认
   `group` 档是 7 844 = **5.4×**。差出来的全部是「跨连接把 N 次 fsync 合成一次」
   这一件事，跟语言、跟零拷贝、跟内存布局无关。换到根盘 ext4 上 `always` 档立刻
   变 12 044–13 557 ops/s（同一格子在机械盘是 1 306–1 586），进一步确认那是盘的
   天花板不是引擎的。
   **但这句里的 1.1× 不成立了**：它量的是旧 `always`（按连接串 fsync）。合批之后
   同一格是 10 913 vs PG 1 469 = **7.4×**（§7.9 第 3 条）。当时「刨掉合批就没有
   结构性优势」的判读只适用于那个实现，不适用于引擎。
3. **写档在开 worker 后没有系统性回退**（`create` 64/1：w=1 9 338 → w=8 12 390；
   `write` 64/32：w=8 那一格 34 099 是明显 outlier，同档 w=1/w=16 是 52 564/50 554）。
   这台机械盘上写是 fsync-bound，线程间递交的 µs 级成本量不出来 —— 与 §7.5 结论一致。

连接轴与并发正确性同轮复跑：

| 证据 | 结果 |
|------|------|
| `thousand_connections_cost_one_fd_and_a_sliver_of_rss`（默认跑） | RSS 0→2 MiB、2.28 KiB/conn、`fd/conn = 1.0000`、ping p50 74 µs / p99 129 µs |
| `ten_thousand_connections_in_one_process`（`--ignored`） | 10 000 条全部建立：RSS 0→6 MiB、**0.56 KiB/conn**、`fd/conn = 1.0000`、p50 80 µs / p99 110 µs |
| `tests/net_protocol.rs` | 9 passed，含 `pipeline_keeps_per_connection_order`、`concurrent_connections_do_not_crosstalk` |
| `tests/read_scaling.rs` | 7 passed，含 `worker_reads_answer_identically_to_inline_reads`（w=1 与 w=8 答复逐条相同）、`concurrent_reads_do_not_obey_stale_or_torn_writes`（w=4 混打看不到撕裂/陈旧）、`acked_writes_survive_crash_with_workers_on`、`conn_id_reuse_does_not_misroute_acks` |
| `cargo test --release`（默认档，本轮复跑） | 71 passed / 0 failed（T1 之前是 99 条一起跑，拆分后语义不变） |

两处必须说白的地方：

- **`tests/concurrency.rs` 不是引擎并发的证据**。它 7 条里有 6 条 `#[ignore]`，
  且测的是 PG 过渡档的 `SharedInstancePool`（池上限、排队、reap、KeyStore 并发）。
  T1 之后整个文件已在 `postgres-compat` 后面，默认档一条也不跑。引擎自身的
  并发证据是上面那两张表里的 `net_protocol`（9）+ `read_scaling`（7）+ lib 单测。
- 会话里曾经口传过一个「单进程 `max_conn = 4382`」的上限数字，**仓内查无出处**
  （`grep -rn 4382 DESIGN.md benches/ tests/` 为空），本轮撤回，不再引用。真实
  约束是 `net.core.somaxconn = 4096` 与临时端口区间，与引擎无关（§7.6 末句）。
  引擎侧只有 `ServerConfig::backlog`（默认 4096）这一个旋钮，没有连接数上限。

### 7.9 合并 fsync 是量出来的，不是推断的（2026-11-19）

§7.3 那句「treedb 写路径的领先全部来自跨连接合并 fsync」以前只有两个档位之间的
吞吐差做支撑。本轮直接读引擎自己的计数器（`--stats` 的 `writes` / `syncs`），算出
「一次 fsync 摊了多少次写」。固定同一格 `op=write conns=64 pipeline=1`，只换盘与
持久化档，复现命令：`benches/sync_ratio.sh 3`（原始输出追加在
`benches/results/sync_ratio.log`，下表是最后四轮）：

| WAL 位置 | 档位 | ops/s（p50 / p99 µs） | writes | syncs | **写/fsync** |
|----------|------|--------------------|--------|-------|---------|
| 机械盘 XFS | `group` | 10 322（2 767 / 28 310） | 32 313 | 549 | **58.9** |
| 机械盘 XFS | `always` | 1 716（30 521 / 65 132） | 6 199 | 6 199 | **1.00** |
| 根盘 ext4 | `group` | 68 758（877 / 1 441） | 211 794 | 3 445 | **61.5** |
| 根盘 ext4 | `always` | 13 600（4 567 / 6 704） | 42 604 | 42 604 | **1.00** |

同一轮独立复跑给的是 61.87 / 1.00（机械盘）与 62.55 / 1.00（根盘），即这个比值本身
很稳。（表中两行 `always` 量的是**改前**实现，第 3 条已把它换掉；`group` 那两行仍有效。）
三条结论：

1. **`group` 档一轮把 ≈ 60 次 fsync 合成了 1 次**（conns=64 几乎全部排上了同一个
   回合），换回 5.1–6.0× 吞吐。这就是 §7.3 那张倍率表的机制本身，现在它在计数器
   上看得见，不用靠吞吐差反推。
2. **（改前）`always` 档在高并发下不领先，尾延迟差于 PG**：同一格 PG（`synchronous_commit=on`、
   WAL 同在机械盘）是 1 469 ops/s（p50 33.5 ms / p99 157 ms），treedb 三次独立量到
   1 306（p99 **556 ms**）、1 722/1 716（p99 84 / 65 ms）—— 吞吐在噪声里持平，尾部
   不可预测。原因在 `commit_batch`：`Always` 分支按组循环 `wal.commit()`，即**单提交
   线程在一回合里串行发 N 次 fsync**，第 k 条连接要等 k × fsync 延迟；而 PG 的
   `XLogFlush` 把后到的等待者挂在一个已在飞的 flush 上，同一个 fsync 能顺手 ack
   一批（对 PG 侧这是其既有语义，本仓未量它内部，仅作机制解释）。
   → 当时记进了 §8 第 11 条与 §11 第 8 条（P1），**现已按第 3 条修完**。
   默认档 `group` 从不受影响。
3. **第 2 条那个串行 fsync 已删**：`commit_batch` 里的 `Always` 分支拿掉，两个档
   走同一条路径（一轮一次 `wal.commit()` 覆盖本轮所有已 stage 的写）。理由不是
   「更快就好」，而是**按连接切 fsync 不给已 ack 的写增加任何持久性**：这一档的
   不变量是「ack 晚于覆盖该 op 的 fsync」，一次覆盖全轮的 fsync 同样满足它。它多
   买的只是「已收到但未 ack 的字节也落盘」，而那在崩溃恢复后表现为**客户端没收到
   成功却可能看到生效**，是要避免的行为，不是一条保证。

   受控 A/B（`benches/ab_durability.sh v0.3.1 3 3`：同一份客户端、只换服务端二进制、
   每格全新 `/data` 目录、old/new 相邻跑、3 次取中位，原始输出在
   `benches/results/ab_durability.log`）：

   | 档位 | 服务端 | ops/s | p50 µs | p99 µs | 写/fsync |
   |------|--------|-------|--------|--------|----------|
   | `always` | v0.3.1 | 1 891 | 29 742 | 81 503 | 1.00 |
   | `always` | 改后 | **10 913**（5.77×） | **1 953**（15.2× 优） | **31 078**（2.62× 优） | 54.8 |
   | `group` | v0.3.1 | 10 580 | 1 875 | 28 876 | 55.6 |
   | `group` | 改后 | 12 232 | 1 882 | 24 485 | 57.7 |

   三点判读：① `always` 现在与 `group` 同一量级（0.89×，已是同一实现，残余差在
   噪声里）；② `group` 没被改坏（p50 一致，吞吐/p99 在噪声内）；③ 本次 old 臂
   复现了上表同格的旧数（`always` 1 716 → 1 891、`group` 10 322 → 10 580，差
   4–10 %），说明这一格的跨日可比性够，因此引 §7.9 表里同格的 PG（`synchronous_commit=on`、
   WAL 同在机械盘）1 469 ops/s（p50 33.5 ms / p99 157 ms）作对照是站得住的：
   **`always` 档现在是 PG 同格的 7.4× 吞吐、p50 优 17×、p99 优 5×** —— 上一轮
   「吞吐持平、尾部差于 PG」的结论作废。

### 7.10 计算侧：readdir 一页为什么要钻 100 次树（两层 dentry + kind 冗余）

读轴的第二个根因不在调度，在状态机布局。原先是

```text
nodes:    BTreeMap<(ViewId, Ino), Node>
dentries: BTreeMap<(ViewId, Ino, Vec<u8>), Ino>
```

`readdir` 一页 100 条要做 100 次 `nodes` 下钻取 `kind`（`node_of()` 还返回
`Option<Node>` 的 `.cloned()`），`lookup` 每次要 `name.clone()`（三元组键不满足
`Borrow<[u8]>`，借位查不了），`view_meta` 又返回 owned `(Vec<u8>, ViewMeta)`。
`treedb-micro`（纯计算档，不经网络不写 WAL）量到一页 9 795 ns / **205 次堆分配**。

改了三处，**线协议、WAL、快照的字节格式一律没动**：

1. **`dentries` 拆两层**：`BTreeMap<(ViewId, Ino), BTreeMap<Vec<u8>, Dent>>`，
   `Dent { ino, kind }`。内层键 `Vec<u8>: Borrow<[u8]>` 成立 → `lookup` 零分配；
   外层按 `(view, parent)` 升序 + 内层按 name 升序 = 原三元组字典序 → `save`/`load`
   写出的字节序列与改造前逐字节相同，**`SNAP_VERSION` 不升**（旧快照重放那条用例
   照绿就是它）。空内层表必须收掉，否则 `dentry_count` 与 `dir_empty` 会骗人。
2. **`kind` 冗余进 dentry**：`PutNode` 恒先于 `PutDentry` 落表，快照里 `nodes` 段
   又先于 `dentries` 段，两处都能就地推出。readdir 一页不再碰 `nodes`。兜底
   `DENT_KIND_UNKNOWN = u8::MAX`（真实 kind 只有 0/1/2），命中兜底才对那条回表。
3. **`view_meta` 改借用**，目录 op 不再白付 2 次克隆。

**收益分两档量**（第二档是本轮差点写错的地方）：

| 口径 | 数字 |
|------|------|
| 纯计算 `treedb-micro`（单目录 32 000 条） | readdir 9 795 → **4 323 ns/页（2.27×）**，allocs 205 → 104；lookup 431 → 372 ns、**3 → 1 allocs**；unlink 935 → 812 ns、6 → 3 allocs（原始输出 `benches/results/micro_{before,mid,after,final}.log`） |
| 端到端受控 A/B（`benches/ab_layout.sh 5a4bf99`：同一客户端只换服务端二进制，`never` 档、conns=64、1 视图；原始输出 `benches/results/ab_layout.log`） | readdir w=1：35 907 → **56 799（1.58×）**；16 000 条目录：28 339 → **48 371（1.71×）**。w=8：380 505 → 458 854（1.21×）/ 366 450 → 492 361（1.34×）。lookup：0.97–1.01×，**在噪声里** |

五条必须一起报的说明：

- **中间那次「优化无效」是错的**。先去掉克隆（只改 `view_meta` 借用 + `kind_of`），
  allocs 从 205 掉到 104，ns/op 反而 9 795 → 10 485 —— 这直接**证伪了「克隆是主要
  成本」**。真正的成本是那 100 次 `nodes` 下钻的 **cache miss**，所以必须做 kind
  冗余，光改借用没用。micro 的 `mid` 档就是这一步，保留它就是为了记住这次证伪。
- **不同日期的矩阵格子不能相减**。我一度用「旧矩阵 32 867（分母 4.78 s）折算 ≈ 52 k」
  去比「新矩阵 50 435」，得出“端到端 readdir 没动”的结论 —— 那是把量具修正、WAL
  所在盘、视图数、目录规模四个变量混在一起算。受控 A/B 才是 1.58–1.71×（w=1），
  它的头注释已写明这条约束。
- **A/B 里 500 条与 16 000 条目录的倍率几乎一样**（1.58 vs 1.71），说明收益主要来自
  「每页省掉 100 次 `nodes` 下钻」这一项 —— 它随**页大小**长，不随目录大小长。
  lookup 只钻一次，所以它基本不变（micro 上 1.16×，端到端 0.97–1.01×）。
- **两次独立跑的可信区间不一样**：另一次（工具化前的临时脚本）量到 1.64 / 1.77 /
  1.45 / 1.44。w=1 那两档复现到 ±4 %，**可以直接引**；w=8 那两档摆动 1.2–1.5×
  （64 条连接分摊到 8 个 worker 本身就是噪声源），只报区间不报小数。
- **剩下的头一个**：一页仍有 **104 次堆分配**，其中 100 次是 `Reply::Dirents.names`
  每个名字一个 `Vec<u8>`。要再往下砍，得让应答直接往连接 `tx` 里编，不要中间造
  `Vec<Vec<u8>>`。这条排在 §11 的通用表能力之后：端到端默认档已经领先 PG 2.9×，
  它不再是短板。

**顺带被证伪的另一个假设**：`never` 档每 `sync_ms` 无条件 `sync_now()`，怀疑它在
机械盘上白付 fsync（矩阵 `/data` 那格比 `/tmp` 探针低就是这么解释过的）。加了
「没有新记录就直接返回」的守卫（`wal.rs::sync`）之后实测 `/data` 50 622 → 49 856，
**没有区别**：对没有脏页的文件 `sync_data` 不下盘。守卫留着（每 20 ms 少一次
syscall，`durable_seq` 记账不变），但那句解释要撤回 —— 两格之差是量具分母，不是盘。

同理，本轮为 `Engine::snapshot_stats()` 里新增的 `dentry_count()`（两层表下是
O(目录数)）另开了 `Engine::wal_bytes()`（`src/engine/core.rs`）：auto-checkpoint 默认
64 MB 档下事件循环**每轮**都要判一次 WAL 尺寸，不能每轮把七张表数一遍。这一条
是自查改动时拦下来的回归，没量到开销，但它属于「读锁 + 全表计数放在热路径」这类
一旦进去就很难发现的问题，所以函数注释里写明了只能用在诊断路径。

---

## 8. 已知限制（诚实边界）

1. **读并行不是免费的，而且默认值本身是个折中**（§7.5）。默认 `workers` 现在按核数
   给：`min(8, 核数/2)`（`net::default_workers()`；`--workers 1` 回到历史基线档，
   `0` = auto）。理由与代价一起看：不给默认并行等于把读吞吐锁在一个核上（本机
   实测单核 50 435 vs 8 前端 427 092），而 8 → 16 只换来 1.6–2.0×、却把下面三条
   代价翻倍，所以封顶 8；除以 2 是给提交线程与同机客户端留核。1–3 核机器算出来
   仍是 1，与旧默认逐字一致。三条代价：① 每条 worker 多 3 个 fd（与连接数无关）；
   ② `SO_REUSEPORT` 语义上允许**另一个进程并列 bind 同一地址端口**，届时流量被两边
   分摊 —— 单实例假设必须靠部署侧保证（`--pid-file` 只能挡住“自己重起自己”，
   挡不住第二个实例也开了这个标志）；③ **写扩展不跟 workers 长**：永远只有一个
   提交线程，N 再大也只是前端收发包并行，写一旦超过那个核的 compute 能力就该先看
   `worst_round_us`（`Report`）而不是继续加 worker。核数自适应只到“起步值”这一层：
   换机器要重测 §7.5 那张表，别把 8 当常数。
2. **状态全内存**：恢复时间 = 快照读入 + 尾日志重放。状态规模上限就是内存上限；
   快照多大、重启多少秒**本轮没实测**（现有测试都在 MB 量级以下），真到
   百 MB / 亿级 inode 前要先量，并另设计分层。
3. **`never` 档会丢数据**：它只是把 fsync 从提交路径挪走，崩溃丢最近 `sync_ms` 窗口。
   选它必须是有外部冗余的场景。
4. **无数据面 / 无 fd 语义**（§5.3）：`O_TRUNC`、POSIX 记录锁的 `(pid,fd)` 所有权这类
   由 FUSE 层自己维护，引擎不参与。
5. **无跨视图原子 op、无用户可见事务**（§5.3）。
6. **单机、无复制**：`refs` 账本和 WAL 都是单主。HA 需要日志传送或让上层继续用 PG
   做权威（过渡档的现状）。
7. **`checkpoint` 是 stop-the-round**：快照在提交线程里同步生成，期间不接新写。
   `workers=1` 时整个循环停住；`workers>1` 时前端仍能就地服务纯读（快照只持**读锁**，
   与 `exec_read` 共享），但已转发的写得等它。大状态下的停顿没有上限保护
   （`auto_checkpoint_bytes` 只是把触发频率压住）。
8. **视图名 25 字节上限**：与 grovefs `pg_store::schema_for` 对齐；突破要同步改
   `VIEW_NAME_MAX`。
9. **预分配的空间代价**：WAL 文件至少 4 MiB 且倍增，`du` 看到的比逻辑长度大。
   `Stats.wal_bytes` 报的是**逻辑长度**（checkpoint 阈值按它算），磁盘长度只会 ≥ 它。
10. **`--bench`/`--pgbench` 是本机同盘对照，不是云端结论**：换 SSD/NVMe、换 PG 大版本、
    换 `wal_compression`/`full_page_writes` 配置，绝对数字都会变，倍数关系才可信。
11. ~~`always` 档在高并发下没有优势，尾延迟还会跑飞~~ **已修**（§7.9 第 3 条）：
    `Always` 分支删了，`always` 与 `group` 现在走同一个「一轮一次 fsync」的提交点，
    64 并发机械盘上 1 891 → 10 913 ops/s、p99 81.5 → 31.1 ms。留下的真实边界有两条：
    ① 不再提供「已收到但未 ack 的写也立即落盘」这一档（合批后就没这个语义了，
    单连接下 `always` 自然退化为每 op 一次 fsync）；② 写扩展仍付「单提交线程 +
    每轮一次阻塞 fsync」的代价（机械盘 ≈ 3.8 ms/轮），要再进一层得把 flush 从提交
    线程挪出去，见 §11 第 8 条。

---

## 9. 测试清单

```sh
cargo test --release                            # 默认档（纯引擎），73 条
cargo test --release --features postgres-compat # 加上过渡档，104 条
cargo test --release --lib                      # 只看引擎单测
cargo test --release --test scale_connections -- --ignored   # 万级连接重压（§7.6）
bash benches/throughput_matrix.sh 3             # 五轴吞吐矩阵（默认只跑 1–4）
bash benches/default_check.sh                   # 零旗标默认档同格重跑 5 次（§7.5）
bash benches/ab_layout.sh <基线 rev>            # 受控 A/B：只换服务端二进制（§7.10）
bash benches/sync_ratio.sh 3                    # 一次 fsync 摊了几次写（§7.9）
bash benches/ab_durability.sh <基线 rev>        # 写轴 A/B：持久化档的吞吐与尾延迟（§7.9 第 3 条）
bash benches/probe_srv.sh                       # 服务端吃了几核（/proc 计数，§7.5）
./target/release/treedb-micro --items 32000     # 纯计算档 ns/op + allocs/op（§7.10）
```

这几条测量脚本都不进回归板（它们要几秒到十几分钟，且数字依赖机器），但它们
输出的每一格都已被本文引用过 —— 引用了就要能复算。

两档都跑完才是全量：`--features postgres-compat` 多出的 31 条 = 过渡档自己的 30 条
单测 + `concurrency.rs` 里那条不需要 PG 的拨号用例。

| 目标 | 数量 | 覆盖 |
|------|------|------|
| `--lib` | **28**（+30 属 `postgres-compat`） | codec round-trip（全 tag）、CRC 已知向量、碎帧/超长帧、WAL 帧与截断重放、**预分配空洞重开**（`prealloc_padding_reopens_cleanly`）、**大批次不跳写**（`huge_batch_writes_every_record`）、**seq 跳号硬错**、状态快照保留 node key、**两层 dentry 与 kind 冗余过快照不坏**（`dentries_are_two_level_and_kind_survives_snapshot`，§7.10）、缓冲压缩、租约过期边界、`default_workers` 上下界 |
| `tests/engine_crud.rs` | **19** | §5.2 全部 POSIX 语义（含 GC 线索与 NOREPLACE）+ 控制面（密钥/审计/定义/放置）+ checkpoint 后重开 + 坏尾截断 |
| `tests/net_protocol.rs` | **9** | TCP 端到端：CRUD、碎帧、pipeline 保序、并发连接不串扰、超长帧只杀违规者、大回包流式读完、`--stats`/`--checkpoint`/`--shutdown`、优雅停服后同地址立刻重绑 |
| `tests/crash_recovery.rs` | **6** | §6.4 |
| `tests/read_scaling.rs` | **7** | 只读分派表逐条对 `translate`（判错一条就是丢写）、`workers=1/8` 答复逐条相同、每连接流内顺序、一写多读不读到半截状态、`ops_total` 记上 worker 就地算的读、并行档下 `kill -9` 已 ack 的写全在、**fd 复用不会把上一任的应答塞给新任** |
| `tests/concurrency.rs` | 0（+7，整文件 feature 门控） | T1 之后整个文件在 `postgres-compat` 后面：无 PG 时跑「拨号失败不漏 live」（1 条，要 feature），其余 6 条 `#[ignore]` 需真实 PG。**它测的是共享池，不是引擎并发**（§7.8）|
| `tests/scale_connections.rs` | 1（+1 ignored） | 1 000 连接 fd/RSS 断言每次跑；万级（`TREEDB_CONNS`，含 25 000）手动 |
| `tests/independence.rs` | **3** | 独立性三把门：内核目录（`src/engine`、`src/net`）不出现客户端名字；`[dependencies]` 非 optional 项只能是 `libc` + `thiserror`；`postgres-compat` 不得进 `default`（§11 第 7 条） |

本轮新加/改写的回归锁：
`dentries_are_two_level_and_kind_survives_snapshot`（两层表形状 + kind 冷启动后
仍在 + 快照字节序列不变，§7.10）、
`prealloc_padding_reopens_cleanly`（预分配空洞不能被当记录）、
`torn_wal_tail_is_discarded_not_fatal` 与 `replay_skips_torn_tail`（坏尾必须注入到
**逻辑末尾**，不是文件 EOF —— 预分配后两者差着一大片零，注错地方就等于什么都没测）、
`ops_stats_checkpoint_shutdown_over_tcp`（`wal_bytes` 改按逻辑长度断言，checkpoint 后归零）。

### 9.1 三条被作废的测量（记录过程，防止有人复用假数据）

1. **`pg op=… pipeline=32 → 14.7 M ops/s`**：对照器里 `ping`/`lookup`/`readdir` 只跑
   1 条语句，外层却按 `pipeline` 计数，虚高 32×。现改成 `one_op` 返回**真正执行掉的
   语句数**，外层按它计 ops 与摊销延迟。
2. **PG 集群曾被 `-F` 启动**：`postgres -F` 是 **turn fsync off**（不是 initdb 的
   `--no-sync`），于是 PG 写入档量出了物理上不可能的数字。现在矩阵脚本用
   `-c fsync=on -c synchronous_commit=on` 起集群，并在跑之前**读回这两项、不为 on
   就直接 exit 1**。§7.3/§7.5 的 PG 数字都是闸门之后重跑的。
3. **`op=readdir` / `op=lookup` 的全部历史吞吐（含上面的 §7.5 与 §7.8）**：
   `treedb-bench` 的分母取在 `join` 之前，于是“建 64 条连接 + 每线程预热 8 视图
   × 500 文件”被算进了计时窗口 —— `--secs 3` 那一档 `over` 字段量到 4.8–6.2 s，
   treedb 的读档吞吐虚低 1.6–2×。`treedb-pgbench` 的预热在父线程做完，同一处只有
   3.0–3.05 s（≤ 1.5 % 偏差），所以**偏差只在一边**，旧对照表里“PG 高并发纯读
   反胜 3–8×”有一半是量具给的。两个客户端现在都改成「每线程自记窗口、汇总取
   最大窗口当分母」，§7.5 与 §7.10 的数字是修完之后重跑的；§7.2/§7.8 里标了
   “旧口径”的格子仍偏低，只能看量级不能相减。

---

## 10. 接入 GroveFS 的路径

### 阶段 A（本 crate，已完成）
引擎 + 协议 + 事件循环 + 五轴验收成文。**grovefs 目录零改动。**

### 阶段 B（接入服务端）
1. `grovefs/Cargo.toml` 加可选依赖，挂一个新 feature `treedb`（默认关，与现有 `pg`
   后端并列）。落地时先写成 `path = "../treedb"`，实测扛不住：**可选的 path 依赖
   即使 feature 关掉也要解析**，旁边没有同级克隆的 checkout 任何 feature 组合都
   编不过。最终形态是按 tag 的 git 依赖
   `treedb = { git = "https://github.com/TechWander/treedb.git", tag = "v0.3.1" }`
   （tag 跟着定版前移，本轮从 `v0.3.0` 升到 `v0.3.1`，格式兼容所以升级只是改行）
   —— 故意用匿名 https，没有 SSH key 也 `cargo fetch` 得动。已拿一个**旁边没有
   treedb 的干净克隆**验证默认档与 `--features treedb` 都能编过。
2. `remote/meta/server_ext.rs::StoreKind` 新增 `Treedb` variant，实现
   `MetaProvider`：把 FUSE 侧调用逐一映射到 §4.2 的 op。**不做 SQL 翻译层**，
   语义按 §5.2 的边界划分：fd/open 状态留在 grovefs 侧。
   ~~`data_ref` 两边表示不同……必须在写接入代码前定下来~~ 已定案：引擎侧
   改成不透明字节，与 grovefs/PG 的字符串键同形（§5.1），provider 里不做映射表。
   同批定下两条接口语义：`Unlink`/`Rmdir`/`Rename` 回 `Reply::NodeRef(freed)` 带
   GC 线索，`Rename` 加 `flags`（`RENAME_NOREPLACE`），否则 §8 要加两条“接入不可行”。
3. CLI：不新增旗标，复用现有 `--meta-store treedb://host:port` scheme（走
   `with_store` 既有管线，改动面比 `meta-server --treedb <addr>` 小）+ 连接池
   （长连接 + pipeline 复用，这才是协议设计的目标用法）。
4. 控制面（密钥/审计）二选一：走 treedb 的 `Key*`/`Audit*`，或保留过渡档的
   `ApiKeyStore`。建议先保留，减少一次性改动面。（spike 现状：保留本地 SQLite。）
   本轮重新定调：`Key*`/`Audit*` 本身形状就是 grovefs 的表，直接接上等于把业务
   语义坐进内核——真正的终态是 §11 第 7 条 T2 的通用表，而不是这两个 op。

### 阶段 B spike 的实际结果（2026-11-19，`exp/treedb-integration`）

上面四步全部落地，端到端跑通。`.build-bin/test_treedb.sh`（31 条断言，默认档
自动 SKIP）里 `treedb-server` + `grovefs meta-server --meta-store treedb://…` +
`data-server` + `mount` 四件套做真读写，逐条对上本文的承诺：

- create / write / read / readdir / mkdir / symlink / rename / unlink 全过；
  `size` 与字节都来自数据面，treedb 只装名字与属性；
- **GC 线索在真实数据面上被验证**：unlink 掉最后一个持有者、以及 rename 覆盖既有
  名字，两次都让 `blocks/` 里的对象计数下降 —— `Reply::NodeRef(freed)` 那条通道
  不是纸面设计，它确实把删除决定送到了持有字节的进程；
- 两台服务（meta-server 与引擎）重启后，树与字节都还在，remount 直接读回；
- `view create` / `view rm --base` 走控制面 RPC，在**引擎里**建/删树，
  `<meta-dir>/meta/team.db` 始终不存在；
- 未知 view 被 M_OPEN 拒绝，而不是悄悄长出一棵空树 —— 这条门的存在检查也改问引擎
  本身（`view_exists`），否则 treedb 档会把「树在引擎里」误判成「没有树」。

默认档不变：`treedb` feature 关掉时 `StoreKind` 根本没有第三个 variant，
`MetaStore` 的 29 个 Treedb 臂与 `src/treedb_store.rs` 整个文件都不参与编译；
`cargo check --all-targets`、两条窄 feature 门（`remote-only`、
`remote-only,fuse-bridge`）与整块回归板都在默认档复跑过 ——
`SKIP_BENCH=1 .build-bin/run_all.sh`：39 个套件、1418 条断言、0 失败，
`cargo test --release` 212 passed。同一份 `test_treedb.sh` 在默认档二进制上跑
只输出 `SKIP=1`（自跳门是**行为探针**：启一次 `--meta-store treedb://` 看它是否
拒绝，不能用 `--help` 判，帮助文本在所有 build 里都提 treedb），所以它在板上
永远记 PASS，不会把一个没有 treedb 的 checkout 弄红。

代价是反过来的：板子上的 build 步骤是默认档，所以**从板子上跑时永远 SKIP 掉
这一节** —— treedb 路径的回归必须显式 `cargo build --release --features treedb`
之后单独跑 `.build-bin/test_treedb.sh`。spike 阶段接受这个不对称（默认档是
必须守住的，实验档是自己守的）；现在依赖已经钉到 tag，剩下的只是要不要给板子
加一步带 feature 的 build，不再卡在分发方式上。

提交与入库：treedb 本身已建 git 仓并推两远端（`origin`=gitee.com:techwanderer/treedb、
`github`=github.com/TechWander/treedb，均为 e2bb51d，tag `v0.3.0`）；`.pgref/`
那个 1.9 G 的对照用 PG 私有集群与 `target/` 不入库，`Cargo.lock` **入库**（交付
的是要部署的二进制）。grovefs 侧两笔：`exp/treedb-integration` @ 53306a0（接入，
8 文件 +1221/-21）+ 1459a60（依赖换成 tag，+隔离克隆验证），**master 仍未动**
（e18b3d4，与 origin/github 一致）。测试脚本在 grovefs 的 `.build-bin/`，按该仓
`.gitignore` 的 `/.*/` 边界不入库。

spike 明确**没**做、也不在接入面上假装做的事 —— 一律返回拒绝，绝不返回空值
（一个空 Picture 会把全部字节判成垃圾，那是比报错糟糕得多的答案）：

| 缺口 | 现在的行为 | 为什么不能假装支持 |
|---|---|---|
| 硬链接 `link_node` | EPERM | 引擎没有 link op（§5.3） |
| POSIX 字节区间锁 | EPERM / None | 引擎只有整 inode 锁（§5.3） |
| `refs` 的字节量 | 恒 0 | 字节权威在数据面，size 也从数据面取 |
| `view export` / `view import` | 拒绝 | dump reader 是 SQL 形状的，treedb 无等价物 |
| `refs rebuild` | 拒绝 | 持有者计数在引擎内同 op 维护，没有独立账本可重算 |
| `gc` 审计 | 拒绝 | 没有聚合的 ledger 视图，回空集等于判所有对象为孤儿 |
| readdir 的属性 | N/64 次往返 | 协议该补 `ReadDirPlus`，见 §11 第 6 条 |
| `view list` 的物理树 | 列不出来 | 控制面枚举器只看本地 db 文件 |
| HA / 快照传送 | 启动即拒绝 | §8「无复制」这条限制本身 |
| ~~path 依赖的分发方式~~ | 已定：按 tag 的 git 依赖 | 钉 `v0.3.1`（随定版前移），无同级克隆也能构建（实测） |

### 阶段 C（回归与灰度）
1. ~~`test_pg.sh` 那套断言在 treedb 后端重跑~~ 已起头：新增 `.build-bin/test_treedb.sh`
   一节 treedb 专属断言（31 条），默认档不变。它钉的是**形状**而不是面积：
   `posix`/`e2e` 那种全量断言矩阵还没在 treedb 上跑过，§11 第 6 条列的缺口也还是缺口。
2. 万级视图 mount + create + readdir 混打，对照 `pg_stat_activity`（旧）与
   `--stats`（新）两条路径的连接数、RSS、p99。
3. 容灾演练：`kill -9` → 重启 → 已 ack 写全在（§6.4 已在 crate 内自动化，
   这步要在 grovefs 的端到端形状上再验一次）。

---

## 11. 下一步

1. ~~**多 worker 读扩展**（补 §7.5 的短板）~~ **已完成，且默认已开**：`SO_REUSEPORT` + N 条前端
   worker，纯读就地算、含写整批交给唯一提交线程（§2.3 A′、§7.5 轴 5）。验收判据
   「64 并发 `readdir` 不被 PG 甩开」现在**默认档就达标**（427 092 vs PG 146 203 =
   2.9×；`workers=16` 是 5.9×）：默认值改成 `min(8, 核数/2)`，不再需要手工
   `--workers`（§8 第 1 条）。当时提的两条备选（按 view 分片状态 / per-worker 只读快照）
   都不需要：状态只有一个，拿 `RwLock` 读锁共享即可，写仍由单一定序者进 WAL ——
   代价只多了一个 `--workers` 旋钮和 §8 第 1 条那三项。接下去要动的是写轴：
   单提交线程已经是上限。
2. **checkpoint 不阻塞回合**（限制 7）：快照生成挪到辅助线程，主循环只切 WAL 代际。
3. **`Stats` 加磁盘长度**：现在只有逻辑长度，容量规划要看两个数。
4. **日志传送做只读副本**：让「无复制」这条限制至少能降级成「无写扩展」。
5. ~~**接入 spike**~~ **已完成并端到端跑通**（grovefs 实验分支
   `exp/treedb-integration`，用户 2026-11-19 授权），结果与缺口清单见 §10 阶段 B
   末节。与本节原写法有两处偏离，原因已写进 §10：CLI 入口用
   `--meta-store treedb://host:port` 而不是新旗标；写接入代码前先把 `data_ref` 改成
   不透明字节、给 `Unlink`/`Rmdir`/`Rename` 补上 GC 线索与 `RENAME_NOREPLACE`。
   原先卡住的「分发方式」已定：本仓推到 gitee+github 并打 `v0.3.0`，grovefs 改成
   按 tag 的 git 依赖，拿没有同级克隆的干净仓验证过默认档与 `--features treedb`
   均绿。**仍未推 master**：推不推是下一轮的决定，而且 treedb 现在只接了「树」，
   定义/密钥/账户还在本地 SQLite —— 要么先接受这个分裂上线，要么先把下面第 7 条
   的通用表能力补进引擎再一次性切（注意：是补「通用表」，不是把 grovefs 的表搬进去）。
6. **`ReadDirPlus`**：接进去才发现 readdir 一页要付 `1 + N/64` 次往返 —— 目录列表
   本身不带属性，provider 只能回头逐条 `Getattr`。这是 §7.5 那条读轴在 grovefs 形状上
   的真实成本，补一个带属性的 dirents 应答即可打平 PG 档的一条 SQL。
7. **独立性审计：treedb 是一个数据库项目，不是 GroveFS 的一个模块**。用户定的约束
   是：grovefs 的业务语义不得漏进引擎内核，为 grovefs 做的适配一律留在 grovefs 仓里
   拦在 feature 门后，treedb 对外只讲通用元数据引擎接口。本轮对着代码校了一遍，
   **三档现状**：

   | 档 | 现状 | 是否违背约束 |
   |----|------|------------|
   | **L0 命名与注释** | 引擎内核里 21 处 grovefs 提及**全部是注释**（「与 grovefs `node.is_dir` 的 0/1/2 一致」这类对齐声明）；无 `use grovefs::`，无 grovefs 类型 | ~~无害，但叙述口径要改~~ **已改**（T3）：24 行重写为「POSIX/FUSE/关系库通用形状」的写法，理由保留、主语不再是某个客户端，并由门 1 守住 |
   | **L1 数据结构与 op 语义** | `KeyRec` 逐列照抄 `grovefs_control.api_keys`（`ro`/`owner`/`note`/`expires_at`/`secret_hint`/`created_by`），`AuditRec` 照抄 `mcp_calls`，`Def*` 就是视图定义，`EADV = 68` 注释写着「借用 grovefs 控制面既有码」，`KeyCheck` 里带 `verb` 授权参数 | **违反**。这是业务形状进了内核 |
   | **L2 依赖图** | ~~`postgres`/`rusqlite(bundled)`/`sha2`/`uuid`/`chrono`/`tracing`/`parking_lot` 全是非 optional 依赖~~ **已关进 `postgres-compat`**（T1 完成）：默认档 `cargo tree` 只剩 `libc` + `thiserror`；依据仍是 grovefs 只用 `treedb::net::Client` 与 `engine::model::Op`，`pool/`、`store/`、`persist/`、`schema/` 一行符号都没用 | ~~违反~~ **已修** |

   好消息是边界已经天然存在一半：`data_ref` 是不透明字节（引擎不解释内容键），
   引擎**不算哈希**（`sha2` 只出现在 `store/keys.rs` 和 demo 里，`KeyCheck` 只比较
   调用者送来的 hash），错误不做二次翻译，字节区间锁与 `link()` 不做 —— 这些都是
   「通用引擎」该有的形状。要改的是下面四步，**每一步都在本仓**：

   - ~~**T1 过渡档关进 feature**~~ **已完成**：`pool/`、`store/`、`persist/`、`schema/`、
     `probe.rs` 关进 `postgres-compat`（`default = []`，默认关），`sha2`/`uuid`/
     `rusqlite`/`postgres`/`chrono`/`tracing`/`parking_lot` 全部转 optional，
     `treedb-demo`/`treedb-pgbench` 上 `required-features`，`error.rs` 里那两个
     `From<postgres::Error>`/`From<rusqlite::Error>` 一并关掉（变体保留，免得下游分支
     突然编不过）。实测：`cargo tree -e no-dev` 默认档**只剩 libc + thiserror**；
     `cargo test --release` 默认档 68 passed / 0 failed（lib 从 56 降到 26，少的 30 条
     正是过渡档自己的单测），`--features postgres-compat` 下 99 passed / 0 failed /
     7 ignored，与开关前逐条相同。顺手删了 `tracing-subscriber` 这个没人在用的
     dev-dependency（拖 11 个 crate，Cargo.lock 少 105 行全是它）。注意锁文件里
     `postgres`/`rusqlite` 条目仍在：cargo 对可选依赖做 union 解析，判据是
     `cargo tree` 与编译日志，不是锁文件行数。
   - **T2 把密钥/审计/定义三张专用表换成通用表能力**：内核只给 `TableCreate` /
     `RowPut` / `RowGet` / `RowList` / `RowDelete`（字段为字节列，可加唯一约束与
     比较条件），具体列名、撤销/过期/只读这些规则全部由客户端定义。协议版本
     同批递增并补 decode 向量，别弄“字段名改了但版本号不动”。
   - ~~**T3 叙述去依赖**~~ **已完成**：crate description、`treedb-server --help` 首行、
     `lib.rs` 头部全部改掉「GroveFS 专用/后端专用」的说法，内核注释按行重写（见上）；
     `lib.rs` 的「三层能力」重排为引擎本体 / 连接层 / 过渡档，并把原来四条不变量
     拆成「引擎本体的」（单一定序点、acked 写不丢、同连接应答不乱序）与「过渡档的」（PG
     唯一权威、只在不可达时降级）两组 —— 原来那四条全在讲脚手架，这本身就是定位跑偏。
     另外补了 `README.md`：公开仓库此前只有一份 800 行设计文档，没有门牌。
   - ~~**T4 把「不许漏 grovefs 语义」写成可测的门**~~ **已完成**：`tests/independence.rs`
     三把门 —— 内核目录（`src/engine`、`src/net`）不出现客户端名字；`[dependencies]`
     里非 optional 项只能是 `libc` + `thiserror`；`postgres-compat` 不得进 `default`。
     关键词是拼出来的，文件也不扫 `tests/`，否则门会把自己拦下来。**反向验过**：
     往 `src/engine/` 丢一个含该名字的探针文件，门 1 立刻 FAILED，删掉又绿 —— 门不是装饰。
     跑法：默认档 `cargo test --release` = **74 passed / 0 failed**（含三把门），
     `--features postgres-compat` = **105 passed / 0 failed**（数字跟 §9 同步）。

   接入侧因此得到的约束：第 5 条里“把定义/密钥/账户补进引擎”的正解不是
   `Key*`/`Audit*` 直接给 grovefs 用，而是 grovefs 用 T2 的通用表自己拼语义；
   §10 阶段 B 第 4 步那句“走 treedb 的 `Key*`/`Audit*`”作废。
8. ~~**`always` 档的 flush 流水线**~~ **已完成一半**（§7.9 第 3 条）：按连接串行
   fsync 已删，p99 从 81.5 ms 降到 31.1 ms、吞吐 5.77×，且原判据（与 `group` 同量级、
   崩溃用例全绿）已满足。**没做的那一半才是剩下的肉**：提交线程仍然在
   `wal.commit()` 里**阻塞等盘**，这一窗口内它既不接 worker 的转发也不发 ack，
   `group` 档 64 并发那 p99 24–29 ms 就是「堆在 fsync 窗口外面」的排队。
   改法：专用 flusher 线程持有 WAL 的 `try_clone()` fd，提交线程只 append（一次
   write）并把目标 seq 交给 flusher，完成事件回送到提交线程后再 ack 那一批；
   checkpoint / 退出前必须 drain（`wal.reset()` 不能与 in-flight flush 共命）。
   验收判据：`benches/ab_durability.sh` 同格 p99 ≤ 8 ms、机械盘 ops/s 从 12 k 再往上，
   且 `tests/crash_recovery.rs` 六条仍全绿 —— 不然只是把尾延迟换成了丢写。
9. **dirents 应答直接编进连接发送缓冲**（§7.10 剩下那一条）：一页仍有 104 次堆
   分配，100 次来自 `Reply::Dirents.names` 的逐名 `Vec<u8>`。改造点是 encode 侧
   往调用方给的 `&mut Vec<u8>` 直接写，而不是先造一个 `Vec<Vec<u8>>`。收益只能
   估（100 次 alloc ≈ 每页 2–3 µs，没实测），所以它排在第 2/6 条之后：默认档
   读轴已经领先 PG 2.9×，这里不再是短板。
