# treedb 设计文档

**定位**：GroveFS 的专用元数据引擎。单进程、epoll 事件循环（默认单线程；`--workers N`
开 N 条前端 worker 并行读，但写永远只有一个提交线程在定序）、自定义二进制线协议、
自带 WAL + 快照恢复。目标是把「万级并发视图 × 每视图多条活跃连接」这件事从 PG 的
process-per-connection 成本模型里解放出来，同时不牺牲已 ack 写入的持久性。

**状态**：v0.3.0，引擎本体已落地并跑完五轴验收；接入 spike 已在 grovefs 实验分支
`exp/treedb-integration` 上**端到端跑通**（§10 阶段 B 末节：31 条 e2e 断言全过，
grovefs 默认档逐字节不变）。`cargo test` 99 条全绿（56 lib +
19 engine_crud + 9 net_protocol + 6 crash_recovery + 7 read_scaling + 1 concurrency +
1 scale_connections）。PG 只出现在
过渡档模块（`src/pool`、`src/store`、`src/persist`）和性能对照器（`src/bin/pgbench.rs`）里，
引擎本体对 PG 零依赖。

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
| 1 | 连接轴 | 单进程 ≥10 000 并发连接，且 1 fd/conn | 10 000 连接：RSS +5 MiB、**0.53 KiB/conn**、`fd/conn = 1.0000`；25 000 连接：0.39 KiB/conn | **达成** |
| 2 | 持久性轴 | `kill -9` 后已 ack 的写零丢失 | 6 个崩溃用例全绿（`always`/`group` 两档、6 线程混打中途崩、checkpoint 瞬间崩、连崩 3 轮） | **达成** |
| 3 | 吞吐轴 | op QPS/p99 与 PG 同语义对照 | 写：treedb 默认档对 PG **1.4–7.6×** QPS（64 并发 + pipeline 时收窄到 1.4×）、p50 **1.5–16×**；读：低并发 2.6–10×；64 并发纯读在默认 `workers=1` 下仍被 PG 反胜 3–8×，**开 `--workers 16` 后反超 2.2–4.1×**（`readdir` 316 780 vs 142 815、`lookup` 861 465 vs 208 239、`ping` 1.72 M vs 509 k，p50 31–113 µs） | **达成**（读轴要显式开并行，代价见 §8 第 1 条） |
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
| `always` | POSIX `O_DSYNC` 级 | 每个写 op 一次 |
| `group`（默认） | group commit | 每 epoll 轮一次，跨连接合并 |
| `never` | 只 write 不 fsync | 仅按 `sync_ms` 周期刷，崩溃会丢最近窗口 |

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

同一份代码、同一个 op，**换盘就是 8×**。结论：`always` 档在机械盘上上限约 1.6k ops/s，
和连接数无关（盘被打满了）；这类格子只能用来比较引擎，不能用来宣称引擎快慢。
GroveFS 生产默认应该是 `group`，`always` 留给「每个 op 独立可恢复」的强需求。

### 7.5 读路径：单线程时 PG 赢了 —— 补上前端 worker 之后反过来

**先看没补之前的账**（`workers=1`，也就是默认档；数据来自轴 2 / 轴 4，`dur=group`、
WAL 同在 `/data`）：

| op | conns/pipe | treedb `group` ops/s（p50 µs） | PG ops/s（p50 µs） | 谁快 |
|----|-----------|-------------------------------|--------------------|------|
| `lookup` | 1/1 | 42 755（20） | 4 461（211） | treedb **9.6×** |
| `lookup` | 8/1 | 94 569（50） | 36 313（211） | treedb 2.6× |
| `lookup` | **64/1** | 66 229（592） | **208 239（303）** | **PG 3.1×** |
| `lookup` | 64/32 | 516 774（67） | 299 561（208） | treedb 1.7× |
| `readdir` | 1/1 | 19 868（43） | 3 176（306） | treedb 6.3× |
| `readdir` | 8/1 | 31 227（196） | 25 107（311） | treedb 1.2× |
| `readdir` | **64/1** | 18 166（**2 056**） | **142 815（434）** | **PG 7.9×** |
| `readdir` | 64/32 | 35 334（1 202） | 179 454（351） | PG 5.1× |
| `ping` | 64/1 | 135 631（443）〔never 档〕 | 509 099（119） | PG 3.8× |

低并发 treedb 全面领先（一次往返就够，纯内存查找），一到 64 并发就被反超。
根因**不是 CPU、不是协议、不是内存布局**，是「每连接一次 `read` + 一次 `write`」
全挤在一条线程上：同一条 `lookup` 64/1 只有 6.6 万，改成 64/32 立刻 51.7 万，
计算量没变，变的只是 syscall 怎么排。PG 每个 backend 一个进程，天然摊在 64 个核上。

**补法**：`--workers N` 起 N 条前端 worker（§2.3 A′）。默认仍是 `workers=1`，
上面那张表就是默认档的原样，历史数字没被污染 —— 本次复跑 lookup 64/1 = 60 499、
readdir 64/1 = 18 769，与表里同源同量级，差的只是这块机械盘的抖动。

**读并行实测**（轴 5，conns = threads = 64，`dur=group`、WAL 在 `/data`，bench 默认
8 视图 × 500 文件预热；格式 ops/s（p50 µs））：

| op | pipe | w=1 | w=2 | w=4 | w=8 | w=16 | PG 同格 |
|----|------|------|------|------|------|------|---------|
| `lookup` | 1 | 60 499（612） | 127 722（292） | 255 665（162） | 513 872（68） | **861 465（37）** | 208 239（303） |
| `lookup` | 32 | 493 548（74） | 717 991（46） | 1 856 005（16） | 1 948 841（17） | 1 832 608（16） | 299 561（208） |
| `readdir` | 1 | 18 769（1 999） | 42 562（976） | 84 162（405） | 150 176（210） | **316 780（113）** | 142 815（434） |
| `readdir` | 32 | 39 057（1 194） | 88 432（390） | 165 565（191） | 338 430（94） | **505 400（49）** | 179 454（351） |
| `ping`〔never〕 | 1 | 136 655（444） | 257 048（244） | 517 624（123） | 990 382（62） | **1 715 136（31）** | 509 099（119） |
| `ping`〔never〕 | 32 | 2 196 644（26） | 3 188 868（18） | 4 989 784（10） | 6 240 328（8） | 7 272 338（7） | —（libpq 做不到真合并往返） |

怎么读这张表：

- **§11 原来那条判据「64 并发 `readdir` 不被 PG 甩开」达标了**：`workers=8` 就追平
  （150 176 vs 142 815），`workers=16` 是 **2.2×**，而且 p50 从 1 999 µs 降到 113 µs。
  `lookup` 在 `workers=4` 过 PG（255 665 vs 208 239），`workers=16` 是 **4.1×**；
  `ping` 是 **3.4×**。尾延迟是同一条线上一起反过来的：PG 那三格 64/1 的 p50 是
  119–434 µs，这边是 31–113 µs。
- **扩展接近线性**：前端从 1 条到 16 条，`lookup` 换来 14.2 倍吞吐、`ping` 12.6 倍、
  `readdir` 16.9 倍。
  后一个超过核数倍率，说明 `workers=1` 那一格还被写回背压额外压着（它的 max 是
  98 ms，`workers=16` 是 5.4 ms），不要把 16.9× 当成对 16 核的线性外推。
- **pipeline=32 的高阶格子只能看趋势**：同一格两次独立复跑在 `workers=2/4` 上摆动了
  近 1.5 倍（客户端那 64 条线程自己就是噪声源）。报得出的结论是「w≥4 后进入
  平台期」，具体倍率别抠。
- **group commit 没被并行化弄丢**：提交线程每一轮把**所有**上行信箱一次排干再做
  一次 `commit_batch`，只排干被唤醒那一条等于把摊薄比例砍到 1/N。下面写档那一堆
  格子没变慢，就是这条还在生效。

**这笔账的另一半 —— 写档有没有被拖下水**（同轴、同机器、conns=64）：

| op | pipe | w=1 | w=8 | w=16 |
|----|------|------|------|------|
| `create` | 1 | 8 410（3 193） | 10 696（1 935） | 10 810（1 961） |
| `create` | 32 | 49 205（1 045） | 47 048（1 141） | 47 662（1 194） |
| `write` | 1 | 10 724（2 650） | 9 309（2 083） | 10 388（1 940） |
| `write` | 32 | 61 680（1 014） | 53 662（1 061） | 61 709（1 033） |

没有系统性回退：`create` pipeline=32 掉 3–4%，`write` 那两行在 ±13% 之间来回，
与这块机械盘单独的 fsync 方差同量级（p50 反而更好，因为一次往返里能合进同一轮的
写更多）。道理也简单：64 连接的写在默认档就是 **fsync-bound**，含写批次多出来的
两次 eventfd 递交是 µs 级，在这盘上它没出现在任何一格里。但要把话说全：写延迟的
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

写路径与连接轴上 treedb 是**结构性**领先（fsync 摊薄方式 + 1 fd/conn）；
纯读高并发以前是**结构性**落后（单核），现在是**可配置**的：`--workers N` 把
读并行度与 syscall 并行度一起摊到 N 个核上，16 个前端下 64 并发纯读反而领先
PG 2.2–4.1×（§7.5）。默认档仍是 `workers=1`：不显式开就没有这份领先，
也没有它的那三个代价（§8 第 1 条）。GroveFS 的元数据负载以写和长连接为主，
这条取舍仍然划得来；写扩展不在这条轴上 —— 它永远只有一个提交线程。

---

## 8. 已知限制（诚实边界）

1. **读并行要显式开，而且它不是免费的**（§7.5）。默认 `workers=1`，此时 64 并发纯读
   仍然被 PG 反超 3–8×；`--workers N` 之后换成这边领先 2.2–4.1×。代价三条：
   ① 每条 worker 多 3 个 fd（与连接数无关）；② `SO_REUSEPORT` 语义上允许**另一个
   进程并列 bind 同一地址端口**，届时流量被两边分摊 —— 单实例假设必须靠部署侧保
   证（`--pid-file` 只能挡住“自己重起自己”，挡不住第二个实例也开了这个标志）；
   ③ **写扩展不跟 workers 长**：永远只有一个提交线程，N 再大也只是前端收发包并行，
   写一旦超过那个核的 compute 能力就该先看 `worst_round_us`（`Report`）而不是继续加 worker。
   拿多少核没有自适应：得自己按核数与实测调，128 核机器上 16 是本轮量出来的拐点附近。
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

---

## 9. 测试清单

```sh
cargo test --release            # 全量，99 条
cargo test --release --lib      # 只看引擎单测
cargo test --release --test scale_connections -- --ignored --nocapture   # 万级连接重压
bash benches/throughput_matrix.sh 3        # 五轴吞吐矩阵（默认只跑 1–4）
```

| 目标 | 数量 | 覆盖 |
|------|------|------|
| `--lib` | **56** | codec round-trip（全 tag）、CRC 已知向量、碎帧/超长帧、WAL 帧与截断重放、**预分配空洞重开**（`prealloc_padding_reopens_cleanly`）、**大批次不跳写**（`huge_batch_writes_every_record`）、**seq 跳号硬错**、状态快照保留 node key、缓冲压缩、租约过期边界 |
| `tests/engine_crud.rs` | **19** | §5.2 全部 POSIX 语义（含 GC 线索与 NOREPLACE）+ 控制面（密钥/审计/定义/放置）+ checkpoint 后重开 + 坏尾截断 |
| `tests/net_protocol.rs` | **9** | TCP 端到端：CRUD、碎帧、pipeline 保序、并发连接不串扰、超长帧只杀违规者、大回包流式读完、`--stats`/`--checkpoint`/`--shutdown`、优雅停服后同地址立刻重绑 |
| `tests/crash_recovery.rs` | **6** | §6.4 |
| `tests/read_scaling.rs` | **7** | 只读分派表逐条对 `translate`（判错一条就是丢写）、`workers=1/8` 答复逐条相同、每连接流内顺序、一写多读不读到半截状态、`ops_total` 记上 worker 就地算的读、并行档下 `kill -9` 已 ack 的写全在、**fd 复用不会把上一任的应答塞给新任** |
| `tests/concurrency.rs` | 1（+6 ignored） | 无 PG 时跑「拨号失败不漏 live」；其余需要真实 PG，属过渡档 |
| `tests/scale_connections.rs` | 1（+1 ignored） | 1 000 连接 fd/RSS 断言每次跑；万级（`TREEDB_CONNS`，含 25 000）手动 |

本轮新加/改写的回归锁：
`prealloc_padding_reopens_cleanly`（预分配空洞不能被当记录）、
`torn_wal_tail_is_discarded_not_fatal` 与 `replay_skips_torn_tail`（坏尾必须注入到
**逻辑末尾**，不是文件 EOF —— 预分配后两者差着一大片零，注错地方就等于什么都没测）、
`ops_stats_checkpoint_shutdown_over_tcp`（`wal_bytes` 改按逻辑长度断言，checkpoint 后归零）。

### 9.1 两条被作废的测量（记录过程，防止有人复用假数据）

1. **`pg op=… pipeline=32 → 14.7 M ops/s`**：对照器里 `ping`/`lookup`/`readdir` 只跑
   1 条语句，外层却按 `pipeline` 计数，虚高 32×。现改成 `one_op` 返回**真正执行掉的
   语句数**，外层按它计 ops 与摊销延迟。
2. **PG 集群曾被 `-F` 启动**：`postgres -F` 是 **turn fsync off**（不是 initdb 的
   `--no-sync`），于是 PG 写入档量出了物理上不可能的数字。现在矩阵脚本用
   `-c fsync=on -c synchronous_commit=on` 起集群，并在跑之前**读回这两项、不为 on
   就直接 exit 1**。§7.3/§7.5 的 PG 数字都是闸门之后重跑的。

---

## 10. 接入 GroveFS 的路径

### 阶段 A（本 crate，已完成）
引擎 + 协议 + 事件循环 + 五轴验收成文。**grovefs 目录零改动。**

### 阶段 B（接入服务端）
1. `grovefs/Cargo.toml` 加 `treedb = { path = "../treedb", optional = true }`，
   挂在 `native` 之下，与现有 `pg` 后端并列，默认关（新 feature `treedb`）。
   ⚠ path 依赖即使 feature 关也必须能解析：旁边没有 `../treedb` 的克隆任何
   feature 组合都编不过。**入库前先定分发方式**（vendored / workspace / registry）。
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
必须守住的，实验档是自己守的）；要让它真正进板，得先定 §11 第 5 条里那个
path 依赖分发方式（vendored / workspace / registry）。

提交：grovefs `exp/treedb-integration` @ 53306a0（8 文件 +1221/-21），**master
未动**（e18b3d4，与 origin/github 一致）。测试脚本在 `.build-bin/`，按本仓
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
| path 依赖的分发方式 | 未定 | 决定能否入库的唯一分叉 |

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

1. ~~**多 worker 读扩展**（补 §7.5 的短板）~~ **已完成**：`SO_REUSEPORT` + N 条前端
   worker，纯读就地算、含写整批交给唯一提交线程（§2.3 A′、§7.5 轴 5）。验收判据
   「64 并发 `readdir` 不被 PG 甩开」在 `workers=8` 上追平、`workers=16` 上是 2.2×。
   当时提的两条备选（按 view 分片状态 / per-worker 只读快照）都不需要：状态只有
   一个，拿 `RwLock` 读锁共享即可，写仍由单一定序者进 WAL —— 代价只多了一个
   `--workers` 旋钮和 §8 第 1 条那三项。接下去要动的是写轴：单提交线程已经是上限。
2. **checkpoint 不阻塞回合**（限制 7）：快照生成挪到辅助线程，主循环只切 WAL 代际。
3. **`Stats` 加磁盘长度**：现在只有逻辑长度，容量规划要看两个数。
4. **日志传送做只读副本**：让「无复制」这条限制至少能降级成「无写扩展」。
5. ~~**接入 spike**~~ **已完成并端到端跑通**（grovefs 实验分支
   `exp/treedb-integration`，用户 2026-11-19 授权），结果与缺口清单见 §10 阶段 B
   末节。与本节原写法有两处偏离，原因已写进 §10：CLI 入口用
   `--meta-store treedb://host:port` 而不是新旗标；写接入代码前先把 `data_ref` 改成
   不透明字节、给 `Unlink`/`Rmdir`/`Rename` 补上 GC 线索与 `RENAME_NOREPLACE`。
   **仍不推 master**：spike 只在实验分支上，且卡在 path 依赖的分发方式这个分叉上
   （vendored / workspace / registry 三选一，决定后才能真正入库）。
6. **`ReadDirPlus`**：接进去才发现 readdir 一页要付 `1 + N/64` 次往返 —— 目录列表
   本身不带属性，provider 只能回头逐条 `Getattr`。这是 §7.5 那条读轴在 grovefs 形状上
   的真实成本，补一个带属性的 dirents 应答即可打平 PG 档的一条 SQL。
