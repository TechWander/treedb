# treedb 设计文档

**定位**：元数据引擎。单进程、epoll 事件循环（前端 worker 默认按核数给
`min(8, 核数/2)`，`--workers 1` 是单线程基线档；但写永远只有一个提交线程在定序）、
自定义二进制线协议、自带 WAL + 快照恢复。要解的问题形状是「万级并发视图 ×
每视图多条活跃连接」，把这件事从 PG 的 process-per-connection 成本模型里解放出来，
同时不牺牲已 ack 写入的持久性。
它**不绑某个客户端**：类型与语义按 POSIX 元数据定，为客户端服务的适配层留在客户端
自己的仓里；本仓唯一与 PostgreSQL 有关的东西是默认关掉的 `postgres-compat` 脚手架，
而这条约束有测试守着（`tests/independence.rs`，详见 §11 第 7 条）。

**状态**：v0.8.0（2026-11-25）。引擎本体已落地并跑完五轴验收（§7.8 是 2026-11-19 的全轴复跑）；
已建 git 仓推 `origin`(gitee) + `github` 两远端。0.3.0 之后的一批全在**读轴**上：
量具口径对称化（§9.1 第 3 条）、默认前端 worker 按核数（§8 第 1 条）、目录项两层表
+ kind 冗余（§7.10）；那一批**线协议、WAL 与快照的字节格式一个字节没动**（当时
`SNAP_VERSION` 仍是 2），旧数据目录直接升级。tag：`v0.3.0` 是引擎首版（e2bb51d），`v0.3.1` 是本轮
定版。接入 spike 已在 grovefs 实验分支 `exp/treedb-integration` 上**端到端跑通**（§10
阶段 B 末节：31 条 e2e 断言全过，grovefs 默认档逐字节不变；treedb 在 grovefs 里是
**按 tag 的 git 依赖**，不再依赖同级克隆）。

`v0.3.1` 之后落了一笔**写轴**改动并单独定版为 `v0.3.2`：`always` 档的按连接串行
fsync 删掉了，与 `group` 共用一个提交点，机械盘 64 并发 1 891 → 10 913 ops/s、
p99 81.5 → 31.1 ms（§7.9 第 3 条，受控 A/B 在 `benches/results/ab_durability.log`）。

`v0.3.2` 之后是**能力面**一轮，把 §11 第 3、6、7（T2）三条一次收口：引擎有了通用表
（`Table*` 四条 + `Row*` 五条，字节列 + 唯一约束 + 条件写/CAS），`ReadDirPlus` 让一页
dirents 直接带属性，`Stats` 补上 WAL 磁盘长度、快照长度与协议版本。**这一轮字节格式
真的变了**：op 与 reply 各加了一批 tag（47 op / 26 reply），`PROTOCOL_VERSION` 2 → 3，
且帧里没有版本位可升（§4.1）；快照尾部多了 tables/rows 两段，`SNAP_VERSION` 2 → 3，
旧快照被版本号闸门拒收而不是静默解错。定版 `v0.3.3`；grovefs 侧仍钉 `v0.3.2`，升级
面见 §10。

`v0.3.3` 之后落的是 §11 第 10 条前半那个断点：**快照迁移工具**。上一轮把
`SNAP_VERSION` 推到 3 的代价是旧数据目录在新二进制前起不来，而「升级等于元数据
重建」这句话不能当作上线依据。现在有了 `treedb-migrate`：把 v2 布局读成内存
`State`、再按 v3 布局写回，**WAL 一个字节不动**，尾巴上的 seq 水位照搬，动手前自动
落一份 `state.snap.v2.bak`，于是回滚只是把备份拷回去再跑旧二进制。常规启动路径
仍然严格拒收旧版本 —— 不给「静默兼容」开口子，只是错误消息现在会给出命令（§6.5）。
这一轮**没有再动字节格式**：`PROTOCOL_VERSION` 与 `SNAP_VERSION` 都仍是 3，加上的只是
一个新命令（`treedb-migrate`）、一对迁移专用的读写入口（`State::load_for_migration` /
`State::save_v2`）与两个离线 API（`probe_snapshot` / `migrate_snapshot`）。定版 `v0.3.4`。

`v0.3.4` 之后是**日志传送与只读副本**一轮（§6.6，收掉 §11 第 4 条与限制 6）：两条新
op（`WalFetch` 读 WAL 帧、`SnapFetch` 切快照文件）、一个只读实例入口
（`treedb-standby`：拉快照 → 追 WAL → 只读应答，写拿 `EROFS`）。**这一轮又动了字节
格式**：`PROTOCOL_VERSION` 3 → 4（49 op / 28 reply，`EngineStats` 尾部多六个 `replica_*`），
而 `SNAP_VERSION` 仍是 3 —— 副本装的就是主的快照原件，格式一变就得先给副本出一条
迁移路径，所以快照布局本轮刻意不动。落盘的旧目录与新二进制之间没有任何新闸门。定版 `v0.3.5`。

`v0.3.5` 之后先是一批**纯取证**（连接上限、多用户、网络、备份四个能力面，`src/`
一个字节没动，故不另起 tag）；本轮是**接入面认证**（§4.4）：两条新 op
（`AuthBegin` / `AuthFinish`）与两个新 reply tag（`Challenge` 0x0190 / `AuthOk` 0x0191），
一次共享密钥的双向挑战握手，三个入口（`server` / `standby` / `bench`）共用一个
`--auth-key-file`，**默认关**。`PROTOCOL_VERSION` 4 → 5（51 op / 30 reply），但这次是
**纯追加**：既有帧一个字节未动、`EngineStats` 也没加长，所以一个 v4 客户端对 v5
服务端的每一格读写与对 v4 逐字相同（连 §4.1 那句「静默丢尾部字段」都不存在）；
`SNAP_VERSION` 仍 3 —— 握手不落盘，不碰任何磁盘形状。代价实测见 §4.4 末表。
定版 `v0.3.6`。

`v0.3.6` 之后是**问题清单收口**一轮（审查提的八条，两处是语义而不是实现）：快照尾巴从
8 字节裸 `seq` 变成 16 字节自描述尾巴（`crc32` + `seq` + magic），`State::load` 不再容忍
正文之后的余量；数据目录加 `flock` 互斥，`treedb-migrate` 拿同一把锁；停服收尾的
fsync/checkpoint、`--durability` 与数值旗标一律 fail-closed；背压时摘掉 `EPOLLIN` 不再
空转；`unlink`/`rmdir` 的「父节点不是目录」回到 `ENOTDIR`。**字节格式没有动**：
`SNAP_VERSION` 与 `PROTOCOL_VERSION` 都仍是 3 / 5，尾巴变长靠 magic 分辨，旧目录零迁移
（§6.8）。定版 `v0.3.7`。

`v0.3.7` 之后补了两笔，都不碰引擎字节与协议：① grovefs 侧 pin 从 `v0.3.6` 提到
`v0.3.7`（§10 阶段 B 第 6 条；`Cargo.toml`/`Cargo.lock` 两行，无适配改动，该分支
`test_treedb.sh` 实测 **53/0**）；② §7.13 把上一轮的三臂对照（treedb / PG 12 /
SQLite3，24 格）写成文，并给 `final.tsv` 补了生成器与两条对账脚本 —— 上一版那张表是
手搓拼出来的，`p50`/`p99` 被拼成了一列而形状完全像真数字。

`v0.3.7` 之后是**切主**一轮（§6.6「切主」那一节，收掉 §11 第 4 条②③）：
**fencing token** 落在一个 `fence` 文件里，主在 `WalBatch` / `SnapChunk` 尾部把它带
下去，副本拒收令牌更小的对端、见到更大的先把令牌落盘再吃数据；`treedb-standby
--promote` 是对目录的离线动作（令牌 +1 后打印下一步命令，不起服务）。**这一轮动了
线协议且不兼容**：`PROTOCOL_VERSION` 5 → 6（op/reply 计数不变，仍是 51 op / 30 reply，
两个既有 reply 各多一个 `u64`），`SNAP_VERSION` 仍 3 —— 令牌故意不进快照也不进 WAL，
所以旧快照零迁移。定版 `v0.4.0`。

`v0.4.0` 之后是**硬链接**一轮（§5.2，收掉 §10 阶段 C 1 里「`Link` op 要动协议」这个
未定价项）：`Op::Link`（tag `0x002c`）给已有 inode 加第二根 dentry。**这一轮是纯追加**：
既有帧一个字节未动、`EngineStats` 也没加长，所以 `PROTOCOL_VERSION` 仍 6、
`SNAP_VERSION` 仍 3，任何 `v0.3.x`/`v0.4.0` 数据目录零迁移；把新 op 发给旧服务端会在
decode 处得到 `unknown op tag 0x002c`，而不是把一条写静默当成别的样子。真正的变化
不在协议在记账：`nlink > 1` 之后「摘一个名字」与「删 inode」必须分开，`Unlink` /
`Rename` 覆盖 / `Symlink` upsert 三条路径都按「是不是最后一个名字」分流 —— 不是的话
只减一票、不下内容引用账、应答里带空键（§5.2）。定版 `v0.5.0`。接入侧随后跟着
闭合（2026-11-21）：grovefs 把 pin 从 `v0.3.7` 升到 `v0.5.0`、`link_node` 改成真发
`Op::Link`，阶段 C 1 的 10 条硬链接断言由 SKIP 转正（§10）。同一轮又把 §8 第 7 条
那个「checkpoint 停轮无上限」收掉了（2026-11-21）：自动档改成旁路重放式快照，
收尾用 `Wal::truncate_before` 只搬尾巴 —— 20 000 节点上停轮 23.2 ms → 派工 37.5 µs。
接入侧随后跟到 `v0.5.1`（grovefs 只动 pin 与 lock，适配层零改动）：矩阵仍
`51/0/0`、`test_treedb.sh` `53/0`、grovefs `cargo test --features treedb` `318/0/4`。

`v0.5.1` 之后（未发版）收了问题清单里的两条能力缺口。第一条是**离线 dump / load**
（§6.9）：新工具 `treedb-dump` / `treedb-load` 把整份状态导成自描述的逻辑记录流（`Log`
标签表当时到 22）再重建目录，容器带 CRC32 与三个 `next_*`（两者都是当时的形状 —— M4
之后是 1..=10 与 19..=27、两个 `next_*`，§6.9）；load 只写 `state.snap`、
不写 WAL。第二条是**在线备份**（§6.10）：`treedb-backup` 用副本那两条只读 op
（`SnapFetch` + `WalFetch`）从在线主拉一份一致切面，落成与 dump 逐字节同格式的文件
—— 主不停机、主端零改动，`PROTOCOL_VERSION` 也不动。
**不碰引擎字节格式与线协议**（无 op/reply/tag 变更，`PROTOCOL_VERSION` 6 / `SNAP_VERSION`
3 不动），所以它是纯增量能力：旧数据目录、接入侧适配层全部零改动。
实测（§6.9）：82 888 节点的目录 dump 279 ms / 11.65 MB、load 265 ms，还原目录的
`state.snap` 与源**逐字节相同**，`dump → restore → dump` 也逐字节相同。

同一轮（未发版）还落地了六格能力，都不改既有帧：写轴 flusher 流水线（`--flush-pipeline`，
**默认关** —— 实测收益为负，§8 第 8 条）、`--max-conn` 与 `--stats --format prometheus`
（§7.14）、通用表九条 `row*` 基准（§7.15）、`RowRange` 区间扫描（§5.1）、`Op::Batch`
原子批（§5.1）、`TableAddColumn` 追加列（§5.1）。`PROTOCOL_VERSION` 仍 6（§4.1 的四条
纯追加先例，最后一例就是 `TableAddColumn`）、`SNAP_VERSION` 仍 3，旧数据目录与接入侧适配层零改动。

同一轮最后一格是 **WAL 归档 + 时间点恢复**（§6.11，`--archive-dir` + `treedb-pitr`）：
checkpoint 在抹掉 `(from, to]` 之前把这一段原始帧与即将被覆盖的快照拷进归档目录
（段名即区间，没有清单文件），恢复工具按 seq 拼「归档段 + 活 WAL」重放到目标点、落成
一个新目录。**仍然不发明协议**：`PROTOCOL_VERSION` 仍 6、`Op` 表一条没加 —— 归档是数据
目录内部的副作用，恢复是一条离线命令。代价是 checkpoint 多了一处 fail-closed 的落盘
（归档失败 = checkpoint 失败），边界见 §6.11。

最后一笔是 **M4：把四组专用控制面 op 从内核删掉**（§11 第 10/17 条，§10 阶段 C 4 的
最后一步）：14 个 `Op` / 9 个 `Reply` / 8 个 `Log` 变体与 `keys`/`audit`/`defs`/`place`
四张表整段删除（tag 区间留空不回收）。**这一轮动了三处字节格式**：
`PROTOCOL_VERSION` 6 → 7（删帧不是纯追加，§4.1）、`SNAP_VERSION` 3 → 4（`refs` 之后
抽掉 24 字节计数段，`SNAP_LEGACY_VERSION` = 3、v2 断链）、dump 容器 `TDBDUMP2` /
版本 2（容器头少一个 `next_audit_seq`）。`treedb-migrate` 只认 v3 → v4，且 v3 那四段
非空就拒（§6.5）；真机彩排（`v0.5.1` → 当前树）12 条 ok / 0 FAIL，尺寸恰好 -24 B、
WAL 与回滚窗口都原样。定版 `v0.6.0`，前面那些「未发版」的格子随这一版一起出。

`v0.6.0` 之后是**批量形状与写扩展**一轮：三个新 op —— `TableClear`（一条记录清空整张表，
名字、id 与形状都不动）、`RowRangeDelete`（按字节区间删一批）、`TableRename`（影子表
顶替，只改名不换 id）—— 加上 `RowPut` 的**同值消除**（写回逐字节相同的值不产生日志）。
**这一轮是纯追加**：只加三个 op tag `0x007c`–`0x007e` 与三个 `Log` tag 25/26/27，不加
reply tag（回 `Count` / `Ok`）、`EngineStats` 一个字段也不长，所以 `PROTOCOL_VERSION` 仍 7、
`SNAP_VERSION` 仍 4、dump 容器仍 `TDBDUMP2` / 版本 2，`v0.6.0` 及更早的数据目录零迁移；
旧服务端收到新 op 在 decode 处得到 `unknown op tag`，旧副本收到 25/26/27 在
`Log::decode_depth` 报 `unknown log tag` —— 两侧都没有静默分支（§4.1 追加判据的第六例，
§5.1）。实测（100 万行表、`dur=group`，§7.16）：同样删完一百万行，逐行 `RowDelete`
16 064 ms / 1 000 000 条记录 / 2 075 次 fsync / 38 MB，`RowRangeDelete` 1 017 ms / 500 条 /
2 次 / 29.5 KB，`TableClear` **924 ms / 1 条 / 1 次 / 25 字节**；同值重写一百万次
**970 ms / 0 条记录 / 0 次 fsync**（真改值同形状是 21 126 ms）。同一轮还第一次量了
**多实例分片的写扩展**（§7.17）：单实例有一个与客户端压力无关的天花板（`dur=never`
下把并发压力从 1 路加到 8 路只拿到 **0.94×**，而服务端 CPU 从 4.30 涨到 8.39 核），
按表切到 8 个进程实例则是 never **6.50×**、group **4.77×** —— 但路由、事务边界、每实例
的备份与副本都在接入方（§8 第 19 条）。定版 `v0.7.0`。

`v0.7.0` 之后是**非认证协议版本闸门**一轮（§4.4）：新增 `VersionBegin` 0x0092 /
`VersionOk` 0x0192，所有正常连接先协商版本，再按需认证；协商完成前服务端拒绝一切
数据 op。既有帧字段没变，但连接建立语义不兼容，所以 `PROTOCOL_VERSION` 7 → 8，
op / reply 计数 44 / 23 → 45 / 24，包版本定版 `v0.8.0`。跨代真二进制探针同时覆盖
认证与不认证两条路径，旧服务端都在 `0x0092` 上被拒。

`v0.8.0` 之后补的是**删除风暴**一轮（§7.18）：量具上给了两套对账脚本 —— 通用表的
`benches/delete_storm.sh`（`rowdelete` 10 万行 + `churn` 4 轮 80 万 op，自动 checkpoint、
RSS/HWM、SIGKILL 尾巴恢复）与 POSIX 的 `benches/posix_delete_storm.sh`（20 万
`Unlink`/`Rmdir`、自动档开关两跑、重开逐 ref 核对）。语义上修了一处真 bug：
`Unlink`/`Rmdir` 删掉 inode 的**最后一个名字**时，原先不清 `locks` 表，而 `LockSet`
的释放入口是先查 inode 的 —— inode 没了，永久锁（`lease_expiry == 0`）就再也无法被
客户端释放，也永远不会被过期 sweep 扫掉。现在这条分支按 `(view, ino)` 前缀级联产生
`Log::DelLock`（`DropView`/`PurgeView` 本来就有级联，这里是补齐），而多链接文件只摘
一个名字时 inode 还在、锁必须保留。**这一轮既不动线协议也不动盘上格式**：没加
op / reply / `Log` 变体，`PROTOCOL_VERSION` 仍 8、`SNAP_VERSION` 仍 4，旧数据目录零迁移。

删除风暴之后是 **grovefs 接入侧的 pin 复核与 `v0.8.1` 发布**（§10 阶段 C 4 末、
§11 第 21 条）：`benches/grovefs_pin_ab.sh` 拿同一份 grovefs 源码只改 `Cargo.toml`
那行 tag，实测 `v0.3.6` → `v0.8.0` 六条端到端套件的三元组逐行相同 —— 升 pin 零源码
改动。复核同时照出 `v0.8.0` 这个 tag 圈不住上面那笔级联清锁（tag 落在 `c455793`
之前），所以把 tag 之后的三笔发成 `v0.8.1`：**依旧没有新 op / reply / `Log` 变体，
`PROTOCOL_VERSION` 仍 8、`SNAP_VERSION` 仍 4、`DUMP_VERSION` 仍 2，旧数据目录零迁移**，
对 grovefs 的线协议面一个字节未动 —— 发它的唯一理由，是让「pin 提到哪」有一个含锁
修复的目标可钉。

**发布标记与 crate 版本**：历史上两者逐版对齐（`v0.4.0` 的 `Cargo.toml` 就是 `0.4.0`）；
`v0.5.1` 是唯一例外 —— 那一笔只改行为、漏了 `version` 字段的同步，而 tag 已先推。
按「不改写已推引用」的纪律保留现状：Cargo 解析 git 依赖只看 tag/rev 与 lock 里的
hash，不看这个字段，所以不影响任何构建；`v0.6.0` 是下一次带 API 变更的发版，
`Cargo.toml` 的 `version` 随它回到 `0.6.0`，两者重新对齐，此后逐版跟齐（`v0.8.0` = `0.8.0`）。

test 现在分两档报（2026-11-25 在本轮交付的树上实跑，含 §7.18 与 §7.19 那批改动）：默认档（纯引擎）`cargo test --release` = **242 passed / 0 failed / 3 ignored**（71 lib +
36 engine_crud + 14 migrate + 21 replication + 12 net_protocol + 11 pitr + 8 read_scaling
+ 7 crash_recovery + 7 delete_storm + 5 mixed_views + 6 batch + 12 table_bulk + 8 auth
+ 5 observe
+ 5 table_alter + 4 dump_restore +
3 backup_online + 3 independence + 1 large_reply + 1 checkpoint_tail + 1 scale_connections
+ 1 scale_restore；另 3 条带 `#[ignore]` —— 万级连接、百万节点、64 MiB 快照收尾 ——
只手动跑，不计入 passed）；`--features postgres-compat` = **273 passed / 0 failed / 9 ignored**，多出的 31 条 = 过渡档自己的 30 条单测 +
`concurrency.rs` 里那条
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
| 1 | 连接轴 | 单进程 ≥10 000 并发连接，且 1 fd/conn | 10 000 连接：RSS +5 MiB、**0.53 KiB/conn**、`fd/conn = 1.0000`；25 000 连接：0.39 KiB/conn（2026-11-19 复跑：+6 MiB、0.56 KiB/conn，见 §7.8）。「万级活跃视图」那一半在客户端侧曾经是线性的（每视图 4 条，100 视图 = 400 条），2026-11-20 提到每进程 4 条，10 000 个驻留 store 实测仍为 4 条（§7.12） | **达成** |
| 2 | 持久性轴 | `kill -9` 后已 ack 的写零丢失 | 6 个崩溃用例全绿（`always`/`group` 两档、6 线程混打中途崩、checkpoint 瞬间崩、连崩 3 轮），另有 `workers=8` 下同语义的一条；覆盖面已扩到通用表（行、索引重推、级联删表，§6.4） | **达成** |
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
- `workers = 1` 根本不走这条路径，走 [`crate::net::server::serve_single`]。但它**不是**
  另一份实现：`Conn`、`Slot`、帧编解码、`apply` 这两档共用同一层构件，所以读并行度
  与 syscall 并行度在 `workers=1` 时就是同一个 1。**因此别把「w=1 那档的代码从未变过」
  当成事实**：本轮三项逐 op 压榨（§7.5 末）全落在共用构件上，两个档位一起抬 ——
  与历史 w=1 基线对比时必须重跑同格 A/B，不能拿旧格子直接减。

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
                                  │  Op/Reply（src/engine/model.rs，45 op / 24 reply）
                                  ▼
                                  src/engine/core.rs   Engine：apply → WAL → 持久化 → 回包
                                  │  src/engine/apply.rs  语义实现（rename/锁/账本/分页/表约束）
                                  │  src/engine/wal.rs    WAL：帧/CRC/预分配/group commit
                                  │  src/engine/mem.rs    State：BTreeMap 状态机 + Log（含表与行）
                                  ▼
                            /data/…/treedb.wal + state.snap
                            （快照换代时：src/bin/migrate.rs 只重写 state.snap，WAL 不动，§6.5）

  只读副本（同一个 `Engine`，另一个进程，§6.6）：
     src/bin/standby.rs   ── WalFetch / SnapFetch ──► 主（pull；主不持任何副本状态）
     src/net/replicate.rs Follower：拉快照 → 追 WAL → gap 重装；tick 在提交线程末
                          └─► 自己的 wal + snap；真写在 Engine::exec 处拿 EROFS

  过渡档（PG 后端，不参与引擎路径）：
     src/pool/shared.rs   实例级共享连接池（search_path 化）
     src/store/{keys,audit,defs}.rs   密钥/审计/定义，PG 主 + SQLite 镜像兜底
     src/persist/mirror.rs            本地 SQLite 镜像
     src/schema.rs                    视图名 ↔ PG schema 编解码（与 grovefs 字节一致）
```

| 文件 | 行 | 职责 |
|------|----|------|
| `src/engine/wal.rs` | 1442 | WAL 帧格式、CRC32、预分配、group commit、扫描/重放、`reset` 与旁路快照收尾用的 `truncate_before`（搬运与扫描都走 1 MiB 缓冲，§8 第 7 条②），外加面向副本的只读交付入口 `deliver`（§6.6）与归档用的定段拷贝 `copy_range` / 整段重读 `read_records`（§6.11） |
| `src/engine/core.rs` | 1874 | `Engine`：op 入口、durability 分档、同步与旁路 checkpoint（`checkpoint_start/poll/wait`）、`Stats`、只读入口 `exec_read`、副本侧的 `install_snapshot`/`replicate_records`、fencing token（`read_fence`/`raise_fence`/`promote`/`admit_epoch`，§6.6 切主）；外加快照探测与迁移（`probe_snapshot`/`migrate_snapshot`，§6.5），以及离线 dump/load 的目录半边（`dump_dir`/`restore_dump`，与 `write_snapshot` 共用同一处落盘，§6.9），再加归档钩子的挂点（`open_with_archive`/`archive_dir`，§6.11）与过期租约的分批回收 `sweep_expired_locks`（§8 第 17 条） |
| `src/engine/archive.rs` | 442 | WAL 归档目录（`Archive`：段/快照的自描述命名、`record` 的顺序与 fail-closed 校验）与时间点恢复（`plan_pitr` 选锚点/拼段/查缺口、`pitr_restore` 落成新目录，§6.11） |
| `src/engine/dump.rs` | 466 | 逻辑 dump 容器：`Dump`/`DumpCounts`、`encode`/`decode`（魔数 + 版本 + 两个 `next_*` + 条数 + 尾部 CRC32）、终态→重建序列的 `state_to_logs`（视图→节点→目录项→…→表→行的三条硬顺序）与文件读写；导出确定性由 `dump → restore → dump` 的字节相等钉住（§6.9） |
| `src/engine/mem.rs` | 1998 | `State`（views/nodes/dentries/locks/refs/tables/rows/row_index）+ `Log`；快照的版本闸门与两代读入口（`load` / `load_for_migration`），以及 v3 → v4 的代际差异（抽掉四张专用控制面表的计数段） |
| `src/engine/apply.rs` | 1594 | 45 个 op 里那 42 个的具体语义：环检测、硬链接引用计数、租约过滤、分页游标、表形状校验与约束、原子批的顺序预演、追加列的补行补索引、批量删的区间展开与整表清空、影子表顶替、同值消除、副本拒写（三条握手 op 不进引擎，§4.4；这里只留一道 `EINVAL` 兜底） |
| `src/engine/model.rs` | 2727 | `Op`/`Reply` 定义与 codec 实现（含 `WalBatch`/`SnapChunk` 尾部的 `epoch`、目录树的 `Link`、通用表的 `RowRange`、`TableAddColumn`、原子批 `Batch`/`Reply::Batch` 及其解码层深度/条数闸门、批量形状的 `TableClear`/`RowRangeDelete`/`TableRename`） |
| `src/engine/sha256.rs` | 365 | 自写的 SHA-256 / HMAC-SHA256 / 常数时间比对 —— 依赖门逼出来的（§11 第 7 条），正确性钉在 FIPS 180-4 与 RFC 4231 的已知向量上（§4.4） |
| `src/net/server.rs` | 986 | `workers=1` 的 epoll 循环、连接生命周期、写回背压、周期任务（含追平 tick、旁路快照的 poll/派工、过期租约清扫）；兼作两条路径共用的 `Conn`/`Slot` 构件 |
| `src/net/multi.rs` | 1386 | `workers>1`：N 条前端 worker + 唯一提交线程（路由闸门、按连接号回程、停机收尾），自动档快照同样只 poll/派工，周期任务里还有过期租约清扫 |
| `src/net/auth.rs` | 623 | 接入认证：`Key`（读密钥 + MAC 派生）与 `Gate`（每连接一份的握手闸门），两条事件循环共用（§4.4） |
| `src/net/replicate.rs` | 729 | 副本侧跟踪器 `Follower`：拉快照 → 追 WAL → gap 重装，连不上主退避重连；拒收被提掉权的旧主（`Error::Fence`）不退出、只抬计数（§6.6） |
| `src/net/mail.rs` | 114 | 两者之间那条能被 `epoll` 唤醒的信箱（`Mutex<VecDeque>` + eventfd） |
| `src/net/client.rs` | 158 | 同步客户端 + pipeline；`connect` / `connect_auth` 先强制协商版本，再按需认证；`connect_raw` / `from_stream` 是只建 TCP 的显式逃生口（§4.4） |
| `src/bin/{server,bench,pgbench,sqlitebench,micro,migrate,standby,demo,dump,load,backup,pitr}.rs` | 5470 | 可执行入口；`pgbench` 是 PG 对照器，`migrate` 是快照升级 工具（§6.5），`standby` 是只读副本兼 `--promote`（§6.6），`dump`/`load` 是离线逻辑导出与重建（§6.9），`backup` 是在线一致快照导出（§6.10），`pitr` 是按 seq 恢复到新目录（§6.11）；三个面向网络的入口共用 `--auth-key-file` |
| `benches/throughput_matrix.sh` | 188 | 五轴吞吐矩阵（§7 的数据来源） |
| `benches/rehearse_migrate.sh` | 159 | 快照升级的真机彩排：旧 tag → 当前树，逐格核四条承诺与回滚窗口（§6.5 的外部效度） |
| `benches/xgen_probe.sh` | 151 | 跨代混跑探针（§4.4 末）：`git archive` 取真 tag 编旧服务端，五臂分别钉 banner、跨代认证 / 非认证拒绝与同代认证 / 非认证放行 |
| `benches/single_conn.sh` | 99 | 单连接与并发档对标（§7.11）：treedb 默认档 vs 私有 PG 集群，`$PIPE` 选一问一答还是批量、`$CONNS`/`$WORKERS` 选形状，逐格附带引擎自计的 fsync 计数 |
| `benches/backup_probe.sh` | 133 | 备份/还原三形状实测（§6.7）：冷拷贝、写入进行中的热拷贝、副本目录当还原点，逐档拿 `--stats` 的数据面四行对拍 |
| `benches/handshake_cost.sh` | 126 | 接入握手的代价（§4.4 末表）：同一轮里 off/on 两臂交替跑 N 条建连，取 bench 自报的 `connected` 时间 + 那一刻的服务端 RSS + 稳态 `ping`/`create`，并校验两臂实际连上的条数一致 |
| `benches/three_way.sh` | 170 | 三臂同口径对照（§7.13）：treedb / PG 12 / SQLite3，24 格；头部七条口径与「不按 pipeline 乘」的计数判据都写在脚本里 |
| `benches/three_way_table.py` | 118 | 由两份原始采集重算 §7.13 引用的 `final.tsv`；`--check` 对账、`--selftest` 自带三处注入错（上一版表是手搓 one-liner，把 p50/p99 拼成了一列） |
| `benches/doc_table_check.py` | 1060 | 把 §7.13 的 markdown 表与 `final.tsv` 逐格对账（24×14），再把 §7.18 的七张表（3 张通用表 + 4 张 POSIX）与 `delete_storm.tsv` / `posix_delete_storm.tsv`、§7.19 的四张矩阵表 + 一张驻留探针表与 `many_views_mix.tsv` / `mix_isolate_auto{0,1}.tsv`、§7.9 的两张持久化表与 `sync_ratio.log` 的两个锚定块、§10 阶段 C 4 补段的 ref 表与套件表与 `grovefs_pin_ab.log` 逐格对账（§7.19 的格子按 `(auto_mb, views, conns)` 三键定位、§7.9 的按块时间戳定位、§10 补段的按 ref 名与「套件名 × 臂」定位，都不按行序），并校固定工作量、WAL/快照水位、账本收敛、RSS 样本、`worst_round_us`、合批比与「改后两档同一提交点」等不变量，再校 §10 补段的六条套件两臂齐备、三元组逐行相等、混代闸门点名行在日志里，列头的臂号与臂上 pin 也照日志的臂头校准而不写死；`--selftest` 用 24 处注入错验证它真的会红 |
| `benches/delete_storm.sh` | 352 | 通用表删除风暴（§7.18）：`rowdelete` N 行与 `churn` 2N×cycles 两档，自动 checkpoint、`VmRSS`/`VmHWM` 采样、停服 `rounds/worst`、SIGKILL 后拿「快照 + 2 条尾巴」重开逐行核对；任一断言失败即非零退出 |
| `benches/posix_delete_storm.sh` | 380 | POSIX 删除风暴（§7.18）：`posixseed` → `posixdelete`（2N op / 6N records / 2N writes）→ `posixverify`（逐 ref 归零），自动档 `0/1` 两跑各核 checkpoint/水位/WAL 形状，并校四本账（nodes/dentries/locks/refs）收敛到基线 |
| `benches/many_views_mix.sh` | 354 | 万级混合视图矩阵（§7.19）：每格 `mvseed` → 优雅停服 → 同目录重开 → `mvmix`（五 op 一组 = 5 records / 2 writes / 净零），`$VIEWS` × `$CONNS` 逐格跑，自动 checkpoint 开关、`VmRSS`/`VmHWM` 采样、停服 `rounds/worst`、落 46 列 TSV；`OUT=`/`TSV=` 把落盘指到别处，供单格隔离探针用而不覆盖正式矩阵；任一断言失败即非零退出 |
| `benches/promote_smoke.sh` | 258 | 切主的活体验证（§6.6「切主」）：真主 → 真副本 → 在线拒写与被锁拒提升 → 停旧主/停副本 → 离线提升 → 新主可写 → 旧主回魂被高令牌副本拒收且副本继续答读；动态分配四个端口、独立日志落盘、任一断言失败即非零退出 |
| `benches/replica_rate.sh` | 359 | 副本传送的三格实测（§6.6 边界 4）：冷启动端到端与传输段（两种 `--fetch-bytes`）、追赶积压、追平时的读侧 p99；每个阶段用独立 standby 进程，好把「最坏一轮」归属到相位 |
| `benches/grovefs_pin_ab.sh` | 135 | grovefs 接入侧的 pin 跨代复核（§10 阶段 C 4 末、§11 第 21 条）：同一个 worktree 只改 `Cargo.toml` 那行 tag，两臂各跑编译 / 仓内测试 / 六条端到端套件，且**每臂配同代引擎二进制**（旧引擎由 `git archive <tag>` 现编并印 `sha256`）、末尾再出混代反证与 tag↔HEAD 的差；跑前要求 `Cargo.toml`/`Cargo.lock` 干净、退出即还原 |
| `benches/replica_rate_xhost.sh` | 459 | 同上的**跨机**版（§6.6 边界 5）：副本放在第二台机上，量冷启动/追赶的真实链路速率与「副本在追时主 p50/p90/p99 + 吞吐」的 A/B；A/B 从同一份种子目录派生、写入量逐字节相同，并先量链路与 fsync 上限做参照 |

行是 `wc -l`，含各文件自己的 `mod tests`（旧版这张表是历史快照，已与代码脱开的
那几个文件本轮按实测回填）。

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
- **帧里没有版本位**（v1 定形时就没给，现在也不能塞 —— 塞进去就把旧客户端的
  整个 payload 偏移挪了两位）。v8 起版本靠每条连接建立时的 `VersionBegin` /
  `VersionOk` 交换，当前 `PROTOCOL_VERSION = 8`；`Stats.proto_version` 只是协商后的
  交叉核对，不是闸门。升级顺序仍是**先升服务端，后升客户端**：窗口期旧客户端主动
  连新服务端会在首个数据 op 上拿到 `EADV`；新客户端连旧服务端时，旧服务端不认
  `0x0092` 后直接关连接，客户端实际看到 `server closed`（§4.4 五臂实测），两边都
  不会静默错读。新客户端解旧服务端的帧会短读报错（能看见），旧客户端解新服务端的
  帧会**静默丢掉尾部字段**（看不见）—— 后者正是 `SNAP_VERSION` 那道闸门在磁盘上防的事。
- 版本历史：`3 → 4`（§6.6）加了两条 op 与两个 reply tag（`WalFetch`/`SnapFetch` →
  `WalBatch` 0x0180 / `SnapChunk` 0x0181），tag 空间仍然只追加、既有帧一个字节未动；
  但 `EngineStats` 尾部多了六个 `replica_*` 字段。这就是上面那句“旧客户端静默丢掉
  尾部字段”的唯一实例：`Rd` 不要求读完（只有 round-trip 用例断言
  `remaining() == 0`），所以 v3 的 `--stats` 拿到 v4 的应答只会少印那六个数，不报错。
  反过来不行：v3 服务端会把 `WalFetch` 的 tag 当未知 op 拒掉，副本就此追不动 ——
  所以副本与主必须同代，而升级顺序照旧（先服务端后客户端）。
  `4 → 5`（§4.4）只加两个 op tag（`AuthBegin` 0x0090 / `AuthFinish` 0x0091）与两个
  reply tag（`Challenge` 0x0190 / `AuthOk` 0x0191），**既有帧一个字节没动、
  `EngineStats` 也没加长** —— 所以这次连“静默丢尾部字段”都不存在：一个 v4 客户端
  只要不去发那两条新 op（它也不知道这两个 tag），它对 v5 服务端的每一格读写都
  与对 v4 服务端逐字相同。开关默认关正是为此（§4.4）。
  `5 → 6`（§6.6「切主」）**不兼容**：`Reply::WalBatch`（0x0180）与 `Reply::SnapChunk`
  （0x0181）尾部各多一个 `u64`（`epoch` / fencing token）。op 与 reply 的条数一个
  没变，但两个**既有** reply 的长短变了 —— 这是第一次把
  “追加字段”用在既有 reply 上，于是 v5 副本解 v6 主会短读报错。这个代价是换来的：
  令牌不对的副本，宁可连不上也不能接。同样的理由让快照格式刻意不动（`SNAP_VERSION`
  仍 3）：令牌能随数据传播但不必被数据携带。
  `6 → 6`（`v0.5.0`，**不升版本**）：只新增一个 op tag `Link`（0x002c），既有帧一个
  字节没动、reply 集合没变。这是「纯追加」判据第一次用在 op 上：旧客户端不会发它，
  旧服务端收到未知 tag 在 `decode` 的兜底那里回 `EINVAL` —— 两边都不会静默走进错误
  分支。所以 `PROTOCOL_VERSION` 留在 6：旧服务端只是不认这条 op，而副本链路
  （`WalFetch`/`SnapFetch`）根本不经过它，主与副本混版不受影响。
  `6 → 6`（第二例，`RowRange` 0x0079）：同样只加一个 op tag，而且**连 reply 都没加**
  —— 它直接复用 `Reply::Rows { rows, end }`（与 `RowList` 同一个应答）。判据与 `Link`
  那次一致：旧客户端不发它，旧服务端回 `EINVAL`，没有静默分支。
  `6 → 6`（第三例，`Op::Batch` 0x007a / `Reply::Batch` 0x01a0）：这一例把「纯追加」
  的另一半也走了一遍 —— 新增 op tag 与新增 reply tag 同时发生。旧客户端不发 `Batch`，
  所以永远收不到 0x01a0；旧服务端收到 0x007a 在 `decode` 兜底回 `EINVAL`。`EngineStats`
  没动、既有帧没动，所以版本留在 6。`6 → 6`（第四例，`TableAddColumn` 0x007b）：与第二例
  同型 —— 只加一个 op tag，**连 reply 都没加**（复用 `Reply::Table(Some(def))`，回的是追加后的
  整张形状）。`6 → 6`（第五例，`Reply::SnapPrefix` 0x0182 / `SnapFetch { len: 0 }` 探针）：
  与第三例同型，但方向反过来 —— 这次**没加 op**，复用 `SnapFetch` 的空读（`len == 0` 原本
  就是个合法的「读 0 字节」请求），只加了一个 reply tag。旧副本从不发 `len == 0`
  （`--fetch-bytes 0` 启动即拒），旧主对空读回的是空 `SnapChunk`（0x0181）而不是 0x0182
  —— 新副本拿到非 `SnapPrefix` 的答复就当「前缀不可留」，退回整份重下。两个方向都没有
  静默分支。五条先例合起来把「什么时候必须升版本」收窄成一句：
  **只有既有帧的字段顺序/长度变了、`EngineStats` 长度变了，或者连接建立/握手的
  前置语义变了，才必须 +1**；单纯新增 op / reply tag 本身不是理由。
  `6 → 7`（`v0.6.0`，§11 第 10 条 M4）**不兼容**：把四组专用控制面 op
  （`Key*`/`Audit*`/`Def*`/`Place*`，共 14 个 Op tag 0x0040–0x0062 与 9 个 Reply tag
  0x0140–0x0161）整段删除，同时 `EngineStats` 少掉中间两个 `u64`（`keys` / `audit`）。
  删帧不是「纯追加」，位置又在中段，所以两边都不能靠「读不完就算了」：v6 客户端发
  `KeyList` 会在 `decode` 撞上 `unknown op tag`，v6 的 `--stats` 读 v7 的应答则会把
  后面的字段整体错位。抬版本让两边都在握手时就 fail-closed。tag 不回收 —— 那段区间
  留给「已删除」而不是「下次复用」（`removed_control_plane_tags_are_unknown`）。
  `7 → 7`（`v0.7.0`，§5.1 的批量形状）**不升版本**：一次加三个 op tag
  （`TableClear` 0x007c / `RowRangeDelete` 0x007d / `TableRename` 0x007e），而且
  **一个 reply tag 都没加**（前两条回 `Reply::Count(n)`，第三条回 `Reply::Ok`，与
  `TableDrop` 同形）、**`EngineStats` 一个字段没加**。判据与第二、四例同型：旧客户端
  不发这三条，旧服务端收到新 tag 在 `decode` 兜底那里回 `EINVAL`，两边都没有静默分支。
  `Log` 侧多的三个 tag（25/26/27，§5.1）只在「服务端自己写的 WAL」与「主↔副本传送」
  里出现，而副本与主必须同代本来就是这条硬规则（上面 `3 → 4` 那一段）；旧主对新副本
  发来的 25/26/27 在 `Log::decode_depth` 的兜底那里报 `unknown log tag`，是可见的错。
  `SNAP_VERSION` 仍 4（快照存的就是 `tables` + `rows`，`row_index` 载入时重建 —— 改名与
  清空被现有格式覆盖）、`DUMP_MAGIC`/`DUMP_VERSION` 仍 2（dump 存终态）。
  同一轮还有一处**不改格式、只改计数**的优化（`RowPut` 同值消除，§5.1）：它让
  `ops_total − writes_total` 这个既有差值成了「被消除的写」的读数，而 `EngineStats` 的
  长度没动 —— 所以按上面那句判据，它同样不构成抬版本的理由。
  `7 → 8`（`v0.8.0`，§4.4）**不兼容**：新增 `VersionBegin` 0x0092 /
  `VersionOk` 0x0192，并把版本协商设为所有正常连接的强制第一步。既有帧字段与
  `EngineStats` 都没变，但旧客户端不会协商、新服务端若放行它就会重新打开跨代静默
  错读；新客户端连旧服务端也会在未知 tag 上失败。连接建立语义本身就是升级判据，
  因此协议与包版本一起抬到 8 / `v0.8.0`。

### 4.2 op 分组（45 个）

| 组 | op |
|----|----|
| 运维 | `Ping` `Stats` `Checkpoint` `Shutdown` |
| 日志传送 | `WalFetch` `SnapFetch` |
| 版本协商 | `VersionBegin` |
| 接入认证 | `AuthBegin` `AuthFinish` |
| 视图 | `CreateView` `DropView` `ListViews` |
| 名字/属性 | `Lookup` `Getattr` `Setattr` `ReadDir` `ReadDirPlus` |
| 增删 | `Create` `Mkdir` `Symlink` `Link` `Readlink` `Unlink` `Rmdir` `Rename` |
| 数据账本 | `RefLookup` `RefUp` `RefDown` |
| 锁 | `LockQuery` `LockSet` |
| 通用表 | `TableCreate` `TableDrop` `TableGet` `TableList` `TableAddColumn` `TableClear` `TableRename` `RowPut` `RowGet` `RowList` `RowRange` `RowDelete` `RowRangeDelete` `RowCount` |
| 原子批 | `Batch` |

45 个 op 分**三档**，两个布尔判据（`is_readonly` / `is_control`）恰好切出三块，
两两不交、并起来是全集：

- **只读 15 个 = worker 可就地执行集**：`Ping` `ListViews` `Lookup` `Getattr`
  `Readlink` `ReadDir` `ReadDirPlus` `RefLookup` `LockQuery` `TableGet` `TableList` `RowGet`
  `RowList` `RowRange` `RowCount`；
- **真写 22 个**：`!is_readonly() && !is_control()`，全部要过 WAL；
- **控制面 8 个**：`Stats` `Checkpoint` `Shutdown` `WalFetch` `SnapFetch` `VersionBegin`
  `AuthBegin` `AuthFinish`。

前两档是语义分档，第三档是**执行位置**分档：控制面既不改状态也不产出 logs，
但它必须在提交线程上跑 —— `Stats` 要读只有提交线程碰得到的计数器，
`Checkpoint`/`Shutdown` 要动 WAL，而 `WalFetch`/`SnapFetch` 要的是**磁盘上的日志与
快照**，`translate(&State, ..)` 根本拿不到它们需要的东西。三条握手 op 是个例外，
它归进控制面的理由是「它不是数据面 op」而不是「它要在提交线程上跑」—— 实际执行
位置在事件循环，比提交线程还靠前一层（§4.4）；把它归进只读档会更贴近「不碰状态」
这句字面意思，但那会让 worker 以为可以在自己手里就地算它，而它必须**串行于同连接
的状态机进度**（`Gate` 是每连接一份的可变状态）。三档的现值由
`tests/read_scaling.rs::ops_partition_into_three_classes` 钉住（15/22/8）。

`Batch` 在只读那一档里是个**递归判据**：`ops.iter().all(|o| o.is_readonly())`。
所以同一个 tag 既可能是只读（纯读批，走 worker、副本放行）也可能是真写（含一条写就
整批进提交线程）。语料里放的是含写那一条，它落在「真写」档；纯读批的可见性由
`tests/batch.rs::a_read_only_batch_costs_no_log_and_answers_like_separate_calls` 验。

- 只读那一档的划分不只是优化提示，**它是语义地基**：`Op::is_readonly()` 判错一条，
  就是把一次应当被确认的写丢在 worker 手里。`readonly_classification_matches_translate`
  拿 45 个 op 的语料逐条跑 `translate`，断言「判为只读 ⇒ logs 必空」，同时钉住
  45/15/30 这三个数 —— 加 op、改分派表，这里和测试必须一起动。语料里每条只读 op 都要
  真查到东西（`seed` 因此也建表灌行），否则「不产 logs」这句只在测「查不存在的表
  不产 logs」那种废话。
- `Stats` 刻意**不在**只读集里：它不写 WAL，但它读的是 `Engine` 手里的计数器，而那些
  计数器只有提交线程碰得到。worker 就地算的 op 数走一个 `AtomicU64` 折回
  `ops_total`（`Engine::attach_external_ops`），否则运维看到的 QPS 会少一大截
  （`stats_counts_worker_reads`）。
- 副本拒写用的也是这条分档（§6.6）：判据写成 `!is_readonly() && !is_control()`，
  所以 `Stats`/`Checkpoint` 在只读副本上仍然可用 —— 一个连自己状态都问不出来的
  副本没法监控，不让它 checkpoint 就等于让它拖着一个与主同大的日志。

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

背压是电平触发的另一面。一条不读的连接会让应答堆到 `MAX_BACKLOG`（4 MiB）；此时若
`EPOLLIN` 还挂着，`epoll_wait` 每轮立刻把同一个 fd 再报回来 —— 读又不读、写又写不动，
worker 就空转了。现在的做法是**摘掉 `EPOLLIN`**（`Conn::hold_read`），等 `flush` 把积压
排到半水位以下（`MAX_BACKLOG / 2`）再挂回去；`EPOLLHUP`/`EPOLLERR` 不受事件掩码影响，
所以对端死了照样能被发现。判据取可观测的 CPU 秒数而不是「某个函数没被调到」：
`backpressure_does_not_spin_the_worker` 先用大应答把 backlog 顶满，再把 8 000 条小请求
压在接收队列里，然后采样 0.6 s 墙钟烧掉多少 CPU —— 摘掩码之前是 0.600 s，之后只剩
epoll 定时唤醒的零头（门限 0.2 s）。

### 4.4 协议版本闸门与接入认证（v5 起认证，v8 起版本必协商）

**版本协商先于认证且不依赖认证**。`Client::connect` / `connect_auth` 与
`from_stream_auth` 都先发 `VersionBegin`；错版本或旧服务端不认 `0x0092` 都直接让
建连失败。服务端的 `Gate` 从 `VersionNeeded` 起，只有 `VersionOk` 之后才进入
`Off`（不认证）或 `Needed`（要认证），数据面只允许 `Off | Ready`。`connect_raw` /
`from_stream` 是给反例测试与跨代探针的显式未协商入口，不作为正常客户端 API。

下面的一次共享密钥双向挑战握手（CHAP）跑在版本协商之后。**它不是 TLS 的
简化版**，它只建立身份，不建立机密性（下面「防不住什么」四条是这个节的全部
边界）。实现一侧在 `src/net/auth.rs`，原语（自写的 SHA-256 / HMAC-SHA256）在
`src/engine/sha256.rs` —— 自写不是选择，是依赖门逼出来的：`[dependencies]` 非 optional
项只允许 `libc` + `thiserror`（§11 第 7 条），所以 `ring`/`rustls` 这类东西进不来，
于是正确性只能拿 FIPS 180-4 / RFC 4231 的已知向量钉住。

```
客户端  ── VersionBegin { client_proto } ──────────►   tag 0x0092
        ◄── VersionOk { server_proto } ─────────────   tag 0x0192
客户端  ── AuthBegin { client_nonce } ────────────►   tag 0x0090
        ◄── Challenge { server_nonce, proto } ─────   tag 0x0190   服务端现取一个 server_nonce
客户端  ── AuthFinish { mac } ───────────────────►   tag 0x0091   客户端算 HMAC(key, cn‖sn)
        ◄── AuthOk { server_mac, authed } ────────   tag 0x0191   服务端交出对 sn‖cn 的签名
```

- **密钥从不上线**，两个方向各自交出「我知道密钥」的证明。MAC 输入是
  `ctx ‖ PROTOCOL_VERSION_LE ‖ a ‖ b`：客户端方向 `ctx = "treedb-auth-v1-cli"`、
  `a = client_nonce, b = server_nonce`；服务端方向 `ctx = "treedb-auth-v1-srv"`、
  **两个随机数顺序相反**。两条域分隔串不同 + 顺序相反，一份证明搬不到另一个
  方向去用（`macs_are_directional_and_nonce_bound`）。版本绑进 MAC 是免费的，
  但不做会留坑：同一份密钥在两个协议版本之间不应当能互相顶替。
- **重放失效**：每次 `AuthBegin` 都现取一个新的 `server_nonce`，所以抓下来的
  `AuthFinish` 换一条连接、甚至同一条连接重拿一次挑战，都必然算不中。钉它的
  用例是 `wrong_key_is_rejected_and_a_replay_does_not_work`：连「用对密钥但算给
  旧挑战」的 MAC 也不能过。
- **错 MAC 不关连接也不锁死**：回到「还没握手」那一步，可以换个密钥重头再来
  （运维换密钥时不需要重连）。但这一拍的挑战值作废。
- **双向**：`AuthOk { server_mac, authed }` 里带回服务端对 `sn‖cn` 的签名，客户端
  验不过就报错。不然一台假服务端可以只靠明文的 `server_nonce` 骗客户端先把
  自己的证明交出去。
- **随机数取不到就当服务坏了**：`getrandom(2)` 失败回 `EAGAIN`，既不静默放行，
  也不拿时间戳、fd 号这种可猜值当挑战。
- **密钥文件只认两种形状**（`Key::load`）：32 原始字节，或 64 个十六进制字符。
  尾随换行只在十六进制那一档容忍；原始字节那一档**一个字节都不剔** —— 随机密钥
  的末尾字节有 1/64 的概率正好是个 ASCII 空白，先剔空白再比长度就等于每隔一阵
  子有一把合法密钥报「长度不对」（本轮那 20 000 条建连的跑批就是这么踩上的）。
  全零与全空白都拒（多半是没填的模板）。`hunter2` 补零到 32 字节这种
  形状直接 `Config` 错退出 —— 不这么做就是把「不防字典攻击」从「不付成本」
  变成「真能被人离线爆破」。
- **`Debug` 只印长度**：`Key` 会跟着 `ServerConfig` 进 panic 文案与日志，把字节
  带出去等于把密钥写进运维日志。

**它落在哪一层**：握手**不进引擎**。两条事件循环（`workers = 1` 的 `server.rs`、
`workers > 1` 的 `multi.rs`）在把 op 交给路由/提交线程之前就地答完，共用同一个
`Gate`；`apply.rs` 里只留一条大声的 `EINVAL "handshake op reached the engine"` 做
兜底报警 —— 它正常永远不该被执行，被执行的代价是测试里立刻可见而不是默默错答。
这个落点不是为了整洁：写进引擎就要占提交线程，而提交线程是写路径的唯一串行点
（§8 第 1 条第 ③ 项）—— 一个「每条连接都要跑一次」的东西挤进去，等于给
写扩展加一个与连接数成正比的项。它也直接决定了下面那个代价表里「稳态两行应当同
量级」这条判据。

**默认关的是认证，不是版本闸门**：没配密钥时，版本协商成功后数据面直通；只有
客户端显式发 `AuthBegin` 才回一次 `AuthOk { authed: false }`。v5–v7 的「不认证就不发
任何新字节」兼容承诺到 v8 为止：所有正常连接现在都多一次版本往返。带了
密钥的客户端收到 `authed: false` 一律报错退出，**不静默降级**；`Challenge.proto`
与本端不一致也报错。

**跨代真二进制探针**（`benches/xgen_probe.sh`，默认基线 `v0.7.0`）。一个二进制里
两侧必然同代，所以单测看不到混代；探针用 `git archive` 编出旧服务端，再拿当前客户端
跑五臂：banner 必须旧 7 / 新 8；跨代认证与跨代非认证都必须在 `0x0092` 上 rc≠0；
同代认证与同代非认证都必须 rc=0 且读到 `protocol = 8`。**2026-11-25 04:47:47 实跑**
（`benches/results/xgen_probe.log`）：banner 为旧 7 / 新 8；跨代两臂均 `rc=1`，客户端
看到 `stats: io: server closed`（旧服务端不认未知 tag 后直接关连接），旧服务端
shutdown 计数均为 `ops 0 writes 0`；同代两臂均 `rc=0` 且 `protocol = 8`。脚本任何
一臂失败都会以非零退出码结束。

两条运维结论：① 升代仍须服务端与接入方**成对**升级，`treedb-migrate` 只管目录
不管线；② 不再需要靠「开认证」换版本闸门，非认证连接也在第一次往返 fail-closed。
`Stats.proto_version` 仍只是交叉核对，不能替代结构体之外的握手判据。

**多 worker 下多一条次序规矩**（它是 §4.3 那条保序的直接推论）：一批帧里只要
夹着握手 op，就不能「握手就地答 + 数据转路由」—— 两条出口的回话次序对不上
请求次序（协议里没有 request id）。所以切点定在**最后一条握手 op 之后**：切点
之前一律就地答：握手按 `Gate` 正常答或拒；已协商但夹在最后一条握手 op 之前的
数据 op 回 `EAGAIN`（还没被碰过，重发即可），仍处于 `VersionNeeded` 的数据 op 回
`EADV`。切点之后要么整批交路由（已放行）要么整批回 `EADV`（未放行）。两个文案都在
`Gate` 上共用，两条事件循环不得各写一份。这段代码里最阴的一个坑是 `continue`：
就地答完就跳下一轮会漏掉 EPOLLOUT 登记与 `mark_dirty`，那一帧答案永远送不出去。用例：
`a_straddled_batch_keeps_the_reply_order`、`data_ops_need_a_handshake_on_both_worker_shapes`。

**副本这一侧**：`treedb-standby --auth-key-file` 一个 flag 管两头 —— 对主发起握手，
并且要求连上来的读者也握手。建连失败与握手失败同一档处理（退避重试，日志
`unreachable` / `unusable` 分开），否则一次密钥轮换能把副本变成无限重连风暴。
用例：`a_replica_hands_shakes_with_an_authenticated_primary`。

**代价口径**：下面这张表是 **v0.7.0 历史读数**（`benches/handshake_cost.sh 20000`，
release，loopback，同轮两臂 A/B、独立跑 3 次，逐格原始输出
`benches/results/handshake_cost.log`）。v0.8.0 所有正常连接都新增一次版本往返；
**本轮尚未实跑 v0.8.0 建连/RSS/稳态代价，不得把下表当成当前版本数据**。

| 量 | 认证关 | 认证开 | 差 |
|----|--------|--------|----|
| 建 20 000 条连接（ms） | 8 712 / 6 840 / 7 001 | 8 944 / 8 658 / 8 886 | +232 / +1 818 / +1 885 ⇒ 中位 **+90.9 µs/conn** |
| 服务端 RSS/conn | 439 / 439 / 439 B | 535 / 532 / 532 B | **+93 ~ +96 B/conn**（总 +1 824 ~ 1 888 KiB） |
| 稳态 `ping` ops/s | 1 074 381 / 1 147 239 / 1 158 602 | 1 163 503 / 1 093 574 / 1 148 689 | 中位 1 147 239 → 1 148 689（**+0.1 %**） |
| 稳态 `create` ops/s | 69 018 / 67 123 / 67 414 | 64 122 / 63 796 / 66 652 | 中位 67 414 → 64 122（−4.9 %） |

三句话读这张表：

- **建连那一列才是握手的代价**，每条连接多两次往返 + 两次 HMAC。但这一列的
  噪声比效应还大：`off` 臂自己三次就是 8 712 / 6 840 / 7 001 ms（摆幅 27 %），
  所以**任何单次读数都不成立**，只能报中位与区间；这也正是脚本必须两臂同轮
  交替、并且把 bench 自报的 `connected N conns in M ms` 与 `opened` 计数都校验
  一遍的原因 —— 分母不同时这个差毫无意义。
- **RSS 那一列是固定布局的开销**：每连接 `Conn.auth`（一个 `Gate` + 两个 nonce）
  握手做完也不还回来。它极可复现（`off` 三次全是 439 B），所以要靠连接数放大
  才量得出 —— 折到单连接就是 +96 B / +21 %，放到万级连接上是 +0.9 MiB 量级。
  说不上免费，但也不是会让人改架构的数字（§7.6 那一节的绝对值比它大得多）。
- **稳态两列与认证无关**（握手不进引擎、不占提交线程，所以 group commit 的
  合并度一字不变），`create` 那 −4.9 % 落在同一臂自身的散布里（`off` 臂三次
  差 2.8 %、`on` 臂三次差 4.5 %），因此**不能宣称回归也不能宣称变快**，只能写
  「与噪声同量级」。要判数据面有没有被这轮改动碰到，用的是两臂都不开认证的
  受控 A/B：`benches/ab_layout.sh v0.3.5` → `benches/results/ab_layout_v0.3.6.log`。
  八格（`readdir`/`lookup` × warm 500/16000 × `workers` 1/8）里**没有一格变慢超过
  1 %**（最差一格 −0.90 %），其余是 +0.0 ~ +6.0 % 的向上抖动（最大那格
  `warm=500 w=8` 的 `readdir` 803 203 → 851 094）—— 方向与量级都是单轮 3 s 采样
  的本底噪声，没有一个数字可以被当成“认证改动让读变快了”的证据。

不开认证时认证代码不参与身份校验，但版本协商仍然执行：`Gate::new()` 从
`VersionNeeded` 起，只有同代 `VersionOk` 才转 `Off`；热路径只判 `allows_data()`。

**它防不住什么**（写清楚而不是等使用者自己发现；与 `src/net/auth.rs` 模块头同源）：

1. **不防窃听**。握手之后的每一个字节仍是明文，CRC32 只判损坏。抓包的人拿不到
   密钥，但拿得到全部数据，也随时能 RST 掉这条连接。
2. **不做授权**。过了握手的连接与今天一样拥有全部 45 个 op，包括 `Shutdown`。
   它是「一把钥匙开整扇门」，不是租户模型 —— 多租户、配额、审计归属仍在引擎
   之外（§8 第 13 条）。
3. **不防字典攻击**。密钥必须是 32 字节随机值；从口令派生的话，抓一次握手就能
   离线试口令（所以 `Key::load` 在长度上不留余地）。
4. **不证明「是当前那个主」**。它只证明对端持有密钥；脑裂后旧主同样持有密钥，
   照样能应答握手。把两者分开看：**副本侧**现在有了 fencing token（§6.6「切
   主」），令牌更小的对端一律拒收；**客户端侧**没有 —— 客户端不会去问“你是第几
   任主”，一个已被换掉的旧主照旧服务它自己的读者/写者。所以切主仍然必须真停旧主。

---

## 5. 数据模型与 POSIX 语义边界

### 5.1 状态

`State` 是 8 张内存表：`views` / `nodes`（ino 为中心）/ `dentries`（parent+name → ino）/
`locks` / `refs` / `tables` / `rows` / `row_index`。
`data_ref` 是一串**不透明字节**（`DataRef = Vec<u8>`，空串 = 没有内容），引擎不存数据块
也不解释它；`refs` 表只记「持有者数 + 字节总量」，不变量是 `bytes == Σ 持有者 size`，
GC 拿它当唯一依据。
（这里曾经是 `u64` 对象 id。接 grovefs 前改掉了：grovefs 侧的键是 `1:4096:dev0`
这种字符串（PG 侧列类型 `TEXT`），`u64` 要接它只有两条路 —— 哈希（碰撞 = 两个文件的
字节互相顶掉）或在 provider 里养一张 `String ↔ u64` 映射表（表本身得持久化，崩溃后
查不到映射就读不到数据）。字节作键零碰撞、零映射表，且与 PG 后端同形。磁盘格式跟
着变了，所以 `SNAP_VERSION` 1 → 2，旧快照会被版本号闸门拒收而不是静默解错。
顺带记下另一面：v2 → v3 那次升版本只动了尾部（多了表与行三段），正文逐字节一致，
所以那一代是**可迁移**的 —— 工具与它的四条承诺见 §6.5。）

**通用表（§11 T2）**。引擎一度有四张照搬某个客户端表形的专用表
（`keys`/`audit`/`defs`/`place`），§11 第 10 条 M4 已把它们的 op、`Log` 变体与快照
段整段删掉，现在只剩这组通用表：`tables` 存形状（名字 + 列定义），
`rows` 是 `表 id → (主键字节 → 列值字节)` 两层表，`row_index` 是
`(表, 列, 值, 主键)` 一张扁平二级索引。四条定型：

- **列没有类型**。值全是不透明字节，比较只有字节相等，排序只有主键字节序。类型一进
  内核，客户端的语义就会顺着类型名漏进来，而引擎没法替所有人定义「什么是一个合法
  的过期时间」。
- **`row_index` 不进 WAL 也不进快照**，与 `Dent::kind`（§7.10）同一个取舍（冗余可推字段不动
  磁盘格式）：它完全由 `rows` + def 推出。代价是把「重开后索引必须被重建得一模一样」
  这条承诺压给了恢复路径，所以它有两个专门的断言顶着：崩溃重开后重复唯一值仍
  `EEXIST`（`control_plane_survives_sigkill`），checkpoint + 重开后整个 `State`
  逐字段相等（`generic_tables_keep_rows_and_rebuild_indexes`、`table_state_survives_checkpoint_and_reopen`）。
- **两层 `rows` + 分层的索引**。与 `dentries` 同一套理由：`Vec<u8>: Borrow<[u8]>` 只在
  内层那一维成立，分两层后主键查不用先复制一个 `Vec`；而索引必须扁平，因为等值列举要
  的就是「`(表,列,值)` 前缀下按主键有序的一段」，一层 `range` 直接命中。
- **id 在 `translate` 里分配，不在 `State::apply` 里**（与 view id 同构）：应答与日志必须
  拿到同一个 id，而 translate 对状态只读 —— 那正是单写者流水线能成立的前提。id 单调
  不回收：`TableDrop` 后重建同名表得到新 id，旧行不可能被误认成新表的数据。

**区间扫描（`RowRange` 0x0079）**。`RowList` 是「等值一点」，`RowRange` 是「左闭右开
的一段」，两者共用 `Reply::Rows { rows, end }` 与同一个页宽规则（`limit = 0` → 1000）：

- **`[lo, hi)` 而不是闭区间**。翻页游标天然是「不含上一页最后一条」，与 `row_page` /
  `index_page` 里的 `Excluded` 同形；做成闭区间的话，要么下一页重复末条，要么得在
  客户端造一个「字节序上紧跟在 x 之后的键」—— 而字节串没有通用 successor。
- **空串 = 该侧无界**，不另设 flag：主键不得为空（`ROW_KEY_MAX` 校验链），所以「空」
  只有「无界」这一个意思。`lo > hi` 是空区间而不是错误 —— 客户端拼出来的窗口可能
  本来就没有，为此回 `EINVAL` 只会逼它去解析错误码做分支。同样地，游标越过上界
  也是空页。这两条都在 `BTreeMap::range` 的 panic 条件上（`start > end`，以及
  `start == end` 且两端都不含），所以守卫必须在 `mem.rs` 里，不能交给调用方去记。
- **`column == ROW_PK` 时区间落在主键上；否则必须是已建索引的列**，区间落在**列值**
  上、同一个值内仍按主键序（`bad_column` 与 `RowList` 同一条拒绝链，不静默扫全表）。
  索引侧的上界不能用 `Excluded((tid, col, hi, ""))`：那只是「值为 `hi` 的最小键」，
  同值的其它行全部排在它之后，一个也挡不住 —— 所以上界是一条 `take_while:
  k.2 < hi`，整段值一起排除。
- **游标是一对 `(after_val, after_key)`**。主键区间只要主键；索引列的值可能跨页，
  只给主键回不到「这一页停在哪一段」。`after_key` 空 = 从区间头开始（主键非空，
  所以不会与「游标恰好是空键」撞车）。
- **只按字节序**：没有 collation、没有数值序。要按数字排就自己定宽零填充 ——
  `benches/bench_rows.sh` 的主键就是 `r%06d`，这也正好让区间窗口的宽度可预测。

**原子批（`Op::Batch` 0x007a / `Reply::Batch` 0x01a0）**。把多条 op 收进
一次提交，四条语义：

- **顺序可见**：子 op 按数组顺序执行，后一条看得到前一条的写（`create` 完直接
  `setattr` 它刚分配的 ino）。实现方式是把前面那几条的 `Log` `apply` 进一份
  `State` **副本**，后面的子 op 对着副本翻译。这是它相对 pipeline 的全部增量：
  pipeline 只摊往返与 fsync（§4.3/§7.9），既不提供跨 op 可见性，也不提供原子性。
- **失败即整批不落**：任一子 op 回 `Reply::Err` 就整批回那条的 errno，`msg` 前缀
  `batch op #<i>: ` 指认下标；**一条 `Log` 都不产**，真状态一个字段没动过。这条能
  成立全靠预演落在副本上 —— 副本是局部变量，失败直接丢弃，所以这条路径**不需要
  undo 日志**，也就没有「回滚漏了一个字段」这类静默腐蚀。
- **崩溃原子**：整批的 `Log` 装进**一条** WAL 记录（`Log::Batch` 容器，`LOG_BATCH`
  = 23），一条记录一个 CRC。§6.2 的坏尾丢弃以**记录**为单位，所以崩溃切片上只可能
  「整批在」或「整批不在」。这条比单 op 的多条 `Log` 更强 —— 后者在同一个 fsync 里
  只是**运行期**原子，崩溃时仍可能只落前几条（§6.1 的那条注）。
- **可见性也是整批的**：容器的 `apply` 在写锁内一次完成，读者要么看不到整批、要么
  看到整批；不存在「批内一半可见」的窗口。

边界（都是刻意的）：

- **最多 64 条**（`MAX_BATCH_OPS`）。上界不是为了协议装得下，而是含写的批要在写锁里
  拷一份 `State` —— 批越宽、状态越大，这次拷贝与锁的持有时间越长（纯读批不拷，
  没有 `Log` 可 apply）。更大的批量该由调用方自己分轮：它要的是原子性，不是吞吐。
- **子 op 不许是控制面**（`is_control`：`Stats`/`Checkpoint`/`Shutdown`/`WalFetch`/
  `SnapFetch`/`VersionBegin`/`AuthBegin`/`AuthFinish`）。它们要的不是 `State`，而 `Checkpoint`
  会在批中间把 WAL 截掉 —— 原子性直接没了。
- **不许嵌套**（子 op 不许再是 `Batch`），空批拒。嵌套与条数上界在**解码层**就拦
  （解码递归爆栈与 `Vec::with_capacity` 的内存放大是崩溃/DoS 面，不是 errno 面）；
  `translate` 再拦一道，给正常客户端一个可读的 `EINVAL`。
- **只读判定是递归的**：全只读的批判只读，照旧吃 worker 并行与副本放行；一条写就
  把整批判进提交线程。这也是三档分类里 `Batch` 归「真写」而不是单独一档的理由
  （§4.2）。
- **不是 MVCC，也不是交互式事务**：没有 `BEGIN`/`COMMIT`、没有跨批的快照读、没有
  并发写冲突检测 —— 批内两条写同一个键就按顺序后写覆盖（这是事务该有的样子），
  批之间仍然是一条单写者流水线上的全序。

**追加列（`TableAddColumn` 0x007b）**。`TableCreate` 定下形状之后，唯一允许的形状变化
是「往末尾加一列」：不能改名、不能删列、不能改已有列的标志（那三样都会改行宽或让
已建的索引失效）。四条语义：

- **只能追加到末尾**。旧行的宽度比新 def 窄，`apply` 把它们 `resize` 到新宽度、补出
  **空字节**。不承诺区分「补出来的空」与「客户端显式写的空」：空串本来就是合法列值
  （§5.2 唯一约束那条），要区分就得给每列加一个 NULL 位，那等于给表的磁盘格式加
  第二维。
- **`COL_INDEX` 允许，`COL_UNIQUE` 一律 `EINVAL`**。前者要的只是把补出来的值也进索引，
  同一个 `apply` 里就能做完；后者要有值才能回填 —— 非空表一加就撞唯一约束，所以
  不提供「先加列再回填」这种两步路径（那两步之间的状态对外可见）。列表上限仍是
  `MAX_COLUMNS`（64）。
- **一条日志、幂等、崩溃原子**：`Log::AddColumn { id, def }` 记的是**追加后的整张 def**，
  不是「新增那一列」。重放时按「新旧列数差」补行补索引，差 0 就直接返回 —— 同一条
  记录 `apply` 两次与一次等价。若只记新列，重放就得分「这条是不是已经落过」，
  而日志里没有那个信息。由于整次变更只有这一条记录，§6.2 的坏尾丢弃以记录为单位，
  所以崩溃切片上要么是旧宽度、要么是新宽度。
- **快照与 dump 格式都不动**：快照仍 `SNAP_VERSION` 4（它存 def + 行本身，载入时
  `index_row` 按新 def 自然重建新增列的索引）；dump 存的是终态，表定义已经是新形状，
  不需要新 tag。

拒绝面：表不存在 `ENOENT`；列名空 / 超 `TABLE_NAME_MAX` / 与已有列重名、已达 64 列、
`COL_UNIQUE` —— 都是 `EINVAL`。

**批量形状（`TableClear` 0x007c / `RowRangeDelete` 0x007d / `TableRename` 0x007e）**。
三条都是为同一个负载形状写的：某些表被随时整表替换或成段删除，而逐行
`RowDelete` 顶十万次就是十万条 WAL 记录、十万次占提交线程。实测见 §7.16（一张
百万行的表上，逐行删与一条 `TableClear` 是 1 000 000 : 1 条记录）。五条语义：

- **一条日志记的是区间，不是键清单**。`Log::DelRange { table, column, lo, hi, max }`
  与请求同形，`Log::ClearTable { table }` 只记一个 id。重放时对着当时的状态用同一个
  `range_keys` 再算一遍 —— 写是全局单序的（§2.3 A′），翻译这条日志时左侧的状态与重放
  它时的那份是同一份，于是算出的必是同一批键。记键清单会让 WAL 体积正比于命中数，
  而那正是要消掉的东西：`a_range_delete_does_not_grow_with_the_number_of_hits` 在**编码层**
  钉这件事 —— 一条 `Log::DelRange` 的 payload 必须 < 40 B（与区间里命中几行无关），而
  一千条 `Log::DelRow` 的总字节必须是它的 500 倍以上。拿字节而不是拿时间来断，是因为
  时间受盘与缓存影响，而「记录宽度与命中数无关」是格式自己的性质。
- **原子性 = 一条记录**。§6.2 的坏尾丢弃以记录为单位，所以崩溃切片上「整段在」或
  「整段不在」，不需要 undo 日志（`table_clear_is_one_record_and_a_torn_tail_keeps_the_rows`）。
- **`max` 是删多少的上界**，`0` = 用 `MAX_TABLE_ITEMS`，与 `RowRange` 的 `limit` 同口径。
  `max` 进了日志，所以「删了几行」这件事重放后仍然相同。空区间不产日志（回
  `Reply::Count(0)`，`writes_total` 不动）；`lo > hi` 是空区间而不是错误，与 `RowRange`
  那条一致（守卫在 `mem.rs` 里，因为 `BTreeMap::range` 在 `start > end` 上是 panic）。
  列的规则也沿用 `RowRange`：`ROW_PK` 扫主键，否则必须是已建索引的列（`bad_column`
  同一条拒绝链，不静默扫全表）。
- **`TableClear` 保留形状与 id**：只拆行与二级索引，def 不动、`table_by_id` 不动，清完
  的表仍在 `TableList` 里。「清空一张表」与「删了再建」不是一件事 —— 后者换 id（上面
  那条「id 单调不回收」的定型），旧行不可能被误认成新表的数据。回 `Reply::Count(n)`，
  n = 被清掉的行数；空表回 0 且零日志。
- **`TableRename` 只改名字，不改 id**：`Log::RenameTable { id, from, to }` 记的是 id，
  幂等判据是「`from` 现在是否仍指向这个 id」—— 重放第二遍时 `from` 已经不在表名空间里，
  直接返回。顶到一个已存在的名字（不带 `RENAME_NOREPLACE`）就是 POSIX `rename` 的覆盖
  语义：被顶替那张表的行与索引**级联拆掉**、`table_by_id` 同步摘除，不许留孤儿表
  （`table_rename_keeps_the_id_and_cascades_the_replaced_table` 逐张数 `tables`/`rows`/
  `row_index` 的条目数）。空名 `EINVAL`、超 `TABLE_NAME_MAX` `ENAMETOOLONG`、`NOREPLACE`
  撞名 `EEXIST`、`from == to` 回 `Ok` 且零日志。

**快照与 dump 都不需要新段**：`SNAP_VERSION` 仍 4、`DUMP_VERSION` 仍 2。快照存的就是
`tables`（id + def）与 `rows`，`row_index` 载入时按 def 重建 —— 于是改名与清空天然被
现有格式覆盖，不需要「快照里记住曾用名」那种新形状。

**同值消除（`RowPut`）**。`translate` 发现新行与旧行逐字节相同就不产日志，直接回
`Reply::Row(旧行)`。位置刻意在两条闸门之间：

- **在 `PUT_IF_ABSENT` / `PUT_IF_EXISTS` / CAS 之后**：那三条是客户端要的语义闸门，不能被
  优化吞掉 —— 否则一次同值重写会把本该 `EEXIST` 的 `PUT_IF_ABSENT` 静默成成功。
- **在唯一性检查之前**：同值不可能新造出唯一冲突（这些值已经带着旧行的主键活在索引
  里），也不必重排索引，所以先消掉反而少跑一遍循环。
- **不为此加统计字段**：`exec` 只在 `!logs.is_empty()` 时抬 `writes_total`，所以
  `ops_total − writes_total` 这个**既有**差值就是「被消除的写」的读数；`seq` 不走、不
  append、不 fsync（`commit_batch` 对空 payload 直接返回）。`putsame` 那一格实测
  records=0 / writes=0 / syncs=0 而 `putchanged` 是 N/N/N（§7.16）。

代价只有一条，但是对外可见的：**一次「值没变」的 `RowPut` 不再产生 WAL 记录**。依
「每次写都有一条日志」做变更捕获（CDC）的消费方要改成看应答里的行、而不是数 `seq`
（写进 §8 第 18 条）。只读副本不吃亏：它缺的只是这条无变化的记录，两边终态相同。

### 5.2 已经锁进测试的语义

| 语义 | 测试 |
|------|------|
| `rename` 原子替换同名目标；拒环（目录挂到自家子孙下 → `EINVAL`）与类型错配；自身改自身按 POSIX 是 no-op 成功而不是 `EEXIST` —— 包括两个名字指向**同一个 inode** 的情形（硬链接之后它会真的发生：不当 no-op 处理会把旧名字删掉，并留下一个没有第二个名字却 `nlink` 虚高的 inode）；`ENOTDIR` 优先于 `ENOENT`（`rename`/`unlink`/`rmdir` 三个入口同一口径：先验父节点是不是目录，再查名字）；`RENAME_NOREPLACE` 位目标存在则 `EEXIST` 且不动现场 | `rename_moves_and_overwrites`、`rename_rejects_cycles_and_type_mismatches`、`unlink_and_rename_report_the_last_holder_key`、`unlink_and_rmdir_report_enotdir_for_a_non_dir_parent`、`overwriting_one_of_several_links_only_drops_one_vote` |
| `unlink`/`rmdir`/`rename` 的应答直接携带 GC 线索：内容对象**刚好**失去最后一个持有者时键原样带回，否则空串。判定必须与删 dentry 在同一个 op 里 —— 客户端自己 `Unlink` 后再 `RefLookup` 之间有窗口，别人重新引用同键就会让 GC 删掉还在用的字节 | `unlink_and_rename_report_the_last_holder_key` |
| `mkdir`/`rmdir` 与 `nlink` 规则（非空目录不能 rmdir；`..` 计入 nlink） | `mkdir_rmdir_and_nlink_rules` |
| 视图根 inode 不可被 lookup/rename/unlink 触达 | `view_root_cannot_be_reached_or_removed` |
| 视图名 ≤ 25 字节校验；视图之间名字空间完全隔离 | `view_name_validation_and_isolated_namespaces` |
| `readdir` 按名字字节序，游标分页稳定（不重不漏） | `readdir_is_byte_ordered_and_pages_stably` |
| `symlink`/`readlink`；同名**非目录**项被原子替换、目录不覆盖（空 → `EISDIR`，非空 → `ENOTEMPTY`）。这里**不是** `symlink(2)`：系统调用遇任何已存在项都回 `EEXIST`，而这条路径是给调用方的 create-or-replace 留的（否则得 `Unlink`+`Symlink` 两步，中间漏一个「名字不存在」的窗口）；`Create`/`Mkdir` 仍按 `EEXIST` | `symlink_readlink_and_posix_override` |
| `refs` 账本的字节不变量 `bytes == Σ 持有者 size`；未登记对象查出来是零行而不是 ENOENT（GC 能无脑查）；只改 size 不动 ref 时账本跟着补差 | `data_ref_ledger_keeps_exact_byte_invariant`、`commit_ref` |
| 锁按 owner 生效，租约过期条目被过滤掉 | `locks_are_owner_scoped_and_lease_filtered` |
| 缺失 inode 上 `setattr` → errno；目录 size 规则 | `setattr_on_missing_inode_and_dir_size` |
| 视图 id 单调，drop + checkpoint 后不回绕复用 | `dropped_view_id_is_never_reused_across_reopen` |
| `readdirplus` 与 `readdir` 给**同一页**（四个平行数组 + `next`/`end` 逐字段相等），每条属性与单独 `Getattr` 相等；错误面同源（`ENOTDIR`/`ENODEV`）。存在理由：不带属性时客户端一页 N 条要回头逐条 `Getattr`，往返从 1 变 N | `readdir_plus_answers_the_same_page_as_readdir_plus_getattr`、`generic_table_and_readdir_plus_over_tcp` |
| 建表只校形状（名字非空有上界、1..=64 列、列名不重）；重名 `EEXIST` 而不是覆盖；查不存在的表 `TableGet` 回 `Table(None)`（不是错误），写侧回 `ENOENT` | `table_shape_is_validated_and_names_can_be_reclaimed` |
| 行写校验链：主键 1..=4096、列数与 def 严格相等、两个互斥 flag 同给 `EINVAL`、条件列越界 `EINVAL` | `row_writes_respect_bounds_constraints_and_conditions` |
| 唯一约束只按字节相等判撞，**空串也是一列的合法值**；同主键覆盖写不算撞。`RowPut` 是整行覆盖（不给列 = 该列清成空字节） | 同上 |
| `PUT_IF_ABSENT`/`PUT_IF_EXISTS` 与 CAS（`cond_col`/`cond_val`）；条件不命中一律 `EAGAIN`，**行不存在也算不命中**（回 `ENOENT` 还是 `EAGAIN` 会误导重试策略） | 同上 |
| `RowDelete` 默认幂等（重发一个已成功的删除不该看到错误），`DEL_IF_EXISTS` 才回 `ENOENT` | 同上 |
| 翻页游标 = 「上一页最后一行的主键」（不含本身），与 `readdir` 同语义；不用「已返回若干条」那种序号游标（BTreeMap 的 `skip(n)` 是 O(n)，翻到第 k 页就把前 k 页重走一遍） | `row_scans_page_by_key_cursor_without_gaps_or_repeats` |
| 只能按主键或有索引的列列举（否则 `EINVAL`，不静默扫全表）；`limit = 0` 走默认页宽而不是「不限」 | 同上 |
| 区间扫描 `[lo, hi)`：左闭右开，空串 = 该侧无界；`lo > hi`、`lo == hi`、游标越过上界一律是空页 + `end=true`，不是错误也不是崩溃（这四条正落在 `BTreeMap::range` 的 panic 条件上）；游标落在 `lo` 之前时仍从 `lo` 起 | `row_range_by_key_is_half_open_and_keeps_the_bounds_when_paging` |
| 索引列的区间上限按**整段值**排除（值为 `hi` 的行一条不返回），同一个值内按主键序，游标是 `(列值, 主键)` 一对；游标停在该值末行之后不能越过 `hi` 去碰下一个值 | `index_range_walks_column_values_and_pages_inside_one_value` |
| `TableDrop` 级联掉行与二级索引，一条不剩；空内层表不留；`next_table_id` 不回收 | `generic_tables_keep_rows_and_rebuild_indexes`、`table_state_survives_checkpoint_and_reopen` |
| 表与行走完全部四条持久化路径：WAL 重放、checkpoint 截断、`kill -9`、TCP 端到端 | `reopen_replays_wal_into_identical_state`、`control_plane_survives_sigkill`、`generic_table_and_readdir_plus_over_tcp` |
| 硬链接：`Op::Link`（`v0.5.0`）给已有 inode 加第二根 dentry，两个名字指向同一个 ino、`nlink` 按引用计数增减、内容对象只在最后一个名字离开时下账。此前 `nlink` 只在目录意义上活动（`2 + 子目录数`）；现在文件与符号链接的 `nlink` 是真实引用计数，`Unlink` / `Rename` 覆盖 / `Symlink` upsert 三条路径都按「是不是最后一个名字」分流 —— 不是的话只减一票且**不下内容引用账**，应答里的空键保证调用方不会把还有人用的对象当孤儿回收。三条拒绝：目录 `EPERM`、名字已存在 `EEXIST`（`link(2)` 没有 upsert，`ln f f` 也是 `EEXIST`）、跨视图不成立（一个 op 只落在一个命名空间里） | `hard_links_share_the_inode_and_keep_bytes_alive`、`link_refusals_name_the_posix_errno`、`overwriting_one_of_several_links_only_drops_one_vote` |
| 原子批：子 op 顺序可见（`create` 完直接 `setattr` 刚分配的 ino、同一张表上第二条 `RowPut` 看得出第一条占住的唯一值）；任一子 op 失败则整批不落，目录 `nlink`/`size`/`mtime`/`ctime`、`next_ino` 与两个名字全都不动（零日志）；整批只推进一个 seq，把那条 WAL 记录从 payload 中间截断重开后整批一条不剩，而它前面的记录照旧重放 | `a_batch_sees_its_own_writes`、`one_bad_op_leaves_the_whole_batch_unapplied`、`a_batch_on_the_generic_table_is_atomic_too`、`a_batch_is_one_wal_record_that_a_torn_tail_drops_whole`、`a_read_only_batch_costs_no_log_and_answers_like_separate_calls`、`batch_refuses_empty_oversize_nested_and_control_ops` |
| 追加列：只能追加到末尾，旧行按新宽度补**空字节**（补出的空与显式空不可区分）；`COL_INDEX` 允许且补出的索引项立即生效，`COL_UNIQUE` 一律 `EINVAL`（无回填手段）；写侧拒绝：表不存在回 `ENOENT`，其余（重名/已满 64 列/空名/超长/`COL_UNIQUE`）回 `EINVAL`，且全部**零日志**；整次变更只有一条 WAL 记录，把它从 payload 中间截断重开后仍是旧宽度；同一条日志 `apply` 两次等价；checkpoint 与 dump → restore 之后新列、旧数据与索引都在，且接着能写 | `add_column_widens_every_row_and_indexes_the_default_value`、`add_column_refusals_leave_the_shape_alone`、`add_column_is_one_record_and_a_torn_tail_keeps_the_old_shape`、`widened_table_survives_checkpoint_and_dump_restore`、`add_column_log_replays_idempotently` |

### 5.3 明确**不**提供的东西

- **数据面**：没有 `open`/`read`/`write`/`close`，文件内容永远在对象存储侧。
  引擎只回答「这个名字对应哪个 inode、inode 的属性和 data_ref 是什么」。
- **fd 表与 open-file 状态**：因此没有 `O_TRUNC`、没有 POSIX 记录锁的
  `(pid, fd)` 所有权语义 —— 锁的 key 是调用方给的 owner 字符串 + 租约到期时间。
- **跨视图原子操作**：每条具体 op 仍只落在一个视图的命名空间里，`Rename` 不跨视图；但
  `Op::Batch`（§5.1）可以同时装两个视图的子 op，所以「跨视图一次原子」现在是有的 ——
  它给的仍然是请求级原子性，不是会话事务。
- **交互式多语句事务（`BEGIN`/`COMMIT`）**：有 `Op::Batch`（§5.1）—— 一个请求里
  的多条 op 顺序可见、要么全生效要么一条不落、崩溃时整批在或整批不在 —— 但没有
  跨请求的事务：没有会话级事务状态、没有 `ROLLBACK`、没有快照隔离。它替代不了的长相
  是「客户端先写一半、想一想、再根据读结果决定下一半」：那要绕两次往返，中间那个
  窗口对别人可见（这是「读不排 fsync」加上单写者流水线的必然）。同样不提供并发写
  冲突检测（`CAS` 是行级的，`RowPut`/`RowDelete` 的 `cond_col` 就是它）。
- **「读不到未落盘的写」这种隔离**：状态在 fsync 之前就对全体读者可见（§8 第 16 条）。
  ack 之后读己之写成立，但「别人看不到还没落盘的写」不成立 —— 这不是疏漏，是
  「读不排 fsync」这一个选择的另一面。
- **硬链接的其余形态**：`Op::Link` 本身已提供（`v0.5.0`，语义与测试见 §5.2），
  不提供的是这三样 —— 目录硬链接（`EPERM`，与 `link(2)` 同款；视图根是目录，因此天然
  被挡）、`link` 的 upsert 变体（名字已存在一律 `EEXIST`）、跨视图链接（与 `Rename`
  同一条边界）。接入侧已闭合（2026-11-21）：grovefs 把 pin 从 `v0.3.7` 升到 `v0.5.0`、
  `link_node` 改成真发 `Op::Link`，阶段 C 1 的矩阵从 `pass=39 fail=0 skip=10` 变成
  `pass=51 fail=0 skip=0`，那 10 条 `[posix C]` 全转正。数字出自那一侧的一次完整板子
  跑，本仓不代替它报数。
- **字节区间锁**：锁的 key 是 `(view, ino, owner)`，没有 `(start, end, pid)`。POSIX 记录锁
  要的是按区间合并/拆分的那套语义，这里给不了 —— provider 只能拒，不能假装支持。
- **通用表只给 §4.2 那十一条 op 的形状**（状态与定型见 §5.1）：没有类型、没有 JOIN 与跨表条件、没有前缀匹配与排序（`RowRange` 只给「按主键或某一个已建索引列的一段字节区间」，
  不给 `LIKE 'a%'` 这种前缀语义，也不给多列组合键）、没有聚合（只有 `RowCount`）、除唯一列外没有其他约束（没 NULL 概念、没外键、没 CHECK）。
  每一条都是把「查询能力」换回「可推的索引 + 不解释的字节」：内核一旦开始解释列值，
  客户端语义就顺着类型漏进来（§11 第 7 条的约束）。
- **表没有命名空间与权限**：`tables` 是全引擎一张平表，不像 `views` 那样按视图隔离
  命名空间。接入方如果需要多租户，表名得自己带前缀 —— 引擎只保证名字唯一，
  不保证两个客户端不撞名。
- xattr、ACL、quota、mtime 之外的时间戳精度增强。

---

## 6. 持久化与恢复

### 6.1 WAL 帧

```
seq: u64 | crc: u32 | len: u32 | reserved: u32   (HDR = 20) | payload(len)
```

- `crc32` 是自带查表的 IEEE CRC32，只覆盖 payload（`crc32_matches_known_vector`）。
- 一次提交可以打包多条 `Log`（一个 op 改多处 = 一次 write + 一次 fsync）。但要注意
  **帧之间没有分组标记**：每条 `Log` 仍然各占一条记录、各有一个 seq 与一个 CRC。
  所以那个「批次」在**运行期**是原子的（不可能只应用一部分），在**崩溃切片**上却
  只能保证「逐条记录完好或丢弃」—— 一个 op 的 5 条 `Log` 如果只落了前 3 条，重放
  出来就是一个非法中间态。这条缺口只对 `Op::Batch` 闭合（它把整批包成 `LOG_BATCH`
  容器，一条记录一个 CRC，见 §5.1）；单 op 的多 `Log` 维持现状 —— 把它也收成一条
  记录会改动全部写路径的 seq 粒度（副本的 `want_seq`、checkpoint 水位、`Stats.seq`
  全部跟着变），属于另一笔独立的账，不是一个 ops 级改动能带上的。
- `Log` 现在 19 个变体，`LOG_*` 字节码爬到 27（1..=10 与 19..=27，11–18 是 M4 删掉后
  留空不回收的那八个，§11 第 10 条）：17 个状态变更 + 2 个「不是状态变更」的特殊体
  —— 容器 `Batch`（`LOG_BATCH` 23）与加列 `AddColumn`（`LOG_ADD_COLUMN` 24）。
  本轮新增的三条批量形状接在尾部：`ClearTable` 25 / `DelRange` 26 / `RenameTable` 27
  （§5.1）。这里以前写着「24 个变体（1..=24）：22 个状态变更」—— 那是 M4 删掉八个
  `Log` 时没跟着改的旧账（22 = 剩下的 14 + 被删的 8），本轮按 `enum Log` 实数核对后改正。
  `Batch` 的 `apply` 就是展开后逐条 `apply`；`AddColumn` 记的是**追加后的整张 def**（而不是
  「新增那一列」），所以重放时按「新旧列数差」补行、差 0 就直接早返 —— 幂等。
  这五条都**不进 dump**（dump 存终态：`Batch` 的效果是它包住的子日志，`AddColumn` /
  `ClearTable` / `DelRange` / `RenameTable` 的效果已经落在表形状与剩下的行里，§6.9）；
  `AddColumn` 也不进快照 —— 快照存 def + 行本身，载入时
  `index_row` 按新 def 自然重建新增列的索引。其余 17 条只记**权威事实**（表定义、
  行全值）与**可重算的意图**（`DelRange` 记区间、`RenameTable` 记 `from`/`to` 而不记被顶替
  表的 id —— 能靠「写全局单序 + 同一份左侧状态」重算出来的东西不占字节，§5.1），
  其余一律不记：`row_index` 由 `rows` + def 重推，`table_by_id` 由 `tables`
  同批维护 —— 与 §5.1「冗余可推字段不进磁盘格式」同一条规矩。`DelTable` 的一条日志
  在恢复时负责级联掉整张表的行与索引。未知 tag 硬错拒启（`unknown log tag`），不能
  静默跳过 —— 跳过就是丢一次已 ack 的写。

### 6.2 恢复规则（`scan` / `replay`）

| 现象 | 处理 | 理由 |
|------|------|------|
| 头读不满 / payload 读不满 | 停在这里，`truncate` 到 good end | 掉电写一半 |
| 头齐但 CRC 不对 | 同上，丢弃 | 坏尾 |
| `len == 0` | **停在这里，不算记录** | 这是预分配空洞，不是记录；当成记录会撞上下一条的跳号硬错 |
| 完好记录之间 seq 跳号 | **硬错误，拒绝启动** | CRC 都对却跳号 = 已 ack 的记录真没落盘，静默重放会把丢数据说成没丢（`seq_gap_is_a_hard_error_not_a_tail`） |
| `seq <= snap_seq` | 跳过 | 快照已覆盖 |

同步 checkpoint 之后 WAL 被 `reset` 成空文件，`fast_forward(snap_seq)` 把发号水位接回快照
位置——否则新记录从 1 重新发号，下次恢复会被当成「快照已覆盖」静默丢掉
（`reset_keeps_sequence_numbering`、`checkpoint_then_reopen_survives_wal_truncation`）。
`reset` 走 `set_len(0)` + 重新预分配 + `sync_all` + rename + 目录 fsync，把上一代记录
**物理抹掉**，不然预分配空洞里的旧字节会被后续扫描误读。旁路快照（§8 第 7 条）不走
这条路：收尾时用 `truncate_before(seq)` 只搬走 `> seq` 的尾巴，理由在那里第 ① 条
（取水位之后才 ack 的写只活在日志里）。

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
| `control_plane_survives_sigkill` | 密钥/审计/定义/放置/锁这类无本地缓存兜底的数据，本轮起同一口径覆盖通用表（形状见表下） |
| `repeated_kill_cycles_do_not_erode_state` | 连崩 3 轮，状态不腐蚀 |

`control_plane_survives_sigkill` 里的表负载形状：崩前建表灌 20 行、覆盖写一行、
再建一张表随后删掉；崩后要求行全在、被覆盖的列读到新值、按索引列能枚举、重复的
唯一值仍 `EEXIST`（这一条才是「索引真被重推出来」的判据）、删掉的表不留残留行。

这六条在 `tests/crash_recovery.rs`（单前端），另有一条同语义的并行档用例
`acked_writes_survive_crash_with_workers_on`（`tests/read_scaling.rs`，`workers=8`）：
300 次目录写与 300 次表写逐条交错，每几条夹一批纯读逼 worker 与提交 fsync 重叠，
崩后行数与索引重推都要对上。「已 ack 的写一条不丢」这句承诺不因为开并行而缩面。

### 6.5 快照升级（v3 → v4，`treedb-migrate`）

版本号闸门把旧目录挡在门外是对的选择（静默按新布局解旧字节 = 把 8 字节整数当
长度前缀读），但它必须有一条出路，否则「升级到 `v0.6.0`」这句话就等于「元数据
重建」。出路是 `treedb-migrate`：把旧文件读成内存 `State`，再按当前布局写回。
它现在能成立靠两条事实 —— **v3 与 v4 的正文只在 `refs` 之后差一段**：v3 在那里
多 24 字节（`keys` / `audit` / `defs` / `placement` 四个 u32 计数 + `next_audit_seq`
一个 u64），v4 直接接 `tables` 段；**且那一段在 v3 里必须全零**（下表第 5 条）——
删表不删数据。前一条由 lib 用例 `legacy_snapshot_is_readable_only_by_the_migration_entry`
钉住（v3 比 v4 长恰好 24 字节、v2 与多字节尾巴都被拒），后一条由
`legacy_snapshot_with_control_plane_rows_is_refused` 钉住（计数非零则拒，错误串
指名是哪张表）。

**这一节最早写于 v2 → v3**（v0.3.3，那一代只是尾部多了 `tables` / `next_table_id` /
`rows` 三段、正文逐字节相同），四条承诺与下面那套 `.tmp → fsync → rename` 的写法
都是那一轮定下来的；工具本身一行没变，变的只是「上一代」是谁。

四条承诺加 v3 → v4 新增的一条，逐条有用例守（`tests/migrate.rs`，12 条）：

| 承诺 | 含义 | 守它的用例 |
|------|------|-----------|
| **WAL 一个字节不动** | 迁移不打开 WAL，只重写 `state.snap`。于是「已 ack 的写不会因为迁移而丢」不用重新论证，回滚也只是把备份文件拷回去再跑旧二进制 | `migration_preserves_every_field_and_leaves_the_wal_alone`（直接比迁移前后 WAL 文件字节） |
| **seq 水位照搬** | 文件尾那 16 字节里的 `seq` 是「这份快照覆盖到哪个 seq」，原样搬过去；重算一次就是自己发明一个水位，下次启动按它跳记录就会丢写 | `seq_watermark_survives_the_migration_and_numbering_continues` |
| **只认上一代** | 版本位不是 3 也不是 4 就拒；正文之后不是恰好 16 字节的尾巴也拒 —— 宁可不改也不猜布局 | `unknown_version_is_refused_without_touching_anything`、`body_that_does_not_match_its_version_is_refused` |
| **拒动时零改动，干完可幂等重跑** | 自检不等 / 哨兵发现并发写者 / 已有备份又没给 `--force` —— 三条失败路径上原文件一个字节没改过；同目录再跑一次是 `AlreadyCurrent` 而不是重写 | `existing_backup_is_not_clobbered_without_force`、`migration_is_idempotent_and_leaves_a_current_dir_untouched` |
| **四张专用表非空就拒**（v3 → v4 新增） | v3 那 24 字节里只要有一个非零计数，就不动文件、不写备份，错误串指名是哪张表、几行，并给出路（先用旧版本把数据搬进通用表） | `legacy_snapshot_with_control_plane_rows_is_refused`（lib 单测：在空状态 v3 正文里把 `keys` 计数改成 1） |

写回顺序照搬 `Engine::checkpoint` 那一套：`.tmp` → `fsync` → `rename` → `fsync(目录)`。
动手前还有两道：先把要写的字节用**严格入口** `State::load` 解一遍并与手上状态逐字段
比对（等于先问一遍「新二进制明天能不能开这个门」），再落一份 `state.snap.v2.bak` 备份
（**文件名是历史**：从 v2 → v3 那一轮起就叫这个，故意不改 —— 回滚脚本与既有备份
都按这个名字找文件，改名换不来任何好处）；
改写前后各读一次源文件比对。任一环节失败要么什么都没改，要么只留下一个没被
`rename` 的 `.tmp`，不影响任何启动路径。

为什么不用更省事的「`Engine::open` 读旧快照 + 立刻 checkpoint」：`wal.reset()` 会把
上一代记录**物理抹掉**（§6.2），那份备份就失去回滚价值；而迁移中途崩掉时，那些已 ack
却还没被旧快照覆盖的写就只剩一份未验证的新文件 —— 拿持久性换省事，不划算。

常规启动路径继续不开兼容口子：`Engine::open` 只用 `State::load`，遇 v3 直接报错，
错误消息里把命令写出来；`load_for_migration` 只被离线工具调用。这是刻意的两面：
闸门守住「不静默解错」，工具守住「不是死路」。

**迁移前必须停服务端**。本工具不 flock（服务端自己也没持文件锁，锁不住任何东西），
那两次重读是 best-effort 哨兵而不是互斥 —— 不要把「没报错」当成「与并发安全」。

**回滚有个真实长度的窗口**：把 `state.snap.v2.bak` 拷回去跑旧二进制，只要新二进制
**还没做过第一次 checkpoint** 就是无损的 —— 迁移之后新二进制吸收的那些写还在 WAL 里，
而 WAL 是两边都能读的（`Log` 标签 1..=10 与 19..=22 在 v3/v4 之间没动；11–18 是删掉
的四组控制面变体，v4 不再认；23..=27 那五个容器与批量形状是 v4 之后才追加的，
v3 的 WAL 里根本不出现那些 tag，所以旧目录重放也不会碰到它们）。一旦 checkpoint 过，
`wal.reset()` 把上一代记录物理抹掉，那些写就只剩新布局快照一份，旧二进制读不了：
此时回滚 = 丢掉「第一次 checkpoint 之后的全部写」。注意优雅停服那一次收尾
checkpoint 也算数，所以「先升、跑一段、发现不行再拷回去」这个顺序本身就是错的。
`treedb-migrate --apply` 把这句话直接印在 `rollback =` 那一行里，
`tests/migrate.rs` 钉住了它。

回滚还有一条更窄的边角，得在升级前就想到：迁移只核**快照**里的四个计数，不扫描
WAL。如果旧二进制在最近一次 checkpoint 之后写过控制面行又删掉（净计数为零，
快照能过），那段写仍以 tag 11–18 留在 WAL 里，v4 重放会报 `unknown log tag`。
出路是先用旧二进制 checkpoint 一次（`wal.reset()` 让它只留快照），再迁移。

v1 → v2 **没有**迁移路径：那一代改的是既有字段的宽度（`data_ref` 从 `u64` 变成不透明
字节，§5.1），没有「读旧写新」的稳妥路子。v2 → v3 现在也断链了 —— v4 只认上一代
v3，所以跨两代要跑两遍旧版本（先 v0.3.3 把 v2 升到 v3，再 v0.6.0 升到 v4）。工具
永远只认上一代的理由不变：认识两代之前的布局等于把「格式变更要 +1 并给出门」这条
规矩变成「格式变更可以攒着」。

```sh
treedb-migrate --dir /var/lib/treedb                  # 只报告，一个字节都不写
treedb-migrate --dir /var/lib/treedb --apply           # 真执行：先落备份再原子重写
treedb-migrate --dir /var/lib/treedb --apply --force   # 连既有备份一起换掉
```

退出码：`0` = 已是当前布局或迁移完成；`1` = 出错（包括拒动）；`2` = 参数错；
`10` = 需要迁移但没给 `--apply`（给脚本用：先探一下再决定动不动手）。输出是
`key = value` 风格、字段名对齐 `treedb-server --stats`，其中 `wal = … untouched …` 那行
就是上面第一条承诺的可验证据，不是装饰。

**外部效度由 `benches/rehearse_migrate.sh` 补**。上面那些用例里的 v3 样本是
`save_v3()` 写的，而 `save_v3()` 与迁移工具是同一轮加的 —— 它只证明代码自洽，不证明
真能接住 `v0.5.1` 落盘的那种文件（万一两边字段顺序、某个计数或尾巴位置不一致，
单测会全绿而线上开门就崩）。彩排脚本 `git archive` 一个真 tag（默认 `v0.5.1`）、真编
译旧二进制、造目录 → 迁移 → 开门 → 回滚，逐格核对六件事：版本位真是 3、WAL 的
sha256 迁移前后相同、备份的 sha256 等于原件、快照尺寸差**恰好 -24 B**（正文抽掉
四段计数，尾巴 16 字节同形）、迁移前后 `--stats` 的 views/nodes/dentries 与文件尾 seq
逐字段相同、以及上面那个回滚窗口的两面（最近一次实测：`v0.5.1` 造的 v3 目录 2 101 350 B
→ 2 101 326 B（恰好 -24）、seq 74 760 照搬、views/nodes/dentries 逐字段相等；WAL
完好时回滚 39 696 → 39 696 无损，checkpoint 之后 62 864 → 39 696 有损）。原始输出在
`benches/results/rehearse_migrate.log`。

### 6.6 日志传送与只读副本（`treedb-standby`）

限制 6 原来写的是「单机、无复制」。这一节把它降级成「有异步只读副本，无同步
复制」：副本不开新写路径、不参与定序，它只是**同一份日志的第二个消费者**。

#### pull 而不是 push

主不需要知道有任何副本存在：它只是回答两条新 op（`WalFetch` / `SnapFetch`），
副本按自己的节奏拉，拉到什么就 apply 什么。于是服务端主循环一行未动 ——
不往连接表里挂反向通道，不为「副本掉线」维护任何状态，副本也不需要在主那边注册。
新增的代码全部在 `net::replicate`（追平状态机）与 `bin/standby.rs`（入口）加上
引擎侧四条方法。push 要付的代价是反面：主得为每条副本连接维护游标、重连、
慢副本的背压，而那些状态恰好是最容易写坏的一类。

#### 传送口径（这四条是这一节的正文）

| 口径 | 定案 | 为什么不是别的 |
|------|------|----------------|
| 交付上界 | 只交付 `seq <= durable_seq` 的记录 | 越过就是把主尚未确认的写交给副本；主一崩就回滚，副本却已经拿它答过读 |
| gap 判据 | `gap = want_seq <= 主.snap_seq` | 主每次 checkpoint 都会截掉水位以下的日志。跳号不是「补一下」的理由，而是「你跟不上」的结论，所以补洞路径只有一条：重装快照 |
| `hint_off` | 是提示不是承诺：上一轮主给的 `next_off` 原样回送 | 采纳前必须验「读出的 seq 相符 **且** 记录完整落在逻辑末尾内」，否则退回从头扫。不验就直接 `seek` 的话，一次 checkpoint 之后的旧偏移会读出垃圾帧；而 `WAL_HINT_NONE = u64::MAX` 那个占位值不做前置判还会让 `off + HDR` 回绕成一个看起来很合理的偏移 |
| 负载上限 | `WAL_FETCH_MAX = SNAP_CHUNK_MAX = 4 MiB`，`MAX_FRAME = 8 MiB` 仍是硬天花板；条数上限 `MAX_FETCH_RECORDS = 262 144` | 最小的 `Log` 是 `DelNode`（13 B），线上一条记录至少 17 B，4 MiB 装满约 25 万条 —— 取 `1 << 18` 刚好高于这个天花板，于是长度先越界、计数这条不会误挡合法应答 |

#### 副本是一个完整的 `Engine`，不是一份缓存

`Engine::open_replica(dir, durability)` 与 `Engine::open` 只差一件事：真写在
`Engine::exec` 那里被 `EROFS` 拒掉。剩下全部一样 —— 自己的 WAL、自己的快照、
自己的 checkpoint。

- **记录以 payload 原字节落盘**，所以两份日志、两份 `state.snap` 都可以按字节
  比对（`a_gap_reinstalls_the_snapshot_in_chunks` 就在校这一件事）。`replicate_records`
  因此对每条做「解码 → 再编码 == 原字节」的往返校验：线帧里每条记录没有自己的
  CRC，而副本落盘时会重算一份新 CRC —— 不拦这一道，在线上悄悄坏掉的字节就永久
  合法了。这一条同时验两件事：内容没坏，两代编码格式没分叉。
- **先 WAL 后 apply**：崩溃重启由自己的日志重放还原，不必再向主拉一轮。这是
  `a_replica_restart_resumes_from_its_own_log` 测的东西，也是「副本自带 WAL」这个
  决定的全部回报（没它的话每次重启都要先过一遍全量下载，而重启恰恰是副本最
  频繁的事件）。
- **拒写拦在引擎层而不是网络层**，判据是 §4.2 的三档（`!is_readonly() &&
  !is_control()`）。多 worker 下写 op 本来就要送到提交线程，两处各拦一份一定会
  对不齐；而拿「非只读」当判据会连 `Stats`/`Checkpoint` 一起拒掉 —— 前者让副本
  没法监控，后者让它拖着一个与主同大的日志。
- **`install_snapshot` 拒绝水位回退**：主交来的快照代际比自己低，只能是连错了主
  或把两代拼错了。这是整个设计里唯一让进程退出的错误 —— 继续服务一份谁也不认的
  状态比停下来更坏。**一例外，见下一节「切主」：对端的令牌更大时，权力优先于新旧。**

#### 切主：fencing token 与 `--promote`（v0.4.0）

上一小节那套东西有一个洞：副本不知道自己跟的是不是“当前那个主”。旧主被替换
后照样能应答 `WalFetch`，副本照样把它交来的记录当成真相 —— 两代状态就这样被
同一个副本接在一起了。切主需要的是一个**单调、可持久、能随数据传播**的比较量。

**令牌住在 `<dir>/fence`，是一个十进制整数 + 换行。**

它不在快照里，也不在 WAL 里（`SNAP_VERSION` 抬到 4 的改动与它无关），这不是偷懒而是刻意的：
① 提升它不需要碰任何数据面字节格式，旧快照零迁移；② 冷拷贝一个目录时它自然
跟着走；③ 一个目录被人误当“空白副本”重用之前，`cat fence` 就能看见它已经被提
升过。代价写在明处：它不在 WAL 的事务边界里 —— 但 fencing 的用处就是“比数据
更早一步知道谁有权写”，先落盘、再由数据追上，这个顺序是设计的一部分。

**传输：两个既有 reply 的尾部各多一个 `epoch`。**

`Reply::WalBatch` 与 `Reply::SnapChunk` 末尾各带主的 `fence`。为什么快照路也要
带：一条只走 `SnapFetch` 的重装路径（冷启动副本、gap 之后）如果拿不到令牌，就
正好绕过了闸门。`PROTOCOL_VERSION` 因此 5 → 6，而且这次是**不兼容**升级：
两个 reply 各多一个 `u64`，v5 的副本解 v6 的主会短读报错（大声失败，符合预期）。
上位记的那句「追加字段可以跨版本」在这里不成立 —— 换来的是一件事：令牌不对的
副本，宁可连不上也不能接。升级时主与副本一起换。

**副本侧的三条规则（`Engine::admit_epoch`）：**

| 对端令牌 | 动作 | 为什么 |
|---------|------|--------|
| 更小 | `Error::Fence`，一个字节不许落地 | 这是一个已经被提掉权的旧主。`net::replicate` 把它当**配置错**而不是链路错：抬 `replica_errors`、退避重连、不退出、也不重装快照 |
| 更大 | **先把令牌落盘，再吃数据** | 顺序不可换：先吃数据后落盘，一次崩溃就留下“按新令牌追过数据、重启又回到旧令牌”的目录。反过来只会保守 |
| 相同 | 正常路径 | 令牌相同才轮到比新旧（`seq` 回退仍然拒绝） |

`Error::Fence` 单开一个变体而不是复用 `Gap`：运维动作完全不同 —— `Gap` 是「跟不
上了」（自动重装快照），`Fence` 是「连错主了」（改 `--primary` 或停掉旧主）。拿错
误文案去区分这两件事，等于让调用方猜。

**`install_snapshot` 的例外：权力优先于新旧。**

原来的规则是“快照代际比自己低就拒”。有令牌之后这条要拆成两半：令牌更小时一律
拒（那才是真的脑裂回滚）；令牌**更大**时，哪怕快照更旧也接受 —— 新主是从别处提
升的，它的令牌更大、它的数据就是权威，旧主以前跑得快不代表它还有权。所以判据
不是“谁更新”，而是“谁有权写”，权力相同时才比新旧。这一条在测试里连同它的
反面（同令牌下回退仍被拒）一起钉住。

**`treedb-standby --promote` 是对目录的离线动作，不起服务。**

它把令牌抬到「本目录见过的最大值 + 1」然后退出，打印接下来要执行的命令。不就地
变成可写服务端，有两个理由：① 提升是运维动作，什么时候开新主、客户端什么时候
改地址，不该被这条命令隐式决定；② 同一条命令行里同时给 `--primary` 与 `--promote`
会自相矛盾（既追别人又当主），所以同时给直接退出码 2。

“提升后的令牌必定大于任何驱动过本目录的主”是**由落盘顺序保证的**，不是靠调用方
记得传对参数：副本每接受一个更高令牌的主，第一件事就是把它落盘。

令牌到 `u64::MAX` 时提升**报错**而不是返回原地不动的值（`checked_add`，不是
`saturating_add`）：后者会让调用方看到“提升成功”而令牌一格未动，旧主仍能过闸门 ——
这个分支实际上永远跑不到，但它恰好是 fencing 唯一要防的那类静默失败。

**它挡不住什么（诚实边界）：**

1. **旧主照旧对自己的客户端服务**。fencing 只在副本侧生效 —— 它不阻止旧主接写，
   只保证副本不会把旧主的写当成真相。所以切主必须真停旧主，抬令牌不能替代停机。
2. **只保护「曾经被提升过的目录」**。一个从未追过新主的副本令牌仍是 0，把它指
   回旧主它照收 —— 而这恰好不是脑裂回滚的形态：回滚的风险在于“已经有目录拥有
   更高权力并且已经按它答过读”，那类目录的令牌一定更高。
3. **不提供自动故障转移**。谁提升、什么时候提升，仍是人（或外层编排）的决定。
4. **没有租约/心跳**。旧主不知道自己的权被拿走；要让它自己发现，得再加一层会
   话期与续租，那是另一套机制（§8 第 6 条③）。

五条测试钉住这一节：`the_fence_is_monotonic_persisted_and_never_defaults_to_zero`
（只升不降 / 落盘 / 坏文件不当 0）、`a_replica_obeys_the_fence_on_snapshots_and_
batches`（四档闸门，含“权力优先于新旧”与它的反面）、
`a_promoted_replica_refuses_its_old_primary_over_the_wire`（真链路上拒旧主、状态不
回滚、仍在答读）、`the_promote_flag_bumps_the_fence_without_serving`（离线动作与两
个拒绝面）、`a_real_standby_promotion_fences_a_resurrected_primary`（真主 + 真
standby + 真 CLI promote + 真新主 + 高令牌真 standby 的完整进程链）。最后一条已经
进 `cargo test --release` 回归板；吞吐量具仍单独跑，不把机器相关数字塞进测试断言。

**活体验证**（2026-11-25 05:24:52，`benches/promote_smoke.sh`，HEAD `f9a385c`、
protocol 8，每个压测臂 1 s）：主 A `create` 1 285 ops/s、零错，`nodes=1 293`、
`seq=3 871` → 真副本 B 追到同一 `nodes`/`seq`、`lag=0`、`errors=0`、`fence=0`；
B 在线写拿 `EROFS(30)` 且水位不变 → B 活着时 `--promote` 退出码 1，错误点名
持锁 pid 1 879 578，令牌仍为 0 → 按顺序停 A、停 B 后提升：`fence 0 -> 1`、
`snap_seq=3 871`、重放 0 条，且没有监听端口 → B 起新主后初始水位不变，`create`
1 572 ops/s 零错，`seq=8 593`、`nodes=2 867` → 旧主 A 回魂仍能写（`seq=8 125`），
但新主 B 的 `nodes`/`seq` 一格不动 → 冷拷的同令牌 C 追 A：`replica_errors=2`、
`lag=4 254`，`nodes=1 293`、`seq=3 871`、`fence=1` 均不回退，日志明确
`is fenced out`，随后纯读 `ping` 51 422 ops/s 零错。旧主回魂只用于演示边界 1，
不是推荐运维顺序；真实切主仍必须先停旧主。

脚本现在动态占四个端口、在 `.live/promote.*` 留独立现场、把结果追加到
`benches/results/promote_smoke.log`，任一断言失败都非零退出。日志保留本轮排障时的
失败记录与最终 PASS；文档只引用最终 PASS 块，钉的是关系而不是具体计数。

#### tick 住在提交线程里

`Follower::tick` 由事件循环每轮调一次，不是另一条线程：apply 需要 `&mut Engine`，
而 `Engine` 按单写者约定只活在提交线程上（`workers > 1` 时就是 `net::multi` 的
committer，`workers = 1` 时是轮末周期任务）。两档跑的是同一份断言
（`the_standby_binary_serves_reads_on_the_{single_thread,multi_worker}_loop`）。
代价是一次 tick 会占住本轮循环，所以每 tick 只做**一次**往返（一块快照或一批
记录）：快照因此是一块一块下的，不会一口气下完几十 MB 把客户端全关在门外。
连不上主时按倍增退避（从 `interval_ms` 到 2 000 ms 封顶），否则主宕机后每条 tick
都要在监听线程上付一次 SYN 超时。

#### 失败政策与可观测面

失败默认**不致命**：连不上主、主回报 `EIO`、交付对不上号，都只抬 `Stats` 里的
`replica_errors`，副本继续用已有状态答读 —— 只读副本最有用的一刻恰恰是主没了的
时候。`gap` 不抬 `replica_errors` 而是抬 `replica_resyncs`：两个数混在一起，运维就
分不出哪一种该报警。六个新字段：`replica`（自报身份）、`replica_primary_seq`、
`replica_lag`、`replica_applied`、`replica_resyncs`、`replica_errors`。`lag` 在引擎里
现算（`replica_primary_seq - 本地 durable_seq`）而不是让调用方自己减，减错一次就是
一个永久不对的数。

两个读法上的坑，都进了 `treedb-server --stats` 的输出说明：`lag == 0` 单独不构成
「已追平」（一个从没联系上主的副本 lag 也是 0），必须与 `replica_applied` /
`replica_primary_seq > 0` 同看；`replica_resyncs` 持续上涨说明追不上主（主 checkpoint
太快或 `--fetch-bytes` 太小），不是 bug 但是要调参的信号。

#### 边界（这一节没做到的）

1. **异步，不保证读到已确认的写**。副本能读到多少取决于上一次 tick 到哪。要
   「读己之写」的客户端必须继续读主，或者自己等 `lag == 0`（这一手很弱，因为
   `lag` 是上次往返的快照而不是未来承诺）。
2. **有主身份认证（§4.4，可选），且 v0.4.0 起有 fencing token**。“切主”那一节
   已经说完全了：令牌更小的对端一律拒，所以一个被提过权的副本不会再被旧主驱动。
   剩下的是两道门：① 旧主自己不知道权被拿走（无租约/心跳），仍对自己的客户端
   服务；② 一个**从未追过新主**的副本令牌仍是 0，指回旧主它照收。两条都指向同一
   句部署约束：切主必须真停旧主，抬令牌不能替代停机。
3. **`SnapFetch` 把整个元数据状态交出去**，`WalFetch` 交出全部尚未截进的写。引擎
   本来就假定这个端口在受控网络内（任何客户端都能 `ListViews`/`RowList`），所以
   这不新增一类暴露面 —— 但它把「端口只能对内」这条部署约束的重要性抬高了。
4. ~~**没有传输吞吐实测**~~ **2026-11-22 补了三格**（`benches/replica_rate.sh`，
   原始逐格输出 `benches/results/replica_rate.log` 里同日连跑三次；同机、主副本共用
   一块机械盘、loopback，rev `62b6fab`，主上 10.2–11.0 万节点 / 快照 10.6–13.1 MB /
   WAL 2.3–4.7 MB）：

   | 格 | 形状 | 实测 |
   |----|------|------|
   | 冷启动 | 空目录副本，1 MiB/块（默认，**定长**） | 端到端 **696–773 ms**；其中装快照那一轮 199–217 ms；扣掉它传输段 ≈ **26–30 MiB/s** |
   | 冷启动 | `--fetch-bytes 4194304`（也是定长） | 端到端 **397–470 ms**；传输段 ≈ **50–71 MiB/s**（§11 第 4 条那个「≈ 50 MiB/s」的算术上界落在这一档下沿） |
   | 追赶积压 | 副本停着，主攒 37–38 MB / 49–51 万条 | **1.9–2.2 s** 追平 → **23.5–25.8 万条/s**、**17.0–18.7 MiB/s**（含副本 apply + fsync） |
   | 稳态 | 主 4.8–5.2 万 `create`/s（13.2–14.0 万条/s） | 副本 applied 13.1–14.1 万条/s；lag 中位 0.6–2.4 万条、最大 2.9–4.5 万条，停写后 **135–150 ms** 归零 |
   | 读侧 | 对副本打 `ping`，8 连接 | 静置 p50 20–24 / p99 35–46 µs；追平中 p50 20–25 / p99 41–54 µs（p50 在噪声内，**p99 +11%…+28%**） |
   | 最坏一轮 | 三个阶段各用独立 standby 进程 | 追赶 107–133 ms、稳态 184–198 ms、读侧压测 178–262 ms |

   **上表是定长块长的读数。`77fe7da` 之后那一笔把自增也铺到快照段，同机再跑三次
   （快照 10.9 MB + WAL 5.2 MB，形状同上）**：1 MiB 档端到端 **551 / 573 / 610 ms**
   （中位 **573**）、4 MiB 档 **391 / 439 / 511 ms**（中位 **439**）—— 两档的逐轮比
   从 1.64–1.79× 收到 **1.19× / 1.26× / 1.47×**（按中位算 1.31×），即「1 MiB 档自己
   跑到了 4 MiB 档附近」。底层机制与跨机复跑见下一条与本节末「追赶块长」。

   三条读法上的结论。① **端到端冷启动被「装快照」那一轮主导**（一次 apply 全量 +
   写 `state.snap`），定长块长下它还把一轮往返的差放大成 1.64–1.79×（全在传输段上）；
   自增铺到快照段之后这条只剩「装快照那一轮」本身（见上面那段）。② **单批 apply + WAL commit 的落盘长尾就是一轮循环的
   长尾**：追赶相位主已空闲、相位内 `checkpoints` 0→0（不是落快照），最坏一轮仍
   107–133 ms；主同时在写时同一形状涨到 0.18–0.20 s（再加 ping 压测 0.18–0.26 s）。
   ③ **稳态能不能跟上取决于写入速率与盘**：4.8–5.2 万 `create`/s 时副本 lag 中位
   0.6–2.4 万条、峰值 4.5 万条（≈0.04–0.32 s），停写后立刻归零 —— 这是**异步追平**，
   不是同步复制，也不承诺读己之写（第 1 条）。

   上面那一组跑在 loopback 上，不含 RTT 与带宽上限，也分不出「复制开销」与「同机 CPU 竞争」——
   这两件事见下一条。

5. ~~**跨机真实网络 + 副本对主 p99**~~ **2026-11-22 补了**（`benches/replica_rate_xhost.sh`，
   原始逐格输出 `benches/results/replica_rate_xhost.log` 同日连跑三轮，修复版；副本放在
   第二台机 `gfs2`（8 核、`/data/gfs1` 独立盘）上，主与负载发生器在宿主，中间是 virtio
   虚拟网桥；日志表头写的「边界 4」是本节编号调整前的旧号）。链路与盘的上限同一轮里现量：
   宿主 → gfs2 裸 TCP 512 MiB **1838 MiB/s**
   （`dd\|ssh` 只有 206 MiB/s，那是 ssh 加密卡的，不是链路）；副本端 4 KiB fsync
   **132 次/s**，宿主同口径 **384 次/s** —— 副本每轮提交一次 fsync，后面的追赶速率就
   压在它上面。

   | 格 | 形状 | 实测（同名三轮）|
   |----|------|------|
   | 冷启动 | 28.6–31.6 MB（快照 23.4–25.5 + WAL 3.0–7.3），1 MiB/块（**定长**） | 端到端 **1006–1254 ms**（22.5–28.9 MiB/s）；装快照那一轮 437–771 ms；扣掉它传输段 ≈ **49–58 MiB/s**（快照段也自增后的同形状见本节末「追赶块长」）|
   | 冷启动 | `--fetch-bytes 4194304`（也是定长） | 端到端 **516–637 ms**（44.3–58.5 MiB/s）；传输段 ≈ **199–351 MiB/s** |
   | 追赶积压 | 副本停着，主攒 93–98 MB / 122.8–128.7 万条 | **5.9–6.4 s** 追平 → **20.1–21.0 万条/s**、**14.6–15.2 MiB/s**（这一格是**定长 1 MiB** 的读数；块长自增后的同形状见本节末「追赶块长」）|
   | 主延迟 A/B | A 无副本 / B 副本在追（B 全程 lag 中位 216–225 万条，整段没追上） | p50 **1.09 → 1.53 ms（+39%）**、p90 **1.87 → 2.30 ms（+23%）**、p99 **2.53 → 2.88 ms（+14%）**、吞吐 **5.04 → 4.18 万 create/s（−17%）**（同为定长 1 MiB；块长自增后主侧代价更大，见本节末）|

   读法五条。① **跨机冷启动仍被「装快照」那一轮主导**：上表是定长读数，两档端到端
   差 1.85–2.03× 全在传输段上；自增铺到快照段之后（本节末「追赶块长」）1 MiB 档
   851–1353 ms、4 MiB 档 550–627 ms，中位比收到 1.56×，剩下的那一截是装快照那一轮
   加一次换代重下 —— **块长能压掉往返次数，压不掉那一轮 apply**。② **链路远未跑满**：4 MiB 档传输段
   199–351 MiB/s，而裸 TCP 有 1838 MiB/s；1 MiB 档只有 49–58 MiB/s，是被 `--interval-ms 20`
   的「一轮一块」封顶（1 MiB / 20 ms = 50 MiB/s）—— 与 §11 第 4 条那个算术上界完全对得上。
   ③ **跨机追赶比同机低 15–20%**（14.6–15.2 vs 17.0–18.7 MiB/s；2026-11-22 晚同口径复跑
   的同一形状是 11.35–13.64 vs 15.82–17.43，差 16–30% —— 方向一致，幅度随宿主负载摆）。这一句原先还接着
   半句「卡在副本每轮一次 fsync，换块长也无益」—— **2026-11-22 的同机 A/B 推翻了它**：
   一轮的固定开销（一次往返 + 一次 commit/fsync）与块长无关，块长翻倍就把同一笔开销
   摊到两倍字节上（定长 1 MiB 16.31 MiB/s → 自适应块长 30.66 MiB/s，见本节末「追赶
   块长」）。真正的上限是「每轮固定开销 ÷ 块长」，不是 fsync 次数本身；4 KiB fsync
   132 次/s 只说小写慢，不说大写也慢。④ **副本
   在追对主的中位延迟是实打的**：p50 +27…+40%、p90 +15…+27%、吞吐 −12…−19%，三轮
   同向且分布整体平移；p99 也涨但分散（+14 / +19 / +32%），因为它是单点分位，宿主
   后台回写会单独抬高它 —— 结论以 p50/p90 + 吞吐为准。⑤ 这一格是**最坏形状**：副本
   落后 200 多万条、整段都在追；稳态余量（副本追平后只做增量）比它宽松得多。

   **这一格还是本轮那个真 bug 的发现现场。** 多 worker 的 `src/net/multi.rs` 把「只带
   `EPOLLOUT`、没有可读字节」的事件在 `ops` 为空时直接 `continue` 掉了，于是大回包剩下
   的字节永远留在用户态，电平触发还会让 worker 空转。loopback 上整帧通常一次就能塞进
   内核发送缓冲区（`net.ipv4.tcp_wmem` 上限 4 MiB），所以它只在「跨机 + 大回包 +
   多 worker」三者同时成立时出现：1 MiB 快照块只能拿到前 ~110 KiB，然后一直读到
   `--timeout-ms` 超时。取证顺序是「把错误本体打出来（`fail_round` 从此带 `&Error`）→
   得到 `recv: Resource temporarily unavailable (os error 11)` → 64 KiB 过 / 128 KiB 挂
   → 30 s 超时也挂（不是慢是死）→ `dd|ssh` 双向 190 MB/s（网络无辜）→ 两端 `ss -tin`
   看到宿主 Send-Q 0、`bytes_acked ≈ bytes_sent`，副本 rcv 队列 0 且只发出过一条 44 B
   请求 → env 埋点看到 `epoll` 每轮都报 `EPOLLOUT` 却从不进处理块」。`--workers 1` 走
   `serve_single`，没有那个分支，所以单线程档一直是对的。修复是抽出 `flush_on_write()`
   在「正常事件末尾」与「空 ops 分支」两处调用；回归用例 `tests/large_reply.rs` 不靠
   网络形状：连发四个 4 MiB `SnapFetch` 再读，16 MiB 应答必然超过 `tcp_wmem` 上限 4 MiB，
   `write` 一定短写返回 `WouldBlock`。摘掉修复实测 **8.45 s 读超时跑红**（错误签名与
   跨机现场逐字相同），恢复后 **0.23 s 过**。

#### 追赶块长：落后时自己放大（2026-11-22）

`--fetch-bytes` 原来是「一轮就是这么大」。定因时把一轮拆成四段（fetch 往返 /
decode+encode 校验 / commit = 写 + fsync / apply），同机约 100 MB（121–129 万条）
积压、用一份临时 env 门控计时（已移除，不在树里）量到：1 MiB/轮时
fetch 18.8% / dec 8.5% / **commit 59.3%** / apply 13.4%，73.9 ms 一轮；换成 4 MiB/轮
则一轮 139.7 ms、13.45 → 27.31 MiB/s（2.03×），四段占比 25.3 / 10.6 / 46.9 / 17.2%。

读法只有一条：**一轮的固定开销（一次往返 + 一次 commit/fsync）与块长无关**。所以
「追得慢就调大 `--fetch-bytes`」这个旋钮本身没错，错的是把它做成定值 —— 稳态需要
小轮（一轮占住事件循环，读延迟当场付账），追赶需要大轮。

现在的规则在 `src/net/replicate.rs::next_fetch_bytes`（纯函数 ＋ 5 条单测）：

- 一轮**拿满**预算 **且** 主那边按本批「字节/条」均值折算的积压 > 4 个配置块长
  → 块长翻倍，封顶 `WAL_FETCH_MAX`（4 MiB）；
- 拿不满、或收到空批 → 退回 `--fetch-bytes`。配置值因此是**下限**，稳态读延迟
  不被牺牲（集成用例 `the_fetch_block_ramps_while_catching_up_and_returns_to_cfg`
  同时钉住「途中放大」与「追平后回落」）；
- 阈值取 4 个块长而不是 1：只落后一两块时放大，白白把一轮拉长到上百毫秒，而那时
  并没有值得抢的积压。

**实测（同机 loopback，改前/改后同一个临时探针，`.spot/` 未入库；改前那一组是把
本次改动暂存掉、重新编译后同脚本重跑）**：

| 形状 | 1 MiB 定长（改前） | 自适应（改后） |
|---|---|---|
| 追赶 86–96 MB / 113–127 万条 | 16.31 / 15.82 / 16.22 / 17.43 / 16.49 MiB/s（中位 **16.31**），22.4–24.1 万条/s | 34.53 / 30.68 / 30.73 / 29.71 / 30.09 / 30.66（中位 **30.66**），41.0–47.7 万条/s |
| 追平窗口内对副本 `ping`（8 连接） | p50 26–28 / p99 45–87 µs、max 15.8–16.1 ms | p50 20–25 / p99 42–45 µs、max 29.1–38.3 ms |
| 静置 `ping`（对照） | p50 19–25 / p99 40 µs | p50 20–23 / p99 43–46 µs |

**1.88×**（中位 16.31 → 30.66 MiB/s）。读侧 p50/p99 两组重叠（40–87 µs 的抖动就是
噪声），4 worker 下读本来就不走提交线程；可分辨的代价只在 `max`：一轮最长占住提交
线程的时间翻倍（15.8–16.1 → 29.1–38.3 ms）。**跨机复跑后这笔交易多了一条：主那一
侧的账也翻倍了**，见下面第二张表。

**跨机同口径复跑（2026-11-22 晚）**：同一脚本 `benches/replica_rate_xhost.sh 3`、
同一对机器，把 `d660e77`（定长）与 `486f5e1`（自适应）的 `treedb-{server,standby}`
先后推到 `/data/gfs1/rrate/bin` 各跑 3 轮，两组原始输出都在
`benches/results/replica_rate_xhost.log`（表头 `REVSHOW=` 标了哪一份二进制）：

| 格 | 定长 1 MiB（`d660e77`）| 自适应（`486f5e1`）|
|---|---|---|
| 跨机追赶 78–95 MB / 102.5–124.8 万条 | 11.35 / 11.74 / 13.64 MiB/s（中位 **11.74**），15.7–18.8 万条/s | 17.56 / 18.83 / 24.46（中位 **18.83**），24.2–33.8 万条/s |
| 逐轮比值 | — | **1.29× / 1.66× / 2.08×** |
| 副本在追时主延迟 A/B | p50 +16%、p90 +3%、p99 +8%、吞吐 **−5%** | p50 **+31%**、p90 **+21%**、p99 +14%、吞吐 **−14%** |
| 冷启动 1 MiB 档（快照段当时还是定长） | 994 / 1008 / 1300 ms | 1098 / 1389 / 1514 ms |

**1.60×**（中位 11.74 → 18.83 MiB/s），低于同机的 1.88×。用两组数各反解一次
`t = F + k·B`（两点拟合，只当量级看）：跨机 89 → 212 ms/轮 ⇒ F ≈ 48 ms、k ≈ 41 ms/MiB；
同机 74 → 140 ms/轮 ⇒ F ≈ 52 ms、k ≈ 22 ms/MiB。**固定项两边差不多，差在边际项** ——
也就是链路 + 对端的写/fsync + 逐条 apply 这一路；这一轮没有再把它们分开，所以只能说
「跨机的边际成本约为同机的两倍」，不说具体卡在哪一段。

主侧代价是这一格新量到的：`ping` 打的是副本，看不到**主那一侧的 fetch**（`Wal::deliver`
对 4 MiB 做 locate + 逐条 CRC + `to_vec`，再一次性写进 socket），而 [3] 那一格量的正是
主自己。三轮同向：B 臂吞吐 49 855 / 45 656 / 46 415 → 43 068 / 43 737 / 40 583 ops/s，
而同轮 A 臂改后反倒更快（46 622 / 49 978 / 48 734 → 50 363 / 49 609 / 52 973），
所以这个差不是宿主漂移。**副本在追时主的吞吐代价从 −5% 变成 −14%、p50 从 +16% 变成 +31%** ——
这是「拿主的长尾换副本的追赶速度」，与同机 ping 那两行不矛盾（那两行量的不是这一头）。

冷启动那一格两组区间重叠（994–1300 vs 1098–1514），而且快照段走的还是 `--fetch-bytes`
定长 —— **这一笔的自增管不到它**，所以这一格与本次改动无关，当噪声看。它指出的下一个
旋钮就是把自增铺到快照段，见下面「快照段也自增」。

一件没做的：块长只在测试侧可见（`FollowerProgress::fetch_bytes`），`--stats` 那行
没加字段（要动 `Reply::Stats` 的编码，收益不够）。

#### 主侧的账：`WalFetch` 里九成时间在 crc（2026-11-23）

上一条把主侧代价归到「`Wal::deliver` 对块做 locate + 逐条 CRC + `to_vec`」就停下了，
没说是哪一段。本笔先量后改：一份 50 MB ／ 35 万条（120 B/条）的 WAL，只换 crc 实现、
其余代码逐字相同，各跑 10 轮取中位（一次性探针，`.spot/` 未入库）：

| 项 | 逐字节单表 | slicing-by-8 | 倍 |
|---|---|---|---|
| `crc32` 吞吐（4 MiB × 10） | 307.1 MiB/s | **1287.4 MiB/s** | **4.19×** |
| `deliver` 1 MiB 一块 | 4.04 ms | **1.50 ms** | 2.69× |
| `deliver` 4 MiB 一块 | 16.64 ms | **6.50 ms** | 2.56× |
| 35k ×（`crc32(120 B)` + `to_vec`） | 14.27 ms | **4.91 ms** | 2.91× |

读法：4 MiB 一块的 16.64 ms 里约 14 ms 就是 crc（307 MiB/s 反推 4 MiB = 13.7 ms），
**占九成以上**；`to_vec` 那 3.5 万次小分配只占 1.6 ms（同一张表最后一行的差值）。
所以修的地方是 `src/engine/wal.rs` 的 `crc32_update`：单表逐字节 → **slicing-by-8**
（`T[n] = T[n-1] 退一字节`，八张表 8 KiB，L1 放得下，一次吃 8 字节）。

零协议、零格式、零 API 变更：`crc32` / `crc32_update` 的签名与语义一字未动，所以
快照尾巴、`dump` 容器、前缀探针、迁移工具全部自动吃到。守门测试是
`slicing_by_8_matches_the_bytewise_reference`（0..=4096 字节全长度 × 「整体喂」与
「两段喂」两种形状，与逐字节参考实现逐位对拍 —— 只靠 `"123456789"` 那条已知向量
不够，它只走了一次 8 字节块）。

**跨机端到端**（脚本仍是 `benches/replica_rate_xhost.sh`，只用 `BIN=` 指向两套
二进制、`OUT=` 指到新日志 `benches/results/replica_crc_xhost.log`；两组各 3 轮；旧 =
`f5709cd` 的二进制，新 = 工作树）：

| 量 | 逐字节（`f5709cd`） | slicing-by-8（工作树） |
|---|---|---|
| A 臂（无副本）p50 中位 | 1 256 µs（1 080–1 309） | **1 057 µs**（1 056–1 141），**−16%** |
| A 臂（无副本）吞吐中位 | 46 663 ops/s | **51 977 ops/s**，**+11%** |
| B 臂相对 A：p50 | +25% | +24% |
| B 臂相对 A：p90 | +36% | **+19%** |
| B 臂相对 A：吞吐 | −18% | **−10%** |

既然能省下的是主侧 fetch 的 CPU，第一、二行就是最直接的证据：**主自己的写路径也吃
这条函数**（每条 WAL 记录一次 `crc32`），所以「A 臂变快」不是副产品。三、四、五行
才是副本在追时的代价：**p50 那一格没动（+25% → +24%）** —— 说明那一段的代价不止
crc，还有大块拷贝、内存带宽与事件循环占用；p90 与吞吐则收窄了一半左右。诚实的两
条边界：n=3；新组 B 臂的副本同期 lag 中位 **193 万条**、旧组只有 **154 万**，新组是在
更不利的条件下量到的，所以这组差值偏保守；p99 是单点分位，不做结论。

**附带的收益与边界**：`snap_prefix_crc`（换代探针，1 MiB 分块链式喂）与
`save_versioned` 的整份快照 crc 走同一个函数，按 4.19× 折算 130 MB 前缀探针从
约 440 ms 降到约 105 ms（**算术折算，未单独实测**）；跨机 [2] 追赶 20.10 → 24.02 MiB/s
（1.19×），但同轮 gfs2 的 4 KiB fsync 探测从 39 次/s 漂到 116 次/s，盘的状态不可比，
这一格只当旁证。

没做的三件：跳过 crc 校验（防御要保留，坏记录必须在交付路径上现形）；
PCLMULQDQ 硬件加速（要 CPU 特性分支，6.5 ms 里剩的那几 ms 不值）；把
`records: Vec<Vec<u8>>` 改成扁平缓冲（3.5 万次分配现在只占 1.6 ms，排在 crc 之后）。

#### 快照段也自增：冷启动的 tick 数不再等于块数（2026-11-22）

上一条把自增只铺在 WAL 段，冷启动最贵的那一段没吃到：`step_snapshot` 每 tick 下一块、
块长恒等于 `--fetch-bytes`，于是 22–26 MB 的快照在默认 1 MiB 下就是 22–26 个 tick
（下载期间不受 `--interval-ms` 限频，但每个 tick 仍是一次完整往返 + 一次 fsync，
两个方向都要付钱）。这一笔把同一条 `next_fetch_bytes` 铺过去，只换「积压」的度量：
WAL 段用主报的 `durable_seq` 差折算字节，快照段直接是「整份还剩多少没下」
（`total - off`）。两个复位点都在 `src/net/replicate.rs`：

- `start_resync` 把 `cur_bytes` 复位成 `--fetch-bytes` —— 两段各自从下限起步。上一段
  （WAL）可能已把块长顶到 4 MiB，直接带进快照段会让一次冷启动的第一轮就扛一整个
  大块，而它未必是最后几块；
- 装完快照（`install_snapshot` 成功那一路）也复位 —— 「配置值是下限」这个承诺不该
  变成「刚跑过快照段就一直保持上一块那么大」。紧接的 WAL 追赶会按当时的积压自己
  再放大。

**集成用例** `the_snapshot_chunk_ramps_while_downloading_and_returns_to_cfg` 钉四件事：
8 KiB 起、4 000 条 `Batch` 播种出 > 8 个块长的快照；下载途中 `fetch_bytes` 涨过配置值
且不超 `WAL_FETCH_MAX`；总轮数**少于**定长下的块数（`ceil(snap / 8 KiB)`）；
装完落回 8 KiB 且数据读得到。

**同机实测**（`benches/replica_rate.sh` 各连跑三次，快照 10.9 MB + WAL 5.2 MB）：

| 档 | 改前（`62b6fab`，定长） | 改后（`77fe7da` 之后那一笔，两段都自增） |
|---|---|---|
| 1 MiB/块起 | 696 / 750 / 773 ms（中位 **750**） | 551 / 573 / 610 ms（中位 **573**，**−24%**） |
| 4 MiB/块 | 397 / 420 / 470 ms（中位 **420**） | 391 / 439 / 511 ms（中位 **439**，噪声内） |
| 两档逐轮比 | 1.64–1.79× | **1.19× / 1.26× / 1.47×** |

4 MiB 档本来就一步到位，改动只该影响 1 MiB 档 —— 实测也是这样：1 MiB 档跑到了
4 MiB 档附近，剩下那截差值就是「装快照那一轮」（184–233 ms），与块长无关。

**台阶是 1 → 2 → 4 MiB。** 为看这一步临时加过 env 门控追踪（打完就删，不在树里）：
一次 12.6 MB 快照的 cap 序列就是 1/2/4/4 MiB，**4 轮下完**（定长下要 13 轮）。同一次
还撞上**快照换代重下**：主在下载期间又 checkpoint 一次，`SnapChunk.snap_seq` 变了，
`buf.clear(); off = 0` 整份重下 —— 先下的约 3 MiB 作废。这解释了两件事：① 1 MiB 档
偶尔多花约 90 ms，不全是 tick 数的账；② **下载窗口越短越不容易撞上换代**，而块长
放大本身就在缩短窗口。

**跨机复跑（同日，`benches/replica_rate_xhost.sh 3`，`benches/results/replica_rate_xhost.log`）**：
1 MiB 档 **851 / 912 / 1353 ms**（中位 **912**；改前 `486f5e1` 那组 1098 / 1389 / 1514，
定长 `d660e77` 那组中位 1008）、4 MiB 档 **550 / 586 / 627 ms**（定长那组
588 / 602 / 680）。与定长基线比，中位差 1.67× → **1.56×**，但两组区间大幅重叠 ——
装快照那一轮在 gfs2 上是 447–611 ms，比这次省下的百来毫秒还大，**所以跨机这一格
只能写「无回归」，不能写「变快」**。追赶与主延迟同轮复量：追赶 24.54 / 21.87 /
19.43 MiB/s（中位 21.87，同轮极差就有 26%，不与上一笔的 18.83 比大小），
p50 +14% / p90 +15% / 吞吐 −13%，与上一笔的代价同量级。

下一笔（下面「换代重下：前缀复用」）把这里那次追踪里的 3 MiB 白拉补上了：判据是
主算的前缀 crc，不是版本号或长度。

#### 换代重下：前缀复用（`Reply::SnapPrefix`，2026-11-22）

上一条追踪里那场「下载 12.6 MB 时换代、先下的 ~3 MiB 作废」就是这一笔要治的东西。
老做法是 `buf.clear(); off = 0`：换代一律整份重下 —— 简单、不会拼错，但把已经收下的
字节全扔了。协议早就支持按 offset 续取（`SnapFetch{offset, len}`），缺的是判据：
**新的一代与手里那份的公共前缀到哪里为止**。两代正文的公共前缀不在协议里承诺
（`nodes` 区一改就从很前面分叉），所以不能按版本号或长度猜。

判据做成一次显式探针：副本发现 `SnapChunk.snap_seq` 变了，先发
`SnapFetch { offset: 已下字节数, len: 0 }`（`len == 0` 原本就是合法的空读），主回
`Reply::SnapPrefix { prefix_crc, total, snap_seq, epoch }` —— `prefix_crc` 是
`state.snap` 前 `min(offset, total)` 字节的 crc32，与快照尾巴那个校验和同一个函数
（`crc32_update` 分 1 MiB 链式喂，不为一个校验和再复制一份同等大小的内存）。副本拿
自己 `buf` 的前缀算一遍 crc32：相等就 `truncate(keep)` 接着下，不等就归零重来。
`snap_seq`/`total` 与当前块不符也归零 —— 主可能正在算下一代。

安全边界有三条，缺一条都不敢这么写：

1. **判据是对内容的**，不是对时间的：crc 相等意味着那 N 个字节两代逐字节相同，不存在
   「猜错前缀」这一档；
2. **装快照那一步仍验尾部 crc**（§6.8）：即便探针在某处出了错，拼出来的字节也不
   可能被静默装进去 —— 最坏是一次 `EIO`，不是一份谁都不认识的快照；
3. **混版双向安全**：旧主不认识 `len == 0` 的新意思，回空 `SnapChunk`；新副本把任何
   非 `SnapPrefix` 的答复当「不可留」处理（§4.1 第五例，`PROTOCOL_VERSION` 留 6）。

`src/net/replicate.rs` 的落点是 `step_snapshot` 里的 `keep` 三支（同代 `off`、首块 0、
换代走 `probe_prefix`）与 `Follower` 的两个计数 `snap_reused` / `snap_restarts`
（只在测试侧可见，`--stats` 没加字段 —— 理由同上一笔）。两条 eprintln 是给现场用的：
`keeping X of Y B already downloaded` / `no usable prefix in the Y B ... restarting from 0`。

**集成用例**（`tests/replication.rs`：探针一条 + 换代三形状）：

- `a_snapshot_probe_reports_the_prefix_crc`：offset 0 / 1 / 4096 / 一半 / 整份 / 越界
  六个点上 `prefix_crc == crc32(&raw[..n])`，且 `len > 0` 仍回 `SnapChunk`；
- `a_snapshot_generation_change_keeps_the_downloaded_prefix`（Tail）：4000 条 `Batch`
  播种 + 一张一行的通用表（它在快照编码的最后一段），下一截后改那一行的值再
  checkpoint —— 前缀整体可复用，且 `snap_probes == 1`（整段对得上就不二分）；
- `a_snapshot_generation_change_keeps_the_common_prefix_mid_body`（Mid）：改中段那个
  节点的 `mode` —— 整段 crc 必然对不上，只有二分能把前半段留下；
- `a_snapshot_generation_change_with_a_different_prefix_keeps_only_the_head`（Head）：
  新建一个文件（`nodes` 条数变了）—— 分叉在第 32 字节，`snap_reused` 只剩一个头。

三条换代用例都断言 `resyncs == 1`、拼出来的快照与主的逐字节相同（Tail 另加
`snap_restarts == 0`，Head 另加 `snap_restarts == 1`）。块长取「整份的四分之一」：
`next_fetch_bytes` 只在落后超过 4 个块长时才翻倍，于是这条路径固定走 4 个块、块长
不变 —— 第 3 块结束时正好下了 3/4 且还在下载中，「换代落在下载中段」不再靠机器快慢
撞出来。断言不写魔法数，而是拿两份快照逐字节比出真实分叉点 `d`：
`snap_reused <= d`（安全：绝不保留不同的字节）且 `snap_reused * 4 >= d * 3`
（有效：边界收在 `d` 附近，误差在已下量的 1/256 以内）。

**同机实测**（`benches/replica_redo.sh 40 3`，原始输出 `benches/results/replica_redo.log`）。
口径：两臂同一份种子（119 万节点、快照 131 MB）；副本从 0 起跑，起跑 500 ms 后把主侧
WAL 推过 1 MB 触发**一次**换代；量「副本进程起 → lag 0」的墙钟，每臂三轮取中位。

| 形状（换代负载） | 老 `c5ff0b7`（整份重下） | 新（探针 + 前缀复用） |
|---|---|---|
| `rowput`（改动落在快照尾部） | 13 700 / 13 730 / 13 062 ms，中位 **13 207** | 9 899 / 10 123 / 10 036 ms，中位 **10 036** |
| `create`（`nodes` 条数变了） | 13 462 / 12 667 / 12 680 ms，中位 **12 680** | 12 451 / 13 009 / 12 883 ms，中位 **12 883** |

`rowput` **1.32×**（13 207 → 10 036 ms）。三轮都留下了一整段已下的前缀：51.0 / 63.0 /
51.0 MiB（34–42%）—— 剩下一半以上没下，换算成下载时间 2.7–3.3 s，与实际省的 3.17 s
对得上。`create` 三轮
都在 47–55 MiB 处判「不可用」并从 0 重来（`整份重下 1`），时间比 0.98× —— **这一格是
判据的反面证据，不是性能数字**。

**为什么要给副本限速**（`benches/throttle.py`）：本机 localhost 上 131 MB 快照约 1.5 s
就下完，而主侧一次 checkpoint 的编码要 3–5 s —— 不限速时换代永远落在下载结束之后，
两臂都只是「装完再 gap 一次」，测不到这一笔。代理把下行压到 20 MB/s（低于编码速率），
换代就必然落在下载中段。**跨机没有跑**：上一笔的跨机读数显示快照段在 gfs2 上的方差比
省下的还大，这一格的正收益需要「下载 > 编码」这个形状，跨机本来就更容易成立，但那是
推断，不是实测。

#### 换代重下：二分找最长公共前缀（2026-11-23）

上一条的探针只答「已下这 N 字节整段是否相同」。改正文中段的节点（前半段逐字节相同、
后半段分叉）时整段 crc 必然对不上，于是那百分之几十已下的前缀也一起扔 —— 上一轮的
`[create]` 形状就是这类「判据对、但只能答否」的极端（0.98×）。补法是二分：公共前缀
是单调的（前 k 字节相同 ⇒ 任何更短的也相同），整段 miss 之后再取
`mid = lo + (hi - lo) / 2`，`lo` 恒为「已知可留」、`hi` 恒为「已知不可留」，直到接壤
或用完预算 `PREFIX_PROBE_BUDGET = 8`（粒度 = 已下量的 1/256）。

**协议与引擎零改动**：`SnapFetch{offset, len: 0}` 本来就能问任意 offset 的前缀 crc，
多点几次全在副本侧（`probe_prefix` → `prefix_matches`）。主侧的代价是每次从 0 线性扫
到 `n`，最坏合计 ≤ 9 × 已下量（上界：二分的各点之和远小于这个数）；slicing-by-8 之后
1.3 GB/s，95 MB 的已下量上界 0.66 s，而 20 MB/s 下重下这 95 MB 要 4.8 s。分叉越靠后，
探针花得越多、留下的也越多；分叉越靠前，探针便宜、留下的少 —— 两端都不比「整份重下」
贵。三条安全边界与上一条相同（内容判据、装快照仍验尾部 crc、混版一律归零），没有
一处依赖「主的公共前缀有多长」。

`FollowerProgress` 多两个字段：`snap_off`（此刻已下到哪里 —— 原来不带）与
`snap_probes`（累计探针次数）；两条 eprintln 末尾带上 `(N probes)`，现场能直接看出
这次换代是「一次就中」还是「二分了 9 次」。

**同机实测**（`benches/replica_redo.sh 40 3`，原始输出
`benches/results/replica_redo_bisect.log`）。口径与上一条同形，只是种子换成单视图、
注入点提前到 2000 ms：两臂同一份种子（125 万节点、快照 149 MB / 正文 167 MB）；
副本经 20 MB/s 限速代理从 0 起跑，起跑 2 s 后把主侧 WAL 从 512 KB 推过 1 MB 触发
**一次**换代（实测换代落在 4.6–5.2 s，已下 95 MB ≈ 正文 57%）；量「副本进程起 → lag 0」，
每臂三轮取中位。旧臂是前一笔 `43f55b3`（探针只判整段）。

| 形状（换代负载） | 旧 `43f55b3` | 新（二分） | 比 |
|---|---|---|---|
| `rowput`（改动在最后一段） | 10 298 / 10 170 / 10 132 ms，中位 **10 170** | 10 101 / 10 147 / 10 112 ms，中位 **10 112** | 1.01× |
| `write --ino`（正文 29.8% 处） | 14 836 / 15 231 / 14 975 ms，中位 **14 975** | 12 586 / 12 764 / 12 665 ms，中位 **12 665** | **1.18×** |
| `create`（正文第 32 字节） | 15 676 / 15 162 / 15 017 ms，中位 **15 162** | 14 988 / 14 899 / 15 148 ms，中位 **14 988** | 1.01× |

三轮 `[mid]` 都留下 49 573 888 of 95 420 416 B（52%），换算成下载时间 2.48 s，与实际
省的 2.31 s 对得上；留下的字节数比真实分叉点（两份快照逐字节比出来的
`d = 49 886 009 B`）少 312 KB —— 正好在二分粒度（95 MB / 256 ≈ 373 KB）以内，方向
永远是「少留一点」，不会多留。`[row]` 与 `[create]` 两格是**快速路径与判据的反面
证据**：前者一次探针就留全部（1.01×，二分没把这条路径弄慢），后者分叉在第 32 字节、
二分满 9 次仍判 0（1.01×，不静默拼错）。

**收益的上限是几何的，不是常数的**：能省的字节 = 改动位置之前的已下量。改动在正文
30%、换代时已下 57%，上界就是 `0.30 × 149 MB ÷ 20 MB/s ≈ 2.2 s`（实测 2.31 s，多出来
的 0.1 s 是 `d` 落在 29.79% 而非整 30%）。改动落在已下范围之外时一格都省不下：第一
次校准跑（换代只下了 25%、改动在 35%）两臂都是 1.00× —— 那不是回归，是那一次换代
本来就没有可丢的字节。

这一轮顺带修掉了 harness 的两个坑，留在这里免得下次再踩：① `PRELOAD` 取
「1 MB − 8 KB」时，10 ms 的轮询间隔在 700 KB/s 的写速下就能越过阈值 7 KB —— 每个
轮次都在副本起跑前就触发了 checkpoint，脚本量到的「换代」其实是预装那一次，已下量
只有两成多；改成预装到阈值的一半，并在预装后断言 `wal < 1 MB`。② 注入点的睡眠写成
`sleep "0.$GAP_MS"`，`GAP_MS=2000` 就变成 0.2 s —— 换成按 1000 进制拆。上一轮
`c5ff0b7` 那组表（13.2 → 10.0 s）是在旧 harness 下测的，那是同口径的 A/B，结论不受
影响；但跨轮直接比绝对值没有意义。

**没做的**：二分只在「整段 miss」之后才启动，`[create]` 那种第 32 字节就分叉的形状
仍要问满 9 次（上界 0.66 s，远小于重下的 4.8 s），所以没有加「先小步试探」的启发式。
跨机没跑：理由同上一条（gfs2 上快照段的方差比省下的还大）。

### 6.7 备份与还原（实测三形状，2026-11-19）

「有 WAL + 快照」不等于「能备份」。这一节把三种做法逐个跑一遍，工具
`benches/backup_probe.sh`（逐档取 `--stats` 的 nodes/dentries/refs/seq 对拍，原始输出
`benches/results/backup_probe.log`）。三档都只用临时目录，不碰真数据。

| 形状 | 做法 | 实测结果（rev `d610663`，每档写 2 s） | 能当备份用吗 |
|------|------|--------------------------------------|--------------|
| **冷备份** | 写 → `SIGTERM` 优雅停服 → `cp -a` → 从拷贝起服 | nodes 13 968 / dentries 13 936 / refs 0 / seq 41 872 **逐项相等 ✓** | 能，RPO = 停服那一刻 |
| **热拷贝** | 写入进行中 `cp -a`，源端继续跑 | 拷贝前一刻 seq 41 872、拷完源端 89 536、还原出来 **56 272** | **不能当备份工具用**：能起服、无撕裂、状态自洽，但切点落在写入流中间（比「拷贝前一刻」多 14 400 条）—— 无法承诺还原到哪一刻，也无法向调用方报 RPO |
| **副本目录** | 主写 → `treedb-standby` 追平 → 把副本目录当普通主起服 | nodes/dentries/refs/seq 与主 **一致 ✓**（当时 `lag 0`、`resyncs 1`、`errors 0`） | 数据面等价于一份持续追平的异地镜像；**这一档当时不是提升**（那时没有 fence，旧主必须确认真停）。v0.4.0 起提升有了名字：`treedb-standby --promote` 抬令牌（§6.6「切主」） |

**v0.7.0 复跑（2026-11-24，rev `a749266`，每档写 3 s）**：三档的形状结论一条没变 ——
冷备份 nodes 23 616 / dentries 23 584 / seq 70 816 逐项相等；热拷贝的切点照旧落在写入
流中间（这一次比「拷贝前一刻」多 **11 448** 条 seq：拷前 70 816、拷完源端 128 440、还
原出来 82 264；上表那次是 14 400 —— 这个数每跑必变、**不可复现**，而「不可复现」正是
「切点不可控」的意思）；副本目录当普通主起服 nodes 22 168 / seq 66 472 与主一致
（`lag 0`、`resyncs 1`、`errors 0`）。同一轮顺带复跑 §6.10 在线备份四问（rev 同，每档
写 2 s）：无快照档从 seq 1 重放 15 142 条后计数与主相等；有快照档切面 seq 14 140 落在
主 [13 870, 14 542] 区间内且恢复水位等于切面；`--out -` 管道档 1 411 964 B 压成 163 595 B
且与同切面的文件版本逐字节相同；以及 §6.6 切主（`benches/promote_smoke.sh`，提升后
旧主回魂被 fence 拒收）。原始输出都追加在 `benches/results/` 的同名日志里。

三条顺带量出来的行为，都容易踩：

- **优雅停服自带最后一次 checkpoint**。冷备份拷贝里快照水位已经 == seq，所以还原
  不依赖尾日志重放；而 `checkpoints` 计数在新进程从 0 起 —— 拿 `--stats` 这两行做
  「备份是否成功」的断言会假红，脚本因此只比数据面那四行。
- **`checkpoint` 会抹掉水位以下的 WAL（同步档 `wal.reset`、自动档 `truncate_before`），
  所以不归档就没有 PITR**。不带归档时能还原到的时间点只有
  「最近一次快照 + 其后的尾日志」这一条线，历史切面不落地。要按时间点
  恢复就得在外面自己留 WAL 分段，或者靠副本（§8 第 14 条）。**2026-11-21 起这条有了
  正面答案**：`--archive-dir` 让 checkpoint 先拷后抹，`treedb-pitr` 按 seq 重放出一
  个新目录（§6.11）。
- **还原手段从三条长到六条，但增量 / 加密仍然没有**。§6.7 那三条照旧：停服后
  `cp -a` 目录、`treedb-standby` 拉一份持续追平的镜像、`treedb-migrate` 只处理快照格式
  版本（§6.5）。2026-11-21 多了三个**文件/时间形状**：`treedb-dump` / `treedb-load`  把整份状态
  导成自描述文件再重建目录（§6.9，要求目录独占），`treedb-backup` 从在线主拉同一格式
  的一致切面（§6.10，不停服），`--archive-dir` + `treedb-pitr` 把历史切面按 seq 回拨
  （§6.11，归档本身要一直开着）。三者都不做增量、不加密。

### 6.8 快照的自校验尾巴与数据目录互斥（2026-11-20）

上一轮审查里发现的两个「事后无法区分于盘坏了」的洞，补在两处。

**快照尾巴从「裸 seq」变成「可自证的 seq」**。v3 快照的文件尾现在是 16 字节：

| 位置 | 长度 | 内容 |
|------|------|------|
| 正文 | n | 按 `SNAP_VERSION` 编的状态 |
| `crc` | 4 | `crc32(正文 ‖ seq)`，小端 |
| `seq` | 8 | 快照水位（旧尾巴就是这 8 字节） |
| `magic` | 4 | `0x3153_4254`（`"TBS1"`）|

读侧只看最后 4 字节是不是 `magic`（`State::snapshot_split`）：命中就按 16 字节切并校验
`crc`，不命中按 8 字节切（凡 `v0.3.6` 及更早写的目录）。两条路都**不升版本、不要求迁移**：
旧目录照常开，下一次 checkpoint 自动写成新形状。之所以用 magic 而不是升 `SNAP_VERSION`，
是因为尾巴变长不影响正文编码 —— 升版本会把所有存量目录推上迁移工具（§6.5），而它们
本来就还能读。

同时把 `State::load` 的尾部检查从「允许有余量」改成无条件 `remaining() == 0`：
「正文没吃完」只可能是三种事 —— 文件被截短、被追加、或这个版本号不是写它的那个版本号。
以前这三种都会被当成正常读完，然后拿一份**前缀状态**起服：表面上一切正常，丢的是一段
dentry。副本 `install_snapshot` 也走同一条校验（`crc` 不过就不安装），因为副本拿到的是
网络上的分块数据，那里比本地盘更容易出位翻转。

**一个数据目录只能被一个 `Engine` 打开**。`<dir>/.lock` 上 `LOCK_EX | LOCK_NB`，拿到锁后把
pid 写进去；拿不到就是启动错误（退出码 1，错误里带持锁 pid），不是「等一下」。以前两个实例
开同一个目录不会报任何错：各自重放同一份 WAL、各自 checkpoint、各自 `wal.reset` —— 谁后
rename 谁赢，先那个以为自己的写已落盘，而这种损坏事后无法区分于盘坏了。`flock` 跟着
open file description 走，进程怎么死的（包括 `kill -9`）都自动释放，所以不需要清陈旧锁。

两个实现细节值得记下来，因为它们都是踩过的：

- **同进程重复打开靠一张注册表判，不靠 `flock`**。`flock` 认的是 open file description，
  而同进程两次 `open` 就是两份 description；更麻烦的是 `Command::spawn` fork 出的子进程
  在 exec 之前会短暂继承同一份拷贝 —— 这期间父进程 `close` 并不**真正**解锁。只靠 `flock`
  判「同进程重复」会得到时有时无的假冲突（测试串行必过、并行随机失败）。现在进程内另有一张
  规范化路径的注册表先把这条判死，`flock` 只负责跨进程那一半；pid 文件里恰好是本进程 pid、
  但注册表里没有席位时，按 fork 窗口处理（5 ms × ≤ 20 次重试）。
- **`treedb-migrate` 也拿同一把锁**。它以前只在改写前后各读一次源文件比对，服务端没停就
  动手只能靠这个哨兵赌输（旧 §8 第 12 条那句）。现在「服务端没停」的结局是拿不到锁 →
  退出码 1，而不是把新快照盖到正在跑的主上。

**停服与启动参数也不吞错**。三处「静默降级」一起改掉：

- `SIGTERM`/`SIGINT` 的收尾 fsync + checkpoint 以前是 `let _ = …`，失败也以 0 退出 ——
  等于告诉运维「干净退出」，而实际上下次启动要重放整个 WAL，或者下次 checkpoint 才炸 EIO。
  现在清理先做完（关 epoll、关连接），最后再抛：失败打 `FATAL` 并以 1 退出。
- `--durability alway` 以前静默落 `group`（拼错一个字母就把持久性降了一档）。现在
  `Durability::parse` 只认 `always|group|never`，其余是 usage error，退出码 2。
- `--workers abc`、`--sync-ms x` 这类以前走 `.parse().unwrap_or(默认值)` 的数字旗标同样
  fail-closed（退出码 2）：`--sync-ms 0` 与不给完全是两件事（前者关掉周期 sync），不该靠猜。

### 6.9 离线 dump / load（2026-11-21 新增）

§6.7 那三条还原手段各有各的缺口：`cp -a` 要停服且只认同一代二进制，`treedb-standby`
是持续镜像不是一次性快照，`treedb-migrate` 只动布局不换地方。补第四个形状：
**把状态导成逻辑记录流，再从它重建目录**。

```text
treedb-dump --dir /var/lib/treedb --out /backup/treedb.dump             # 导出
treedb-dump --dir /var/lib/treedb --out - | gzip > /backup/t.dump.gz    # 管道档
treedb-load --in /backup/treedb.dump                                    # 只体检，不写
treedb-load --in /backup/treedb.dump --dir /srv/treedb-new              # 重建目录
```

**为什么不是拷 `state.snap`**。快照是内部格式，读它的人必须跟着 `SNAP_VERSION` 走、
跨代还要 `treedb-migrate`；dump 走的是**日志标签表**（`Log`，tag 1..=27，历史上只追加；
11–18 是四组专用控制面表的八个变体，§11 第 10 条 M4 删掉后留空不回收；五个不直接
落终态的变体不进 dump —— 容器 `LOG_BATCH` 23（导出的是它展开后的那些）、
`LOG_ADD_COLUMN` 24（加列后的整张表定义）、`LOG_CLEAR_TABLE` 25 / `LOG_DEL_RANGE` 26 /
`LOG_RENAME_TABLE` 27（清空后的空表、删完后的剩余行、改名后的新名）—— 所以 dump 里实际只会出现
1..=10 与 19..=22），
所以它跨版本、跨机器、跨目录能读懂，也能被人用 `xxd` 一条条看。容器长这样：

| 偏移 | 长度 | 内容 |
|------|------|------|
| 0 | 8 | 魔数 `TDBDUMP2` |
| 8 | 4 | 容器版本（现在是 2，未知值 fail-closed；v1 的容器头多一个 `next_audit_seq`） |
| 12 | 8 | `seq` 水位（导入后写进快照尾巴） |
| 20 | 4 | `next_view_id` |
| 24 | 4 | `next_table_id` |
| 28 | 8 | 记录条数 |
| 36 | … | 逐条 `长度 + Log` |
| 末尾 | 4 | `crc32`（覆盖前面全部字节） |

两个 `next_*` 必须在容器头上，理由不是洁癖：`State::apply` 能自己抬 `next_view_id` /
`next_table_id`，但**空表时它们没有任何 `Log` 能表达**，丢了就会在导入后从 1 重发，
把旧 id 复用。读侧取「重放结果」与「容器头」的较大者：正常 dump 两者相等，取 max 只为
挡手改文件把 id 绕回去。

**重建序列的顺序不是任选的**，三条硬约束都在 `state_to_logs` 里：视图先于节点与目录项
（目录项只带 view id，要反查）；节点先于目录项（`PutDentry` 用 nodes 推冗余的 `kind`，
反了就全落成 `DENT_KIND_UNKNOWN`）；表先于行（行的二级索引由 apply 按 `TableDef` 重建）。
其余各段无依赖，按 BTreeMap 键序输出 —— 导出因此是**确定性**的，于是「`dump →
restore → dump` 字节相等」可以当判据（`tests/dump_restore.rs` 就是这么断言的）。

**load 只写 `state.snap`**（水位 = dump 的 `seq`），不写 WAL：导入后「快照之后没有
任何写」是字面意思，恢复退化成一次快照读取 + 零条重放。`write_snapshot` 被同步
checkpoint、旁路快照与 `restore_dump` 三处共用，所以三条路径写下去的字节同形 ——
下面实测里「还原目录的 `state.snap` 与源**逐字节相同**」正是这条同形的副产品。

两侧都要求目录独占（同一把 `.lock`），load 还额外拒非空目标：往有数据的目录里合并
是另一种没有契约的操作。**`fence` 令牌不搬**：那是「谁是当前主」的部署态，不是逻辑
数据；从提升过的副本导出的 dump 载回来按新主（fence 0）起步，要 fencing 的拓扑由
调用方重新发号（§6.6）。

实测（2026-11-21，本机，`treedb-bench --op create --secs 2 --conns 4 --threads 4`
灌出 32 views / 82 888 nodes / 82 856 dentries，`seq 248 632`，优雅停服后快照
10 825 876 B）：

| 动作 | 耗时 | 产出 |
|------|------|------|
| `treedb-dump --out <文件>` | 279 ms | 11 654 728 B（165 776 条记录） |
| `treedb-load --in <文件>` | 265 ms | 还原目录的 `state.snap` 与源**逐字节相同**（`cmp` 静默） |
| 再从还原目录导一次 | 280 ms | `cmp` 与第一份 dump **逐字节相同** |
| `--out -` \| `gzip -1` → `gunzip` \| `--in -` | 334 + 320 ms | 压缩后 1 133 299 B，还原后快照仍逐字节相同 |

还原目录直接能服务：`--stats` 报 `views 32 / nodes 82 888 / dentries 82 856 / seq 248 632
(snapshot 248 632) / wal 0 B logical`，`--op readdir` 跑 1 s 得 73 076 ops/s、`errors=0`。
注意 WAL **文件**在开服后仍占 4 MiB（预分配），所以「没写 WAL」的判据是逻辑长度与
可重放条数（0），不是 `st_size`。

拒绝路径也逐条跑过（都是退出码 1，且都在动字节之前）：服务在跑时 dump →
`data dir ... is already locked ... pid`；load 到非空目标 → `refusing to restore into ...`；
改一个字节再 load → `dump crc mismatch: ...`；缺参数 → 退出码 2。

**边界**：dump 把整份状态读进内存再展开成记录流（源 `State` + 记录流同时在手），所以
它是离线工具；不停服的那一条见 §6.10；按时间点回拨见 §6.11。也没有增量、没有加密：
`seq` 只是水位，不是可回拨的时间轴（要可回拨得先开 `--archive-dir`）。dump 文件比快照大约 7.7%（11.65 MB vs 10.83 MB）：换的
是「不依赖 `SNAP_VERSION`、人眼可读」这一层自描述成本。

### 6.10 在线一致快照（`treedb-backup`，2026-11-21 新增）

§6.7 的三形状有一个共同前提：要拿一致切面就得停服，或者常驻一个副本。§6.9 的
dump 更进一步 —— 它要目录独占，服务在跑直接拒。补第五个形状：**从在线主拉一份
一致快照，落成与 dump 同格式的文件**。

```text
treedb-backup --primary 10.0.0.1:7777 --out /backup/treedb.dump
treedb-backup --primary 10.0.0.1:7777 --out - | gzip > /backup/t.dump.gz   # 管道档
treedb-load --in /backup/treedb.dump --dir /srv/treedb-new                 # 恢复
```

**它没有发明任何协议**：两条腿都是 §6.6 给副本开的那两条只读 op。

| 段 | op | 语义 |
|----|----|------|
| 状态主体 | `SnapFetch { offset, len }` | 分块读主的 `state.snap`；`snap_seq` 换代即整份重下 |
| 增量尾巴 | `WalFetch { want_seq, hint_off, max_bytes }` | 交付 `seq >= want_seq` 的 WAL 记录**原字节**，上界是主已 fsync 的 `durable_seq` |

**为什么拼出来的一定自洽**，三条一起看：

1. WAL 只追加、不改历史 —— `(S, T]` 这段重放出来的状态就是主在 seq `T` 那一刻的状态；
2. `WalFetch` 的上界是 `durable_seq`，不是「文件里已经写出去的字节」—— 因此切面里
   不会有主崩溃时可能回滚的写（与副本同一条规矩）；
3. 快照自带 `seq` 与 crc（§6.8），拼错代际会在读取时就被拒，而不是悄悄拼出一份
   两个时代都不属于的状态。

**目标水位在第一轮 `WalFetch` 就固定**（`stop_at` = 该轮返回的 `durable_seq`），此后
不再上移。这条是实测逼出来的：最初写成「一直追到追平」，主持续写入时备份耗时与
写入时长同步增长（3 s 写入 → **3 336 ms** 备份，仍然追不完），而且说不清切面落在
哪一刻。固定之后同一个库、同样 3 s 写入 → **322 ms**，与写入量无关 —— 备份是
「取某一刻的切面」，不是「跟上写入流」（后者是副本的职责）。

**RPO = 「第一次 `WalFetch` 那一刻主已确认的最大 seq」**。要把切面钉死在备份
启动那一刻的调用方，可以先读一次 `--stats` 的 `Stats.seq` 再启动备份，两者之差
就是这份文件的 RPO 上界（量级是快照下载耗时加一次往返）。

**两条要重来的失败态**，判据与副本逐个对应：

- **`gap`（主在备份期间 checkpoint 追过了头）**：`want_seq <= snap_seq` 时主回
  `gap=true`，整份重来。这不是错误而是「没跟上」，`--retries`（默认 3）管次数；
  触发与否取决于主自动 checkpoint 的周期与备份窗口的重叠。
- **`epoch` 中途变动**（备份期间发生 `--promote`）：同样整份重来 —— 快照与尾巴可能
  来自两个时代，拼起来没有意义。

两次实测都没撞上这两条（备份窗口在本机是 50–400 ms 量级，主每 1 MB WAL 换一次快照），所以
验收没有把「一定重来过」写成断言；替代的钉子是把**重试循环本身**钉死
（`--retries 3` 必须真的转满 3 轮，见下面第 3 条用例）。

**实测（2026-11-21，本机 `127.0.0.1`，主 `--workers 4 --auto-checkpoint-mb 1`）**：

| 场景 | 主的数据量 | 产出 | 耗时 |
|------|-----------|------|------|
| 无快照（`snap_seq 0`，从头重放 WAL） | 32 views / 5 371 nodes / seq 16 081 | 748 KB，10 742 条 | 79 ms |
| 快照 + 尾巴（主静置） | 32 / 10 577 / seq 31 699 | 1.47 MB，21 154 条 | 107 ms |
| 快照 + 尾巴（主在写 3 s，未触发重来） | 64 / 78 529 / seq 235 523 | 10.97 MB，157 058 条 | 322 ms |
| 同上，连跑 5 次（主持续写 1 s/轮） | 64 / 86 532→97 033 | 12.1→13.5 MB | 353 / 342 / 328 / 365 / 405 ms |
| 管道档（`--out -` \| `gzip -1`） | 32 / 10 577 / seq 31 699 | 1.47 MB → 161 KB | 74 ms |

每次摘要都打印四段：切面 `seq`、快照水位与字节数、尾巴区间与条数、以及 7 张表的
行数 —— 最后一组与主 `--stats` 的同一组逐格相等（上表五档都核过）。

表里是**某一次**的数（每轮写入量不同，耗时随之变）；同一组关系可以复现：
`bash benches/backup_online.sh [每档写入秒数]` 跑四档（无快照 / 快照+尾巴 / 写入中 /
管道），四档全 ✓，原始输出落在 `benches/results/backup_online.log`。

**验收**在 `tests/backup_online.rs`（3 条，全部真起子进程）：

1. `online_backup_equals_the_instance_it_came_from` —— 静置后备份；停机取源目录的
   `State` 作期望，`treedb-load` 恢复出来的 `state_snapshot()` 必须**逐字段相等**；
   同时要求「备份前后 `Ping` 都通」（在线）与「`backup → load → dump` 逐字节相同」
   （确定性）。
2. `an_online_backup_is_a_consistent_slice_while_the_primary_keeps_writing` —— 主每
   400 条换一代快照的同时持续写；断言切面 `seq ∈ [备份前, 备份后]`、`nodes` 不倒退、
   恢复后水位等于 `dump.seq` 且 `wal_records == 0`。
3. `cli_exit_codes_are_the_scripting_contract` —— 2 = 参数不全/未知参数，
   1 = 连不上，且 `--retries N` 真的转满 N 轮（`gap` 走的就是这条 `continue`）。

**边界**：仍是**全量**备份（没有增量/差异）；**不带 WAL 归档**（要回到 checkpoint 之间
的任意一秒得看 §6.11）；不搬 `fence`（备份是内容不是权杖，要提升由 `treedb-standby
--promote` 发号）；不做压缩（管道交给 `gzip`，摘要走 stderr 以免污染字节流）。主端
**零改动**：它只是被「副本之外的另一类只读客户端」读了同一对 op，`PROTOCOL_VERSION`
不动。要持续追平的镜像仍用 `treedb-standby`；要能停服的物理拷贝仍用 `cp -a`；
`treedb-backup` 补的是中间那一格 —— **不停服、一次性、可跨机器搬**。

---

### 6.11 WAL 归档与时间点恢复（`--archive-dir` / `treedb-pitr`，2026-11-21 新增）

§6.7 / §6.9 / §6.10 加起来有五种还原形状，但它们能回到的时间点只有一类：**某一次
checkpoint 的水位**。本节补的是第六种，也是唯一能落到 checkpoint 之间任意一条 op
末尾的一种。原因是 checkpoint 的副作用本身就是「把 `(旧水位, 新水位]` 的
日志抹掉」—— 同步档 `wal.reset()` 整份重写、自动档 `truncate_before()` 搬走前段。
抹掉之后那一段只剩快照里的一格，于是「回到两小时前」这件事在归档之前根本不成立，
能选的粒度由「多久折一次日志」决定，而不是用户要的粒度。

本节补的是**历史切面归档 + 按 seq 恢复**。它不发明协议（`PROTOCOL_VERSION` 仍 6，
`Op` 表一条没加）：归档是数据目录内部的副作用，恢复是一个离线命令。

```text
# 服务端：每次 checkpoint 先把要抹掉的那段拷走，再动源目录
treedb-server --dir /srv/treedb --archive-dir /backup/treedb-wal

# 恢复：把归档 + 活日志重放到某个 seq，落成一个新目录
treedb-pitr --dir /srv/treedb --archive /backup/treedb-wal --to-seq 918273 \
            --out /srv/treedb-0918
treedb-pitr --dir /srv/treedb --archive /backup/treedb-wal --to-seq 918273 --dry-run
```

**布局是自描述的，不另存清单**（`src/engine/archive.rs`）：

```text
wal-<from:020>-<to:020>.wal   # 原始 WAL 帧串，覆盖 (from, to]，可直接被 read_records 读
snap-<seq:020>.snap           # 与数据目录 state.snap 逐字节同源（拷贝，不重新编码）
```

不写清单的理由：清单与内容会分叉（rename 之后、写清单之前崩），而文件名就长在文件上，
扫一遍目录拿到的就是真值；完整性由两侧原有的校验各自负责（帧 CRC 与快照尾巴的 crc32）。

**四个顺序**，每一个错了都会产出一份「看起来连续」的假历史：

1. **归档在 checkpoint 动任何字节之前**。同步档在 `checkpoint()` 的最前面；自动档在
   后台那条线程里、`write_snapshot` 之前 —— 放提交线程上等于把「停轮与状态大小无关」
   （§8 第 7 条）当场取消，因为拷段与拷旧快照都是 O(日志 + 状态) 的字节搬运。
2. **段先于快照**。段是不可再生的（拷成功之后源记录立刻被抹），快照丢了只让锚点往前
   退一格（更早的锚点 + 更长的段还能重放到同一刻）。
3. **首条必须是 `from + 1`**（`Wal::copy_range` 里查）。`from` 是快照水位，日志里
   ≤ 它的记录本该早被上一次 checkpoint 抹掉；第一条不是它就说明段中间有洞，此时宁可
   让 checkpoint 失败，也不产出一个连续外观的归档 —— 后者要等到真做 PITR 才发现，
   而那时源日志已经没得对账了。
4. **归档失败 = checkpoint 失败**（fail-closed）。盘满、归档目录不可写都会让
   `commit` 之后的 `checkpoint_poll()` 把错抛给事件循环（进程退出）；此时 `truncate_before`
   尚未执行，源日志原样在盘上，重启后那次 checkpoint 可以重来（同一段重拷一遍是幂等的，
   重复的 seq 内容相同会被 PITR 接受）。历史不能因为「归档盘满了」而静默丢掉。

**恢复算法**（`plan_pitr` + `pitr_restore`）：

- **锚点** = 「水位 ≤ `to_seq` 的最大快照」，候选是归档里的 `snap-*` 与源目录自己的
  `state.snap`（后者覆盖最后一次 checkpoint 之后的尾巴），都没有则是空状态（seq 0）。
  取最大是为了让重放最短，也让「恢复到刚刚才过去的某一秒」这种最常用请求落在活 WAL 上。
- **拼段**：所有归档段 + 源目录活 WAL 的记录按 `seq` 收进一张 `BTreeMap`；同一 seq
  出现两次且内容不同就报错（两条历史都在，选哪条都是编造），内容相同则接受（重试幂等）。
- **缺口**：`anchor + 1 ..= to_seq` 必须一条不缺。缺一条就报「缺的是哪条、下一个实际存在
  的是哪条」，**不猜、不跳过** —— 跳过一条就是悄悄产出另一份历史。目标越过现有末尾
  时报「历史到哪儿为止」。
- **落点**：目标目录必须不存在或只含本工具自己的 `.lock`（与 `treedb-load` 同一个
  `open_restore_target`），只写 `state.snap`（水位 = `to_seq`），**不写 WAL、不搬 `fence`**
  —— 恢复出来的目录直接 `treedb-server --dir` 就能起，起来看到的就是那一刻。

`--dry-run` 走的是**同一条**规划路径（同样的锚点、同样的缺口校验、同样的重放），只是
最后不落盘：它证明的是「这份恢复会不会成」，不是「另一份账算得对不对」。

**实测（2026-11-21，本机同一块盘；`treedb-server --archive-dir ... --auto-checkpoint-mb 1`）**：

| 场景 | 输入 | 结果 |
|------|------|------|
| 归档产出 | 两次 3 s `create`（2 487 + 1 393 ops/s），自动 checkpoint 2 次 | `wal-0-14036.wal` 1 048 696 B、`snap-14036` 604 644 B、`wal-14036-28013.wal` 1 048 770 B |
| dry-run 到 20 000 | 锚点 `snap-14036`，归档段 1 条 | 5 964 条记录，40 ms，零写入 |
| 恢复到 14 036（正好一个锚点） | 零条重放 | 76 ms，写出 `state.snap` = 604 KB 档 |
| 恢复到 35 015（活库水位） | 锚点 `state.snap`@28 013 + 活 WAL 7 002 条 | 86 ms |
| **等价性** | 恢复目录 dump vs 活库 dump | 11 677 nodes / 11 661 dentries —— **1 633 029 B 逐字节相同** |
| 越界（40 000 > 35 015） | — | 退出码 1，`history ends at 35015` |

最后一行才是这个功能的验收判据：恢复到活库水位时，两份逻辑 dump **逐字节相同**，
说明「归档段 + 活 WAL」拼出来的状态就是活库本身，不是「看起来差不多」。

**验收**在 `tests/pitr.rs`（11 条）：归档布局与「段 = 源 WAL 前缀」逐字节比对；后台
快照也归档且先拷旧快照再覆盖；两个中间点（归档锚点 / 活 WAL 尾巴）恢复后状态逐字段
相等且能继续写；目标正好落在快照水位时零条重放；中途关掉归档后停在洞的左边；归档丢
一段时报缺口并指出下一个存在的 seq；越过末尾；目标非空拒绝且不动字节；归档目录不能
是数据目录；不开归档时目录里只有快照 + WAL + `.lock`。

2026-11-24 又补两条，专测 v0.7.0 那三条批量记录（`Log` 25/26/27）走不走得通这条路：

- `bulk_logs_replay_from_the_archive_segment` —— 三个点必须**严格落在两次 checkpoint 之间**。
  这里踩过一次自己的坑：`checkpoint()` 返回的水位就是它覆盖的最后一条，所以「rename 之后
  立刻 checkpoint」会让 `q == s2`，恢复直接从锚点快照取、归档段压根没被读；要多写一行把
  水位推过去。断言 `anchor == s1` 且 `from_wal == 0`（从活 WAL 取就等于没测归档）且
  `from_archive > 0`（同值消除不该把批量记录省掉），恢复目录重开后状态逐字段相等、行集也对。
- `bulk_pitr_restore_matches_a_cold_dump_byte_for_byte` —— 用本节那条等价判据（恢复到末尾的
  目录与同库停服冷 dump `Dump::encode()` 后**逐字节相同**）钉住含三条批量 op 的一段历史，
  只看视图数 / 行数会放过字段级偏差。

两条都在默认回归板上：`cargo test --release` **225 passed / 0 failed / 3 ignored**
（2026-11-24，rev `a749266`；补这两条之前是 223 / 0 / 3）。

**边界**：

- **没有增量**：每个段就是那一段原始 WAL，压缩/去重/加密都不做（要压缩就自己 `gzip`
  归档目录，或者把 `--archive-dir` 指到一个开了压缩的文件系统上）。
- **没有保留策略**：归档只增不减 —— 自动删除（按时间/按大小）需要一套「还应该回到多早」
  的契约，而这个契约属于部署侧。运维得自己 `find -mtime` 清老段；清之前请确认那些快照
  还在，否则洞左边的历史就只剩下洞。
- **开关必须一直开着**：关掉归档期间做过的 checkpoint 只在当时的数据目录快照里活一次，
  下一次 checkpoint 一覆盖就永久没了。此后 PITR 越过那个洞会报「历史到哪儿为止」。
- **`--to-seq` 取 `--stats` 打出的 `seq` 行**：一条 op 可能产生多条日志（`Create` 是三条），
  目标落在组中间会得到「该 op 只生效了一半」的状态。WAL 里没有组边界标记，这一层查不出
  来，所以不猜也不拦 —— 取一次 ack 之后的读数就是组末尾。
- **`--dir` 要停服（或冷拷贝）**：归档只覆盖到上一次 checkpoint，尾巴要靠活 WAL 补，
  而活 WAL 被活着的主继续写时那一段会一直变。库本身不抢目录锁（它不该让一次恢复把
  主锁死），所以这条是使用契约。
- **归档与数据目录不同盘时**，checkpoint 多一次跨盘字节搬运；同一块盘上则只是多一次
  写 —— 量级见上表（1 MB 段 + 604 KB 快照 / 次 checkpoint）。

---

## 7. 性能实测（五轴）

口径提醒：本节各节里的「本轮」指**它所记录的那一轮**（v0.3.1 读轴、v0.3.2 写轴），
不是本文开头那句 v0.4.0 —— 从 v0.3.3 起那五批（能力面、快照迁移、日志传送、接入
认证、切主）都没改过稳态读写路径，所以本节数字一个没重跑；**例外是 §7.11**（`v0.3.5` 上重测的
单连接格）与**§7.13**（`v0.3.6` 上新增的三臂同口径一轮）。本轮新量的那两列（握手开/关）
不混进本节，它们单独落在
§4.4 末表与 `benches/results/handshake_cost.log`；与本轮同批跑的受控 A/B
（`benches/results/ab_layout_v0.3.6.log`，两臂都不带密钥）只用来回答一个问题：
这 2 op / 2 reply 的改动有没有碰到未开认证时的读路径 —— 八格里最差一格 −0.90 %，没有。

复现：`benches/throughput_matrix.sh [每格秒数=3] [轴…]`，原始逐格输出在
`benches/results/matrix.log`。下面只报表格化的结论。注意脚本默认只跑轴 1–4，
§7.5 那张读并行与写档回退表来自轴 5，要显式传 `5`。三臂那一轮用
`benches/three_way.sh`（`SECS`/`OPS`/`CONNS`/`PIPES`/`WORKERS` 四个环境变量控制形状），
表由 `benches/three_way_table.py` 重算（§7.13）。

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

> 表里 `1/1` 那两行是单连接形状，`v0.3.5` 上已单独重测并配了 fsync 计数器对照，
> 看 §7.11。与本表不同日期，两边的数字不要互减（§7.10 那条教训）。

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
- **`workers=1` 那一列「输给 PG 0.26–0.48×」这句已作废**。它是拿 treedb 的一个核去比
  PG 不限亲和性的 64 个 backend，而那个比值从未同时记下对面吃了几个核。本轮四臂重测
  （§7.5 末）：同核 `t1/p1` 是 **9.0–86×**，除掉核数的每核 `t1/pf` 是 **13.4–102×**；实际用到的
  五个 op（`ping`/`lookup`/`readdir`/`create`/`write`）在同核口径下全部碾压，最低的一格也是 9×。
  只剩「1 核 vs 46–52 核」那个绝对口径还低于 1（`ping` 0.29、`lookup` 0.56、`readdir` 0.60），
  而单核 `ping` 153 329 已是**同核裸 epoll echo 服务端中位 167 999 的 91.3 %**。这一列到顶了，
  不是没压榨；它也不是真实使用时的档位（本机默认 `workers=8`）。
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
- **单核那一列是实的**：独立复跑与矩阵逐格咬合（0.99 / 1.02 / 0.97）。但当时把它换算成
  「对 PG 只有 0.25–0.49×」是一句口径错配的话 —— 那个 PG 格子不限亲和性（本轮实测
  它摊在 46–52 个核上）。「一个核打不过 64 个 PG backend」确实不是量具问题，可它也不是
  效率问题：两边都只给一个核时是 9.0–86×，见 §7.5 末的四臂表。

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

**四臂口径：把并行度从比值里摘出去**（本轮重测；`benches/sc_ops.sh 3 3`，输出在
`benches/results/sc_ops.log` 的 `run=final` 段）。上面那句「`workers=1` 输给 PG
0.26–0.48×」的全部问题在于：treedb 这边钉死一个核，PG 那边 64 个 backend 不限亲和性——
而这一轮把核数记下来之后，PG 那一臂**真的摊在 46–52 个核上**。一个核对四十几个核，
比值里装的是并行度，不是效率。四臂各管一件事：

| 臂 | 约束 | 用来回答 |
|---|---|---|
| `t1` | treedb `--workers 1`，`taskset` 钉 CPU 1 | 被测对象 |
| `p1` | PG postmaster 钉**同一个** CPU 1（backend 是 fork 出来的、继承亲和性，脚本读回 `Cpus_allowed_list` 核验） | 同核效率 |
| `pf` | PG 不限亲和性，但**实测记下它吃了几个核** | 历史严格判据 |
| `t8` | treedb `--workers 8`，不钉 | 参考上限 |

同一次运行、同一台机器、同一块 `/data` 机械盘、同并发 conns=64/pipe=1，每格 3 s × 3 次取中位：

| op | `t1` | `p1` | `pf`（实测核数） | `t8` | t1/p1 同核 | t1/pf 绝对 | t1/pf 每核 |
|----|------|------|-----------------|------|-----------|-----------|-----------|
| `ping` | 153 329 | 17 035 | 528 940（46.3） | 1 124 353 | **9.0×** | 0.29× | **13.4×** |
| `lookup` | 119 001 | 6 501 | 210 990（49.8） | 1 066 281 | **18.3×** | 0.56× | **28.1×** |
| `readdir` | 87 075 | 4 486 | 145 937（51.5） | 861 270 | **19.4×** | 0.60× | **30.7×** |
| `create` | 65 122 | 1 583 | 1 790（1.16） | 95 032 | **41.1×** | 36.4× | **53.4×** |
| `write` | 71 213 | 828 | 1 117（1.21） | 102 103 | **86.0×** | 63.8× | **101.5×** |

三列得一起给，因为它们各自会骗人：

- `t1/p1` 是**同核绝对吞吐**，这才是「单核配置下 vs PG」的验收口径。p1 那臂 PG 只吃掉
  了 0.06–0.13 个核（p50 3.6–48 ms），但那不是它省下来的性能，是 64 个进程在同一个核上
  抢 CFS 时间片抢不动 —— 把整整一个核给它，它就只有 17 035 ops/s。
- `t1/pf 绝对` 是唯一低于 1 的那列，而它比的是 1 个核对 46–52 个核。treedb 这边的并行度
  开关就是 `--workers`，拿关掉它的档位去要求赢，等价于要求 1 个核做出 47 个核的事。
- `t1/pf 每核` 把核数除掉之后是 13.4–102×。

**「一个核的天花板」现在有了仪器读数**：`benches/lb_floor.c` 是一个不碰 treedb 任何代码
的最小服务端（`epoll_wait` + `recv` + `send`，收到什么回什么，无业务无分配），服务端线程
钉同一个 CPU 1、客户端每连接一条线程不钉核。同口径 64/1 本次三次实测
**166 934–168 245 ops/s（中位 167 999，5.95 µs/op）**，原始输出 `benches/results/lb_floor.log`。
treedb 单核 `ping` 153 329 = 那条中位线的 **91.3 %**。剩下 8.7 % 是协议解码、stats 计数、帧
编码；而这一列的上限由内核的 loopback 往返决定，不由数据结构决定 —— 换任何实现都不可能超它。

**那一核的时间花在哪儿**（`benches/split_attr.sh`，无 strace 的真实负载下读
`/proc/<pid>/task/*/stat` 的 utime/stime 差分，8 s 窗口；「改前」那一臂的服务端是把 tag
`v0.3.2` 用 `git worktree` 重新编译出来的二进制，客户端与口径完全不变 —— 本轮三项优化
一项都没有；原始输出 `benches/results/split_attr.log`）：

| op | 服务端 | ops/s | user 核 | sys 核 | sys 占比 | user µs/op |
|----|--------|-------|--------|-------|---------|-----------|
| `ping` | v0.3.2 | 137 884 | 0.11 | 0.89 | 89 % | 0.80 |
| `ping` | 本轮 | 158 509 | 0.10 | 0.90 | **90 %** | 0.63 |
| `lookup` | v0.3.2 | 108 921 | 0.27 | 0.73 | 73 % | 2.48 |
| `lookup` | 本轮 | 124 039 | 0.26 | 0.74 | **74 %** | 2.10 |
| `readdir` | v0.3.2 | 51 471 | 0.58 | 0.42 | **42 %** | 11.27 |
| `readdir` | 本轮 | 90 144 | 0.39 | 0.61 | **61 %** | 4.33 |

两个结论一起读：三个读 op 现在全是系统时间主导，把它们推到 1.0 核以上的唯一办法是少付
syscall，而不是少算几纳秒 —— 这就是第一项优化为什么动 `epoll_ctl` 而不动 `BTreeMap`。而
**user 时间真正掉下来的只有 `readdir`（11.27 → 4.33 µs/op）**，那一笔就是分配（下面第 ③ 项）；
`ping`/`lookup` 的 user 只动了 0.17 / 0.38 µs，它们的吞吐来自同样一个核上多跑了几轮往返。

**不缺索引：把项规模拉散重测**（`treedb-micro`，不经网络不写 WAL，钉 CPU 1；本轮实测，
原始输出 `benches/results/micro_names.log`）：

| items | `lookup` ns | `readdir`（一页 100 条）ns | `create` ns |
|---|---|---|---|
| 32 000 | 365.5 | 1 292.9 | 1 343.6 |
| 96 000 | 391.4 | 1 256.5 | 1 328.2 |
| 288 000 | 462.7 | 1 294.5 | 1 371.8 |

项数×9，`lookup` 只涨 26.6 %、`readdir`/`create` 根本不涨 —— 这是 log 形状，不是扫描。
另有一份**优化前二进制**的同工装对照（`benches/results/micro_names_before.log`，
`lookup` 367.8 → 398.0），两档的 `lookup` 逐格差在 ±2 % 内 —— 本轮三项优化没动过表结构，
它们动的全在 syscall 与分配上。形状上它也不需要新索引：`dentries` 是
`BTreeMap<(view,parent), BTreeMap<name,Dent>>`，`lookup` 两层钻取共 2 次树查找，
`readdir` 一次定位到 `(view,parent)` 后拿内层表做 range + `skip(offset)`，
**一页只碰这一棵子树**；`Dent` 里冗余 `kind` 就是为了让 readdir 一页不用回 `nodes` 表下钻
（那是 100 次 cache miss）。所以「单核输 PG」与索引无关，它只与「一个核能付几次
syscall」有关。

**本轮三项逐 op 压榨**（同一格受控 A/B，`SKIP_PG=1` 只跑 treedb 两臂；单位 ops/s）：

| op | 改前 | ① `Conn::rearm` | ② `Conn::push_frame` | ③ `Names` 扁平缓冲 | 合计 |
|----|------|----------------|---------------------|-------------------|------|
| `ping` | 132 673 | 151 881 | 155 090 | 153 329 | **+16 %** |
| `lookup` | 105 372 | 116 258 | 121 079 | 119 001 | **+13 %** |
| `readdir` | 49 943 | 52 875 | 57 751 | 87 075 | **+74 %** |
| `create` | 61 399 | 64 714 | 64 361 | 65 122 | **+6 %** |
| `write` | 68 430 | 70 880 | 69 166 | 71 213 | **+4 %** |

最后一列取含 PG 四臂那次（load 更高），与单项跑差 ±2 %（`ping` 155 327、`readdir` 88 597）。

- **① `epoll_ctl` 归零**。`benches/strace_attr.sh` 本次双分实测（同一客户端、同一核、
  分母取测量窗口 ops；原始输出 `benches/results/strace_attr.log`）：`v0.3.2` 服务端每 op
  **5.10 次** —— `recvfrom` 2.040（err/op 1.019，即其中一次是注定 EAGAIN 的空转）、
  `epoll_ctl` 2.040（err/op 1.019，掩码没变也拿 ADD 撞 EEXIST 再 MOD）、`sendto` 1.019；
  本轮服务端 **2.02 次** —— `recvfrom` 1.010（err 归零）、`sendto` 1.009、`epoll_ctl` 跌到
  0.001（只剩连接建立那几次）。`Conn` 记住当前真挂的掩码（`ep_out`）、只在变得不一致
  时才 `epoll_mod`，读循环收到短读就 `break`（电平触发不丢数据）。
- **② 应答直接编进发送缓冲**。`push_frame` 把 reply 直接编到 `c.tx` 尾部，省掉
  `reply_frame` 的 2 次分配与 `pump` 里的第三次拷贝；`out` 非空时返回 false 退回原来的
  插槽路径，以保证同连接应答顺序。
- **③ readdir 的名字不再每条一次 malloc**。这是三项里唯一只属于单个 op 的。micro 把
  每条目录项定价定得很死（`micro_names_before.log` vs `micro_names.log`，同机同核同工装）：
  limit 从 1 扫到 400，改前耗时 220.6 → 482.7 → 2 293.5 → 4 280.5 → 17 038.1 ns，分配数恰好
  5 → 14 → 54 → 104 → 404，即**每条 ≈42 ns、每条 1 次 malloc**，占一页 100 条成本的 97 %。
  名字只有十几字节，那 42 ns 几乎全是 glibc 的 malloc/free 对。
  换成 `Names`（一块扁平 `blob` + 每项起点）后分配固定 **5 次、与 limit 无关**，边际从
  42 ns/条降到 ≈10.6 ns/条，引擎侧 4 280.5 → 1 268.5 ns（**3.4×**），端到端 `readdir` +53 %，
  并把这一格从「计算主导」翻回「syscall 主导」（sys 占比 42 % → 61 %，见上面那张
  `split_attr.log` 表；strace 里也能看到同一个改动的旁证：`brk` 从 0.045 次/op 降到 0.018）。
  **线协议一个字节未变**，编码仍逐条 `u32 len + 字节`。

三项全部只动 `workers=1` 与多前端**共用**的那层（`Conn` / codec / apply），所以 `t8` 同步受益：
`readdir` 690 070 → 861 270（+25 %）、`ping` 977 533 → 1 124 353（+15 %）。

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

**`v0.3.5` 上重跑了一次，并把它往 5 万推**（逐格原始输出
`benches/results/scale_connections.log`）：

```sh
TREEDB_CONNS=25000 cargo test --release --test scale_connections -- --ignored --nocapture
```

| 目标连接数 | 结果 | 读数 |
|-----------|------|------|
| 25 000 | **通过** | RSS 0 → 8 MiB（**0.31 KiB/conn**）、fd 57 → 25 057（`fd/conn = 1.0000`）、`ping` p50 23 µs / **p99 65 µs** |
| 50 000 | **失败，但不是引擎挡住的** | 8 条连接器线程在各自第 6 250 条上收到 `EADDRNOTAVAIL`；服务端 `RLIMIT_NOFILE` soft=1 048 576 远没用满 |

失败那一格得说清是谁的上限：量具与被测者在同一台机器上，客户端源端口池就是
`ip_local_port_range = 32768–60999`（28 232 个），而四元组的目的地只有一个
`127.0.0.1:port`，所以 5 万条 loopback 连接**在建立侧就凑不齐源端口**。要真验 5 万，
得换多目标 IP 或多台真客户端。也就是说「最大支持多少连接」这个问题，现在手上的
证据只到 **2.5 万条常驻、每条约 0.3 KiB、一个 fd、p99 仍在一次系统调用的量级**，
再往上是被本机量具的源端口空间截断，而不是被引擎截断。引擎自己没有任何连接数
上限常量：服务端侧只剩 `RLIMIT_NOFILE` 与 `somaxconn`（后者只影响突发 accept 队列，
不影响常驻数），客户端侧每连接还有 4 MiB 的发送缓存上限 `MAX_BACKLOG`（满了就不
再收它的写，不是掉线）。

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

表中 `w=1` 那几行对的是**不限亲和性**的 PG，而那一臂本轮实测吃 46–52 个核 ——
它们是历史观测记录，不是「单核效率」的判据；同核口径的四臂表在 §7.5 末。

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

### 7.9 合并 fsync 是量出来的，不是推断的（2026-11-19 首测，11-25 复跑）

§7.3 那句「treedb 写路径的领先全部来自跨连接合并 fsync」以前只有两个档位之间的
吞吐差做支撑。本轮直接读引擎自己的计数器（`--stats` 的 `writes` / `syncs`），算出
「一次 fsync 摊了多少次写」。固定同一格 `op=write conns=64 pipeline=1`，只换盘与
持久化档，复现命令：`benches/sync_ratio.sh 3`（原始输出追加在
`benches/results/sync_ratio.log`；下表取 `@ 2026-11-19T06:17:12` 那一块的四格，本节末的
复跑表取 `@ 2026-11-25T13:05:31` 那一块，两张都由 `benches/doc_table_check.py` 按块时间
戳锚定逐格回对，抄错任一格即非零退出）：

| WAL 位置 | 档位 | ops/s（p50 / p99 µs） | writes | syncs | **写/fsync** |
|----------|------|--------------------|--------|-------|---------|
| 机械盘 XFS | `group` | 10 322（2 767 / 28 310） | 32 313 | 549 | **58.9** |
| 机械盘 XFS | `always` | 1 716（30 521 / 65 132） | 6 199 | 6 199 | **1.00** |
| 根盘 ext4 | `group` | 68 758（877 / 1 441） | 211 794 | 3 445 | **61.5** |
| 根盘 ext4 | `always` | 13 600（4 567 / 6 704） | 42 604 | 42 604 | **1.00** |

同一份日志里还有更早的两轮：机械盘 `group` 61.87 / 61.71，根盘 `group` 62.55 / 61.48
（`always` 每轮都是 1.00）。所以 11-19 当天这一格的比值是 **58.9–62.6**（机械盘
58.9–61.9、根盘 61.5–62.6）。跨三批采集看，全距是 **53.6–62.6**：11-19 的 58.9–62.6、
`benches/ab_durability.sh` 的 53.9–58.0、11-25 复跑（本节末）的 53.6–60.5。引用时不要
再把它写成单一「≈ 55」—— 55 那个数是 §7.11 另一格（`conns=64`、`pipeline=1`、
`dur=group`、私有 PG 集群同机对比那一组）的范围 53–58，两批采集不是同一次。（表中两行
`always` 量的是**改前**实现，第 3 条已把它换掉；`group` 那两行仍有效。）
三条结论：

1. **`group` 档一轮把 53–63 次 fsync 合成了 1 次**（比值按采集分批见上；conns=64 几乎
   全部排上了同一个回合）。换回的 5.1–6.0× 是**改前** `always`（按连接串 fsync）与
   `group` 的差，那个档已不存在，见第 3 条与本节末的复跑表。这就是 §7.3 那张倍率表的
   机制本身，现在它在计数器上看得见，不用靠吞吐差反推。
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

   表里取的是三次中位，逐次读数也在日志里：改后 `always` 的写/fsync 是
   **54.79 / 53.91 / 57.74（范围 53.9–57.7）**，改后 `group` 是 57.71 / 57.74 / 58.04，
   旧版 `group` 是 56.76 / 55.59 / 55.08 —— 即这一格的合批比在 54–58 带里抖，
   与上面 `sync_ratio.sh` 那批（58.9–62.6）**不是同一次采集**，引用时要分清。

   三点判读：① `always` 现在与 `group` 同一量级（0.89×，已是同一实现，残余差在
   噪声里）；② `group` 没被改坏（p50 一致，吞吐/p99 在噪声内）；③ 本次 old 臂
   复现了上表同格的旧数（`always` 1 716 → 1 891、`group` 10 322 → 10 580，差
   4–10 %），说明这一格的跨日可比性够，因此引 §7.9 表里同格的 PG（`synchronous_commit=on`、
   WAL 同在机械盘）1 469 ops/s（p50 33.5 ms / p99 157 ms）作对照是站得住的：
   **`always` 档现在是 PG 同格的 7.4× 吞吐、p50 优 17×、p99 优 5×** —— 上一轮
   「吞吐持平、尾部差于 PG」的结论作废。

**复跑（2026-11-25，`HEAD=c455793`，`bash benches/sync_ratio.sh 3`）。** 上面那张表的两行
`always` 采于 `bef5730`（11-19 09:03Z）之前，同块的 `group` 两行仍有效；这一格在最终代码
上重跑一次，好让「① 已修」这句话有一份落在当前 HEAD 上、且能被 checker 逐格回对的读数：

| WAL 位置 | 档位 | ops/s | p50 µs | p90 µs | p99 µs | max µs | writes | syncs | 写/fsync | errors |
|----------|------|-------|--------|--------|--------|--------|--------|-------|----------|--------|
| 机械盘 XFS | `group` | 10 501 | 1 913 | 14 782 | 32 051 | 96 183 | 32 624 | 609 | 53.57 | 0 |
| 机械盘 XFS | `always` | 9 823 | 1 856 | 16 443 | 32 808 | 99 914 | 30 129 | 498 | 60.50 | 0 |
| 根盘 ext4 | `group` | 111 847 | 513 | 905 | 1 085 | 7 068 | 336 133 | 6 129 | 54.84 | 0 |
| 根盘 ext4 | `always` | 107 026 | 555 | 859 | 1 108 | 8 532 | 321 693 | 5 782 | 55.64 | 0 |

四点读数：

1. **`always` 不再是 1.00**：两格分别合到 60.50 / 55.64 写/fsync，`syncs` 远小于 `writes`
   （498 vs 30 129、5 782 vs 321 693）。「两个档走同一个提交点」这条 §8 第 11 条的结论，
   现在在计数器上、也在当前 HEAD 上看得见，不再只有 `ab_durability` 那一份。
2. **两档吞吐与尾部贴在一起**：机械盘 9 823 / 10 501 = 0.94×、根盘 107 026 / 111 847 =
   0.96×，p99 32 808 vs 32 051 µs。改前同一格的 `always` 是 1 716 ops/s、p99 65 132 µs
   —— 那 5.7× 的差已被这笔改动吃掉。
3. **合批靠的是并发连接，不是 pipeline**：这一格 `pipeline=1` 而 `conns=64`，一轮仍然攒得
   到几十次写。`conns=1` 时「一轮一次 fsync」自然退化成每 op 一次，这是 §8 第 11 条边界
   ① 说的那件事：合批之后不再有「已收到但未 ack 的字节也落盘」这一档。
4. **跨日绝对值不可照抄**：机械盘 `group` 10 322 → 10 501（+1.7 %，可复现），根盘
   `group` 68 758 → 111 847（+62.6 %）。两块 11-19 的采集块都在 `bef5730` 与 v0.7.0 之前，
   根盘那一格的位移本轮没有对应的受控 A/B，**不作归因**；能站住的是 `always`/`group` 的
   比值关系与「合批发生在回合而不是连接」这两条结构性结论。

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
- **~~剩下的头一个~~ 已闭合（2026-11-19，`8e4a4bf`）**：当时一页仍有 **104 次堆分配**，
  其中 100 次是 `Reply::Dirents.names` 每个名字一个 `Vec<u8>`。改成一块扁平 `blob` +
  每项起点（`Names`）之后，每页分配固定 **5 次、与 limit 无关**，micro 边际
  42 → 10.6 ns/条、端到端 readdir **+53%**（§7.5 ③）。再往下只有「直接编进连接
  `tx`」这一档，只把 5 次压到 2–3 次，已在噪声里。

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

### 7.11 单连接对标 + 并发档复核（`v0.3.5` 复测，2026-11-19）

上面几张表都是 8/64 并发的形状，而接入方第一个问题通常是「**我只开一条连接**，
比你快多少」。这一格形状完全不同：`conns=1 pipeline=1` 下 group commit **无对象可合**，
每个写都要自己付一次盘；而副本/多 worker 这两轮新加的东西也都不在这条路径上。
所以单独量一支，工具 `benches/single_conn.sh`（两遍取区间，原始输出
`benches/results/single_conn.log`，两边都钉了 `fsync=on` + `synchronous_commit=on`）。

**一问一答（`conns=1` / `pipeline=1`）**：

| op | treedb ops/s（p50 / p99 µs） | PG ops/s（p50 / p99 µs） | QPS 比 | p50 比 |
|----|------------------------------|--------------------------|--------|--------|
| `lookup` | 43 567–44 597（19 / 36） | 4 421–4 606（210–212 / 453–473） | **9.5–10.1×** | 11× |
| `readdir` | 36 775–37 715（25 / 41–43） | 3 184–3 238（307 / 447–643） | **11.4–11.9×** | 12× |
| `create` | 1 740–1 753（373–385 / 1 747–1 990） | 531–550（624–625 / **22 022–22 816**） | 3.2–3.3× | 1.6×（p99 优 11–13×） |
| `write` | 1 764–1 816（375–377 / 1 747–1 954） | 232–614（711–766 / 22 686–39 139） | 2.9–7.8×（中位 ≈ 3.1×） | 1.9–2.0× |

**单连接但批量（`conns=1` / `pipeline=32`）**：

| op | treedb ops/s（p50 µs） | PG ops/s（p50 µs） | QPS 比 | p50 比 |
|----|------------------------|--------------------|--------|--------|
| `lookup` | 563 356–576 098（1） | 7 168–7 359（129） | **77–80×** | 129× |
| `readdir` | 143 390–145 565（6） | 4 152–4 220（227–232） | **34–35×** | 38× |
| `create` | 7 664–8 031（47–48） | 1 447–1 525（614–652） | 5.0–5.6× | **13×** |
| `write` | 7 957–9 241（44） | 1 282–1 384（630–671） | 5.8–7.2× | **14–15×** |

**写入档这一格两边是同一个持久化语义，3× 不是靠少付 fsync 换来的** —— 直接拿
引擎自计的计数器作证（同一份 log 里每一格后面那行 `[stats]`）：

| 形状 | writes | syncs | 一次 fsync | 折算吞吐 |
|------|--------|-------|-----------|----------|
| 1/1 `create` | 5 267 | **5 267** | 505–527 µs | 1/570 µs ≈ 1 750 ops/s ✓ |
| 1/32 `create` | 23 048 | **728**（平均一轮合 31.7 条） | 3.7–3.9 ms | 32 条/4 ms ≈ 8 000 ops/s ✓ |

也就是：单连接一问一答时 treedb **每 op 付一次真 fsync**（`syncs == writes`），ack 一定
晚于它；PG 那一格每事务一次 `XLogFlush`。两边的中位盘成本在同一量级（373 µs vs
624 µs），差在尾延迟：PG 的 p99 稳定在 22–24 ms，而**同一块盘上** treedb 同一格只有
1.7–2.0 ms —— 所以那 22 ms 不是盘的固有尾延迟，而是 PG 每事务多付的东西（
checkpoint 之后首次改页的 full-page write 会让一次 commit 写的字节数跳档，同机
其他 PG 实例也在搅盘）。**本支没做归因实验**，只报读数；但结论不依赖归因：
**单连接也躲不掉那个 p99**。而 pipeline=32 就是分界线：treedb 把 32 次写合成一次盘（
吞吐 1 753 → 8 031，p50 373 → 47 µs），PG 的 p50 几乎不动（624 → 614 µs），因为它不能把
别人的 commit record 算成自己的已持久化。

**把连接数提上来（同一支脚本、同一天、同一块盘、`pipeline=1`）** —— 接第二个问题
「多连接是不是更快」。两遍区间，treedb 一侧给了两档：`workers=1`（历史基线档，与本节
上面两张表同档）与本机默认档 `workers=8`：

| op | conns | treedb w=1 ops/s（p50 µs） | treedb w=8 ops/s（p50 µs） | PG ops/s（p50 µs） | QPS 比 w=1 | QPS 比 w=8 |
|----|-------|---------------------------|---------------------------|-------------------|-----------|-----------|
| `lookup` | 8 | 131 706–133 112（43–44） | 355 073–387 466（19–21） | 35 553–36 478（212） | 3.6–3.7× | **9.9–10.6×** |
| `readdir` | 8 | 105 464–106 716（84–89） | 248 548–269 984（27–31） | 24 269–25 229（311–314） | 4.3–4.4× | **9.9–10.8×** |
| `create` | 8 | 3 489–4 335（957–996） | 3 859–4 534（992–1 166） | 395–750（1 747–16 779） | 6.1–8.8× | 5.1–6.5× |
| `write` | 8 | 4 365–4 530（921–970） | 3 766–3 894（1 167–1 180） | 326–801（1 726–17 251） | 5.6–13.9× | 4.9–10.2× |
| `lookup` | 64 | 115 507–117 013（501–518） | **984 190–984 811（57–61）** | 208 731–209 912（305–307） | **0.55–0.56×** | 4.7× |
| `readdir` | 64 | 84 545–86 878（695–717） | **763 878–763 941（72–75）** | 144 904–145 878（431–434） | **0.58–0.60×** | 5.2–5.3× |
| `create` | 64 | 9 884–10 468（2 743–2 806） | 10 397–11 822（2 044–2 104） | 1 656–1 848（33 546–33 574） | 5.4–6.3× | 5.9–6.9×（**p50 优 16×**） |
| `write` | 64 | 10 583–11 003（2 680–2 751） | 11 071–11 766（1 818–1 916） | 1 287–1 468（33 493–39 936） | 7.2–8.6× | 6.5–8.7×（**p50 优 17–18×**） |

三件事，第二件最容易被说错：

- **写确实更快，而且不是靠多核**：`workers` 两边都是同一个提交线程，treedb 的 `create`
  还是从 1 740–1 816（1/1）涨到 3 489–4 534（8/1）再到 10 397–11 822（64/1）。涨的是
  **group commit 的合并度**，引擎自计数器把这条线钉得很直（`[stats]` 行，`create` 档）：

  | conns（一问一答） | writes | syncs | 一次 fsync 摊几条 |
  |---|---|---|---|
  | 1 | 5 267 | 5 267 | **1** |
  | 8 | 11 649–13 670 | 2 670–2 944 | 4.4–4.6 |
  | 64 | 31 832–36 029 | 601–618 | **53–58** |

  盘的成本从「每 op 一次」变成「每 55 op 一次」，而 PG 那一侧 64/1 的 p50 两遍都钉在
  33.5–39.9 ms（四个 op 里写档全一样），那是它 commit 队列的排布，不是盘。64 连接这一档
  提交线程还没排干（`workers` 再加也不涨，§8 第 1 条第 ③ 项）。
- **读在 `workers=1` 下 64 连接会被 PG 反超（0.55–0.60×），这一档不能拿来说赢**。根因
  §7.5 归过：一条线程上「每连接一次 read + 一次 write」全排在一个核，PG 每 backend 一个
  进程天然摊在 64 个核。本机默认档 `min(8, 核/2)` 把这一格翻成 4.7×（`lookup`）与
  5.2–5.3×（`readdir`），8 连接那档甚至 9.9–10.8×；也就是说**报并发数字必须把 workers 档
  一起报**。`benches/single_conn.sh` 的 `WORKERS` 默认 1 只是为了和本节上面两张表同档。
- **提高 pipeline 会让倍率收窄而不是继续拉大**：同一份 log 里 `PIPE=32` 那两遍，
  `lookup` 8/32 是 23.2–23.9×，但 64/32 只剩 3.2×；`readdir` 64/32 1.98–1.99×；`create`
  64/32 1.6–1.7×。原因还是 §7.3 那句：一个回合排干 64 条连接各 32 条就是 2 048 条 op，
  后面的连接只能排在同一轮里，这是单提交线程用延迟换吞吐的固有形状。`write` 64/32
  那两格 PG 的测量窗口溢出到 3.95 s / 4.16 s（§7.5 记过同一口径问题），不入对照。

三条不能不说的提醒：

- **这两张表与 §7.2–§7.5 那些行不可相减**。§7.10 已经记过一次这个坑（不同日期的
  矩阵格子不能互减）。本支只回答「现在这一版、这一格、两边同盘同参数下是多少」。
  例如 `readdir` 1/1 这里量到 36–37 k，而旧矩阵同形状那行是 19 715（p50 42 → 25 µs）：
  本支没有逐项 A/B，不拿它当「快了 1.9×」的结论，只当作 v0.3.5 的新基准。
- **`write` 那一格 PG 自己两遍差 2.6×**（232 vs 614 ops/s）。机械盘上只跑 3 s、
  单连接、每 op 一次 flush 的形状就是会抖；报区间而不是报小数，中位倍率取 ≈ 3.1×。
- **一次 fsync 从 0.5 ms 变 3.9 ms 不是回退，是载荷变大**。§7.4 那个「裸 fsync
  3.8–4.6 ms」探针对应的恰好是“一批已摊开”的形状，而本支 1/1 那格是“每轮只追
  一条十几字节的记录”的形状。两者测的不是同一次盘，不要把其中一个当“这台盘的 fsync 单价”。

### 7.12 万级视图形状：连接池从「每视图一份」提到「每进程一份」（2026-11-20）

§7.6 证明的是**引擎**侧能撑住 2.5 万条连接，所以「万级活跃视图」在引擎那一头早就
不是能不能的问题。但客户端那一头曾经还是**线性**的：grovefs 的 treedb 后端
`TreedbStore` 每个实例自己拨 `DEFAULT_CONNS = 4` 条长连接（`GROVEFS_TREEDB_CONNS`
可覆盖），而 meta-server 的 `store_for` 按**视图名**缓存 store —— 于是 1 万视图 =
4 万条连接。4 万条连接在账上付得起（§7.6 那格 0.31 KiB/conn），但那等于把一条
结构性优势又换回了线性成本。

**这不是「性能不够」，是「曲线形状错了」。** treedb 的每条 op 自带 `view` 字段，
连接上没有 per-view 状态（只有握手门与 pipeline 顺序），所以按视图切池纯粹是记账
选择。改法是把池提到进程级：`ConnPool::for_addr(addr)` 在一个
`OnceLock<Mutex<HashMap<..>>>` 里按地址共享，`TreedbStore` 只持 `Arc<ConnPool>` +
`view` / `view_id`。**失败重拨语义一个字没改**：`xcall` 仍只对只读 op 重连重试一次，
写 op 失败直接 `Db`（重发会二次生效）。

**改前 / 改后，同一条探针**：顺序开 N 个视图（挂载 → `ls` → 卸载），`--engine-idle-secs`
钉住所以卸载不回收 store，于是表里每个视图都是一个**驻留 store**。原始输出
`benches/results/conn_pool_before_seq.log` 与 `conn_pool_after_seq.log`：

| 驻留视图 | 改前 engine_conns | 改前 meta_socks / meta_fds | 改后 engine_conns | 改后 meta_socks / meta_fds |
|---|---|---|---|---|
| 0 | 0 | 2 / 47 | 0 | 2 / 73 |
| 1 | 4 | 6 / 51 | 4 | 6 / 77 |
| 10 | 40 | 42 / 87 | 4 | 6 / 77 |
| 50 | 200 | 202 / 247 | 4 | 6 / 77 |
| 100 | **400** | 402 / **447** | **4** | 6 / **77** |

「改后 precreated 就是 4」不是巧合：`view create` 本身要列一次视图，那一步就建了
store、也就建了池，此后**再加多少视图都不再拨号**。另外两份各管一件事——
`conn_pool_before_conc.log` / `conn_pool_after_conc.log`（10 个视图同时挂）是
40 → 4；`conn_pool_before_ab.log` / `conn_pool_after_ab.log`（A 挂 → 杀 A → 挂 B）
是「4 → 4 → 8」与「4 → 4 → 4」，两者一起说明**连接不随挂载结束而回收**，所以
线性的那一版是实打实会爬到 4×N 的，而不是被卸载顺手清掉了。

**万级阶梯**（`benches/many_views.sh`，原始输出 `benches/results/many_views.log`；
每档先把视图建到该数，再把**本档新建的每个视图各挂一遍**，1 万档就是 1 万个驻留
store）：

| 视图 | 挂过 | engine_conns | 峰值 | meta 侧 socket fd | meta RSS KiB | KiB/store | view create 均壁钟 ms | 挂载内 create+unlink µs |
|---|---|---|---|---|---|---|---|---|
| 100 | 100 | 4 | 4 | 6 | 8 812 | 10 | 159 | 3 965 |
| 1 000 | 1 000 | 4 | 4 | 6 | 10 964 | 2 | 162 | 4 357 |
| 5 000 | 5 000 | 4 | 4 | 6 | 19 224 | 2 | 160 | 4 299 |
| 10 000 | 10 000 | 4 | 4 | 6 | 23 216 | 0 | 163 | 4 194 |

三条断言，逐条对表：

1. **连接数不随视图数增长**：10 000 个驻留 store 下引擎侧 established socket 仍是
   **4**、全程峰值 **4**，meta-server 的 socket fd 仍是 **6**（2 个监听 + 4 条引擎
   连接）—— 与 100 视图那一格逐字相同。旧形状外推到 10 000 视图是 **40 000** 条，
   比 §7.6 本机能测到的 25 000 还多，也就是说**旧形状在「万级视图」这个规模上连
   测都测不了**。
2. **10 000 视图下的写延迟与 100 视图同档**：`view create` 的均壁钟 159 → 163 ms
   （每格都含一次 grovefs 进程启动的常量项），挂载内 200 次 create+unlink 的均值
   3 965 → 4 194 µs（+5.8 %）。没有随视图数劣化 —— 因为 op 的成本压在提交路径上
   （§7.9 的 fsync 合批），不在连接数上。
3. **每视图驻留成本**：8 812 → 23 216 KiB，**10 000 个驻留 store 多付 14.1 MiB ≈
   1.44 KiB/视图**（每档边际那一列 10 / 2 / 2 / 0 KiB 是整数截断的余数，总量才是
   权威数字；第一档偏高是基准时刻的冷分配）。

**池小的代价当场定价**（`benches/results/many_views_conc.log`）：同一档 100 视图，
8 个视图**同时**挂载、各跑 200 次 create+unlink，只换池大小：

| 池（`GROVEFS_TREEDB_CONNS`） | 8 并发 × 200 pairs 壁钟 | 聚合 µs/pair | 引擎连接数 |
|---|---|---|---|
| 4（默认） | 1 712 ms | 1 070 | 4 |
| 4（前一轮复跑） | 1 761 ms | 1 100 | 4 |
| 32 | 1 351 ms | 844 | 32 |
| 32（前一轮复跑） | 1 553 ms | 971 | 32 |

池从 4 提到 32（8×）只换来 **13–26 %**：这不是瓶颈，是余量。要更多并发就调这个
环境变量，它改的是**进程**的池 —— 同一份 log 里池=32 那一趟在 100 视图下仍然是
32 条（不是 3 200），阶数与视图数解耦。

最后留一句踩坑记录，因为它让第一次测量的结论是**反的**：不要在循环里「建一个视图 →
挂一次 → 卸掉」去数连接。`view create` 结尾会 `rt.evict()` 清掉**整个**引擎缓存，
于是读数恒定成 4，看起来像「本来就是共享的」。必须先把视图全部建完，再逐个挂。
这条警告现在就写在 `benches/many_views.sh` 的头部注释里。

**残留边界**：① 池是**每进程**共享，所以多开一个 meta-server 进程就多 4 条连接
（这与「与视图数无关」不冲突，但部署时的连接总数要按进程数算）；② `evict_idle`
（无 session 且超过 `--engine-idle-secs` 才清缓存）现在只影响内存，不再影响连接数；
③ 本节的 create 延迟是**单个挂载内**的形状，跨视图并发的上限见上面那张池大小表。

---

### 7.13 三臂同口径：treedb / PostgreSQL 12 / SQLite3（2026-11-20 采集）

§7.3、§7.5、§7.8 的对照都是两两做的 —— 要么 treedb vs PG，要么对裸 `epoll` echo。
一直没有一张表把**两个对手放进同一轮、同一套客户端、同一个计时窗**。这一节补上，
顺带把上一轮「原始读数入库了但没写成文」的缺口收口。

**先说这张表不能回答什么**，否则下面的倍率一定会被引用错：① 单次采集，每格 3 s，
两个对手只复跑过一轮（PG 那一列，见后文），所以只有**量级**意义，没有 ±5 % 的精度；
② 客户端与服务端同机、走 loopback，绝对 ops/s 不是跨机数字；③ SQLite 是**进程内库**，
treedb/PG 要付一次 TCP 往返加协议编解码 —— 这条差异不可能被任何「同口径」抹平，
所以 SQLite 的单连接列说明的是「库内 vs 跨进程」的价差，不是引擎快慢。

**口径**（权威处是 `benches/three_way.sh` 的头注释，七条逐项对齐）：每个连接一条自己的
命名空间、里面 500 条预热文件；线程数 = 连接数；计时窗从参数设完起算、建连与预热不进
分母；`ops/s` 只数**真正执行完**的语句/请求，不按 pipeline 乘；落盘强度 treedb `group`、
PG `fsync=on` + `synchronous_commit=on`（脚本读回闸门，不是 on 就 exit 1）、SQLite
`journal_mode=WAL` + `synchronous=FULL`（每条连接都设并读回）；`pipeline=32` 在 PG/SQLite
侧是「一个事务 32 条语句」、在 treedb 侧是「一次往返 32 个请求」，两边摊的都是每 op 的
fsync 账（§7.9），但机制不同；每格都从空状态起跑（treedb 删目录重建、PG 把
`grovefs_v_*` schema 全 drop、SQLite 删库含 `-wal`/`-shm`）。

环境：同一台机（128 核，Linux 5.4.0-216），同一块 xfs 盘（`/dev/sdb1`，MR9361-8i，
`ROTA=1` 机械盘阵列，裸 fsync 3.8–4.6 ms —— 所以下面写轴的量级全部由 fsync 定）；
treedb `v0.3.6` 零旗标默认档（`workers=8`，`dur=group`）；PG 12.22；SQLite 3.31.1。
treedb 这一列**没有**在 `v0.3.7` 与 `v0.4.0` 上复跑：`v0.3.7` 只改失败路径（快照 crc、目录 flock、
启动参数闸门、停服报错、背压掩码、`ENOTDIR`），`v0.4.0` 只给两个**副本专用**的 reply
尾部加了一个 `u64`（`WalBatch`/`SnapChunk`，主客通信里的 CRUD 一个字节未动）；两版
的稳态提交路径与 §7.9 量的那条一字未动，
唯一新增的稳态成本是 checkpoint 时多算一遍 `crc32`（不在 3 s 计时窗内）。

`p50`/`p99` 单位 µs；`vs PG` / `vs SQLite` 是**同行 ops/s 之比**，> 1 表示 treedb 快。

| op | 连接 | 管道 | treedb ops/s | p50 | p99 | PG ops/s | p50 | p99 | SQLite ops/s | p50 | p99 | vs PG | vs SQLite |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| lookup | 1 | 1 | 45 317 | 19 | 39 | 4 710 | 209 | 432 | **252 158** | 3 | 4 | 9.6× | **0.18×** |
| lookup | 1 | 32 | 569 676 | 1 | 3 | 7 415 | 128 | 260 | 715 462 | 1 | 2 | 76.8× | 0.80× |
| lookup | 8 | 1 | 339 213 | 21 | 43 | 36 420 | 210 | 452 | 302 945 | 24 | 63 | 9.3× | 1.12× |
| lookup | 8 | 32 | 3 039 997 | 1 | 3 | 57 813 | 127 | 271 | 374 432 | 21 | 34 | 52.6× | 8.1× |
| lookup | 64 | 1 | 1 066 288 | 49 | 215 | 214 606 | 299 | 397 | 208 529 | 321 | 705 | 5.0× | 5.1× |
| lookup | 64 | 32 | 5 251 278 | 11 | 23 | 308 636 | 206 | 253 | 265 272 | 273 | 348 | 17.0× | 19.8× |
| readdir | 1 | 1 | 35 698 | 25 | 44 | 2 898 | 323 | 723 | 44 291 | 22 | 22 | 12.3× | 0.81× |
| readdir | 1 | 32 | 142 575 | 6 | 11 | 3 972 | 245 | 451 | 50 348 | 19 | 22 | 35.9× | 2.8× |
| readdir | 8 | 1 | 290 912 | 26 | 45 | 23 628 | 322 | 642 | 45 042 | 170 | 355 | 12.3× | 6.5× |
| readdir | 8 | 32 | 1 005 272 | 7 | 12 | 31 768 | 246 | 405 | 45 489 | 191 | 311 | 31.6× | 22.1× |
| readdir | 64 | 1 | 838 990 | 71 | 185 | 147 572 | 427 | 602 | 36 812 | 1 732 | 2 518 | 5.7× | 22.8× |
| readdir | 64 | 32 | 2 650 590 | 23 | 38 | 186 909 | 340 | 424 | 38 460 | 1 683 | 1 873 | 14.2× | 68.9× |
| create | 1 | 1 | 1 790 | 375 | 1 680 | 483 | 605 | 36 241 | 54 | 16 819 | 39 358 | 3.7× | 33.2× |
| create | 1 | 32 | 8 575 | 45 | 711 | 1 543 | 597 | 1 496 | 1 655 | 533 | 1 556 | 5.6× | 5.2× |
| create | 8 | 1 | 4 628 | 995 | 15 542 | 854 | 1 629 | 47 357 | 41 | 16 852 | 3 454 072 | 5.4× | 112.9× |
| create | 8 | 32 | 23 219 | 257 | 1 314 | 6 727 | 1 072 | 2 048 | 1 289 | 536 | 110 909 | 3.5× | 18.0× |
| create | 64 | 1 | 11 746 | 1 927 | 32 338 | 1 881 | 33 547 | 67 800 | 24 | 17 070 | 8 846 453 | 6.2× | 489.4× |
| create | 64 | 32 | 47 503 | 1 191 | 5 078 | 32 857 | 1 943 | 3 025 | 732 | 1 077 | 264 618 | **1.45×** | 64.9× |
| write | 1 | 1 | 1 766 | 378 | 1 915 | 512 | 777 | 21 600 | 51 | 16 791 | 45 775 | 3.45× | 34.6× |
| write | 1 | 32 | 7 869 | 47 | 786 | 1 346 | 658 | 1 563 | 1 593 | 524 | 1 673 | 5.9× | 4.9× |
| write | 8 | 1 | 4 355 | 1 028 | 16 496 | 847 | 1 786 | 43 967 | 45 | 16 795 | 3 365 454 | 5.1× | 96.8× |
| write | 8 | 32 | 22 422 | 269 | 1 310 | 5 246 | 1 298 | 3 901 | 1 403 | 525 | 111 022 | 4.3× | 16.0× |
| write | 64 | 1 | 12 033 | 1 784 | 27 338 | 1 052 | 33 857 | 894 142 | 28 | 16 853 | 7 950 032 | 11.4× | 429.8× |
| write | 64 | 32 | 59 452 | 1 040 | 2 631 | 10 626 | 1 970 | 84 594 | 735 | 536 | 257 622 | 5.6× | 80.9× |

**读得出来的五条**：

1. **对 PG，24 格全胜**，最差一格是 `create` 64 连接/管道 32 的 **1.45×**，最好一格是
   `lookup` 1 连接/管道 32 的 **76.8×**。倍率的形状有解释：`lookup`/`readdir` 的极高
   倍率都出现在 `pipeline=32` 上（treedb 一次往返摊 32 个 op，1–3 µs/op；PG 那边
   每条语句仍要付解析与执行），`create`/`write` 的最小倍率也出现在 64/32（PG 用
   「一个事务 32 条语句」把 commit 摊薄了 —— 正是 §7.9 说的那个共同技术）。
2. **写轴上 treedb 的倍率随连接数涨、随管道缩**：`create` 1/1 3.7× → 64/1 6.2× →
   64/32 1.45×。64/1 那一格是组提交摊 fsync 的直接证据（§7.9 的结论在 64 并发下再证一次）。
3. **对 SQLite 只有并发档可比**。单连接 `lookup` 输 **5.6×**（45 317 vs 252 158）、
   `readdir` 输 1.24× —— 进程内零往返的账；`create`/`write` 却赢 33×/34.6×，因为
   SQLite 在 `synchronous=FULL` 下每条写一次 fsync 且没有组提交。SQLite 的 64 连接列
   **不是「慢」，是并发上限**：`create` 24–732 ops/s、p99 到 **8.8 s**，`write` 28–735、
   p99 7.95 s（单写者 + busy 等待 + 每次 sync）。这几格只能当「这个形状下要不要选
   SQLite」的证据，不能当引擎速度的对照。
4. **treedb 自己那两个「难看」的格子要解释而不是藏**：`create`/`write` 单连接只有
   1 766–1 790 ops/s 而 p50 只有 375 µs —— 一条连接没有可合批的对象，每个 op 付一次
   fsync，均值被 p99≈1.7–1.9 ms 的尾巴拉到 ≈560 µs；同形状 64 连接反而 6.7×（11 746），
   因为只有连接多了才摊得开。另一处是 64 连接的 `create`/`write` p99 到 27–32 ms：那是
   组提交队列的等待上限（一个 fsync 期的排队），不是计算成本 —— 同格 PG 的 p50 是
   33.5 ms 且 p99 到 67.8 ms（`write` 64/1 的 PG p99 更是 894 ms）。
5. **`workers=8` 没有污染单连接那 8 格**：同轮的 `workers=1` 臂与 `workers=8` 在同 8 格上
   差 0.93–1.04×（最大偏幅是 `write` 1/32 的 7 869 vs 8 466，即 −7 %；其余 ≤5 %），
   数据在 `benches/results/three_way.tsv` 末尾的 `treedb-w1` 行。并行收益只在多连接档
   出现。这条是「报并发数字必须标注 workers 档」那个纪律的反向自检。

**PG 那一列用的是 `ANALYZE` 之后的复跑，原因写在数字里**：第一轮没 ANALYZE，`readdir`
六格只有 1 513–75 710 ops/s，复跑后 2 898–186 909（**+52 % ~ +147 %**）—— 计划一跳，
同一套 SQL 的读数翻倍。其余列复跑差 ≤15 %，唯一例外是 `write` 64/32 的 **−39 %**。
所以表里 PG 用复跑值（同一台机同一块盘、24 分钟后的另一轮），treedb/SQLite 两列仍是
第一轮；逐格 delta 的原始数据在 `benches/results/pg_rerun.tsv`。**这条的含义是：PG 那一
列自身的抖动在 ±40 % 量级，任何小于这个幅度的「倍率变化」都不必当结论。**

**数据出处与生成器**：原始采集 `benches/results/three_way.tsv`（3 引擎 × 24 格）与
`three_way_raw.log`；PG 复跑 `pg_rerun.tsv` / `pg_rerun_raw.log`；供引用的
`final.tsv` 由 `benches/three_way_table.py` 生成，`--check` 校验盘上文件与两份原始采集
自洽（表头、24 行、14 列、每行倍率能由同行 ops 回算），`--selftest` 用三处注入的错
（p50/p99 拼成一列、倍率与 ops 不符、缺一格）验证校验器真的会红。

**为什么这个生成器是必需的**：上一版 `final.tsv` 是手搓 one-liner 拼的，`p50` 与 `p99`
被拼进了同一列（`3` 和 `4` 变成了 `34`），形状完全像真数字 —— 这张表差一点就以
「看起来像真的」的身份被抄进本文。拼列这种错无法靠读表发现（`34` 也是合法读数），
只能靠「表必须由原始采集重算 + 校验器自带注入自测」挡住（与 §7.5 那次量具不对称同型）。

---

### 7.14 接入面两个旋钮：`--max-conn` 与 `--stats --format prometheus`（2026-11-21）

§7.6 的连接轴已经把「资源不是瓶颈」量死了：25 000 条连接 0.39 KiB/conn。
既然如此，把进程跑死的方式就只剩一种 —— **一条不守规矩的客户端无限开连接**（或一个失去
回收的 fork 风暴），把 fd 拍额度与内存慢慢吃光，而症状是「所有连接一起变慢」而不是
「多出来的那条被拒」。所以上限要能配；配了之后还得好观测，这就是这一节的两件事。

**两件事都没碰协议。** `PROTOCOL_VERSION` 仍是 6，`Op` 表一条没加（拒绝不靠发帧，
靠关闭连接），`EngineStats` 一个字段没动 —— 理由在第三段。所以旧数据目录、旧客户端
全部零改动。

**上限怎么算。** 判定必须在 `accept` 之后、入表之前：一条已经落在内核接受队列里的连接
如果没人 `accept` 就拿不到 fd，关掉未 accept 的连接只能关监听 socket。`--workers > 1` 时
每条 worker 各自 `accept`，所以不能写「读 live → 比较 → 加一」（两条线程会同时看到
`max-1` 而各收一条，超拨）；一律 `fetch_add` 之后判超，超了就 `fetch_sub` 退回并 `drop`。
拒绝的形状是**接受后立刻关闭**（客户端看到 EOF），不发任何帧：协议每连接严格 FIFO
且没有 request id，在没有请求的情况下先塞一个 `Reply::Err` 既没有位置放、客户端也
无从对应。原因只能在服务端看，而那里日志是**限速**的：只打第 1 条与之后每 1 000 条
（1 000 条一轮的连接风暴下逐条打日志会把盘写满），总数给在停服汇总里。

**为什么连接计数不进 `EngineStats`。** 那个结构没有版本位：`encode` 在尾巴追加、
`decode` 按定长读，加一个字段就必须把 `PROTOCOL_VERSION` 从 6 抬到 7。而连接数是
**进程**的属性，不是**引擎状态**的属性 —— 为一个运维数字把全部客户端跟一次协议升级
绑在一起，代价与收益完全不对等。所以 `accepted / refused / peak` 只走两处：停服
汇总那一行，和 `--stats` 的 prometheus 形态里的外部指标。

**指标口径三条**（实现见 `src/metrics.rs`）：

1. **不开 `/metrics` 端点**。接入面只有二进制协议，多开一个 HTTP 监听就是多一份
   攻击面与一份配置；`treedb-server --stats --format prometheus` 就是采集器，采集器
   自己管超时与重试，不需要服务端再长一个口。
2. `_total` 的语义是**进程启动以来**（`treedb_ops_total` / `writes_total` /
   `fsyncs_total` / `fsync_seconds_total` / `checkpoints_total` 与副本的
   `applied_total` / `resyncs_total` / `errors_total`）。重启就归零，率要在面板侧
   用 `rate()` 算；字节一律基本单位不换算，微秒到秒保留 6 位小数。
3. 副本专用项在主上也输出 0（`treedb_replica == 0`）。面板要过滤
   `treedb_replica == 1` 才画 lag，否则主上的「0」会被读成「追得很紧」。

抓取失败时的形状也定死了：stdout 只有 `treedb_up 0` 那三行（HELP/TYPE/样本）并以
**退出码 1** 退出 —— 面板上应当是 down，而不是 no data（后者看起来像监控没配好）。
参数错（`--format` 拼错、`--format` 不带 `--stats`、`--max-conn abc`）一律
**退出码 2 且不产出任何字节**，与其它 usage error 一致。

**实测（2026-11-21，本机 `127.0.0.1`，`bash benches/observe.sh`，`--workers 4
--max-conn 8`）**：

| 档 | 量到的 |
|----|--------|
| 空载探测（正对照） | 连上并保持 —— 判据不是一味返回「拒」 |
| 装满 8 条后探测 | 被拒；64 条并发探测 **64/64 全拒、0 漏过**（这就是「先加再判超」在多 worker 下不超拨的证据） |
| 拒绝风暴后 | 在册 8 条一条没掉，新探测仍被拒 |
| 拒绝日志 | `refused connection #1` 出现 1 次、`#2` 0 次（限速生效） |
| 释放 1 格后 | 立即能进 —— 上限算的是「同时在册」，不是「连过一次就永久拉黑」 |
| `--format prometheus` | 81 行 = HELP 27 + TYPE 27 + 样本 27，每段三行、名字唯一、值都是数字，`treedb_up 1` |
| 与文本档对账 | `views` / `nodes` / `dentries` / `seq` 四项逐个相等 |
| 抓取失败 | 3 行 `treedb_up 0` + 退出码 1；三种参数错全部退出码 2 且 stdout 0 B |
| 停服汇总 | `accepted 14 refused 66 peak 8` —— 与脚本里逐次点名算的账**逐项相等** |

**顺带修了一个真 bug（档 4 逼出来的）**：`--workers > 1` 下 `treedb-server --shutdown`
从来没拿到过 ack，客户端只看到 `shutdown: io: server closed` —— 而服务端其实已经
乖乖停了。原因是停位 `flag` 由提交线程**在回包之前**置位（它一看到 `Op::Shutdown`
就置），前端线程完全可能先看到停位就跳出循环，那个 ack 于是永远停在信箱里。单线程
路径没有这个窗口，所以这个 bug 只存在于多 worker 形态，而它恰好把「停服到底有没有
被确认」这件事变成了不可观测。修法是退出前先等一拍 BYE（上限 100 ms）再把信箱排干、
填进槽位统一写出；`tests/observe.rs` 里 `cli_shutdown_is_acked_in_both_worker_shapes`
钉住它（两种 worker 形状各跑一遍，2026-11-21 之后 15/15 次全过）。

**脚本里踩的两个坑**（与引擎无关，但值得记）：一是拿 `sleep` 常驻持连接会把 fd 传给
子进程，`kill` 掉子 shell 后连接仍然开着，服务端等不到 EOF、槽位永不释放 —— 第一轮
跑出来的「释放 1 格后仍被拒」与 70 次（而不是 66）拒绝全出自这一个坑，改法是让子壳
最后一个动作是**内建** `read`。二是「两份空文件对账 4/4 相等」也会变绿：抓取失败时
文本档与指标档都是空的，所以断言把指标数钉死在 27，不再允许「段数等于样本数」这种
自洽就过关的形状（与 §7.13 那次「表必须从原始采集重算」同型）。

---

### 7.15 通用表基准：九条 `row*` 路径（2026-11-21，G4 后）

§5.1 的通用表此前只有功能测试，没有性能数字。而 G6（`TableAddColumn`，§5.1）要回答的
正是「加列之后旧路径有没有变慢」，拿目录树的 ops/s 去类比是不成立的：两条路的常数开销
完全不同（表要定位主键树、维护二级索引、帧里多两列），差一个数量级的优化在另一个
轴上可能根本不存在。所以量具先落地，G4（`RowRange` 0x0079）落地后再把它自己的
两族扫描格补进来 —— 下面这张表是补了之后的完整一次。

**九条路径各量什么**（`treedb-bench --op row*`）：

| op | 走的路 | 为什么单列一格 |
|----|--------|----------------|
| `rowinsert` | 新键 + 主键索引插入（`PUT_IF_ABSENT`） | 键带 nonce，重跑不会退化成覆盖；最重的写 |
| `rowput` | 覆盖已有行 | 命中已有的树节点与索引项，与插入分得开 |
| `rowget` | 主键点查 | 不进 WAL，量树查找本身的常数地板 |
| `rowlist` | 主键序翻页（`limit 100`） | 树的顺序遍历 + 帧编码，行数由 `limit` 定 |
| `rowidx` | 二级索引等值页 | 索引树定位 + 回查，路径与主键序不同 |
| `rowidxrange` | 索引列值区间 `[t0, t1)` | 约 31 行，与 `rowidx` 回包同大小 —— 两格只差「等值前缀」与「值区间 + `take_while`」这两条路径本身 |
| `rowrange` | 主键区间 `[lo, hi)`，窗宽 `--range` | 跑两档窗宽：64 行（一页装完，`end=true`）与 400（每页满 100 且 `end=false`）。后者与 `rowlist` 回包同形，两格对拉才能把「扫描一段」与「回包大小」分开 |
| `rowcount` | 按主键计数（O(1)） | 不碰树，量的是**分派地板**（分帧/派发/ 回包） |
| `rowchurn` | put/del 成对同键 | 树节点长到一格再缩回零，含索引侧反向删除；表规模恒定 |

**表形状固定，且只在测量窗口外建一次。** `benchkv(tag 索引, payload)` 两列是刻意
的：再加列只是把行变宽，而这里要量的是表这条路径的常数开销，不是某种业务行宽。
`row*` 档**不建视图**（表是全局的，建 64 个视图只会把建视图成本混进预热）；表由
`setup_row_table` 在开线程之前建好，八条线程不抢着建同一张表。已经在的**必须校验
形状**：上一轮若用别的列序建过同名表，后续每个 `RowPut` 都 `EINVAL`，而在压测输出
里那只是一行 `errors=NNN` —— 查起来要绕一大圈。形状不同直接退出码 2 并把列数说清。

**三档形状**（同一份代码，只换服务端旗标）：

- A `durability=never`：把 WAL 落盘摘掉，剩下树 + 索引 + 帧这一层。
- B `durability=group`（默认）：加回每轮一次 fsync，A→B 的落差即落盘占比。
- C `durability=never --workers 1`：单线程事件循环，读轴对照组。

三档都用**各自的空目录**，且**读格排在写格之前**。理由是 `rowinsert` 每跑一遍就往
表里添十几万行：先跑写，后面的读就压在逐渐变大的表上，A/B/C 三栏的读数就混进了
「表多大」这个变量。现在三档的读都发生在预热刚写完的 500 行上（`TREEDB_BENCH_WARM`
可改），`rowchurn` 前后行数 **500 = 500** 就是这条的证 —— 它同时证明 put/del 真的
成对（行数要是涨了，量到的就不是「树节点缩回零」那条路）。

**主键从这一轮起定宽零填充（`r%06d`）。** `rowrange` 的区间两端就是两个主键字符串：
不定宽的话字节序与数字序不一致（`r10` 排在 `r2` 前面），「宽 64 个键的窗口」到底
装了几行要看 `span` —— 而这一格量的正是「扫描 + 回包」的单位成本，窗宽必须可预测。
代价是**本轮之前的 §7.15 数字不能与本轮直接对拉**（行键从 2–4 字节变 9 字节），
`benches/results/bench_rows.log` 里在两种格式之间留了一行说明。附带好处：这次改键
格式正好把「预热后必须查得到行」那道守卫真实行使了一遍 —— 探针键与预热键是同一个
`row_key()`，若两边有一边漏改，`rowrange`/`rowget` 会在预热后直接 `exit 2`。

**每格跑 `REPS` 次取最大值**（默认 2），行尾附 `lo/hi` 离散度。这条是跑出来的：
同一格 `rowinsert/group` 两次跑出 20 557 与 8 441（2.4×），落盘档的 fsync 抖动就是
这么大，单次采样不配当基线。看到 `lo/hi < 0.7` 就该知道那一格的绝对值不能拿去跟
另一台机器对拉。

**一条假绿和它的守卫。** 四条「查」的 op 在**空表**上都零错误：`RowGet` 未命中回的
是 `Reply::Row(None)`、`RowList` 回空行集、`RowCount` 回 0，三者都不是 `Err`。于是
一张空表能跑出「几百万 ops/s、errors=0」这种最好看也最没用的数字。所以预热之后
先探一次，必须查到行，否则 `exit 2` 并说明是哪一格。这个守卫本身也验过：把 `rowidx`
的探针 tag 临时改成不存在的值，输出 `RowList(tag=t0, limit=1) 预热后仍查不到行：
这一格会量成「空表快路径」` 并 `exit 2`（没用 `assert!`：panic 只在这个线程里炸，
主线程再 `join` 一次，真正的文案被不透明的 `Any { .. }` 冲掉）。

**实测（2026-11-21，本机 `127.0.0.1`，`bash benches/bench_rows.sh`，`conns 8
pipeline 8 secs 3`，500 行表，auto-checkpoint 关）**：

| op | A never/auto | B group/auto | C never/w1 | B/A |
|----|-------------:|-------------:|-----------:|----:|
| `rowinsert` | 91 557 | 22 882 | — | 0.25 |
| `rowput` | 105 666 | 19 233 | 138 815 | 0.18 |
| `rowget` | 1 805 948 | 1 786 262 | 754 250 | 0.99 |
| `rowlist` | 124 572 | 129 917 | 51 676 | 1.04 |
| `rowidx` | 286 026 | 257 389 | 112 431 | 0.90 |
| `rowidxrange` | 277 062 | 315 792 | 114 194 | 1.14 |
| `rowrange_w64` | 158 148 | 142 176 | 82 799 | 0.90 |
| `rowrange_w400` | 127 532 | — | — | — |
| `rowcount` | 2 423 400 | 2 452 916 | 921 424 | 1.01 |
| `rowchurn` | 150 734 | 22 668 | — | 0.15 |

延迟（p50 / p99 µs，A 档）：`rowget` 3/7、`rowlist` 52/85、`rowidx` 22/40、
`rowidxrange` 24/48、`rowrange_w64` 40/72、`rowrange_w400` 48/97、`rowcount` 3/5、
`rowinsert` 31/2620、`rowput` 30/1695、`rowchurn` 16/68。

**怎么读这组数：**

1. **写轴的成本 80% 在落盘。** `B/A` 落在 0.15–0.25，p50 从 31 µs 抬到 190 µs
   （一次 fsync 的量级）。口径是 `8 连接 × pipeline 8 = 64 op` 摊一次 fsync，
   与 §7.9 的摊比同形；`rowchurn` 更低（0.15）是因为它一半 op 是删除，每轮仍只
   fsync 一次。
2. **读轴与 durability 无关**：0.90–1.14，全在 run 内噪声内 —— 读不进 WAL。
   `rowidxrange` 的 1.14 是「反过来」的那一例，它说明这组单次 best-of-2 的噪声量级
   就是 ±15%；所以下结论只看方向（区间与等值同价），不看百分位小数。
3. **写轴不吃多前端，读轴吃**：C 档 `rowget` 754 250 vs A 档 1 805 948（2.4×）、
   `rowcount` 921 424 vs 2 423 400（2.6×）、`rowidxrange` 114 194 vs 277 062（2.4×）、
   `rowrange_w64` 82 799 vs 158 148（1.9×）；而 `rowput` 单线程 138 815 **高于**
   auto 的 105 666 —— 写由唯一提交线程定序，多前端只多排队。与 §7.9、§8 第 1 条是
   同一件事。
4. **区间的价就是回包大小的价，不是另一条路。** 窗宽 64（回包 64 行）158 148、
   窗宽 400（回包满页 100 行）127 532、`rowlist`（100 行）124 572 —— 后两者差 2.4%，
   而 64→100 行只换来 1.24×，说明区间扫描没有额外的「路径税」，差额主要落在
   帧长与逐行拷贝上。这一格也正是 G8 留下的那个问题（「G4 必须按 `limit` 分档量」）
   的答案：不分档把它与 `rowlist` 对拉，会得出「区间比等值快 27%」这种错结论。
5. **值区间与等值前缀同价。** `rowidxrange` 与 `rowidx` 在 A 档是 277 062 vs 286 026
   （0.97）、C 档 114 194 vs 112 431（1.02）—— 全在噪声内。索引侧上界那条
   `take_while`（§5.1）实测量不到成本：它扫的仍是同一棵 `row_index` 树，只多一次
   每个键的边界比较。
6. **「不 fsync」不等于「无尾延迟」：** A 档写轴的 p99 仍到 1.7–2.6 ms。这一格
   没有做逐层归因（本机 `--auto-checkpoint-mb 0`，快照不参与），只记下来，别拿
   p50 当 SLO 依据。

**不要外推。** 这是 500 行表的本地数：换 `TREEDB_BENCH_WARM=50000` 就是另一种形状
（树更深、页更满、p99 更长），而 `rowlist`/`rowidx`/`rowrange` 的绝对值对表规模
敏感 —— 尤其区间档，窗宽固定时表越大，一页装完的比例越低，`rowrange_w64` 与
`rowrange_w400` 的差别会反过来。

### 7.16 批量形状的价：三种删法与同值消除（2026-11-23，`benches/bench_bulk.sh`）

§5.1 那三条 op 卖的是**记录数**，不是「删得快」，所以量具必须换口径。§7.15 那一套是
时长口径（跑满 `SECS` 秒数 ops/s），拿它量破坏性负载会直接翻车：表在第 0.3 秒就被
删空，剩下 2.7 秒全在数空区间，报出来的 ops/s 反而是全场最好看的那一个。`treedb-bench
--bulk` 因此是**固定工作量口径**：删掉正好 N 行，报 `time_ms` / `records`（= `Stats.seq`
差）/ `writes` / `syncs` / `wal_bytes` / `rows_left`。灌数据、建影子表都在测量窗口外。

**每一档都带格式自己的断言**（不是时间断言，所以不会 flaky）：`clear` 只推 1 个 seq、
`range` 正好 `ceil(N/batch)` 个、`putsame` records=0 且 writes=0、`putchanged` records=N、
三种删法之后 `rows_left=0`、`rename` 之后 `rows_left=N` 且首行来自影子表（payload 前缀
换成 `q`，光看行数对不出「看见的是不是新行」）、`shadow_left=0`（不许留孤儿影子表）、
每格 `errors=0`。数字再好看，断言不过脚本就非零退出。

实测两档规模，同一台机、同一块 160 TiB 机械 XFS（`/data`），`conns 8 pipeline 64`，
`--auto-checkpoint-mb 0`；逐格原始输出 `benches/results/bench_bulk.log`。

**A) 200 000 行，`batch=1000`**

| 档 | never ms | group ms | records | group syncs | group WAL 字节 |
|----|---------:|---------:|--------:|------------:|--------------:|
| `rowdelete` 逐行 | 1 647 | 3 102 | 200 000 | 465 | 7 600 000 |
| `range/b1000` | 197 | 206 | 200 | 2 | 11 800 |
| `range/b10000` | 170 | 172 | 20 | 2 | 1 180 |
| `clear` | 168 | 185 | 1 | 1 | **25** |
| `rename` 影子表顶替 | 191 | 181 | 1 | 1 | 51 |
| `putsame` 同值重写 N 次 | 328 | 232 | **0** | **0** | 0 |
| `putchanged` 真改值 N 次 | 1 885 | 4 142 | 200 000 | 448 | 11 963 890 |

**B) 1 000 000 行，`batch=2000`**

| 档 | never ms | group ms | records | group syncs | group WAL 字节 |
|----|---------:|---------:|--------:|------------:|--------------:|
| `rowdelete` | 6 199 | 16 064 | 1 000 000 | 2 075 | 38 000 000 |
| `range/b2000` | 1 016 | 1 017 | 500 | 2 | 29 500 |
| `range/b10000` | 964 | 1 036 | 100 | 2 | 5 900 |
| `clear` | 1 364 | 924 | 1 | 1 | **25** |
| `rename` | 1 231 | 964 | 1 | 1 | 51 |
| `putsame` | 1 467 | 970 | 0 | 0 | 0 |
| `putchanged` | 9 890 | 21 126 | 1 000 000 | 2 046 | 60 263 890 |

**怎么读这两张表：**

1. **记录数比值就是设计目标本身。** 200k 档：逐行 200 000 条 → 区间 200 条（省 1 000×，
   正好等于 `batch`）→ 清空 1 条（省 200 000×，正好等于 N）；1M 档省 2 000× /
   1 000 000×。WAL 字节同向：38 MB → 29.5 KB → 25 字节（帧头 20 + 一个 `u32` 表 id）。
   `rename` 也是 1 条记录，只是正文里多带两个表名（25 → 51 字节）。
2. **落盘税全在逐行那一档。** group 相对 never 的时长差：`rowdelete` +88%（200k）/
   +159%（1M），而 `range` +5% / +0%。fsync 那一列更直接：465 / 2 075 次 → 2 次 →
   1 次；WAL 在机械盘上时这个量级就是钱（§7.9 把 `group` 的合并比量成「53–63 次合成
   1 次，全距见 §7.9 末」）。`clear` 的两档时间基本同价（168/185、1 364/924），1M 那次 group 反而更快
   —— 一次 fsync 的差别被 run 间噪声盖住了，别拿它当「落盘变快」的结论。
3. **时间不随记录数同比例下降。** 1M 档清空只比逐行快 17.4×（924 ms vs 16 064 ms），
   不是 1 000 000×。理由写在 §5.1：**一条记录 ≠ 常数时间** —— `ClearTable` 的 apply 仍要
   逐行摘二级索引，O(行数)，且全在唯一提交线程上。这三条 op 省的是 WAL 记录、fsync
   与往返，索引维护那笔账一分不少；真要控回合时长就用 `RowRangeDelete` 分批，`max`
   是唯一那个旋钮。
4. **`rename` 与 `clear` 同价**（181 / 964 ms，各 1 条记录），因为改名不动 id，而 `rows`
   与 `row_index` 都以 id 为键 —— 切换只是换掉 `tables` 里一个键，成本与表大小无关。
   但这一档**只量切换那一刀**：影子表得先灌一份同样 N 行的新表（N 次 `RowPut`，代价见
   `putchanged` 那一行），它在测量窗口外。整表替换的真实总成本是「灌新表 + 一次
   rename」，比 `clear` + 重灌更贵，只在「新内容已经在别处备好了」时才划算。
5. **同值消除省下的是整条写路径。** 200k 次逐字节相同的 `RowPut`：records=0、writes=0、
   syncs=0、WAL=0，232 ms；同样条数真改值：records=200 000、448 次 fsync、11.9 MB、
   4 142 ms（17.9×；1M 档 21.8×）。这就是 §5.1 那句「`ops_total − writes_total` 成了
   『被消除的写』的读数」的实测形状：不 append、不 fsync、`seq` 不走 —— 所以副本与
   PITR 也看不见这些「写」，代价记在 §8 第 18 条。
6. **批大小的拐点不在时间上。** 200k 档 b1000 → b10000 是 206 → 172 ms（−16%），1M 档
   1 017 → 1 036 ms（持平）：命中数一多，成本就被「逐行 unindex」吃满，记录数那侧只剩
   2 vs 2 次 fsync。所以选 `batch` 的判据是「一条 op 允许占住提交线程多久」与 WAL 体积，
   不是 wall time。
7. **量具自己也翻过一次车**：`bench_bulk.sh` 的汇总段以前写死过 `range/b1000` 这个键，
   换 `BATCH=2000` 就跑出 awk 除零、整段结论丢失，而脚本照样报「全部通过」。现在键名由
   `-v bat` 动态构造、「这一格在不在」用 `have[k]` 判（`records=0` 是 `putsame` 的正确结果，
   不能拿字段值为 0 当缺格）、awk 非零退出计入 `fail`。这是「脚本自己报绿」不等于
   「量具没坏」的一个实例（§9.1 第 3 条同一条规矩）。

**不要外推**：两档都在同一台机、同一块机械 XFS、`--auto-checkpoint-mb 0` 下跑。开自动
checkpoint 时 `clear` 会连累快照（十万行落进快照正文，收尾那一次写盘更大），这一档
刻意把它关掉是为了隔离变量 —— 那不是「生产也该这么配」。

### 7.17 多实例分片的写扩展（2026-11-23，`benches/shard_write.sh`）

问题从 §8 第 1 条 ③ 那句「写扩展不跟 workers 长」来：既然永远只有一个提交线程在定序，
那么**多个进程实例**能不能扩、能扩多少？引擎里没有分片层，所以这只能按部署形状实测：

- `split S`：S 个 `treedb-server`，各占一个 `--dir` 一个端口，每个实例一个 bench 客户端
  （`PER` 条连接）。实例之间零共享：不同 WAL、不同提交线程。
- `solo S`：**1** 个服务端吃下同样 S 个客户端，每客户端打自己那张表（表名靠
  `TREEDB_BENCH_TABLE` 分开，本轮为这一档加的旗标）。两边的客户端进程数、连接数、
  pipeline 逐格相同 —— 唯一被研究的变量就是服务端实例数。

对照组为什么是「S 个客户端进程压 1 个服务端」而不是「1 个客户端开 S×PER 条连接」：
后者会把工具自己变成瓶颈（一个进程里几十条线程抢 CPU），那时 solo 的数字里混着
「量具跑不动」，分不出哪部分是服务端定序的墙。三条自检不做就是假绿：每格 `errors=0`
（`rowinsert` 用 `PUT_IF_ABSENT`，键撞上回 `EEXIST` 而回包本身是成功往返）、**服务端**
`--stats` 的行数与逐路 `q × 实测时长` 之和对得上（±2% 卡关）、每格从空目录起。

实测（本机 128 核，`SECS=5 PER=8 PIPE=32`，逐格原始输出
`benches/results/shard_write.log`）：

| 实例数 | group solo | 扩展 | group split | 扩展 | split 每实例 | never solo | 扩展 | never split | 扩展 | split 每实例 |
|-------:|-----------:|-----:|------------:|-----:|-------------:|-----------:|-----:|------------:|-----:|-------------:|
| 1 | 37 252 | 1.00 | 38 086 | 1.02 | 38 086 | 114 505 | 1.00 | 115 074 | 1.00 | 115 074 |
| 2 | 46 897 | 1.26 | 66 499 | 1.79 | 33 249 | 108 439 | 0.95 | 207 293 | 1.81 | 103 646 |
| 4 | 69 488 | 1.87 | 122 855 | 3.30 | 30 713 | 113 313 | 0.99 | 409 550 | 3.58 | 102 387 |
| 8 | 86 898 | 2.33 | 177 575 | 4.77 | 22 196 | 107 356 | 0.94 | 744 004 | **6.50** | 93 000 |

服务端 CPU 占用（`/proc/<pid>/stat` 的 utime+stime 差）：never 档 solo/1 用 4.30 核、
solo/8 用 **8.39** 核，而吞吐没变；split/8 共 43.46 核、**每实例 5.43** 核。

**怎么读：**

1. **单实例写吞吐存在一条与客户端压力无关的天花板。** never 档从 1 个客户端加到 8 个：
   114 505 → 107 356 ops/s（0.94×，四格全在噪声里），而服务端烧的核从 4.30 涨到 8.39
   —— 多一倍 CPU 换不到任何吞吐。这就是排队而不是算力：worker 能并行收发包，但
   定序与 apply 只有一路（§2.3 A′）。
2. **进程级分片确实能扩。** never 档 8 实例 744 004 ops/s = **6.50×**（每实例仍是 93 000，
   单实例基线的 81%）；group 档 **4.77×**（每实例 22 196 = 60%）。这台机 128 核，8 实例
   远没碰 CPU 顶，所以剩下的 19–40% 不是「核不够」而是真实开销：客户端侧连接、页缓存
   与中断、每实例自己的 worker。
3. **group 档 solo 也能涨到 2.33× —— 但那是合并率涨，不是扩展性涨。** 客户端多 →
   一个 fsync 窗口里排上的 op 多；每实例吞吐从 37 252 塌到 10 862（−71%），而 split 只
   从 38 086 到 22 196（−42%）。拿 solo 的总量曲线去论证「单实例也能扩」是把合并
   收益误读成并行收益 —— 这正是本档要分开两列的原因。
4. **分片不是免费的。** 这些数字的背面是 S 份内存状态 + S 份 WAL + S 个进程各自
   checkpoint / 备份 / 副本 / fencing，而且**路由要接入方自己做**：引擎不认得「表 X 在哪个
   实例上」，跨实例没有事务、没有级联、表不共享（§8 第 19 条）。
5. **外推边界就是那列核数。** split 每实例 ~5.4 核：同机实例数只能堆到「核数 / 5.4」，
   不够时扩展比会先于实例数封顶。换机器要重跑，别拿 6.50× 当常数。

**不要外推**：负载是 `rowinsert` 纯写、单表、键带 nonce，所以这一档量的是写轴上限；
混合读写、多表热点分布不均的形状要靠 §7.15 那一套另测。

### 7.18 删除风暴：通用表与 POSIX 的收敛、自动 checkpoint 和 RSS（2026-11-25）

§7.16 量的是「哪一种批量删法省日志」，这一节换一个问题：**删完以后，状态、WAL、
快照、锁与进程驻留内存是否真的收口**。两套语料不能混成一条曲线：通用表走
`RowDelete`/put-delete churn，POSIX 走真实 `Unlink`/`Rmdir`、唯一内容引用与目录项。

**采集口径**：本机 `127.0.0.1`，`workers=8`、`durability=group`；通用表
`rows=100000 cycles=4 conns=8 pipeline=64`，POSIX `rows=100000 conns=8 pipeline=64`。
通用表原始输出是 `benches/results/delete_storm.log` / `delete_storm.tsv`，代码指纹
`80538c44416a5ef7ef12542c7b7069672dcc9f1d057184df3b508a822afbd969`；POSIX 原始输出是
`posix_delete_storm.log` / `posix_delete_storm.tsv`，代码指纹
`1446c80ddf757aa08170a63ee5c9a635744242ce509aadfe6b9e839b053bf48a`。两份日志都记录
采集时的 `HEAD=e013701c9c33261258643e54cf85b05cfd79d306` 与本节尚未提交的工作树指纹。
每个数字都能由对应 TSV 重算；`benches/doc_table_check.py` 同时检查七张表与下列不变量。

**通用表：工作量与落盘。** `rowdelete` 删正好 N 行；`churn` 做 N×cycles 次删后重插，
所以 op 数是 `2×N×cycles`。两相的 records/writes 都等于各自 op 数。`checkpoints_auto`
是测量结束后再读一次 `--stats` 得到的累计值（`rowdelete` 的 bench 自报 4 → 8，第 9 次
在收尾等待里出现；`churn` 收尾仍读到 42，即没有新增），不是显式补做的那一次。

| phase | rows | cycles | ops | time_ms | records | writes | syncs | checkpoints_start | checkpoints_auto | wal_start | wal_peak | wal_end | wal_after_auto | snap_seq_start | snap_seq_auto | snap_bytes_auto | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| rowdelete | 100 000 | 4 | 100 000 | 1 470 | 100 000 | 100 000 | 219 | 4 | 9 | 1 605 314 | 1 605 314 | 1 158 848 | 103 360 | 72 501 | 197 281 | 101 760 | 0 |
| churn | 100 000 | 4 | 800 000 | 14 427 | 800 000 | 800 000 | 1 743 | 5 | 42 | 554 564 | 1 680 508 | 678 132 | 678 132 | 90 501 | 888 769 | 3 484 230 | 0 |

**通用表：崩溃尾巴与终态。** 自动快照收口后只写两条终态不变的尾巴记录，然后直接
SIGKILL；重开时 WAL 逻辑长度必须与崩溃前相同，才能证明恢复路径真的吃了「快照 +
尾巴」。`rowdelete` 重开是空表，`churn` 重开保留 N 行并逐行核对内容。

| phase | tail_records | wal_before_crash | wal_reopen | rows_reopen | verify_checked |
|---|---:|---:|---:|---:|---:|
| rowdelete | 2 | 103 472 | 103 472 | 0 | 0 |
| churn | 2 | 678 244 | 678 244 | 100 000 | 100 000 |

**通用表：服务端 RSS。** 采样对象是真 `treedb-server` PID 的 `/proc/<pid>/status`，不是
bench 客户端。`retained_delta_kb = rss_after_kb − rss_before_kb`；采样在 SIGKILL 前停止，
所以不包含崩溃与重开成本。retained RSS 是分配器/进程仍持有的驻留页，**不是**泄漏证明，
也不是 allocator 级碎片指标。

| phase | rss_before_kb | rss_after_kb | rss_reopen_kb | retained_delta_kb | rss_peak_kb | hwm_after_kb | hwm_peak_kb | rss_samples |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| rowdelete | 2 476 | 55 248 | 5 280 | 52 772 | 113 640 | 113 836 | 113 836 | 111 |
| churn | 2 636 | 84 824 | 56 912 | 82 188 | 135 392 | 135 436 | 135 436 | 508 |

**POSIX 的固定工作量形状。** seed 与 delete 分两次服务端生命周期：先造 N 个文件、N 个
空目录与 N 个唯一 `data_ref`，优雅停服 checkpoint 后再起 delete 服务。这样删除起点 WAL=0，
停服 Report 的 `worst_round_us` 也不会混入造 2N 个 inode 的成本。每个文件只有唯一名字，
因此 N 个 `Unlink` 都必须回非空 `NodeRef`，N 个 `Rmdir` 回空 `NodeRef`。无锁语料下：

- `Unlink` = `DelDentry + DelNode + DelRef`，3 条记录；
- `Rmdir` = `DelDentry + DelNode + PutNode(parent)`，3 条记录；
- 合计固定 `2N` op、`6N` records、`2N` writes。

延迟沿用仓库现有 bench 口径：一个 pipeline 的往返微秒数除以实际回包数，再按 op 重复求
nearest-rank 分位。它是 **pipeline 摊还 RTT**，不是 `pipeline=1` 的单请求延迟；两档各
`lat_samples=200000`，与 `2N` 逐项相等。

| auto_mb | rows | ops | time_ms | records | writes | syncs | lat_p50_us | lat_p90_us | lat_p99_us | lat_max_us | lat_samples | freed_refs | verify_refs | errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 100 000 | 200 000 | 5 823 | 600 000 | 200 000 | 403 | 181 | 509 | 802 | 1 372 | 200 000 | 100 000 | 100 000 | 0 |
| 1 | 100 000 | 200 000 | 5 433 | 600 000 | 200 000 | 392 | 142 | 455 | 808 | 1 235 | 200 000 | 100 000 | 100 000 | 0 |

**POSIX：自动 checkpoint 与账本。** 开启档在删除过程中先产生 20 次 checkpoint，等待
收口时读到第 21 次；快照水位从 900002 前进到 1495778，WAL 从采样峰值 4 984 320 B 降到
234 496 B。关闭档观察 500ms，checkpoint、快照水位与 WAL 均不动。关闭档的
`snap_bytes_auto=28800197` 是 seed 停服时显式 checkpoint 留下的**旧快照**，不能当作自动
checkpoint 证据；两档停服后的空终态快照都是 197 B。账本基线是 root view 自身的一个
node，故 `nodes=1`、其余三项为 0。

| auto_mb | checkpoints_start | checkpoints_auto | wal_start | wal_peak | wal_end | wal_after_auto | snap_seq_start | snap_seq_auto | snap_bytes_auto | snap_bytes_shutdown | nodes_left | dentries_left | locks_left | refs_left | errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 0 | 0 | 0 | 33 000 000 | 33 000 000 | 33 000 000 | 900 002 | 900 002 | 28 800 197 | 197 | 1 | 0 | 0 | 0 | 0 |
| 1 | 0 | 21 | 0 | 4 984 320 | 1 332 736 | 234 496 | 900 002 | 1 495 778 | 201 605 | 197 | 1 | 0 | 0 | 0 | 0 |

**POSIX：重开核对。** 10 万行联合脚本走的是优雅停服 → 同目录重开，不是崩溃恢复；重开后
`ReadDir(root)` 必须空，且 N 个 `RefLookup` 全部返回 `(count=0, bytes=0)`。SIGKILL 下
「自动快照 + 小尾巴」的 POSIX 恢复另有小规模真进程回归
`posix_delete_and_small_tail_survives_sigkill_reopen`，不能把两者混称为同一轮 10 万行崩溃实测。

| auto_mb | nodes_reopen | dentries_reopen | locks_reopen | refs_reopen | verify_checked_reopen | errors |
|---:|---:|---:|---:|---:|---:|---:|
| 0 | 1 | 0 | 0 | 0 | 100 000 | 0 |
| 1 | 1 | 0 | 0 | 0 | 100 000 | 0 |

**POSIX：服务端 RSS 与回合。** RSS 采样在优雅停服前停止，因此不含停服 checkpoint 的峰值。
`worst_round_us` 不在 `EngineStats` 里，而从 delete 服务端唯一的停服汇总行读取；该生命周期
包含删除、自动 checkpoint 等待、Stats 抓取与停服请求，但 seed 已在另一个进程生命周期完成。
它是服务端最坏回合，不是客户端 p99。

| auto_mb | rss_before_kb | rss_after_kb | rss_reopen_kb | retained_delta_kb | rss_peak_kb | hwm_after_kb | hwm_peak_kb | rss_samples | worst_round_us | rounds |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 104 656 | 105 524 | 4 008 | 868 | 105 524 | 131 436 | 131 436 | 196 | 87 804 | 452 |
| 1 | 104 708 | 165 144 | 3 812 | 60 436 | 264 688 | 266 272 | 266 272 | 167 | 103 292 | 415 |

**锁的修复与回归边界。** `LockSet` 的释放入口先检查 inode；inode 消失后，表里的永久锁
无法再被客户端释放，而周期 sweep 只删过期租约。本轮在 `Unlink`/`Rmdir` 确认
`last == true` 时按 `(view, ino)` 前缀级联写 `DelLock`；多链接文件摘掉一个名字时只减
`nlink`，node/ref/lock 都保留。`last_posix_delete_cascades_locks_and_returns_ledger_to_baseline`
关掉周期 sweep，分别钉住永久锁与已过期锁：中间态全保留，最后一个名字删除后文件锁消失，
空目录删除后目录锁也消失。`DropView`/`PurgeView` 原本已有级联；`Rename` 覆盖目标与
`Symlink` upsert 的同类路径本轮**未审计、未修改**。10 万行 POSIX 性能语料本身不带锁，不能用
`locks_left=0` 替代锁语义回归。

**这轮还不能回答的**：allocator 级碎片、跨机械盘/SSD 的绝对延迟、10 万行 POSIX 的
SIGKILL 恢复、`Rename`/`Symlink` 覆盖路径的锁收敛，均为**未实测**。「混合读写下万级视图
的并发曲线」原先也挂在这里，现已由 §7.19 量出并单列边界；剩下四项进入后续 POSIX 审计，
不在本节冒充已闭合。

### 7.19 万级视图下的混合读写

§7.18 的 POSIX 语料是「先造满、再清空」，两个相位各自纯写。真实挂载点不是这个形状：
`ls` 与 `open`/`unlink` 混在同一条连接上，视图（一个客户端一棵树）可以到万级。这一节换
一个问题：**读多写少、边建边删的混合负载下，万级视图是否仍然账本守恒、并发是否还在
扩展、驻留内存到底涨在谁身上**。

**语料形状与记录公式。** 两套量具共用一个形状，只差规模。种子档 `mvseed` 造 N 个视图、
每视图 W 个文件：`CreateView` = `PutView` + `PutNode(root)` 两条记录，一次不带
`data_ref` 的 `Create` = `bump` + `PutNode` + `PutDentry` 三条，于是

- 种子固定 `records = 2N + 3NW`、`writes = N + NW`、终态
  `(views, nodes, dentries) = (N, N(W+1), NW)`；W=20 即每视图 62 条记录，四档实测
  `seed_records` = 6 200 / 62 000 / 310 000 / 620 000 与公式逐项相等。

测量档 `mvmix` 每组固定 5 个 op：2 次 `Lookup`（同目录两个不同名字）+ 1 次
`ReadDir(offset=0, limit=20)` + 1 次 `Create(唯一临时名)` + 1 次 `Unlink(同名)`。
`Lookup`/`ReadDir` 不产生记录，`Create` 3 条，最后一个名字且无锁无 ref 的 `Unlink`
2 条，所以 G 组恰好 `records = 5G`、`writes = 2G` 且**净零**。八格实测
`ops == records == 5G`、`writes == lookups == 2G`、`readdirs == creates == unlinks == G`
全部成立，`nodes_left`/`dentries_left` 回到种子态。

**为什么一组必须落在同一个 pipeline 批里。** `src/net/multi.rs:549` 的判据是
`has_write = route.iter().any(|o| !o.is_readonly())`：批内只要含一个写 op，整批就交给
唯一提交线程按序执行，只有全只读批才分流到前端 worker。于是「先 Create 再 Unlink」在
同一批里天然有序，客户端不必排队等 ack，也不会出现 unlink 先落地的乱序假错。这就是
`PIPE` 必须取 5 的倍数的原因（脚本先验这一条）—— 否则一组被截成两批，顺序承诺消失。

**两个服务端生命周期。** 每格先起一个进程跑 `mvseed`，优雅停服 checkpoint 后**重开**
再跑 `mvmix`。所以 `nodes_left = 210 000`、`dentries_left = 200 000` 不是「引擎报自己
的内存态」，而是重启恢复出来的账本；种子的停服 `worst_round_us` 单独打印并标注
「不计入混合档」，表里那一列只含测量生命周期。

**采集口径。** 本机 `127.0.0.1`、`durability=group`、`workers=8`、
`--auto-checkpoint-mb 1`、`rows=20`、`pipeline=50`、`secs=5`、`sample_ms=20`，矩阵是
`views ∈ {100, 1 000, 5 000, 10 000} × conns ∈ {8, 64}` 共 8 格。原始输出
`benches/results/many_views_mix.log` / `many_views_mix.tsv`（46 列 × 8 行），采集时
`HEAD=c45579370670169f6bc3f599b87c585d1e60b1a5`、代码指纹
`92487d46f10715c491d1c85b0fab887d2c1b75e7c8cfc2e15a9b6cac6d45e373`（`src/bin/bench.rs` +
`benches/many_views_mix.sh` + `benches/many_views.sh` 三者联合）。本节五张表由 TSV 机器
生成，再由 `benches/doc_table_check.py` 按 `(auto_mb, views, conns)` 三键逐格回对，
抄错任一格即非零退出。

**混合负载工作量。**

| views | rows | conns | pipeline | secs | time_ms | groups | ops | records | writes | syncs | lookups | readdirs | creates | unlinks | ops_per_s | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 20 | 8 | 50 | 5 | 5 005 | 46 920 | 234 600 | 234 600 | 93 840 | 650 | 93 840 | 46 920 | 46 920 | 46 920 | 46 873 | 0 |
| 100 | 20 | 64 | 50 | 5 | 5 013 | 114 610 | 573 050 | 573 050 | 229 220 | 181 | 229 220 | 114 610 | 114 610 | 114 610 | 114 312 | 0 |
| 1 000 | 20 | 8 | 50 | 5 | 5 016 | 47 910 | 239 550 | 239 550 | 95 820 | 717 | 95 820 | 47 910 | 47 910 | 47 910 | 47 757 | 0 |
| 1 000 | 20 | 64 | 50 | 5 | 5 032 | 108 100 | 540 500 | 540 500 | 216 200 | 170 | 216 200 | 108 100 | 108 100 | 108 100 | 107 412 | 0 |
| 5 000 | 20 | 8 | 50 | 5 | 5 018 | 42 420 | 212 100 | 212 100 | 84 840 | 601 | 84 840 | 42 420 | 42 420 | 42 420 | 42 267 | 0 |
| 5 000 | 20 | 64 | 50 | 5 | 5 015 | 154 100 | 770 500 | 770 500 | 308 200 | 242 | 308 200 | 154 100 | 154 100 | 154 100 | 153 639 | 0 |
| 10 000 | 20 | 8 | 50 | 5 | 5 005 | 45 180 | 225 900 | 225 900 | 90 360 | 635 | 90 360 | 45 180 | 45 180 | 45 180 | 45 134 | 0 |
| 10 000 | 20 | 64 | 50 | 5 | 5 021 | 117 390 | 586 950 | 586 950 | 234 780 | 185 | 234 780 | 117 390 | 117 390 | 117 390 | 116 899 | 0 |

组数是 5 秒窗口积分出来的（42 420–154 100 组），每格 `lat_samples == ops`，即每个 op 都有
一条样本；`ops_per_s` 是 bench 自报的 `ops × 1000 / time_ms`（整数除法），`time_ms` 在
join 之后取，所以 `time_ms ≥ 5 000` 是硬事实而不是目标。

**延迟与覆盖。**

| views | conns | groups | lat_p50_us | lat_p90_us | lat_p99_us | lat_max_us | lat_samples | views_touched | gone_ok | kept_ok | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 8 | 46 920 | 67 | 413 | 753 | 1 522 | 234 600 | 100 | 400 | 400 | 0 |
| 100 | 64 | 114 610 | 572 | 834 | 1 423 | 2 184 | 573 050 | 100 | 400 | 400 | 0 |
| 1 000 | 8 | 47 910 | 71 | 411 | 826 | 1 332 | 239 550 | 1 000 | 400 | 400 | 0 |
| 1 000 | 64 | 108 100 | 630 | 920 | 1 236 | 1 306 | 540 500 | 1 000 | 400 | 400 | 0 |
| 5 000 | 8 | 42 420 | 87 | 428 | 963 | 1 582 | 212 100 | 5 000 | 400 | 400 | 0 |
| 5 000 | 64 | 154 100 | 403 | 733 | 1 141 | 1 601 | 770 500 | 5 000 | 400 | 400 | 0 |
| 10 000 | 8 | 45 180 | 81 | 405 | 895 | 1 606 | 225 900 | 10 000 | 400 | 400 | 0 |
| 10 000 | 64 | 117 390 | 514 | 937 | 1 393 | 1 871 | 586 950 | 10 000 | 400 | 400 | 0 |

8→64 连接：吞吐 ×2.25–3.63（四档依次 2.44 / 2.25 / 3.63 / 2.59），p50 涨 4.6–8.9×，
p99 只涨 1.18–1.89× —— 尾延迟的增幅明显小于中位数，因为 64 连接每次 fsync 摊到
1 266–1 274 个写，8 连接只有 134–144 个；group commit 把「等落盘」从每个请求头上挪到
一次批量提交的头上。`views_touched` 由 bench 内共享位图真实统计（不是 `G % N` 反推），
四档都等于 N，也就是窗口内每个视图至少被摸过一次；`gone_ok = kept_ok = 400` 是头尾各
`min(G, 400)` 个名字的逐名核对（临时名必须 `NotFound`、种子名必须仍在），不是抽样估计。

**视图数不是这条曲线的自变量。** 同连接档内 100 → 10 000 视图，吞吐只在
42 267–47 910（8 连接）/ 107 412–153 639（64 连接）之间摆动，看不到单调衰减；种子时长
263 ms → 31.5 s 则是干净的 O(N)。但单轮抖动本身就有 ±10% 量级，所以「5 000 比 10 000
快」这类读法不成立 —— 这里能读出的是**万级视图数在该形状下不构成结构性代价**；要判定
轻微衰减需要配对复采，本节没做。

**种子、checkpoint 与账本。**

| views | conns | auto_mb | seed_time_ms | seed_records | checkpoints_start | checkpoints_delta | wal_start_bytes | wal_end_bytes | wal_peak_bytes | snap_seq_start | snap_seq_end | nodes_left | dentries_left | locks_left | refs_left | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 8 | 1 | 263 | 6 200 | 0 | 13 | 0 | 572 220 | 1 172 310 | 6 200 | 231 450 | 2 100 | 2 000 | 0 | 0 | 0 |
| 100 | 64 | 1 | 263 | 6 200 | 0 | 29 | 0 | 988 800 | 1 384 320 | 240 800 | 797 850 | 2 100 | 2 000 | 0 | 0 | 0 |
| 1 000 | 8 | 1 | 3 297 | 62 000 | 0 | 13 | 0 | 859 860 | 1 441 260 | 62 000 | 287 500 | 21 000 | 20 000 | 0 | 0 | 0 |
| 1 000 | 64 | 1 | 3 297 | 62 000 | 0 | 28 | 0 | 197 760 | 1 965 420 | 301 550 | 838 850 | 21 000 | 20 000 | 0 | 0 | 0 |
| 5 000 | 8 | 1 | 16 148 | 310 000 | 0 | 11 | 0 | 1 029 000 | 1 701 000 | 310 000 | 504 950 | 105 000 | 100 000 | 0 | 0 | 0 |
| 5 000 | 64 | 1 | 16 148 | 310 000 | 0 | 22 | 0 | 2 966 400 | 4 350 720 | 522 100 | 1 244 600 | 105 000 | 100 000 | 0 | 0 | 0 |
| 10 000 | 8 | 1 | 31 513 | 620 000 | 0 | 12 | 0 | 1 074 060 | 1 890 720 | 620 000 | 828 350 | 210 000 | 200 000 | 0 | 0 | 0 |
| 10 000 | 64 | 1 | 31 513 | 620 000 | 0 | 13 | 0 | 2 966 400 | 5 679 360 | 845 900 | 1 384 850 | 210 000 | 200 000 | 0 | 0 | 0 |

种入速率 18 800–23 600 records/s。8 连接档窗口内 checkpoint 增量 11–13 次，
`records / checkpoint` = 18 046–19 281，与「WAL 越过 1 MiB 才派快照」自洽（约 55 B/
record，由这两个数推算，不是引擎直接报的）；64 连接档同一阈值下这个比值涨到
19 760–45 150，WAL 峰值也跟着从 1.12–1.80 MB 涨到 1.32–5.42 MB —— 说明并发上去之后
**旁路编码追不上写入**，阈值只是触发下限，两次落盘之间 WAL 实际跑多远由上一份快照的
编码耗时决定。`wal_end_bytes < wal_peak_bytes` 八格全部成立（比值 0.10–0.71），即窗口内
确实落过快照并截了 WAL。`wal_start_bytes` 八格全为 0，但**那是种子优雅停服 checkpoint +
重开的后果，不是不变量**：若种子跑完直接 SIGKILL，这里就是一段非零尾巴，
`benches/delete_storm.sh` 的 `tail_records` 量的正是那种形状。`locks_left`/`refs_left`
恒 0 只说明本语料不申请锁、不挂内容引用；锁的清理语义证据仍归
`tests/delete_storm.rs::last_posix_delete_cascades_locks_and_returns_ledger_to_baseline`，
不能拿这里的 0 当锁语义回归。

**回归测试钉住同一套公式。** `tests/mixed_views.rs`（5 条，全部起真服务端进程）实跑打印：

- `seed_ledger_matches_the_documented_record_formula` →
  `mv_seed views=2000 rows=20 records=124000 writes=42000 paged=2000 errors=0`
  （124 000 = 2·2000 + 3·2000·20，且 `ReadDir` 分页真的走了一圈）；
- `one_mixed_group_costs_five_records_and_two_writes` → `records_per_group=5 writes_per_group=2`；
- `mixed_groups_leave_the_ledger_exactly_where_it_started` → `mv_netzero views=200 groups=5000
  records=25000 writes=10000 touched=200 gone=400 kept=400 reopen_gone=400`；
- `concurrent_mixed_groups_stay_net_zero_across_connections` → `mv_concurrent views=200
  conns=8 groups=8000 records=40000 touched=200`；
- `view_rotation_covers_min_of_views_and_groups` → `more_touched=60 fewer_touched=25`。

第三条在重启后**另开连接**复查那批临时名仍全部 `NotFound`，所以矩阵里「重启后账本」那句
在函数级也有一条秒级回归守着；第四条把 8 条连接打同一批视图，验证跨连接仍然净零。

**服务端 RSS 与回合。** RSS 采样对象是真 `treedb-server` 的 `/proc/<pid>/status`，采样在
优雅停服前停止，所以不含停服 checkpoint 的峰值。

| views | conns | rss_before_kb | rss_after_kb | retained_delta_kb | rss_peak_kb | hwm_after_kb | rss_samples | worst_round_us | rounds |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 100 | 8 | 4 932 | 14 164 | 9 232 | 14 164 | 14 164 | 154 | 65 700 | 705 |
| 100 | 64 | 4 876 | 17 992 | 13 116 | 17 992 | 17 992 | 150 | 117 762 | 199 |
| 1 000 | 8 | 13 496 | 18 000 | 4 504 | 32 592 | 32 592 | 150 | 52 610 | 752 |
| 1 000 | 64 | 13 452 | 21 388 | 7 936 | 36 912 | 36 996 | 151 | 88 671 | 190 |
| 5 000 | 8 | 52 288 | 66 900 | 14 612 | 126 688 | 129 104 | 152 | 65 310 | 633 |
| 5 000 | 64 | 52 180 | 107 636 | 55 456 | 139 316 | 139 316 | 150 | 115 393 | 258 |
| 10 000 | 8 | 100 424 | 247 400 | 146 976 | 247 400 | 247 400 | 150 | 62 672 | 668 |
| 10 000 | 64 | 100 436 | 258 644 | 158 208 | 265 592 | 267 704 | 150 | 115 390 | 204 |

`retained_delta_kb` 随视图数从 9 232 涨到 158 208，`rss_peak_kb` 同向。10 000 视图那两格
看着像「混合读写会驻留 150 MB」，但这个读法不能就这么进结论 —— 它可能是快照编码的瞬时
缓冲恰好被采样撞上。于是做一次单格隔离。

#### 7.19.1 单格隔离：那 150 MB 是 churn 的还是快照的

同机、同 `views=10 000 conns=8 rows=20 pipeline=50 secs=5`，**只切
`--auto-checkpoint-mb`**；`OUT=…`/`TSV=…` 两个旋钮把落盘指到
`benches/results/mix_isolate_auto{0,1}.{log,tsv}`，不覆盖正式矩阵。两份日志的
`HEAD` 与 `code_sha256` 与正式矩阵完全相同，所以这是一组可比的 A/B。

| auto_mb | views | conns | groups | time_ms | ops_per_s | checkpoints_delta | wal_end_bytes | wal_peak_bytes | rss_before_kb | rss_after_kb | retained_delta_kb | rss_peak_kb | hwm_after_kb | rss_samples | errors |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 10 000 | 8 | 49 980 | 5 009 | 49 890 | 0 | 15 260 550 | 15 260 550 | 100 344 | 102 140 | 1 796 | 102 140 | 122 436 | 153 | 0 |
| 1 | 10 000 | 8 | 40 760 | 5 014 | 40 646 | 11 | 801 720 | 2 026 770 | 100 440 | 104 012 | 3 572 | 247 624 | 247 624 | 152 | 0 |

**读数。** 关闭档整段 5 秒没有任何快照（`checkpoints_delta = 0`，`snap_seq` 停在
620 000），WAL 一路涨到 15 260 550 B —— 是矩阵同格峰值 1 890 720 B 的 8 倍，因为它没有
东西去截它。而这一档 `VmRSS` 只涨 **1 796 KiB**；开启档涨 **3 572 KiB**，窗口末同样回到
104 MB 量级，但采样峰值 247 624 KiB。所以：

- 万级视图下 Create/Unlink churn 自身只值 MB 级驻留，**不是** 150 MB 的来源；
- 矩阵里那 147 000 / 158 000 KiB 是「编码一次 210 000 节点 / 200 000 目录项的快照」的
  瞬时缓冲：8 连接那格 `rss_after_kb == rss_peak_kb`（247 400），64 连接那格
  258 644 也只比峰值 265 592 低 6 948 KiB，两次收尾采样都正吃在编码窗口里。它们报的是
  「此刻仍在编码」，不是稳态驻留。

这比 §7.18 末尾那句「retained RSS 不是泄漏证明」具体一步：**峰值 RSS 由快照编码决定，
量级 O(节点数)；retained 由分配器仍持有的页决定，本形状下只有 KB 级。** 两者混写成一个
「涨了多少内存」会得出完全错误的容量结论。

自动 checkpoint 的价格也在这两格里，而且不只付吞吐：49 890 → 40 646 ops/s（**−18.5 %**），
组数同比例 −18.4 %（49 980 → 40 760）；p50 73 → 87 µs、p99 643 → 1 060 µs、
max 1 094 → 2 079 µs，服务端最坏回合 42 039 → 147 799 µs（743 → 598 轮）。快照编码跑在
旁路线程（`src/engine/core.rs:412`），但阈值判在事件循环的周期任务里（
`src/net/multi.rs:1234`），1 MiB 对一棵 21 万节点的树意味着五秒里派了 11 次全树重编。
默认 64 MiB 档下这笔开销会被摊薄，代价换成 WAL 按 64 倍长；两个默认值之间的取舍本节
**没有扫档，未实测**。

**本节仍未回答的。** allocator 级碎片（需要 `malloc_info` 或换 allocator）；十万级视图
与每视图更多文件（矩阵只到 10 000 × 20）；混合读写下的 SIGKILL 崩溃恢复（本节全程优雅
停服 + 重开，崩溃形状只有 §7.18 通用表与 `tests/` 里的小规模真进程用例覆盖）；
`Rename`/`Symlink` 覆盖路径的锁收敛（沿用 §7.18 同一条边界，未动）；
`--auto-checkpoint-mb` 的摊薄曲线；跨机械盘/SSD 的绝对延迟；视图数轻微衰减的配对复采。

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
2. **状态全内存**（2026-11-23 已量，分层仍未做）：恢复时间 = 快照读入 + 尾日志重放。
   状态规模上限就是内存上限；这个规模不只算 inode —— 通用表的每一行都全量住在
   `rows` 里（`row_index` 额外占一份，它不进快照但占内存）。
   `tests/scale_restore.rs` 量了 10 万 → 1 000 万节点这条曲线（10 万档进回归板，
   百万以上手动跑：`TREEDB_RESTORE_DIR=… TREEDB_RESTORE_TAIL=100000 cargo test
   --release --test scale_restore -- --ignored --nocapture`）：

   | 节点数 | 快照 | 插入（单条提交） | checkpoint | 恢复冷 / 热 | 重放 10 万条 | RSS / 峰值 |
   |--------|------|------------------|------------|-------------|--------------|-------------|
   | 10 万 | 12.5 MB | 0.37 s（267 k op/s） | 80 ms | 97 / 83 ms | 46 ms（2 万条） | 43 / 57 MB |
   | 100 万 | 124.9 MB | 3.83 s（261 k op/s） | 1 109 ms | 1 228 / 1 101 ms | 184 ms | 425 / 551 MB |
   | 300 万 | 374.8 MB | 12.14 s（247 k op/s） | 2 532 ms | 3 764 / 3 335 ms | 319 ms | 1 273 / 1 649 MB |
   | 1 000 万 | 1 249.3 MB | 41.3 s（242 k op/s） | 8 827 ms | 12 927 / 11 659 ms | 619 ms | 4 243 / 5 493 MB |
   （本表是 2026-11-23 第二轮。同日早一轮的 checkpoint 一列快约 2×（1 000 万档
   4 332 ms），重测两遍仍是 8.6–8.8 s，归因未查 —— 同期根盘已用 96%，测试目录
   落在 /tmp。两轮不可互减，引用请整行取同一轮。）
   三条硬数：① **内存 445–455 B/节点**（RSS，线性到 1 000 万）；② **快照 131 B/节点**，
   checkpoint 期间峰值 = RSS + 快照大小（编码缓冲，1 000 万档 5 493 MB），编码吞吐
   ≈ 115–150 MB/s、0.8–1.1 µs/节点；③ **恢复 ≈ 1.0–1.17 µs/节点**（页缓存热时
   857 k–987 k 节点/s；冷读 1.25 GB 只多 1.1–1.4 s，约 9–11%），尾巴重放
   13 万–43 万条/秒（随状态变大而下降 —— 每条都要往更大的 dentry 表里插，
   1 000 万档 13 万/s）。同日把 WAL 的两个读口（`open` 的 scan、`replay` 的
   scan_each）从逐帧无缓冲读换成 1 MiB 缓冲后，重放 10 万条从 724 / 858 / 1 173 ms
   降到 184 / 319 / 619 ms（与 §8 第 7 条② 同一笔优化），恢复的冷/热两列基本没动
   —— 那边的大头是快照解码。
   线性外推到 **1 亿 inode**（外推，未实测）：RSS ≈ 44.5 GB、checkpoint 峰值 ≈ 57.6 GB、
   快照 ≈ 13.1 GB、重启 ≈ 117 s（热）/ 129 s（冷）；若一直不 checkpoint，WAL 按
   209 B/节点会到 20.9 GB。这三个数就是「分层」的触发线：单机内存到 64 GB 就该动手，把 `nodes` 的
   冷段挪出内存（或换 mmap 快照 + 懒加载）——**分层本轮仍没做**，真上亿级前必须先
   做这一项设计；本轮只把「未量」变成「有数」，顺带把百 MB 级恢复的语义钉进回归板
   （`restore_survives_a_hundred_thousand_nodes`）。
3. **`never` 档会丢数据**：它只是把 fsync 从提交路径挪走，崩溃丢最近 `sync_ms` 窗口。
   选它必须是有外部冗余的场景。
4. **无数据面 / 无 fd 语义**（§5.3）：`O_TRUNC`、POSIX 记录锁的 `(pid,fd)` 所有权这类
   由 FUSE 层自己维护，引擎不参与。
5. **无跨视图单 op、无用户可见事务**（§5.3）：`Rename` 等每条具体 op 只落在一个视图里；
   `Op::Batch` 能一次原子地跨视图（§5.1），但它不是会话事务（无 `BEGIN`/`COMMIT`）。
6. **有异步只读副本，无同步复制**（§6.6）：`refs` 账本和 WAL 仍是单主，副本只是
   同一份日志的第二个消费者。四条具体边界：① 副本**不保证读到已确认的写**，
   要「读己之写」的客户端仍得读主；② 主身份认证只能证明「对端持有这把密钥」（§4.4，
   且默认关）；③ **fencing token 只在副本侧生效**（v0.4.0，§6.6「切主」）：副本拒收
   被提掉权的旧主，但旧主自己不知道权被拿走，它对自己的客户端照旧服务，所以切主
   必须真停旧主；④ 没有自动故障转移 —— 主没了副本只是继续答读，不会把自己提上
   去，`--promote` 是人手动的一步（它不起服务，只抬令牌）。写扩展也不因副本而变
   （第 1 条第 ③ 项那句仍成立）。HA 要拿上层继续用 PG 做权威仍是另一条路（过渡档现状）。
7. ~~**`checkpoint` 是 stop-the-round**~~ **已闭合**（2026-11-21）：自动档改成
   **旁路重放式快照** —— 热路径只取一次 `wal.durable_seq()` 再 spawn 一个
   `treedb-snap` 线程；线程读磁盘上的 `state.snap` + 重放 WAL 到该水位，编码后
   `.tmp` → `fsync` → `rename`，事件循环每轮 `checkpoint_poll()` 就把结果收进来。
   收尾不再 `wal.reset` —— 那会连「取水位之后才 ack、只活在日志里」的尾巴一起抹掉，
   等于静默丢已确认的写 —— 改用 `Wal::truncate_before(seq)` 只搬走 `> seq` 的记录。
   实测（`background_snapshot_costs_a_dispatch_not_a_pause`，20 000 节点、`never` 档）：
   同步快照停轮 23.2 ms，派工 37.5 µs（约 1/600），且停轮不再随状态大小增长。
   仍有的三条边界：① 一份在编时不派第二份（同一个 `state.snap.tmp`）；② 后台那份
   读盘 + 重放 + 全量编码的总工作量**没有减少**，只是挪出了回合 —— 派工那一刻到
   上一次快照之间的日志越长，后台那段越久；收尾的 `truncate_before` 仍然在提交
   线程上逐帧读一遍日志（丢掉水位前的），是 O(日志长度)、不是 O(状态)。这一笔
   2026-11-23 单独量了，原文写的「几十毫秒量级」在**改之前**是错的：老实现一帧
   两次 `read` 系统调用、保留的记录再把 20 字节头拆成四次 `write_all`，64 MiB /
   88 万条记录下收尾停 510 ms，尾巴 32 MiB 时 2.7 s（`tests/checkpoint_tail.rs`
   的端到端计时，`never` 档、状态只有 1 个节点，所以没把编快照的账算进来）。两侧都换
   1 MiB 缓冲、头一次写完、CRC 复用刚校验过的值之后，同一条路径是 82 / 112 / 267 ms
   （尾巴 0 / 4 / 32 MiB），`Wal::open` 的 scan 与 `replay` 的 scan_each 同款改进
   （64 MiB 扫描 664–762 → 59–199 ms，第三轮起 59–144）；剩下的下限是 CRC 本身
   （64 MiB ≈ 50 ms，slicing-by-8）。③ 退出、显式
   `Checkpoint` 与副本装快照之前都 `checkpoint_wait()` 把它收掉，代价落在该处。
   `auto_checkpoint_bytes` 现在决定的是派工频率，不再决定停轮长短。
8. **视图名 25 字节上限**：与 grovefs `pg_store::schema_for` 对齐；突破要同步改
   `VIEW_NAME_MAX`。
9. **预分配的空间代价**：WAL 文件至少 4 MiB 且倍增，`du` 看到的比逻辑长度大。现在
   两个数都在 `Stats` 里且各说一件事：`wal_bytes` 是**逻辑长度**（checkpoint 阈值按它
   算），`wal_disk_bytes` 是文件实际长度（容量规划按它算，只会 ≥ 前者）；`snap_bytes`
   是上一代快照的字节数（未 checkpoint 时为 0）。`treedb-server --stats` 直接把这三个
   数并列打出来。注意 `snap_bytes` 滞后一代：它反映的是上一次落盘的快照，不是当前
   状态的编码长度。
10. **`--bench`/`--pgbench` 是本机同盘对照，不是云端结论**：换 SSD/NVMe、换 PG 大版本、
    换 `wal_compression`/`full_page_writes` 配置，绝对数字都会变，倍数关系才可信。
11. ~~`always` 档在高并发下没有优势，尾延迟还会跑飞~~ **已修**（§7.9 第 3 条）：
    `Always` 分支删了，`always` 与 `group` 现在走同一个「一轮一次 fsync」的提交点，
    64 并发机械盘上 1 891 → 10 913 ops/s、p99 81.5 → 31.1 ms；11-25 在 `HEAD=c455793`
    上按 `benches/sync_ratio.sh 3` 复跑同一格，读到 9 823 ops/s、p99 32.8 ms、498 次
    fsync 摊 30 129 次写（§7.9 末的复跑表）。留下的真实边界有两条：
    ① 不再提供「已收到但未 ack 的写也立即落盘」这一档（合批后就没这个语义了，
    单连接下 `always` 自然退化为每 op 一次 fsync）；② 写扩展仍付「单提交线程 +
    每轮一次阻塞 fsync」的代价（机械盘 ≈ 3.8 ms/轮），要再进一层得把 flush 从提交
    线程挪出去，见 §11 第 8 条。
12. **快照迁移只认上一代**（§6.5）：`treedb-migrate` 只接受
    `SNAP_LEGACY_VERSION`（= 3）那一个版本位，两代之前的目录仍然只有「重建」这一条
    路 —— 这是规矩不是遗漏，攒多代 decode 分支会把「格式变更要 +1 并给出门」变成
    「可以攒着」。v3 → v4 还多一道：「四张已删专用表为空」的拒迁闸门（§6.5）。
    互斥那一块已经补上（§6.8）：迁移工具与服务端抢同一把 `.lock`，
    「服务端没停就动手」现在的结果是拿不到锁 → 退出码 1；改写前后各读一次源文件的
    哨兵也保留，它挡的是绕过锁直接改文件的人。迁移本身是「全量读进内存 `State`
    再全量写出」，成本正比于快照大小与 §8 第 2 条同源，没有增量路径。
13. **接入面：认证是可选的，而且只管身份；没有授权、没有传输加密**。这条以前写的是
    「根本没有认证」，v5 之后要改成「补了一小块、剩下的仍然没有」（§4.4）。剩下的
    具体是四条：① **默认仍关** —— `--auth-key-file` 不给就是裸 TCP + 长度前缀 +
    CRC32，`Client::connect` 连上并完成同代版本协商后拿到全部 45 个 op 的权限，包括 `Shutdown` 与
    `Checkpoint`；② **开了也是「一把钥匙开整扇门」** —— 过了握手的连接与其他连接
    权限完全相同，无租户、无配额、无按 op 分组；③ **不加密** —— 握手之后仍是
    明文，CRC32 只判损坏，抓包的人读得到全部数据也随时能 RST；④ **不证明「是当前
    那个主」** —— 这是 v0.4.0 之后要分两半看的一条（第 6 条③）：副本侧有 fencing
    token 了，**客户端侧仍然没有**，一个不会去问“你是第几任主”的客户端照旧能连上
    一个已被换掉的旧主。所以「谁能连到这个端口 + 谁持有这把密钥」还是等于
    「谁能改整个状态」：多租户、配额、审计归属仍得在**引擎之外**做（grovefs 的
    apikey 与网关白名单就是这一层的现状），或者把端口锁在本机回环/私有网段里 ——
    `--listen` 默认就是 `127.0.0.1:7777`。也没有 TLS，没有 Unix socket 选项。视图名
    25 字节那道墙（第 8 条）只是命名空间隔离，不是访问控制。
14. **备份能力是「六形状、三条不做」**。六种：停服后 `cp -a` 目录；`treedb-dump`
    → `treedb-load` 的离线整份导出；副本目录当还原点（`treedb-standby`，必要时
    `--promote`）；`treedb-backup` 从在线主拿一致切面（§6.10）；`--archive-dir` 的
    历史切面归档；以及由它撑起来的 `treedb-pitr` 按 seq 恢复到任意一条 op 末尾
    （§6.11）—— 最后两种是唯一能落在 checkpoint **之间**的形状。v0.7.0 新加的三条批量记录（`Log` 25/26/27）实测也过得了归档 → PITR，且恢复结果与冷 dump 逐字节相同（§6.11 验收段）。三条不做：**无增量
    备份、无加密备份、无归档保留策略**（归档只增不减，清老段是运维的活，且清之前
    要确认那些被当作锚点的快照还在）。归档那两种还有两个前提：开关必须**从第一天
    起一直开着**（关掉期间做过的 checkpoint 会在归档里留一个洞，PITR 越过就报缺口，
    不去拼接两边的历史）；`--to-seq` 要取 `--stats` 打出的 `seq` 行（一条 op 可能产多条
    日志，落在组中间会得到「该 op 只生效一半」的状态，WAL 里没有组边界可查）。
    其余边界不变：dump/load 是**停服的整份导出**（两侧都要目录独占）；`treedb-backup`
    能从在线主拿一致切面、不用停服，但它同样是**整份**导出且切面固定在一个 seq 上，
    不是持续追赶；运行中 `cp -a` 拷不到撕裂尾但切点不可控（实测：比拷贝前一刻多
    14 400 条 seq），所以它不是备份工具；副本目录可以当还原点，且从 v0.4.0 起有了
    `treedb-standby --promote` 这条提升路径（§6.6「切主」），但它只抬令牌、不开服务，
    旧主还得人确认停掉（第 6 条③）。
15. **treedb 后端的连接池是「每进程」共享，池大小就是跨视图并发上限**（§7.12）。
    默认 `GROVEFS_TREEDB_CONNS = 4` 条长连接服务该进程里的**全部**视图，
    `TreedbStore` 不再各持一份 —— 这才是「10 000 视图 4 条连接」能成立的原因，但也
    意味着池小的时候所有视图排在 4 条连接上。代价实测过：8 个视图同时打时把池从
    4 提到 32（8×）只快 **13–26 %**（1 070 → 844 µs/pair），所以默认值不是瓶颈；
    不过**它确实是上限**，要更高并发就调这个环境变量，它改的是进程而不是视图。
    另一条边界：多开一个 meta-server 进程就多一份池（4 条），部署时连接总数要按
    进程数算；`evict_idle` 现在只回收内存，不再回收连接。
16. **跨连接的可见性先于持久性**（§6.2 两段式提交）。状态在 `exec` 的写锁里就改完了，
    `WAL append + fsync` 在那之后；这段窗口里**别的连接已经读得到**那条写。所以
    「读己之写」在 ack 之后成立，「别人读不到还没落盘的写」**不**成立：若这次 fsync 失败
    （进程按契约退出），那条写重启后不存在，而曾经读到它的客户端看到的是一个从没发生
    过、也不会再出现的状态。要消掉这个窗口只有两条路，都有代价：把可见性推到 fsync 之后
    （读者要排在提交线程后面，读尾延迟被 fsync 拖住，§7.9 那种合批收益直接归零），或者给
    读者 MVCC 快照（每条连接都要维持版本，而且「状态全内存」的假设就不再成立）。当前选择：
    承认窗口，把它当 API 语义写进文档 —— 依赖这条的人只能在 ack 之后才开始信。
17. **过期租约的回收已闭合**（原 F6 的尾声，2026-11-23）。`locks` 表里超过 lease 的条目在
    应答时一律被当成不存在（`locks_are_owner_scoped_and_lease_filtered`），而**读 op 仍然
    不产生日志**（§4.2 的只读划分是副本拒写与「worker 就地把算」的判据）——所以回收不放在
    读路径，而是放进提交线程的周期任务：`Engine::sweep_expired_locks(now, limit)`。
    它按 `(view, ino, owner)` 的键序分批走表（每批 `LOCK_SWEEP_BATCH = 1024` 条，游标只在
    内存里），只删 `lease_expiry != 0 && lease_expiry <= now`；`lease_expiry == 0`
    （永不过期）与未到期的锁一行都不动。删行走既有 `Log::DelLock`，因此回收和普通写一样
    进 WAL、进快照、经复制链路收敛到副本 —— 帧格式、op 表、`PROTOCOL_VERSION` 全不变，
    这是挑这个方案的理由：**闭环一个功能洞，不动协议**。副本上不扫（引擎里直接返回
    `(0, 0)`）：副本的 WAL 是主的镜像，它自己发一条 `DelLock` 就会让本地 seq 与主对不上，
    下一批 `WalFetch` 当场报 gap；主发出的同一批日志会把副本收敛到相同状态。
    旋钮 `--lock-sweep-ms`（默认 1000，`0` = 关）。单轮代价与表大小无关，一圈的成本摊在
    `⌈n/1024⌉` 轮上；扫到东西才 `commit_batch`，空扫不写 WAL、不 fsync。
    当轮实测（本机，除注明外 `Durability::Never`）：10 万条过期锁分 **99 轮**（98 满批
    + 1 空扫收尾）扫完、共 100 000 条，**55.3 ms**，每轮 p50 **0.387 ms** / p95 1.204 /
    max 10.986（首轮含页错误）；同一份状态里 10 万条死锁占快照 **3 300 294 B**，扫掉后
    checkpoint 只剩 **294 B**（**-100.0 %**，约 33 B/条）。`Durability::Always` 下每轮
    1 024 条 DelLock + 一次 fsync 为 **1.18–1.66 ms**；同样 3 072 条若逐条走客户端写 op
    （每条一次 fsync）是 **185.7 ms** —— 分批把 3 072 次 fsync 合并成 3 次，约 **66×**。
    常态成本（表里 10 万条**不过期**的锁）：每轮看 1 024 条 **0.198–0.274 ms**，`seq` 与
    `wal_bytes` 都不动 = 确实没写。
    剩余边界四条：① 游标只在内存，进程重启后从表头再扫一圈（只读，代价即上一句的 0.2 ms/轮）；
    ② `Stats.locks` 仍报**行数**，但语义从「只增不减」变成「一个 `--lock-sweep-ms` 周期内
    收敛到有效租约数」——要看实时有效数只能等这一拍；③ 只有主扫，副本等日志；
    ④ inode 的最后一个名字删除必须级联清锁，否则永久锁在 inode 消失后再也无法由客户端
    释放。`Unlink`/`Rmdir` 的 `last` 分支已补 `DelLock`，多链接中间态不提前清锁，回归见
    `tests/delete_storm.rs::last_posix_delete_cascades_locks_and_returns_ledger_to_baseline`；
    `Rename` 覆盖目标与 `Symlink` upsert 的同类路径本轮未审计。
18. **同值消除让「`seq` 有没有动」不再等价于「有没有写」**（§5.1，`v0.7.0` 新添的对外
    可见代价）。`RowPut` 写进去的值与现存行逐字节相同时不产生日志：不 append、
    不 fsync、`seq` 不走，而应答仍然是 `Reply::Ok` —— 客户端从应答里看不出区别。
    三个后果：① 靠「`Stats.seq` 前进」判断「这张表被动过」的下游会漏掉这类写；
    ② 副本与 PITR 本来就没有那条记录（这正是目的：不把无变化的写传播出去），所以
    以「日志里出现过」为凭的审计不能建在 `RowPut` 上；③ 要「每次调用都留痕」
    的形状，得让那一行真的变（带时间戳或版本号列），或者用 `Op::Batch` 包一条必然
    产生日志的 op。判据是**字节相同**（整行所有列），不是「调用方以为没变」；条件写
    （`PUT_IF_ABSENT`/`PUT_IF_EXISTS`/按列 CAS）在闸门**之前**判，所以「同值 + 条件不满」
    仍然是 `EEXIST`/`ENOENT`，不会因消除而变成功。可观测面没加新字段：`ops_total −
    writes_total` 就是被消除的写数（§4.1 那个判据因此也不用抬），实测见 §7.16 的
    `putsame` 档（records=0、writes=0、syncs=0）。省下的是真的：200k 次心跳式重写从
    448 次 fsync / 11.9 MB WAL 降到 0 次 / 0 字节（§7.16）。
19. **写扩展要自己分片：引擎里没有分片层**（§7.17 实测）。单实例写吞吐有一条与
    客户端压力无关的天花板：never 档 8 路并发压力只拿到 0.94×（114 505 → 107 356 ops/s），
    而服务端 CPU 从 4.30 涨到 8.39 核 —— 多烧的 CPU 全在排队；group 档总量能到 2.33×，
    但那是每个 fsync 窗口里排上了更多 op，**每实例**从 37 252 塌到 10 862（−71%）。按表
    hash 到多个进程实例确实能扩：never 8 实例 744 004 ops/s（6.50×，每实例保留 81%）、
    group 4.77×（每实例 60%）。代价全部在接入方：路由表要自己维护（引擎不认得「表 X
    在哪个实例上」，路由错就是静默的 `ENOENT`）、S 份目录 / WAL / checkpoint / 备份 /
    副本 / `fence` 令牌、一表一实例是部署不变量（`TableRename` 不会把表搬到另一个实例），
    而**跨实例没有事务、没有级联**：`Op::Batch` 只在一个实例内原子，引用账本（`refs`）
    与视图也各自一份 —— 本来就不能拆。引擎侧没有 rebalance、没有目录发现，那些只能建在
    接入层（§10 阶段 C）。

---

## 9. 测试清单

```sh
cargo test --release                            # 默认档（纯引擎），245 条（242 passed + 0 failed + 3 ignored）
cargo test --release --features postgres-compat # 加上过渡档，282 条（273 passed + 0 failed + 9 ignored）
cargo test --release --lib                      # 只看引擎单测
cargo test --release --test migrate              # 快照升级：11 条走 API + 3 条真进程跑 CLI（§6.5）
cargo test --release --test scale_connections -- --ignored   # 万级连接重压（§7.6）；并发数由 TREEDB_CONNS 控制，本机到 25 000 为限（再高是客户端源端口不够）
TREEDB_RESTORE_NODES=10000000 TREEDB_RESTORE_TAIL=100000 cargo test --release --test scale_restore -- --ignored --nocapture   # 百 MB 到 GB 级快照的恢复成本（§8 第 2 条）
cargo test --release --test checkpoint_tail -- --ignored --nocapture   # 64 MiB 阈值下快照收尾那一轮的停顿（§8 第 7 条②）
bash benches/backup_probe.sh 2                  # 备份/还原三形状：冷拷贝、热拷贝、副本目录（§6.7）
bash benches/throughput_matrix.sh 3             # 五轴吞吐矩阵（默认只跑 1–4）
bash benches/default_check.sh                   # 零旗标默认档同格重跑 5 次（§7.5）
bash benches/ab_layout.sh <基线 rev>            # 受控 A/B：只换服务端二进制（§7.10）
bash benches/handshake_cost.sh 20000            # 接入认证的代价：建连风暴 + 稳态两臂（§4.4）
bash benches/observe.sh                         # 接入面两个旋钮：上限/拒绝/限速/指标/停服总账（§7.14）
bash benches/bench_rows.sh                      # 通用表七条路径 × 落盘/单线程三形状（§7.15）
bash benches/bench_bulk.sh                      # 批量形状固定工作量档：逐行/区间/清空/顶替 + 同值消除（§7.16）
bash benches/shard_write.sh                     # 多实例写扩展：solo vs split × 实例数，附服务端核数（§7.17）
bash benches/delete_storm.sh                    # 通用表删除/churn：自动 checkpoint、RSS、SIGKILL 尾巴（§7.18）
bash benches/posix_delete_storm.sh              # POSIX Unlink/Rmdir：固定 2N op、自动 checkpoint 开关、重开核对（§7.18）
bash benches/many_views_mix.sh                  # 万级视图混合读写矩阵：views 四档 × conns 两档，净零账本 + 自动 checkpoint + RSS（§7.19）
AUTO=0 VIEWS=10000 CONNS=8 OUT=benches/results/mix_isolate_auto0.log TSV=benches/results/mix_isolate_auto0.tsv bash benches/many_views_mix.sh   # 单格隔离探针：只切 `--auto-checkpoint-mb`，不覆盖正式矩阵（§7.19.1）
bash benches/promote_smoke.sh                   # 切主真进程链：停旧主 → 提升 → 新主可写 → 旧主被高令牌副本拒收（§6.6）
bash benches/sync_ratio.sh 3                    # 一次 fsync 摊了几次写（§7.9，两张表逐格对账）
bash benches/ab_durability.sh <基线 rev>        # 写轴 A/B：持久化档的吞吐与尾延迟（§7.9 第 3 条）
bash benches/probe_srv.sh                       # 服务端吃了几核（/proc 计数，§7.5）
bash benches/rehearse_migrate.sh v0.3.2         # 快照升级真机彩排：旧 tag 编的二进制 → 迁移 → 回滚（§6.5）
bash benches/grovefs_pin_ab.sh                  # grovefs 迁移分支的 pin 跨代复核：只改 tag 一行 × 两臂各配同代引擎，六条端到端套件 + 混代反证（§10 阶段 C 4 末）
bash benches/sc_ops.sh 3 3                      # 单核四臂：t1/p1/pf/t8，逐格记实测核数（§7.5 末）
bash benches/sc_ops.sh 3 1 readdir              # 只跑同一格的受控 A/B（配 SKIP_PG=1 与 RUN=…）
bash benches/strace_attr.sh ping                # 每 op 几次 syscall（分母取测量窗口 ops）
bash benches/split_attr.sh readdir 1 8          # 那一核的时间有多少在 sys 里（无 strace）
gcc -O2 -pthread -o /tmp/lb_floor benches/lb_floor.c && /tmp/lb_floor 1 64 3   # 1 核物理下界
./target/release/treedb-micro --items 32000     # 纯计算档 ns/op + allocs/op（§7.10）
```

这几条测量脚本都不进回归板（它们要几秒到十几分钟，且数字依赖机器），但它们
输出的每一格都已被本文引用过 —— 引用了就要能复算。`rehearse_migrate.sh` 与
`grovefs_pin_ab.sh` 是其中不量性能、只核正确性的两条，但它们都得拿真二进制跑（
后者还要两代各一份），所以也不进回归板。

§7.9、§7.13、§7.18、§7.19 与 §10 补段的对账脚本是例外：它们是秒级纯文本比对、
不碰机器，可以随时跑 —— `python3 benches/three_way_table.py --check`（`final.tsv`
vs 两份原始采集）与
`python3 benches/doc_table_check.py --selftest`（§7.13 的 `final.tsv` 加 §7.18 的
`delete_storm.tsv` / `posix_delete_storm.tsv`、§7.19 的 `many_views_mix.tsv` /
`mix_isolate_auto{0,1}.tsv`、§7.9 的 `sync_ratio.log`、§10 补段的 `grovefs_pin_ab.log`
—— §7.18 的七张加 §7.19 的五张再加 §7.9 的两张与 §10 补段的两张，共 16 张逐格表，外加
§7.13 那张 24×14，并自带 24 处注入错）。§7.9 那两张与 §10 那两张的键分别是日志里的
**块时间戳**与 **ref 名 / 套件名 × 臂**：日志是追加式的，跑一次就多一块、多一组块，
锚定这些标签才不会因为后来复跑而误报；§10 套件表的列头同理只锚「臂 X：pin vY」这个
形状、不写死 Y，臂上那个 tag 由日志的臂头校准，所以发了新 tag 后重跑不会假红。它们都靠「从原始采集重算」而不是「人眼比对」来定对错。

两档都跑完才是全量：`--features postgres-compat` 多出的 31 条 = 过渡档自己的 30 条
单测 + `concurrency.rs` 里那条不需要 PG 的拨号用例。

**端口是怎么分的。** 起真进程的用例都从 `tests/common/mod.rs::free_addr()` 拿监听地址，
它只从 `ip_local_port_range` **之外**发号（本机是 61 000–65 534），本进程内按原子计数器
往后走，每个号发出前再自己 `bind` 一次探活、跳过机器上别的服务占用的号。要堵的是两个
check-then-use 窗口：旧写法「`bind :0` 拿号 → `drop` → 交给服务端去占」既允许同进程两个
并发用例先后拿到同一个号，也允许任何一次 `connect()` 在这个窗口里把同号当源端口用掉
（监听端口与源端口本来就是同一片地址空间）。撞上的表现是输的那个服务端
`FATAL: Address already in use`、赢的那个还替对方接住了客户端，于是**一次撞号红两条用例**。
2026-11-25 的两轮全量各撞过一次（默认档红 `net_protocol` 两条、`postgres-compat` 档红
`delete_storm` 两条，服务端日志都明写撞号），换成静音段发号后两档重跑全绿。

| 目标 | 数量 | 覆盖 |
|------|------|------|
| `--lib` | **71**（+30 属 `postgres-compat`） | codec round-trip（全 tag：45 op / 24 reply）、**fetch 应答的两条长度/计数上限在 decode 处兼得**（`fetch_reply_bounds_are_enforced_on_decode`，§6.6）、**`Log` 全变体 round-trip**、**握手三条 op / 两条握手 reply 的定长字段少一个字节都拒**（`handshake_fixed_fields_reject_wrong_length`，§4.4）、**自写 SHA-256/HMAC 的已知向量 10 条**（FIPS 180-4 与 RFC 4231 逐条对表 + `ct_eq` 长度严格，`src/engine/sha256.rs`）、**`Key::load` 收哪三种形状拒哪三种**（`key_load_is_strict`：32 原始字节**一个字节都不剔**、hex 档忽略尾随换行、全空白与全零拒 —— 那条「不剔」是拿真密钥踩出来的，随机密钥末位正好是 ASCII 空白的概率是 1/64，§4.4）、**`Gate` 的五档进度、错版本 fail-closed 与双向 MAC**（`gate_gates`、`gate_without_key_is_open`、`gate_rejects_a_protocol_mismatch`、`macs_are_directional_and_nonce_bound`、`key_debug_does_not_leak_bytes`）、CRC 已知向量、**slicing-by-8 与逐字节参考实现的对拍（0..=4096 字节全长度 + 分段喂，§6.6「主侧的账」）**、碎帧/超长帧、WAL 帧与截断重放、**预分配空洞重开**（`prealloc_padding_reopens_cleanly`）、**大批次不跳写**（`huge_batch_writes_every_record`）、**交付只到 durable 水位**（`deliver_stops_at_the_durable_watermark_not_the_logical_end`，§6.6 第一条口径）、**hint 命中与退回全扫**（`deliver_streams_forward_with_the_returned_hint` / `deliver_ignores_a_stale_or_absent_hint_and_still_lands_right`）、**跨滑动窗口不丢记录**、**max_bytes 是软上限**（至少给一条 + 逐轮恰好吃完整批）、**上一代快照只多一段计数且逐字节可对**（`legacy_snapshot_is_readable_only_by_the_migration_entry`，§6.5 的前提）、**四张已删表非空就拒迁**（`legacy_snapshot_with_control_plane_rows_is_refused`）、**已删的控制面 tag 落到 `unknown`**（`removed_control_plane_tags_are_unknown`，§4.1 的「不回收」）、**seq 跳号硬错**、状态快照保留 node key、**两层 dentry 与 kind 冗余过快照不坏**（`dentries_are_two_level_and_kind_survives_snapshot`，§7.10）、**通用表行与索引过快照不坏**（`generic_tables_keep_rows_and_rebuild_indexes`，§5.1）、缓冲压缩、租约过期边界、**快照收尾只切割水位以下的记录**（`truncate_before_drops_the_covered_prefix_and_keeps_the_tail`，§8 第 7 条）、**dump 容器的四条自测**（全域往返逐字段相等、容器头独占两个 `next_*`、编解码字节稳定且 CRC 拓坏必拒、未知容器版本点名拒，§6.9）、`default_workers` 上下界、**自适应块长五条**（`net::replicate::tests`：拿不满/浅滞后→配置值、深滞后逐级翻倍并封顶、配置值即上限、以及「恒在 [floor, WAL_FETCH_MAX]」的不变量）、**跨缓冲的大记录过一遍裁剪与扫描**（`records_larger_than_the_copy_buffer_survive_compaction_and_scan`，§8 第 7 条②） |
| `tests/engine_crud.rs` | **36** | §5.2 全部 POSIX 语义（含 GC 线索、NOREPLACE 与硬链接）+ checkpoint 后重开 + 坏尾截断；v0.5.0 新增 3 条硬链接：多名字共享 inode 与字节（含 `drop` 后重开仍是 `nlink = 2`）、五种拒绝各报对应 errno 且不动票数、覆盖其中一个名字只减一票；再往前新增 6 条：4 条盖通用表（表形状校验与表名回收、行写边界/约束/CAS/flags、主键与索引列游标翻页、表状态过 checkpoint+重开），1 条盖 `ReadDirPlus` 与 `ReadDir`+`Getattr` 逐字段对齐，1 条盖 `Stats` 新字段；2026-11-20 又加 2 条：`a_data_dir_refuses_a_second_engine`（同一个目录开第二个引擎必须被拒，且 `drop` 后立刻可开）、`unlink_and_rmdir_report_enotdir_for_a_non_dir_parent`（§5.2 的 `ENOTDIR` 口径）；2026-11-21 又加 2 条旁路快照：`background_snapshot_costs_a_dispatch_not_a_pause`（派工 37.5 µs vs 停轮 23.2 ms @ 20 000 节点，且取水位之后的尾巴必须活过 `truncate_before`）、`explicit_checkpoint_drains_the_background_one_first`（同步 checkpoint 先收掉在编那份，不许两份抢同一个 `.tmp`）；2026-11-23 又加 3 条过期租约回收（§8 第 17 条）：`expired_leases_are_reclaimed_and_stay_gone_after_restart`（只删过期那把、永久锁与未到期锁不动、`DelLock` 进了 WAL，重开不复活）、`the_lock_sweep_walks_the_table_in_batches_and_terminates`（2 148 条锁、每轮 ≤ 1 024，四轮内扫完且能收尾）、`the_lock_sweep_never_writes_on_a_replica`（副本返回 `(0,0)`、`seq` 不动、行还在） |
| `tests/net_protocol.rs` | **12** | TCP 端到端：CRUD、碎帧、pipeline 保序、并发连接不串扰、超长帧只杀违规者、大回包流式读完、`--stats`/`--checkpoint`/`--shutdown`（含两个磁盘长度与协议版本）、优雅停服后同地址立刻重绑、**通用表 + readdirplus 走 TCP**（1000 行 pipeline、错误帧经网络回真实 errno、8 KiB 收缓冲下大应答完整接回）、**过期租约由服务端周期回收**（`--lock-sweep-ms 50` 下 `Stats.locks` 自己从 2 降到 1，客户端一个 op 都不发，永久锁不动） |
| `tests/dump_restore.rs` | **4** | §6.9 的目录级闭环：真造一份「每张表都有内容」的状态（视图/目录/文件/硬链接/符号链接/锁/引用/两张表与行）→ 同步 checkpoint → 再写两条尾巴 → `dump_dir` → 落文件读回 → 导进新目录 → `Engine::open` 重开逐字段相等、`snap_seq == dump.seq`、`wal_records == 0`；**再从目标目录导一次比字节**（确定性）；第二条钉拒绝路径（非空目标拒且一个字节不动、空目录可导、服务在跑时 dump 被锁拒且释放后可导）；第三条钉零票引用不进 dump、`Kind` 与冗余 `Dent::kind` 过重建不变；第四条起**真进程**钉两个命令的退出码契约（0 完成 / 1 拒绝且不动字节 / 2 参数不全，含改一个字节后的 CRC 拒收与「锁解除后立即可导」） |
| `tests/crash_recovery.rs` | **7** | §6.4；2026-11-21 加一条多 worker（`workers=4`）下 `kill -9` 的已 ack 写验收 —— 同一套断言在两条 ack 路径（默认同步档与 `--flush-pipeline`）上各跑一遍 |
| `tests/backup_online.rs` | **3** | §6.10 的在线面：主静置时备份，停机取源目录 `State` 作期望，恢复后 **逐字段相等**，且「备份前后 `Ping` 都通」+ `backup → load → dump` 逐字节相同；主每 400 条换一代快照且持续写时，切面 `seq ∈ [备份前, 备份后]`、`nodes` 不倒退、恢复水位 = `dump.seq` 且 `wal_records == 0`；第三条起真进程钉退出码（2 参数 / 1 连不上）并验证 `--retries N` 真的转满 N 轮 |
| `tests/pitr.rs` | **11** | §6.11 的归档与 PITR 十二面：归档段与源 WAL 前缀逐字节相同且其后只剩空洞、第二次 checkpoint 多一段 + 一份「将被覆盖」的快照；**后台旁路快照也归档且先拷旧快照再覆盖**（顺序错就把水位 `from` 那份字节永久抹掉）；目标落在两次 checkpoint 之间（归档锚点 + 归档段）与最后一次之后（数据目录快照 + 活 WAL）两条路径重放后状态逐字段相等、恢复目录能继续写；目标正好落在快照水位时零条重放；中途关掉归档后停在洞的左边（`history ends at`）；归档被外力丢一段时报缺口并指出下一个存在的 seq（`no record N` / `next one present is M`）；越过末尾拒绝；目标非空拒且一个字节不动（只允许多一个 `.lock`）；归档目录等于数据目录时开局就拒；不开归档时目录里只有快照 + WAL + `.lock`；**v0.7.0 的三条批量记录（25/26/27）严格落在两次 checkpoint 之间时从归档段重放**（`anchor == s1`、`from_wal == 0`、`from_archive > 0`，即同值消除没把批量记录省掉）且恢复目录行集也对、含三条批量 op 的尾巴恢复与停服冷 dump `encode()` 逐字节相同 |
| `tests/read_scaling.rs` | **8** | 只读分派表逐条对 `translate`（判错一条就是丢写；语料 45 条全变体，钉住 45/15/30 三个数）、**三档划分穷举且两两不交**（`ops_partition_into_three_classes`，15/22/8，§4.2 与副本拒写的判据同源）、`workers=1/8` 答复逐条相同（负载含表读与 `ReadDirPlus`，混合批里把 CAS 写夹在读之间）、每连接流内顺序、一写多读不读到半截状态、`ops_total` 记上 worker 就地算的读、并行档下 `kill -9` 已 ack 的写全在（目录写与表写逐条交错）、**fd 复用不会把上一任的应答塞给新任** |
| `tests/batch.rs` | **6** | §5.1「原子批」的六面：批内顺序可见（`create` 后直接对刚分配的 ino `setattr`，`apply_seq` 只 +1）、一条失败整批不落且父目录三个属性逐字段不变（`msg` 带 `batch op #<i>: `，失败批预留的 ino 被回收）、通用表上同样原子（唯一索引冲突让整批零行）、**整批只占一条 WAL 记录**（把末条记录截到 payload 一半，重开后批内三个子 op 全不在而批前的视图仍在）、纯读批零日志且应答与逐条单发逐字段相等、空/超限/嵌套/控制面四种拒都在 `translate` 层给 `EINVAL` 且在解码层也能拦住越界与嵌套 |
| `tests/table_bulk.rs` | **12** | §5.1「批量形状」与「同值消除」的十二面：`TableClear` 拆行拆索引但留形状与 id（清完仍在 `TableList` 里）、**一次清空只推一个 seq**（`apply_seq` 差恰为 1）+ 砍掉尾字节的撕裂重放必须回到清空前（切点留在帧头里、只砍 payload 尾巴，因为这条只有 5 字节）、`RowRangeDelete` 跟 `RowRange` 同一条列规则（`ROW_PK` / 已索引列 / 未索引列 `EINVAL`）、**区间记录的宽度与命中数无关**（一条 `DelRange` < 40 B，一千条 `DelRow` 是它 500 倍以上，在编码层比字节）、三条新 `Log` 幂等（逐条手动 `apply` 两次与一次等价、整个 WAL 重放一遍仍是同一份 `State`）、`TableRename` 保留 id 且被顶替表级联拆干净（逐张数 `tables`/`rows`/`row_index`）、改名四个边界（空名/超长/`NOREPLACE`/`from == to`）、同值 `RowPut` 零日志零 fsync（三条闸门 `PUT_IF_ABSENT`/`PUT_IF_EXISTS`/CAS 仍在消除之前生效）、被消除的写跨重启不留痕迹、含批量 op 的 `Batch` 仍是一条记录且失败整批不落、四种形状都过得了 `Checkpoint` 与 `dump → restore` |
| `tests/table_alter.rs` | **5** | §5.1「追加列」的五面：旧行全被拉宽到新宽度且新增的 `COL_INDEX` 列索引里每行都有一项（索引计数 `row_index.len()` 把唯一列隐含的索引也算进去）、五类拒绝（不存在/重名/已满 64 列/空名/`COL_UNIQUE`）逐条核 errno 且 `apply_seq` 不动（零日志）、**只占一条 WAL 记录**（从 payload 中间截断重开后仍是旧宽度、旧列一个不少）、`Checkpoint` 与 `dump → restore` 之后新列/旧数据/索引都在且接着能写、同一条 `Log::AddColumn` 手动 `apply` 两次与一次等价 |
| `tests/auth.rs` | **8** | §4.4 在线协议上的版本/认证形状：未协商版本的连接一个数据 op 也拿不到（`workers=1/8` 各跑）、错版本被拒且可改回同代、客户端拒绝伪造错版本应答、错密钥被拒但不锁死 + 旧 MAC 重放不上 + 换对密钥能过、带密钥客户端拒绝降级、未配密钥服务端返回 `authed: false`、握手与数据 op 挤同批时应答保序 |
| `tests/replication.rs` | **21** | §6.6 的十三件事（二十一条例，端到端那一件两档各一条；主是真进程，副本是测试里的 `Engine` + `Follower`，只有最后几件起真 `treedb-standby`）：主写逐条收敛（ino/属性/readdir 与主对齐）、**主被 `kill -9` 后副本仍答读且不退出**（`replica_errors` 抬、`connected` 落 true→false）、**gap 触发分块重装快照**（4 KiB 块下 ≥ 2 块，且副本落盘的 `state.snap` 与主的逐字节相同）、**副本拒写在引擎层**（六个形状不同的写真 op 全 `EROFS` 且零 logs、零水位推进，而 `ListViews`/`Stats`/`Checkpoint` 放行）、**lag 中途为正、追平归零**（2 KiB/轮喂不饱一批）、**副本重启靠自己的日志接上**（`resyncs == 0`，快照+尾巴那条恢复路径）、端到端两档事件循环同一份断言（`workers=1`/`2`：线上 `EROFS`、`Stats` 新字段过线、追平后读到主的新写）、启动参数三道门（缺 `--primary` / 自己拉自己 / `--fetch-bytes 0` 均退出码 2）、**开了认证的主只有握得上手的副本追得动**（对密钥逐条收敛、错密钥不收敛）；v0.4.0 新增四条 fencing：令牌只升不降/落盘/坏文件不当 0、副本侧四档闸门（含“权力优先于新旧”与它的反面）、**真链路上提权后拒旧主的批**（状态不回滚、仍在答读、`replica_errors` 抬）、`--promote` 离线动作（不开端口、两次 0→1→2、与 `--primary` 同时给退出码 2）；2026-11-22 新增 `the_fetch_block_ramps_while_catching_up_and_returns_to_cfg`（64 条/批播 8 192 个文件，`--fetch-bytes` 32 KiB 下块长峰值放大到 1 MiB、收敛后回落到 32 KiB）；同日把同一条规则铺到快照段：`the_snapshot_chunk_ramps_while_downloading_and_returns_to_cfg`（8 KiB 起、4 000 条批量播出 > 8 个块长的快照，下载途中块长涨过配置值且不超 4 MiB，轮数少于定长下的块数，装完落回 8 KiB）；同日再补三条快照换代：`a_snapshot_probe_reports_the_prefix_crc`（六个 offset 上 `prefix_crc == crc32(&raw[..n])`）、`a_snapshot_generation_change_keeps_the_downloaded_prefix`（改尾部节点的 `mode` 再 checkpoint：`snap_reused > 0`、`snap_restarts == 0`、两份快照逐字节相同）、`a_snapshot_generation_change_with_a_different_prefix_restarts`（新建文件：`snap_restarts == 1`，仍然收敛）；2026-11-25（P1）补上 `a_real_standby_promotion_fences_a_resurrected_primary`：真主 + 真副本都起 `treedb-standby` 进程，离线提升后旧主回魂发的 `WalBatch` 被高令牌副本拒收、副本继续答读（把 `promote_smoke.sh` 那条人工链锁进回归，§6.6「切主」） |
| `tests/concurrency.rs` | 0（+7，整文件 feature 门控） | T1 之后整个文件在 `postgres-compat` 后面：无 PG 时跑「拨号失败不漏 live」（1 条，要 feature），其余 6 条 `#[ignore]` 需真实 PG。**它测的是共享池，不是引擎并发**（§7.8）|
| `tests/scale_connections.rs` | 1（+1 ignored） | 1 000 连接 fd/RSS 断言每次跑；万级（`TREEDB_CONNS`，含 25 000）手动 |
| `tests/scale_restore.rs` | 1（+1 ignored） | §8 第 2 条的规模档：10 万节点 + 2 万条尾巴走完「插入 → 同步 checkpoint → 冷/热两次恢复 → 再写尾巴 → 恢复」，钉节点数、`wal_bytes == 0`、随机名字 lookup 命中；百万到千万档（1M/3M/10M）`#[ignore]`，手动跑，打印耗时/内存供 §8 第 2 条引用 |
| `tests/checkpoint_tail.rs` | 1（+1 ignored） | §8 第 7 条②：后台快照收尾那一轮（`Wal::truncate_before`）的正确性与代价。默认档 4 MiB 触发 + 512 KiB 尾巴，钉「尾巴留下、日志变小、不 checkpoint 直接重开能重放出最后一次 setattr」；64 MiB 档 `#[ignore]`，尾巴 0/4/32 MiB 三档打印耗时并守 1500 ms 回归线（缓冲搬运被退回去就会红） |
| `tests/independence.rs` | **3** | 独立性三把门：内核目录（`src/engine`、`src/net`）不出现客户端名字；`[dependencies]` 非 optional 项只能是 `libc` + `thiserror`；`postgres-compat` 不得进 `default`（§11 第 7 条） |
| `tests/large_reply.rs` | **1** | `full_chunk_crosses_a_full_socket`：`workers = 4` 下连发四个满块请求（共 16 MiB 应答）而**发完才开始读**，必让 `write` 短写返回 `WouldBlock`，钉住「`EPOLLOUT` 那条尾巴必须把剩下字节续完」—— 旧形状里多 worker 的空 `ops` 分支直接 `continue`，跨机拉快照只能收到前 ~110 KiB 后超时 |
| `tests/migrate.rs` | **14** | §6.5 那四条承诺加 v3 → v4 的拒迁闸门。11 条直接打 API：v3 样本迁移后 `State` 逐字段相等且 WAL 文件字节不变、尺寸恰好少 24 字节、重开后每个语义面都能读到（视图/名字/锁/引用/通用表）、尾巴 seq 照搬且发号接着走、幂等重跑不动当前目录、未知版本与「正文配不上版本位」都拒动且零改动、既有备份不被覆盖、迁完的目录能直接用通用表。3 条用 `env!("CARGO_BIN_EXE_treedb-migrate")` 起**真进程**钉退出码（10 / 1 / 0）与输出里的命令、备份路径、回滚步骤 |
| `tests/observe.rs` | **5** | §7.14 的接入面契约，全部起**真进程**：`--max-conn` 在 `workers = 1` 与 `8` 两种形状下都只放行上限内、第三条起被拒且在册连接不受牵连、日志只打第 1 条、`drop` 后能重新进；prometheus 输出与 `Op::Stats` 逐项对上（稳定项精确相等、计数器只要求 ≥、`fsync_seconds_total ≈ sync_us_total/1e6`）且默认文本档 5 行格式不变；抓不上时只剩一行 `treedb_up 0` 且退出码 1；四种参数错全 exit 2 不产出字节；`--shutdown` 在两种 worker 形状下都得到 ack 且进程干净退出（§7.14 那条真 bug 的回归锁） |
| `tests/delete_storm.rs` | **7** | §7.18 的删除风暴回归：通用表固定删除/churn 的 checkpoint、WAL 尾巴与重开内容；多连接 churn 的半轮屏障；POSIX 最后一个名字删除时级联清永久锁/过期锁且多链接中间态不提前清；关闭自动 checkpoint 后 WAL/水位不动、显式 checkpoint 才收口；POSIX SIGKILL 后自动快照 + 小尾巴恢复；`posixseed`/`posixdelete` 真进程固定 `2N` op、`6N` records、N 个非空 NodeRef 与账本基线 |
| `tests/mixed_views.rs` | **5** | §7.19 的混合读写回归，五条全部起**真服务端进程**：种子账本逐条对 `records = 2N+3NW` / `writes = N+NW` / `(views, nodes, dentries) = (N, N(W+1), NW)` 且 `ReadDir` 真的分页走完（`seed_ledger_matches_the_documented_record_formula`）；单组五 op 恰好 5 records / 2 writes，跑完账本回到**组开始前**（基线取在插入之前，不做同连接自反比对）；5 000 组净零 + 头尾各 400 个名字的逐名核对 + 重启后**另开连接**复查临时名仍全 `NotFound`；8 条连接打同一批视图仍净零；视图轮转覆盖数恰为 `min(N, G)`（60 / 25 两档） |

本轮（接入面旋钮，2026-11-21）新加的回归锁（全部在 `tests/observe.rs`）：
`max_conn_bounds_the_registry_in_both_worker_shapes`（上限 2 下前两条可用、第 3~5 条
`Ping` 必 `Err` 而在册连接照答，日志只出现 `refused connection #1` 且**不**出现 `#2`，
`drop` 后 5 s 内新连接能进 —— 两种 worker 形状各跑一遍）、
`prometheus_exposition_matches_the_wire_stats`（造数据 + checkpoint 后与 `Op::Stats`
逐项对照，且 stderr 为空、默认文本档格式仍在）、
`prometheus_scrape_says_down_when_nothing_listens`（exit 1 + 只有一条样本 +
stderr 非空）、`bad_flags_exit_two_instead_of_starting_a_server`（四种参数错）、
`cli_shutdown_is_acked_in_both_worker_shapes`（§7.14 那个「多 worker 下 ack 永远发不
出去」的真 bug：单线程与多 worker 各跑一遍）。

本轮（硬链接，`v0.5.0`）新加的三条回归锁（`tests/engine_crud.rs` 的「硬链接」一节）：
`hard_links_share_the_inode_and_keep_bytes_alive`（两个名字一个 ino、`nlink` 记账、
`Link` 不动引用账本、前两个名字离开时应答空键与账本不变、最后一个名字才把键带回、
`drop` 后重开 `nlink` 仍对）、
`link_refusals_name_the_posix_errno`（`EEXIST` 含 `ln f f`、目录 `EPERM`、源/父
`ENOENT`、父是文件 `ENOTDIR`、名字带 `/` 的 `EINVAL`，且拒绝不改 `nlink`）、
`overwriting_one_of_several_links_only_drops_one_vote`（`Rename` 覆盖与 `Symlink`
upsert 各只减一票；另两个名字互相 `Rename` 按 POSIX 必须是 no-op —— 把那条早期
返回拿掉，测试立刻红在「旧名字被删掉」上，这条是写用例时真踩出来的）。

本轮（切主 / fencing，`v0.4.0`）新加的回归锁（全部在 `tests/replication.rs`）：
`the_fence_is_monotonic_persisted_and_never_defaults_to_zero`（promote 与
`raise_fence` 都只升不降；重启后令牌还在；`fence` 文件内容非法则拒启而不是读成
0）、
`a_replica_obeys_the_fence_on_snapshots_and_batches`（四档：更大令牌接受并先落盘、
更大令牌下允许 seq 回退、同令牌下旧快照仍拒、更小令牌的批一个字不许进且不动
令牌）、
`a_promoted_replica_refuses_its_old_primary_over_the_wire`（提权后旧主的新写只在它
自己那儿生效；副本 `replica_errors` 抬升、状态逐字段未变、仍在答读、`tick` 不退出）、
`the_promote_flag_bumps_the_fence_without_serving`（`--promote` 不开监听口、连跑
两次 0→1→2、与 `--primary` 同时给退出码 2 且不改令牌）。
上一轮（问题清单 F1–F8，`v0.3.7`）新加的回归锁：
`snapshot_trailer_carries_crc_and_accepts_the_legacy_shape`（lib：新尾巴带 crc 且能回读、旧 8 字节尾巴仍能开，§6.8）、
`checkpoint_writes_a_crc_trailer_and_reopens_cleanly`（migrate：checkpoint 产物的尾巴以 magic 收尾）、
`corrupted_snapshot_body_is_refused_at_open`（migrate：正文翻一个 bit 就拒启，不再拿前缀状态起服）、
`legacy_eight_byte_trailer_still_opens`（migrate：不升版本、不要求迁移的承诺）、
`a_data_dir_refuses_a_second_engine`（engine_crud：数据目录互斥）、
`unlink_and_rmdir_report_enotdir_for_a_non_dir_parent`（engine_crud：`ENOTDIR` 优先）、
`backpressure_does_not_spin_the_worker`（net_protocol：背压期间 0.6 s 墙钟的 CPU 秒数 < 0.2，§4.3）。
活体验证（真二进制，不进回归板）：坏快照/尾随垃圾 → 退出码 1 且错误点名 crc 或余量；同一目录第二个
实例 → 退出码 1 并带持锁 pid；`--durability alway` / `--workers abc` → 退出码 2；只读目录下 `SIGTERM`
→ `FATAL: io: Permission denied` + 退出码 1（旧行为是静默 0）。

上一轮（读轴/写轴）新加或改写的回归锁，本轮仍然全绿：
`dentries_are_two_level_and_kind_survives_snapshot`（两层表形状 + kind 冷启动后
仍在 + 快照字节序列不变，§7.10）、
`prealloc_padding_reopens_cleanly`（预分配空洞不能被当记录）、
`torn_wal_tail_is_discarded_not_fatal` 与 `replay_skips_torn_tail`（坏尾必须注入到
**逻辑末尾**，不是文件 EOF —— 预分配后两者差着一大片零，注错地方就等于什么都没测）。

上一轮（能力面，`v0.3.3`）新加的回归锁：
`generic_tables_keep_rows_and_rebuild_indexes`（lib：`row_index` 不进快照，必须由
`rows` + def 重推得逐字相等；`row_page`/`index_page` 游标不重不漏；`DelTable` 后
`row_index.is_empty()`）、
`table_shape_is_validated_and_names_can_be_reclaimed`（表名/列数/重名列的校验顺序，
以及删表后 id 不重新发票）、
`row_writes_respect_bounds_constraints_and_conditions`（key 与列数边界、`flags` 互斥、
唯一碰撞、条件写不命中一律 `EAGAIN`、幂等删）、
`row_scans_page_by_key_cursor_without_gaps_or_repeats`（主键游标与索引列游标翻页、
不索引列 `EINVAL`、`limit=0` 走默认页宽）、
`table_state_survives_checkpoint_and_reopen`、
`readdir_plus_answers_the_same_page_as_readdir_plus_getattr`（同一页、同一分页边界、
逐字段对齐 —— 属性必须是算出来的而不是另一套真相）、
`stats_reports_table_counts_and_both_disk_sizes`（两个 WAL 长度与 `snap_bytes` 的
「没 checkpoint 就是 0」口径）、
`generic_table_and_readdir_plus_over_tcp`、
`ops_stats_checkpoint_shutdown_over_tcp`（那一轮补：`wal_disk_bytes == st_size`、
checkpoint 前 `snap_bytes == 0`、`proto_version == PROTOCOL_VERSION`）。

同一轮里被「改写」而不只是「新增」的两条：`readonly_classification_matches_translate`
的语料从 37 条扩到 47 条、计数断言改钉 22/25；`control_plane_survives_sigkill` 与
`acked_writes_survive_crash_with_workers_on` 把通用表纳入「已 ack 的写一条不丢」与
「索引必须被重推」的验收面。`seed` 现在也建表灌行 —— 不然五条只读表 op 的
「不产 logs」只在测「查不存在的表」那种废话（§4.2）。

上一轮（快照迁移，`v0.3.4`）新加的回归锁：
`legacy_snapshot_decodes_from_the_same_prefix`（lib，§6.5 的地基：同一状态按两代布局
各写一遍，比前缀必须逐字节相同 —— 正文漂了就得把 v2 写出侧一起改，不然迁移工具
在说谎）、
`legacy_writer_refuses_to_drop_tables`（lib：表非空时禁止写 v2 快照，而失败必须发生
在**一个字节都没写出去**之前）、
`migration_preserves_every_field_and_leaves_the_wal_alone`（集成：用
`downgrade_to_v2` 造样本 —— checkpoint 后按 v2 重写快照文件，而不是手工拼一个假文件，
这样「旧布局」永远与代码里的写出侧同步）。

上一轮（日志传送/只读副本，`v0.3.5`）新加的回归锁。
lib 侧六条全部压在「传送口径」的四条句子上：
`fetch_reply_bounds_are_enforced_on_decode`（越上限的 `WalBatch` 必须在 **decode**
处拒帧，不能先按对端自报的计数分配再去索引一个更短的 body；反之 `SnapFetch` 的
`len` 越上限**不在解码侧拦**，得解出来回 `EINVAL` —— 一个请求不该把连接敲掉，两边
不对称是有意的）、
`deliver_stops_at_the_durable_watermark_not_the_logical_end`（一条已 `write` 未
`fsync` 的记录绝不能交付：副本照收无误、主随即崩掉，那条写在主上根本不存在，副本就
凭空多了一个主永远答不出的 seq；而 `sync()` 之后同一条立即可给 —— 送不送只由水位
决定，不由谁读过决定）、
`deliver_streams_forward_with_the_returned_hint` 与
`deliver_ignores_a_stale_or_absent_hint_and_still_lands_right`（hint 是提示不是
承诺：命中时按它跳，指错位置时必须退回全扫并落在同一处 —— 否则「按 offset 恢复」
这个机制本身就是丢记录的原因）、`deliver_slides_the_window_across_many_records`（一批
记录多到跨过 256 KiB 滑动窗口时不能丢也不能重）、
`deliver_honors_max_bytes_but_always_gives_at_least_one`（`max_bytes` 是软上限：
至少给一条，否则一条大记录就能让副本永久卡住）。

集成侧九条见上表，其中三处的断言形状是写用例时才浮出来的：
`the_replica_keeps_answering_reads_after_the_primary_dies` 用 `kill -9` 而不是优雅停服
（副本在主消失后必须继续答读、必须不退，这是本轮唯一的产品级承诺）、
`a_gap_reinstalls_the_snapshot_in_chunks` 把每轮字节数压到 4 KiB 让「分块」真的发生
（不压就是一次下完，`chunks >= 2` 根本过不去 —— 分块那整条路径等于没被走过）、
`a_replica_restart_resumes_from_its_own_log` 必须在 checkpoint **之后**再写尾巴 ——
没 checkpoint 过的副本目录里根本没有快照文件，那条 `resyncs == 0` 会恒真。

同一轮里被改写而不只是新增的报警器：`tests/read_scaling.rs` 的
`readonly_classification_matches_translate` 语料 47 → 49、计数断言改钉 49/22/27，
并新增 `ops_partition_into_three_classes`。理由是副本拒写用的是**同一条三档判据**
（`!is_readonly() && !is_control()`）—— 判据一改，副本的读写面跟着改，所以它必须和
分派表钉在同一个穷举下（§4.2）。`op_codec_round_trip_all_tags` /
`reply_codec_round_trip` 也从「逐 tag 列名」改成逐条枚举全部变体（49 / 28 那两个
数字只出现在注释里，带数字的断言在 `tests/read_scaling.rs`，本轮在 §9 末修正了
这一点），新增两条 op 与两个 reply tag 自动进语料。

本轮（接入认证，`v0.3.6`）新加的回归锁 —— **22 条，且上一轮的 112 条一条没删**。
这句不是估计：`git worktree add … v0.3.5` 单独编一份旧二进制实跑 `cargo test --release`
得 112 passed / 1 ignored（lib 38），本轮是 134 / 1（lib 54），两边测试名 `comm` 逐条
对出来是 **22 新增、0 删除**。密码学那部分没有可「信任的库」可用（§11 第 7 条那道
依赖门），所以回归面的重心全部压在「已知向量 + 语义形状」上。

lib 侧 16 条分三处。`src/engine/sha256.rs` 十条是自写原语的唯一防线：FIPS 180-4 的
`""`/`"abc"`/两分组三条标准摘要，加上 `sha256_padding_boundary`（55 / 56 / 64 三个长度
必须落进**不同的填充分支**且摘要互不相同，再拿 7 字节一次地调 `update` 与一次
`sha256` 交叉验证 —— 分块喂数据的路径不能只有自己骗自己）；HMAC 那五条是 RFC 4231
的 case 1/2/3/6/7 加上 `hmac_key_between_digest_and_block`（50 字节密钥：大于一份
摘要、小于一个分组，上面那些标准样本全部跳不出这个区间，而阈值误取成
`> DIGEST_LEN` 恰好只在这里露馅，期望值是本轮拿 `python3 -c "import hmac,hashlib"`
逐字对出来的）。`ct_eq_matches_but_is_length_strict` 钉住常数时间比对**不隐藏长度
差** —— 这是 `Key::load` 在长度上零容忍的同一口径。

`src/net/auth.rs` 五条压握手语义本身：`macs_are_directional_and_nonce_bound`（换方向、
换任一 nonce、换密钥都必须换结果 —— 三个都是握手能成立的必要条件）、`gate_gates`
（当前五档逐条走：未协商版本拦数据但不断连、没有待决挑战就交 MAC 同样拒、错 MAC 回到
`Needed` 而不是锁死、**上一拍的 MAC 因 challenge 已被新的替换而重放不上**、放行后
`on_op` 返回 `None`）、`gate_without_key_is_open`（没密钥时先协商版本再直通，显式认证
仍是一次空转）、`key_load_is_strict`、`key_debug_does_not_leak_bytes`。v0.8.0 另加
`gate_rejects_a_protocol_mismatch`：错版本回 `EADV` 且闸门仍停在 `VersionNeeded`。
`src/engine/model.rs` 那条 `handshake_fixed_fields_reject_wrong_length` 在 codec 层：
nonce 与 MAC 都是定长，decode 必须按长度硬拒 —— 少了它，一个短帧会解出一条「字段
错位」的握手 op，而错位后的 MAC 永远对不上任何东西，看起来就像「密钥错了」。

集成侧六条 = `tests/auth.rs` 五条 + `tests/replication.rs` 的
`a_replica_hands_shakes_with_an_authenticated_primary`（对密钥逐条收敛、错密钥不收敛 ——
副本一头最容易被测到的假象是「连上了」，断言得写在数据收敛上而不是
连接成功上）。`tests/auth.rs`
里两条的形状是写到才发现非这样不可：`data_ops_need_a_handshake_on_both_worker_shapes`
必须 `workers=1/8` 各跑一遍（单线程循环与多 worker 循环的拦截点是**两处代码**，
只测一档等于另一档裸奔）；`a_straddled_batch_keeps_the_reply_order` 是 §4.4 那条次序
规矩的唯一防线，它只能挂在 `out` 队列上数应答顺序（协议里没有 request id，
可观测的只有 FIFO）。

v0.8.0 的版本闸门又补三条集成用例：`an_open_server_requires_version_before_data_on_both_worker_shapes`、
`a_protocol_mismatch_is_rejected_on_both_worker_shapes`、
`a_client_refuses_a_mismatched_version_reply`；`tests/auth.rs` 因此为 8 条。codec 侧把
`VersionBegin` / `VersionOk` 加入全变体 round-trip，并在 `handshake_fixed_fields_reject_wrong_length`
里补两条短帧拒绝；跨代真二进制的五臂判据见 §4.4 与 `benches/results/xgen_probe.log`。

同一轮里被**改写**而不只是新增的四个报警器：`op_codec_round_trip_all_tags` 与
`reply_codec_round_trip` 的语料各加两条（两条握手 op、`Challenge` 与 `AuthOk`）。**得修正
上一段的一句话**：那两条测试**没有**计数断言 —— 本轮核代码时才发现「改成按 49 / 28
计数」这句写错了，它们逐条枚举变体并断言 round-trip 与 `remaining() == 0`，数字只在
注释里。真正钉住数字的是 `tests/read_scaling.rs` 那两条：`readonly_classification_matches_translate`
的语料 49 → 51、计数 49/22/27 → **51/22/29**，`ops_partition_into_three_classes` 的三档
**(22, 22, 5) → (22, 22, 7)** —— 后一条改动就是 §4.2 里那段「握手为何归控制面」的
理由被钉住的地方：它一旦被人改回只读档，worker 就会以为可以在自己手里串行化一条
每连接的可变状态。换句话说：**枚举式的语料本身不是闸门**，闸门是那两个带数字的断言。
`tests/common/mod.rs` 不算回归锁，但它是本轮一个真实的形状变化：`Server` 现在存着
自己的 `Option<Key>`，因为 `wait_ready` 与 `client()` 都要按它决定要不要握手 ——
不带这个字段时，开了认证的实例在**探活那一步**就永远不 ready，而报出来的错是
「服务端没起来」，这是本轮调试里最误事的一个假诊断。

`key_load_is_strict` 里那个**尾部空白**循环（`\n` / 空格 / `\t` / `\r` 四种末位字节
都必须原样收下）不是设计出来的，是被真密钥踩出来的：`head -c 32 /dev/urandom`
写出的密钥每 64 把就有 1 把末位正好是 ASCII 空白，而 `Key::load` 原先先剔尾随
空白再比长度，于是它报 `got 31` 并以退出码 2 拒绝启动 —— 一次 20 000 条建连的
跑批跑到一半才爆。先当脚本坏了查，才对上这是产品 bug。修法是「32 原始字节那一档
一个字节都不剔」，并把全空白单独拒掉（它长度合法，新规则保不住它，而它强度为零）。
选在 20 000 条建连那一轮里才重现，是因为它取决于随机数据：这个依赖形状（只在
读密钥那一刻拒启、概率 1/64、报错文案看起来像用户手抖）是本轮最值得记下来的
一个坑。

### 9.1 五条被作废的测量（记录过程，防止有人复用假数据）

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
   本轮又挖出同一个坑的第二个版本：`benches/strace_attr.sh` 原先拿服务端**生命期**
   累计 ops 当分母，而预热那 25.6 万条是 `pipeline` 批量发的，于是 `lookup`/`readdir`
   被量出「每 op 0.2 次 syscall」这种物理上不可能的读数（`ping` 没有文件预热，两种
   分母同值，所以这个坑只在 lookup/readdir 上暴露）。现在分母改取测量窗口 ops
   （bench 自报的 rate × over），读数才与 `ping` 那条线对上。
4. **「`workers=1` 那一列输给 PG 0.26–0.48×」这个比值**（本轮作废）：treedb 那边
   `taskset` 钉了一个核，PG 那边 64 个 backend 不限亲和性，而比值从来没同时记下两边
   各用了几个核。`benches/sc_ops.sh` 现在四臂同跑、逐格记录服务端实测核数
   （`benches/cores.py` 10 Hz 采样 `/proc/<pid>/task/*/stat`），一记就露出来：PG 那一臂
   实际摊在 **46–52 个核**上。三个口径各自的正确数字见 §7.5 末。

   同一个理由也适用于「本轮三项优化到底抬了多少」：那不能拿历史格子减，只能同格
   A/B。本轮的改法是先把 tag `v0.3.2` 用 `git worktree` 重新编译一份旧服务端，只换
   服务端二进制、客户端与口径全不动（`benches/results/strace_attr.log` 与
   `split_attr.log` 的 before 段就是这么出的，旧版 `benches/ab_layout.sh` 已经用过同
   一手法）；跑完立即还原二进制并在日志里记下 sha256，避免下一轮把旧二进制当成新的测。

   同一条里还藏着一个**反向陷阱**：p1 臂（PG 钉同一个核）只用掉 0.07 个核，拿它算
   「每核效率」会把 PG 在单核上抢不到时间片（p50 3.6–48 ms 全在等调度）洗成优势。
   所以 `benches/sc_sum.py` 里 `t1/p1` 报的是**绝对吞吐**、`t1/pf` 才除核数，两者不能
   互换 —— 上一版把 `t1/p1` 那列标成了「每核」，就是这个混淆。

5. **一红一绿的两个读数：`background_snapshot_costs_a_dispatch_not_a_pause` 的那次
   FAIL（2026-11-23）**。本轮跑三档验证时，默认档里这条报红：`paused=12.6 ms /
   dispatch=31.1 ms`；紧接着 `--features postgres-compat` 那一档在**同一份树**上跑过
   （exit 0）。差的不是代码 —— 那条断言拿 `dispatch * 5 < paused` 拦「派工退化成停轮」，
   而两个读数都是**单次采样**：机器当时 load 12（上一轮 bench 收尾），`thread::spawn`
   那一句自己被调度延迟拖到 31 ms。这种 FAIL 不是回归，是**断言的量测口径不够硬**：
   它把「这台机此刻很忙」当成了一句「派工变成停轮」。修法与 §7.15 那条「单次采样不配
   当基线」同一条规矩：取三次派工的最小值，每派一份就 `checkpoint_wait` 收掉，而两次
   之间必须先写一条 op（水位没动时 `checkpoint_start` 直接回 `false`，不给读数）。
   修后在 48 路 `yes` 负载下三次实测 `dispatch=22.6 / 32.6 / 40.2 µs`（对 `paused≈12.6 ms`
   是 300×+），与 §8 第 7 条那句「停轮 23.2 ms → 派工 37.5 µs」对上。

---

## 10. 接入 GroveFS 的路径

### 阶段 A（本 crate，已完成）
引擎 + 协议 + 事件循环 + 五轴验收成文。**grovefs 目录零改动。**

### 阶段 B（接入服务端）
1. `grovefs/Cargo.toml` 加可选依赖，挂一个新 feature `treedb`（默认关，与现有 `pg`
   后端并列）。落地时先写成 `path = "../treedb"`，实测扛不住：**可选的 path 依赖
   即使 feature 关掉也要解析**，旁边没有同级克隆的 checkout 任何 feature 组合都
   编不过。最终形态是按 tag 的 git 依赖
   `treedb = { git = "https://github.com/TechWander/treedb.git", tag = "v0.3.2" }`
   （tag 跟着定版前移：`v0.3.0` → `v0.3.1` → `v0.3.2`，那三步磁盘与线协议格式都兼容，
   升级就是改行；`v0.3.3` 不再兼容，见下面「bump tag 前必读的两处破坏面」）
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
   （长连接 + pipeline 复用，这才是协议设计的目标用法）。池的**粒度**当时没写清楚，
   落地时按视图拆了，2026-11-20 又提到**每进程**一份并量完了万级视图的形状
   （§7.12）：100 视图 400 条 → 4 条，10 000 视图仍是 4 条。
4. 控制面（密钥/审计）：**已经没有「二选一」了**。当日那个选项本身就带着一个
   结论：`Key*`/`Audit*` 的形状是某个客户端的表，直接接上等于把业务语义坐进内核
   —— 终态只能是 §11 第 7 条 T2 的通用表。grovefs 从 M1 起就是拿 `TableCreate` +
   `Row*` 自己拼 `api_keys`/`mcp_calls` 的列与条件（§11 第 7 条末）；M4（`v0.6.0`）
   再把那四组专用 op（连同 `Def*`/`Place*`）从内核整段删掉，接口面只剩通用表。
   「先保留本地 SQLite」那句是 M1 之前的现状，已作废。

**bump tag 前必读的两处破坏面**

① **`Dirents` 的 Rust 接口变更**（§7.5 末那轮 `Names` 扁平缓冲带过来的）。
`Reply::Dirents.names` 的类型从 `Vec<Vec<u8>>` 变成了 [`Names`](src/engine/model.rs)。
**线协议一字未改**，所以跨版本连通没问题；断的是**编译期**：
`grovefs/src/treedb_store.rs` 的 `page()` 里那句 `inos.iter().zip(names)` 要改成
`zip(&names)`（`&Names` 实现了 `IntoIterator<Item = &[u8]>`，`from_utf8_lossy(&name)`
那一行不用动）。选这个方案而不是给 `Names` 加一个
`to_vec()` 兼容层：后者能让 grovefs 一行不改地编过，但它会把刚刚消掉的那 100 次
`malloc` 原封不动放回客户端侧 —— 用静默的性能回退换编译兼容，不如把断点摆在明处。

② **`v0.3.3` 那一轮同时动了磁盘格式与线协议 tag**，这一处不是编译期能兼容的：
`SNAP_VERSION` 2 → 3，旧 `state.snap` 会被版本号闸门**拒收而不是静默解错**（与
`data_ref` 改字节键那次同一个选择）。后果曾经是「任何已经在用 treedb 跑过元数据
的数据目录，升级后**起不来**」—— 这一句在 `v0.3.4` 已改写：闸门仍严，但有了
出路 `treedb-migrate`（§6.5，只重写 `state.snap`，不动 WAL）。grovefs 侧的
`view export`/`import` 在 treedb 档仍是拒绝的（缺口表），所以升级数据目录只这一条路：

```sh
treedb-server --listen 127.0.0.1:7777 --shutdown             # 先停：本工具不做互斥
treedb-migrate --dir /var/lib/treedb                         # 只看：exit 10 = 需要迁移
treedb-migrate --dir /var/lib/treedb --apply                 # 再动：先落 state.snap.v2.bak
treedb-server --dir /var/lib/treedb --listen 127.0.0.1:7777  # 新二进制现在能开门
# 回滚：停服务端，把 state.snap.v2.bak 拷回 state.snap，再跑旧二进制（WAL 没被动过）
#      但这一手只在新二进制做过第一次 checkpoint 之前无损：checkpoint 会 reset WAL（§6.5）
```

`PROTOCOL_VERSION` 2 → 3 叠加了升级顺序硬约束（§4.1）：**先升服务端、后升
客户端**，反过来旧客户端会静默丢掉新应答的尾部字段。grovefs 现在钉 `v0.3.2`，两端
一起动才涉及这两条，按既有做法拿临时
`[patch."https://github.com/TechWander/treedb.git"]` 跑一次
`cargo check --features treedb` 再拆掉。

### 阶段 B spike 的实际结果（2026-11-19，`exp/treedb-integration`）

上面四步全部落地，端到端跑通。`.build-bin/test_treedb.sh`（53 条断言，默认档
自动 SKIP；其中 7 条是 2026-11-20 加的认证一节、15 条是同日加的崩溃一节）里
`treedb-server` +
`grovefs meta-server --meta-store treedb://…` +
`data-server` + `mount` 四件套做真读写，逐条对上本文的承诺：

- create / write / read / readdir / mkdir / symlink / rename / unlink 全过；
  `size` 与字节都来自数据面，treedb 只装名字与属性；
- **GC 线索在真实数据面上被验证**：unlink 掉最后一个持有者、以及 rename 覆盖既有
  名字，两次都让 `blocks/` 里的对象计数下降 —— `Reply::NodeRef(freed)` 那条通道
  不是纸面设计，它确实把删除决定送到了持有字节的进程；
- 两台服务（meta-server 与引擎）重启后，树与字节都还在，remount 直接读回；
- **认证入口已接**（2026-11-20）：grovefs 侧拿环境变量
  `GROVEFS_TREEDB_KEY_FILE` 给密钥**文件路径**（与仓里既有的
  `GROVEFS_JOIN_KEY_FILE`/`GROVEFS_ROSTER_KEY_FILE` 同规：密钥字节永不进 argv），
  四条 `Client::connect` 全改 `connect_auth`。没设 = 不握手（协议 v5 的默认档），
  设了但读不到 = 每次 open 都失败，不静默降级成不认证。§11 第 4 条 ② 那句
  「已补一半」的客户端这一半补上了。fence 在**副本侧**随后也补上了（v0.4.0，§6.6
  「切主」），但交接面的客户端不参与：grovefs 只读「谁是当前主」这一个问题，它拿到
  的答案是部署给的地址。
- **崩溃演练已跑**（2026-11-20）：挂载点活着时 `kill -9` 引擎、同 dir 同 port 重开，
  已 ack 的元数据全在、恢复不需要重启任何进程 —— 逐条实测与那次一并照出来的**错误
  errno**（读侧回答 `ENOENT`，当日已在 grovefs 仓闭合）见下面阶段 C 3 与 §11 第 11 条。
- `view create` / `view rm --base` 走控制面 RPC，在**引擎里**建/删树，
  `<meta-dir>/meta/team.db` 始终不存在；
- 未知 view 被 M_OPEN 拒绝，而不是悄悄长出一棵空树 —— 这条门的存在检查也改问引擎
  本身（`view_exists`），否则 treedb 档会把「树在引擎里」误判成「没有树」。

默认档不变：`treedb` feature 关掉时 `StoreKind` 根本没有第三个 variant，
`MetaStore` 的 29 个 Treedb 臂与 `src/treedb_store.rs` 整个文件都不参与编译；
`cargo check --all-targets`、两条窄 feature 门（`remote-only`、
`remote-only,fuse-bridge`）与整块回归板都在默认档复跑过 ——
`SKIP_BENCH=1 .build-bin/run_all.sh`：39 个套件、1418 条断言、0 失败，
`cargo test --release` 214 passed。同一份 `test_treedb.sh` 在默认档二进制上跑
只输出 `SKIP=1`（自跳门是**行为探针**：启一次 `--meta-store treedb://` 看它是否
拒绝，不能用 `--help` 判，帮助文本在所有 build 里都提 treedb），所以它在板上
永远记 PASS，不会把一个没有 treedb 的 checkout 弄红。

代价是反过来的：板子上的 build 步骤是默认档，所以**不加开关时从板子上跑永远 SKIP
这一节** —— treedb 路径的回归要么手工 `cargo build --release --features treedb`
之后单独跑 `.build-bin/test_treedb.sh`，要么走板子但带上 `FEATURES_TREEDB=1
bash .build-bin/run_all.sh treedb treedbmatrix`（2026-11-20 加的这一档让 build 步骤
补 `--features treedb`，实测三段 `build PASS / treedb PASS / treedbmatrix PASS`，
RESULT: GREEN）。spike 阶段接受那个不对称（默认档是必须守住的，实验档是自己守的）；
现在依赖钉到了 tag，两条路都通。

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
| ~~硬链接 `link_node`~~ **已闭合**（2026-11-21） | provider 改发 `Op::Link`（pin `v0.5.0`，§5.2） | 两个仓各欠一半，现在两半都合上了：引擎 `v0.5.0` 加了 `Op::Link`，grovefs 升 pin 后 `link_node` 真发这条 op。阶段 C 1 的 10 条 `[posix C]` 从 SKIP 转正（`39/0/10 → 51/0/0`），`nlink` 报表与「最后一个名字离开才释放字节」两条都按 SQLite 档同一契约实测 |
| POSIX 字节区间锁 | EPERM / None | 引擎只有整 inode 锁（§5.3） |
| `refs` 的字节量 | 恒 0 | 字节权威在数据面，size 也从数据面取 |
| ~~`view export` / `view import`~~ **已闭合**（2026-11-21） | 可用，但**只能经治理口**（`view --control-server …`）；实现在适配层 `treedb_store::export_view` / `import_view` | **引擎侧一行未加**：dump 是 grovefs 自己的 `grovefs-view-metadata` v1 JSONL（每行一个 node：id/parent_id/name/is_dir/data_ref/link_target/mode/uid/gid/三个时间/ino），适配层用已有的树面 op 把树读出来、建回去 —— `ReadDirPlus` 分页走树，`Mkdir`/`Create`/`Symlink`/`Link` 建节点，每节点一条 `Setattr`（valid 位一次说清）写属性。让引擎认识那个 JSONL 是反向依赖，所以没做；`PROTOCOL_VERSION` 仍 6。与 SQLite/PG 两档**不同的两条保证**写进了实现注释与 grovefs `USAGE` §3.4.2：① 导出不是点时刻快照（引擎没给客户端开快照的口，逐目录边走边读，与离线 `du` 同档）；② 导入不是原子的（引擎只有建/删视图两个原子动作：先整份校验 dump、再 `DropView`、再建、再走一遍核对条目数；`--replace` 的失败窗口是旧树已删、新树未建完，报错里明说）。`ctime` 是唯一不还原的字段（没有 `SA_CTIME`，导入时刻由引擎盖章）。顺带照出一条**既有缺陷**并修了：`Setattr` 与 `Link` 应答的是 `Reply::Node`，适配层却按 `Reply::Ok` 校验且 `let _ =` 吞错 —— 这个后端上每次 `chmod`/`chown`/`utimes` 都静默失败（挂载侧看不见，因为内核拿 setattr 应答里的属性更新缓存，重挂后权限就回去了）。e2e：grovefs `.build-bin/test_xfer_treedb.sh`，板上一行 `xfertreedb`，`pass=28 fail=0` —— 手写 dump（硬链接/符号链接/嵌套目录/属主与时间）先经 SQLite 档渲染成参考导出，再经控制面导入引擎并导出，按 **path** 比 8 个字段与硬链接分组；另含四条拒绝路径、`--replace` 换小树、`--as-fork` 记定义并按 `kind=fork` 导出、以及 ctime 那条差异 |
| `refs rebuild` | 拒绝 | 持有者计数在引擎内同 op 维护，没有独立账本可重算 |
| `gc` 审计 | 拒绝 | 没有聚合的 ledger 视图，回空集等于判所有对象为孤儿 |
| ~~readdir 的属性~~ **已闭合**（2026-11-20） | provider 改用 `ReadDirPlus` | 原写法准确：协议面 2026-11-19 就闭合了，缺的是调用。两臂实测（同一份 `du` 走完一棵树，配方 grovefs `.build-bin/bench_treedb_readdir.sh`，引擎侧计数器读 `--stats`）：引擎应用的 op 数 3000 条时 **3006 → 6**、20000 条时 **20023 → 23**；把引擎放到每段 2 ms 延迟的 TCP 代理后面墙钟 **256 ms → 52 ms**，同机 loopback 上 106 vs 88 ms（三次分布重叠，量不出差别）。**这项收益的单位是往返**，本机测试恰好把它藏起来，所以两个数都得写。语义零回归：阶段 C 1 那套矩阵改前改后都是 `pass=39 fail=0 skip=10`（硬链接闭合后同一套是 `51/0/0`，见阶段 C 1） |
| 定义/密钥/账户 | 行表全部在引擎（M3，无开关），定义仍在本地 SQLite | 密钥、审计、账户、授权**已在通用表上**（M1 2026-11-20，§11 第 13/14 条），M3 删掉了开关：treedb 档读写都走引擎，没跑过搬家命令的部署**拒绝启动**并点名 `control-migrate`（§11 第 15/16 条）。视图**定义**仍在本地（引擎没有定义表）；`Key*`/`Audit*` 那套专用 op 已在 `v0.6.0`（M4）整段删除，接入面从 M1 起就只走通用表（上文第 4 步）。迁法与切写步骤见下面阶段 C 4 |
| ~~`view list` 的物理树~~ **已闭合**（2026-11-20，M1b） | `TreedbControlStore::list_physical` 回答「本地 ∪ 引擎的 `Op::ListViews`」 | 控制面枚举器只看本地 db 文件时，引擎里的基树在 `view list`/`store.info` 里都不存在（委派也因此无法解析）。修了 `list_physical`/`has_physical` 两处双答案，枚举与存在性判据同时看见引擎 |
| HA / 快照传送 | 手工可做（不是自动化） | 接入面用不上它：§8 限制 6 现在是「有异步只读副本、无同步复制」。v0.4.0 起有了 `treedb-standby --promote` + fencing token（§6.6「切主」），但它是**人手动的一步离线动作**：不开服务、不自动选主、旧主靠人停。grovefs 要的「主挂了自动切副本」仍然没有 |
| ~~path 依赖的分发方式~~ | 已定：按 tag 的 git 依赖 | grovefs 侧已跟着钉到 `v0.7.0`（`v0.5.1` 是 2026-11-21、`v0.7.0` 是 2026-11-23，后者见 §11 第 18 条末；`v0.3.7` 曾是无同级克隆也能构建的实测点）；两仓同版。`v0.4.0` 与 `v0.5.0` 都不会逼它升级：前者的线协议变更只在 `WalBatch`/`SnapChunk` 两个副本专用 reply 的尾部，后者是纯追加的新 op tag（旧服务端回 `unknown op tag`），grovefs 走的普通 CRUD 与 `Stats` 字节一个未动。升 pin 的动机到 `v0.5.0` 只有一条：把 `link_node` 接上真硬链接（上一行）；`v0.5.1` 再加一条行为动机：自动档 checkpoint 不再停轮（§8 第 7 条），对「事件循环是唯一 ack 路径」的接入方比性能数字更值钱。三档都实测过（阶段 C 1、§11 第 18 条） |

### 阶段 C（回归与灰度）
1. ~~`posix`/`e2e` 那种全量断言矩阵还没在 treedb 上跑过~~ **已跑**（2026-11-20，
   grovefs 侧新配方 `.build-bin/test_treedb_matrix.sh`，板上一行 `treedbmatrix`）。
   对账要精确到条：两套在默认档的基线实测是 **posix 16 + e2e 21 = 37 条**，这里按
   标签重放 **36 条**（唯一没搬的是 `e2e §7` 那格要 PTY 的「tty 默认日志」），另加
   6 条钉「拒绝得看得见」与引擎计数器、7 条夹具与旁证，合计 **49 条**。在 grovefs
   `ce44fbe` 的干净 worktree（`--features treedb` release）上独立跑了四次，逐条
   对账一致：头两次是两计数器版本（`pass=39 fail=10`，那 10 条全落在 `[posix C]`），
   后两次是现在的三计数器版本（`pass=39 fail=0 skip=10`）：一次直接跑脚本、
   一次走板子 `FEATURES_TREEDB=1 bash .build-bin/run_all.sh treedb treedbmatrix`，
   得 `build PASS / treedb PASS / treedbmatrix PASS`。
   第五次是硬链接闭合之后（2026-11-21，同一台机器、同一套夹具，grovefs 侧把 pin
   升到 `v0.5.0` 且 `link_node` 真发 `Op::Link`）：`pass=51 fail=0 skip=0`，直接跑脚本
   连跑两次一致，板子同样 `build PASS / treedb PASS / treedbmatrix PASS`。新增的三格
   是拒绝面：重名回 `EEXIST`、源缺失回 `ENOENT`、两次被拒都不新增 dentry；原来那
   10 条 `[posix C]` 全部转正（其中「同一个 inode 两个名字」与「删最后一个名字才
   释放字节」两条才是这条 op 的正题）。
   第六次紧接在后（同日，`v0.5.1` 的 pin，只换 tag 与 lock）：`pass=51 fail=0
   skip=0` 与 `test_treedb.sh` 的 `53/0` 都不动，grovefs 的 `cargo test --release
   --features treedb` 是 `318 passed / 0 failed / 4 ignored` —— v0.5.1 只改了自动档的行为
   （旁路快照），协议与快照格式未动，预期就是不动的三格。

   **为什么改成三个计数器**：硬链接那一节 `[posix C]` 的 10 条不是「这一跑坏了」
   （FAIL），而是「这个后端问不了」（没有第二个名字，断言的主语不存在）；把两者
   混在一个数里，缺口要么把板子永久考红，要么被一句「已知不通过」洗成沉默。
   反过来，`SKIP` 也不是垃圾桶：夹具起不来走 `abort`（直接 FAIL 退出），只有「主语
   在本后端不可能存在」这一类才能计 skip。草稿阶段付过两次学费：一次是
   `stat -c %i` 在一个名字存在时也答「1」，把「一个 inode 两个名字」抬成了绿；
   一次是数据面异步释放对象，两条 refcount GC 断言靠同一次延迟双双变绿。
   现在靠 `settle`（1.5 s）与依赖守卫钉住。
   前四次里，除硬链接之外每一格都过：视图隔离、append/rename/rm、4 MiB md5 逐字节、
   孤儿对象回收、symlink 四形态、`df`、`touch -d` 的 mtime、`chmod` 两档、`mkfifo`
   干净拒绝、detached mount 的整套守护进程契约；第五次连硬链接那 14 格也全过。
   前四次里 `[posix C*]` 那两条钉的是**缺口的形状**：`ln` 必须回 EPERM 而不是造出
   半个名字，且挂载点必须活着（缺口本身可以接受，静默的半链接不行）。第五次起
   这两条退场，接替它们的是三格拒绝面：重名 `EEXIST`、源缺失 `ENOENT`、两次被拒
   都不新增 dentry；目录 `EPERM` 在 coreutils 层到不了（`ln` 先拒），留在引擎侧
   `link_refusals_name_the_posix_errno` 里守。
   板上这一行**按实测三元组门控而不是按零门控**：`pass=51 fail=0 skip=0` 记 PASS
   （旧 pin 上 `39/0/10` 也记 PASS —— 那正是文档化的缺口），其他三元组一律记红 ——
   多一条 FAIL（回归）、少一条断言（脚本退化）、skip 变了（要么缺口合上、要么
   脚本退化，两种都得重读文档再改契约）都会让这一行变红。一个永远红的
   格子等于没有格子；默认构建（没开 feature）落 INFO，不拉低 GREEN。
   仍然没定价的：字节区间锁（`locks` 那一套没重放）、以及 `edge`/`errors`/
   `concurrent` 等其余套件。`Link` op 曾经也列在这里：`v0.5.0` 给了引擎半边（31 条
   engine_crud 里的三条硬链接用例），2026-11-21 接入半边也接上并实测（上一段第五次
   重放），`skip` 归零 —— 这一项已从清单上划掉。
2. 万级视图 mount + create + readdir 混打，对照 `pg_stat_activity`（旧）与
   `--stats`（新）两条路径的连接数、RSS、p99。
3. ~~容灾演练：`kill -9` → 重启 → 已 ack 写全在（§6.4 已在 crate 内自动化，
   这步要在 grovefs 的端到端形状上再验一次）~~ **已跑**（2026-11-20：grovefs 侧
   `.build-bin/test_treedb.sh` 第 10 节 15 条断言 `pass=53 fail=0`；故障签名另有两份
   逐 syscall 探针 `.build-bin/probe_crash.sh`、`.build-bin/probe_crash_ls.sh`）。
   演练形状：挂载点活着时 `kill -9` **引擎**，再拿**同一个 dir、同一个 port** 把它
   拉起来，全程不动 mount、不动 meta-server。成立的每一条都是实测：

   - mount 不被拆：故障期 `findmnt` 仍认这个挂载点 —— 后端没了不会把挂载点从
     调用方脚下抽走；
   - **写侧全部拒绝，且拒得不留痕迹**：故障期 `create`/`mkdir`/`rm` 都失败，恢复后
     listing 仍是 seeded 的那几条 —— 被拒的 `after.txt`/`after_dir` 不存在，被拒的
     `rm f3.txt` 也没少一个名字。§5.2 那条「判定必须与删 dentry 在同一个 op 里」
     在这里是外部可观察的：后端没收到的删除，名字就不会消失；
   - **恢复不需要重启任何进程**：引擎回来之后同一个 mount 上第一次 `ls` 就成功
     （12 个条目的目录列出 12/12，第二次崩溃重演也一样），文件内容、嵌套路径、
     symlink target 逐条读回，之后的新写也成功；
   - 同样的 `kill -9` 打在 **meta-server** 上（引擎保持存活）得到**同一个签名**
     ⇒ 这条恢复路径与 treedb provider 无关，是 mount→meta-server 那一层在兜；
   - 顺带照出来的一条：引擎不可达期间 meta-server 不会锁死在旧连接上 —— 每一次
     M_OPEN 都重拨引擎，拨不通就拒（日志里逐条
     `treedb connect 127.0.0.1:<port>: Connection refused`），所以它自己也不用重启。

   **读侧的 errno 曾经是错的，2026-11-20 已闭合**（原缺口记在 §11 第 11 条）：故障期
   一次需要触达后端的读，对**挂载点自己**回答 `ENOENT` —— 探针的六次尝试（`ls`、
   `ls -U`、`ls -f`、`ls -l`、`find`、`stat`）全一样，整个文件系统看起来「不在了」。
   诚实的答案是 `EIO`，最次是 `ESTALE`；`ENOENT` 会让一个正常的脚本去重建一棵其实
   还在的树。

   修法是**加法式**的（grovefs 仓 `723a013`，6 文件 +417/−105）：`MetaProvider` 的四个
   读方法（`lookup`/`getattr`/`readdir`/`readdir_page`）各多一个 `*_checked` 孪生，
   默认实现就是 `Ok(self.xxx(...))`，所以只有真有错误源的三份实现要动 ——
   `RemoteMeta`（`call_node` 由 `Option<NodeInfo>` 换成
   `Result<Option<NodeInfo>, MetaError>`：`E_NOTFOUND → Ok(None)`、其它线上码走
   `err_from_code`、传输失败与坏帧 → `MetaError::Db`，`readdir` 同构）、`TreedbStore`
   （`Fail::to_meta()`，只有 `ENOENT`/`ENOTDIR` 折成「空」）、以及 meta-server 用的
   `MetaStore` 枚举分派（少了这一层，通道会在服务端被折回去）。FUSE 层把 `Err` 统一
   交给 `err_to_errno`，`Db → EIO`。旧的 `Option`/`Vec` 签名留着，变成 `_checked` 的
   薄包装。**引擎侧一行没动。**

   实测（同一份 `.build-bin/probe_crash_ls.sh`，`kill -9` 引擎后逐条量 rc/stderr）：
   改前故障期那六次尝试**全部**是 "No such file or directory"（`rc=2`、`n=0/12`），
   改后**全部**是 "Input/output error"；让缓存过期（`sleep 8`）之后再列一次，改前仍是
   `ENOENT`、改后仍是 `EIO`；引擎在同 dir 同 port 回来之后，第一次 `ls` 就 12/12，
   与改前一样。

   **为什么当时不能钉、现在能钉**：第一版断言写下去的那一跑，故障期 `stat` 是**成功**的
   （属性缓存还热），换一份探针跑同一句就成了 `ENOENT` —— 同一分钟内自己推翻自己，
   差别只在缓存热不热。有通道之后这件事反过来成了论据：缓存 TTL 一过，`getattr` 就
   必须打到后端，后端不可达就必须是 `EIO`，所以那个值是**确定**的而不是硬币。第 10 节
   因此从「只钉写侧」升级成两条确定性读侧断言（`sleep 8` 后故障期 `stat` 必须 `EIO`、
   故障期 `ls` 必须失败），`test_treedb.sh` 53 条断言 → **55 条，全绿**。

   同一次改动还关掉了一条**没被观察到的坏路径**：`RemoteMeta::readdir` 把失败折成
   `Vec::new()`（= 「目录到此为止」），而 FUSE 的分页循环用 `got < want` 判 EOF ⇒
   失败会静默截断。改前六次故障期列举**没有一次**是 `rc=0` 少条目，只是因为更早的
   getattr 已经先失败了 —— 那是个碰巧没被走到的洞，不是一条好性质。

   残留三处，明确不在这笔里：`PgEngine`/SQLite 引擎的读仍在各自的 SQL 调用处把 `Err`
   折成空值、`LayoutMeta` 这个展示透镜不转发内层错误、`dump_tree`（`du`）没有通道。
   `_checked` 的默认实现已经在 trait 上，补这三处是纯加法。
4. **控制面搬家：`grovefs_control` 那六张表迁进引擎的通用表**（**方案 2026-11-20
   成文；M1–M4 已全部落地**——接入侧 M1–M3 改在 grovefs 仓，内核侧的 M4 在 `v0.6.0`，
   见 §11 第 13/14/15/16/17 条）。表形状、绕法、切换顺序定死在这里，开工时不再做决定。

   **走通用表（`TableCreate` + `Row*`），不走 `Key*`/`Audit*`/`Def*`/`Place*`。**
   §11 第 7 条已定过调（专用 op 的形状就是某个客户端的表，直接接上等于把业务语义坐进
   内核）。这一轮对着列名核了一遍，多出一条更硬的理由：**专用记录装不下今天要存的字节**。

   | 引擎侧 | 客户端表（grovefs `src/apikeys.rs`、`src/mcp.rs`、`src/pg_store.rs`、`src/webauth.rs`） | 差在哪 |
   |---|---|---|
   | `KeyRec` 13 列 | `api_keys` **14** 列（M2 实测，原文写 15） | 缺 `scope`（这把 key 的授权范围）与 `secret_hint`（显示指纹）。原文写的 `secret_enc` 在本仓**不存在**，见下表后的「M2 的两处计划纠正」 |
   | `AuditRec` | `mcp_calls` 7 列 | 缺 `key_label`；多一个引擎自己发的 `seq` |
   | `DefRec` 4 列 | `view_defs` 3 列 | 不缺列，多一列 `kind` |
   | `PlaceSet`/`PlaceGet` | `view_placement` 3 列 | 多 `updated_at` |

   拿 `Key*` 接上去只有两条路：丢 `scope`（那是安全属性，不是备注），或者往 `KeyRec`
   上补两列 —— 后者正是 §11 第 7 条 L1 那一档在反对的事。所以搬家**顺手解掉**了
   四组专用 op，不是绕开它们。

   **M2 的两处计划纠正（`secret_enc` 不存在）**：上面这行核对是把客户端的**表**当成
   凭据形状写的，落地时逐列对了一遍，两处不对：① 本地 `api_keys` 是 **14 列**
   （`id` 做主键、`last_used_at` 由 M1 拆进 `api_key_usage`，其余 **12 列 1:1 进引擎**，
   连 `scope` 与 `secret_hint` 都原样有位置）；② 凭据**只有** `secret_hash`
   （SHA-256）与 `secret_hint`（头 4 尾 4 指纹），全仓没有 `secret_enc`、没有
   `STORE_KEY_ENV`/`STORE_KEY_FILE`、也没有 `<base>/meta/store.key` —— 所以 M2 原文那句
   「包裹材料必须跟着数据目录一起走 / 换着包裹材料搬 = 全部密钥解不开」在本仓
   **不成立**（不是风险被低估，是那条路径不存在）。于是「绝不重新包裹」落实成更强的
   读法：**摘要一字不动** —— 不重算、不换盐、不规范时间戳。

   **表形状。** 表名一律带 `grovefs_control__` 前缀：§5.3 末写着 `tables` 是全引擎一张
   平表、没有命名空间也没有权限，所以前缀是唯一的隔离手段，代价是**一个引擎实例只放一套
   grovefs 控制面** —— 与今天 PG 侧「control schema 只长在实例一上」同构，不算倒退。

   | 表 | 主键 | 索引列 | 现在的 SQL 为什么能落成这个形状 |
   |---|---|---|---|
   | `api_keys` | `id` | 无 | 鉴权路径只按 id 查（`fetch_key`）；列举是 `RowList{ROW_PK}` 全表扫 + 客户端排序（见下表「排序」那一格） |
   | `api_key_usage` | `id` | 无 | **新增**，只装 `last_used_at`，理由见「热路径」 |
   | `mcp_calls` | `!ts_us`（8 B 大端）`\|\| !seq`（8 B） | `bucket`（UTC 日 `YYYYMMDD`） | 主键取反 ⇒ 「最新 N 条」就是主键序前 N 条，`ORDER BY rowid DESC` 有了着落 |
   | `view_defs` | `name` | 无 | PG 那边靠 `SELECT xmin::text` 做乐观并发，这里换成对 `definition` 列 CAS（见下） |
   | `view_placement` | `view` | 无 | 「表还不存在」这个状态今天由 `lookup_placement` 返回的 bool 表示，正好对上 `TableGet → Table(None)`（不是错误） |
   | `web_accounts` | `account` | `role` | `SELECT COUNT(*)` → `RowCount{column: ROW_PK}`（O(1)）；`ORDER BY account` → 主键字节序，天然一致。**落地时多了一列 `rev`**（第 6 列，8 B 大端）：整行覆盖下两个写者改一行的 CAS 计数器，见下表后的「第五处」 |
   | `web_grants` | `account \|\| 0x00 \|\| base_view` | `account`、`base_view` | 两个方向都要问（某账户有哪些视图 / 某视图给了谁），而通用表**没有前缀谓词**，所以复合主键不能代替那两个索引列。分隔符取 `0x00`：两段都是 UTF-8 文本，NUL 不可能出现 |

   三条通用约定：没有 NULL 概念 ⇒ 今天的 `IS NULL` 一律编成**空字节串**（`revoked_at`、
   `disabled_at`、`expires_at`…）；布尔用 1 字节；时间戳**保持现有的 RFC 3339 文本**，
   迁移时不改写值，少一处出错的地方 —— 只有 `mcp_calls` 的主键需要定宽整数，那就让新行
   同时存 `ts`（文本，给人看）与进主键的 `ts_us`。
   **对得上、不需要绕的**（逐条对着 grovefs 现在的 SQL 核过，不是推理）：按主键取行、
   覆盖写（`RowPut` 默认就是 upsert，正对 `web_grants` 的 `ON CONFLICT DO UPDATE`）、
   只插不改（`PUT_IF_ABSENT`）、条件更新（`RowPut{cond_col, cond_val}`，`RowDelete` 同）。
   `UPDATE api_keys SET revoked_at=? WHERE id=? AND revoked_at IS NULL` 落成
   `RowPut(key=id, cond_col=revoked_at, cond_val="")`，而且**只有这一种**正确写法：
   `RowPut` 是整行覆盖（§5.2），所以「只改一列」必须是读 → 改 → 带 CAS 写回，CAS
   不命中回 `EAGAIN`。撤销/过期/只读这些判断今天就不在 SQL 里（`fetch_key` 把行取回来
   交给 `authenticate_for`/`live_state` 判），所以「没有范围谓词」这一条**不挡**鉴权路径。

   **对不上的四处与绕法**：

   | 现在的 SQL | 缺的能力 | 绕法（全在客户端侧，不动内核） |
   |---|---|---|
   | `SELECT … FROM api_keys ORDER BY created_at DESC` | 没有排序 | 全表扫 + 客户端排序。这张表是运维手工发的 key，量级在几百；真到了需要排序访问路径的那天，把 `created_at` 编进主键、`id` 转成 `COL_UNIQUE` 索引列 |
   | `count(*) FROM mcp_calls WHERE ts < cutoff` 与配套的 `DELETE … WHERE ts < cutoff` | `RowCount` 只按**等值**，也没有范围/批量删除 | 按 `bucket` 逐日 `RowCount` 求和；清扫同理，逐日 `RowList` + 逐行 `RowDelete`。今天那条 `DELETE` 在 SQLite 里也是一条条删，只是被语句数骗过去了 |
   | `SELECT COUNT(*) WHERE role='admin' AND disabled_at IS NULL` | 没有跨列条件 | `RowList{column=role, value="admin"}` + 客户端过滤 `disabled_at == ""` 再数（管理员是个位数到十位数）。**不引入派生列**（写一列 `admin_active` 之类）：派生列要求每个写侧都记得维护，漏一处就是一个说不清来源的错数 |
   | 删账户 = `DELETE FROM web_grants WHERE account=?` 再 `DELETE FROM web_accounts WHERE account=?`，两条语句 | 没有多语句事务（§5.3） | 顺序照今天「先删授权、再删账户」，风险挪到「孤儿授权」那一侧。那一侧今天已经是 fail-closed：登录与鉴权都先 `fetch(account)`，账户行不在就直接拒（`webauth.rs:745-758`、`webauth.rs:917-928`），授权行根本没机会被读到。**这条已不再只是「读代码核实的性质」：M1b 落地后由 `treedbctl` 套件断言——`account rm` 删掉 bob 后，`grant list` 里没有 bob、`account list` 里也没有，网关登录回 401；引擎重启后再读一遍，两者都没有回来** |

   **第五处（M1b 落地时新发现）：同一行的两个写者。** 这张表列的是「SQL 能力 vs
   引擎能力」，而真正咬人的一处不在能力，在**写模式**：`RowPut` 整行覆盖，「登录
   stamp `last_login_at`」与「管理员 disable」同时发生是两个写者，后写者会把自己
   读到的**陈旧另一半**写回——登录把已禁用账户重新启用是安全 bug，不是普通丢更新
   （正好与「不做长期双读」那一段同一个理由：分叉的字段里有安全属性）。先想的是
   SQL 那条 `RowPut{cond_col=disabled_at}`，但它只能表达一个方向（登录不得修改
   disabled 字段），表达不了「disable 不得旧写 last_login」；所以最终是加一列
   `rev`（每次编辑自增，`cond_col=rev`）：任何输家拿 `EAGAIN` 重读，只重放自己
   那一列。代价是一个「不装业务」的列与每次编辑一次额外往返。

   **热路径。** 每次鉴权都带一次 `UPDATE api_keys SET last_used_at=?`（`stamp_used`）。
   搬进引擎它就是**一次 WAL 写**：fsync 有 group commit 攒批，但它落在凭据那一行上，与
   rotate/revoke 抢同一行。三条一起用：① `last_used_at` 拆进 `api_key_usage`，热写不再
   CAS 凭据行；② 客户端限流，距上次落库不足 60 s 就不写 —— 这不改变可观察契约，今天的
   注释自己写着这条 UPDATE 可能整条丢（"a no-op for an id that vanished"），它从来不是
   精确计数；③ 拆表的代价说清楚：删 key 变成删两行而引擎没有级联，顺序是**先删 usage
   再删 key**，孤儿 usage 行只在 key 行存在时才会被读到，内容又只是一个时间戳。

   **切换顺序 M1–M4，每步单独可回滚：**
   - **M1** grovefs 侧加第三臂，新臂**默认不启用**，只在 `--meta-store treedb://`
     且设了 `GROVEFS_TREEDB_CONTROL=engine` 时才走。开关必须有：M1 落地而 M2 还没跑
     时，引擎里的控制面是**空的**，默认切过去等于把所有密钥和账户判成不存在 ——
     症状是「谁都登录不了」，不是「迁移没做」。表在首次 open 时 `TableCreate`，
     撞 `EEXIST` 视为成功（与 `resolve_or_create_view` 同一套路）。M3 完成后删掉这
     个开关（**2026-11-20 已删**，见本条 M3 段与 §11 第 16 条：变量不再被读，
     treedb 档 `control_tables_uri()` 恒为 `Some`；旧 unit 里残留该变量只在启动时
     打一行提示，不会把读源拉回本地）。

     **A 半（凭据 + 审计，`apikeys`）2026-11-20 已落地**：grovefs `0a3b92c`
     （`feat/treedb-read-errno`，7 文件 +827/−26，其中新文件
     `src/apikeys/treedb_ctl.rs` 580 行）。落地时原文那句「`apikeys.rs` 与
     `webauth.rs` 各自有一个 `enum Backend`，加 `Treedb` 就是加一个变体」被实测
     纠正两处，记在这里免得下一个人照原文重画一遍：
     ① `apikeys::Backend` 分派的不是后端，是**两套 SQL 方言**
     （`exec2(sqlite_sql, ps, pg_sql, pp)` / `query2(...)`）。treedb 没有 SQL，
     第三臂只能接在**方法层**：12 个动词各自先问一句「是不是引擎后端」，是就把语义
     交给 `treedb_ctl`。`exec2`/`query2` 里那个 `Treedb` 臂是「永远不该走到」的
     拒绝（同 PG 臂的理由，不是 `unreachable!()`）；方法里 5 处
     `let n = match &self.backend` 还得显式补一臂 —— 编译器看不见函数体里的早返回。
     ② 凭据读哪一份原本只由 `StoreKind::control_uri()` 一个出口决定，而它同时管着
     视图定义、凭据、web 账户三样东西；treedb 档它恒 `None`（"control plane
     stayed local"），于是 meta-server 的 control 面读的是本地 SQLite —— 端到端
     先撞出来的就是 `authentication failed: unknown api key id`。新增的
     `keys_uri()` 只回答凭据：PG → URI、SQLite → `None`、treedb → **仅当开关打开
     时** `Some("treedb://…")`，`WebAuthStore` 仍走 `control_uri`。
     引擎里的形状就是上面那三张表（`grovefs_control__` 前缀，12 + 1 + 7 列），
     热写按本节拆进 `api_key_usage`，`mcp_calls` 主键 `!ts_us || !seq` 让主键序 =
     时间倒序。证据：板上一行 `treedbctl`（`.build-bin/test_treedb_control.sh`，
     本地脚本不入库）**32/0** —— 开关关时逐字拒绝并点名变量、每个动词被下一个
     进程看见、control 面自己的鉴权写 4 条 `mcp_calls` 并 stamp `last_used_at`、
     `purge --before` 逐行真删、引擎重启后全在；回归 318/0、55/0、39/0/10 不动。
     边界（写在套件头里，不是沉默的缺口）：`grovefs mcp --meta-store treedb://`
     仍拒绝该 URI（视图定义没有引擎表，M2 不搬它），所以审计行的真实写入方目前只有 control 面鉴权
     这一条腿（web 账户表当时还在本地，同日的 B 半已把它也搬进引擎）。

     **B 半（web 账户 + 授权，`webauth`）同日落地**：grovefs `245fd63`（12 文件
     +1036/−233，其中 `src/webauth/treedb_ctl.rs` 373 行新文件、
     `src/control_treedb.rs` 271 行新文件）。本节表里那句
     `COUNT(*) WHERE role='admin' AND disabled_at IS NULL` 的跨列条件就在这一侧。
     落地时的三处计划纠正：
     ① **两张表都多一个 `rev` 列，而它不做业务**。`RowPut` 整行覆盖，「登录 stamp
     `last_login_at`」与「管理员 disable」是两个写者，没有条件时后写者会把陈旧
     的另一半写回——登录把已禁用账户写回来是**安全 bug**，不是丢更新。所以
     `web_accounts` 第 6 列 `rev`（8 字节大端，每次编辑自增）就是 CAS 列：输家拿
     `EAGAIN`、重读后只重放自己那一列，8 次为限。`web_grants` 5 列，主键
     `account\0base_view`，两列都 `COL_INDEX`——引擎没有前缀谓词，「这个账户的
     基视图」与「谁持有这棵树」只能走二级索引问，`0x00` 是安全分隔符（两半都是
     校验过的文本）。
     ② **`grant` 的 upsert 必须保住 `perm`**。SQL 是
     `ON CONFLICT DO UPDATE SET granted_at, granted_by`；照抄成整行覆盖会把一条
     ro 授权悄悄升成 rw。引擎侧是「有行就 CAS 只改这两列、没行就 `PUT_IF_ABSENT`
     撞键重试」。A 半那个 `keys_uri()` 也在这一半改名 `control_tables_uri()`：
     它现在同时回答凭据与账户表，控制面块用**同一个**出口开 `ApiKeyStore` 与
     `WebAuthStore`，两个出口迟早会说两句话。
     ③ **真正的拦路虎不在 `enum Backend`，而在控制面对引擎树不可见**（第三个
     计划纠正，单独说）：treedb 档 `rt.control` 的 `has_physical` 只枚举本地
     `meta/*.db`，而引擎里的基树在磁盘上没有文件 ⇒ `webauth::resolve_base` 对任
     何 treedb 基视图都回答 `view 'x' has neither a definition nor a tree` ⇒
     **委派永远命中不了**：账户能登录，但管不了任何东西（`web grant add --base
     team` 三连红才露头）。解法是 `mcp::TreedbControlStore` 装饰器——17 个方法里
     12 个定义动词逐个委派给 `FileControlStore`，只有 `list_physical`（本地 ∪
     引擎的 `Op::ListViews`）与 `has_physical`（本地 || 引擎含）是双答案，
     `store_for_db_path` 保持绑定（与 PG 同），`server_ext` 按 `StoreKind` 路由到
     `control_store_for_treedb`。**这条的教训：控制面每个「存在性」判据都得先问
     一句「它看得见引擎吗」**——单元层全绿，因为每一步都只碰本地文件，缺口只在
     端到端现形。
     证据（当轮实跑）：`treedbctl` **61/0**（A 半时 32/0）。新增三段——安装期
     owner 落进引擎（本地没有 `meta/web_accounts.db`）、create/grant/role/
     disable/rm 每步被下一个进程看见；网关 `POST /api/login` 把
     `last_login_at` **就地**写进引擎行、禁用账户登不进且文案说 `disabled`；引擎
     重启后账户、授权、改过的字段全在，重启的 meta-server 复用同一个
     `--admin-user` 不重建（`grep -c alice` = 1）。两条把「委派真的咬人」钉住的
     断言：bob 对授予的引擎基树 `view.stats` 回答 `"tree":true`，对未授予的名字
     回答 `not delegated base view 'other'`。回归：318/0、55/0、39/0/10，两档
     `cargo build --release`（含 `--features treedb`）无 warning；板子上
     web/apikey/ctlplane/manager 四红是基线红（改动前两份日志读数相同，且失败点
     依赖别的分支才有的 `apikey.secret`）。
     边界：视图**定义**与 placement 仍永远在本地（引擎没有对应的表），所以 treedb
     档的形状是「定义本地 + 树在引擎 + 行表在引擎」，
     `StoreKind::control_uri()` 与 `control_tables_uri()` 这一对就是它的编码；
     `fork`/`import` 的 treedb 臂未动。M1 至此覆盖整个控制面（凭据、审计、账户、
     授权）。原文那句「只剩视图定义在 M2」作废：视图**定义**没有引擎表（本段末尾的
     边界），M2 只搬行。
   - **M2** 新命令 `grovefs control-migrate --to engine`：读旧库、写通用表，**幂等**
     （重跑走覆盖写），跑完做全量比对 —— 每张表比行数与逐行逐列字节相等，比不过就
     非零退出、不进 M3。`secret_enc` 原样搬，**绝不重新包裹**：包裹材料在
     `<base>/meta/store.key` 或 `GROVEFS_STORE_KEY`（`apikeys.rs` 的 `STORE_KEY_ENV` /
     `STORE_KEY_FILE`），它必须跟着数据目录一起走。这是整批迁移里唯一一处不可逆的
     损失点：换着包裹材料搬 = 全部密钥解不开，而现场看到的症状同样是「谁都登录不了」。

     **2026-11-20 已落地**（grovefs `feat/treedb-read-errno`，10 文件 +823/−10，其中
     `src/control_migrate.rs` 421 行新文件）。上面那句 `secret_enc`/`store.key` 是
     本条唯一的计划纠正，理由见本节前面的「M2 的两处计划纠正」。形状：一条命令、两张
     源文件 → 五张通用表，每个表一条 `Move`，里面的 `TableDef`/列名表/
     `put`/`dump` **全部从各 store 的 `treedb_ctl` 借**，迁移侧不抄列号；三处不是
     直搬（`last_used_at` → `api_key_usage`、`mcp_calls` 的键要算、`rev` 要补种）
     每个都写了理由。两道守卫：开关已开就拒（引擎是活库，覆盖 = clobber）、两个源
     文件都不在也拒（M3 把第一道改了口：开关不存在了，判据换成「源已改名成
     `*.pre-treedb` ⇒ 这台已切写」——见本条 M3 段与 §11 第 16 条）。源库以只读
     打开，**不删源文件**（回滚 = 关开关）。证据：`treedbctl` **88/0**（M1b 61/0），
     实测 `migrated 12 rows across 5 tables`
     （1/1/7/2/1，等于两个本地文件的总行数），详见 §11 第 15 条。
   - **M3** 切写：控制面读写都走引擎，旧文件**改名保留**（`api_keys.db.pre-treedb`），
     删除留给下一个版本；回滚 = 把名字改回来（**2026-11-20 已落地**，见 §11 第 16 条）。
     落地时定死的三件事：① 切写由**服务进程自己**做（打开控制面前的一个 `settle`），
     不是由 `control-migrate` 改名 —— 命令只写引擎，改名是「这个进程能服务了吗」
     的后果，两者分开才能让「迁移完但还没重启」这个合法状态存在；② 没迁移就拒绝
     启动并点名命令（引擎空 + 本地有行），而不是带着空表服务；③ 改名只动主文件，
     `-wal`/`-shm` 留在原名下，回滚 `mv` 回去才能拿到被服务时的那个库。
   - **M4**（已完成，`v0.6.0`）动 §11 第 10 条那一半：删 `Key*`/`Audit*`/`Def*`/`Place*`
     四组 op，§5.1 的 12 张表缩回 8 张、L1 那一档从「仍违反」改成「不违反」
     （细节见 §11 第 17 条）。顺序不能换 —— 删早了会把 M1–M3 这条路踩断。

   **明确不做长期双读。** 直觉上「先双读、发现不一致再切」最稳，这里不成立：
   `last_used_at` 一直在写，两份副本从任何一处开始写的那一刻就分叉，而分叉的字段里
   有 `revoked_at`。读「哪份有就用哪份」等于给一把已撤销的 key 第二次机会 —— 那是安全
   属性，不是可用性属性。可接受的窗口只有 M2 那种**写仍只在一侧**的复制 + 比对，
   且不长于一次维护窗口。

   **接入侧前置复核（2026-11-25，`benches/grovefs_pin_ab.sh`，原始输出
   `benches/results/grovefs_pin_ab.log`）**：M1–M4 的代码全落在 grovefs 仓，引擎这一轮
   还欠的只有「把 pin 往上提」这件事值多少改动。做法是同一份 grovefs 源码、同一个
   worktree（`feat/treedb-read-errno` @ 16184ca），唯一变量 = `Cargo.toml` 里那行 tag；
   两臂各跑编译、仓内测试、六条端到端套件，**且各配同代引擎二进制** —— 共用一份引擎的
   话测的是「新引擎兼容旧客户端」，不是「pin 变了」。

   | ref | pin | `control-migrate` | `Op::Link` |
   |---|---|---|---|
   | `master` | `v0.3.7` | 无 | 无 |
   | `origin/master` | `v0.3.7` | 无 | 无 |
   | `feat/treedb-hardlink` | `v0.7.0` | 无 | 有(`src/treedb_store.rs`) |
   | `origin/feat/treedb-hardlink` | `v0.7.0` | 无 | 有(`src/treedb_store.rs`) |
   | `feat/treedb-read-errno` | `v0.3.6` | 有 | 无 |
   | `origin/feat/treedb-read-errno` | `v0.3.6` | 有 | 无 |

   | 套件 | 臂 A：pin v0.3.6 | 臂 B：pin v0.8.0 |
   |---|---|---|
   | `test_treedb_control.sh` | 111 / 0 | 111 / 0 |
   | `test_treedb_matrix.sh` | 39 / 0 / 10 | 39 / 0 / 10 |
   | `test_treedb.sh` | 60 / 0 | 60 / 0 |
   | `test_locks_treedb.sh` | 6 / 0 / 9 | 6 / 0 / 9 |
   | `test_govcheck_treedb.sh` | 21 / 0 / 4 | 21 / 0 / 4 |
   | `test_govtime_treedb.sh` | 8 / 0 / 0 | 8 / 0 / 0 |

   读数之外，还有四件事得从这两张表里读出来：

   - **两臂对称是「零源码改动」的尺子。** 六个三元组逐行相同（合计 245 pass / 0 fail /
     23 skip），`cargo check --release --features treedb --all-targets` 两臂都是
     0 warning / 0 error，`cargo test --release --features treedb` 两臂都是
     **318 passed / 0 failed / 4 ignored**（19 个测试二进制），而两臂之间被改的文件只有
     `Cargo.toml | 2 +-` 加 `Cargo.lock | 4 ++--` —— lock 里动的恰是 version 与 source
     两行。套件表里的字段数 = 该套件 `SUMMARY` 印出来的字段数，只带两个计数器的不补零，
     免得把「这套没有 skip 概念」读成「skip=0」。
   - **集成分支还不存在。** 控制面搬家（`src/control_migrate.rs`）只在
     `feat/treedb-read-errno` 上、pin `v0.3.6`；硬链接（`Op::Link`）只在
     `feat/treedb-hardlink` 上、pin `v0.7.0`；两边的 `master` 两样都没有。所以阶段 C 1
     那句「矩阵保持 `51/0/0`」在两个分支合流之前**结构上无法达成**：迁移分支上没有
     `Op::Link`，那 10 条 `[posix C]` 只能 SKIP（`39/0/10`），红的是「能力缺席」而不是
     「行为回归」—— 同一套断言在硬链接分支上确实是 `51/0/0`（见阶段 C 1 的表）。
   - **M1–M4 的接入面确实只剩通用表。** 该分支往引擎写的是
     `grovefs_control__api_keys`、`grovefs_control__api_key_usage`、
     `grovefs_control__mcp_calls`、`grovefs_control__web_accounts`、
     `grovefs_control__web_grants` 五张表，而 `Op::Key*`/`Op::Audit*`/`Op::Def*`/
     `Op::Place*` 四组专用 op 在它自己的 `src/` 里各残留 **0 行**（删 op 是引擎侧
     `v0.6.0` 的 M4，接入面从 M1 起就没再碰它们）。
   - **混代是硬失败，而默认值会咬人。** 拿旧 pin 建出的 grovefs 去接**新**引擎二进制
     —— 这正是套件里 `TREEDB_BIN` 不设时的默认取值
     `$PWD/../treedb/target/release/treedb-server`，而它当时是 HEAD（613c1f9）的
     `v0.8.0` 构建 ——
     控制面是 `33 / 81`、矩阵是 `0 / 1 / 0`（两条都 `rc=1`），点名
     `errno 68 (protocol version negotiation required: send VersionBegin first (§4.4))`：
     §4.4 的闸门要求 `client_proto` 与 `PROTOCOL_VERSION` **精确相等**，7↔8 与 8↔7 都拒，
     没有混代滚动窗口。这一档不该当回归读，但它是个运维坑：六条套件都**没有引擎世代门**
     （self-skip 只覆盖「没开 feature」「没找到二进制」两种），所以今天谁在迁移分支上直接
     跑板子，看到的第一现场是 `cannot reach the control plane` 而不是「引擎太新」。两臂
     各配自己的引擎、并把两份 `sha256` 随块印出来，才是「pin 变了」这一件事的测法。

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
2. ~~**checkpoint 不阻塞回合**（限制 7）~~ **已完成**（2026-11-21，§8 第 7 条）：
   自动档改成旁路重放式快照 —— 主循环只取水位 + spawn，后台 `treedb-snap` 线程读
   盘上快照 + 重放 WAL 到该水位，收尾用 `truncate_before` 保留尾巴。20 000 节点实测：
   停轮 23.2 ms → 派工 37.5 µs，且停轮不再随状态大小增长。
3. ~~**`Stats` 加磁盘长度**~~ **已完成**：`EngineStats` 多了五个字段 —— `wal_disk_bytes`
   （文件 `st_size`，预分配后只会 ≥ 逻辑长度）、`snap_bytes`（上一代快照的字节数，
   没 checkpoint 就是 0）、`tables`/`rows` 两个计数、`proto_version`。两个 WAL 长度各说
   一件事：逻辑长度决定 checkpoint 何时触发，磁盘长度决定这块盘被占了多少（限制 9）。
   `proto_version` 不只是好看：帧里没有版本位（§4.1），它是线上唯一能查到对端协议
   版本的通道。`treedb-server --stats` 把这三个长度并列打出来。
4. ~~**日志传送做只读副本**~~ **已完成**（§6.6）：pull 式 `WalFetch` + 分块 `SnapFetch`，
   新入口 `treedb-standby`（拉快照 → 追 WAL → 只读应答），限制 6 从「无复制」
   降级成「无同步复制」。原写法里那句「至少能降级成无写扩展」只做到一半：
   副本确实开了只读容量，但写扩展仍在单提交线程上（§8 第 1 条第 ③ 项）。
   接下去要动的三项，按价值排序：① ~~传送吞吐没实测~~ **六格都已补**。同机三格
   （2026-11-22，`benches/replica_rate.sh`，§6.6 边界 4）：冷启动 1 MiB/块 696–773 ms
   端到端（传输段 26–30 MiB/s）、4 MiB/块 397–470 ms（50–71 MiB/s），追赶 23.5–25.8
   万条/s（17.0–18.7 MiB/s），追平时副本 `ping` 的 p99 +11%…+28%。跨机三格（同日
   `benches/replica_rate_xhost.sh`，§6.6 边界 5；副本放第二台机，主与负载发生器在宿主，
   中间是真 virtio 链路，裸 TCP 1838 MiB/s）：冷启动 1 MiB/块 1006–1254 ms、4 MiB/块
   516–637 ms，追赶 20.1–21.0 万条/s（14.6–15.2 MiB/s）；**「副本在追对主」做成了
   A/B**（A 无副本 / B 副本全程落后 200 多万条，写入量逐字节相同）：p50 1.09 → 1.53 ms
   （+39%）、p90 1.87 → 2.30 ms（+23%）、吞吐 5.04 → 4.18 万 create/s（−17%），p99 也涨
   但分散（+14…+32%，它是单点分位，结论以 p50/p90 + 吞吐为准）。这一格还是
   `src/net/multi.rs` 那个「空 `ops` 分支 `continue` 掉纯 `EPOLLOUT`」bug 的发现现场
   （只在「跨机 + 大回包 + 多 worker」同时成立时现形），修复与不依赖网络形状的回归
   用例 `tests/large_reply.rs` 见 §6.6 边界 5 末段；其中冷启动那两格是**定长块长**的
   读数，其后 `--fetch-bytes` 又从定值改成下限（同机追赶 1.88×、跨机 1.60×），并把
   这条规则铺到快照段（同机冷启动 1 MiB 档 750 → 573 ms、跨机判无回归）—— 见 §6.6
   「追赶块长」；
   ② ~~没有主身份认证~~ **已补一半**（§4.4）：共享密钥双向握手能证明
   「对端持有这把密钥」；③ ~~没有提升路径（`--promote`）~~ **已完成**（v0.4.0，
   §6.6「切主」）：`fence` 文件 + 两个 reply 尾部的 `epoch` + `--promote`，副本
   拒收被提掉权的旧主。仍缺的是「旧主自己知道权被拿走」那道门（要租约/心跳），
   以及自动故障转移 —— 二者都不在近期计划里。
5. ~~**接入 spike**~~ **已完成并端到端跑通**（grovefs 实验分支
   `exp/treedb-integration`，用户 2026-11-19 授权），结果与缺口清单见 §10 阶段 B
   末节。与本节原写法有两处偏离，原因已写进 §10：CLI 入口用
   `--meta-store treedb://host:port` 而不是新旗标；写接入代码前先把 `data_ref` 改成
   不透明字节、给 `Unlink`/`Rmdir`/`Rename` 补上 GC 线索与 `RENAME_NOREPLACE`。
   原先卡住的「分发方式」已定：本仓推到 gitee+github 并打 `v0.3.0`，grovefs 改成
   按 tag 的 git 依赖，拿没有同级克隆的干净仓验证过默认档与 `--features treedb`
   均绿。**仍未推 master**：推不推是下一轮的决定。原本卡在这条上的理由已经消除：
   treedb 此前只接了「树」，定义/密钥/账户还在本地 SQLite，要么先接受这个分裂上线，
   要么先把下面第 7 条的通用表能力补进引擎再一次性切（注意：是补「通用表」，不是把
   grovefs 的表搬进去）—— **第 7 条的 T2 本轮已完成**，所以现在只剩选择本身；切过去
   之前还得先算清 §10「bump tag 前必读的两处破坏面」里第 ② 条那个数据目录升级问题 ——
   现在它已经有答案了（`treedb-migrate`，§6.5），剩下的只是执行顺序。
6. ~~**`ReadDirPlus`**~~ **协议面已完成**：接进去才发现 readdir 一页要付 `1 + N/64` 次
   往返 —— 目录列表本身不带属性，provider 只能回头逐条 `Getattr`。现在
   `Reply::DirentsPlus` 在一页里直接带上与 `Getattr` 同源的 `NodeAttr`：四个平行数组
   同长（缺 inode 时补默认值而不是丢条目，否则客户端按下标对位会错位），分页边界与
   `ReadDir` 逐字一致。守门用例两条：
   `readdir_plus_answers_the_same_page_as_readdir_plus_getattr`（同一页、同一边界、
   逐字段对齐 —— 属性必须是算出来的而不是另一套真相）、
   `generic_table_and_readdir_plus_over_tcp`。**剩下的在接入侧** —— 2026-11-20 已接：
   grovefs `treedb_store.rs::page()` 改发 `ReadDirPlus`，`Op::ReadDir` 在这个文件里
   一处不剩；往返省下多少见 §10 阶段 B 末缺口表那一行（op 数 3006 → 6）。当时写的
   「而它钉 `v0.3.2`」也已过时；2026-11-21 grovefs 侧提到 `v0.3.7`（提交 `a8fcfae`，
   只动 `Cargo.toml` / `Cargo.lock` 两行，无适配改动；`test_treedb.sh` 在该分支实测
   **53/0**）。本仓随后到了 `v0.4.0`（切主），但那一版的线协议变更只在两个副本专用
   的 reply 尾部，grovefs 那条路径不受影响 —— 所以 grovefs 仍钉 `v0.3.7` 是有意的，
   不是漏升级（§10 阶段 B 末表最后一行）。**2026-11-21 这一行也翻页了**：硬链接闭合
   时 grovefs 把 pin 升到了 `v0.5.0`（动机只有 `link_node`，§10 阶段 C 1），所以「仍钉
   `v0.3.7` 是有意的」只在硬链接闭合之前成立；几小时后 `v0.5.1` 又跟了一档，动机是
   自动 checkpoint 不再停轮（§8 第 7 条），适配层同样零改动。
7. **独立性审计：treedb 是一个数据库项目，不是 GroveFS 的一个模块**。用户定的约束
   是：grovefs 的业务语义不得漏进引擎内核，为 grovefs 做的适配一律留在 grovefs 仓里
   拦在 feature 门后，treedb 对外只讲通用元数据引擎接口。本轮对着代码校了一遍，
   **三档现状**：

   | 档 | 现状 | 是否违背约束 |
   |----|------|------------|
   | **L0 命名与注释** | 引擎内核里 21 处 grovefs 提及**全部是注释**（「与 grovefs `node.is_dir` 的 0/1/2 一致」这类对齐声明）；无 `use grovefs::`，无 grovefs 类型 | ~~无害，但叙述口径要改~~ **已改**（T3）：24 行重写为「POSIX/FUSE/关系库通用形状」的写法，理由保留、主语不再是某个客户端，并由门 1 守住 |
   | **L1 数据结构与 op 语义** | ~~`KeyRec` 逐列照抄 `grovefs_control.api_keys`（`ro`/`owner`/`note`/`expires_at`/`secret_hint`/`created_by`），`AuditRec` 照抄 `mcp_calls`，`Def*` 就是视图定义，`EADV = 68` 注释写着「借用 grovefs 控制面既有码」，`KeyCheck` 里带 `verb` 授权参数~~ **已修**（M4，`v0.6.0`）：14 个专用 op / 9 个 reply / 8 个 `Log` 变体 / 四张内存表与快照段整段删除，tag 区间 0x0040–0x0062 与 0x0140–0x0161 留空不回收（`removed_control_plane_tags_are_unknown`）；接入侧早在 M1–M3 就已改用通用表 | ~~违反~~ **不违反** |
   | **L2 依赖图** | ~~`postgres`/`rusqlite(bundled)`/`sha2`/`uuid`/`chrono`/`tracing`/`parking_lot` 全是非 optional 依赖~~ **已关进 `postgres-compat`**（T1 完成）：默认档 `cargo tree` 只剩 `libc` + `thiserror`；依据仍是 grovefs 只用 `treedb::net::Client` 与 `engine::model::Op`，`pool/`、`store/`、`persist/`、`schema/` 一行符号都没用 | ~~违反~~ **已修** |

   好消息是边界已经天然存在一半：`data_ref` 是不透明字节（引擎不解释内容键），
   引擎**不算哈希**（`sha2` 只在 `postgres-compat` 档与 demo 里），错误不做二次
   翻译，字节区间锁与 `link()` 不做 —— 这些都是
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
   - ~~**T2 把密钥/审计/定义三张专用表换成通用表能力**~~ **引擎侧已完成**（本轮）：
     内核给的是 `TableCreate`/`TableDrop`/`TableGet`/`TableList` + `RowPut`/`RowGet`/
     `RowList`/`RowDelete`/`RowCount`，列为不透明字节，可标 `COL_UNIQUE`/`COL_INDEX`，
     写可带 `PUT_IF_ABSENT`/`PUT_IF_EXISTS` 与按列 CAS，删可带 `DEL_IF_EXISTS`；具体列名、
     撤销/过期/只读这些规则全部由客户端定义（形状与刻意边界见 §5.1、§5.3）。引擎
     **没有**替客户端解释任何一列：`revoked_at` 在 `KeyRec` 里是语义，在 `RowPut` 里只是
     一串字节。协议版本同批递了（`PROTOCOL_VERSION` 2 → 3）并补了 decode 向量：op/reply/`Log`
     三个 round-trip 用例当时覆盖全 47 op / 26 reply / 22 条 `Log` 变体（`v0.3.5` 起是
     49/28，§4.2），不是「字段名改了
     但版本号不动」。**剩下的一半**：那几张专用表与它们的 op 还在内核里，拆干净才算
     这项完成（第 10 条）。**2026-11-23 已拆完**（M4，`v0.6.0`）：14 个 op / 9 个
     reply / 8 个 `Log` 变体与四张内存表整段删除，`SNAP_VERSION` 3 → 4、
     `PROTOCOL_VERSION` 6 → 7，§5.1 回到 8 张表（细节见第 10 条与 §6.5）。
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
     跑法：默认档与 `--features postgres-compat` 两档都要跑这三门。**具体计数不复述**
     （写了就会漂）：以 §9 那张表为准，本轮那一格是 112 / 143。本轮加通用表时它们一条没红 —— 新 op 没带进客户端名字，
     门 1 测的就是这条线。

   接入侧因此得到的约束：第 5 条里“把定义/密钥/账户补进引擎”的正解不是
   `Key*`/`Audit*` 直接给 grovefs 用，而是 grovefs 用 T2 的通用表自己拼语义 ——
   T2 本轮已可用，这句从「得先造工具」变成了「可以开工」；§10 阶段 B 第 4 步那句
   「走 treedb 的 `Key*`/`Audit*`」作废。
8. **`always` 档的 flush 流水线**：**已实现，但默认关 —— 实测推翻了原本的收益
   假设**。两段历史要分开看。
   - 第一段（已完成）：按连接串行 fsync 已删，p99 从 81.5 ms 降到 31.1 ms、吞吐
     5.77×，原判据（与 `group` 同量级、崩溃用例全绿）已满足。
   - 第二段（本轮做完，结论是**不该默认开**）：专用 flusher 线程（fd 由
     `Wal::try_clone_sync_fd` 从提交线程 dup）、提交线程只 `append`、`SyncDone`
     回执到了才 ack 那一批（`net::multi::Flusher` / `Pending::Flight`）。三条实现
     纪律：同一时刻只允许一个 fsync 在飞（每轮都 submit 会把批次切碎，fsync
     次数变多、每次覆盖变少，实测 QPS 再掉 13%）；checkpoint 收口与退出前
     `drain_all`，收口后 `Flusher::retarget` 换 fd（`truncate_before` 会换 inode，
     旧 fd 上的 fsync 全打在孤儿 inode 上）；`never` 档不进 flusher。

     **实测（128 核、fsync 1.3–1.6 ms，同目录同参数各 3 跑取中位）**：

     | 格 | 默认（提交线程 fsync） | `--flush-pipeline` | 差 |
     |---|---|---|---|
     | create 64/1 | 3 945 ops/s，p50 1 073 µs | 3 862 ops/s，p50 1 243 µs | QPS −2.1%，p50 +15.8% |
     | create 64/32 | 22 257 ops/s，p50 269 µs | 21 074 ops/s，p50 304 µs | QPS −5.3%，p50 +13% |
     | create 1/1（8 线程在飞） | 4 121 ops/s，p50 1 052 µs | 3 856 ops/s，p50 1 211 µs | QPS −6.4%，p50 +15% |
     | p99（64/1 / 64/32） | 18.1 / 1.3 ms | 18.0 / 1.3 ms | 持平 |

     结论：**原判断里「fsync 窗口内不接活」这半句不成立**。旧实现里转发本来就堆
     在 worker 信箱里，fsync 返回后照样一次性排干；提交线程在窗口内唯一没做的是
     `translate`，而它是 µs 级。把 fsync 挪出去换来的重叠抵不过多出来的
     「eventfd + poll + 线程唤醒」约 150 µs。默认档因此回到同步路径，开关
     `--flush-pipeline` 保留：它的收益条件是「fsync 占周期比例高 **且** 单 op 的
     `translate` 重」（大目录 readdir 的兄弟 op），届时可单独打开。

     验收：`tests/crash_recovery.rs` 七条在**两档下都绿**（新增
     `acked_writes_survive_sigkill_group_multi_worker`；`TREEDB_TEST_FLUSH_PIPELINE=1`
     把整套用例切到流水线路径）。原判据里的「p99 ≤ 8 ms / 12 k ops/s」在本机不可达，
     因为瓶颈是 64 连接轮转（每 op 一次 fsync、1.3 ms），与是否异步无关 —— 那两个
     数字依赖的是「一轮能合并多少写」，那是批大小的取舍，不是流水线能给的。
9. ~~**dirents 应答直接编进连接发送缓冲**~~ **陈旧条目，前提已经不成立**：这条当初
   的靶子是「`Reply::Dirents.names` 逐名一个 `Vec<u8>`，一页 104 次分配里 100 次来自
   它」。但 `Names` 早已改成扁平缓冲（`src/engine/model.rs`，名字连着放 + 偏移表），
   `8e4a4bf` 把每页分配从 104 降到 **5**，边际成本 42 → 10.6 ns/条，端到端 readdir
   +53%（并发档 +25%，§7.5 ③ 有 micro A/B 表）。剩下的「再直接编进发送缓冲」只把
   5 次压到 2–3 次，量级已经在噪声里 —— 不再当改进项排。
10. **两件收尾的活，两件都已做完**：
    - ~~**快照迁移工具**~~ **已完成**（`v0.3.4`，§6.5）：`SNAP_VERSION` 2 → 3 把旧数据
      目录挡在门外那句「升级等于重建」现在有出路了 —— `treedb-migrate` 读旧布局、
      按新布局写回，WAL 不碰。当时列的两条备选里选了一条更窄的那条：**没有**做
      `treedb-server --migrate <dir>`，也没有在内核里养若干代旧快照的 decode 分支 ——
      只多认一代，且只在离线工具里认（启动路径依旧拒），兼顾「可用」与「不在
      内核里堆历史」。回滚窗口另有一段要紧的真实边界写在 §6.5：无损只维持到新二进制
      第一次 checkpoint 之前，`benches/rehearse_migrate.sh` 拿 `v0.5.1` 真编译的旧二进制
      把这一面与那一面都量过。剩下没做的：v1 → v2 仍无路径（字段宽度变了），v2 → v3
      也随 `v0.6.0` 的「只认上一代」断链（跨两代要跑两遍旧版本）；再早的目录要不要管，
      得等到真有人从那里升级时再说。
    - ~~**把 `Key*`/`Audit*`/`Def*`/`Place*` 四组专用 op 从内核拆掉**（T2 剩下的那一半）~~
      **已完成**（M4，`v0.6.0`，2026-11-23）：14 个 op / 9 个 reply / 8 个 `Log` 变体 /
      四张内存表与快照段整段删除；`SNAP_VERSION` 3 → 4（legacy 只认 3，四表非空即拒）、
      `DUMP_MAGIC` `TDBDUMP2` / 容器版本 2、`PROTOCOL_VERSION` 6 → 7（删帧不是纯追加）。
      删前先核过 grovefs 三个工作树（master 与两个 feat 分支）对那 14 个 op 零引用，
      删后默认档 211/0、`--features postgres-compat` 242/0、clippy 无 error。
      L1 那档因此从「仍违反」改成「不违反」，§5.1 的 12 张表也缩回 8 张。
      同一次核对还查出专用记录本来就装不下今天要存的字节（`KeyRec` 缺 `scope` 与
      `secret_hint`，`AuditRec` 缺 `key_label`）—— 所以这几组 op 不是「能用但丑」，
      是**不够用**，拆它们的理由比原先记的更硬。
11. **后端不可达时，grovefs 的读回答 `ENOENT`**（§10 阶段 C 3 测出来的；**改在
    grovefs 仓，引擎侧一行不用动**）—— **2026-11-20 已闭合**：grovefs
    `feat/treedb-read-errno` `723a013`，6 文件 +417/−105。症状：引擎被 `kill -9`
    之后，`stat` 一个确实存在的挂载点回答 "No such file or directory"。根因在行：
    读侧的 RPC 封装 `RemoteMeta::call_node` 签名是 `Option<NodeInfo>`，`_ => None` 把
    「传输失败」「后端报了个 errno」「这个名字不存在」三件事折成一个值，FUSE 层只能
    回 `ENOENT`；`readdir` 同构，失败折成 `Vec::new()`（= 「目录到底了」），而分页
    循环用 `got < want` 判 EOF ⇒ 静默截断。**写侧没有这个洞**：`call_id`/`call_status`
    返回 `Result<_, MetaError>`，传输失败单独成 `MetaError::Db` —— 那个「写被拒
    （`EIO`）、读说谎（`ENOENT`）」的不对称，全部来源就是这两组封装的差别。

    难在修法的半径：`MetaProvider` 的读方法本身就是 `Option`/`Vec`
    （grovefs `src/meta.rs`），实现有六份（`RemoteMeta`、`MetaEngine`、`MetaStore`、
    `LayoutMeta`、`PgEngine`、`TreedbStore`）。最后选的**不是**改签名（124 个调用点、
    78 个在引擎内部），而是加法：四个读方法各长一个 `*_checked` 孪生，默认
    `Ok(self.xxx(...))`，只有 `RemoteMeta`、`TreedbStore` 与 `MetaStore` 的枚举分派
    覆盖它；`Err` 在 FUSE 层统一 `err_to_errno`（`Db → EIO`）。被否的另一条是「只让
    `RemoteMeta` 记住上一次 RPC 是传输失败」—— 改动更小，但把事实藏进隐式状态，
    且 PG/SQLite 档照样在说这个谎。

    验收判据达成：§10 阶段 C 3 那条「listing 要么失败要么列全」升级成「必须失败且
    errno ∉ {`ENOENT`, `ENOTDIR`}」—— `.build-bin/probe_crash_ls.sh` 故障期六次尝试
    （`ls`、`ls -U`、`ls -f`、`ls -l`、`find`、`stat`）改前全是 `ENOENT`、改后全是
    `EIO`；缓存过期（`sleep 8`）之后仍是 `EIO`，这条才让第 10 节敢钉读侧
    （`test_treedb.sh` **53 → 55 条，55/0**）。回归面：`cargo test --release` 318/0、
    阶段 C 1 矩阵 39/0/10、`.build-bin/run_all.sh` 整板逐套件摘要与改前一致。

    残留（同样形状，未修）：`PgEngine`/SQLite 引擎的读仍在 SQL 调用处把 `Err` 折成
    空值、`LayoutMeta` 透镜不转发内层错误、`dump_tree`（`du`）无通道。`_checked` 已在
    trait 上，补它们是纯加法。
12. **万级视图下客户端侧的连接数是线性的**（§7.12 量出来的；**改在 grovefs 仓，
    引擎侧一行不用动**）—— **2026-11-20 已闭合**：grovefs `feat/treedb-read-errno`
    `502d929`，1 文件 +118/−66。形状：`TreedbStore` 每个实例自己拨
    `DEFAULT_CONNS = 4` 条，而 meta-server 的 `store_for` 按**视图名**缓存 store，
    于是 4×N —— 探针实测 1/10/50/100 视图 = 4/40/200/**400** 条。引擎侧早在 §7.6
    就证明能撑 2.5 万条，但那是把结构性优势又换回了线性成本；而且 4×10 000 = 4 万
    条，在本机量具上连测都测不了。

    判据：treedb 的每条 op 自带 `view`，连接上没有 per-view 状态，所以池该按地址
    共享 —— `ConnPool::for_addr()` 用 `OnceLock<Mutex<HashMap<..>>>` 做到进程级一份，
    `TreedbStore` 只持 `Arc<ConnPool>` + `view`/`view_id`。**失败重拨语义不变**：
    `xcall` 仍只对只读 op 重连重试一次，写 op 失败直接 `Db`。验收达成：
    `benches/many_views.sh` 在 {100, 1 000, 5 000, 10 000} 视图、每档把新建视图
    **全部挂一遍**（即 10 000 个驻留 store）下，引擎侧 established socket **恒 4、
    峰值 4**，meta 侧 socket fd 恒 6；挂载内 create+unlink 3 965 → 4 194 µs（+5.8 %）；
    RSS 10 000 个驻留 store 多付 **14.1 MiB ≈ 1.44 KiB/视图**。回归面：
    `cargo test --release` 318/0、`test_treedb.sh` 55/0、阶段 C 1 矩阵 39/0/10。
    原始输出：`benches/results/many_views.log`、`many_views_conc.log` 与六份
    `conn_pool_{before,after}_{seq,conc,ab}.log`。

    残留：池大小就是每进程的跨视图并发上限（`GROVEFS_TREEDB_CONNS`；8 个视图同时打
    时把池 4 → 32 实测只快 13–26 %，见 §8 第 15 条）；多开一个 meta-server 进程就
    多一份池，部署时连接总数按进程数算。另记一个把结论测反的坑：不要在循环里
    「建视图 → 挂一次 → 卸掉」去数连接，`view create` 结尾的 `rt.evict()` 会清掉整个
    引擎缓存，读数会恒定成「本来就是共享的」（§7.12 末）。

13. **M1 的 A 半：控制面凭据 + 审计已在 grovefs 侧落到通用表上**（§10 阶段 C 4
    切换顺序的第一步；**改在 grovefs 仓，引擎侧一行不用动**）—— **2026-11-20 已
    闭合**：grovefs `feat/treedb-read-errno` `0a3b92c`，7 文件 +827/−26
    （`src/apikeys/treedb_ctl.rs` 580 行新文件、`apikeys.rs` +192/−4、
    `treedb_store.rs` +20/−11、`remote/meta/server_ext.rs` +26/−2，另三份文档）。

    形状：`apikeys` 的 12 个动词全部有引擎路径（`fetch_key`/`stamp_used`/
    `create_extended`/`set_ro`/`update_fields`/`rotate`/`revoke`/`list`/
    `log_call`/`recent_calls`/`count_calls_older_than`/
    `purge_calls_older_than`），三张 `grovefs_control__` 前缀的通用表
    （`api_keys` 12 列 / `api_key_usage` 1 列 / `mcp_calls` 7 列），首次 open 时
    `TableCreate`、撞 `EEXIST` 视为成功。开关 `GROVEFS_TREEDB_CONTROL=engine`
    **默认关**，关时拒绝且点名变量、不建本地 `api_keys.db` —— 这正是 M1 原文要求
    “在 M2 跑完之前不能默认切”的那道闸（**M3 已于同日删掉这个变量**：现在
    treedb 档无条件走引擎，没迁移的部署启动即拒，见第 16 条）。

    两处与原文不成立的地方（详细理由写进 §10 C 4 的 M1 条）：`Backend` 分派的是
    两套 SQL 方言而非后端（第三臂落在**方法层**）；凭据的出口要从
    `StoreKind::control_uri()` 拆出只回答凭据的 `keys_uri()`，否则 meta-server 的
    control 面永远读本地 SQLite（端到端先撞出来的是
    `authentication failed: unknown api key id`）。

    证据（当轮实跑）：新套件 `.build-bin/test_treedb_control.sh`——板上 `treedbctl`
    一行，本地脚本不入库——**32/0**；端到端草稿 31/0。回归：默认档
    `cargo test --release` **318/0**、`test_treedb.sh` **55/0**、阶段 C 1 矩阵
    **39/0/10**，与 M1 前逐项相同。另外三处散文计数也顺手对齐了：USAGE.md /
    USAGE.en.md / README.md 的「全板 41 段（38 个套件）」实为 **44 段（41 个套
    件）**——`manager` 与 `treedbmatrix` 早就进了 CANON 而散文没跟上，本轮再
    加 `treedbctl`。

    未做的部分（不在本条范围内，开关按 M1 原文保留到 M3）：**B 半** = `webauth`
    的账户/授权表，同日已落地（第 14 条）；视图定义仍在本地（M2 迁移之前
    `grovefs mcp --meta-store treedb://` 继续拒绝该 URI，所以审计行目前唯一的真实
    写入方是 control 面的鉴权）。

14. **M1 的 B 半：web 账户 + 授权已落到通用表上，M1 至此覆盖整个控制面**（§10 阶段
    C 4；**同样改在 grovefs 仓，引擎侧一行不用动**）—— **2026-11-20 已闭合**：
    grovefs `feat/treedb-read-errno` `245fd63`，12 文件 +1036/−233
    （`src/webauth/treedb_ctl.rs` 373 行新文件、`src/control_treedb.rs` 271 行新
    文件——把 A 半 `apikeys/treedb_ctl.rs` 里的行级原语抽出来共用于两家，该文件
    因此 −224 行；`webauth.rs` 143 行改动、`mcp.rs` +151、`server_ext.rs` 75 行
    改动，另四份文档）。

    形状：`grovefs_control__web_accounts` 6 列（`pw_hash` / `role` / `created_at`
    / `disabled_at` / `last_login_at` / **`rev`**）、`grovefs_control__web_grants`
    5 列（`account` / `base_view` / `perm` / `granted_at` / `granted_by`，主键
    `account\0base_view`，前两列 `COL_INDEX`）。14 个动词全部有引擎路径，三个 SQL
    分派器（`exec2`/`query2`/`changes2`）各补一条「忘了委派」的拒绝臂。

    三处计划纠正（前两条与 A 半同构，第三条是本轮唯一的架构改动）：
    ① `rev` 是 CAS 列而不是业务字段 —— 整行覆盖 + 登录 stamp 与管理员 disable
    两个写者 ⇒ 后写者会把陈旧的另一半写回，**登录把已禁用账户写回来是安全 bug**；
    输家拿 `EAGAIN`、重读后只重放自己那一列（8 次为限）。
    ② `grant` 是保住 `perm` 的 upsert（SQL 的 `ON CONFLICT DO UPDATE SET
    granted_at, granted_by`），整行覆盖会把 ro 授权悄悄升成 rw；`active_admins`
    是 `scan_eq(role,"admin")` + 客户端滤空，对齐那句跨列 `COUNT`。
    ③ **控制面对引擎树不可见，委派根本命中不了**：treedb 档 `rt.control` 的
    `has_physical` 只枚举本地 `meta/*.db`，引擎基树没有本地文件 ⇒
    `resolve_base` 恒答「neither a definition nor a tree」⇒ 账户能登录但管不了
    任何东西（`web grant add --base team` 三连红才露头）。新增
    `mcp::TreedbControlStore` 装饰器：17 个方法里 12 个定义动词委派
    `FileControlStore`，`list_physical`/`has_physical` 双答案（引擎侧走
    `Op::ListViews`），`store_for_db_path` 绑定，`server_ext` 按 `StoreKind` 路由。
    **控制面每个「存在性」判据都得先问「它看得见引擎吗」。**

    证据（当轮实跑）：`treedbctl` **61/0**（A 半 32/0），含「登录就地 stamp 引擎
    行」「引擎重启后账户与授权全在且 `--admin-user` 幂等」「bob 对授予的引擎基树
    `tree:true`、对未授予名字按名拒绝」；回归 318/0、55/0、39/0/10 不动；
    web/apikey/ctlplane/manager 四红是基线红（改动前日志读数相同）。

    边界：视图定义与 placement 仍永远在本地 ⇒ treedb 档是「定义本地 + 树在引擎 +
    行表在引擎」；`fork`/`import` 的 treedb 臂未动。

15. **M2：`grovefs control-migrate --to engine` 把本地控制面搬进通用表，搬完逐列读回
    比对**（§10 阶段 C 4 切换顺序的第二步；**改在 grovefs 仓，引擎侧一行不用动**）
    —— **2026-11-20 已落地**：grovefs `feat/treedb-read-errno`，10 文件 +823/−10
    （`src/control_migrate.rs` 421 行新文件，其余九份是两个 `treedb_ctl` 各自多出一段
    「迁移面」、`control_treedb::Fields::set`、命令面与文档）。

    形状：两种源文件（`<base>/meta/api_keys.db`、`<base>/meta/web_accounts.db`）→
    五张通用表。每表一条 `Move`，带「开表用的 `TableDef`、源 db、源 SELECT、列名表、
    键怎么算、要不要补种、写函数、回读函数」，后四样**全部从各 store 的 `treedb_ctl`
    借**（`KEY_COLS`/`ACCOUNT_COLS`/`tables()`/`put_*`/`dump_*`）—— 列序知识仍然
    只有一份，迁移侧一个列号也不抄；名字错了报错点名，本地文件少了后加的列
    （`secret_hint`/`expires_at`…）退化成空字节串，而不是让后面每列错位。

    三处不是直搬，理由就写在各 `Move` 旁边：① `api_keys.last_used_at` →
    `api_key_usage` 的一行（M1 把热写拆出去之后的形状）；② `mcp_calls` 的键是**算出来
    的**（`!ts_us || !seq`，本地表没有 id：RFC 3339 解析成微秒，同一微秒内按 `rowid`
    序自增 seq，否则两条同微秒的审计行会互相覆盖）；③ `web_accounts.rev` 要**补种**
    （本地账户行没有 CAS 计数器；种子是 `initial_rev()` = `bump(&[])`，唯一的用处是
    让搬过去的行与引擎自己新建的行逐字节同形，而不是让第一行特殊）。

    四条规矩（写在模块文档里，不是注释）：① **值原样搬** —— 不重算摘要、不重编码、
    不规范时间戳；② **幂等** —— 全部无条件 `RowPut`，重跑是覆盖，没有「已迁移」
    标记可与行本身失步；③ **先读回再收工** —— 每表比双向键集 + 逐列字节，不一致就
    非零退出（M3 不许建在半份拷贝上）；④ **按列名，不按列号**。

    两道守卫：开关已经开着就拒（`GROVEFS_TREEDB_CONTROL=engine` 时引擎是**活库**，
    覆盖 = clobber；M3 删掉开关后这一道改成「源已改名成 `*.pre-treedb` 就拒」，
    见第 16 条），两个源文件都不在也拒（没有东西可搬）。源库以
    `SQLITE_OPEN_READ_ONLY` 打开 —— 这条命令只能写引擎。**不删源文件**：回滚就是
    「把开关关掉」，吃掉源文件的迁移会把这条退路一起吃掉（删名留给 M3）。默认构建
    （无 `--features treedb`）里这个子命令**也存在**，只给一句具名拒绝，而不是让
    clap 回「unrecognized subcommand」。

    证据（当轮实跑）：`treedbctl` **88/0**（M1b 时 61/0）。第 9 段专跑这条命令：第二个
    空引擎 + 开关关着的 meta-server（控制面仍在本地）→ 铸一把 admin key、
    `view create team`（审计行因此是真的）、建 `carol` 账户并授权 → `migrated 12 rows
    across 5 tables`（1/1/7/2/1，等于 python3 直接数两个 SQLite 文件的总行数 —— 避免
    「跳过一张表也能平衡」）→ 重跑同读数（幂等）→ 四个拒绝（开关已开、`--to sqlite`、
    不给 `--meta-store`、给 `postgres://`）→ 开关打开重启后：**旧 admin key 仍能
    `view create`**（「摘要一字不动」的可观察后果）、`carol` 用**迁移前**的口令登录
    200、账户/授权/审计/usage 都在 → 引擎重启后全在，且安装期 `--admin-user` 不重建。

    边界：只搬行，**不搬视图定义**（引擎没有定义表，§10 C 4 末尾的边界），所以
    `grovefs mcp --meta-store treedb://` 仍按名拒绝该 URI；`fork`/`import` 的 treedb 臂
    也未动。服务进程要先停：源库是只读打开的，但活着的 SQLite 写者仍能在「读」与
    「读回」之间提交一行，那一行会被报成缺失。

    回归面（当轮实跑）：`cargo test --release` **318/0**、`test_treedb.sh` **55/0**、
    阶段 C 1 矩阵 **39/0/10**（与 M1 后逐项相同），整板 `run_all.sh` **GREEN** ——
    包括第 13/14 条里记为「基线红」的那四行（`web` 183/0、`apikey` 61/0、
    `ctlplane` 95/0、`manager` 41/0），可见它们与这两笔无关（**这句 GREEN 后来
    复现不了**：当日 06:03/06:28 两次整板已经是逐项同红，具体读数与原因见第 16 条
    的附注）。另记一次自己的误判：
    矩阵曾报过 24/15/10，当时同一个 `target/` 目录上还有另一支整板 `run_all.sh` 在跑
    `build`/`unit`；独占重跑两次都是 39/0/10，症状（挂载点全 `EIO`、mount 死掉）就是
    那次并发污染，不是回归。**测 treedb 套件时不要与整板并行。**

16. **M3：切写 —— 服务进程自己把旧文件改名退休，套件跟着改契约**
    （§10 阶段 C 4 切换顺序的第三步；**改在 grovefs 仓，引擎侧一行不用动**）
    —— **2026-11-20 已落地**：grovefs `e0085c9`（`feat/treedb-read-errno`，
    11 文件 +355/−178，主体是 `src/control_migrate.rs` 里的 `settle`，其余是
    删开关后的文档与两个 `treedb_ctl` 的 M3 说法）。

    开关删掉之后「选路」这个动作不存在了：treedb 档 `control_tables_uri()` 恒
    `Some("treedb://…")`，`ApiKeyStore::open_treedb(base, uri)` /
    `WebAuthStore::open_treedb(base, uri)` 都先跑 `control_migrate::settle(base, addr)`
    再开表。切写不是命令做的，是**进程做的** —— `control-migrate` 只写引擎、
    不改名；改名是「这个进程现在能服务了吗」的后果。两件事分开，「迁移完但还没
    重启」这个合法状态才存在。

    `settle` 的两个半边（都只在**打开控制面**时跑：没给 `--control-listen` 的
    meta-server 不建控制面 store，也就不碰这两个文件）：两个文件都不在 ⇒ 两次
    `stat` 返回（稳态零引擎流量）；在而引擎五张表全空 ⇒ **拒绝启动**，点名
    `grovefs control-migrate --to engine --meta-dir … --meta-store treedb://…`，
    并把「引擎 0 行、本地 N 行」写进拒绝文案；在而引擎有行 ⇒ 改名
    `<name>.pre-treedb`，目标名已存在则保留原文件 + 一行说明（绝不覆盖旧副本）。
    改名**只动主文件**：`-wal`/`-shm` 留在原名下 —— 回滚 `mv` 回去要配上「被服务时
    的那个库」，把伴生文件一起搬走，回滚拿到的是缺最后几次提交的版本。

    第三件事是诚实性：`settle` 逐表比较本地与引擎的行数，`local > engine` 时在
    改名**之前**按表点名差额（`… row(s) exist only in the copy about to be
    retired`）。行没丢，只是从活库退下去；不说出来就是静默丢行。

    旧变量不再读，但没被忘掉：treedb 档 meta-server 启动时 `retired_switch_note()`
    打一行 `GROVEFS_TREEDB_CONTROL=engine is no longer read (M3) … drop it from the
    unit file`。`control-migrate` 的第一道守卫也改了口：开关不存在了，判据换成
    「源已改名成 `*.pre-treedb` ⇒ 这台已切写」，拒绝里直接给出
    `mv <name>.pre-treedb <name>` 这条回滚。

    落地时的一条契约发现（套件先红后改）：`test_treedb.sh` §3 与矩阵都先用裸
    `--meta-dir` 铸 key、再去控制口用 —— M3 之后这个 key 在引擎里根本不存在，
    症状是 `authentication failed: unknown api key id`。**这不是回归**：控制面就是
    引擎，写它的 CLI 必须带同一个 `--meta-store`。三处铸 key 改到引擎后，§9 的
    授权四臂还露出更强的性质：控制面 store 是**启动时**打开的，所以「没 key /
    key 文件读不了 / key 不对」现在是**启动即拒**（rc 非 0、不打印 listening，
    并点出 `authentication required` / 变量名 / `handshake`），而不是第一次
    `view create` 才被拒 —— 断言因此从「调用被拒」升级成「起不来，且说得清
    为什么」。这条也写进了 USAGE：切写后 CLI 要带 `--meta-store`，忘了不会静默
    丢（下次打开控制面的进程会退休那个新文件并按表点名），但刚铸的 key 在那之前
    用不了。

    证据（当轮实跑）：`treedbctl` **111/0**（M2 88/0；新增第 9 段切写五步与
    第 1 段的「旧变量只打一行」）；`test_treedb.sh` **60/0**（M2 55/0；多出的 5
    条是 §9 四臂各自的「key 铸进引擎」与右臂的「控制口开了」）；阶段 C 1 矩阵
    **39/0/10** 逐项不动；`cargo test --release` **318/0**，`--features treedb`
    也是 **318/0**（顺手补了 `apikeys.rs` 单测缺的 `Backend::Treedb` 臂 —— HEAD
    上这一档本来就编不过）。

    附注（整板的真实读数）：`run_all.sh` 那次是 **RED**，红行是 migrate 73/1、
    fork 75/1、layout 80/2、apikey 56/5、ctlplane 89/4、web 178/5、manager（无
    summary）；treedb 三行（`treedb` / `treedbmatrix` / `treedbctl`）在默认构建里
    **SKIP**，因为要 `--features treedb` —— 三套的数字全部来自同一天单独实跑。
    这些红点与 M3 无关：06:03 与 06:28 两次整板（都早于任何 M3 改动）是逐项同读数，
    失败点分别是「远端派生挂载缺那句 note」、「`--register-with`/`--manager` 不
    存在」、「`apikey secret` 子命令不存在」、「`--audit-retention` 不存在」——
    都是套件先行的另一分支能力。第 15 条那句「整板 GREEN」在当时那次运行里成立，
    但在随后的整板上复现不了，以本附注为准。

17. **M4：四组专用控制面 op 从 treedb 内核删掉**（§10 阶段 C 4 切换顺序的最后一步；
    **改在 treedb 仓**）—— **2026-11-23 已落地，`v0.6.0`**。

    删的范围：14 个 `Op`（`Key*` 5 / `Audit*` 2 / `Def*` 4 / `Place*` 3，tag
    0x0040–0x0062）、9 个 `Reply`（0x0140–0x0161）、8 个 `Log` 变体（tag 11–18）、
    `State` 里的 `keys`/`audit`/`defs`/`place` 四张表与 `next_audit_seq`，以及它们
    在 `apply.rs`、`dump.rs`、`bench --op key`、CLI 打印里的全部分支。tag 区间
    **留空不回收**，旧 tag 落到 `unknown op/reply tag` 由
    `removed_control_plane_tags_are_unknown` 钉住。

    随之抬的三件套：`PROTOCOL_VERSION` 6 → 7（删帧不是纯追加，§4.1）、
    `SNAP_VERSION` 3 → 4（`refs` 之后抽掉 24 字节计数段；`SNAP_LEGACY_VERSION` = 3，
    v2 断链）、`DUMP_MAGIC` `TDBDUMP2` / `DUMP_VERSION` 2（容器头少一个 `next_audit_seq`，
    读侧 fail-closed）。迁移工具因此只认 v3 → v4：v3 那四段只要非空就拒（删表不删
    数据），错误串用 `control-migrate --to engine` 指出出路。

    前置：删前核过 grovefs 三个工作树（master `6cab7d6`、`feat/treedb-read-errno`
    `16184ca`、`feat/treedb-hardlink` `eee0927`）对那 14 个 op **零引用** ——
    L1 那条约束的落地面本来就已经在 M1–M3 搬到通用表上了。

    验证（当轮实跑）：默认档 `cargo test --release` **211/0**（1 ignored）、
    `--features postgres-compat` **242/0**、`cargo clippy --release --all-targets`
    无 error。新增用例：旧 tag 全拒（`model.rs`）、v3 非空四表拒迁（`mem.rs`）、
    迁移尺寸差 -24 B（`tests/migrate.rs`）；`benches/rehearse_migrate.sh` 的基线
    同步改成 `v0.5.1`、断言从 +12 B 改为 -24 B，并实跑一遍：12 条 ok / 0 FAIL
    （v3 目录 2 101 350 B → v4 2 101 326 B、回滚无损 39 696 → 39 696）。

18. **批量形状与同值消除：`TableClear` / `RowRangeDelete` / `TableRename` 三条新 op
    + `RowPut` 的空 payload 早返**（§5.1、§7.16；**改在 treedb 仓**）—— **2026-11-23
    已落地，`v0.7.0`**。

    起因是一个真实负载形状（用户原话）：「某些表随时更新替换内容或者删除，而且是
    频繁的」。逐行 `RowDelete` 在这个形状下每条都要付一次 WAL 帧加一次 unindex，
    而「整张表换掉」这件事的语义本来就只有「换掉」—— 所以把一批删除压成一条记录，
    把「重写回同一个值」压成零条记录。

    六处登记点全部改到（逐处对实码核对，不凭记忆）：`Op` 表与 `encode`/`decode`（tag
    `0x007c`–`0x007e`）、`is_readonly`/`is_control` 分派表（三条都算真写）、`translate`
    （各产一条 `Log`）、`Log` 表与 `apply`（25/26/27，**记区间不记键**）、dump 白名单
    （三条都不落 dump，§6.9）、文档与 `bench --op` 档位。**故意不动的三处**同样要记：
    `Reply` 表（复用 `Count`/`Ok`，仍是 23 个变体）、`EngineStats`（零新字段，同值消除
    靠 `ops_total − writes_total` 这个差值观测）、快照布局（`SNAP_VERSION` 仍 4）。

    版本判定：这一轮是 §4.1「纯追加」判据的**第六例** —— 加 op tag、不加 reply tag、
    不碰 `EngineStats`，所以 `PROTOCOL_VERSION` 停在 7、`DUMP_VERSION` 停在 2，
    `v0.6.0` 及更早的数据目录零迁移。旧新两侧各有一条兜底：旧服务端收新 op 在 decode
    处 `unknown op tag 0x007c`，旧副本收 `Log` 25/26/27 在 `Log::decode_depth` 报
    `unknown log tag` —— 两边都拒绝得点名，没有一条被静默当成别的样子。

    实测（`bash benches/bench_bulk.sh`，`dur=group`，破坏性负载按**固定工作量**而不是
    按时长量；逐格 `benches/results/bench_bulk.log`）：100 万行表删完，逐行
    **16 064 ms / 1 000 000 条记录 / 2 075 次 fsync / 38 MB** → `RowRangeDelete`（batch
    2000）**1 017 ms / 500 条 / 2 次 / 29.5 KB**（记录数省 2 000×、快 15.8×）→
    `TableClear` **924 ms / 1 条 / 1 次 / 25 字节**（省 1 000 000×、快 17.4×）；同值重写
    100 万次 **970 ms / 0 条 / 0 次 fsync**，真改值同形状 21 126 ms（17.9×，且 `seq` 根本
    不走）。200 k 档同一批脚本一起跑，比值一致（batch 1000 → 省 1 000×）。七条读法、
    含「时间为什么不随记录数同比例下降」（逐行 unindex 仍在提交线程上）在 §7.16。

    同一轮把「单进程是不是写瓶颈」也量了（`bash benches/shard_write.sh`，§7.17）：
    单实例有一个**与客户端压力无关**的天花板 —— `dur=never` 下把并发压力从 1 路加到
    8 路只拿到 **0.94×**，而服务端 CPU 从 4.30 涨到 8.39 核；按表切到 8 个进程实例则
    never **6.50×**、group **4.77×**。但分片不免费：每实例各自一个提交线程、各自备份、
    各自追副本，`Op::Batch` 只在一个实例内原子，而**路由完全在接入方**（§8 第 19 条）。

    验证（当轮实跑）：默认档 `cargo test --release` **223 passed / 3 ignored**、
    `--features postgres-compat` **254 passed / 9 ignored**、
    `cargo clippy --release --all-targets` 无 error 且未新增 warning（剩下的
    `useless_vec` 全在 `src/engine/sha256.rs` 的测试模块与 `tests/engine_crud.rs:345`，
    本轮之前就有）。新增 `tests/table_bulk.rs` **12 条**：原子性与撕裂重放、区间边界
    （空区间 / `max` 截断 / 未索引列 `EINVAL`）、`TableRename` 保 id 且被顶替表级联拆净、
    三条新 `Log` 幂等、同值消除零日志零 fsync 且三条闸门仍在消除之前生效、四种形状都
    过得了 `Checkpoint` 与 `dump → restore`。分类计数跟着抬：语料 41→44 条、
    真写 19→22（`tests/read_scaling.rs` 的 44/15/29 与三档 15/22/7 两侧同改）。
    另一笔与该用例相关的修法：`engine_crud.rs` 的
    `background_snapshot_costs_a_dispatch_not_a_pause` 原来拿单次采样当基线，机器被
    压满时 `thread::spawn` 自己就能花掉几十毫秒而报假红 —— 改成三次取最小值
    （§9.1 第 5 条），它与本轮改动无关，但混在同一轮里必须分开定性。

    接入侧跟版（2026-11-23，grovefs `feat/treedb-hardlink`）：pin `v0.5.1` → `v0.7.0`，
    只动 `Cargo.toml` / `Cargo.lock` 两行（lock 里 treedb 从 `0.5.0@e85e87b` 变
    `0.7.0@315e18e`，依赖仍是 `libc` + `thiserror`），适配层零源码改动 —— 这是「M4 删帧
    不影响这个接入方」的第二次实测。取证形状是**同一个 worktree、同一份源码、只换 dep**
    的两臂对跑，不然很易把工具不配算成回归：`cargo check --release --features treedb
    --all-targets` 0 error / 0 warning；`cargo test --release --features treedb` 两臂**同为
    321 passed / 0 failed / 4 ignored**（§10 阶段 C 1 那行记的 318/0/4 是另一棵树上
    的测量；同 pin 在本 worktree 实跑就是 321）；端到端板（真挂载文件系统 + 真引擎，53 条断言）两臂**同为 53 pass /
    0 fail**。板子的世代必须对得上：master 世代的 `.build-bin` 在这条分支上跑不起来（它
    的 `gfs_mgr` 要 `grovefs manager serve`，而这条分支的二进制没有 manager 子命令），
    旧 pin 臂同样红在同一条断言 —— 那是工具与分支不配，不是回归；换到 master 跟版时要重
    新看过。混代那一块见 §4.4 末的四臂表（不开认证时静默错读，不报错）。

19. **删除风暴的两个量具与一处锁语义修补**（§7.18，2026-11-25，**已做完**）。
    上一轮只回答了「哪一种批量删法省日志」（§7.16），没回答「删完之后状态是否真的
    收口」。本轮一次补了三件：

    ① **通用表删除量具** `benches/delete_storm.sh` —— `rowdelete`（N 行删尽）与
       `churn`（2N×cycles 删后重插）两档，固定工作量、自动 checkpoint、`VmRSS`/`VmHWM`
       采样、停服 `rounds/worst`，并在自动快照后写两条终态不变的尾巴、SIGKILL、
       重开逐行核对内容（10 万行 **errors=0**）。
    ② **POSIX 删除量具** `benches/posix_delete_storm.sh` —— 真 `Unlink`/`Rmdir` 20 万 op、
       60 万条记录、10 万个内容引用释放；自动档开/关各跑一遍（开：checkpoint 0 → 21、
       WAL 峰值 4.98 MB 回落 234 KB；关：checkpoint/水位/WAL 三样全不动），重开后再逐
       ref 核对 10 万个全部归零，四本账收敛到基线 `(1,0,0,0)`。
    ③ **一处真 bug**：`Unlink`/`Rmdir` 删到 inode 的最后一个名字时不清 `locks` 表，
       而 `LockSet` 的释放入口先查 inode —— 永久锁因此在 inode 消失后再也无法被客户端
       释放（过期 sweep 也不碰 `lease_expiry == 0`）。现在按 `(view, ino)` 前缀级联
       `Log::DelLock`，**不动协议也不动盘上格式**（无新 op/reply/`Log` 变体，
       `PROTOCOL_VERSION` 8 / `SNAP_VERSION` 4 不变，旧目录零迁移）。

    七张引用表全部由 `benches/doc_table_check.py` 从两份 TSV 重算，它自带 10 处注入
    自测（三处在 §7.13、三处在通用表、四处在 POSIX）—— 同一类「表看着像真但手撮拼
    出来」的错（§7.13）不允许再犯一次。第 20 轮把这张表扩到 14 张（再加 §7.9 那两张）、注入扩到 19 处。

    **本轮没做的事（不是做到一半，是没开工）**：`Rename` 覆盖目标与 `Symlink` upsert
    两条同类路径的锁收敛未审计；allocator 级碎片、跨机械盘/SSD 的绝对延迟、10 万行
    POSIX 在 SIGKILL 下的恢复（只有小规模真进程回归）、以及混合读写下万级视图的并发
    曲线，全部**未实测**。这五项里只有「混合读写下万级视图的并发曲线」由第 20 轮量出
    （§7.19），其余四项仍留在 §7.18 末的边界里。

20. **万级混合视图量具，外加一次驻留内存的证伪**（§7.19，2026-11-25，**已做完**）。
    第 19 轮末尾欠着一条「混合读写下万级视图的并发曲线，未实测」，本轮补上，顺带把
    「retained RSS 不是泄漏证明」往前推一步。四件产出：

    ① **bench 两档**：`--bulk mvseed`（N 视图 × W 文件，固定 `2N+3NW` 条记录、
       `N+NW` 次写）与 `--bulk mvmix`（每组 2 `Lookup` + 1 `ReadDir` + 1 `Create` +
       1 `Unlink`，固定 `5G` records / `2G` writes 且净零）。一组必须整批进同一个
       pipeline —— `src/net/multi.rs:549` 的 `has_write` 判据保证含写批交给唯一提交
       线程按序执行，所以 `PIPE` 必须是 5 的倍数（脚本先验这一条）。
    ② **矩阵量具** `benches/many_views_mix.sh`：`views ∈ {100, 1 000, 5 000, 10 000}`
       × `conns ∈ {8, 64}` 八格，每格两个服务端生命周期（种子停服 → 同目录重开 →
       测量），自动 checkpoint、`VmRSS`/`VmHWM` 采样、停服 `rounds/worst`、46 列 TSV。
       八格全绿：`ops == records == 5G`、`writes == lookups == 2G`、
       `readdirs == creates == unlinks == G`、`views_touched = N`、
       `nodes/dentries` 回到种子态、`errors = 0`。读数：8→64 连接吞吐 ×2.25–3.63，
       p50 涨 4.6–8.9× 而 p99 只涨 1.18–1.89×（每次 fsync 从摊 134–144 个写涨到
       1 266–1 274 个）；视图数从 100 到 10 000 不构成结构性代价，但单轮抖动 ±10 %
       量级，故不据此排衰减次序。
    ③ **一次证伪**：矩阵里 10 000 视图档 `retained_delta_kb` 是 146 976 / 158 208，
       看着像「万级视图的混合读写驻留 150 MB」。单格 A/B（同机同参数只切
       `--auto-checkpoint-mb`，靠 `OUT=`/`TSV=` 落到处而不覆盖正式矩阵）把它否了：
       关闭档五秒内零快照、WAL 一路涨到 15 260 550 B，`VmRSS` 只涨 **1 796 KiB**；
       开启档涨 3 572 KiB、窗口末回到 104 MB 量级，而采样峰值 247 624 KiB —— 那
       150 MB 是「编码一次 21 万节点 / 20 万目录项的快照」的瞬时缓冲，矩阵的收尾
       采样正吃在编码窗口里（`rss_after_kb == rss_peak_kb`）。顺带量到 1 MiB 档自动
       checkpoint 的价格：49 890 → 40 646 ops/s（**−18.5 %**）、p99 643 → 1 060 µs、
       服务端最坏回合 42 039 → 147 799 µs。
    ④ **关掉一类测试假红**：`tests/common/mod.rs::free_addr()` 原来是「`bind :0` 拿号
       → `drop` → 交给服务端去占」。那有两个 check-then-use 窗口 —— 同进程两个并发用例
       可以先后拿到同一个号；任何一次 `connect()` 也可以在服务端真的 `bind` 之前把这个号
       当源端口用掉（监听端口与源端口是同一片地址空间）。撞上的表现固定：输的那个服务端
       `FATAL: bind ...: Address already in use`，赢的那个还替对方接住了客户端，一次撞号
       红两条用例。本轮两次全量各撞一次 —— 默认档红 `net_protocol` 的
       `generic_table_and_readdir_plus_over_tcp`（日志明写撞号）与
       `fragmented_request_is_reassembled`（`BrokenPipe`），`postgres-compat` 档红
       `delete_storm` 两条，而同一份树换个档跑就全绿。改成只从 `ip_local_port_range` 之外
       发号 + 本进程内原子计数 + 发号前探活（§9 的「端口是怎么分的」）。这是测试脚手架，
       不动引擎、不动协议、不动盘上格式。

    ⑤ **一次复跑与一个矛盾的定性**：`benches/sync_ratio.sh 3` 在最终代码上重跑，新块追加进
       `benches/results/sync_ratio.log`。`always` 两格读到 60.50 / 55.64 写/fsync，而 11-19 旧块
       恒为 1.00 —— 这不是脚本坏、也不是回归：`bef5730`（11-19 09:03Z，旧块采于 06:17Z）把
       `Always` 的按连接串 fsync 删了，两个档自此共用同一个提交点。于是「已修」这句话在
       当前 HEAD 上有了可回对的读数（机械盘 9 823 ops/s、p99 32.8 ms、498 次 fsync 摊
       30 129 次写，§7.9 末的复跑表）。同一块里根盘 `group` 68 758 → 111 847 ops/s，这一格
       本轮没有受控 A/B，**不作归因**，只报数。

    验证（当轮实跑）：新增 `tests/mixed_views.rs` **5 条**全绿（0.79 s，五条都起真服务
    端进程）；默认档 `cargo test --release` **242 passed / 0 failed / 3 ignored**、
    `--features postgres-compat` **273 passed / 0 failed / 9 ignored**。
    `benches/doc_table_check.py` 从 394 行扩到 828 行：§7.19 的四张矩阵表 + 一张探针表按
    `(auto_mb, views, conns)` 三键逐格回对、带 24 条不变量，§7.9 的两张按**块时间戳**锚定
    `sync_ratio.log` 逐格回对，注入自测从 10 处增到 **19 处**全部被拦下。协议与盘上格式零改动（无新 op/reply/`Log` 变体，
    `PROTOCOL_VERSION` 8 / `SNAP_VERSION` 4 不变，旧目录零迁移）。

    **本轮没做的事**：allocator 级碎片、跨机械盘/SSD 的绝对延迟、10 万行 POSIX 的
    SIGKILL 恢复、`Rename`/`Symlink` 覆盖路径的锁收敛（四条仍沿用第 19 条的边界）；
    本节自己新留下的则是十万级视图、每视图更多文件、混合读写下的 SIGKILL 恢复、
    `--auto-checkpoint-mb` 的摊薄曲线、视图数轻微衰减的配对复采 —— 均**未实测**，
    逐条写在 §7.19 末。

21. **grovefs 控制面迁移：引擎侧的前置已复核完，剩下的全是跨仓动作**（§10 阶段 C 4 末，
    2026-11-25）。验收口径原话是「选定集成分支 pin `v0.7.0`；`control-migrate` 可用；
    未迁移部署拒绝启动并点名命令；密钥/审计/账户确实写入引擎通用表；旧 SQLite 退休；
    矩阵保持 `51/0/0`；容灾演练保持全绿」。这一轮把**引擎侧能独立落地的那部分**做完并
    实测，三条结论：

    - **「只把 pin 往上提」值多少代码：零。** 同一份 grovefs 源码、同一个 worktree，
      `v0.3.6` → `v0.8.0` 只改 `Cargo.toml` 那行 tag（lock 跟着改 version/source），
      check 两臂 0 warning / 0 error、test 两臂 **318/0/4**、六条端到端套件的六个三元组
      逐行相同；而那个锁修补是 `src/engine/apply.rs` 上的 20 行，不新加 op、不新加
      `Log` 变体。量具 `benches/grovefs_pin_ab.sh`，原始输出
      `benches/results/grovefs_pin_ab.log`，§10 补段那两张表由
      `benches/doc_table_check.py` 逐格回对，并校三条不变量：两臂三元组相等、六条套件
      齐备、混代闸门的点名行确实在日志里。
    - **但 pin `v0.8.0` 吃不到最后一笔修补。** tag `v0.8.0` 标注的是 commit `f9a385c`，
      §7.18 那笔「POSIX 最后一个名字删除时级联清锁」（`c455793`）在 tag **之后**（当时
      HEAD 在 tag 之后共 3 笔：`e013701`、`c455793`、`613c1f9`）—— 升到 `v0.8.0` 等于领
      一个**没有**那个锁修复的构建。所以跨仓第一件要做的不是改 grovefs，而是**再发一个
      tag**（即下面待办 1，本笔已发）。
    - **混代是硬失败，而 grovefs 的板子没有世代门。** §4.4 的闸门是精确等号（7↔8 与
      8↔7 都拒），旧 pin 客户端接新引擎实测 `33 / 81` 与 `0 / 1 / 0`，点名 `errno 68`；
      六条套件的 `TREEDB_BIN` 默认又指向本仓 HEAD 的构建，于是「分支坏了」是误读。

    剩下五条里第一条是引擎侧的发布动作（本笔已做），后四条在 grovefs 仓或运维面，
    逐条写清验收判据：

    1. ~~**发 `v0.8.1`**~~ **已发**（本笔发布提交即 `v0.8.1`，把 tag 之后那三笔
       `e013701` / `c455793` / `613c1f9` 圈进来）。判据逐条实测：新 tag 上
       `PROTOCOL_VERSION` 仍 8、`SNAP_VERSION` 仍 4、`DUMP_VERSION` 仍 2（那三笔只动
       `src/engine/apply.rs` 的 20 行锁级联与测试 / 量具 / 文档，三个版本常量一行未改
       ⇒ 不跨代，接入方不需要为重编两遍买单）；`Cargo.toml` 的 `version` 跟到 `0.8.1`，
       保持上面《发布标记与 crate 版本》那句逐版对齐；两远端都显式推了 master 与 tag。
       于是 grovefs 侧有了钉得住的目标：**该 pin `v0.8.1` 而不是 `v0.8.0`**（后者拿不到
       §7.18 的锁修复）。
    2. **选出集成分支**。判据：**同一个 ref** 上 `src/control_migrate.rs` 与 `Op::Link`
       两项都是「有」（今天是一个分支各一半），且它的 `Cargo.toml` pin = 第 1 条那个
       `v0.8.1`。在这之前「矩阵保持 `51/0/0`」这句话没有可测对象 —— 迁移分支今天只能跑出
       `39 / 0 / 10`，那 10 条硬链接断言全部 SKIP（同一套断言在硬链接分支上是
       `51 / 0 / 0`，见阶段 C 1）。
    3. **给 grovefs 那六条端到端套件加引擎世代门**（改在 grovefs 仓的 `.build-bin/`）。
       判据：引擎 `--stats` 报的 `proto_version` 与客户端 pin 不同代时，套件打 SKIP 并
       点名两侧版本号与「请先起同代引擎」，而不是抛 81 条红；沿用既有的 pass/fail/skip
       三计数器与「自跳闸必须可见」两条规矩。
    4. **把版本窗口写进部署文档**（grovefs 仓）。判据：`USAGE`/README 里有一句明写
       「`client_proto == PROTOCOL_VERSION` 精确相等，7↔8 与 8↔7 都拒，没有混代滚动
       升级窗口」，并给出升级顺序（先起新引擎 → 客户端换 pin 重建 → 旧客户端进程必须
       重启，不接受新旧客户端并存）。
    5. **grovefs 主检出的分叉与 WIP 先收口**（跨仓协作项，别叠在别人的改动上）。实测
       现状：本地 `master` 落后 `origin/master` 9 笔、独有 1 笔；主检出 48 个未提交文件，
       最新一笔 mtime 2026-11-25 12:37:29Z。判据：这三项归零之后再谈 pin 提交，否则
       「pin 升了」这个结论落不到一个可复现的 commit 上。

    **本轮没做的事**：不改 grovefs 任何受版本控制的文件、不推任何 pin、不在主检出上构建
    （只在临时 worktree 上临时改 tag，脚本退出即还原 `Cargo.toml`/`Cargo.lock`）；
    `feat/treedb-hardlink`（pin `v0.7.0`）跨到 `v0.8.0` 这一格**未实测** —— 它欠的是同
    一行 tag 的改动，但那一半的接入代码（真发 `Op::Link` 的 provider）不在我复现的那棵
    树上，跑出来的三元组也不能替集成分支背书。
