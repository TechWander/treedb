# treedb

面向「大量小对象 + 高频点查 + 目录遍历 + 万级常驻连接」这一负载形状的**元数据引擎**：
单进程 epoll 事件循环、自定义二进制线协议、内存状态机 + CRC32 WAL + 快照 checkpoint。
POSIX 元数据语义，客户端无关 —— 它是一个数据库项目，不是某个客户端的一个模块。

> 为什么不用现成关系库：这个形状下，每条连接要 fork 一个 backend（实测 ≈14 MiB，
> 万级连接就是百 GiB 量级），每次写要付一次 WAL flush。treedb 侧每连接只有一个
> fd（0.56 KiB RSS）和一次跨连接合并的 fsync。判据、代价与被否方案在
> [DESIGN.md](DESIGN.md) §2。

## 快速开始

```sh
cargo build --release                                   # 默认档：依赖只有 libc + thiserror
./target/release/treedb-server --dir /var/lib/treedb --listen 127.0.0.1:7777
./target/release/treedb-bench --op lookup --conns 64 --pipeline 32 --secs 3

cargo test --release                                    # 默认档 74 passed（引擎本体）
cargo test --release --features postgres-compat         # 105 passed（含 PG 过渡档）
bash benches/throughput_matrix.sh 3 1 2 3 4 5           # 五轴复现，输出进 benches/results/
bash benches/sync_ratio.sh 3                           # 写路径机制：一次 fsync 摊了多少写
bash benches/ab_layout.sh <基线 rev>                     # 受控 A/B：只换服务端二进制比引擎改动
bash benches/default_check.sh                           # 零旗标默认档同格跑 5 次，验默认值真的生效
bash benches/ab_durability.sh <基线 rev>                  # 写轴 A/B：只换服务端二进制比持久化档
./target/release/treedb-micro --items 32000             # 纯计算档（不经网络/不写 WAL）+ 分配计数
```

`--workers N` 把只读 op 摊到 N 条前端线程上并行；写永远由唯一提交线程定序，
group commit 不丢。默认按核数给：`min(8, 核数/2)`（1–3 核机器算出来仍是 1，与旧
默认逐字一致）；`--workers 1` 是历史基线档。为什么不再默认单线程、以及它的三个
代价，见 [DESIGN.md](DESIGN.md) §8 第 1 条。

## 实测（同一台机器、同一块盘，2026-11-19 口径修正后重跑）

| 轴 | 结果 |
|----|------|
| 连接 | 单进程 10 000 常驻连接：RSS +6 MiB、**0.56 KiB/conn**、`fd/conn = 1.0000` |
| 写（`dur=group` vs PG `synchronous_commit=on`，WAL 同盘） | 64 并发 `create` **10 386 vs 1 837 ops/s**（**5.7×**，PG 那一格 p50 = 33.6 ms）、`write` **12 753 vs 1 415**（**9.0×**） |
| 64 并发纯读（默认档 `workers=8`） | `readdir` **427 092 vs 146 203** ops/s（**2.9×**）、`lookup` **847 297 vs 210 868**（**4.0×**）、`ping` **977 771 vs 531 437**（**1.8×**）；p50 62–112 µs vs PG 117–431 µs |
| 同一格只给一个核（`workers=1`） | `readdir` 50 435、`lookup` 101 098、`ping` 136 496 —— 输给 64 个 PG backend 是**并行度**差异，不是语言差异：`/proc` 计 CPU 显示这一档服务端稳定 100 % 单核（`benches/probe_srv.sh`）；独立复跑 5 次与本行差 ±3 % |
| 零旗标默认档（照上面两行命令起服务，同格跑 5 次取中位） | 服务端 banner 就是 `workers=8`：`readdir` **654 983**（= PG 4.5×）、`lookup` **1 062 564**（**5.0×**）、`ping` **1 016 868**（1.9×）—— 领先不靠调用方传 `--workers`；矩阵连跑的那两行是**下界**（`benches/default_check.sh`） |
| 为什么快（引擎自己的计数器） | 两个「会 fsync」的档位现在共用同一个提交点：64 并发下一轮只付 1 次 fsync、摊 **≈ 55–58 次写/fsync**（`benches/sync_ratio.sh`） |
| 每 op 一次 fsync（`dur=always` vs PG `synchronous_commit=on`，WAL 同盘） | 改前 1 891 vs 1 469 ops/s（p99 81.5 ms vs 157 ms）；**改后 10 913 vs 1 469 = 7.4×**，p50 优 17×、p99 优 5×（受控 A/B：`benches/ab_durability.sh v0.3.1`）。旧版这句「刨掉合并 fsync，treedb 对 PG 就没有结构性优势」是当时那个**按连接串 fsync** 的实现留下的错判，已作废：两家都是靠一次 fsync 摊多次写，treedb 在这一格摊得动（≈ 55）而 `always` 档以前自己把它抵消掉了 |
| 持久性 | `kill -9` 六个崩溃用例零丢失（含 checkpoint 瞬间崩、连崩 3 轮） |

两个读数字的历史口径要提醒：旧版这张表写的是「64 并发纯读被 PG 反胜 3.6–7.5×」，
**已作废** —— `treedb-bench` 的分母把「建连 + 预热」算进去了，而 PG 侧对照器没算，
两边口径不对称（[DESIGN.md](DESIGN.md) §9.1 第 3 条）。现在两个客户端同口径，
而引擎改动的收益只用 `benches/ab_layout.sh` 的受控 A/B 说话。

数字全部落在 161 TB 机械 XFS 上，报的是倍率区间而不是单格小数的第六位；逐格原始
输出在 [benches/results/matrix.log](benches/results/matrix.log)，读法与陷阱在
DESIGN.md §7（含 §7.8 那次全五轴复跑）。

## 独立性策略

- 引擎内核 = `src/engine/` + `src/net/`。它不允许携带任何具体客户端的业务语义，
  也不允许把客户端的依赖塞进默认构建图。这条由 `tests/independence.rs` 三把门守着：
  内核目录不出现客户端名字、`[dependencies]` 非 optional 项只能是 `libc` + `thiserror`、
  `postgres-compat` 不得进 `default`。
- 为某个具体客户端服务的适配层，留在**那个客户端自己的仓库**里。例如 GroveFS 的接入
  （`--meta-store treedb://host:port`、把 op 映射成 `MetaProvider`、GC 线索的处理）全部
  在 grovefs 仓内，且 behind 一个独立 cargo feature。
- `postgres-compat`（默认关）是本仓保留的**过渡脚手架**：PG 共享池、SQLite 镜像、
  控制面表形。它是搬迁工具，不是引擎的一部分；迁完即可整体删除。

## 目录

| 路径 | 内容 |
|------|------|
| `src/engine/` | 线协议类型（`model.rs`，37 个 op / 20 个 Reply）、帧编码（`codec.rs`）、状态机（`mem.rs`）、应用与定序（`apply.rs`）、WAL/快照（`wal.rs`）、核心（`core.rs`） |
| `src/net/` | 单线程事件循环（`server.rs`）、多前端 worker + 唯一提交线程（`multi.rs`）、跨线程信箱（`mail.rs`）、阻塞客户端（`client.rs`） |
| `src/{pool,store,persist,schema,probe}.rs` | `postgres-compat` 过渡档 |
| `src/bin/` | `treedb-server` / `treedb-bench` / `treedb-pgbench`（对照工具）/ `treedb-micro`（纯计算 + 分配计数）/ `treedb-demo` |
| `tests/` | `engine_crud` `net_protocol` `read_scaling` `crash_recovery` `scale_connections` `independence`（默认档全部要跑）；`concurrency` 属过渡档 |
| `benches/` | 五轴矩阵脚本与历史/复跑原始输出 |
| `DESIGN.md` | 选型判据、协议字节布局、语义边界、五轴实测、已知限制、接入路径与下一步 |

## 许可

Apache-2.0。
