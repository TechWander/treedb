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

cargo test --release                                    # 默认档 71 passed（引擎本体）
cargo test --release --features postgres-compat         # 102 passed（含 PG 过渡档）
bash benches/throughput_matrix.sh 3 1 2 3 4 5           # 五轴复现，输出进 benches/results/
```

`--workers N`（N > 1）把只读 op 摊到 N 条前端线程上并行；写永远由唯一提交线程定序，
group commit 不丢。默认 `--workers 1`，历史基线数字都在这一档下测的。

## 实测（同一台机器、同一块盘，2026-11-19 复跑）

| 轴 | 结果 |
|----|------|
| 连接 | 单进程 10 000 常驻连接：RSS +6 MiB、**0.56 KiB/conn**、`fd/conn = 1.0000` |
| 写（`dur=group` vs PG `synchronous_commit=on`，WAL 同盘） | 64 并发 `create` **7 844 vs 1 441 ops/s**（PG 那一格 p50 = 33.6 ms） |
| 每 op 一次 fsync（`dur=always`） | 1 586 vs 1 441 ops/s = **1.1×** —— 领先全部来自跨连接合并 fsync，不是来自语言或内存布局 |
| 64 并发纯读 | `workers=1` 被 PG 反胜 3.6–7.5×；`workers=16` 反超 2.0–3.8×（`lookup` 792 k vs 207 k ops/s，p50 37 µs） |
| 持久性 | `kill -9` 六个崩溃用例零丢失（含 checkpoint 瞬间崩、连崩 3 轮） |

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
| `src/bin/` | `treedb-server` / `treedb-bench` / `treedb-pgbench`（对照工具）/ `treedb-demo` |
| `tests/` | `engine_crud` `net_protocol` `read_scaling` `crash_recovery` `scale_connections` `independence`（默认档全部要跑）；`concurrency` 属过渡档 |
| `benches/` | 五轴矩阵脚本与历史/复跑原始输出 |
| `DESIGN.md` | 选型判据、协议字节布局、语义边界、五轴实测、已知限制、接入路径与下一步 |

## 许可

Apache-2.0。
