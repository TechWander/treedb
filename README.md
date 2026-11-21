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

cargo test --release                                    # 默认档 172 passed（引擎本体）
cargo test --release --features postgres-compat        # 203 passed（含 PG 过渡档）
bash benches/throughput_matrix.sh 3 1 2 3 4 5           # 五轴复现，输出进 benches/results/
bash benches/sync_ratio.sh 3                           # 写路径机制：一次 fsync 摊了多少写
bash benches/single_conn.sh 3                          # 单连接对标 PG（$PIPE=32 走批量档、$CONNS/$WORKERS 走并发档）
bash benches/backup_probe.sh 2                         # 备份/还原三形状实测（冷拷贝 / 热拷贝 / 副本目录）
bash benches/ab_layout.sh <基线 rev>                     # 受控 A/B：只换服务端二进制比引擎改动
bash benches/default_check.sh                           # 零旗标默认档同格跑 5 次，验默认值真的生效
bash benches/ab_durability.sh <基线 rev>                  # 写轴 A/B：只换服务端二进制比持久化档
bash benches/rehearse_migrate.sh v0.3.2                 # 快照升级真机彩排：旧 tag 的二进制 → 迁移 → 回滚
bash benches/handshake_cost.sh 20000                   # 接入握手的代价：两臂同轮 A/B（建连 + RSS/conn + 稳态）
bash benches/bench_rows.sh                             # 通用表七条路径（rowput/rowget/rowlist/rowidx/…）× 三形状
./target/release/treedb-micro --items 32000             # 纯计算档（不经网络/不写 WAL）+ 分配计数
```

数据目录是从旧快照布局（`SNAP_VERSION` 2）上来的，新二进制会拒收 —— 那不是 bug，
而是不静默按错布局解字节的故意选择。出路随包安装：

```sh
./target/release/treedb-migrate --dir /var/lib/treedb          # 只看，不写；exit 10 = 需要迁移
./target/release/treedb-migrate --dir /var/lib/treedb --apply  # 只重写 state.snap，WAL 不动
```

回滚是把自动落下的 `state.snap.v2.bak` 拷回去再跑旧二进制 —— 但无损只维持到新二进制
做过第一次 checkpoint 之前（同步 checkpoint 会 `wal.reset()`，把上一代日志物理抹掉）。
四条承诺、回滚窗口与为什么不在启动路径上开「自动兼容读」的口子，见
[DESIGN.md](DESIGN.md) §6.5；`bash benches/rehearse_migrate.sh v0.3.2` 拿真旧二进制
把整条链（造旧目录 → 迁移 → 开门 → 回滚）复算一遍。

要把一份数据搬到别的机器/别的目录，用逻辑 dump（停服后两侧都独占目录）：

```sh
./target/release/treedb-dump --dir /var/lib/treedb --out /backup/treedb.dump
./target/release/treedb-load --in /backup/treedb.dump            # 只体检：解码 + 打规模
./target/release/treedb-load --in /backup/treedb.dump --dir /srv/treedb-new
```

**不想停服就用 `treedb-backup`**：它从在线主拉一份一致切面（`SnapFetch` + `WalFetch`
两条只读 op，与副本同源），落成与 dump 完全相同的文件，`treedb-load` 直接吃得下。
主端零改动，也不需要任何配置：

```sh
./target/release/treedb-backup --primary 10.0.0.1:7777 --out /backup/treedb.dump
./target/release/treedb-backup --primary 10.0.0.1:7777 --out - | gzip > /backup/t.dump.gz
```

切面就是「第一次拉 WAL 那一刻主已确认的最大 seq」（RPO 可据此报）；主在备份期间
checkpoint 追过了头会整份重来（`--retries` 默认 3）。仍是整份一次性导出，不是持续
追赶 —— 要镜像用 `treedb-standby`（§6.10）。

dump 走的是日志标签表（`Log`，1..=22）而不是内部快照格式，所以它跨版本、跨机器能读；
容器自带 CRC32 与三个 `next_*` 计数器。load 只写 `state.snap`（水位 = dump 的 seq）、
不写 WAL，目标目录必须为空。导出是确定性的：`dump → load → dump` 字节相同。
格式与实测（82 888 节点：dump 279 ms / 11.65 MB，还原快照与源逐字节相同）见
[DESIGN.md](DESIGN.md) §6.9。

**运维面两个旋钮**（都不碰协议：`PROTOCOL_VERSION` 仍 6，`Op` 表一条没加）：

```sh
# 1) 连接上限：防「一条不守规矩的客户端把 fd 吃光」。默认 0 = 不限。
#    超额的连接会被 accept 后立刻关掉（客户端看到 EOF）；日志限速（第 1 条 + 每 1000 条），
#    总数在停服汇总里：accepted 14 refused 66 peak 8
./target/release/treedb-server --dir /var/lib/treedb --max-conn 2000

# 2) 机器可读指标：27 个，直接进 Prometheus / 采集脚本
./target/release/treedb-server --listen 127.0.0.1:7777 --stats --format prometheus
#    treedb_up 1 / treedb_views … / treedb_ops_total … / treedb_replica_lag …
```

抓不到服务时 `--format prometheus` 仍会输出 `treedb_up 0` 三行并以**退出码 1** 退出
（面板上是 down，不是 no data）；参数错一律退出码 2 且不产出字节。`_total` 的语义是
**进程启动以来**，率在面板侧用 `rate()` 算；副本专用项在主上输出 0，面板要过滤
`treedb_replica == 1`。口径与实测（含 `src/metrics.rs` 的三条纪律与一个顺带修掉的
停服 ack bug）见 [DESIGN.md](DESIGN.md) §7.14，复现跑 `bash benches/observe.sh`。

只读副本是另一个二进制，它不需要主做任何配置：

```sh
./target/release/treedb-standby --primary 10.0.0.1:7777 \
    --dir /var/lib/treedb-replica --listen 0.0.0.0:7778
./target/release/treedb-server --listen 127.0.0.1:7778 --stats   # 多打一行 replica = yes (…)
```

目录里有什么就先用什么：冷启动先向主要一份快照，之后按 seq 追 WAL；追上之前对读的
答案是「空库」而不是报错。写拿 `EROFS`，主挂了副本照答读。拉取式、异步、
无读己之写 —— 边界与没做到的四条见 [DESIGN.md](DESIGN.md) §6.6。

切主是**两步**，中间那一步是这个命令（v0.4.0）：

```sh
# 1. 停掉旧主（fencing 挡不住旧主对自己的客户端服务，必须真停）
./target/release/treedb-standby --dir /var/lib/treedb-replica --promote
#    promoted /var/lib/treedb-replica: fence 0 -> 1 (snap_seq 412, 4180 wal recs replayed)
# 2. 把提示出来的命令跑起来，把客户端地址改过去
./target/release/treedb-server --dir /var/lib/treedb-replica --listen 0.0.0.0:7777
```

`--promote` **不开服务**，只把目录里那个 `fence` 文件抬一格然后退出 —— 什么时候
开新主、客户端什么时候改地址，不该被一条命令隐式决定。旧主交来的批会被副本按
`fence` 拒掉（日志里 `is fenced out`，`replica_errors` 跟着涨），所以拿错主是
看得见的而不是静默回滚。它与 `--primary` 不能同时给（一个目录既追别人又当主
是自相矛盾的，退出码 2）。边界同见 §6.6「切主」：不自动选主、旧主要人停、
令牌不保护从未追过新主的副本。

`--workers N` 把只读 op 摊到 N 条前端线程上并行；写永远由唯一提交线程定序，
group commit 不丢。默认按核数给：`min(8, 核数/2)`（1–3 核机器算出来仍是 1，与旧
默认逐字一致）；`--workers 1` 是历史基线档。为什么不再默认单线程、以及它的三个
代价，见 [DESIGN.md](DESIGN.md) §8 第 1 条。

接入认证是**可选的、默认关**。要给出入口上锁，就配一把 32 字节随机密钥，三个
入口共用同一个 flag：

```sh
head -c 32 /dev/urandom > /etc/treedb/auth.key && chmod 0600 /etc/treedb/auth.key
./target/release/treedb-server  --auth-key-file /etc/treedb/auth.key ...   # banner 多一位 auth=on
./target/release/treedb-standby --auth-key-file /etc/treedb/auth.key ...   # 一头对主握手，一头要读者握手
./target/release/treedb-bench   --auth-key-file /etc/treedb/auth.key ...   # 不带密钥的客户端会被 EADV 拒掉
```

它是共享密钥的双向挑战握手（两次往返，密钥从不上线），**不是 TLS**：只建立身份，
不建立机密性，也不做授权。带密钥的客户端遇到没开认证的服务端会直接报错退出，
不静默降级；反过来不带密钥的客户端面对开了认证的端口，每个数据 op 拿 `EADV`。
没配密钥时线协议与 v4 逐字相同（§4.1 的 `4 → 5`）。

代价量过了（`bash benches/handshake_cost.sh 20000`，20 000 条连接、两臂同轮 A/B
独立跑 3 次，逐格原始输出 `benches/results/handshake_cost.log`）：常驻内存
**+93 ~ 96 B/conn**（每连接一个定长 `Gate`，握手做完也不还），建连时间中位
**+90.9 µs/conn**（两次往返 + 两次 HMAC；但这一列噪声比效应还大 —— 不开认证的
那一臂自己三次就摆 27 %，所以**单次读数不作数**）；稳态读与写与未开认证同量级
（握手不进引擎、不占提交线程，group commit 的合并度一字不变）。形状、实现层次与
四条「防不住什么」见 [DESIGN.md](DESIGN.md) §4.4。

## 实测（同一台机器、同一块盘，2026-11-19 口径修正后重跑）

| 轴 | 结果 |
|----|------|
| 连接 | 单进程 10 000 常驻连接：RSS +6 MiB、**0.56 KiB/conn**、`fd/conn = 1.0000`。v0.3.5 重跑到 **25 000 条仍全绿**：RSS +8 MiB（**0.31 KiB/conn**）、`fd/conn = 1.0000`、`ping` p50 23 µs / **p99 65 µs**（逐格原始输出 `benches/results/scale_connections.log`）；再往上到 50 000 败在**客户端源端口耗尽**（`ip_local_port_range` 只有 28 232 个，`EADDRNOTAVAIL`）而不是引擎 —— 服务端 `RLIMIT_NOFILE` 1 048 576 没用满，引擎也没任何连接数上限常量（DESIGN.md §7.6） |
| 写（`dur=group` vs PG `synchronous_commit=on`，WAL 同盘） | 64 并发 `create` **10 386 vs 1 837 ops/s**（**5.7×**，PG 那一格 p50 = 33.6 ms）、`write` **12 753 vs 1 415**（**9.0×**） |
| **万级视图**（客户端侧，grovefs 的 treedb 后端，2026-11-20） | 连接池以前是**每视图 4 条**（100 视图 = 400 条），现在**每进程 4 条**：10 000 个驻留视图（每个都真挂载过、读过一次目录）实测引擎侧 established socket **恒 4、峰值 4**，meta-server 的 socket fd 恒 **6**；挂载内 create+unlink 3 965 → 4 194 µs（100 → 10 000 视图，+5.8 %，不随规模劣化）；RSS 多付 **14.1 MiB ≈ 1.44 KiB/视图**。旧形状外推到 10 000 视图是 **4 万条**，比本机能测到的 25 000 还多 —— 旧形状在这一档连测都测不了。原始输出 `benches/results/many_views.log`、`many_views_conc.log`，见 [DESIGN.md](DESIGN.md) §7.12 |
| 64 并发纯读（默认档 `workers=8`） | `readdir` **427 092 vs 146 203** ops/s（**2.9×**）、`lookup` **847 297 vs 210 868**（**4.0×**）、`ping` **977 771 vs 531 437**（**1.8×**）；p50 62–112 µs vs PG 117–431 µs |
| **单连接**（`conns=1`，v0.3.5 复测，同机同盘两边都真落盘） | 一问一答：`lookup` **43 567–44 597 vs 4 421–4 606** ops/s（**9.5–10.1×**，p50 19 µs vs 211 µs）、`readdir` 36 775–37 715 vs 3 184–3 238（**11.4–11.9×**）、`create` 1 740–1 753 vs 531–550（**3.2–3.3×**，p50 优 1.6×、p99 优 11–13×）、`write` 1 764–1 816 vs 232–614。单连接批量（`pipeline=32`）：`lookup` **77–80×**、`create` 5.0–5.6× 且 p50 优 **13×**（treedb 把 32 次写合成一次 fsync，PG 的 p50 不随批量降）。写入那一格 treedb **也每 op 付一次真盘**（引擎自计 `syncs == writes`），3× 不是靠放松持久化换来的 —— 逐格原始输出 `benches/results/single_conn.log`，机制解释见 [DESIGN.md](DESIGN.md) §7.11 |
| **并发档**（`conns=8/64`、`pipeline=1`，与上一行同一天、同盘复核） | treedb 一侧给两档：`workers=1`（基线）与本机默认 `workers=8`。默认档：`lookup` 8 连接 **9.9–10.6×**、64 连接 **4.7×**；`readdir` 8 连接 **9.9–10.8×**、64 连接 **5.2–5.3×**；`create` 8 连接 5.1–6.5×、64 连接 5.9–6.9×（**p50 优 16×**）；`write` 64 连接 6.5–8.7×（**p50 优 17–18×**）。写那一列涨的是 group commit 跨连接合并（64 连接时引擎自计**一次 fsync 摊 53–58 条写**），不是多核 —— 提交线程始终只有一个。**要诚实说的一面**：`workers=1` 且 64 连接时纯读被 PG 反超（`lookup` 0.55–0.56×、`readdir` 0.58–0.60×），因为那一档只吃一个核；pipeline 提到 32 时倍率收窄而不是继续拉大（`create` 64/32 只 1.6–1.7×）。同样见 §7.11 |
| 只给一个核（`workers=1`，且 PG 钉在**同一个**核上） | 同核、同并发 64/1：`ping` **153 329 vs 17 035**（**9.0×**）、`lookup` **119 001 vs 6 501**（**18.3×**）、`readdir` **87 075 vs 4 486**（**19.4×**）、`create` **65 122 vs 1 583**（**41.1×**）、`write` **71 213 vs 828**（**86.0×**）。旧版这一行写的「输给 PG 0.26–0.48×」**已作废**：那是拿一个核去比 PG 不限亲和性的 46–52 个核。同一张四臂表里还钉了另两件事：单核 `ping` 153 329 已是**同核裸 epoll echo 服务端（中位 167 999 ops/s，`benches/results/lb_floor.log`）的 91.3 %**，而这一核 90 % 的时间在系统时间里（`benches/results/split_attr.log`）—— 这一列到顶了。详见 [DESIGN.md](DESIGN.md) §7.5 末、`benches/sc_ops.sh` |
| 零旗标默认档（照上面两行命令起服务，同格跑 5 次取中位） | 服务端 banner 就是 `workers=8`：`readdir` **654 983**（= PG 4.5×）、`lookup` **1 062 564**（**5.0×**）、`ping` **1 016 868**（1.9×）—— 领先不靠调用方传 `--workers`；矩阵连跑的那两行是**下界**（`benches/default_check.sh`） |
| 为什么快（引擎自己的计数器） | 两个「会 fsync」的档位现在共用同一个提交点：64 并发下一轮只付 1 次 fsync、摊 **≈ 55–58 次写/fsync**（`benches/sync_ratio.sh`） |
| 每 op 一次 fsync（`dur=always` vs PG `synchronous_commit=on`，WAL 同盘） | 改前 1 891 vs 1 469 ops/s（p99 81.5 ms vs 157 ms）；**改后 10 913 vs 1 469 = 7.4×**，p50 优 17×、p99 优 5×（受控 A/B：`benches/ab_durability.sh v0.3.1`）。旧版这句「刨掉合并 fsync，treedb 对 PG 就没有结构性优势」是当时那个**按连接串 fsync** 的实现留下的错判，已作废：两家都是靠一次 fsync 摊多次写，treedb 在这一格摊得动（≈ 55）而 `always` 档以前自己把它抵消掉了 |
| 持久性 | `kill -9` 六个崩溃用例零丢失（含 checkpoint 瞬间崩、连崩 3 轮）；并另有 `workers=8` 下同语义的一条，目录写与表写逐条交错 |
| 接入认证（可选、默认关） | 20 000 条连接、两臂同轮 A/B × 3：**RSS +93 ~ 96 B/conn**（439 → 532/535 B，极可复现）、建连中位 **+90.9 µs/conn**（+232 / +1 818 / +1 885 ms —— 噪声比效应大，`off` 臂自己就摆 27 %）、稳态 `ping` 中位 1 147 239 → 1 148 689 ops/s、`create` 67 414 → 64 122（−4.9 %，**落在同臂自身 2.8 ~ 4.5 % 的散布里，既不报回归也不报变快**）。未开认证的数据面另用两臂都不带密钥的 `ab_layout.sh v0.3.5` 证零回归（八格最差 −0.90 %）。逐格原始输出 `benches/results/handshake_cost.log` 与 `ab_layout_v0.3.6.log`，读法见 §4.4 |

两个读数字的历史口径要提醒：旧版这张表写的是「64 并发纯读被 PG 反胜 3.6–7.5×」，
**已作废** —— `treedb-bench` 的分母把「建连 + 预热」算进去了，而 PG 侧对照器没算，
两边口径不对称（[DESIGN.md](DESIGN.md) §9.1 第 3 条）。现在两个客户端同口径，
而引擎改动的收益只用 `benches/ab_layout.sh` 的受控 A/B 说话。

数字全部落在 161 TB 机械 XFS 上，报的是倍率区间而不是单格小数的第六位；逐格原始
输出在 [benches/results/matrix.log](benches/results/matrix.log)，读法与陷阱在
DESIGN.md §7（含 §7.8 那次全五轴复跑）。

## 能做什么

三组接口，每组都是定长的 op，逐条分组与只读/写划分见
[DESIGN.md](DESIGN.md) §4.2：

- **POSIX 元数据面**：视图（命名空间）、目录树、属性、符号链接、硬链接（`v0.5.0`
  的 `Op::Link`，多名字共享一个 inode，内容对象只在最后一个名字离开时下账）、
  整 inode 锁、内容引用账本（GC 的唯一依据）。没有数据面 —— 文件字节永远在客户端侧。
- **通用表一面**：`TableCreate`/`TableDrop`/`TableGet`/`TableList` + `RowPut`/`RowGet`/
  `RowList`/`RowDelete`/`RowCount`。列为不透明字节，可标唯一/索引列，写可带
  `PUT_IF_ABSENT`/`PUT_IF_EXISTS` 与按列 CAS。存在理由：接入方的表形（密钥、审计、
  定义……）不该被搬进引擎内核，内核只给机制。列名、过期规则、权限语义全部由
  客户端定义；引擎不解释任何一列，也不做 JOIN、排序、范围谓词（§5.3）。
- **运维面**：`Ping`/`Stats`/`Checkpoint`/`Shutdown`，加上命令行侧的 `treedb-migrate`。`Stats` 同时报 WAL 的逻辑长度与
  磁盘长度、快照长度与协议版本 —— 帧里没有版本位，`proto_version` 是线上唯一能
  查到对端协议版本的通道（§4.1）。
- **日志传送两面**：`WalFetch`（从指定 seq 起按 durable 水位送记录）、`SnapFetch`（按
  字节切快照文件）。它们是服务端与副本之间的两面，客户端不会发到它们；
  客户端看见的差别只有一条：副本上写拿 `EROFS`。
- **连接与网络面**：一个地址一条 TCP 连接，服务端单进程 epoll，**每连接 1 个 fd +
  约 0.3 KiB**，实跑到 25 000 条常驻全绿（§7.6）。`--listen` 接 `host:port` 或
  `名字:端口`（走 std DNS），默认 `127.0.0.1:7777`，也可以给 `0.0.0.0`。新连接一律
  `TCP_NODELAY` + keepalive 空闲 60 s、探活 10 s×3（客户端崩了不会长期占 fd）。多条
  连接只共享一个状态机：锁按**连接号**归属（不用会被复用的 fd），死连接的锁由租约过期回收。
  引擎没有连接数上限常量，卡它的是服务端 `RLIMIT_NOFILE` 与客户端源端口空间（§7.6）。
- **持久性与备份面**：CRC32 WAL + 快照 checkpoint + 崩溃重放（§6.1/§6.2），优雅
  停服自带最后一次 checkpoint。备份可用的形状有五种且都跑过（§6.7/§6.9/§6.10）：停服后 `cp -a` 目录、
  `treedb-standby` 拉一份持续追平的异地镜像（那个目录可以直接当还原点起服，
  v0.4.0 起还能用 `treedb-standby --promote` 把它合法地提成主）、
  `treedb-migrate` 处理快照格式跨代、`treedb-dump`/`treedb-load` 做离线逻辑导出与重建、
  `treedb-backup` 从在线主拉同一格式的一致切面（不停服）。
  `--durability` 三档：`group`（默认，一轮一次
  fsync、跨连接合并）/ `always`（同一个提交点，单连接下自然退化成每 op 一次盘）/
  `never`（会丢一个窗口）。

## 接入前必读：没做的（和只做了一半的）四件事

- **认证只管身份，授权与传输加密仍然没有**（§4.4、§8 第 13 条）。v5 起可以
  `--auth-key-file` 打开一次共享密钥的双向握手，但它默认关、而且过了握手的连接与
  不开时一样拥有全部 52 个 op 的权限，包括 `Shutdown` 与 `Checkpoint`；握手之后仍是
  裸 TCP + 长度前缀 + CRC32，而 CRC32 只判损坏。所以「谁能连上 + 谁持有这把密钥」
  = 「谁能改整个状态」，多租户、配额、审计归属必须在引擎之外做（grovefs 的
  apikey + 网关白名单就是这一层），或者把端口锁在回环/私有网段。副本对主那一侧
  能证明「持有密钥」了，`v0.4.0` 起也能认出被提掉权的旧主（`fence`，§6.6）；但
  那只保护副本，不阻止旧主对自己的客户端继续服务。
- **没有 PITR、没有增量与加密备份**（§8 第 14 条、§6.7）。`treedb-dump`/`treedb-load`
  是停服的整份导出，`treedb-backup` 不停服但同样是整份一次性切面（不是持续追赶）；
  checkpoint 会抹掉水位以下的日志（同步档
  `wal.reset()`、自动档 `truncate_before()`），历史切面不归档；运行中 `cp -a` 能起服、
  无撕裂，但切点不可控（实测比「拷贝前一刻」多 14 400 条 seq），所以它不是备份工具。
- **没有 SQL、没有 JOIN/排序/范围谓词、没有用户可见事务、没有跨视图原子 op**（§5.3）。
  通用表只给「按唯一键取一行、按前缀列举个数」这一档机制，语义留给接入方。
- **没有数据面**（§5.3）：文件字节永远在客户端侧，引擎只答元数据。也没有同步复制、
  没有自动故障转移（`--promote` 只是手工一步的离线动作，不开服务、不选主，§6.6）。

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
| `src/engine/` | 线协议类型（`model.rs`，52 个 op / 30 个 Reply）、帧编码（`codec.rs`）、状态机（`mem.rs`）、应用与定序（`apply.rs`）、WAL/快照（`wal.rs`）、核心（`core.rs`）、离线 dump 容器（`dump.rs`，§6.9）、自写的 SHA-256/HMAC（`sha256.rs`，§4.4） |
| `src/net/` | 单线程事件循环（`server.rs`）、多前端 worker + 唯一提交线程（`multi.rs`）、跨线程信箱（`mail.rs`）、接入认证闸门（`auth.rs`，§4.4）、副本跟踪器（`replicate.rs`）、阻塞客户端（`client.rs`） |
| `src/{pool,store,persist,schema,probe}.rs` | `postgres-compat` 过渡档 |
| `src/bin/` | `treedb-server` / `treedb-bench` / `treedb-pgbench`（对照工具）/ `treedb-micro`（纯计算 + 分配计数）/ `treedb-migrate`（快照升级）/ `treedb-standby`（只读副本 + `--promote`）/ `treedb-dump` + `treedb-load`（离线逻辑导出与重建）/ `treedb-backup`（在线一致快照）/ `treedb-demo` |
| `tests/` | `engine_crud` `net_protocol` `read_scaling` `replication` `auth` `crash_recovery` `migrate` `dump_restore` `backup_online` `scale_connections` `independence`（默认档全部要跑）；`concurrency` 属过渡档 |
| `benches/` | 五轴矩阵脚本与历史/复跑原始输出；通用表的七条 `row*` 路径基准在 `bench_rows.sh`（§7.15） |
| `DESIGN.md` | 选型判据、协议字节布局、语义边界、五轴实测、已知限制、接入路径与下一步 |

## 许可

Apache-2.0。
