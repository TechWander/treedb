//! 单线程 epoll 事件循环服务端。
//!
//! ## 为什么是单线程
//!
//! 引擎的写路径是「stage → WAL fsync → ack」的定序流水线，单线程独占
//! 状态就不需要任何锁，也不需要事务隔离；并发度全部体现在连接轴上
//! （万级连接共用一条线程，每次 epoll_wait 一轮就把它们合并成一次
//! fsync）。元数据 op 的 CPU 成本是微秒级，一条线程足够跑满 fsync 预算。
//!
//! ## 每连接内存
//!
//! 一个 `TcpStream`（1 fd）+ 一个按需增长的 `RxBuf` + 一个待发 FIFO。
//! ping-pong 型负载下稳态约每连接几百字节到 2 KB，`RSS/conn` 由
//! `tests/scale.rs` 实测，不靠估算。
//!
//! ## 应答顺序不变量
//!
//! 协议没有 request id，因此**每连接的应答必须严格按请求顺序**。一轮
//! 事件循环里既 stage 写 op（要等 fsync）也 stage 读 op（立刻有答案），
//! 做法：
//!
//! - 每连接一个 `VecDeque<Slot>` FIFO，`Slot = Filled(frame) | Pending(idx) |
//!   Await`；
//! - 写 op 入队 `Pending(idx)` 并把编好的帧放进本轮的 `staged` 列表；
//! - fsync 成功后把 `staged` 的帧按下标回填到对应 `Pending` 槽；
//! - 交给提交线程的 op（只在多 worker 模式）入队 `Await`，答案回来时按
//!   同连接的出现顺序逐个填充；
//! - 发送端只从队头弹出，所以排在写后面的读不会插队。
//!
//! `pump` 只弹队头的 `Filled`，而待回填的槽一定在本轮 stage 时位于其后面，
//! 于是 `idx` 在回填前始终有效。
//!
//! ## 读并行（`workers > 1`，见 [`crate::net::multi`]）
//!
//! 单线程循环的天花板不在 CPU，在**每连接一次 read + 一次 write 系统调用全都
//! 挤在同一条线程**上：实测 64 连接 `lookup`、pipeline=32 时单线程能跑到
//! 70 万 op/s（CPU 根本不是墙），pipeline=1 时却掉到 9.6 万 —— 差的 7 倍就是
//! syscall 串行化。所以扩展切在**前端**：N 条 worker 线程各自 `SO_REUSEPORT`
//! 监听 + epoll + 拥有关属于自己的连接，只读 op 就地并行执行（读锁），含写的
//! 批次整批转发给唯一提交线程 —— 写仍然单点定序，group commit 仍然跨全部
//! 连接合并成一次 fsync。
//!
//! 进池闸门只有一条：**这条连接上没有未回话的转发 op，且本批帧里一个写都没
//! 有**。不满足就整批交给提交线程排队，因为前缀式分派会让先进去的读看到后
//! 应用的写（凭空提前生效），反过来延后执行的读又看不到它前面的写。混打负载
//! 因此退回串行 —— 不拿语义换数字。
//!
//! `workers = 1` 走的是本文件里那条循环，代码路径与多 worker 改造之前
//! 逐字一致，所以历次基线数字仍然可以直接对比。
//!
//! [`crate::net::multi`]: crate::net::multi

use crate::engine::codec::Rd;
use crate::engine::mem::Log;
use crate::engine::{reply_frame, Durability, Engine, FRAME_HDR, Op, Reply, RxBuf};
use crate::error::{Error, Result};
use crate::net::auth::Gate;
use crate::net::replicate::{Follower, ReplicaCfg};
use std::collections::{HashMap, VecDeque};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 每轮 `epoll_wait` 最多取回的事件数。
pub(crate) const MAX_EVENTS: usize = 2048;
/// 单次 `read` 的搬运上限；大帧靠多次可读事件继续读。
pub(crate) const READ_CHUNK: usize = 64 * 1024;
/// 单连接待发字节超过该值后暂停从对端读取（背压，防 OOM）。
pub(crate) const MAX_BACKLOG: usize = 4 * 1024 * 1024;

pub struct ServerConfig {
    /// 监听地址。
    pub listen: SocketAddr,
    /// 数据目录（WAL + 快照）。
    pub dir: PathBuf,
    pub durability: Durability,
    /// epoll_wait 超时（毫秒）；到点做一次周期 sync。0 = 一直阻塞到有事件。
    pub sync_ms: i32,
    /// WAL 超过该字节数自动 checkpoint。0 = 只接受 `Op::Checkpoint`。
    pub auto_checkpoint_bytes: u64,
    /// `listen(2)` 的 backlog。
    ///
    /// std 绑定时写死 128，而万级视图挂载/滚动重启时客户端是**成批**重连的：
    /// 128 的接受队列会在几百毫秒内打满，后来的 SYN 被内核直接丢弃。那不是
    /// 引擎的连接容量不够，是监听参数不够，所以这里显式可调。
    /// 实际值还会被 `net.core.somaxconn` 截断（本机 4096）。
    pub backlog: i32,
    /// 前端 worker 线程数。只扩展读：写永远由唯一提交线程定序。
    ///
    /// `ServerConfig::new` 把它填成 [`crate::net::default_workers`]（按核数），
    /// `--workers 1` 是旧默认，也是全部历史基线数字的档。超过核数没有意义，
    /// 超过并发连接数也没有意义（内核按四元组哈希分流，多出来的线程只会空转）。
    pub workers: usize,
    /// 写轴流水线（§11 第 8 条）：提交线程只 `append`，`fdatasync` 交给专用
    /// flusher 线程，落盘回执到了再 ack。
    ///
    /// **默认关**，因为实测在本项目的目标硬件上是净负收益：一次 fsync
    /// 1.3–1.6 ms 的盘上，多出来的「eventfd + poll + 线程唤醒」往返约
    /// 150 µs（占周期 10%），而它换来的「fsync 期间继续接活」在 group 档
    /// 并没有可接的活 —— 旧实现里转发本来就堆在 worker 信箱里，fsync 返回
    /// 后照样一次性排干；translate 本身是 µs 级，把它从周期里挑出去赚不回
    /// 线路往返。
    ///
    /// 开关留着是因为收益条件是可判定的：单次 fsync 占周期比例越高、
    /// 且单个 op 的 translate 越重（大目录 readdir 的兄弟 op），重叠才赚得
    /// 回来。默认关意味着默认档的数字与改造前逐字一致。
    pub flush_pipeline: bool,
    /// 非 `None` 就是**只读副本**：本实例以 [`Engine::open_replica`] 开局，
    /// 事件循环每轮拿这个配置去主那里追一次（§6.6）。
    ///
    /// 主那侧不需要任何对应配置：传送是 pull 的，主只是回答 `WalFetch` /
    /// `SnapFetch`，它不知道也不需要有谁在拉。
    pub replication: Option<ReplicaCfg>,
    /// 接入认证密钥（§4.4）。`None` = 不认证，这也是默认值。
    ///
    /// 一个 `Arc` 而不是 `Key`：它要在 accept 时递给每条新连接的 [`Gate`]，
    /// 而 `Gate` 自己只存进度、不存密钥（密钥跟着连接数复制 32 字节本身
    /// 无害，但“谁持有密钥”应该只有一个来源，便于 `--auth-key-file` 轮换时
    /// 只看一处）。
    ///
    /// 开启后**只影响握手**：数据面每 op 多一次 `Gate::allows_data()` 判等，
    /// 握手本身不占提交线程。代价由 `benches/handshake_cost.sh` 实测。
    pub auth: Option<Arc<crate::net::auth::Key>>,
    /// 同时在册的连接数上限；`0` = 不限（默认，与历史行为逐字一致）。
    ///
    /// 为什么需要它：§7 的连接轴实测到 25 000 条连接、0.31 KiB/conn，也就是说
    /// **资源不是瓶颈** —— 那么把进程跑死的方式就剩一种：一条不守规矩的客户端
    /// 无限开连接（或一个失去回收的 fork 风暴），把 fd 配额与内存慢慢吃光，
    /// 而症状是「所有连接一起变慢」而不是「多出来的那条被拒」。
    ///
    /// 判定的原子性：多 worker 模式下每条 worker 各自 accept，所以不能用
    /// 「读 live → 比较 → 加一」三步（两条线程会同时看到 `max-1` 而各收一条，
    /// 超拨）。这里一律 `fetch_add` 之后再判超，超了就减回去，读数只在
    /// 成功那条路径上生效。
    ///
    /// 拒绝形状是**接受后立刻关闭**（客户端看到 EOF），不发任何帧：协议每连接
    /// 严格 FIFO 且没有 request id，在没有请求的情况下先塞一个 `Reply::Err`
    /// 没有任何位置可放，客户端也无从对应。要让客户端看到原因，看服务端
    /// 那行 `refused connection #N`（限速打印）。
    pub max_conn: usize,
    /// WAL 归档目录（§6.11）。`None` = 不归档，与引入归档之前逐字一致。
    ///
    /// 开了之后每次 checkpoint 会先把要抹掉的那段日志与旧快照拷进这个目录，
    /// 于是「回到某个秒」有了物料；归档失败会让 checkpoint 失败（fail-closed），
    /// 因为历史不能静默丢。开关必须一直开着：中间关过一段，那几次 checkpoint
    /// 就会在归档里留一个洞，`treedb-pitr` 越过它时会直接报缺口。
    pub archive: Option<PathBuf>,
    /// 过期租约的清扫间隔（毫秒），`0` = 关。默认 1000。
    ///
    /// `LockQuery` 一直把过期租约当不存在，但记录本身要等被覆盖或被
    /// `Close` 才消失 —— 不改这一点的话，`locks` 只增不减（§8 第 17 条）。
    /// 回收是一条**写**（`Log::DelLock`），所以只能在提交线程上做，且必须
    /// 去 WAL、参与副本收敛；这里只决定它多久跑一次。每次最多看
    /// [`crate::engine::LOCK_SWEEP_BATCH`] 条，一圈的成本摊在多轮上，与
    /// 表大小无关。副本上这个旋钮无效：它的 WAL 是主的镜像。
    pub lock_sweep_ms: i32,
}

impl ServerConfig {
    pub fn new(listen: SocketAddr, dir: impl Into<PathBuf>) -> Self {
        ServerConfig {
            listen,
            dir: dir.into(),
            durability: Durability::Group,
            sync_ms: 20,
            auto_checkpoint_bytes: 64 * 1024 * 1024,
            backlog: 4096,
            // auto：按核数给。默认 1 等于把读吞吐锁在一个核上：64 条连接的
            // read/exec/write 全排在同一条线程上，而 PG 是每个 backend 一个进程、
            // 天然摊在几十个核上。这差距是**并行度**不是效率 —— 两边都只给一个核时
            // 单核档反而领先一个量级（同核口径见 `benches/sc_ops.sh` 的四臂表）。
            // 理由见 [`crate::net::default_workers`]；旧版这里写的那句「高并发纯读
            // 直接输给 PG（实测 3-8×）」已作废，见 DESIGN.md §9.1 第 3、4 条。
            workers: crate::net::default_workers(),
            // 默认关：本机实测净负收益，理由见字段上的注释与 DESIGN §11 第 8 条。
            flush_pipeline: false,
            replication: None,
            auth: None,
            // 默认不限：v0.5 之前的实例没有这个概念，默认值一变就是行为变更。
            max_conn: 0,
            // 默认不归档：额外一次字节搬运与一份磁盘占用，不该默认替用户决定。
            archive: None,
            // 默认 1 s 扫一圈的前缀：对 1 024 条的批次是微秒级，而一圈的
            // 代价与表大小成正比 —— 1 s 在「租约以秒计」的场景下足够早。
            lock_sweep_ms: 1000,
        }
    }
}

/// 按配置打开引擎。`replication` 里有主就以只读副本身份开 —— 这个单点存在
/// 是为了让两条事件循环（单线程与多 worker）不可能一个开了副本、一个忘了。
pub(crate) fn open_engine(cfg: &ServerConfig) -> Result<Engine> {
    match &cfg.replication {
        Some(_) => Engine::open_replica_with_archive(
            &cfg.dir,
            cfg.durability,
            cfg.archive.clone(),
        ),
        None => Engine::open_with_archive(&cfg.dir, cfg.durability, cfg.archive.clone()),
    }
}

/// 一次 `serve` 结束（收到停止信号）后的汇总，供 bin 打印。
#[derive(Debug, Clone, Copy, Default)]
pub struct Report {
    /// 启动时崩溃恢复的度量（快照水位 / 重放条数 / 耗时）。
    pub rec: crate::engine::core::Recovery,
    pub peak_conns: usize,
    pub accepted: u64,
    /// 因 `max_conn` 被接受后立刻关闭的连接数。与 `accepted` 并列才说明得清
    /// 「没人来」与「来了但被上限挡回去了」—— 两者在 `peak_conns` 里长得一样。
    pub refused: u64,
    pub rounds: u64,
    pub worst_round_us: u64,
    pub ops: u64,
    pub writes: u64,
    pub syncs: u64,
    pub avg_sync_us: u64,
    pub checkpoints: u64,
    /// 前端 worker 线程数（1 = 单线程模式）。
    pub workers: usize,
    /// 在 worker 线程就地算完的只读 op 数。与 `ops` 对比可以看出读里有多大
    /// 比例真并行；`workers = 1` 时恒为 0。
    pub reads_inline: u64,
    /// 交给提交线程的 op 数（写 + 排在写之后的读）。这个数占比高说明负载
    /// 混打，并行只捞到了读的那一部分。
    pub forwarded: u64,
    /// 本实例是不是以只读副本身份跑的（§6.6）。
    pub replica: bool,
    /// 退出时已 apply 到第几条日志。与 `replica_lag` 一起才说明得清：
    /// lag = 0 可能是“真追上了”，也可能是“从来没连上过、水位还在 0”。
    pub replica_applied: u64,
    /// 退出时还差主多少条。副本停在一个谁也没告诉过它的 lag 上是运维最不想
    /// 看到的场景，所以这几个数进 `Report` 而不是只留在 `Stats` 里。
    pub replica_lag: u64,
    /// 退出前重装过几次快照。
    pub replica_resyncs: u64,
    /// 退出前拉取/应用失败过几轮。
    pub replica_errors: u64,
}

pub(crate) enum Slot {
    /// 已可发送的应答帧。
    Filled(Vec<u8>),
    /// 等 WAL fsync 的写：下标指向本轮 `staged` 列表。
    Pending(usize),
    /// 等提交线程回话的 op（只在多 worker 模式）：答案按同连接的提交
    /// 顺序送回来，所以不需要 id，填到第一个 `Await` 槽就是它。
    Await,
}

pub(crate) struct Conn {
    pub(crate) fd: i32,
    /// 全局唯一连接号，**从不复用**。多 worker 模式靠它把提交线程的应答
    /// 路由回正确的连接：fd 会被内核重分配，id 不会。
    pub(crate) id: u64,
    pub(crate) stream: TcpStream,
    pub(crate) rx: RxBuf,
    /// 待应答 FIFO，严格保持请求顺序。
    pub(crate) out: VecDeque<Slot>,
    /// `out` 里还有多少个 `Await`（已转发、提交线程未回话）。
    /// 只要它非零，本连接后续的读就不能就地算。
    pub(crate) queued: usize,
    /// 已就绪待发的字节。
    pub(crate) tx: Vec<u8>,
    pub(crate) tx_pos: usize,
    /// 还有没写完的东西 → 需要关心 EPOLLOUT。
    pub(crate) want_write: bool,
    /// epoll 里当前真的挂着 EPOLLOUT 吗。与 `want_write` 相等时
    /// [`Conn::rearm`] 直接返回，一个 syscall 都不付 —— 而 ping/lookup 这类
    /// 小回包恰恰几乎永远相等（本轮就发完了），于是每 op 的 `epoll_ctl`
    /// 从 2 次降到 0 次。
    pub(crate) ep_out: bool,
    /// 这条连接的握手进度（§4.4）。`Stage::Off` 时所有数据 op 直通，
    /// 判据只有一次 `matches!`。
    pub(crate) auth: Gate,
    /// 背压把读端从 epoll 掩码里摘下来时置位。
    ///
    /// 为什么必须摘：epoll 是**电平触发**的，而背压时我们主动不读 socket ——
    /// 只要不把 EPOLLIN 拿掉，`epoll_wait` 就会每轮立刻返回同一个 fd，事件
    /// 循环 100% 空转，别的连接跟着饿死。摘掉之后由 `backlog` 排空到一半以下
    /// 自动恢复（`flush` 末尾判）。EPOLLHUP/EPOLLERR 不受掩码影响，对端死了
    /// 照样能醒。
    pub(crate) read_hold: bool,
    /// epoll 掩码里当前真的没挂 EPOLLIN 吗。与 `read_hold` 相等时
    /// [`Conn::rearm`] 不为它下一 syscall。
    pub(crate) ep_hold: bool,
}

impl Conn {
    /// `auth_required` = 本实例启用了接入认证。它只决定这条连接的闸门
    /// 从 `Needed` 还是 `Off` 开局，之后两者走的是同一套代码。
    pub(crate) fn new(stream: TcpStream, id: u64, auth_required: bool) -> Self {
        let fd = stream.as_raw_fd();
        Conn {
            fd,
            id,
            stream,
            rx: RxBuf::new(),
            out: VecDeque::new(),
            queued: 0,
            tx: Vec::new(),
            tx_pos: 0,
            want_write: false,
            // accept 路径刚用 epoll_set(…, false) 注册过，掩码里没有 EPOLLOUT。
            ep_out: false,
            read_hold: false,
            ep_hold: false,
            auth: Gate::new(auth_required),
        }
    }

    /// 本轮活儿干完后把 epoll 掩码对齐到「`want_write` + 是否允许读」。
    ///
    /// 关键在于**对齐不等于下 syscall**：掩码已经对的时候（小回包一轮发完，
    /// `want_write` 与 `ep_out` 都是 false、又没有背压）直接返回。实测单核 ping
    /// 每 op 本来要付 2 次 `epoll_ctl`（ADD 撞 EEXIST + MOD），这里一次都不付。
    pub(crate) fn rearm(&mut self, ep: i32) -> Result<()> {
        if self.want_write == self.ep_out && self.read_hold == self.ep_hold {
            return Ok(());
        }
        epoll_mod(ep, self.fd, self.want_write, !self.read_hold)?;
        self.ep_out = self.want_write;
        self.ep_hold = self.read_hold;
        Ok(())
    }

    /// 背压满档：把读端从掩码里摘下来（真正落地的 syscall 在 [`Conn::rearm`] 里）。
    pub(crate) fn hold_read(&mut self) {
        self.read_hold = true;
    }

    /// 积压排到一半以下才恢复读：贴着上限恢复会让每轮都在「摘/恢复」之间抖，
    /// 每轮付两次 `epoll_ctl`。
    fn resume_read_if_drained(&mut self) {
        if self.read_hold && self.backlog() < MAX_BACKLOG / 2 {
            self.read_hold = false;
        }
    }

    /// 未发送字节（含已回填但未弹出的帧）——背压判据。
    pub(crate) fn backlog(&self) -> usize {
        self.tx.len() - self.tx_pos
            + self
                .out
                .iter()
                .map(|s| match s {
                    Slot::Filled(f) => f.len(),
                    // 待回填的槽不知道成品帧多大（写等 fsync、转发等提交线程），
                    // 按 0 计：背压只需要拦住那些真把内存堆起来的情况。
                    Slot::Pending(_) | Slot::Await => 0,
                })
                .sum::<usize>()
    }

    /// 把一条应答**直接编进 `tx`**，帧长回填在最后一步。
    ///
    /// 老路是 `reply_frame` → `Slot::Filled(Vec<u8>)` → `pump` 再 `extend_from_slice`
    /// 进 `tx`：一个 readdir 页（~4 KB）要在用户态被拷三遍、多付两次分配。直写
    /// 之后只剩「编一次」。
    ///
    /// 返回 false 表示**不能**走这条快路：`out` 里还有已排队但未成品的槽
    /// （写等 fsync、转发等 committer），此时新应答必须继续排到 `out` 后面，
    /// 直接追到 `tx` 尾部会把字节插到那些尚未定序的答复之前。
    pub(crate) fn push_frame(&mut self, reply: &Reply) -> bool {
        if !self.out.is_empty() {
            return false;
        }
        let at = self.tx.len();
        let mut buf = std::mem::take(&mut self.tx);
        buf.resize(at + FRAME_HDR, 0);
        {
            use crate::engine::codec::W;
            let mut w = W(buf);
            reply.encode(&mut w);
            buf = w.0;
        }
        let n = (buf.len() - at - FRAME_HDR) as u32;
        buf[at..at + FRAME_HDR].copy_from_slice(&n.to_le_bytes());
        self.tx = buf;
        true
    }

    /// 把队头连续的 `Filled` 帧搬进 `tx`。
    fn pump(&mut self) {
        while let Some(Slot::Filled(_)) = self.out.front() {
            match self.out.pop_front() {
                Some(Slot::Filled(f)) => self.tx.extend_from_slice(&f),
                _ => break,
            }
        }
    }

    /// 尽力写出；`WouldBlock` 时保留剩余字节并置 `want_write`。
    pub(crate) fn flush(&mut self) -> std::io::Result<()> {
        self.pump();
        while self.tx_pos < self.tx.len() {
            match self.stream.write(&self.tx[self.tx_pos..]) {
                Ok(0) => {
                    self.want_write = false;
                    return Err(ErrorKind::WriteZero.into());
                }
                Ok(n) => self.tx_pos += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) => {
                    self.want_write = false;
                    return Err(e);
                }
            }
        }
        if self.tx_pos == self.tx.len() {
            self.tx.clear();
            self.tx_pos = 0;
        }
        self.want_write = !self.out.is_empty() || !self.tx.is_empty();
        // 积压真排下去了就把读端挂回去：这一判必须在 `want_write` 算完之后，
        // 否则刚写的字节还没算进 backlog，会被误判成已排空。
        self.resume_read_if_drained();
        Ok(())
    }
}

pub(crate) fn epoll_create() -> Result<i32> {
    let ep = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if ep < 0 {
        return Err(Error::Io(format!("epoll_create1: {}", last_os())));
    }
    Ok(ep)
}

/// 设置某个 fd 感兴趣的事件。首次 ADD、之后 MOD，统一先试 ADD。
///
/// **只给真正的「第一次注册」用**（监听 fd、accept 新连接、eventfd）：那条路径上
/// ADD 会成功。每轮事件循环里对已连接的 fd 再走这个函数就是纯浪费 —— 实测
/// 单核 `--workers 1` 每 op 因此多付 2 次 `epoll_ctl`（第一次 ADD 必然
/// EEXIST，第二次才是真正要做的 MOD），占掉整个 op 系统调用预算的四成。
/// 每 op 的重新武装请走 [`Conn::rearm`]。
pub(crate) fn epoll_set(ep: i32, fd: i32, want_write: bool) -> Result<()> {
    let mut ev = unsafe { std::mem::zeroed::<libc::epoll_event>() };
    ev.events = libc::EPOLLIN as u32 | if want_write { libc::EPOLLOUT as u32 } else { 0 };
    ev.u64 = fd as u64;
    if unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_ADD, fd, &mut ev) } < 0 {
        if unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_MOD, fd, &mut ev) } < 0 {
            return Err(Error::Io(format!("epoll_ctl(fd={fd}): {}", last_os())));
        }
    }
    Ok(())
}

/// 纯 MOD（fd 必须已注册）。与 `epoll_set` 的区别就是不拿 ADD 去撞 EEXIST。
///
/// `want_read = false` 只出现在背压路径（见 [`Conn::read_hold`]）：把 EPOLLIN
/// 从掩码里拿掉，电平触发的 socket 才不会把事件循环烧成一个忙等。`EPOLLHUP` /
/// `EPOLLERR` 永远会报，所以摘掉读端不会让我们错过对端断开。
pub(crate) fn epoll_mod(ep: i32, fd: i32, want_write: bool, want_read: bool) -> Result<()> {
    let mut ev = unsafe { std::mem::zeroed::<libc::epoll_event>() };
    ev.events = if want_read { libc::EPOLLIN as u32 } else { 0 }
        | if want_write { libc::EPOLLOUT as u32 } else { 0 };
    ev.u64 = fd as u64;
    if unsafe { libc::epoll_ctl(ep, libc::EPOLL_CTL_MOD, fd, &mut ev) } < 0 {
        return Err(Error::Io(format!("epoll_ctl(MOD fd={fd}): {}", last_os())));
    }
    Ok(())
}

pub(crate) fn last_os() -> String {
    std::io::Error::last_os_error().to_string()
}

/// 新连接的服务端侧调优：无延迟 + 开 keepalive 并指定空闲/探活参数。
///
/// 内核默认 keepalive 空闲是 7200s，对元数据服务而言约等于没开：
/// 客户端崩溃后半开连接会长期占着 fd，故直接 setsockopt 把空闲压到
/// 60s（之后 10s×3 次探活失败即回收）。
pub(crate) fn tune_conn(stream: &TcpStream) -> std::io::Result<()> {
    stream.set_nodelay(true)?;
    let fd = stream.as_raw_fd();
    let on = 1 as libc::c_int;
    let opts: [(libc::c_int, libc::c_int, libc::c_int); 4] = [
        (libc::SOL_SOCKET, libc::SO_KEEPALIVE, on),
        (libc::IPPROTO_TCP, libc::TCP_KEEPIDLE, 60),
        (libc::IPPROTO_TCP, libc::TCP_KEEPINTVL, 10),
        (libc::IPPROTO_TCP, libc::TCP_KEEPCNT, 3),
    ];
    for (level, opt, val) in opts {
        let v = val as libc::c_int;
        unsafe {
            libc::setsockopt(
                fd,
                level,
                opt,
                &v as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    Ok(())
}

/// 本轮已 stage、还没 fsync 的一条写。
pub(crate) struct Staged {
    pub(crate) logs: Vec<Log>,
    pub(crate) frame: Vec<u8>,
}

/// 本轮的 stage 区：写列表 + 哪些连接队列里有待回填槽。
#[derive(Default)]
pub(crate) struct Round {
    pub(crate) staged: Vec<Staged>,
    pub(crate) write_fds: std::collections::HashSet<i32>,
}

impl Round {
    fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }
}

/// 跑事件循环，直到 `stop` 置位（`Op::Shutdown` 或信号）。
///
/// `workers = 1` 时就是本文件里的 [`serve_single`]；>1 时交给
/// [`crate::net::multi`]：N 条前端线程 + 1 条提交线程。分流靠这个
/// 入口而不是靠循环里加 `if`：默认档的代码路径必须与改造之前逐字一致。
pub fn serve(cfg: &ServerConfig, stop: &AtomicBool, live: &AtomicUsize) -> Result<Report> {
    if cfg.workers > 1 {
        crate::net::multi::serve(cfg, stop, live)
    } else {
        serve_single(cfg, stop, live)
    }
}

/// 拒绝新连接时打一行，**限速**：连接风暴下每条一行日志会把盘写满，
/// 而真正要看的只是「从某一个时刻起开始拒了」和「一共拒了多少」。
/// 所以只打第 1 条与之后的每 1000 条；总数在停服汇总里给。
///
/// 两条事件循环共用（单线程直接调、多 worker 从 `multi` 里调）：
/// 拒绝的日志格式不该因为 `--workers` 不同而不同。
pub(crate) fn note_refusal(total: u64, max_conn: usize) {
    if total == 1 || total % 1000 == 0 {
        eprintln!("treedb: refused connection #{total} (--max-conn {max_conn} is full)");
    }
}

/// 单线程事件循环：一个监听 fd、一个 epoll、全部连接。
pub(crate) fn serve_single(cfg: &ServerConfig, stop: &AtomicBool, live: &AtomicUsize) -> Result<Report> {
    let mut engine = open_engine(cfg)?;
    // 副本的追平状态机。`None` 时下面那句 tick 就是一次指针判空，主那侧的
    // 代码路径与改造之前逐字一致。
    let mut follower = cfg
        .replication
        .as_ref()
        .map(|c| Follower::new(c.clone()));
    let listener = TcpListener::bind(cfg.listen)
        .map_err(|e| Error::Io(format!("bind {}: {e}", cfg.listen)))?;
    if cfg.backlog > 0 {
        // std 不给 backlog，只能在 bind 已 listen 之后自己调一次：
        // 对已在 LISTEN 的 fd 重新 listen 会只换 backlog，不影响已有队列。
        let rc = unsafe { libc::listen(listener.as_raw_fd(), cfg.backlog) };
        if rc != 0 {
            return Err(Error::Io(format!("listen backlog {}: {e}", cfg.backlog, e = last_os())));
        }
    }
    listener.set_nonblocking(true)?;
    let lfd = listener.as_raw_fd();
    let ep = epoll_create()?;
    epoll_set(ep, lfd, false)?;

    let mut conns: HashMap<i32, Conn> = HashMap::new();
    // 握手由事件循环就地处置，不进引擎；`as_deref` 是为了下面能直接
    // 送 `Option<&Key>` 给 [`Gate::on_op`]。
    let auth_key = cfg.auth.as_deref();
    let auth_required = cfg.auth.is_some();
    let mut events: Vec<libc::epoll_event> = vec![unsafe { std::mem::zeroed() }; MAX_EVENTS];
    let mut buf = vec![0u8; READ_CHUNK];
    let mut rep = Report::default();
    rep.rec = engine.recovery();
    rep.workers = 1;
    let mut dirty: Vec<i32> = Vec::new();
    // Never 档的周期 sync 计时基准。必须限频：每轮都 sync 等于每轮一次 fsync，
    // 那 `Never` 就和 `Group` 一模一样了（实测两档都是 ~820 ops/s），
    // “无持久化上限”这根轴就量不到事件循环本身。
    let mut last_sync = Instant::now();
    // 过期租约清扫的节拍（§8 第 17 条）。与 sync 分开计时：后者是落盘语义，
    // 前者是内存/快照体积的清理，两者的默认间隔不同（20 ms vs 1 s）。
    let mut last_sweep = Instant::now();

    while !stop.load(Ordering::Relaxed) {
        let t0 = Instant::now();
        let timeout = if cfg.sync_ms <= 0 {
            -1
        } else {
            cfg.sync_ms
        };
        let n = unsafe {
            libc::epoll_wait(ep, events.as_mut_ptr(), events.len() as i32, timeout)
        };
        if n < 0 {
            let e = last_os();
            if e.contains("Interrupted") {
                continue;
            }
            return Err(Error::Io(format!("epoll_wait: {e}")));
        }
        rep.rounds += 1;
        // 本轮 stage 的写：帧内存的是已编好的应答，下标即 `Slot::Pending` 的索引。
        let mut round = Round::default();
        dirty.clear();

        for i in 0..n as usize {
            let fd = events[i].u64 as i32;
            let bits = events[i].events as u32;

            if fd == lfd {
                // ---- 新连接：一条 epoll 事件可能对应多个 backlog ----
                loop {
                    match listener.accept() {
                        Ok((stream, _addr)) => {
                            // 上限判定放在 accept 之后、注册之前：一条已经落在
                            // 接受队列里的连接必须被拿出来再关，否则它会永远占着
                            // 队列位（关掉未 accept 的 fd 需要关闭监听 socket）。
                            if cfg.max_conn > 0 && conns.len() >= cfg.max_conn {
                                drop(stream);
                                rep.refused += 1;
                                note_refusal(rep.refused, cfg.max_conn);
                                continue;
                            }
                            if stream.set_nonblocking(true).is_err() || tune_conn(&stream).is_err() {
                                continue;
                            }
                            // id 在多 worker 模式才用得到；单线程按 fd 索引，
                            // 永远不路由应答，故统一填 0。
                            let c = Conn::new(stream, 0, auth_required);
                            let nfd = c.fd;
                            epoll_set(ep, nfd, false)?;
                            conns.insert(nfd, c);
                            rep.accepted += 1;
                            let now = conns.len();
                            if now > rep.peak_conns {
                                rep.peak_conns = now;
                            }
                            live.store(now, Ordering::Relaxed);
                        }
                        // WouldBlock（读完 backlog）或 EMFILE 等：都退出本轮
                        // accept，fd 留给下一轮（监听 socket 仍可读）。
                        Err(_) => break,
                    }
                }
                continue;
            }

            if !conns.contains_key(&fd) {
                continue;
            }

            // ---- 可读：搬进 rx ----
            if bits & (libc::EPOLLIN as u32 | libc::EPOLLHUP as u32 | libc::EPOLLERR as u32) != 0 {
                let mut eof = false;
                loop {
                    let room = conns
                        .get(&fd)
                        .map(|c| c.backlog() < MAX_BACKLOG)
                        .unwrap_or(false);
                    if !room {
                        // 背压：数据还在 socket 里，但我们暂时不读。电平触发的
                        // EPOLLIN 不摘掉，下一轮 `epoll_wait` 会立刻把同一个 fd
                        // 再报上来 —— 那就是 100% CPU 的空转。摘下来，等
                        // `flush` 把积压排到一半以下再挂回去。
                        if let Some(c) = conns.get_mut(&fd) {
                            c.hold_read();
                        }
                        break;
                    }
                    match conns.get_mut(&fd) {
                        Some(c) => match c.stream.read(&mut buf) {
                            Ok(0) => {
                                eof = true;
                                break;
                            }
                            // 读不满一个整块 = 对端这一刻没再发了，直接收工。
                            // 不等于是把数据丢了：epoll 是电平触发，下一段到了
                            // 还会再报一次 EPOLLIN。省下来的正是每 op 那一次
                            // 只能拿到 EAGAIN 的 `recvfrom`（实测 ping 每 op
                            // 2 次 recvfrom，其中恰好 1 次报错）。
                            Ok(m) => {
                                let full = m == buf.len();
                                c.rx.extend(&buf[..m]);
                                if !full {
                                    break;
                                }
                            }
                            Err(e)
                                if matches!(
                                    e.kind(),
                                    ErrorKind::WouldBlock | ErrorKind::Interrupted
                                ) =>
                            {
                                break;
                            }
                            Err(_) => {
                                eof = true;
                                break;
                            }
                        },
                        None => break,
                    }
                }
                if eof {
                    close_conn(&mut conns, ep, fd, live);
                    continue;
                }
            }

            // ---- 拆帧并 stage ----
            let mut frames: Vec<Result<Vec<u8>>> = Vec::new();
            loop {
                match conns.get_mut(&fd) {
                    Some(c) => match c.rx.take_frame() {
                        Ok(Some(p)) => frames.push(Ok(p)),
                        Ok(None) => break,
                        Err(e) => {
                            frames.push(Err(e));
                            break;
                        }
                    },
                    None => break,
                }
            }
            let mut ops: Vec<Op> = Vec::with_capacity(frames.len());
            let mut drop_conn = false;
            for f in frames {
                let payload = match f {
                    Ok(p) => p,
                    Err(e) => {
                        push(&mut conns, fd, &Reply::err(crate::engine::model::errno::EINVAL, e.to_string()));
                        drop_conn = true;
                        break;
                    }
                };
                match Op::decode(&mut Rd::new(&payload)) {
                    Ok(o) => ops.push(o),
                    Err(e) => {
                        push(&mut conns, fd, &Reply::err(crate::engine::model::errno::EINVAL, e.to_string()));
                        drop_conn = true;
                        break;
                    }
                }
            }
            if drop_conn {
                close_conn(&mut conns, ep, fd, live);
                continue;
            }
            for op in ops {
                // 握手拦在引擎之前：它不读也不写状态，所以就地答完就下一
                // 条（§4.4）。未启用认证时这个 `if` 只对两个握手 tag 成立，
                // 数据 op 一次额外分派也不付。
                if auth_required || crate::net::auth::is_handshake(&op) {
                    let gated = match conns.get_mut(&fd) {
                        Some(c) => c.auth.on_op(auth_key, &op),
                        None => None,
                    };
                    if let Some(reply) = gated {
                        push(&mut conns, fd, &reply);
                        continue;
                    }
                }
                // checkpoint / shutdown 会动 WAL，必须先把本轮已 stage 的写
                // 落盘，否则快照水位会跑到磁盘前面。
                if matches!(op, Op::Checkpoint | Op::Shutdown) {
                    drain_round(&mut engine, &mut conns, &mut round)?;
                }
                let shutdown = matches!(op, Op::Shutdown);
                let (logs, reply) = engine.exec(&op);
                if logs.is_empty() {
                    push(&mut conns, fd, &reply);
                } else {
                    stage(&mut conns, fd, logs, &reply, &mut round);
                }
                if shutdown {
                    stop.store(true, Ordering::Relaxed);
                }
            }
            if bits & libc::EPOLLOUT as u32 != 0 {
                match conns.get_mut(&fd) {
                    Some(c) => {
                        if c.flush().is_err() {
                            close_conn(&mut conns, ep, fd, live);
                            continue;
                        }
                        c.rearm(ep)?;
                    }
                    None => continue,
                }
            }
            if conns
                .get(&fd)
                .map(|c| !c.out.is_empty() || c.tx_pos < c.tx.len())
                .unwrap_or(false)
            {
                dirty.push(fd);
            }
        }

        drain_round(&mut engine, &mut conns, &mut round)?;

        // ---- 轮末统一写出，并按需注册 EPOLLOUT ----
        if !flush_dirty(&mut conns, ep, &dirty)?.is_empty() {
            live.store(conns.len(), Ordering::Relaxed);
        }

        // ---- 周期任务 ----
        // 只在 Never 档真正 fsync（`sync_now` 内部判档），且不超过 sync_ms 一次；
        // Group/Always 的 fsync 已经在 drain_round 里按语义付过了。
        if cfg.sync_ms > 0 && last_sync.elapsed() >= Duration::from_millis(cfg.sync_ms as u64) {
            engine.sync_now()?;
            last_sync = Instant::now();
        }
        // 旁路快照（§8 第 7 条）：每轮先收一份已完成的（常数时间：看一眼
        // 线程是否结束），再判要不要派新的。派工只取水位 + 起线程，本轮停顿
        // 与状态大小无关；编快照的成本在后台线程里，收尾只做一次「抹掉前段」。
        engine.checkpoint_poll()?;
        if cfg.auto_checkpoint_bytes > 0 {
            // 只要 WAL 尺寸：走 snapshot_stats() 会变成每轮数一遍七张表。
            if engine.wal_bytes() > cfg.auto_checkpoint_bytes {
                engine.checkpoint_start()?;
            }
        }
        // 副本追平（§6.6）。放在 checkpoint 之后：自己这边的周期任务先做完，
        // 拉回来的记录才不会当场又逼出一次 checkpoint。
        if let Some(f) = follower.as_mut() {
            f.tick(&mut engine)?;
        }
        // 过期租约回收（§8 第 17 条）：读路径只过滤、不回收（读 op 不许产生
        // 日志），回收放在提交线程的周期任务里，用已有的 `Log::DelLock`。
        // 放在副本 tick 之后：这一轮拉回来的 `PutLock` 不会在同一轮就被
        // 自己判过期再删一遍。
        if cfg.lock_sweep_ms > 0
            && last_sweep.elapsed() >= Duration::from_millis(cfg.lock_sweep_ms as u64)
        {
            engine.sweep_expired_locks(crate::engine::now_ms(), crate::engine::LOCK_SWEEP_BATCH)?;
            last_sweep = Instant::now();
        }
        let us = t0.elapsed().as_micros() as u64;
        if us > rep.worst_round_us {
            rep.worst_round_us = us;
        }
    }

    // 优雅退出：落一次快照，把下次重启的重放窗口压到最短。
    //
    // **不吞错**：checkpoint/fsync 失败说明这次的磁盘状态不是我们以为的那个，
    // 退出码必须反映它 —— 吞掉等于告诉运维“干净退出”，而实际上下次启动要
    // 重放整个 WAL（或者下次 checkpoint 才炸的 EIO）。清理先做完，最后再抛。
    let shutdown = engine
        .sync_now()
        .and_then(|_| engine.checkpoint().map(|_| ()));
    let s = engine.snapshot_stats();
    rep.ops = s.ops_total;
    rep.writes = s.writes_total;
    rep.syncs = s.syncs_total;
    rep.checkpoints = s.checkpoints;
    rep.avg_sync_us = if s.syncs_total > 0 {
        s.sync_us_total / s.syncs_total
    } else {
        0
    };
    rep.replica = s.replica;
    rep.replica_applied = s.replica_applied;
    rep.replica_lag = s.replica_lag;
    rep.replica_resyncs = s.replica_resyncs;
    rep.replica_errors = s.replica_errors;
    live.store(0, Ordering::Relaxed);
    let fds: Vec<i32> = conns.keys().copied().collect();
    for fd in fds {
        close_conn(&mut conns, ep, fd, live);
    }
    unsafe { libc::close(ep) };
    shutdown?;
    Ok(rep)
}

/// 只读 op 的应答：立刻可发。
fn push(conns: &mut HashMap<i32, Conn>, fd: i32, reply: &Reply) {
    if let Some(c) = conns.get_mut(&fd) {
        if !c.push_frame(reply) {
            c.out.push_back(Slot::Filled(reply_frame(reply)));
        }
    }
}

/// 轮末统一写出。返回这一轮被关掉的连接 id（多 worker 路径要拿它摘掉
/// id→fd 映射；单线程路径只关心个数）。
///
/// `dirty` 里同一 fd 可能出现多次（一轮里多条 epoll 事件），故先去重再 flush；
/// 不去重的话一条大回包会被 write 循环重复扫几遍。
pub(crate) fn flush_dirty(
    conns: &mut HashMap<i32, Conn>,
    ep: i32,
    dirty: &[i32],
) -> Result<Vec<u64>> {
    let mut closed: Vec<u64> = Vec::new();
    let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for fd in dirty.iter() {
        if !seen.insert(*fd) {
            continue;
        }
        if let Some(c) = conns.get_mut(fd) {
            if c.flush().is_err() {
                if let Some(id) = close(conns, ep, *fd) {
                    closed.push(id);
                }
                continue;
            }
            c.rearm(ep)?;
        }
    }
    Ok(closed)
}

/// 写 op 的应答：占一个 `Pending` 槽，帧存进本轮 `staged`。
///
/// 槽里存的是 `staged` 的全局下标而不是队列位置——`flush` 会从队头
/// pop，队列下标会漂，全局下标不会。
fn stage(conns: &mut HashMap<i32, Conn>, fd: i32, logs: Vec<Log>, reply: &Reply, round: &mut Round) {
    if let Some(c) = conns.get_mut(&fd) {
        round.staged.push(Staged {
            logs,
            frame: reply_frame(reply),
        });
        c.out.push_back(Slot::Pending(round.staged.len() - 1));
        round.write_fds.insert(fd);
    }
}

/// 本轮收口：一次 fsync 覆盖所有已 stage 的写，成功后回填应答帧。
///
/// **ack 严格晚于 fsync** —— 这是「已确认的写不丢」的实现点。fsync 失败
/// 时内存状态已超前于磁盘，继续应答等于撒谎，故直接把 Err 上抛给 bin，
/// bin 必须终止进程（重启后由快照 + WAL 重放还原到真实持久点）。
pub(crate) fn drain_round(
    engine: &mut Engine,
    conns: &mut HashMap<i32, Conn>,
    round: &mut Round,
) -> Result<()> {
    if round.is_empty() {
        return Ok(());
    }
    let groups: Vec<&[Log]> = round.staged.iter().map(|s| s.logs.as_slice()).collect();
    engine.commit_batch(&groups)?;
    // fsync 已成，把帧回填到本轮真有过写的连接的槽位里。
    for fd in round.write_fds.iter() {
        if let Some(c) = conns.get_mut(fd) {
            for slot in c.out.iter_mut() {
                if let Slot::Pending(k) = slot {
                    *slot = Slot::Filled(std::mem::take(&mut round.staged[*k].frame));
                }
            }
        }
    }
    round.staged.clear();
    round.write_fds.clear();
    Ok(())
}

/// 关连接，并把对外可见的连接数同步成新值。单线程路径专用。
pub(crate) fn close_conn(
    conns: &mut HashMap<i32, Conn>,
    ep: i32,
    fd: i32,
    live: &AtomicUsize,
) {
    if close(conns, ep, fd).is_some() {
        live.store(conns.len(), Ordering::Relaxed);
    }
}

/// 摘掉一条连接。`TcpStream` drop 即关 fd，epoll 自动摘除 interest；fd 从此
/// 可能被内核重新分配，所以返回的连接号 `id` 是调用方唯一能拿去对账的东西。
pub(crate) fn close(conns: &mut HashMap<i32, Conn>, _ep: i32, fd: i32) -> Option<u64> {
    let c = conns.remove(&fd)?;
    let id = c.id;
    drop(c);
    Some(id)
}
