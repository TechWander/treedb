//! 只读副本的追平循环（DESIGN.md §6.6）。
//!
//! **形状是 pull，不是 push**：主不需要知道有任何副本存在，它只是回答
//! `Op::WalFetch` / `Op::SnapFetch`；副本按自己的节奏拉，拉到什么就 apply 什么。
//! 于是服务端的主循环一行未动 —— 不需要往连接表里挂反向通道，也不需要为「副本
//! 掉线」维护任何状态。剩下唯一的新代码就是这个文件加上 `bin/standby.rs`。
//!
//! **它跑在事件循环里**，不是自己的线程。理由不是省事：apply 需要 `&mut Engine`，
//! 而 `Engine` 按本 crate 的单写者约定只能由提交线程持有（见 `engine::core` 的
//! 模块文档）。另起一条线程去 apply 等于把「谁在改状态」这个问题重新问一遍。
//! 代价是一次 tick 会占住本轮循环，所以这里每 tick 只做**一次**往返（要么一块
//! 快照、要么一批记录），并把 tick 之间限频；快照因此是一块一块下的，不是
//! 一口气下完 40 MB 把客户端全关在门外。
//!
//! **失败不致命**：连不上主、主回报 `EIO`、交付对不上号，都只是把 `Stats` 里
//! 那几个 `replica_*` 计数抬一下，然后继续用已有状态答读。一个只读副本最有用
//! 的时候恰恰是主没了的时候 —— 把它做成会退出，等于把这个卖点做成反向卖点。
//! 真正会退出的只有一种：主交来的快照水位比自己还旧，那是配错了（连错主），
//! 继续服务一份谁也不认的状态比停下来更坏。

use crate::engine::model::{errno, Op, Reply, WAL_FETCH_MAX, WAL_HINT_NONE};
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::net::auth::Key;
use crate::net::client::Client;
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 副本的追平参数。`ServerConfig::replication` 里出现它，本实例就是副本。
#[derive(Debug, Clone)]
pub struct ReplicaCfg {
    /// 主的地址。
    pub primary: SocketAddr,
    /// 两次 tick 之间的最小间隔。它决定稳态下副本对主的最多问数频率。
    pub interval_ms: u64,
    /// 一次 `WalFetch` 要的负载上限。超过 [`WAL_FETCH_MAX`] 主会回 `EINVAL`，
    /// 所以这里不校验，让主那侧说话。
    pub max_bytes: u32,
    /// 一次往来的读写超时。事件循环里跑，不能没有上限。
    pub timeout_ms: u64,
    /// 主启用了接入认证时的共享密钥（§4.4）。`None` = 不握手。
    ///
    /// 副本与主之间跑的仍是同一套握手，没有专门身份。注意它只证明
    /// 「对端持有这把密钥」：脑裂后的旧主同样持有，照样能应答握手 ——
    /// fencing token 仍然欠着（§11 第 4 条②③）。
    pub auth: Option<Arc<Key>>,
}

impl ReplicaCfg {
    pub fn new(primary: SocketAddr) -> Self {
        ReplicaCfg {
            primary,
            // 20 ms：与 `sync_ms` 同量级。再小就是把主压成串行问数端，再大就是
            // 把副本的稳态 lag 抬到肉眼可见。
            interval_ms: 20,
            // 1 MB / 轮：`WAL_FETCH_MAX` 的 1/4。稳态下每轮只有一两次往返，
            // 但一次 tick 占住循环的时间得压得住；真要快点追就让 interval 小。
            max_bytes: 1 << 20,
            timeout_ms: 5_000,
            auth: None,
        }
    }
}

/// 一个副本的追平状态机。由事件循环独占持有，每轮 `tick` 一次。
pub struct Follower {
    cfg: ReplicaCfg,
    /// 到主的连接。`None` = 还没连上或上一次断了。
    client: Option<Client>,
    /// 上次真正做过一次往返的时刻，用来限频。
    last: Option<Instant>,
    /// 连不上时的退避窗口：从 `interval_ms` 倍增到 [`BACKOFF_MAX_MS`]。
    /// 没有它的话，主宕机时每条 tick 都要在监听线程上付一次 SYN 超时。
    backoff_ms: u64,
    /// 上一轮主给的偏移提示。`WAL_HINT_NONE` = 让主自己按 seq 定位。
    hint_off: u64,
    /// 正在整份下载主那侧的快照。这一状态里不拉 WAL：装完快照之后水位才接得上。
    snap: Option<SnapDl>,
}

/// 快照下载的中间状态。
struct SnapDl {
    /// 已拼好的字节。整份收在内存里（快照就是全量状态，本来也要进内存）。
    buf: Vec<u8>,
    /// 下一块的偏移。
    off: u64,
    /// 已见过的代际（主那边的 `snap_seq`）。变了就说明手里的块不是同一代。
    gen: Option<u64>,
}

const BACKOFF_MAX_MS: u64 = 2_000;

impl Follower {
    pub fn new(cfg: ReplicaCfg) -> Self {
        let backoff_ms = cfg.interval_ms.max(1);
        Follower {
            cfg,
            client: None,
            last: None,
            backoff_ms,
            hint_off: WAL_HINT_NONE,
            snap: None,
        }
    }

    /// 事件循环每轮调一次。自己判该不该动手，所以调用方不需要记时刻。
    ///
    /// 返回 `Err` 只有那条「继续服务下去更坏」的情形，见模块文档。
    pub fn tick(&mut self, eng: &mut Engine) -> Result<()> {
        let now = Instant::now();
        // 下载中的每一块都值这个钱，不必等满一个间隔；稳态才限频。
        if self.snap.is_none() {
            if let Some(t) = self.last {
                if now.duration_since(t) < Duration::from_millis(self.cfg.interval_ms) {
                    return Ok(());
                }
            }
        }
        // 退避窗口：连不上主时按倍增间隔重试。没这道限频的话，主宕机时每一轮
        // 都要在监听线程上付一次 SYN 超时。
        if self.client.is_none() {
            if let Some(t) = self.last {
                if now.duration_since(t) < Duration::from_millis(self.backoff_ms) {
                    return Ok(());
                }
            }
            if !self.connect(eng) {
                self.last = Some(now);
                return Ok(());
            }
            self.backoff_ms = self.cfg.interval_ms.max(1);
        }
        self.last = Some(Instant::now());
        if self.snap.is_some() {
            return self.step_snapshot(eng);
        }
        self.step_wal(eng)
    }

    /// 建连。用 `connect_timeout` 而不是 `Client::connect`：后者只给读写设超时，
    /// 一个不可达的地址会把 SYN 重传的全程（默认几分钟）付在监听线程上；
    /// 而只管建连不设读写超时，一个接受连接却不答话的对端同样能把循环钉住。
    ///
    /// 握手失败归入与连不上同一档：它意味着这台副本的密钥与主对不上，
    /// 重试不会自己变好，但也不能把追平循环钉死 —— 继续用已有状态答读、
    /// 同时指数退避重试，是这里唯一不偷工的走法（错误得在 `Stats` 里看得见）。
    fn connect(&mut self, eng: &mut Engine) -> bool {
        let dur = Duration::from_millis(self.cfg.timeout_ms.max(1));
        // 一个 `std::result::Result<Client, String>`：建连与握手的失败在这里同
        // 档，但错误文本必须保留各自的形状（“连不上”与“密钥不对”是完全不同的
        // 运维动作）。不用 crate 的 `Result` 别名：它的错型是 `Error`，装不下
        // 两种来源。
        let sock = TcpStream::connect_timeout(&self.cfg.primary, dur);
        let built: std::result::Result<Client, String> = match sock {
            Err(e) => Err(format!("connect: {e}")),
            Ok(s) => {
                let _ = s.set_nodelay(true);
                let _ = s.set_read_timeout(Some(dur));
                let _ = s.set_write_timeout(Some(dur));
                Client::from_stream_auth(s, self.cfg.auth.as_deref()).map_err(|e| e.to_string())
            }
        };
        match built {
            Ok(c) => {
                self.client = Some(c);
                true
            }
            Err(e) => {
                eng.note_replica_error();
                if self.backoff_ms == self.cfg.interval_ms.max(1) {
                    eprintln!("treedb-standby: primary {} unusable: {e}", self.cfg.primary);
                }
                self.backoff_ms = (self.backoff_ms * 2).min(BACKOFF_MAX_MS);
                false
            }
        }
    }

    /// 拉一批 WAL 记录并追平。一轮一次往返，剩下的一轮再来。
    fn step_wal(&mut self, eng: &mut Engine) -> Result<()> {
        let want = eng.apply_seq();
        let op = Op::WalFetch {
            want_seq: want,
            hint_off: self.hint_off,
            max_bytes: self.cfg.max_bytes,
        };
        let rep = match self.call(&op) {
            Ok(r) => r,
            // 传输层断了：丢连接、计数、下轮重连。已有的状态照旧可答读。
            Err(_) => {
                self.fail_round(eng, "wal fetch");
                return Ok(());
            }
        };
        match rep {
            Reply::WalBatch {
                records,
                base_seq,
                next_off,
                durable_seq,
                snap_seq,
                gap,
                epoch,
            } => {
                eng.note_replica_progress(durable_seq);
                self.hint_off = next_off;
                if gap {
                    // 要的那条已经被主 checkpoint 截进快照了。这不是错，是「你
                    // 跟不上」，代价是整份重装：`snap_seq` 就是为此存在的。
                    self.start_resync(snap_seq);
                    return Ok(());
                }
                if records.is_empty() {
                    return Ok(());
                }
                match eng.replicate_records(base_seq, &records, epoch) {
                    Ok(_) => Ok(()),
                    // 交付接不上号：本地缺一段（比如副本自己的目录比主老）。
                    // 提示作废，走重装快照那条路 —— 它比从中间补洞诚实。
                    Err(Error::Gap(_)) => {
                        // `start_resync` 自己会把提示清掉：上一代的偏移在新日志里
                        // 没有意义。
                        self.start_resync(snap_seq);
                        Ok(())
                    }
                    // 对面是被提掉权的旧主（§6.6 第 3 条）：本地已经提升过，
                    // 这一批哪怕接得上也不能收。不退出也不重试 —— 这是配置错，
                    // 不是链路错：每次退避后重问、把错误计数抬上去，让
                    // `--stats` 里那行 `replica_errors` 自己说话。
                    Err(Error::Fence(m)) => {
                        eng.note_replica_error();
                        if self.backoff_ms == self.cfg.interval_ms.max(1) {
                            eprintln!(
                                "treedb-standby: primary {} is fenced out: {m}",
                                self.cfg.primary
                            );
                        }
                        self.backoff_ms = (self.backoff_ms * 2).min(BACKOFF_MAX_MS);
                        self.client = None;
                        Ok(())
                    }
                    // 解码不过 / 往返不一致：两代格式分叉或线上坏了，不是能靠
                    // 重试绕开的东西。报出去让进程停在配置层面。
                    Err(e) => {
                        eng.note_replica_error();
                        Err(e)
                    }
                }
            }
            Reply::Err { code, msg } => {
                eng.note_replica_error();
                let m = String::from_utf8_lossy(&msg).into_owned();
                eprintln!("treedb-standby: primary rejected wal fetch ({code}): {m}");
                // 主那侧日志真坏了（`EIO`）：继续问也问不出东西，退回没有提示，
                // 让它至少有机会在下一轮给出新的一段。
                if code == errno::EINVAL {
                    self.hint_off = WAL_HINT_NONE;
                }
                Ok(())
            }
            other => {
                eng.note_replica_error();
                Err(Error::Io(format!("primary answered {other:?} to WalFetch")))
            }
        }
    }

    /// 装完快照之前先把整份下下来。每次 tick 下一块，块长 = `max_bytes`。
    fn step_snapshot(&mut self, eng: &mut Engine) -> Result<()> {
        let (off, cap) = {
            let dl = self.snap.as_ref().expect("step_snapshot 只在下载态调用");
            (dl.off, self.cfg.max_bytes)
        };
        let rep = match self.call(&Op::SnapFetch { offset: off, len: cap }) {
            Ok(r) => r,
            Err(_) => {
                self.fail_round(eng, "snapshot fetch");
                return Ok(());
            }
        };
        let (bytes, total, gen, epoch) = match rep {
            Reply::SnapChunk {
                bytes,
                total,
                snap_seq,
                epoch,
            } => (bytes, total, snap_seq, epoch),
            Reply::Err { code, msg } => {
                eng.note_replica_error();
                let m = String::from_utf8_lossy(&msg).into_owned();
                eprintln!("treedb-standby: primary rejected snapshot fetch ({code}): {m}");
                return Ok(());
            }
            other => {
                eng.note_replica_error();
                return Err(Error::Io(format!(
                    "primary answered {other:?} to SnapFetch"
                )));
            }
        };
        {
            let dl = self.snap.as_mut().expect("下载态");
            if total == 0 {
                // gap 意味着主至少做过一次 checkpoint，它因此必然有快照文件。
                // 真拿到 0 只可能是文件被人删了 —— 这不是能自动重来的东西，
                // 但也不该把副本打死：它手上的状态仍然自洽，先停在这里等它回来。
                eng.note_replica_error();
                eprintln!(
                    "treedb-standby: primary {} reports snapshot seq {gen} but serves 0 bytes; resync stays pending",
                    self.cfg.primary
                );
                return Ok(());
            }
            if dl.gen != Some(gen) {
                // 代际变了（或这是第一块）：换代之后的字节不能与换代之前的拼在
                // 一起，整份重来。主这边一次 checkpoint 就能撞上位，这是设计里
                // 的正常分支，所以只数 `replica_resyncs`，不报错误。
                dl.buf.clear();
                dl.off = 0;
                dl.gen = Some(gen);
                return Ok(());
            }
            dl.buf.extend_from_slice(&bytes);
            dl.off = dl.off.saturating_add(bytes.len() as u64);
            if dl.off < total {
                return Ok(());
            }
        }
        // 收齐了。`install_snapshot` 会顺手把 WAL 截到这份快照的水位，之后第一
        // 次 `step_wal` 要的 `want_seq` 正好是主那侧还在的最早一条。
        let buf = std::mem::take(&mut self.snap.as_mut().unwrap().buf);
        match eng.install_snapshot(&buf, epoch) {
            Ok(_) => {}
            // 快照来自被提掉权的旧主：丢掉这份下载，下一轮重问。同样是配置错，
            // 不是链路错（§6.6 第 3 条）。
            Err(Error::Fence(m)) => {
                eng.note_replica_error();
                eprintln!(
                    "treedb-standby: primary {} served a fenced snapshot: {m}",
                    self.cfg.primary
                );
                self.snap = None;
                self.client = None;
                self.backoff_ms = (self.backoff_ms * 2).min(BACKOFF_MAX_MS);
                return Ok(());
            }
            Err(e) => return Err(e),
        }
        eng.note_replica_resync();
        self.snap = None;
        self.hint_off = WAL_HINT_NONE;
        // 本地水位装到这一代，主却可能已经又 checkpoint 过一次：那种情况下一轮
        // `step_wal` 会再收到 gap，再装一次。`replica_resyncs` 涨不停就是这个
        // 信号（追不上），不是这里能就地解决的。
        Ok(())
    }

    /// 开始一次快照重装。gap 不是错，是「跟不上」，所以这里不抬
    /// `replica_errors`：两个数混在一起，运维就分不出哪一种该报警。
    fn start_resync(&mut self, snap_seq: u64) {
        if self.snap.is_none() {
            eprintln!(
                "treedb-standby: behind the primary's checkpoint (snap_seq {snap_seq}), \
                 reinstalling its snapshot"
            );
        }
        self.hint_off = WAL_HINT_NONE;
        self.snap = Some(SnapDl {
            buf: Vec::new(),
            off: 0,
            // `gen` 先记成主现在报上来的这一代：第一块带的就是它，不需要重来。
            gen: Some(snap_seq),
        });
    }

    /// 一次往来的传输层失败：断连接、计数、下次重连。
    fn fail_round(&mut self, eng: &mut Engine, what: &str) {
        self.client = None;
        eng.note_replica_error();
        if self.backoff_ms == self.cfg.interval_ms.max(1) {
            eprintln!("treedb-standby: {what} failed, primary {} lost", self.cfg.primary);
        }
        self.backoff_ms = (self.backoff_ms * 2).min(BACKOFF_MAX_MS);
    }

    /// 一次请求。`Client` 是阻塞式的，读写超时在建连时按 `timeout_ms` 设过。
    fn call(&mut self, op: &Op) -> Result<Reply> {
        let c = self
            .client
            .as_mut()
            .ok_or_else(|| Error::Io("not connected to the primary".into()))?;
        match c.call(op) {
            Ok(r) => Ok(r),
            Err(e) => {
                self.client = None;
                Err(e)
            }
        }
    }

    /// 副本自己的进度一句话。`bin/standby.rs` 的周期打印与测试都用它。
    pub fn progress(&self, eng: &mut Engine) -> FollowerProgress {
        let s = eng.snapshot_stats();
        FollowerProgress {
            applied: s.replica_applied,
            lag: s.replica_lag,
            primary_seq: s.replica_primary_seq,
            resyncs: s.replica_resyncs,
            errors: s.replica_errors,
            connected: self.client.is_some(),
            downloading: self.snap.is_some(),
        }
    }

    /// 只读副本的上限校验，给 bin 启动时用：把明显跑不了的参数挡在动手之前。
    pub fn check_cfg(cfg: &ReplicaCfg) -> std::result::Result<(), String> {
        if cfg.max_bytes == 0 || cfg.max_bytes > WAL_FETCH_MAX {
            return Err(format!(
                "--fetch-bytes must be in 1..={WAL_FETCH_MAX}, got {}",
                cfg.max_bytes
            ));
        }
        if cfg.interval_ms == 0 {
            return Err("--interval-ms must be at least 1".into());
        }
        Ok(())
    }
}

/// 副本进度的一个不可变视图，只为打印与断言。
#[derive(Debug, Clone, Copy, Default)]
pub struct FollowerProgress {
    pub applied: u64,
    pub lag: u64,
    pub primary_seq: u64,
    pub resyncs: u64,
    pub errors: u64,
    pub connected: bool,
    pub downloading: bool,
}
