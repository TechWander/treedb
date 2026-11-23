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
    /// 一次取块的**下限**。WAL 段与快照段共用它，落后多个块长时按
    /// [`next_fetch_bytes`] 自己翻倍、稳定后落回这里。超过 [`WAL_FETCH_MAX`]
    /// 主会回 `EINVAL`，所以这里不校验，让主那侧说话。
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
            // 但一次 tick 占住循环的时间得压得住；落后时块长会自己翻倍，所以
            // 这个值是「稳态一轮多大」而不是「最快能多快」。
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
    /// 本轮实际要的负载上限。平时等于 [`ReplicaCfg::max_bytes`]，与主差得
    /// 远时按 [`next_fetch_bytes`] 翻倍，追回积压之后自己降回去。
    ///
    /// 快照下载段也用它（此时「积压」是还没下完的字节）：冷启动的 tick 数就等于
    /// 块数，一段 28 MB 的快照在 1 MiB 档下要 28 轮，块长放开后 <10 轮。
    cur_bytes: u32,
    /// 换代时从手里那半份里**留下来**的字节（累计）与整份重下的次数。
    ///
    /// 只给测试与 `progress` 看，不进 `Stats`：后者是线协议的一帧，加字段就得
    /// 抬 `PROTOCOL_VERSION`（§4.1），而这三个数的读者只有运维和用例。
    snap_reused: u64,
    snap_restarts: u64,
    /// 换代探针累计问了几次（含「整段是否相同」那一次）。二分让这个数从 1 变成
    /// 最多 `1 + PREFIX_PROBE_BUDGET`；只给 `progress` 与用例看。
    snap_probes: u64,
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

/// 一次换代最多额外问主几次前缀 crc（不含「整段是否相同」那一次）。
///
/// 每多问一次就把「留/丢」的边界往中间收一格：8 次把粒度收到已下量的 1/256。
/// 主侧每次都从 0 算到 `n`（[`Engine::snap_prefix_crc`] 是线性扫，slicing-by-8
/// 之后约 1.3 GB/s），最坏额外代价约 8 × 已下量 —— 130 MB 的快照约 0.8 s，
/// 仍比整份重下（限速 20 MB/s 下 6.5 s）便宜一个量级。
///
/// 分叉越靠后，探针花掉的 crc 越多，但留下的字节也越多，两个方向同阶；
/// 分叉越靠前，探针越便宜，留下的一格越少。
pub const PREFIX_PROBE_BUDGET: u32 = 8;

/// 追赶时开始放大块长的滞后量，单位是「配置块长的倍数」。
///
/// 取 4 而不是 1：只落后一两块时放大块长，白白把一轮拉长到上百毫秒，
/// 而读延迟的代价当场就付了；真正值得抢的是「落后好几个块长」的积压。
const RAMP_LAG_ROUNDS: u64 = 4;

/// 自适应块长：一轮拿满了预算 **且** 主那边还剩多个块长的积压时翻倍（上限
/// [`WAL_FETCH_MAX`]）；只要拿不满（= 已经追上）就退回 `cfg`。
///
/// `cfg` 永远是下限，所以 `--fetch-bytes` 仍然是「稳态一轮多大」的旋钮。
/// WAL 段与快照段共用这一条规则，区别只在「积压」的量出去哪儿取：WAL 段是主
/// 报的 `durable_seq` 差（折成字节），快照段直接是「整份还剩多少没下」。
///
/// 为什么是块长而不是频率：一轮的固定开销（一次往返 + 一次 commit/fsync）与块长
/// 无关，块长翻倍就把同一笔开销摊到两倍字节上。同机实测（86–96 MB / 113–127
/// 万条积压）：定长 1 MiB **16.31** MiB/s，自适应 **30.66** MiB/s（1.88×，
/// 中位；§6.6 末段有逐轮明细）。
fn next_fetch_bytes(cur: u32, cfg: u32, batch_bytes: u64, backlog_bytes: u64) -> u32 {
    let floor = cfg.clamp(1, WAL_FETCH_MAX);
    if batch_bytes < floor as u64 || backlog_bytes <= RAMP_LAG_ROUNDS * floor as u64 {
        return floor;
    }
    cur.max(floor).saturating_mul(2).min(WAL_FETCH_MAX)
}

impl Follower {
    pub fn new(cfg: ReplicaCfg) -> Self {
        let backoff_ms = cfg.interval_ms.max(1);
        let cur_bytes = cfg.max_bytes;
        Follower {
            cfg,
            client: None,
            last: None,
            backoff_ms,
            hint_off: WAL_HINT_NONE,
            snap: None,
            cur_bytes,
            snap_reused: 0,
            snap_restarts: 0,
            snap_probes: 0,
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
            max_bytes: self.cur_bytes,
        };
        let rep = match self.call(&op) {
            Ok(r) => r,
            // 传输层断了：丢连接、计数、下轮重连。已有的状态照旧可答读。
            Err(e) => {
                self.fail_round(eng, "wal fetch", &e);
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
                    // 已跟上：块长退回配置值。追赶时放大是为了抢回积压；稳态下
                    // 一轮占住事件循环上百毫秒只是在伤自己的读延迟。
                    self.cur_bytes = self.cfg.max_bytes;
                    return Ok(());
                }
                let batch_bytes: u64 = records.iter().map(|r| r.len() as u64).sum();
                let last_seq = base_seq + records.len() as u64 - 1;
                let behind = durable_seq.saturating_sub(last_seq);
                // 用本批的字节/条均值把「还差多少条」折成「还差多少字节」：
                // WAL 里每条字节数差一个量级（目录项 vs 节点），拿条数当量度
                // 会让触发点随工作负载飘。
                let avg = (batch_bytes / records.len() as u64).max(1);
                self.cur_bytes = next_fetch_bytes(
                    self.cur_bytes,
                    self.cfg.max_bytes,
                    batch_bytes,
                    behind.saturating_mul(avg),
                );
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

    /// 装完快照之前先把整份下下来。每次 tick 下一块，块长与 WAL 段共用一条规则
    /// （见 [`next_fetch_bytes`]）：从 `--fetch-bytes` 起步，剩余还多于 4 个块长就
    /// 翻倍，封顶 `WAL_FETCH_MAX`。
    ///
    /// 下载途中主又 checkpoint 一次（`snap_seq` 变了）是设计里的正常分支：两代
    /// 的字节不能拼，但**已下过的前缀未必作废** —— 换代时向主要前缀 crc
    /// （[`Follower::probe_prefix`]），能接上就接着下；整段对不上时再二分，把
    /// 「前半段相同、后半段分叉」的那一半也留下。
    fn step_snapshot(&mut self, eng: &mut Engine) -> Result<()> {
        let (off, cap, seen) = {
            let dl = self.snap.as_ref().expect("step_snapshot 只在下载态调用");
            (dl.off, self.cur_bytes, dl.gen)
        };
        let rep = match self.call(&Op::SnapFetch { offset: off, len: cap }) {
            Ok(r) => r,
            Err(e) => {
                self.fail_round(eng, "snapshot fetch", &e);
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
        // 代际变了（或这是第一块）：换代之后的字节不能与换代之前的拼在一起。
        // 已下的前缀能不能留下，由主算一次 crc 说 —— 而不是由版本号或长度猜：
        // 两代正文的公共前缀不在协议里承诺。
        let keep = if seen == Some(gen) {
            off
        } else if off == 0 {
            0
        } else {
            match self.probe_prefix(off, gen, total) {
                Ok(k) => k,
                Err(e) => {
                    self.fail_round(eng, "snapshot probe", &e);
                    return Ok(());
                }
            }
        };
        {
            let dl = self.snap.as_mut().expect("下载态");
            if dl.gen != Some(gen) {
                // 换代：截到能留的那段（可能为 0），代际标签改成新的。只数
                // `snap_restarts` / `snap_reused`，不报错误 —— 主这边一次
                // checkpoint 就能撞上位，这是设计里的正常分支。
                dl.buf.truncate(keep as usize);
                dl.off = keep;
                dl.gen = Some(gen);
                if keep != off {
                    // 这一块是按旧偏移取的，起点接不上新前缀，丢掉等下一轮。
                    return Ok(());
                }
                // 走到这里：前缀整个留住了，手里这一块刚好接在它后面。
            }
            dl.buf.extend_from_slice(&bytes);
            dl.off = dl.off.saturating_add(bytes.len() as u64);
            if dl.off < total {
                // 「还剩多少没下」就是这一段的积压。不翻倍的话，冷启动的耗时
                // 直接等于块数 × 间隔：28 MB 的快照在 1 MiB/轮的默认值下要 28 个
                // tick，其中传输只占很小一部分。
                self.cur_bytes = next_fetch_bytes(
                    self.cur_bytes,
                    self.cfg.max_bytes,
                    bytes.len() as u64,
                    total - dl.off,
                );
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
        // 装完就回落到下限：紧接的 WAL 追赶会按当时的积压自己再放大，而“配置值
        // 是下限”这个承诺不该因为刚跑过快照段就变成“上一个快照块多大就一直多大”。
        self.cur_bytes = self.cfg.max_bytes;
        // 本地水位装到这一代，主却可能已经又 checkpoint 过一次：那种情况下一轮
        // `step_wal` 会再收到 gap，再装一次。`replica_resyncs` 涨不停就是这个
        // 信号（追不上），不是这里能就地解决的。
        Ok(())
    }

    /// 换代之后问主：手里这 `off` 字节的前缀在新的一代里到底能留多长。
    ///
    /// 每次探针是 `SnapFetch { offset: n, len: 0 }`（`len == 0` 在协议里本来就是
    /// 个空读，这一笔给它加了「顺便把前 n 字节的 crc 报上来」的意思，
    /// [`Reply::SnapPrefix`]）。旧主不认识这个语义，会回一个空 `SnapChunk`：任何
    /// 非 `SnapPrefix` 的答复都当「不能留」，退回整份重下 —— 代价是一轮白拉，而
    /// 不是拼出一份谁都不认识的快照。
    ///
    /// 整段对不上不等于一格都留不下。公共前缀是单调的（前 k 字节相同 ⇒ 任何更短
    /// 的也相同），所以整段 miss 之后再二分：`lo` 恒为「已知可留」、`hi` 恒为
    /// 「已知不可留」，直到接壤或用完 [`PREFIX_PROBE_BUDGET`]。只有「一次探针只判
    /// 全等」的年代，改一个中段节点（前半段相同、后半段分叉）也要从 0 重下。
    ///
    /// 三种「对不上」都归零：主已经在算下一代的途中（`snap_seq` 或长度与本块
    /// 不符）、旧主不认识探针、二分到头也没找到。返回 0 让调用方从 0 重来。
    fn probe_prefix(&mut self, off: u64, gen: u64, total: u64) -> Result<u64> {
        let cap = off.min(total);
        // 第一个点就是「已下的整段」：对得上说明一格没变，是最常见的那一支，
        // 也是唯一不需要二分的一支。
        let mut lo = 0u64;
        let mut hi = cap;
        let mut probes = 1;
        match self.prefix_matches(cap, gen, total)? {
            Some(true) => lo = cap,
            Some(false) => {}
            None => return Ok(0),
        }
        let mut budget = PREFIX_PROBE_BUDGET;
        while hi - lo > 1 && budget > 0 {
            let mid = lo + (hi - lo) / 2;
            budget -= 1;
            probes += 1;
            match self.prefix_matches(mid, gen, total)? {
                Some(true) => lo = mid,
                Some(false) => hi = mid,
                // 主在算下一代：这次判据不再属于同一份正文，整段作废。
                None => return Ok(0),
            }
        }
        let keep = lo;
        if keep == 0 {
            self.snap_restarts += 1;
            eprintln!(
                "treedb-standby: snapshot generation moved to {gen}, no usable prefix in the \
                 {cap} B already downloaded; restarting from 0 ({probes} probes)"
            );
        } else {
            self.snap_reused += keep;
            eprintln!(
                "treedb-standby: snapshot generation moved to {gen}; keeping {keep} of {cap} B \
                 already downloaded ({probes} probes)"
            );
        }
        Ok(keep)
    }

    /// 问主一次「前 `n` 字节的 crc 是否与我手里这段相同」。
    ///
    /// `Some(..)` 是同一代、同一个 `total` 下的比较结果；`None` 表示判据不可用
    /// （旧主不认识探针，或主已经在算下一代 —— `snap_seq`/`total` 与本块不符）。
    fn prefix_matches(&mut self, n: u64, gen: u64, total: u64) -> Result<Option<bool>> {
        self.snap_probes += 1;
        let rep = self.call(&Op::SnapFetch { offset: n, len: 0 })?;
        let (prefix_crc, ptotal, pgen) = match rep {
            Reply::SnapPrefix {
                prefix_crc,
                total,
                snap_seq,
                ..
            } => (prefix_crc, total, snap_seq),
            _ => return Ok(None),
        };
        if pgen != gen || ptotal != total {
            return Ok(None);
        }
        let dl = self.snap.as_ref().expect("下载态");
        let k = n.min(dl.buf.len() as u64) as usize;
        Ok(Some(crate::engine::wal::crc32(&dl.buf[..k]) == prefix_crc))
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
        // 两段都从下限起步：上一段（WAL）可能已经把块长顶到 4 MiB，直接带进
        // 快照段会让一次冷启动的第一轮就扛上一整个大块，而它未必是最后几块。
        self.cur_bytes = self.cfg.max_bytes;
        self.snap = Some(SnapDl {
            buf: Vec::new(),
            off: 0,
            // `gen` 先记成主现在报上来的这一代：第一块带的就是它，不需要重来。
            gen: Some(snap_seq),
        });
    }

    /// 一轮失败：丢连接、计数、按退避重连。
    ///
    /// 错误本体必须打出来 —— 只写「primary lost」会让跨机故障没法归因：
    /// 超时、对端 RST、协议不符在运维眼里长得一模一样，而它们要改的东西
    /// 完全不同（调 --timeout-ms / 查网络 / 查版本）。退避期只在第一次打印，
    /// 避免每轮刷屏。
    fn fail_round(&mut self, eng: &mut Engine, what: &str, e: &Error) {
        self.client = None;
        eng.note_replica_error();
        if self.backoff_ms == self.cfg.interval_ms.max(1) {
            eprintln!(
                "treedb-standby: {what} failed, primary {} lost ({e})",
                self.cfg.primary
            );
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
            fetch_bytes: self.cur_bytes,
            snap_off: self.snap.as_ref().map(|d| d.off).unwrap_or(0),
            snap_reused: self.snap_reused,
            snap_restarts: self.snap_restarts,
            snap_probes: self.snap_probes,
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
    /// 下一轮实际要的负载上限（追赶时会大于配置值，见 [`next_fetch_bytes`]）。
    pub fetch_bytes: u32,
    /// 快照下载的当前偏移（没在下载就是 0）、换代时留下来的字节（累计）、
    /// 整份重下的次数与总探针次数。它们只在下载阶段会动，不进 `Stats`
    /// （那会改线协议），是给用例与排查用的。
    pub snap_off: u64,
    pub snap_reused: u64,
    pub snap_restarts: u64,
    pub snap_probes: u64,
}

#[cfg(test)]
mod tests {
    use super::{next_fetch_bytes, WAL_FETCH_MAX};

    const MIB: u32 = 1 << 20;

    /// 没拿满 = 已经追上。哪怕主那边还剩一点，也不放大：稳态一轮就该是配置值。
    #[test]
    fn unsaturated_round_falls_back_to_cfg() {
        // 主说自己还剩一整个 WAL_FETCH_MAX 的积压，但这一轮只拿到半块。
        let got = next_fetch_bytes(4 * MIB, MIB, (MIB / 2) as u64, (4 * MIB) as u64);
        assert_eq!(got, MIB);
    }

    /// 拿满了但只落后一两块：不值得把一轮拉长，退回配置值。
    #[test]
    fn shallow_lag_stays_at_cfg() {
        for rounds in 0..=super::RAMP_LAG_ROUNDS {
            let backlog = rounds * MIB as u64;
            let got = next_fetch_bytes(4 * MIB, MIB, MIB as u64, backlog);
            assert_eq!(got, MIB, "lag {rounds} rounds must not ramp");
        }
    }

    /// 拿满 + 欠账超过阈值：逐级翻倍，到 `WAL_FETCH_MAX` 封顶。
    #[test]
    fn deep_lag_ramps_to_the_cap() {
        let deep = 64 * MIB as u64;
        let mut cur = MIB;
        let mut seen = Vec::new();
        for _ in 0..8 {
            cur = next_fetch_bytes(cur, MIB, MIB as u64, deep);
            seen.push(cur);
        }
        assert_eq!(&seen[..3], &[2 * MIB, 4 * MIB, 4 * MIB]);
        assert_eq!(cur, WAL_FETCH_MAX, "must stop at the protocol ceiling");
    }

    /// 配置值本身就是上限（或超了）：没有可放大的空间，原样返回上限。
    #[test]
    fn cfg_at_ceiling_never_grows_past_it() {
        let deep = 64 * (WAL_FETCH_MAX as u64);
        let got = next_fetch_bytes(WAL_FETCH_MAX, WAL_FETCH_MAX, WAL_FETCH_MAX as u64, deep);
        assert_eq!(got, WAL_FETCH_MAX);
        // 配置值超过协议上限：取上限，绝不把非法值送上线（主会回 EINVAL）。
        let got = next_fetch_bytes(2 * WAL_FETCH_MAX, 2 * WAL_FETCH_MAX, 0, deep);
        assert_eq!(got, WAL_FETCH_MAX);
    }

    /// 不变量：结果恒在 `[floor, WAL_FETCH_MAX]`，`floor` 至少 1。
    /// `check_cfg` 会挡掉 `max_bytes == 0`，这里只保证这个纯函数自己不产出 0。
    #[test]
    fn result_is_always_within_floor_and_ceiling() {
        for &cfg in &[0u32, 1, 4096, MIB, WAL_FETCH_MAX, WAL_FETCH_MAX + 1] {
            let floor = cfg.clamp(1, WAL_FETCH_MAX);
            for &cur in &[0u32, 1, 4096, MIB, WAL_FETCH_MAX] {
                for &batch in &[0u64, 1, floor as u64, WAL_FETCH_MAX as u64] {
                    for &backlog in &[0u64, floor as u64, 8 * floor as u64, u64::MAX] {
                        let got = next_fetch_bytes(cur, cfg, batch, backlog);
                        assert!(
                            got >= floor && got <= WAL_FETCH_MAX,
                            "cfg {cfg} cur {cur} batch {batch} backlog {backlog} -> {got}"
                        );
                    }
                }
            }
        }
    }
}
