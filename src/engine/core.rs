//! 引擎核心：状态 + WAL + checkpoint 的提交流水线。
//!
//! **单写者定序**：一个 `Engine` 只能被事件循环线程独占使用，所有写
//! 都在这一条线程上按到达顺序执行，因此内部没有任何锁，也不需要有
//! 事务隔离——「并发」全部体现在网络层（万级连接共用一条线程）。
//!
//! **两段式提交（这是持久性承诺的全部）**：
//!
//! ```text
//! stage:   translate(只读查状态) → 把 logs 应用到内存状态 → 预生成 reply
//! commit:  WAL append + fsync    → 成功才把 reply 写回 socket
//! ```
//!
//! stage 先改内存是有意的：同一批里两条 `Create` 若都按旧状态分配 ino
//! 会撞号，边算边推进状态才能让批内后续 op 看到前面的结果。代价是
//! 「fsync 失败时内存已超前于磁盘」——此时任何 ack 都是谎言，故
//! [`Engine::commit_batch`] 一旦报错，进程必须立即退出（见返回值的
//! 文档），重启后状态由快照 + WAL 重放重建，不会留下半提交状态。

use crate::engine::apply::translate;
use crate::engine::codec::{Rd, W};
use crate::engine::mem::{Log, State, SNAP_LEGACY_VERSION, SNAP_VERSION};
use crate::engine::model::{Op, Reply};
use crate::engine::wal::{log_payload, Wal};
use crate::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{SystemTime, UNIX_EPOCH};

/// 取读锁。poison 只可能来自 worker 线程 panic（它们只读，不会把状态改成
/// 半截），此时直接取回内部值继续服务，比让整个进程陪着一次无关 panic 死掉
/// 更符合一个元数据服务的可用性目标。
pub fn read_of<'a>(s: &'a Arc<RwLock<State>>) -> RwLockReadGuard<'a, State> {
    s.read().unwrap_or_else(|e| e.into_inner())
}

/// 取写锁（只有写者线程会走到这里），poison 同上。
pub fn write_of<'a>(s: &'a Arc<RwLock<State>>) -> RwLockWriteGuard<'a, State> {
    s.write().unwrap_or_else(|e| e.into_inner())
}

/// 落盘强度。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// 每个已 ack 的 op 都晚于一次覆盖它的 fsync。实现上与 [`Durability::Group`]
    /// 共用同一条路径（一轮一次 fsync 覆盖本轮全部已 stage 的写）：把 fsync
    /// 按连接切开**不给已 ack 的写增加任何持久性**，只是让第 k 条连接多等
    /// (k-1) × fsync 延迟。旧实现正是那样，实测差 6×（DESIGN.md §7.9）。
    /// 保留这个名字只为兼容既有启动参数与测试。
    Always,
    /// 每轮事件循环一次 fsync，覆盖本轮所有已 stage 的写：group commit。
    /// ack 一定晚于 fsync，所以「已确认的写不丢」不变量成立。
    Group,
    /// 不主动 fsync，交给 OS 回写 + 周期 sync。**仅用于测试/基准**，
    /// 掉电可能丢未 ack 之外的写。
    Never,
}

/// 快照文件名（WAL 同目录）。
pub const SNAP_FILE: &str = "state.snap";
/// WAL 文件名。
pub const WAL_FILE: &str = "treedb.wal";
/// 上一代快照的备份文件名（迁移工具写，见 [`migrate_snapshot`]）。
pub const SNAP_BACKUP: &str = "state.snap.v2.bak";

/// 引擎自报状态，`Op::Stats` 的载荷，也是 `treedb-server` 健康检查的来源。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub views: u64,
    pub nodes: u64,
    pub dentries: u64,
    pub locks: u64,
    pub refs: u64,
    pub keys: u64,
    pub audit: u64,
    /// WAL 已分配到的 seq。
    pub seq: u64,
    /// 快照覆盖到的 seq（重放水位）。
    pub snap_seq: u64,
    /// WAL 的**逻辑**长度（已写入的字节数），checkpoint 阈值按它算。
    pub wal_bytes: u64,
    pub ops_total: u64,
    pub writes_total: u64,
    pub syncs_total: u64,
    /// 累计 fsync 耗时（微秒），除以 syncs_total 得均值。
    pub sync_us_total: u64,
    /// 累计 checkpoint 次数。
    pub checkpoints: u64,
    /// 通用表数与总行数（§11 T2）。
    pub tables: u64,
    pub rows: u64,
    /// WAL 文件的**磁盘**长度（st_size，含预分配空洞）。与 `wal_bytes` 是两个数：
    /// 前者是写了多少，后者是占了多大。容量规划看这一个（§8 第 9 条）。
    pub wal_disk_bytes: u64,
    /// 快照文件长度（含尾部 8 字节 seq）。没有快照时为 0。
    /// 恢复时间 = 读它 + 重放 `wal_bytes`，所以这两个数得并列才能算账。
    pub snap_bytes: u64,
    /// 线协议版本（[`crate::engine::PROTOCOL_VERSION`]）。拿它核对客户端与
    /// 服务端是不是同一代字节布局。
    pub proto_version: u16,
}

impl EngineStats {
    pub fn encode(&self, w: &mut W) {
        w.u64(self.views);
        w.u64(self.nodes);
        w.u64(self.dentries);
        w.u64(self.locks);
        w.u64(self.refs);
        w.u64(self.keys);
        w.u64(self.audit);
        w.u64(self.seq);
        w.u64(self.snap_seq);
        w.u64(self.wal_bytes);
        w.u64(self.ops_total);
        w.u64(self.writes_total);
        w.u64(self.syncs_total);
        w.u64(self.sync_us_total);
        w.u64(self.checkpoints);
        // 新字段一律加在尾部：这个结构体没有自己的版本位，前 15 个字的
        // 位置就是它的 ABI，插中间会让旧客户端静默解错。
        w.u64(self.tables);
        w.u64(self.rows);
        w.u64(self.wal_disk_bytes);
        w.u64(self.snap_bytes);
        w.u16(self.proto_version);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(EngineStats {
            views: r.u64()?,
            nodes: r.u64()?,
            dentries: r.u64()?,
            locks: r.u64()?,
            refs: r.u64()?,
            keys: r.u64()?,
            audit: r.u64()?,
            seq: r.u64()?,
            snap_seq: r.u64()?,
            wal_bytes: r.u64()?,
            ops_total: r.u64()?,
            writes_total: r.u64()?,
            syncs_total: r.u64()?,
            sync_us_total: r.u64()?,
            checkpoints: r.u64()?,
            tables: r.u64()?,
            rows: r.u64()?,
            wal_disk_bytes: r.u64()?,
            snap_bytes: r.u64()?,
            proto_version: r.u16()?,
        })
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 启动时崩溃恢复的度量，bin 负者打印它来证明“重放了多少条”不是空话。
#[derive(Debug, Clone, Copy, Default)]
pub struct Recovery {
    pub snap_seq: u64,
    pub wal_records: u64,
    pub durable_seq: u64,
    pub elapsed_ms: u64,
}

/// 引擎写者的独占程度由使用模式决定：
///
/// - **`workers = 1`（默认）**：只有事件循环线程碰状态，读/写锁全部无竞争，
///   成本就是一次原子加减，等价于单线程独占。
/// - **`workers > 1`**：写者仍然只有一个（提交线程，即持有本结构体的那条），
///   前端 worker 线程只取读锁跑 [`Op::is_readonly`] 为真的 op。锁的粒度是
///   **一个 op**，绝不是"一轮"，否则一次 4 ms 的 fsync 会把所有读者卡在门外。
///
/// 状态用 `std::sync` 而不是 `parking_lot`：引擎本体的依赖边界是
/// libc + thiserror + 标准库，为省一次无竞争的自旋把这个 crate 拉进核心依赖
/// 不划算（`parking_lot` 现在只属于过渡档的 PG 池）。
pub struct Engine {
    pub state: Arc<RwLock<State>>,
    wal: Wal,
    dir: PathBuf,
    durability: Durability,
    snap_seq: u64,
    stats: EngineStats,
    recovery: Recovery,
    /// 别的线程（前端 worker）已经执行掉、还没折进 `stats.ops_total` 的只读
    /// op 数。写路径永远只在提交线程上，所以整个引擎只有这一个数需要原子。
    external_ops: Arc<AtomicU64>,
}

impl Engine {
    /// 打开（或创建）数据目录并完成崩溃恢复。
    ///
    /// 恢复顺序：读 `state.snap`（若有）→ 重放 `seq > snap_seq` 的 WAL
    /// 记录 → 截掉坏尾。空目录得到一个全新引擎。
    pub fn open(dir: &Path, durability: Durability) -> Result<Engine> {
        let t0 = std::time::Instant::now();
        std::fs::create_dir_all(dir)?;
        let snap_path = dir.join(SNAP_FILE);
        let mut state = State::new();
        let mut snap_seq = 0u64;
        if snap_path.exists() {
            let raw = std::fs::read(&snap_path)?;
            let mut r = Rd::new(&raw);
            state = State::load(&mut r)?;
            // 快照尾 8 字节是写快照时的 seq 水位。
            if raw.len() < 8 {
                return Err(Error::Codec("snapshot truncated".into()));
            }
            snap_seq = u64::from_le_bytes(raw[raw.len() - 8..].try_into().unwrap());
        }
        let replayed = Wal::replay(&dir.join(WAL_FILE), snap_seq)?;
        for l in &replayed {
            state.apply(l);
        }
        let state = Arc::new(RwLock::new(state));
        // 第二个返回值是扫描得到的 seq：下面统一从 `wal` 重取（空 WAL 时
        // 要靠快照水位接回），这里不用它。
        let (mut wal, _, wal_end) = Wal::open(&dir.join(WAL_FILE))?;
        // checkpoint 刚做完就重启：WAL 是空的，发号水位只能从快照接回来，
        // 否则新记录会从 1 重发、下次恢复时被当成已覆盖而丢。
        if wal_end == 0 && snap_seq > 0 {
            wal.fast_forward(snap_seq);
        }
        let last_seq = wal.durable_seq();
        if last_seq < snap_seq {
            return Err(Error::Io(format!(
                "wal seq {last_seq} behind snapshot {snap_seq}: corrupt data dir"
            )));
        }
        let mut eng = Engine {
            state,
            wal,
            dir: dir.to_path_buf(),
            durability,
            snap_seq,
            stats: EngineStats::default(),
            external_ops: Arc::new(AtomicU64::new(0)),
            recovery: Recovery {
                snap_seq,
                wal_records: replayed.len() as u64,
                durable_seq: last_seq,
                elapsed_ms: t0.elapsed().as_millis() as u64,
            },
        };
        // Stats 反映本进程服务量，故不把恢复期的重放计入 ops/writes；
        // 但 seq 水位必须对齐磁盘，否则重启后第一次 Stats 会回退。
        eng.stats.seq = last_seq;
        eng.stats.snap_seq = snap_seq;
        Ok(eng)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 把「别的线程执行掉的只读 op」计数句柄交给调用方（前端 worker 线程）。
    ///
    /// 单线程模式下这个句柄永远是 0，`snapshot_stats` 折进去也不改变任何数字。
    pub fn attach_external_ops(&mut self) -> Arc<AtomicU64> {
        self.external_ops.clone()
    }

    /// 状态句柄，交给前端 worker 线程取读锁。Arc 克隆，不转移所有权。
    pub fn shared_state(&self) -> Arc<RwLock<State>> {
        self.state.clone()
    }

    /// 状态的一个一致副本：测试拿它做"重放后逐字段相等"的比对，嵌入用法拿它
    /// 做诊断快照。比直接外露 `Arc<RwLock<State>>` 好读，也不会把锁型泄露给测试。
    pub fn state_snapshot(&self) -> State {
        read_of(&self.state).clone()
    }

    pub fn recovery(&self) -> Recovery {
        self.recovery
    }

    pub fn durability(&self) -> Durability {
        self.durability
    }

    /// 当前 WAL 已写入的逻辑字节数（不算预分配空洞）。checkpoint 测试靠它
    /// 确认日志真被截断了。
    pub fn wal_len(&self) -> u64 {
        self.wal.logical_size().unwrap_or(0)
    }

    pub fn snap_seq(&self) -> u64 {
        self.snap_seq
    }

    /// WAL 已 durable 到的 seq：任何 seq <= 此值的写都已 fsync。
    pub fn durable_seq(&self) -> u64 {
        self.wal.durable_seq()
    }

    /// 翻译 op 并把结果 applied 到内存状态。**不做 fsync**，也不发 ack。
    ///
    /// 返回的 `Vec<Log>` 是该 op 需要进 WAL 的记录（只读 op 为空）。
    /// 调用方（事件循环）负责在一轮结束后把本轮所有 logs 交给
    /// [`Engine::commit_batch`]，再把 reply 写回对应连接。
    pub fn exec(&mut self, op: &Op) -> (Vec<Log>, Reply) {
        self.stats.ops_total += 1;
        match op {
            Op::Ping => (vec![], Reply::Pong),
            Op::Stats => (vec![], Reply::Stats(Box::new(self.snapshot_stats()))),
            Op::Checkpoint => match self.checkpoint() {
                Ok(_seq) => (vec![], Reply::Ok),
                Err(e) => (vec![], Reply::err(crate::engine::model::errno::EIO, e.to_string())),
            },
            Op::Shutdown => (vec![], Reply::Ok),
            _ => {
                // 只读：取读锁即可，写者可以插到两条读中间。写：translate+apply
                // 整段在写锁里完成，但锁**绝不横跨 fsync** —— 那会把所有读者
                // 卡在机械盘的一次寻道外面。
                let (logs, reply) = if op.is_readonly() {
                    let s = read_of(&self.state);
                    translate(&s, now_ms(), op)
                } else {
                    let mut s = write_of(&self.state);
                    let r = translate(&s, now_ms(), op);
                    for l in &r.0 {
                        s.apply(l);
                    }
                    r
                };
                if !logs.is_empty() {
                    self.stats.writes_total += 1;
                }
                (logs, reply)
            }
        }
    }

    /// worker 线程的只读执行：只拿读锁、只跑 `translate`，不动 WAL、不动 stats
    /// （计数由本结构体所在线程折，见 [`Engine::attach_external_ops`]）。
    ///
    /// 第二个返回值是 `translate` 产出的 logs —— 对只读 op 恒为空。一旦非空
    /// 就说明 [`Op::is_readonly`] 分派错了，调用方必须当内部错误报出去：在
    /// worker 里把 logs 丢掉等于丢一次已被确认的写。
    pub fn exec_read(state: &Arc<RwLock<State>>, op: &Op) -> (Reply, Vec<Log>) {
        let s = read_of(state);
        let (logs, reply) = translate(&s, now_ms(), op);
        (reply, logs)
    }

    /// 内嵌用法（不经网络）：一个 op 走完 stage → WAL → 返回 ack。
    ///
    /// 客户端如果选同进程嵌入而非连 TCP，这就是入口；测试也走它，
    /// 能把“语义对不对”与“网络对不对”分开验。服务端事件循环不用它
    /// （那里需要跨连接 group commit）。
    pub fn submit(&mut self, op: &Op) -> Result<Reply> {
        let (logs, reply) = self.exec(op);
        if logs.is_empty() {
            return Ok(reply);
        }
        self.commit_batch(&[logs.as_slice()])?;
        Ok(reply)
    }

    /// 一轮 event loop 的收口：把本轮所有 op 的日志一次 write + 一次 fsync。
    ///
    /// `Always` 与 `Group` 在这里走同一条路径，区别只在[`Durability`]的文档语义；
    /// 按连接逐个 fsync 的旧实现已于实测后废除（§7.9：机械盘 1 716 → 接近
    /// `group` 那一档，尾延迟从 p99 65–556 ms 回到同一个量级）。
    ///
    /// 返回 Err 时**内存状态已经超前于磁盘**，调用方必须停止服务并退出
    /// 进程；继续 ack 会违反「已确认的写不丢」。
    pub fn commit_batch(&mut self, groups: &[&[Log]]) -> Result<()> {
        let t0 = std::time::Instant::now();
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        for g in groups {
            for l in *g {
                payloads.push(log_payload(l));
            }
        }
        if payloads.is_empty() {
            return Ok(());
        }
        let last = match self.durability {
            Durability::Never => self.wal.append_nodur(&payloads)?,
            _ => self.wal.commit(&payloads)?,
        };
        let us = t0.elapsed().as_micros() as u64;
        if self.durability != Durability::Never {
            self.stats.syncs_total += 1;
            self.stats.sync_us_total += us;
        }
        self.stats.seq = last;
        Ok(())
    }

    /// `Durability::Never` / `Group` 下把缓冲刷下去（周期调用，不阻塞本轮 ack
    /// 语义由调用方保证——只在 Never 模式用）。
    pub fn sync_now(&mut self) -> Result<()> {
        if self.durability == Durability::Never {
            self.wal.sync()?;
            self.stats.seq = self.wal.durable_seq();
        }
        Ok(())
    }

    /// 当前 WAL 逻辑字节数。给事件循环的 auto-checkpoint 判断用。
    ///
    /// 为什么单独开这个口而不是用 [`Engine::snapshot_stats`]：后者要把那几张表
    /// 各数一遍（其中 dentry 条数在两层表下是 O(目录数)、通用表行数要扫全部
    /// 表），而 auto-checkpoint 每轮只需要这一个数。默认档 64 MB 下那是每轮
    /// 白付一次全表计数。
    pub fn wal_bytes(&self) -> u64 {
        self.wal.logical_size().unwrap_or(0)
    }

    /// 全量快照 + WAL 重置。
    ///
    /// 顺序：state.save → tmp → fsync(tmp) → rename → fsync(dir) → wal.reset。
    /// 任一步崩溃都只可能留下「旧快照 + 更长的 WAL」，重放仍能还原。
    pub fn checkpoint(&mut self) -> Result<u64> {
        // 水位取当前已 durable 的 seq：内存可能比磁盘更靠前（本轮 commit
        // 尚未发生），把未落盘的记录当成"快照已覆盖"会丢数据。
        let seq = self.wal.durable_seq();
        let mut w = W::new();
        {
            // 快照只需要一个一致的时间切面：读锁挡住写者 apply 就够了，不需要
            // 挡 fsync/网络。大状态机下这是唯一会被拉长的读临界区。
            read_of(&self.state).save(&mut w);
        }
        let mut buf = w.finish();
        buf.extend_from_slice(&seq.to_le_bytes());
        let tmp = self.dir.join(format!("{SNAP_FILE}.tmp"));
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, self.dir.join(SNAP_FILE))?;
        if let Ok(d) = File::open(&self.dir) {
            let _ = d.sync_all();
        }
        self.wal.reset(seq)?;
        self.snap_seq = seq;
        self.stats.snap_seq = seq;
        self.stats.checkpoints += 1;
        Ok(seq)
    }

    /// 给 `Op::Stats` 用的即时快照。
    pub fn snapshot_stats(&mut self) -> EngineStats {
        // 先把前端线程的量折进来。`swap(0)` 而不是 `load + fetch_sub`：后者在
        // 两步之间插入的增量会被减两次（u64 减穿就是回绕成一个天文数字）。
        self.stats.ops_total += self.external_ops.swap(0, Ordering::Relaxed);
        // 先把计数在 Guard 作古前抽出来：guard 借的是 `&self.state`，而下面
        // 要改 `self.stats`，不拆开就过不了借用检查。
        let (views, nodes, dentries, locks, refs, keys, audit, tables, rows) = {
            let s = read_of(&self.state);
            (
                s.views.len() as u64,
                s.nodes.len() as u64,
                // 两层表的 len() 是目录数，报数要的是 dentry 条数。
                s.dentry_count() as u64,
                s.locks.len() as u64,
                s.refs.len() as u64,
                s.keys.len() as u64,
                s.audit.len() as u64,
                s.tables.len() as u64,
                // 同理：`rows.len()` 是有行的表数。
                s.row_count() as u64,
            )
        };
        self.stats.views = views;
        self.stats.nodes = nodes;
        self.stats.dentries = dentries;
        self.stats.locks = locks;
        self.stats.refs = refs;
        self.stats.keys = keys;
        self.stats.audit = audit;
        self.stats.tables = tables;
        self.stats.rows = rows;
        self.stats.seq = self.wal.durable_seq().max(self.stats.seq);
        self.stats.snap_seq = self.snap_seq;
        self.stats.wal_bytes = self.wal.logical_size().unwrap_or(0);
        self.stats.wal_disk_bytes = self.wal.disk_size().unwrap_or(0);
        // 快照大小直接 stat 文件，不重解内容：这个数只用于容量规划，
        // 而读一遍快照等于把整个状态拷进内存再扫一次。
        self.stats.snap_bytes = std::fs::metadata(self.dir.join(SNAP_FILE))
            .map(|m| m.len())
            .unwrap_or(0);
        self.stats.proto_version = crate::engine::PROTOCOL_VERSION;
        self.stats.clone()
    }

    /// 已写但未 fsync 的记录数对应的 seq 水位（测试用）。
    #[allow(dead_code)]
    pub fn next_seq(&self) -> u64 {
        self.stats.seq
    }
}

// ---- 快照升级（§6.5）------------------------------------------------------------
//
// 这一段的存立理由：`SNAP_VERSION` 从 2 升到 3 把已有数据目录挡在门外，而
// 引擎不会替旧文件猜布局。它**只在离线工具里被调用**（`treedb-migrate`），
// 常规启动路径一个字节也不走这里 —— `Engine::open` 永远用 [`State::load`]，
// 那个入口不开兼容口子。

/// 快照文件的体检结果。`treedb-migrate` 不带 `--apply` 时打的就是它，也是它动手前
/// 的唯一判断依据：只有 `version == SNAP_LEGACY_VERSION` 才允许改写。
#[derive(Debug, Clone, Default)]
pub struct SnapshotInfo {
    /// 快照文件路径（不存在也填，供上层报账）。
    pub path: PathBuf,
    /// 目录里有没有快照文件。没有 = 新目录，本来没什么可迁移。
    pub present: bool,
    /// 文件自报的版本位。
    pub version: u32,
    /// 本二进制会写的版本。两个数不等就是「这个目录开不了门」。
    pub written_by_this_build: u32,
    /// 正文能否被当前代码解释。解不开时 `note` 给出原因，计数字段全 0
    /// 且不应当被引用。
    pub decodable: bool,
    pub note: Option<String>,
    /// 快照尾 8 字节：这份快照覆盖到哪个 seq。
    pub snap_seq: u64,
    pub bytes: u64,
    /// WAL 文件的磁盘长度（st_size）。迁移不碰 WAL，所以这个数在迁移前后必须
    /// 一致；它是「日志一个字节没动」那句承诺的可验证据。
    pub wal_disk_bytes: u64,
    pub views: u64,
    pub nodes: u64,
    pub dentries: u64,
    pub locks: u64,
    pub refs: u64,
    pub keys: u64,
    pub audit: u64,
    pub defs: u64,
    pub placement: u64,
    pub tables: u64,
    pub rows: u64,
}

impl SnapshotInfo {
    /// 已经是本二进制认识的当前布局，不需要也不应该动手。
    pub fn is_current(&self) -> bool {
        self.present && self.version == SNAP_VERSION
    }

    /// 处于「能升、该升」的那一档。
    pub fn needs_migration(&self) -> bool {
        self.present && self.version == SNAP_LEGACY_VERSION && self.decodable
    }
}

fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// 读快照头并尽量把计数填上。**不写任何东西**，也不打开 WAL。
///
/// 认不认识的版本都返回 `Ok`：只读排查的价值就是「不动手也知道目录里是什么」，
/// 而版本不认识的目录本来就没有可迁移的语义，把它做成 panic 级的错误只会让
/// 运维在只读排查时也多一个出口。真正的「要不要改」由调用方看 `note` 决定。
pub fn probe_snapshot(dir: &Path) -> Result<SnapshotInfo> {
    let snap = dir.join(SNAP_FILE);
    let mut info = SnapshotInfo {
        path: snap.clone(),
        written_by_this_build: SNAP_VERSION,
        wal_disk_bytes: file_len(&dir.join(WAL_FILE)),
        ..Default::default()
    };
    if !snap.exists() {
        info.note = Some("no snapshot file: a fresh data dir starts on the current layout".into());
        return Ok(info);
    }
    info.present = true;
    info.bytes = file_len(&snap);
    let raw = std::fs::read(&snap).map_err(|e| Error::Io(format!("read {}: {e}", snap.display())))?;
    info.version = State::snapshot_version(&raw)?;
    if info.version != SNAP_VERSION && info.version != SNAP_LEGACY_VERSION {
        info.note = Some(format!(
            "snapshot reports version {}, which this build neither writes nor migrates",
            info.version
        ));
        return Ok(info);
    }
    // 走迁移专用的严格解码：整个文件必须正好被布局吃完（尾巴只剩 8 字节 seq）。
    // 用它而不是 [`State::load`] 是为了让只读体检在 v2 目录上也能报出计数。
    match State::load_for_migration(&mut Rd::new(&raw)) {
        Ok(s) => {
            info.decodable = true;
            info.snap_seq = u64::from_le_bytes(raw[raw.len() - 8..].try_into().unwrap());
            info.views = s.views.len() as u64;
            info.nodes = s.nodes.len() as u64;
            info.dentries = s.dentry_count() as u64;
            info.locks = s.locks.len() as u64;
            info.refs = s.refs.len() as u64;
            info.keys = s.keys.len() as u64;
            info.audit = s.audit.len() as u64;
            info.defs = s.defs.len() as u64;
            info.placement = s.placement.len() as u64;
            info.tables = s.tables.len() as u64;
            info.rows = s.row_count() as u64;
        }
        Err(e) => {
            info.note = Some(format!("snapshot body cannot be decoded: {e}"));
        }
    }
    Ok(info)
}

/// [`migrate_snapshot`] 的结果。三态而不是一个 bool：脚本要能区分
/// 「无事可做」与「真的改了东西」，两者的退出码一样但日志完全不一样。
#[derive(Debug, Clone)]
pub enum MigrationOutcome {
    /// 目录里还没有快照：新目录，本来就没东西可升。
    NoSnapshot(PathBuf),
    /// 已经是当前布局（包括迁移后再跑一次的幂等情形）。
    AlreadyCurrent(SnapshotInfo),
    Migrated {
        before: SnapshotInfo,
        after: SnapshotInfo,
        /// 原 v2 快照的逐字节副本。WAL 没被碰过，所以「备份文件 + 现有 WAL」
        /// 仍是旧二进制可完整还原的一组件 —— 这就是回滚路径。
        backup: PathBuf,
        /// 搬运前后必须相等的那个 seq 水位。
        seq: u64,
        elapsed_ms: u64,
    },
}

/// 把旧布局的快照就地重写成当前布局：读旧 → 内存状态 → 按新布局写回。
///
/// **WAL 一个字节不动**。这不只是为了少做一步：不碰日志才保住「已 ack 的写
/// 不会因迁移而丢」与「回滚只要把备份文件挪回去」两条，而那些只要把
/// 快照重放后做一次 checkpoint 就得付代价（`wal.reset` 会把旧一代记录物理抹掉）。
///
/// 同一个理由也定了回滚窗口的长度：只要新二进制还没做过第一次 checkpoint，
/// 把备份拷回去跑旧二进制就是无损的（那些写还在 WAL 里）；checkpoint 之后就只剩
/// 有损一条路。`benches/rehearse_migrate.sh` 拿两个真二进制把这两个窗口都量过。
///
/// 写回用的是 [`Engine::checkpoint`] 那同一条顺序：先落盘 `.tmp` 并 fsync，再
/// rename，再 fsync 目录。任一步崩溃只会留下「旧快照 + 完整 WAL」，下次启动
/// 仍然是旧行为。动手前还有一道自检：把即将写出的字节先解一遍并与手上的状态
/// 逐字段比，不等就直接失败 —— 此刻原文件一个字节都没改过。
///
/// `force` 只影响一件事：已存在备份文件时要不要覆盖。默认不覆盖，因为那份
/// 备份可能是上一轮唯一可回滚的原件。
///
/// **前提：服务端已停**。本函数不 flock（服务端也没持文件锁，锁不住任何东西），
/// 只在写回前再读一次原文件与手上的字节比对：并发写者会在这里露馅。那是
/// best-effort 的哨兵，不是互斥，不要把「没报错」当成「与并发安全」。
pub fn migrate_snapshot(dir: &Path, force: bool) -> Result<MigrationOutcome> {
    let t0 = std::time::Instant::now();
    let before = probe_snapshot(dir)?;
    if !before.present {
        return Ok(MigrationOutcome::NoSnapshot(before.path.clone()));
    }
    if !before.decodable {
        return Err(Error::Config(format!(
            "refusing to touch {}: {}",
            before.path.display(),
            before.note.unwrap_or_else(|| "undecodable".into())
        )));
    }
    if before.version == SNAP_VERSION {
        return Ok(MigrationOutcome::AlreadyCurrent(before));
    }
    if before.version != SNAP_LEGACY_VERSION {
        return Err(Error::Config(format!(
            "no migration path from snapshot version {} to {SNAP_VERSION}: this tool knows \
             exactly one previous generation ({SNAP_LEGACY_VERSION})",
            before.version
        )));
    }

    let raw = std::fs::read(&before.path).map_err(|e| Error::Io(format!("read {}: {e}", before.path.display())))?;
    if raw.len() < 12 {
        return Err(Error::Codec(format!(
            "snapshot {} is {} bytes, too short to hold a body plus the seq trailer",
            before.path.display(),
            raw.len()
        )));
    }
    let state = State::load_for_migration(&mut Rd::new(&raw))?;
    // 尾巴照搬：这个数是「快照覆盖到哪个 seq」，重算一次就是自己发明一个水位，
    // 下次启动按它跳记录就会丢写。
    let trailer: [u8; 8] = raw[raw.len() - 8..].try_into().unwrap();
    let seq = u64::from_le_bytes(trailer);

    // 动手前的自检：编 → 解 → 逐字段比。这里用的解法是严格入口（不是迁移入口），
    // 等于把「新二进制明天能不能开门」先问一遍。
    let mut w = W::new();
    state.save(&mut w);
    let mut out = w.finish();
    out.extend_from_slice(&trailer);
    let check = State::load(&mut Rd::new(&out))
        .map_err(|e| Error::Codec(format!("the rewritten snapshot does not reload: {e}")))?;
    if check != state {
        return Err(Error::Codec(
            "rewritten snapshot decodes to a different state than the source: refusing to write".into(),
        ));
    }

    // 哨兵：源文件在我们手上变过，说明有写者。此刻还没写任何东西，直接退出
    // 就是零代价。
    let again = std::fs::read(&before.path).map_err(|e| Error::Io(format!("re-read {}: {e}", before.path.display())))?;
    if again != raw {
        return Err(Error::Config(format!(
            "{} changed while the migration was reading it: a server is still writing this \
             directory, stop it first",
            SNAP_FILE
        )));
    }

    // 备份先落盘。没有备份就改写是单程票：万一这份旧文件是最后一份原件，
    // 覆盖掉它就把回滚路径一起抹了。
    let backup = dir.join(SNAP_BACKUP);
    if backup.exists() && !force {
        return Err(Error::Config(format!(
            "backup {} already exists; re-run with --force to replace it",
            backup.display()
        )));
    }
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&backup)
            .map_err(|e| Error::Io(format!("open backup {}: {e}", backup.display())))?;
        f.write_all(&raw)?;
        f.sync_all()?;
    }

    let tmp = dir.join(format!("{SNAP_FILE}.tmp"));
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| Error::Io(format!("open {}: {e}", tmp.display())))?;
        f.write_all(&out)?;
        f.sync_all()?;
    }
    // 再确认一次（上面那次比对到这里已经过了几次磁盘操作），失败就清掉 `.tmp`：
    // 留下一个没人认的临时文件只会让下一个运维不确定它能不能删。
    let again = std::fs::read(&before.path).map_err(|e| Error::Io(format!("re-read {}: {e}", before.path.display())))?;
    if again != raw {
        let _ = std::fs::remove_file(&tmp);
        return Err(Error::Config(format!(
            "{} changed between the backup and the swap: refusing to rename over a live writer",
            SNAP_FILE
        )));
    }
    std::fs::rename(&tmp, &before.path)?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }

    // 写后读回：不只看字节相等，还按严格入口再解一遍并确认版本位已经对上。
    // 这一步跑完才敢说「新二进制现在能开这个目录」。
    let written = std::fs::read(&before.path).map_err(|e| Error::Io(format!("re-read {}: {e}", before.path.display())))?;
    if written != out {
        return Err(Error::Codec(
            "the snapshot on disk is not the bytes the migration wrote".into(),
        ));
    }
    let after = probe_snapshot(dir)?;
    if !after.is_current() || after.snap_seq != seq {
        return Err(Error::Codec(format!(
            "post-migration probe disagrees: version {}, seq {} (expected {SNAP_VERSION}, {seq})",
            after.version, after.snap_seq
        )));
    }
    Ok(MigrationOutcome::Migrated {
        before,
        after,
        backup,
        seq,
        elapsed_ms: t0.elapsed().as_millis() as u64,
    })
}
