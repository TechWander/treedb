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
use crate::engine::mem::{Log, State};
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
    /// 每个写 op 一次 fsync：最安全，吞吐最低（约 1-3 万 ops/s 量级）。
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
    pub wal_bytes: u64,
    pub ops_total: u64,
    pub writes_total: u64,
    pub syncs_total: u64,
    /// 累计 fsync 耗时（微秒），除以 syncs_total 得均值。
    pub sync_us_total: u64,
    /// 累计 checkpoint 次数。
    pub checkpoints: u64,
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
    /// 返回 Err 时**内存状态已经超前于磁盘**，调用方必须停止服务并退出
    /// 进程；继续 ack 会违反「已确认的写不丢」。
    pub fn commit_batch(&mut self, groups: &[&[Log]]) -> Result<()> {
        let t0 = std::time::Instant::now();
        // Always：一个 op 一次 fsync，ack 粒度最细；Group：本轮一次。
        let n_syncs = match self.durability {
            Durability::Always => groups.len() as u64,
            _ => 1,
        };
        let last = if self.durability == Durability::Always {
            let mut last = self.wal.durable_seq();
            for g in groups {
                let payload: Vec<Vec<u8>> = g.iter().map(log_payload).collect();
                if payload.is_empty() {
                    continue;
                }
                last = self.wal.commit(&payload)?;
            }
            last
        } else {
            let mut payloads: Vec<Vec<u8>> = Vec::new();
            for g in groups {
                for l in *g {
                    payloads.push(log_payload(l));
                }
            }
            if payloads.is_empty() {
                return Ok(());
            }
            match self.durability {
                Durability::Never => self.wal.append_nodur(&payloads)?,
                _ => self.wal.commit(&payloads)?,
            }
        };
        if n_syncs == 0 {
            return Ok(());
        }
        let us = t0.elapsed().as_micros() as u64;
        if self.durability != Durability::Never {
            self.stats.syncs_total += n_syncs;
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
        let (views, nodes, dentries, locks, refs, keys, audit) = {
            let s = read_of(&self.state);
            (
                s.views.len() as u64,
                s.nodes.len() as u64,
                s.dentries.len() as u64,
                s.locks.len() as u64,
                s.refs.len() as u64,
                s.keys.len() as u64,
                s.audit.len() as u64,
            )
        };
        self.stats.views = views;
        self.stats.nodes = nodes;
        self.stats.dentries = dentries;
        self.stats.locks = locks;
        self.stats.refs = refs;
        self.stats.keys = keys;
        self.stats.audit = audit;
        self.stats.seq = self.wal.durable_seq().max(self.stats.seq);
        self.stats.snap_seq = self.snap_seq;
        self.stats.wal_bytes = self.wal.logical_size().unwrap_or(0);
        self.stats.clone()
    }

    /// 已写但未 fsync 的记录数对应的 seq 水位（测试用）。
    #[allow(dead_code)]
    pub fn next_seq(&self) -> u64 {
        self.stats.seq
    }
}
