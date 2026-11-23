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
use crate::engine::archive::Archive;
use crate::engine::codec::{Rd, W};
use crate::engine::dump::Dump;
use crate::engine::mem::{Log, State, SNAP_LEGACY_VERSION, SNAP_VERSION};
use crate::engine::model::{Op, Reply};
use crate::engine::wal::{log_payload, Wal};
use crate::error::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::ops::Bound;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::JoinHandle;
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

impl Durability {
    /// 解析启动参数里的名字。
    ///
    /// **拼错不许静默降级**：以前这里是个 `_ => Group`，于是
    /// `--durability alway`（少一个 s）会默默跑成 group，而
    /// `--durability nevr`（本该是 never）也会跑成 group —— 后者会让
    /// “never 档的上限”被测成一个多付 fsync 的数，两种都让数字失去意义。
    /// 只认三个字面量，其余一律回错误由 bin 以退出码 2 挡下来。
    pub fn parse(s: &str) -> Result<Durability> {
        match s {
            "always" => Ok(Durability::Always),
            "group" => Ok(Durability::Group),
            "never" => Ok(Durability::Never),
            other => Err(Error::Config(format!(
                "unknown --durability {other:?}: expected one of always | group | never"
            ))),
        }
    }
}

/// 快照文件名（WAL 同目录）。
pub const SNAP_FILE: &str = "state.snap";
/// WAL 文件名。
pub const WAL_FILE: &str = "treedb.wal";
/// 数据目录互斥锁文件名（[`DirLock`]）。
pub const LOCK_FILE: &str = ".lock";
/// fencing token 文件名（§6.6 第 3 条）：内容是一个十进制整数 + `\n`。
///
/// 它不在快照里，也不在 WAL 里 —— 故意做成一个独立的、人眼能 `cat` 的小文件：
/// ① 提升它不需要碰任何数据面字节格式（fencing 是独立文件，快照布局没动）；
/// ② 冷拷贝/副本目录自然带着它走；
/// ③ 一个目录被人误当“空白副本”重用之前，`cat fence` 就能看见它已经被提升过。
/// 代价是它不在 WAL 的事务边界里 —— 但 fencing 的用处就是“比数据更早一步知道
/// 谁有权写”，它先落盘、再由数据追上，这个顺序是刻意的。
pub const FENCE_FILE: &str = "fence";
/// 上一代快照的备份文件名（迁移工具写，见 [`migrate_snapshot`]）。
///
/// 名字里的 `v2` 是历史：它始于 v1 → v2 那次升级，后来每代都沿用同一个文件名。
/// 故意不改 —— 运维的回滚脚本、已经就位的旧备份都指着这个名字，而它到底是哪一代
/// 备份只取决于迁移前那份 `state.snap` 自报的版本位。
pub const SNAP_BACKUP: &str = "state.snap.v2.bak";

/// 读数据目录的 fencing token。文件不存在 = 0（从未被提升过）。
///
/// 内容不合法直接报错而不是当作 0：一个坏掉的令牌被读成 0，等于把一个已被
/// 提升过的目录重新降级成可被旧主驱动的副本，而这种错只能在脑裂时暴露。
///
/// 公开是因为 `treedb-standby` 要在 `serve` 之前把令牌打进 banner，而那时
/// 引擎还没打开（[`Engine::open`] 会去抢目录锁）；读一个小文件不需要那把锁。
pub fn read_fence(dir: &Path) -> Result<u64> {
    let path = dir.join(FENCE_FILE);
    let raw = match std::fs::read_to_string(&path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(Error::Io(format!("read {}: {e}", path.display()))),
    };
    raw.trim().parse::<u64>().map_err(|e| {
        Error::Config(format!(
            "{} is not a fencing token ({e}): refusing to start on an unknown fence \
             (a corrupt token must not read as 0 — that would re-enable a demoted primary)",
            path.display()
        ))
    })
}

/// 引擎自报状态，`Op::Stats` 的载荷，也是 `treedb-server` 健康检查的来源。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EngineStats {
    pub views: u64,
    pub nodes: u64,
    pub dentries: u64,
    pub locks: u64,
    pub refs: u64,
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
    /// 本实例是只读副本（由 [`Engine::open_replica`] 置位）。运维拿它一眼分清
    /// 主与副本 —— 两者的 `seq` 长得很像，但一个是发的、一个是追的。
    pub replica: bool,
    /// 主最近一次报上来的 durable 水位；不是副本自己算的。
    pub replica_primary_seq: u64,
    /// `replica_primary_seq - 本地 durable_seq`：副本落后多少条。运维面板上
    /// 那个唯一需要报警的数（`replica_primary_seq` 未拿到前为 0）。
    pub replica_lag: u64,
    /// 追平落地的记录数。与 `writes_total` 差别在于：后者数的是客户端写 op，
    /// 副本一个也没有，它的应用量只能单开一个口。
    pub replica_applied: u64,
    /// 因 gap 重装快照的次数。每次重装 = 下完一份快照 + 从头追日志，主
    /// checkpoint 得比副本追得快才会有；一直涨就是副本跟不上了。
    pub replica_resyncs: u64,
    /// 拉取/应用失败的轮次（连不上主、记录不连续、解码不过……）。
    /// 不数这个的话，一个静默断线的副本跟一个空闲副本在 `Stats` 里一模一样。
    pub replica_errors: u64,
}

impl EngineStats {
    pub fn encode(&self, w: &mut W) {
        w.u64(self.views);
        w.u64(self.nodes);
        w.u64(self.dentries);
        w.u64(self.locks);
        w.u64(self.refs);
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
        w.bool(self.replica);
        w.u64(self.replica_primary_seq);
        w.u64(self.replica_lag);
        w.u64(self.replica_applied);
        w.u64(self.replica_resyncs);
        w.u64(self.replica_errors);
    }
    pub fn decode(r: &mut Rd) -> Result<Self> {
        Ok(EngineStats {
            views: r.u64()?,
            nodes: r.u64()?,
            dentries: r.u64()?,
            locks: r.u64()?,
            refs: r.u64()?,
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
            replica: r.bool()?,
            replica_primary_seq: r.u64()?,
            replica_lag: r.u64()?,
            replica_applied: r.u64()?,
            replica_resyncs: r.u64()?,
            replica_errors: r.u64()?,
        })
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 数据目录的进程间互斥。
///
/// 两个进程开同一个目录不会看到任何报错，只会各自重放同一份 WAL、各自
/// checkpoint、各自 `wal.reset`：谁后 rename 谁赢，先那个以为自己的写已落盘。
/// 这种损坏事后无法区分于“盘坏了”，所以拿 `flock` 在开门时就把它变成
/// 一个启动错误。
///
/// 锁是 `LOCK_EX | LOCK_NB`，落在目录里的 `.lock` 上：非阻塞是因为“等另一个
/// 实例退出”不是运维想要的语义（他会以为是卡住），而锁跟着 fd 走，进程怎么
/// 死的（包括 `kill -9`）都自动释放。
pub(crate) struct DirLock {
    /// 只为持有 fd：`flock` 锁在 open file description 上，关掉即释放。
    _file: File,
    /// 注册表里的键，`Drop` 时据此摘除。
    key: PathBuf,
}

/// 本进程当前持锁的目录（规范化路径）。
///
/// `flock` 认的是 open file description，而同进程两次 `open` 就是两份
/// description，互相冲突；更麻烦的是 `Command::spawn` fork 出的子进程在
/// exec 之前会短暂继承同一份拷贝——这期间父进程 `close` 并不真正解锁。
/// 只靠 `flock` 判「同进程重复打开」会得到时有时无的假冲突（测试串行必过、
/// 并行随机失败）。注册表把这条判据变成本进程内的确定性事实，`flock` 只负责
/// 跨进程那一半。
static HELD: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// fork 窗口的重试预算：5 ms × 20 ≈ 100 ms。子进程 exec 通常在一毫秒内完
/// 成，但要给满负载下的调度留余量；真正被别的进程占着的目录仍会立刻报错
/// （pid 对不上就不重试）。
const FORK_WAIT_TRIES: u32 = 20;

impl Drop for DirLock {
    fn drop(&mut self) {
        HELD.lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|p| p != &self.key);
    }
}

impl DirLock {
    pub(crate) fn acquire(dir: &Path) -> Result<DirLock> {
        // 规范化后再比：`./x`、`x`、软链指向的同一目录必须算同一个目录。
        let key = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
        if HELD
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&key)
        {
            return Err(Error::Config(format!(
                "data dir {} is already locked by this process (pid {}): two engines on one \
                 directory would each replay the same WAL and overwrite each other's snapshots",
                dir.display(),
                std::process::id(),
            )));
        }
        let path = dir.join(LOCK_FILE);
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(|e| Error::Io(format!("open {}: {e}", path.display())))?;
        let mut tries = 0;
        loop {
            if unsafe { libc::flock(f.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } == 0 {
                break;
            }
            let e = std::io::Error::last_os_error();
            let pid = std::fs::read_to_string(&path).unwrap_or_default();
            let pid = pid.trim();
            // pid 是本进程却不占注册表席位，只可能是 fork 出来的子进程还没
            // exec 掉继承来的 fd：等它，而不是把一次正常重启判成目录被占。
            if !pid.is_empty() && pid == std::process::id().to_string() && tries < FORK_WAIT_TRIES {
                tries += 1;
                std::thread::sleep(std::time::Duration::from_millis(5));
                continue;
            }
            let who = if pid.is_empty() { "unknown" } else { pid };
            return Err(Error::Config(format!(
                "data dir {} is already locked ({e}): pid {who} holds it; two engines on one \
                 directory would each replay the same WAL and overwrite each other's snapshots",
                dir.display(),
            )));
        }
        // 把 pid 写进去：拿到 EWOULDBLOCK 的人第一个问题就是“谁占的”。
        // 写失败不影响互斥本身（锁已拿到），故不把错误上抛。
        f.set_len(0).ok();
        let _ = f.write_all(format!("{}\n", std::process::id()).as_bytes());
        HELD.lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(key.clone());
        Ok(DirLock { _file: f, key })
    }
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
    /// 只读副本模式：真写一律 `EROFS` 挡在 [`Engine::exec`]（§6.6）。
    replica: bool,
    /// 主最近一次 `WalBatch` 报上来的 durable 水位，只为算 `replica_lag`。
    replica_primary: u64,
    /// fencing token（§6.6 第 3 条）：本目录「是谁在写、写到第几任」的令牌。
    /// 启动时从 [`FENCE_FILE`] 读，副本见到更大的值会抬上去（先落盘再使用）。
    fence: u64,
    /// 数据目录的进程间互斥。字段只为活着（drop = 解锁）。
    _lock: DirLock,
    /// WAL 归档目录（§6.11）。`None` = 不归档，行为与引入归档之前逐字一致。
    archive: Option<PathBuf>,
    /// 正在后台编的一份快照（§8 第 7 条）。有它时自动档不再派新的；
    /// 退出、显式 checkpoint 与副本装快照都会先把它收掉。
    inflight: Option<Inflight>,
    /// 过期租约清扫扫到了哪一格（§8 第 17 条）。`None` = 从头开始。
    ///
    /// 只在内存里：它是「这一轮从哪儿接着看」的进度，不是状态的一部分，
    /// 重启后从头发一遍只是多扫一圈。
    lock_sweep_cur: Option<(u32, u64, Vec<u8>)>,
}

/// 一次过期租约清扫最多看多少条（§8 第 17 条）。
///
/// 清扫跑在提交线程上，所以单轮的代价必须与表大小无关：1024 条 × 一次
/// `BTreeMap` 迭代是微秒级，而一整圈的成本是 `ceil(n/1024)` 轮摊出来的。
pub const LOCK_SWEEP_BATCH: usize = 1024;

/// 一份正在后台生成的快照。
struct Inflight {
    /// 要写进快照尾巴的水位，也是回来之后 `truncate_before` 的切割点。
    seq: u64,
    handle: JoinHandle<Result<u64>>,
}

/// 旁路快照的全部工作：读 `state.snap` → 重放 `(snap_seq, upto]` 的 WAL →
/// 编成新快照 → tmp / fsync / rename。
///
/// **不碰活状态**，因此不持任何读锁：这正是「停顿与状态大小无关」的来源。
/// 读的是磁盘上那两个文件，与 `Engine::open` 的恢复路径是同一份语义；写入端
/// 同时在追加 WAL 也没关系——只要 `upto` 之前的字节都落了盘（`durable_seq`
/// 的定义），按帧顺序读到那里就是完整的。
///
/// 无归档时失败只会留下「旧快照 + 更长的 WAL」，与崩溃恢复看到的形状一样。
/// 开了归档（§6.11）则多一步：**在 `write_snapshot` 之前**把 `(snap_seq, upto]`
/// 的 WAL 段与水位的 `state.snap` 拷进归档目录。顺序不可换——这一行之后的
/// `state.snap` 已是水位 `upto` 的那一份，水位 `snap_seq` 那份的字节只在归档
/// 里还活着；而归档失败要让整份快照失败，让 `truncate_before` 不执行，历史
/// 不能因为「归档盘满了」而静默丢掉。
fn build_snapshot_from_disk(dir: &Path, upto: u64, archive: Option<&Path>) -> Result<u64> {
    let snap_path = dir.join(SNAP_FILE);
    let (mut state, snap_seq) = if snap_path.exists() {
        let raw = std::fs::read(&snap_path)?;
        let (body, seq) = State::snapshot_split(&raw)?;
        (State::load(&mut Rd::new(body))?, seq)
    } else {
        (State::new(), 0u64)
    };
    if upto < snap_seq {
        return Err(Error::Io(format!(
            "background snapshot target {upto} is behind the on-disk snapshot {snap_seq}"
        )));
    }
    let mut logs = Wal::replay(&dir.join(WAL_FILE), snap_seq)?;
    let need = (upto - snap_seq) as usize;
    if logs.len() < need {
        return Err(Error::Io(format!(
            "background snapshot wanted {need} wal record(s) past {snap_seq}, log has {}",
            logs.len()
        )));
    }
    // 多出来的是取水位之后才进的写：它们不进这份快照，留给日志（
    // `truncate_before` 会把它们原样搬到新日志里）。
    logs.truncate(need);
    for l in &logs {
        state.apply(l);
    }
    // 归档必须在 `write_snapshot` 之前，理由见函数文档。
    if let Some(ar) = archive {
        Archive::new(ar).record(&dir.join(WAL_FILE), snap_seq, upto, &snap_path)?;
    }
    write_snapshot(dir, &state, upto)?;
    Ok(upto)
}

/// 把一份状态写成 `state.snap`：正文与水位一起编码，`.tmp` → `fsync` →
/// `rename` → 刷父目录。
///
/// 同步 checkpoint、旁路快照与 `restore_dump` 共用这一处 —— 三条路径写下去的
/// 字节必须同形，否则「换一种方式落盘」就会变成隐性格式分叉。PITR 的落点
/// （§6.11）也走这一口，理由同一个。
pub(crate) fn write_snapshot(dir: &Path, state: &State, seq: u64) -> Result<()> {
    let mut w = W::new();
    state.save_with_trailer(&mut w, seq);
    let buf = w.finish();
    let tmp = dir.join(format!("{SNAP_FILE}.tmp"));
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(&buf)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(SNAP_FILE))?;
    if let Ok(d) = File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// 离线导出：持目录锁，读「快照 + 能重放到的全部 WAL」，产出一份逻辑 dump。
///
/// 与 [`Engine::open`] 的恢复路径同语义（所以它也会带上未 durable 的记录），
/// 但**不写任何字节**：dump 不应该因为「看一眼」就把一个只读盘变成可写盘。
pub fn dump_dir(dir: &Path) -> Result<Dump> {
    if !dir.is_dir() {
        return Err(Error::Config(format!(
            "{} is not a data directory",
            dir.display()
        )));
    }
    // 持锁 = 要求目录独占：服务在跑时这里会拒，而不是读一份撕裂的组合。
    let _lock = DirLock::acquire(dir)?;
    let snap_path = dir.join(SNAP_FILE);
    let (mut state, snap_seq) = if snap_path.exists() {
        let raw = std::fs::read(&snap_path)?;
        let (body, seq) = State::snapshot_split(&raw)?;
        (State::load(&mut Rd::new(body))?, seq)
    } else {
        (State::new(), 0u64)
    };
    let logs = Wal::replay(&dir.join(WAL_FILE), snap_seq)?;
    for l in &logs {
        state.apply(l);
    }
    // 日志记录 seq 连续（跳号是硬错，重放已经报过），所以水位就是快照 + 条数。
    let seq = snap_seq + logs.len() as u64;
    Ok(Dump::of_state(&state, seq))
}

/// 新建一个「只含 `.lock`」的空目录并拿住锁：dump 导入与 PITR 的落点共用。
///
/// 返回的锁必须活到写完 —— 丢掉它就是允许第二个进程同时往同一个目录里写。
/// 检查在动任何字节之前：拒绝时目标要么不存在、要么只多一个 `.lock`。
pub(crate) fn open_restore_target(dir: &Path) -> Result<DirLock> {
    std::fs::create_dir_all(dir)?;
    let lock = DirLock::acquire(dir)?;
    let mut foreign = Vec::new();
    for e in std::fs::read_dir(dir)? {
        let name = e?.file_name();
        if name != LOCK_FILE {
            foreign.push(name);
        }
    }
    if !foreign.is_empty() {
        return Err(Error::Config(format!(
            "refusing to restore into {}: it already has {} entrie(s), first is {:?}",
            dir.display(),
            foreign.len(),
            foreign[0]
        )));
    }
    Ok(lock)
}

/// 离线导入：把一份 dump 写成一个**新的**数据目录。
///
/// 只落 `state.snap`（水位 = dump 的 seq），不写 WAL —— 导入后「快照之后没有
/// 任何写」是字面意思。目标目录必须不存在、或只含本工具自己的 `.lock`：
/// 往一个有数据的目录里合并不是导入，那是另一种（且没有契约的）操作。
pub fn restore_dump(dir: &Path, d: &Dump) -> Result<()> {
    let _lock = open_restore_target(dir)?;
    write_snapshot(dir, &d.state(), d.seq)
}

impl Engine {
    /// 打开（或创建）数据目录并完成崩溃恢复。
    ///
    /// 恢复顺序：读 `state.snap`（若有）→ 重放 `seq > snap_seq` 的 WAL
    /// 记录 → 截掉坏尾。空目录得到一个全新引擎。
    pub fn open(dir: &Path, durability: Durability) -> Result<Engine> {
        Self::open_with_archive(dir, durability, None)
    }

    /// 同 [`Engine::open`]，外加一个 WAL 归档目录（§6.11）。
    ///
    /// 归档目录在这里就建出来、而且要求它不等于数据目录：不可写与「归档跟
    /// 快照抢同一个目录」都要在开局暴露成启动错误，而不是等到第一次 checkpoint
    /// 才把服务打下来 —— 那时进程里已经有未归档的历史。
    pub fn open_with_archive(
        dir: &Path,
        durability: Durability,
        archive: Option<PathBuf>,
    ) -> Result<Engine> {
        if let Some(a) = &archive {
            Archive::new(a).ensure()?;
            let same = match (a.canonicalize(), dir.canonicalize()) {
                (Ok(x), Ok(y)) => x == y,
                _ => false,
            };
            if same {
                return Err(Error::Config(format!(
                    "archive dir {} is the data directory itself: archive segment names would \
                     get mixed into the directory that `dump_dir` / `pitr_restore` scan for \
                     other files; give the archive its own directory",
                    a.display()
                )));
            }
        }
        let t0 = std::time::Instant::now();
        std::fs::create_dir_all(dir)?;
        // 先拿锁再读一个字节：与另一个实例抢同一份 WAL 的重放是这里唯一
        // 会真把数据弄坏的行为。
        let lock = DirLock::acquire(dir)?;
        // 令牌文件必须在任何“看起来能当空白目录用”的判断之前读：把它当成 0
        // 重启一个已被提升过的目录，等于把 fencing 默默解除了。内容不合法就是
        // 启动错误（退出码 2），不给“猜一个默认值”的口子。
        let fence = read_fence(dir)?;
        let snap_path = dir.join(SNAP_FILE);
        let mut state = State::new();
        let mut snap_seq = 0u64;
        if snap_path.exists() {
            let raw = std::fs::read(&snap_path)?;
            // 先切尾巴：新版尾巴带 crc32，验不过就是坏文件，连解都不该解。
            // 尾巴之前的字节才是正文，多一个都不行（`State::load` 末尾查）。
            let (body, seq) = State::snapshot_split(&raw)?;
            state = State::load(&mut Rd::new(body))?;
            snap_seq = seq;
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
            replica: false,
            replica_primary: 0,
            fence,
            recovery: Recovery {
                snap_seq,
                wal_records: replayed.len() as u64,
                durable_seq: last_seq,
                elapsed_ms: t0.elapsed().as_millis() as u64,
            },
            _lock: lock,
            archive,
            inflight: None,
            lock_sweep_cur: None,
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

    /// 归档目录（没开归档 = `None`）。
    pub fn archive_dir(&self) -> Option<&Path> {
        self.archive.as_deref()
    }

    /// 本目录当前的 fencing token（从未被提升过 = 0）。
    pub fn fence(&self) -> u64 {
        self.fence
    }

    /// 把令牌抬到 `to`（**只升不降**），先把文件落盘再改内存值。
    ///
    /// 顺序不可换：先改内存再落盘，一次崩溃就会留下「本进程按新令牌追过数据、
    /// 重启后又回到旧令牌」的目录与磁盘不一致。反过来只会保守——令牌已经高
    /// 于数据，重启后它只会更不愿意接受旧主。
    pub fn raise_fence(&mut self, to: u64) -> Result<u64> {
        if to <= self.fence {
            return Ok(self.fence);
        }
        let tmp = self.dir.join(format!("{FENCE_FILE}.tmp"));
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(format!("{to}\n").as_bytes())?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, self.dir.join(FENCE_FILE))?;
        if let Ok(d) = File::open(&self.dir) {
            let _ = d.sync_all();
        }
        self.fence = to;
        Ok(to)
    }

    /// `treedb-standby --promote` 的引擎侧：把令牌抬到「本目录见过的最大值 + 1」。
    ///
    /// “见过的最大值”就是本目录落盘的那个数 —— 副本每接受一个更高令牌的主
    /// 都会先把它落盘。于是“提升后的令牌必定大于任何驱动过本目录的主”是**由
    /// 落盘顺序保证的**，而不是靠调用方记得传对参数。
    pub fn promote(&mut self) -> Result<u64> {
        // 到顶就报错，不能 `saturating_add` 之后交给 `raise_fence` 去“成功返回不
        // 变的令牌”——那会让调用方以为切主完成，而令牌其实一格没动，旧主仍能
        // 通过闸门。这个分支跑不到，但它的失败模式正好是 fencing 唯一要防的事。
        let next = self.fence.checked_add(1).ok_or_else(|| {
            Error::Config("fencing token is at u64::MAX: cannot promote any further".into())
        })?;
        self.raise_fence(next)
    }

    /// 副本侧的令牌闸门：`epoch` 小于本地就是被提掉权的旧主。
    ///
    /// 大于本地时**先把令牌落盘再吃它的数据**（§6.6 第 3 条）。
    fn admit_epoch(&mut self, epoch: u64, what: &str) -> Result<()> {
        if epoch < self.fence {
            return Err(Error::Fence(format!(
                "{what} carries fencing token {epoch}, this directory is at {} — refusing a \
                 demoted primary (this dir was promoted; point the replica at the new primary)",
                self.fence
            )));
        }
        if epoch > self.fence {
            let was = self.fence;
            self.raise_fence(epoch)?;
            eprintln!("replica: fencing token {was} -> {epoch} (the primary was promoted)");
        }
        Ok(())
    }

    /// 以**只读副本**身份打开一个数据目录（§6.6）。
    ///
    /// 与 [`Engine::open`] 只差一件事：真写在 `exec` 那里被 `EROFS` 拒掉。剩下
    /// 的全部一样：自己的 WAL、自己的快照、自己的 checkpoint。
    ///
    /// 为什么副本要自带 WAL 而不是「内存状态 + 拉到什么算什么」：
    ///
    /// - 崩溃后能自己从本地日志重放还原，不必从主那里再拉一轮；
    /// - 记录存的是 payload 原字节，两份日志因此可以按字节比对，
    ///   「副本重放的是同一条记录」这句话才有得验；
    /// - checkpoint 能把 WAL 长度控住，不然只靠日志的副本运行一天就
    ///   拖着一个与主同大的日志文件。
    pub fn open_replica(dir: &Path, durability: Durability) -> Result<Engine> {
        Self::open_replica_with_archive(dir, durability, None)
    }

    /// 同 [`Engine::open_replica`]，外加归档目录（§6.11）。
    ///
    /// 副本自己也 checkpoint（§6.6 的第三条理由），所以它同样需要归档：否则
    /// 副本上的「回到某一刻」只能做到最后一次 checkpoint，而那正是副本被用来
    /// 顶位时最想回溯的那个时间点。
    pub fn open_replica_with_archive(
        dir: &Path,
        durability: Durability,
        archive: Option<PathBuf>,
    ) -> Result<Engine> {
        let mut eng = Self::open_with_archive(dir, durability, archive)?;
        eng.replica = true;
        eng.stats.replica = true;
        Ok(eng)
    }

    pub fn is_replica(&self) -> bool {
        self.replica
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

    /// 下一条要落进自己 WAL 的 seq。副本拿它当拉取请求的 `want_seq`，
    /// [`Engine::replicate_records`] 拿它验交付是否接得上。
    ///
    /// 与 `durable_seq` 的差别在 `Never` 档才存在（已写出未 fsync），而副本永远
    /// 走 durable 提交，所以这两个数在副本上只差一个已提交批次的尾巴。
    pub fn apply_seq(&self) -> u64 {
        self.wal.next_seq()
    }

    /// 翻译 op 并把结果 applied 到内存状态。**不做 fsync**，也不发 ack。
    ///
    /// 返回的 `Vec<Log>` 是该 op 需要进 WAL 的记录（只读 op 为空）。
    /// 调用方（事件循环）负责在一轮结束后把本轮所有 logs 交给
    /// [`Engine::commit_batch`]，再把 reply 写回对应连接。
    pub fn exec(&mut self, op: &Op) -> (Vec<Log>, Reply) {
        // 批内的子 op 也各算一次：不这么做的话一个 64 条的批在 `Stats` 里只值 1，
        // 吞吐读数与实际处理的请求量差两个数量级。
        self.stats.ops_total += match op {
            Op::Batch { ops } => 1 + ops.len() as u64,
            _ => 1,
        };
        match op {
            Op::Ping => (vec![], Reply::Pong),
            Op::Stats => (vec![], Reply::Stats(Box::new(self.snapshot_stats()))),
            Op::Checkpoint => match self.checkpoint() {
                Ok(_seq) => (vec![], Reply::Ok),
                Err(e) => (vec![], Reply::err(crate::engine::model::errno::EIO, e.to_string())),
            },
            Op::Shutdown => (vec![], Reply::Ok),
            Op::WalFetch {
                want_seq,
                hint_off,
                max_bytes,
            } => (vec![], self.wal_fetch(*want_seq, *hint_off, *max_bytes)),
            Op::SnapFetch { offset, len } => (vec![], self.snap_fetch(*offset, *len)),
            _ => {
                // 只读副本的真写挡在这里而不是网络层：写 op 在多 worker 下本来
                // 就要送到提交线程上来，拦在这一处等于只拦一处；两处各拦一份
                // 则一定会对不齐。判据是三档里的那一档：`is_readonly` 与
                // `is_control` 同时为假（见 [`Op::is_control`]），不然连 `Stats`
                // 都会被拒，副本就没法被监控了。
                if self.replica && !op.is_readonly() && !op.is_control() {
                    return (
                        vec![],
                        Reply::err(
                            crate::engine::model::errno::EROFS,
                            "read-only replica: this instance follows a primary (§6.6)",
                        ),
                    );
                }
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

    /// 只追加、不 fsync：`multi` 路径把提交这一步与落盘那一步拆开。
    ///
    /// 提交线程 append 完就把控制权交回事件循环（接下一轮转发、translate
    /// 下一批），落盘交给 flusher 线程。**ack 仍然严格晚于 fsync**：调用方
    /// 必须把返回的 seq 交给 flusher，并且只有收到它的完成通知才 ack 这一批。
    /// 分离只把「等盘」从提交线程的窗口里拿出去，没有把 ack 提前一个毫秒。
    ///
    /// 返回 `None` = 这一批一条日志都没有（纯读轮），不需要落盘。
    pub fn stage_batch(&mut self, groups: &[&[Log]]) -> Result<Option<u64>> {
        let mut payloads: Vec<Vec<u8>> = Vec::new();
        for g in groups {
            for l in *g {
                payloads.push(log_payload(l));
            }
        }
        if payloads.is_empty() {
            return Ok(None);
        }
        let last = self.wal.append_nodur(&payloads)?;
        self.stats.seq = last;
        Ok(Some(last))
    }

    /// flusher 落完一次盘：推进 durable 水位，记一次 sync。
    ///
    /// `seq` 是提交线程 append 时给的承诺上界（不是 fsync 实际覆盖到哪里 ——
    /// 那期间可能又 append 了新记录）。只按承诺计，宁可少算不多算。
    pub fn note_synced(&mut self, seq: u64, us: u64) {
        self.wal.advance_durable(seq);
        if self.durability != Durability::Never {
            self.stats.syncs_total += 1;
            self.stats.sync_us_total += us;
        }
    }

    /// 给 flusher dup 一个 fsync 专用 fd。
    ///
    /// checkpoint 收口（`truncate_before` / `reset`）会把日志换成新 inode，
    /// 那之后必须重新取一次并让 flusher 换 fd。
    pub fn wal_sync_fd(&self) -> Result<File> {
        self.wal.try_clone_sync_fd()
    }

    /// 写是否需要落盘才允许 ack（即 `group` / `always` 两档）。
    ///
    /// `never` 档下 ack 本来就不等盘，流水线必须跳过 flusher —— 否则等于
    /// 把 never 档静默升级成 group 档，性能特征与文档承诺同时失效。
    pub fn durable_writes(&self) -> bool {
        self.durability != Durability::Never
    }

    /// 旁路快照已经编完了吗（尚未收口）。
    ///
    /// 事件循环用它决定收口前要不要先把 in-flight 的 fsync 排空：
    /// `truncate_before` 会换 inode，不能让一次旧的 fsync 跨过去。
    pub fn checkpoint_ready(&self) -> bool {
        self.inflight
            .as_ref()
            .map(|j| j.handle.is_finished())
            .unwrap_or(false)
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

    /// 派一份旁路快照（§8 第 7 条）。返回是否真派出去了。
    ///
    /// 热路径在这里只做两件事：取一个已经 durable 的水位、起一个线程。编快照
    /// （重放 + 全量编码 + 写盘 + fsync）全在后台，所以本轮停顿与状态大小
    /// 无关 —— 这是与 [`Engine::checkpoint`] 那条 stop-the-round 路径的区别。
    ///
    /// 归档（§6.11）也在那个线程里做，而且排在 `write_snapshot` 之前：拷段与
    /// 拷旧快照都是 O(日志 + 状态) 的字节搬运，搬到提交线程上等于把「停顿与
    /// 状态大小无关」这条性质当场取消。放进后台的代价只有一条：归档失败会让
    /// 这份快照一起失败（`checkpoint_poll` 把错抛给事件循环），而那时
    /// `truncate_before` 尚未执行，历史仍然完整。
    ///
    /// 已经有一份在路上时直接返回 `false`：让它编完更划算，硬插一份只会
    /// 两份抢同一个 `state.snap.tmp`。
    pub fn checkpoint_start(&mut self) -> Result<bool> {
        if self.inflight.is_some() {
            return Ok(false);
        }
        // 水位取已 durable 的 seq：内存可能比磁盘更靠前（本轮 commit 尚未
        // fsync），把未落盘的记录当成“快照已覆盖”会丢数据。
        let seq = self.wal.durable_seq();
        if seq <= self.snap_seq {
            return Ok(false);
        }
        let dir = self.dir.clone();
        let archive = self.archive.clone();
        let handle = std::thread::Builder::new()
            .name("treedb-snap".into())
            .spawn(move || build_snapshot_from_disk(&dir, seq, archive.as_deref()))
            .map_err(|e| Error::Io(format!("spawn background snapshot: {e}")))?;
        self.inflight = Some(Inflight { seq, handle });
        Ok(true)
    }

    /// 收一份已完成的旁路快照（非阻塞）：`Some(seq)` = 这一次落定。
    ///
    /// 要每轮调一次。后台线程的错不吞：编不出来说明盘或日志有真问题，与
    /// 同步 checkpoint 报错同一个处置（事件循环随之退出）。
    pub fn checkpoint_poll(&mut self) -> Result<Option<u64>> {
        match self.inflight.as_ref() {
            Some(j) if j.handle.is_finished() => self.finish_checkpoint(),
            _ => Ok(None),
        }
    }

    /// 等在路上的那份落定（没有就什么都不做）。
    pub fn checkpoint_wait(&mut self) -> Result<()> {
        self.finish_checkpoint().map(|_| ())
    }

    fn finish_checkpoint(&mut self) -> Result<Option<u64>> {
        let Some(job) = self.inflight.take() else {
            return Ok(None);
        };
        let seq = match job.handle.join() {
            Ok(Ok(seq)) => seq,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(Error::Io("background snapshot thread panicked".into())),
        };
        if seq != job.seq {
            return Err(Error::Io(format!(
                "background snapshot reported seq {seq} for a job dispatched at {}",
                job.seq
            )));
        }
        // 快照已经是水位 `seq` 的那一份：日志里 ≤ seq 的记录从此多余，而取
        // 水位之后才 ack 的那些只活在日志里 —— 所以是「抹掉前段、留下尾巴」，
        // 不是 `wal.reset`（那会把尾巴一起清掉，等于静默丢已确认的写）。
        self.wal.truncate_before(seq)?;
        self.snap_seq = seq;
        self.stats.snap_seq = seq;
        self.stats.checkpoints += 1;
        Ok(Some(seq))
    }

    /// 全量快照 + WAL 重置（停轮），退出路径与显式命令走这条。
    ///
    /// 顺序：state.save → tmp → fsync(tmp) → rename → fsync(dir) → wal.reset。
    /// 任一步崩溃都只可能留下「旧快照 + 更长的 WAL」，重放仍能还原。
    ///
    /// 开了归档（§6.11）则在上面全部动作**之前**先拷一份 `(snap_seq, seq]` 与
    /// 水位 `snap_seq` 的快照。这条路径本来就在停轮，归档的搬运不能从中挑出
    /// 去（后台那份的收口已经等完了），而顺序不能换：`rename` 之后旧快照的
    /// 字节就没了，`wal.reset` 之后那段日志也没了。归档失败则整次 checkpoint
    /// 失败，源目录一个字节不动。
    ///
    /// 自动档不走这里（§8 第 7 条）：它在轮与轮之间派 [`Engine::checkpoint_start`]，
    /// 只在本函数里停轮一口气编完。开编前先把在路上的那份收掉：两份抢同一个
    /// tmp，而且收得晚的那份会把水位往回带。
    pub fn checkpoint(&mut self) -> Result<u64> {
        self.checkpoint_wait()?;
        // 水位取当前已 durable 的 seq：内存可能比磁盘更靠前（本轮 commit
        // 尚未发生），把未落盘的记录当成"快照已覆盖"会丢数据。
        let seq = self.wal.durable_seq();
        if let Some(ar) = &self.archive {
            Archive::new(ar).record(
                self.wal.path(),
                self.snap_seq,
                seq,
                &self.dir.join(SNAP_FILE),
            )?;
        }
        let mut w = W::new();
        {
            // 快照只需要一个一致的时间切面：读锁挡住写者 apply 就够了，不需要
            // 挡 fsync/网络。大状态机下这是唯一会被拉长的读临界区。
            //
            // 尾巴（crc32 + seq）在同一个函数里写完：正文与它覆盖的水位
            // 必须是一起算出来的，分两处拼就有一段可以被别人改。
            read_of(&self.state).save_with_trailer(&mut w, seq);
        }
        let buf = w.finish();
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

    // ---- 日志传送与只读副本（§6.6）----

    /// `Op::WalFetch` 的应答。三段口径各自的位置不同：
    ///
    /// 1. 越上限是**请求方的错** → `EINVAL`。不在解码侧拦（见 `model.rs` 那条
    ///    注释）：解码失败在事件循环里就是断连，一个合法的请求值不该敲掉一条
    ///    连接，而副本那边重试比重新建连便宜得多。
    /// 2. **gap 优先于交付**：`want_seq <= snap_seq` 意味着要的那条已经被 checkpoint
    ///    截进快照，日志里根本没有。与其让 [`Wal::deliver`] 报「not in the log」
    ///    （那听起来像真损坏），不如照实说 gap，并把游标退回 [`WAL_HINT_NONE`]
    ///    —— 快照水位之前的偏移在新日志里没有意义，留着只会误导下一轮。
    /// 3. 剩下的错才是真损坏 → `EIO`。交付永远不越过 `durable_seq`（那三条口径
    ///    在 [`Wal::deliver`] 里），副本因此不会领先于主的已确认集。
    fn wal_fetch(&self, want_seq: u64, hint_off: u64, max_bytes: u32) -> Reply {
        use crate::engine::model::{errno, WAL_FETCH_MAX, WAL_HINT_NONE};
        if want_seq == 0 {
            return Reply::err(
                errno::EINVAL,
                "wal fetch: seq numbers start at 1, there is no record 0",
            );
        }
        if max_bytes > WAL_FETCH_MAX {
            return Reply::err(
                errno::EINVAL,
                format!("wal fetch of {max_bytes} bytes over {WAL_FETCH_MAX}"),
            );
        }
        let durable = self.wal.durable_seq();
        if want_seq <= self.snap_seq {
            return Reply::WalBatch {
                records: Vec::new(),
                base_seq: want_seq,
                next_off: WAL_HINT_NONE,
                durable_seq: durable,
                snap_seq: self.snap_seq,
                gap: true,
                epoch: self.fence,
            };
        }
        match self.wal.deliver(want_seq, hint_off, max_bytes) {
            Ok(d) => Reply::WalBatch {
                records: d.records,
                base_seq: d.base_seq,
                next_off: d.next_off,
                durable_seq: durable,
                snap_seq: self.snap_seq,
                gap: false,
                epoch: self.fence,
            },
            Err(e) => Reply::err(errno::EIO, e.to_string()),
        }
    }

    fn snap_fetch(&self, offset: u64, len: u32) -> Reply {
        use crate::engine::model::{errno, SNAP_CHUNK_MAX};
        if len > SNAP_CHUNK_MAX {
            return Reply::err(
                errno::EINVAL,
                format!("snapshot chunk of {len} bytes over {SNAP_CHUNK_MAX}"),
            );
        }
        if len == 0 {
            // 探针（见 `Reply::SnapPrefix`）：不交字节，只答前缀的 crc32。它只
            // 在副本发现换代时发一次，常规路径一个字节都不多算。
            return match self.snap_prefix_crc(offset) {
                Ok((prefix_crc, total)) => Reply::SnapPrefix {
                    prefix_crc,
                    total,
                    snap_seq: self.snap_seq,
                    epoch: self.fence,
                },
                Err(e) => Reply::err(errno::EIO, e.to_string()),
            };
        }
        match self.snap_chunk(offset, len) {
            Ok((bytes, total)) => Reply::SnapChunk {
                bytes,
                total,
                snap_seq: self.snap_seq,
                epoch: self.fence,
            },
            Err(e) => Reply::err(errno::EIO, e.to_string()),
        }
    }

    /// `state.snap` 前 `min(offset, total)` 字节的 crc32（连同整份长度）。
    ///
    /// 分块喂 [`crc32_update`] 而不是把前缀读成一整块：探针的 `offset` 就是副本
    /// 已经下过的量，冷启动里它可能接近整份快照 —— 为算一个校验和再复制一份同
    /// 等大小的内存没有意义。结果与 `crc32(&raw[..min(offset,total)])` 逐位相同，
    /// 链式调用只是把同一个循环分成几截。
    fn snap_prefix_crc(&self, offset: u64) -> Result<(u32, u64)> {
        use crate::engine::wal::crc32_update;
        use std::io::Read;
        let path = self.dir.join(SNAP_FILE);
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((0, 0)),
            Err(e) => return Err(Error::Io(format!("open {}: {e}", path.display()))),
        };
        let total = f.metadata()?.len();
        let mut left = offset.min(total);
        let mut c = 0xFFFF_FFFFu32;
        let mut buf = vec![0u8; (1 << 20).min(left.max(1) as usize)];
        while left > 0 {
            let want = (buf.len() as u64).min(left) as usize;
            let mut got = 0usize;
            while got < want {
                match f.read(&mut buf[got..want]) {
                    Ok(0) => break,
                    Ok(n) => got += n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(Error::Io(format!("read {}: {e}", path.display()))),
                }
            }
            // 文件被截短：按已读到的算。副本那一侧比的是「它自己的前缀」与
            // 「这个数」，对不上它就从 0 重下 —— 这里不报错，因为「换了一代」
            // 本来就是这一条的常态。
            if got == 0 {
                break;
            }
            c = crc32_update(c, &buf[..got]);
            left -= got as u64;
        }
        Ok((!c, total))
    }

    /// 从 `state.snap` 的 `offset` 读至多 `len` 字节，返回 (字节, 整份长度)。
    ///
    /// 越过文件尾是短读不是错：副本按 `total` 判下完没有。跨代的拼接由
    /// `snap_seq` 兑：checkpoint 换过快照文件，本函数给出的就是新代的水位，
    /// 副本看到它变了会先探一次前缀（[`Engine::snap_prefix_crc`]）：能接上就
    /// 接着下，接不上才整份重下 —— 而不是一律把已下的几十块全丢掉（2026-11-22
    /// 的追踪里，一次 1 MiB 档冷启动为此白拉 3 MiB）。这样做的好处是不会拼出
    /// 一份两个时代都不属于的快照，而代价只剩「前缀真的变了」的那一次。所以
    /// 下载窗口越短越安全 —— 块长自增在这件事上也是帮忙的。
    ///
    /// 用 `seek` + `read` 循环而不是 `read_exact`：后两者之间文件可能被换掉，
    /// 短读要照实交出去，不该把一次正常的重下载报成 `EIO`。
    fn snap_chunk(&self, offset: u64, len: u32) -> Result<(Vec<u8>, u64)> {
        use std::io::{Read, Seek, SeekFrom};
        let path = self.dir.join(SNAP_FILE);
        let mut f = match File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
            Err(e) => return Err(Error::Io(format!("open {}: {e}", path.display()))),
        };
        let total = f.metadata()?.len();
        if offset >= total || len == 0 {
            return Ok((Vec::new(), total));
        }
        let want = (len as u64).min(total - offset) as usize;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; want];
        let mut got = 0usize;
        while got < want {
            match f.read(&mut buf[got..]) {
                Ok(0) => break,
                Ok(n) => got += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::Io(format!("read {}: {e}", path.display()))),
            }
        }
        buf.truncate(got);
        Ok((buf, total))
    }

    /// 副本装快照：整份替换内存状态，把快照字节原样落进自己的数据目录，
    /// 再把 WAL 截到快照水位。
    ///
    /// 落盘顺序与 [`Engine::checkpoint`] 一致：tmp → fsync → rename → fsync(dir) →
    /// `wal.reset`。任一步崩溃都只可能留下「旧快照 + 更长的 WAL」，重启后重放
    /// 仍能把状态还原到一个自洽点。快照字节不重编而是直接存主的原件：两份
    /// `state.snap` 因此可以按字节比对，也让副本重启后不必重新下载。
    ///
    /// 拒绝水位回退：`seq < 本地 snap_seq` 等于把已有的更新状态换成旧的。主的
    /// 快照水位只前进不回退，看到回退只能是把两代拼错了，宁可报错。
    ///
    /// **一例外，且只有一种（§6.6 第 3 条）：对端的 fencing token 更大。** 这时
    /// “旧”不代表“被提掉权”：新主是从别处提升的，它的令牌更大、它的数据就是
    /// 权威，哪怕 seq 更小（旧主以前跑得更快）。反之令牌更小的对端一律拒收 ——
    /// 那才是真的脑裂回滚。所以判据不是“谁更新”，而是“谁有权写”，权力相同
    /// 时才比新旧。
    pub fn install_snapshot(&mut self, bytes: &[u8], epoch: u64) -> Result<u64> {
        // 在路上的旁路快照先收掉：它和下面这句都要动 `state.snap`。
        self.checkpoint_wait()?;
        let was = self.fence;
        self.admit_epoch(epoch, "snapshot")?;
        // 先切尾巴（顺带验主的 crc32）：传输层保的是“字节到了”，不保“字节没
        // 被改”；这里错一个比特比整份重下来贵得多，宁可在入库口就拒。
        let (body, seq) = State::snapshot_split(bytes)?;
        if seq < self.snap_seq {
            if epoch <= was {
                return Err(Error::Io(format!(
                    "snapshot generation {seq} is behind the local watermark {} at the same \
                     fencing token {epoch}; refusing to rewind",
                    self.snap_seq
                )));
            }
            eprintln!(
                "replica: taking snapshot seq {seq} below the local watermark {} — \
                 fencing token {was} -> {epoch} says the new primary outranks this one",
                self.snap_seq
            );
        }
        let fresh = State::load(&mut Rd::new(body))?;
        *write_of(&self.state) = fresh;
        let tmp = self.dir.join(format!("{SNAP_FILE}.tmp"));
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, self.dir.join(SNAP_FILE))?;
        if let Ok(d) = File::open(&self.dir) {
            let _ = d.sync_all();
        }
        self.wal.reset(seq)?;
        self.snap_seq = seq;
        self.stats.snap_seq = seq;
        self.stats.seq = seq;
        Ok(seq)
    }

    /// 副本追平：把主交来的记录**原字节**落进自己的 WAL，再逐条 apply。
    ///
    /// 先 WAL 后 apply：崩溃重启由自己的日志重放还原，不必再向主拉一轮；反过来
    /// （先 apply）会留下「内存比日志新」的状态，重启就退回到 apply 之前。
    ///
    /// `base_seq` 必须正好接在自己的水位后面：跳号 = 交付缺一块，静默补上等于
    /// 把一份永远对不齐的状态留着当真相用。直接报错，让调用方去走 gap 那条
    /// 重装快照的路。
    ///
    /// 每条都验「解码 → 再编码 == 原字节」。需要这一道的原因很具体：线帧里每条
    /// 记录没有自己的 CRC，而本函数之后记录会以**新算的 CRC** 落进副本自己的
    /// WAL —— 在线上悄悄坏掉的字节过了这一关就永久合法了。这一条同时验两件
    /// 事：内容没坏，两代编码格式没分叉。
    ///
    /// 不看 `durability`：副本的 WAL 就是它的崩溃依恃，每批一次 fsync 不可省。
    pub fn replicate_records(
        &mut self,
        base_seq: u64,
        payloads: &[Vec<u8>],
        epoch: u64,
    ) -> Result<u64> {
        // 令牌闸门在任何字节落地之前：一条来自被提掉权的旧主的记录，哪怕
        // seq 接得上，也不得进本目录的 WAL。
        self.admit_epoch(epoch, "wal batch")?;
        if payloads.is_empty() {
            return Ok(self.wal.durable_seq());
        }
        let want = self.wal.next_seq();
        if base_seq != want {
            return Err(Error::Gap(format!(
                "replica is at seq {want}, the batch starts at {base_seq} \
                 (records in between are gone locally: reinstall the snapshot)"
            )));
        }
        let mut logs = Vec::with_capacity(payloads.len());
        for (i, p) in payloads.iter().enumerate() {
            let mut r = Rd::new(p);
            let log = Log::decode(&mut r)?;
            let mut w = W::new();
            log.encode(&mut w);
            let back = w.finish();
            if back != *p {
                return Err(Error::Codec(format!(
                    "replicated record at seq {} does not survive a decode/encode round trip \
                     (bytes corrupted in flight, or the two ends disagree on the log format)",
                    base_seq + i as u64
                )));
            }
            logs.push(log);
        }
        let last = self.wal.commit(payloads)?;
        {
            let mut s = write_of(&self.state);
            for l in &logs {
                s.apply(l);
            }
        }
        self.stats.seq = last;
        self.stats.replica_applied += payloads.len() as u64;
        Ok(last)
    }

    /// 副本每轮拉取后记下主的水位。取 max：主自己的 durable_seq 不会倒退，
    /// 看到倒退只能是有两轮应答乱序到了这里。
    pub fn note_replica_progress(&mut self, primary_durable_seq: u64) {
        self.replica_primary = self.replica_primary.max(primary_durable_seq);
    }

    /// 拉取/应用失败一轮。不数这个的话，一个静默断线的副本跟一个空闲副本
    /// 在 `Stats` 里一模一样（lag 也是 0，因为拿不到主的水位）。
    pub fn note_replica_error(&mut self) {
        self.stats.replica_errors += 1;
    }

    /// 因 gap 重装了一次快照。
    pub fn note_replica_resync(&mut self) {
        self.stats.replica_resyncs += 1;
    }

    /// 清一批过期租约（§8 第 17 条）。
    ///
    /// 读路径一直把过期租约当不存在（`LockQuery` 过滤 `lease_expiry`），
    /// 而那个过滤**不能**顺手删行：读 op 不许产生日志（§4.2 的只读划分是
    /// 副本拒写与「worker 就地把算」的判据），删行会把一条读变成写。于是
    /// 记录只能等同一个 `(view, ino, owner)` 被覆盖或 `Close` 才消失 ——
    /// 长尾场景下 `locks` 只增不减，既是内存也是快照体积。
    ///
    /// 回收放在这里，由提交线程的周期任务调用：用的是**已有的**
    /// [`Log::DelLock`]（没有新 op、没有新日志变体、不动 `PROTOCOL_VERSION`），
    /// 所以副本照旧收敛，`Stats.locks` 也从一个周期内的「行数」收敛到有效租约数。
    ///
    /// 分批而不是全表扫：游标记住上一批扫到哪，每次最多看 `limit` 条，
    /// 扫到尾就把游标清空、下一轮从头开始。单轮开销与「表多大」无关，
    /// 一整圈的成本是 `O(表大小)` 摊在 `ceil(n/limit)` 轮上。
    ///
    /// 返回 `(看过, 删掉)`。`lease_expiry == 0` 是「不过期」，永不回收。
    /// 没删到东西时不会碰 WAL，也不会多付一次 fsync。
    pub fn sweep_expired_locks(&mut self, now: i64, limit: usize) -> Result<(usize, usize)> {
        // 副本的 WAL 是主的镜像：本地发一句 `DelLock` 就会让自己的 seq 与主
        // 对不上，下一批 `WalFetch` 当场报 gap。主那边会发同样的回收日志，
        // 副本只负责重放。
        if self.replica || limit == 0 {
            return Ok((0, 0));
        }
        let mut logs: Vec<Log> = Vec::new();
        let scanned;
        {
            let mut s = write_of(&self.state);
            let keys: Vec<(u32, u64, Vec<u8>)> = match self.lock_sweep_cur.take() {
                Some(k) => s
                    .locks
                    .range((Bound::Excluded(k), Bound::Unbounded))
                    .take(limit)
                    .map(|(k, _)| k.clone())
                    .collect(),
                None => s.locks.iter().take(limit).map(|(k, _)| k.clone()).collect(),
            };
            scanned = keys.len();
            // 没扫满说明这一圈到尾巴了，下一轮从头开始；扫满了就从最后一格
            // 接着往后看。两种情况下这一批都已经看/删完了。
            if scanned == limit {
                self.lock_sweep_cur = keys.last().cloned();
            }
            for k in &keys {
                // `get` 而不是直接拿迭代时的值：两行之间没有别人能改，
                // 但这样写能显式把「已不存在」当成不需要删。
                let expired = matches!(s.locks.get(k), Some(r) if r.lease_expiry != 0 && r.lease_expiry <= now);
                if expired {
                    let l = Log::DelLock {
                        view: k.0,
                        ino: k.1,
                        owner: k.2.clone(),
                    };
                    s.apply(&l);
                    logs.push(l);
                }
            }
        }
        let removed = logs.len();
        if !logs.is_empty() {
            self.commit_batch(&[logs.as_slice()])?;
        }
        Ok((scanned, removed))
    }

    /// 给 `Op::Stats` 用的即时快照。
    pub fn snapshot_stats(&mut self) -> EngineStats {
        // 先把前端线程的量折进来。`swap(0)` 而不是 `load + fetch_sub`：后者在
        // 两步之间插入的增量会被减两次（u64 减穿就是回绕成一个天文数字）。
        self.stats.ops_total += self.external_ops.swap(0, Ordering::Relaxed);
        // 先把计数在 Guard 作古前抽出来：guard 借的是 `&self.state`，而下面
        // 要改 `self.stats`，不拆开就过不了借用检查。
        let (views, nodes, dentries, locks, refs, tables, rows) = {
            let s = read_of(&self.state);
            (
                s.views.len() as u64,
                s.nodes.len() as u64,
                // 两层表的 len() 是目录数，报数要的是 dentry 条数。
                s.dentry_count() as u64,
                s.locks.len() as u64,
                s.refs.len() as u64,
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
        // 副本那几个数：lag 现算而不是让调用方自己减，减错一次就是一个永久
        // 不对的数。主那边这六个字段全是默认值（`replica_primary` 永远是 0，
        // 因为没人为它调 `note_replica_progress`），所以 lag 自然也是 0。
        self.stats.replica = self.replica;
        self.stats.replica_primary_seq = self.replica_primary;
        self.stats.replica_lag = self
            .replica_primary
            .saturating_sub(self.wal.durable_seq());
        self.stats.clone()
    }

    /// 已写但未 fsync 的记录数对应的 seq 水位（测试用）。
    #[allow(dead_code)]
    pub fn next_seq(&self) -> u64 {
        self.stats.seq
    }
}

impl Drop for Engine {
    /// 退出时不能把后台快照线程丢下：它可能在别的进程已经接管目录、写下更新的
    /// 快照之后才 rename，把快照水位往回带。这里等的是本地一次重放 + fsync，
    /// 不等网络 —— 值得。
    ///
    /// 不在这里补 `truncate_before`：日志里还剩哪些记录都不影响恢复（`open` 按
    /// 快照水位过滤），而 drop 阶段拿不到 `Result` 可回，做一半的整理只能靠猜。
    fn drop(&mut self) {
        if let Some(job) = self.inflight.take() {
            match job.handle.join() {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => eprintln!("treedb: background snapshot failed on drop: {e}"),
                Err(_) => eprintln!("treedb: background snapshot thread panicked on drop"),
            }
        }
    }
}

// ---- 快照升级（§6.5）------------------------------------------------------------
//
// 这一段的存立理由：快照布局一旦升代（v2 → v3 加了通用表段，v3 → v4 抽掉了
// 四张控制面表的计数段），已有数据目录就开不了门，而引擎不会替旧文件猜布局。
// 它**只在离线工具里被调用**（`treedb-migrate`），
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
    /// 快照尾里的 seq：这份快照覆盖到哪个水位。两种尾巴形状都认（见
    /// [`State::snapshot_split`]）。
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
    // 走迁移专用的严格解码：整个文件必须正好被布局吃完（尾巴已由
    // `snapshot_split` 切掉，剩下的正文一个字节也不能多）。
    // 用它而不是 [`State::load`] 是为了让只读体检在上一代目录上也能报出计数。
    match State::snapshot_split(&raw).and_then(|(body, seq)| {
        State::load_for_migration(&mut Rd::new(body)).map(|s| (s, seq))
    }) {
        Ok((s, seq)) => {
            info.decodable = true;
            info.snap_seq = seq;
            info.views = s.views.len() as u64;
            info.nodes = s.nodes.len() as u64;
            info.dentries = s.dentry_count() as u64;
            info.locks = s.locks.len() as u64;
            info.refs = s.refs.len() as u64;
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
        /// 上一代（v3）快照的逐字节副本。WAL 没被碰过，所以「备份文件 + 现有 WAL」
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
/// **前提：服务端已停**。这个前提现在由 [`LOCK_FILE`] 上的 `flock` 真兑现：
/// 服务端开门/迁移工具都拿同一把锁，两个进程不会再同时改一个目录（旧版注释里
/// 写的「服务端也没持文件锁」已成历史）。写回前再读一次原文件与手上的字节比对
/// 保留作第二道哨兵 —— 锁拦的是本仓的两个入口，拦不住手搓脚本。
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
    // 动手之前拿锁：服务端还在跑的话这里就报错，而不是写完才发现两边在
    // 抢同一个目录。锁跟着本函数的栈帧走，返回即释放。
    let _lock = DirLock::acquire(dir)?;
    let (body, seq) = State::snapshot_split(&raw)?;
    let state = State::load_for_migration(&mut Rd::new(body))?;

    // 动手前的自检：编 → 解 → 逐字段比。这里用的解法是严格入口（不是迁移入口） ，
    // 等于把「新二进制明天能不能开门」先问一遍。尾巴按当前布局重写（带 crc）。
    let mut w = W::new();
    state.save_with_trailer(&mut w, seq);
    let out = w.finish();
    let check = State::snapshot_split(&out)
        .and_then(|(body, _)| State::load(&mut Rd::new(body)))
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
