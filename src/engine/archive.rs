//! WAL 归档与时间点恢复（PITR，§6.11）。
//!
//! checkpoint 的副作用是「把 `(快照水位, 新水位]` 的记录从活日志里抹掉」。
//! 抹掉之后那一段历史只剩一个去处：快照。所以「回到两小时前」在开启归档
//! 之前根本不成立 —— 能选的时间点只有历史上每一次 checkpoint 的水位，而这
//! 个数完全由「多久折一次日志」决定，不是用户要的粒度。
//!
//! 本模块做两件事，边界划得很清楚：
//!
//! 1. **归档**（[`Archive::record`]）：checkpoint 动任何字节之前，把
//!    `(from, to]` 的 WAL 帧**原样**拷进归档目录，再拷一份当前 `state.snap`
//!    （水位 `from`）。段在前、快照在后 —— 段是不可再生的（拷成功之后源记录
//!    立刻会被抹掉），快照丢了只让锚点往前退一格，还能从更早的锚点重放回来。
//! 2. **恢复**（[`plan_pitr`] + [`pitr_restore`]）：挑一个 `seq <= to_seq` 的
//!    锚点，把归档段与活日志的记录按 seq 拼成一条连续流，重放到 `to_seq`，
//!    落成一个**新**数据目录（只写 `state.snap`，不写 WAL、不碰 `fence`）。
//!
//! 三条 fail-closed 约定：
//!
//! - 归档失败 → checkpoint 失败。历史不能因为「归档盘满了」而静默丢掉；
//!   宁可服务停下来，也不让「以后能回到某一刻」这句话变成假的。
//! - 段的首条必须是 `from + 1`（[`crate::engine::wal::copy_range`] 里查）：
//!   否则宁可让 checkpoint 失败，也不产出一个「看起来连续」的归档 —— 后者
//!   要等到真做 PITR 时才会发现，而那时源日志已经没得对账了。
//! - PITR 里 `anchor + 1 ..= to_seq` 必须一条不缺。缺一条就报缺口并指出下一个
//!   实际存在的 seq，**不猜、不跳过**：跳过一条就是悄悄产出另一份历史。
//!
//! 归档目录的布局是自描述的，不另存清单：
//!
//! ```text
//! wal-<from:020>-<to:020>.wal   # 原始 WAL 帧串，覆盖 (from, to]
//! snap-<seq:020>.snap           # 与数据目录的 state.snap 逐字节同源
//! ```
//!
//! 不写清单文件的理由：清单与内容会分叉（rename 之后、写清单之前崩），而文件
//! 名就长在文件上 —— 扫一遍目录拿到的就是真值，CRC 由 [`Wal::read_records`]
//! 与 [`State::snapshot_split`] 各自校验。
//!
//! **开关必须一直开着**：源目录的 WAL 只在归档成功后才被抹掉，所以关掉归档
//! 期间做过的那几次 checkpoint 会在归档里留下一个洞。此后 PITR 只要越过那个
//! 洞就会报缺口 —— 这是诚实的失败，而不是把洞两边的历史接起来冒充连续。

use crate::engine::core::{open_restore_target, write_snapshot, SNAP_FILE, WAL_FILE};
use crate::engine::mem::{Log, State, SNAP_MAGIC, SNAP_TRAIL, SNAP_TRAIL_LEGACY};
use crate::engine::wal::{self, Wal};
use crate::error::{Error, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// 归档段文件名前缀。
const SEG_PREFIX: &str = "wal-";
/// 归档段文件名后缀。
const SEG_SUFFIX: &str = ".wal";
/// 归档快照文件名前缀。
const SNAP_PREFIX: &str = "snap-";
/// 归档快照文件名后缀。
const SNAP_SUFFIX: &str = ".snap";

/// `(from, to]` 这一段的名字。定宽 20 位十进制，`ls` 的字典序即 seq 序。
fn segment_name(from: u64, to: u64) -> String {
    format!("{SEG_PREFIX}{from:020}-{to:020}{SEG_SUFFIX}")
}

/// 水位 `seq` 那一份快照的名字。
fn snapshot_name(seq: u64) -> String {
    format!("{SNAP_PREFIX}{seq:020}{SNAP_SUFFIX}")
}

fn parse_segment(name: &str) -> Option<(u64, u64)> {
    let mid = name.strip_prefix(SEG_PREFIX)?.strip_suffix(SEG_SUFFIX)?;
    let (a, b) = mid.split_once('-')?;
    let from = a.parse::<u64>().ok()?;
    let to = b.parse::<u64>().ok()?;
    (to > from).then_some((from, to))
}

fn parse_snapshot(name: &str) -> Option<u64> {
    name.strip_prefix(SNAP_PREFIX)?
        .strip_suffix(SNAP_SUFFIX)?
        .parse()
        .ok()
}

/// 刷归档目录：段/快照的 rename 只有目录项落了盘才算真发生。
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// 读一份快照尾巴上的水位。只读最后 16 字节，不校验正文 crc ——
/// 调用方要么紧接着会完整校验（PITR 的 [`load_snapshot`]），要么只是
/// 想确认「这份快照确实是 `from` 那一刻的」。[`State::snapshot_split`]
/// 才是权威解释器，这里只是它的一枚便宜探针。
fn snapshot_seq(path: &Path) -> Result<u64> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    if len < SNAP_TRAIL_LEGACY as u64 {
        return Err(Error::Codec(format!(
            "{} is {len} bytes, shorter than its {SNAP_TRAIL_LEGACY}-byte seq trailer",
            path.display()
        )));
    }
    let take = len.min(SNAP_TRAIL as u64) as usize;
    let mut tail = vec![0u8; take];
    f.seek(SeekFrom::Start(len - take as u64))?;
    f.read_exact(&mut tail)?;
    if take == SNAP_TRAIL {
        let magic = u32::from_le_bytes(tail[12..16].try_into().unwrap());
        if magic == SNAP_MAGIC {
            return Ok(u64::from_le_bytes(tail[4..12].try_into().unwrap()));
        }
    }
    let at = take - SNAP_TRAIL_LEGACY;
    Ok(u64::from_le_bytes(tail[at..].try_into().unwrap()))
}

/// 完整读一份快照文件（校验 crc + 版本），PITR 的锚点与源目录快照都走这一口。
fn load_snapshot(path: &Path) -> Result<(State, u64)> {
    let raw = std::fs::read(path)?;
    let (body, seq) = State::snapshot_split(&raw)?;
    Ok((State::load(&mut crate::engine::codec::Rd::new(body))?, seq))
}

/// 一个归档目录。只是个路径包装 —— 目录里没有清单，状态全在文件上。
#[derive(Debug, Clone)]
pub struct Archive {
    dir: PathBuf,
}

impl Archive {
    pub fn new(dir: impl Into<PathBuf>) -> Archive {
        Archive { dir: dir.into() }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 建目录。启动时调一次，让「归档盘不可写」在开局就暴露成启动错误，
    /// 而不是等到第一次 checkpoint 才把服务打下来。
    pub fn ensure(&self) -> Result<()> {
        std::fs::create_dir_all(&self.dir)?;
        Ok(())
    }

    /// 目录里的段，按 `from` 升序。目录不存在 = 空（没开过归档，不是错）。
    pub fn segments(&self) -> Result<Vec<(u64, u64, PathBuf)>> {
        let mut out = Vec::new();
        let rd = match std::fs::read_dir(&self.dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        for e in rd {
            let path = e?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if let Some((from, to)) = parse_segment(name) {
                out.push((from, to, path));
            }
        }
        out.sort();
        Ok(out)
    }

    /// 目录里的快照，按水位升序。目录不存在 = 空。
    pub fn snapshots(&self) -> Result<Vec<(u64, PathBuf)>> {
        let mut out = Vec::new();
        let rd = match std::fs::read_dir(&self.dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        for e in rd {
            let path = e?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if let Some(seq) = parse_snapshot(name) {
                out.push((seq, path));
            }
        }
        out.sort();
        Ok(out)
    }

    /// 把 `(from, to]` 与水位 `from` 的快照拷进归档目录。**checkpoint 抹掉
    /// 源记录之前**的唯一一次落盘动作，所以每一步都同步到目录项。
    ///
    /// `snap` 不存在是合法的：空目录的第一次 checkpoint 从 seq 0 起步，那时
    /// 「水位 0 的状态」就是空状态，不需要一份文件来代表它。
    pub fn record(&self, wal_path: &Path, from: u64, to: u64, snap: &Path) -> Result<wal::Copied> {
        if to <= from {
            return Ok(wal::Copied::default());
        }
        self.ensure()?;
        let name = segment_name(from, to);
        let seg = self.dir.join(&name);
        let tmp = self.dir.join(format!("{name}.tmp"));
        let copied = wal::copy_range(wal_path, from, to, &tmp)?;
        // 尾条也必须是 `to`：`to` 是已经 fsync 的水位，日志里读不到它说明
        // 调用方给错了水位（或日志被外力截过）。首条由 `copy_range` 查过。
        if copied.first != Some(from + 1) || copied.last != Some(to) {
            let _ = std::fs::remove_file(&tmp);
            return Err(Error::Io(format!(
                "wal archive of {}: wanted ({from}, {to}], copied {:?}..={:?} ({} record(s))",
                wal_path.display(),
                copied.first,
                copied.last,
                copied.records
            )));
        }
        std::fs::rename(&tmp, &seg)?;
        sync_dir(&self.dir)?;
        if snap.exists() {
            let seq = snapshot_seq(snap)?;
            if seq != from {
                return Err(Error::Io(format!(
                    "refusing to archive {}: its trailer says seq {seq}, but the checkpoint \
                     starts at {from} (archiving it under {from} would hand PITR a state at \
                     the wrong point)",
                    snap.display()
                )));
            }
            let dst_name = snapshot_name(from);
            let dst = self.dir.join(&dst_name);
            let tmp = self.dir.join(format!("{dst_name}.tmp"));
            std::fs::copy(snap, &tmp)?;
            // 拷贝是纯字节搬运：长度与内容先落定再换名，理由同 `write_snapshot`。
            if let Ok(f) = File::open(&tmp) {
                let _ = f.sync_all();
            }
            std::fs::rename(&tmp, &dst)?;
            sync_dir(&self.dir)?;
        }
        Ok(copied)
    }
}

/// 一次 PITR 的完整计划：锚点、覆盖区间、以及在内存里已经重放好的状态。
///
/// 分成「计划」与「落盘」两步，是为了让 `treedb-pitr --dry-run` 能复用同一条
/// 代码路径却一个字节不写 —— dry-run 若另写一份账，那它证明的就不是恢复
/// 会不会成，而是那份账会不会算。
#[derive(Debug, Clone)]
pub struct PitrPlan {
    /// 重放的起点：所选快照的水位（没有可用快照时是 0 = 空状态）。
    pub anchor: u64,
    /// 锚点来源的人读说明（归档里的 `snap-*.snap` / 数据目录的 `state.snap` /
    /// 空状态）。
    pub anchor_desc: String,
    /// 目标水位。
    pub to_seq: u64,
    /// 其中来自归档段的记录数。
    pub from_archive: u64,
    /// 其中来自源目录活 WAL 的记录数。
    pub from_wal: u64,
    /// 用到的归档段数（不含活 WAL）。
    pub segments: usize,
    /// 归档与活日志合起来最新的那条 seq：`to_seq` 的上界就是它。
    pub newest: u64,
    /// 重放完成的状态。私有：落盘只经 [`pitr_restore`] 这一口，保证写出的
    /// 字节与 `Engine` 自己的快照同形。
    state: State,
}

impl PitrPlan {
    /// 合计要重放的条数。
    pub fn records(&self) -> u64 {
        self.from_archive + self.from_wal
    }

    pub fn state(&self) -> &State {
        &self.state
    }
}

/// 把一条记录塞进 `seq -> Log` 的表里；同一 seq 出现两次且内容不同就报错。
///
/// 内容相同则接受：归档失败后重试会把同一段再拷一遍，那不该变成「历史冲突」。
/// 内容不同则只能是归档与活日志重叠了（关掉归档期间做过 checkpoint、或者目录
/// 被人换过），此时两条历史都在，选哪条都是编造，必须报出去。
fn insert_unique(map: &mut BTreeMap<u64, Log>, seq: u64, log: Log, path: &Path) -> Result<()> {
    if let Some(old) = map.get(&seq) {
        if *old == log {
            return Ok(());
        }
        return Err(Error::Config(format!(
            "seq {seq} appears twice with different content (second copy from {}): the archive \
             and the live WAL overlap, so neither history can be trusted",
            path.display()
        )));
    }
    map.insert(seq, log);
    Ok(())
}

/// 规划一次 PITR：选锚点、拼段、查缺口、重放，全部在内存里。
///
/// `src` 是数据目录（提供活 WAL 与它自己的 `state.snap`），`archive` 是归档
/// 目录。两者都给，是因为归档只覆盖「上一次 checkpoint 之前」，`to_seq` 落在
/// 最后一次 checkpoint 之后时必须靠活日志把尾巴补上 —— 也就是「恢复到刚刚
/// 才过去的某一秒」，这恰恰是最常用的那个点。
///
/// **`to_seq` 应该是一个 op 组的末尾**：一条 op 可能产生多条日志（`Create`
/// 是三条），落在组中间会得到「该 op 只生效了一半」的状态。WAL 里没有组边界
/// 标记，这一层查不出来 —— 所以运维该拿 `--stats` 打出的 `seq`（永远是某次
/// ack 之后的水位）当目标，而不是自己插一个中间值。
pub fn plan_pitr(src: &Path, archive: &Path, to_seq: u64) -> Result<PitrPlan> {
    if !src.is_dir() {
        return Err(Error::Config(format!(
            "{} is not a data directory",
            src.display()
        )));
    }
    let ar = Archive::new(archive);

    // 锚点 = 水位 <= to_seq 的最新快照。归档里的 snap-* 与源目录的 state.snap
    // 同义，都是「那一刻的完整状态」，取水位最大的那个 —— 它让重放最短。
    let mut best: Option<(u64, PathBuf, &'static str)> = None;
    for (seq, path) in ar.snapshots()? {
        if seq <= to_seq && best.as_ref().map(|b| seq > b.0).unwrap_or(true) {
            best = Some((seq, path, "archive snapshot"));
        }
    }
    let live_snap = src.join(SNAP_FILE);
    if live_snap.exists() {
        let seq = snapshot_seq(&live_snap)?;
        if seq <= to_seq && best.as_ref().map(|b| seq > b.0).unwrap_or(true) {
            best = Some((seq, live_snap.clone(), "data dir snapshot"));
        }
    }
    let (anchor, anchor_desc, mut state) = match best {
        Some((seq, path, what)) => {
            let (state, got) = load_snapshot(&path)?;
            if got != seq {
                return Err(Error::Codec(format!(
                    "{} is named seq {seq} but its trailer says {got}: the file was renamed \
                     or overwritten",
                    path.display()
                )));
            }
            (seq, format!("{what} {}", path.display()), state)
        }
        None => (
            0,
            format!("empty state (no snapshot at or below seq {to_seq})"),
            State::new(),
        ),
    };
    let mut newest = anchor;

    // 归档段 + 活 WAL 的记录按 seq 拼成一条流。区间外的（<= anchor 已进快照、
    // > to_seq 是目标之后）一律不读进表，省内存也省掉后续过滤。
    let mut map: BTreeMap<u64, Log> = BTreeMap::new();
    let mut segments = 0usize;
    let mut from_archive = 0u64;
    for (from, to, path) in ar.segments()? {
        if to <= anchor || from >= to_seq {
            continue;
        }
        let mut used = 0u64;
        for (seq, log) in Wal::read_records(&path)? {
            newest = newest.max(seq);
            if seq <= anchor || seq > to_seq {
                continue;
            }
            insert_unique(&mut map, seq, log, &path)?;
            used += 1;
        }
        if used > 0 {
            segments += 1;
            from_archive += used;
        }
    }
    let live_wal = src.join(WAL_FILE);
    let mut from_wal = 0u64;
    for (seq, log) in Wal::read_records(&live_wal)? {
        newest = newest.max(seq);
        if seq <= anchor || seq > to_seq {
            continue;
        }
        insert_unique(&mut map, seq, log, &live_wal)?;
        from_wal += 1;
    }

    // 缺口：anchor + 1 ..= to_seq 一条都不能少。
    let mut expect = anchor + 1;
    // `anchor == to_seq` 时区间为空（`range` 的起点大于终点会 panic），
    // 而它完全合法：锚点正好落在目标水位上。
    if anchor < to_seq {
        for (&seq, _) in map.range(expect..=to_seq) {
            if seq != expect {
                return Err(Error::Config(format!(
                    "cannot restore to seq {to_seq}: the archive and the live WAL have no \
                     record {expect} (the next one present is {seq}); the archive was not kept \
                     on for that stretch of history"
                )));
            }
            expect += 1;
        }
        if expect <= to_seq {
            return Err(Error::Config(format!(
                "cannot restore to seq {to_seq}: with the anchor at {anchor} the available \
                 history ends at {} (newest record seen: {newest})",
                expect - 1
            )));
        }
        for (_, log) in map.range(anchor + 1..=to_seq) {
            state.apply(log);
        }
    }

    Ok(PitrPlan {
        anchor,
        anchor_desc,
        to_seq,
        from_archive,
        from_wal,
        segments,
        newest,
        state,
    })
}

/// 把计划落成一个**新**数据目录（只写 `state.snap`，水位 = `to_seq`）。
///
/// 不写 WAL：恢复后的目录里「快照之后没有任何写」是字面意思，于是
/// `treedb-server` 起来看到的正是 `to_seq` 那一刻。不搬 `fence`：那是「谁是
/// 当前主」的部署态，与逻辑数据无关（同 `treedb-load`）。
///
/// 目标目录的口径与 `restore_dump` 完全一致（不存在或只含 `.lock`），因为这里
/// 调的就是同一个 [`open_restore_target`]：往有数据的目录里合并是另一种没有
/// 契约的操作。
pub fn pitr_restore(plan: PitrPlan, out: &Path) -> Result<PathBuf> {
    let _lock = open_restore_target(out)?;
    write_snapshot(out, &plan.state, plan.to_seq)?;
    Ok(out.to_path_buf())
}
