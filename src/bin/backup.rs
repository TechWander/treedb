//! `treedb-backup`：不碰源目录的在线一致快照导出（DESIGN.md §6.7）。
//!
//! ```text
//! treedb-backup --primary 10.0.0.1:7777 --out /backup/treedb.dump
//! ```
//!
//! 与 `treedb-dump` 的分工：dump 读**本地目录**，要求目录独占（服务在跑就拒），
//! 因为它要同时看 `state.snap` 与正在增长的 WAL；backup 读**在线主**，两条腿
//! 都是主已经实现的只读 op ——
//!
//! 1. [`Op::SnapFetch`] 分块拉整份 `state.snap`，按 `snap_seq` 代际校验拼接；
//! 2. [`Op::WalFetch`] 从快照水位往上拉，追到主报的 durable 尾巴为止。
//!
//! 两段拼出来的切面就是「主在 seq = 最后一条重放记录时的状态」：WAL 只追加、不
//! 修改历史，`WalFetch` 的上界又是 `durable_seq`（主已 fsync 的那条），所以这份
//! 切面**一定**是主已经确认过的状态，不会包含主崩溃时可能丢的写。副本走的也是
//! 这两条 op，因此这里不需要任何引擎改动，也不动 `PROTOCOL_VERSION`。
//!
//! 三条边界必须写清楚：
//!
//! - **RPO 是「备份开始那一刻」不是「备份结束那一刻」**：完成后文件顶在切面 seq
//!   上，之后主的写不在这份文件里。要更小的 RPO 就缩短备份间隔，或者用 §6.6 的
//!   热副本逐条追。
//! - **主在备份期间 checkpoint 掉了还没拉的那段，本轮作废重来**（`gap`）。
//!   这不是错，是「没跟上」，重试整份即可；`--retries` 管次数。
//! - **文件形状与 dump 完全相同**：`TDBDUMP2` 容器，`treedb-load` 直接吃得下。
//!   唯一区别是它不来自目录，所以连 `.lock` 都不碰 —— 主可以一直在跑。
//!
//! 不搬 `fence`（与 dump 同一条规矩）：备份是内容快照，不是「谁有权写」的凭证，
//! 从备份恢复出来的实例该不该当主，由 `treedb-standby --promote` 而不是这份文件
//! 决定。

use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;
use treedb::engine::model::{Op, Reply, SNAP_CHUNK_MAX, WAL_FETCH_MAX, WAL_HINT_NONE};
use treedb::engine::{Dump, Log, Rd, State};
use treedb::net::auth::Key;
use treedb::net::client::Client;

const HELP: &str = "\
treedb-backup —— 从在线主导出一份一致快照（不需要停服务）

用法：
  treedb-backup --primary <HOST:PORT> --out <文件> [选项]
  treedb-backup --primary <HOST:PORT> --out - | gzip > <文件.gz>
                          （`-` = 写标准输出，摘要改走 stderr）

选项：
  --auth-key-file PATH   集群共享密钥（§4.4）；主启用认证时必须与它一致
  --timeout-ms N         建连与单次往返的超时，默认 5000
  --retries N            整份备份的重试次数（主在备份期间 checkpoint 换代会撞上），
                         默认 3
  --quiet                打完摘要就不再输出进度

退出码：0 = 导出完成；1 = 出错（连不上、被拒、内容不自洽）；2 = 参数不全。
产出文件与 `treedb-dump` 同格式，可用 `treedb-load --in <文件> --dir <新目录>` 恢复。";

/// 一次备份里「需要重来」与「不能重来」必须分开：前者是主 checkpoint 追过了
/// 头（正常的竞态），后者是配置错 / 线上坏了。
enum Fail {
    Retry(String),
    Fatal(String),
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut primary: Option<String> = None;
    let mut out: Option<PathBuf> = None;
    let mut key_file: Option<PathBuf> = None;
    let mut timeout_ms: u64 = 5_000;
    let mut retries: u32 = 3;
    let mut quiet = false;

    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--primary" => primary = Some(next(&mut i)),
            "--out" => out = Some(PathBuf::from(next(&mut i))),
            "--auth-key-file" => key_file = Some(PathBuf::from(next(&mut i))),
            "--timeout-ms" => match next(&mut i).parse() {
                Ok(n) if n > 0 => timeout_ms = n,
                _ => {
                    eprintln!("bad --timeout-ms\n\n{HELP}");
                    std::process::exit(2);
                }
            },
            "--retries" => match next(&mut i).parse() {
                Ok(n) => retries = n,
                Err(_) => {
                    eprintln!("bad --retries\n\n{HELP}");
                    std::process::exit(2);
                }
            },
            "--quiet" => quiet = true,
            "-h" | "--help" => {
                println!("{HELP}");
                return;
            }
            other => {
                eprintln!("unknown argument: {other}\n\n{HELP}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    let (Some(primary), Some(out)) = (primary, out) else {
        eprintln!("--primary 与 --out 都是必需的\n\n{HELP}");
        std::process::exit(2);
    };

    // 密钥在连之前就要读出来：路径写错属于参数错，不该先连上再失败。
    let key = match &key_file {
        None => None,
        Some(p) => match Key::load(p) {
            Ok(k) => Some(k),
            Err(e) => {
                eprintln!("bad --auth-key-file {}: {e}", p.display());
                std::process::exit(2);
            }
        },
    };

    let started = Instant::now();
    let mut last_err = String::new();
    for attempt in 1..=retries.max(1) {
        let mut client = match Client::connect_auth(&primary, timeout_ms, key.as_ref()) {
            Ok(c) => c,
            Err(e) => {
                // 连不上是重试得好的（主可能正在重启），但没理由退避太久。
                last_err = format!("connect {primary}: {e}");
                if !quiet {
                    eprintln!("treedb-backup: {last_err} (attempt {attempt})");
                }
                continue;
            }
        };
        match backup_once(&mut client, !quiet) {
            Ok((state, seq, meta)) => {
                let dump = Dump::of_state(&state, seq);
                let to_stdout = out.as_os_str() == "-";
                let bytes = if to_stdout {
                    // 与 `treedb-dump --out -` 同一个档：管道里只有字节，摘要不得
                    // 混进 stdout，否则 `gzip > f` 会把摘要一起压进去。
                    let raw = dump.encode();
                    let mut so = std::io::stdout();
                    match so.write_all(&raw).and_then(|_| so.flush()) {
                        Ok(()) => raw.len() as u64,
                        Err(e) => {
                            eprintln!("treedb-backup: write stdout: {e}");
                            std::process::exit(1);
                        }
                    }
                } else {
                    match dump.write_file(&out) {
                        Ok(n) => n,
                        Err(e) => {
                            eprintln!("treedb-backup: write {}: {e}", out.display());
                            std::process::exit(1);
                        }
                    }
                };
                let c = dump.counts();
                let say = |line: String| {
                    if to_stdout {
                        eprintln!("{line}");
                    } else {
                        println!("{line}");
                    }
                };
                say(format!(
                    "treedb-backup: {} -> {} ({} bytes, {} logs, seq {})",
                    primary,
                    if to_stdout { "-".to_string() } else { out.display().to_string() },
                    bytes,
                    dump.logs.len(),
                    seq
                ));
                if meta.wal_records == 0 {
                    say(format!(
                        "  snapshot seq {} ({} bytes) + wal none (already at the watermark, epoch {})",
                        meta.snap_seq, meta.snap_bytes, meta.epoch
                    ));
                } else {
                    say(format!(
                        "  snapshot seq {} ({} bytes) + wal seq {}..{} ({} records, epoch {})",
                        meta.snap_seq, meta.snap_bytes, meta.snap_seq + 1, seq, meta.wal_records, meta.epoch
                    ));
                }
                say(format!(
                    "  views {} nodes {} dentries {} locks {} refs {} tables {} rows {}",
                    c.views, c.nodes, c.dentries, c.locks, c.refs, c.tables, c.rows
                ));
                say(format!("  took {:.1} ms", started.elapsed().as_secs_f64() * 1000.0));
                return;
            }
            Err(Fail::Retry(why)) => {
                last_err = why;
                if !quiet {
                    eprintln!("treedb-backup: {last_err}; retrying (attempt {attempt})");
                }
            }
            Err(Fail::Fatal(why)) => {
                eprintln!("treedb-backup: {why}");
                std::process::exit(1);
            }
        }
    }
    eprintln!("treedb-backup: gave up after {retries} attempts: {last_err}");
    std::process::exit(1);
}

/// 一次完整的「拉快照 + 追 WAL」。切面语义见模块文档。
fn backup_once(client: &mut Client, verbose: bool) -> Result<(State, u64, Meta), Fail> {
    let (mut state, snap_seq, snap_bytes, epoch) = fetch_snapshot(client, verbose)?;
    let wal_records = catch_up(client, &mut state, snap_seq, epoch)?;
    Ok((
        state,
        snap_seq + wal_records,
        Meta {
            snap_seq,
            snap_bytes,
            wal_records,
            epoch,
        },
    ))
}

struct Meta {
    snap_seq: u64,
    snap_bytes: u64,
    wal_records: u64,
    epoch: u64,
}

/// 分块拉整份快照。换代（`snap_seq` 变了）意味着手里那几块属于旧 inode，
/// 拼起来会是两个时代都不属于的东西，所以整份丢重来 —— 与副本同一条规矩。
fn fetch_snapshot(
    client: &mut Client,
    verbose: bool,
) -> Result<(State, u64, u64, u64), Fail> {
    let mut buf: Vec<u8> = Vec::new();
    let mut gen: Option<u64> = None;
    let mut chunks = 0u32;
    // 循环把「最终拼好的那一代」一次性交出来；`buf` 是 move 出去的，所以
    // 循环之后不存在「哪一轮的 total 才算数」这种歧义。
    let (buf, total, epoch) = loop {
        let req_off = buf.len() as u64;
        let rep = client
            .call(&Op::SnapFetch {
                offset: req_off,
                len: SNAP_CHUNK_MAX,
            })
            .map_err(|e| Fail::Fatal(format!("snap fetch at offset {req_off}: {e}")))?;
        let (bytes, t, seq, ep) = match rep {
            Reply::SnapChunk {
                bytes,
                total,
                snap_seq,
                epoch,
            } => (bytes, total, snap_seq, epoch),
            Reply::Err { code, msg } => {
                return Err(Fail::Fatal(format!(
                    "primary rejected snap fetch ({code}): {}",
                    String::from_utf8_lossy(&msg)
                )))
            }
            other => {
                return Err(Fail::Fatal(format!(
                    "snap fetch answered an unexpected {other:?}"
                )))
            }
        };
        if req_off != 0 && gen != Some(seq) {
            // 下载中途主做了 checkpoint：代际变了。上一块是用旧 offset 取的，
            // 不能接到新代上 —— 丢掉，从 0 重来（新一轮 offset 自然是 0）。
            if verbose {
                eprintln!(
                    "treedb-backup: snapshot generation moved {:?} -> {seq} mid-download, restarting from 0",
                    gen
                );
            }
            buf.clear();
            gen = Some(seq);
            continue;
        }
        gen = Some(seq);
        if t == 0 {
            // 主还没做过 checkpoint，因此没有 state.snap 可拉。这不是错：
            // 那侧 WAL 从 1 开始完整保留，下面从 0 重放即可。
            break (Vec::new(), 0, ep);
        }
        if bytes.is_empty() {
            return Err(Fail::Fatal(format!(
                "primary reports snapshot seq {seq} of {t} bytes but serves nothing \
                 (its state.snap is missing or empty)"
            )));
        }
        buf.extend_from_slice(&bytes);
        chunks += 1;
        if buf.len() as u64 >= t {
            break (buf, t, ep);
        }
        if chunks as u64 > t / SNAP_CHUNK_MAX as u64 + 4 {
            return Err(Fail::Fatal(format!(
                "snapshot download does not converge: {chunks} chunks, {} of {t} bytes",
                buf.len()
            )));
        }
    };
    if total == 0 {
        return Ok((State::new(), 0, 0, epoch));
    }
    let (body, seq) = State::snapshot_split(&buf)
        .map_err(|e| Fail::Fatal(format!("snapshot of {} bytes: {e}", buf.len())))?;
    let state = State::load(&mut Rd::new(body))
        .map_err(|e| Fail::Fatal(format!("snapshot body at seq {seq}: {e}")))?;
    Ok((state, seq, total, epoch))
}

/// 从 `from + 1` 追到**本次备份开始时**主报的 durable 水位，逐条 apply 进 `state`。
///
/// 返回的是真正重放的条数，所以调用方可以据此算出切面 seq。
///
/// 目标水位在**第一轮就固定**，这是「备份耗时与写入量无关」的唯一写法。改成
/// 「一直追到追平」会活锁在持续写入的主上：每拉完一批它又长出了一批，备份永远
/// 不结束 —— 而且那时还说不清它到底备份到了哪一刻。固定目标之后，切面就是
/// 「第一次 `WalFetch` 那一刻主已 fsync 的最大 seq」，RPO 含义也清楚了。
///
/// 主在这一轮里又做了 checkpoint（把还没拉的段截进快照）会回 [`Fail::Retry`]。
fn catch_up(
    client: &mut Client,
    state: &mut State,
    from: u64,
    epoch0: u64,
) -> Result<u64, Fail> {
    let mut want = from + 1;
    let mut hint = WAL_HINT_NONE;
    let mut last = from;
    let mut target: Option<u64> = None;
    loop {
        let rep = client
            .call(&Op::WalFetch {
                want_seq: want,
                hint_off: hint,
                max_bytes: WAL_FETCH_MAX,
            })
            .map_err(|e| Fail::Fatal(format!("wal fetch at seq {want}: {e}")))?;
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
                if epoch != epoch0 {
                    // 备份中途被提权：前一份快照与后一段日志可能来自不同时代。
                    return Err(Fail::Retry(format!(
                        "primary's fencing token moved {epoch0} -> {epoch} mid-backup"
                    )));
                }
                if gap {
                    return Err(Fail::Retry(format!(
                        "primary checkpointed past seq {want} (its snap_seq is now {snap_seq})"
                    )));
                }
                let stop_at = *target.get_or_insert(durable_seq);
                if records.is_empty() {
                    // 已到主当前 durable 尾巴。`last` 可能仍小于 `stop_at`，只在
                    // 主这一轮里又 checkpoint 过时出现（那会是 gap），所以这里
                    // 直接收口是对的。
                    return Ok(last - from);
                }
                if base_seq != want {
                    return Err(Fail::Fatal(format!(
                        "wal fetch answered base_seq {base_seq} for want_seq {want}"
                    )));
                }
                for (i, p) in records.iter().enumerate() {
                    let at = base_seq + i as u64;
                    let log = Log::decode(&mut Rd::new(p))
                        .map_err(|e| Fail::Fatal(format!("record at seq {at}: {e}")))?;
                    state.apply(&log);
                }
                last = base_seq + records.len() as u64 - 1;
                want = last + 1;
                hint = next_off;
                if last >= stop_at {
                    return Ok(last - from);
                }
            }
            Reply::Err { code, msg } => {
                return Err(Fail::Fatal(format!(
                    "primary rejected wal fetch ({code}): {}",
                    String::from_utf8_lossy(&msg)
                )))
            }
            other => {
                return Err(Fail::Fatal(format!(
                    "wal fetch answered an unexpected {other:?}"
                )))
            }
        }
    }
}
