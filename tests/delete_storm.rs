//! 固定删除风暴、重复删后重插、自动 checkpoint 与服务端 RSS 的联合回归。
//!
//! 三条口径必须同时成立，少一条都会把「删完」误读成「系统已经恢复」：
//!
//! 1. 工作量固定：删除档正好 N 个 op；churn 每轮正好 2N 个 op，重插值逐轮变化，
//!    不走同值消除，也不拿配置批大小冒充实际完成数。
//! 2. 量的是服务端：RSS/VmHWM 从 `/proc/<treedb-server pid>/status` 读，Stats 从线上
//!    `Op::Stats` 读；采样贯穿测量窗口，能看见 WAL 峰值与 checkpoint 后的回落。
//! 3. 终态要重开：删除档重开后必须零行；churn 重开后逐行核对最后一轮 payload。
//!
//! RSS 只报阶段值与 retained delta，不设「必须回落」的断言。BTreeMap 释放给 glibc
//! 不等于进程立刻把页还给内核；当前没有 allocator 级碎片指标，不能把 retained RSS
//! 直接判成泄漏。默认 40 000 行保证 group 档在 1 MiB 阈值下确实触发自动 checkpoint。

use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use treedb::engine::core::EngineStats;
use treedb::engine::model::{
    errno, ColumnDef, Op, Reply, TableDef, COL_INDEX, COND_NONE, DEL_IF_EXISTS, ROW_PK, SA_DATA_REF,
};
use treedb::net::client::Client;

mod common;
use common::Server;

const TABLE: &[u8] = b"delete-storm";
const AUTO_MIB: u64 = 1;
const PIPE: usize = 500;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

fn cols(i: u64, round: u64) -> Vec<Vec<u8>> {
    vec![
        format!("t{}", i % 16).into_bytes(),
        format!("p{i}c{round}").into_bytes(),
    ]
}

fn put(i: u64, round: u64) -> Op {
    Op::RowPut {
        table: TABLE.to_vec(),
        key: key(i),
        cols: cols(i, round),
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

fn delete(i: u64) -> Op {
    Op::RowDelete {
        table: TABLE.to_vec(),
        key: key(i),
        flags: DEL_IF_EXISTS,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

fn stats(c: &mut Client) -> EngineStats {
    match c.call(&Op::Stats).expect("Stats") {
        Reply::Stats(s) => *s,
        other => panic!("Stats → {other:?}"),
    }
}

fn rows(c: &mut Client) -> u64 {
    match c
        .call(&Op::RowCount {
            table: TABLE.to_vec(),
            column: ROW_PK,
            value: Vec::new(),
        })
        .expect("RowCount")
    {
        Reply::Count(n) => n,
        other => panic!("RowCount → {other:?}"),
    }
}

fn execute(c: &mut Client, ops: &[Op]) -> Result<u64, String> {
    let mut done = 0u64;
    for batch in ops.chunks(PIPE) {
        let rs = c.pipeline(batch).map_err(|e| format!("pipeline: {e}"))?;
        if rs.len() != batch.len() {
            return Err(format!("应答 {} 条，请求 {} 条", rs.len(), batch.len()));
        }
        for r in rs {
            match r {
                Reply::Ok | Reply::Row(_) | Reply::Node(_) | Reply::NodeRef(_) => done += 1,
                Reply::Err { code, msg } => {
                    return Err(format!("errno {code}: {}", String::from_utf8_lossy(&msg)));
                }
                other => return Err(format!("写 op 回包形状 {other:?}")),
            }
        }
    }
    Ok(done)
}

fn seed(c: &mut Client, n: u64) {
    match c
        .call(&Op::TableCreate {
            def: TableDef {
                name: TABLE.to_vec(),
                columns: vec![
                    ColumnDef {
                        name: b"tag".to_vec(),
                        flags: COL_INDEX,
                    },
                    ColumnDef {
                        name: b"payload".to_vec(),
                        flags: 0,
                    },
                ],
            },
        })
        .expect("TableCreate")
    {
        Reply::Table(Some(_)) => {}
        other => panic!("TableCreate → {other:?}"),
    }
    let ops: Vec<Op> = (0..n).map(|i| put(i, 0)).collect();
    assert_eq!(execute(c, &ops).expect("seed"), n);
    assert_eq!(rows(c), n);
    // 把灌数据可能派出的后台快照在测量窗口外收干净：起点 WAL=0，且没有上一轮
    // 的 inflight job 会被误记进删除窗口。
    assert!(matches!(c.call(&Op::Checkpoint), Ok(Reply::Ok)));
    let s = stats(c);
    assert_eq!(s.wal_bytes, 0, "测量起点必须没有 WAL 尾巴");
}

fn rss_kb(pid: u32) -> Result<(u64, u64), String> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .map_err(|e| format!("读 /proc/{pid}/status: {e}"))?;
    let (mut rss, mut hwm) = (0u64, 0u64);
    for line in text.lines() {
        if let Some(v) = line.strip_prefix("VmRSS:") {
            rss = v.split_whitespace().next().unwrap().parse().unwrap();
        } else if let Some(v) = line.strip_prefix("VmHWM:") {
            hwm = v.split_whitespace().next().unwrap().parse().unwrap();
        }
    }
    if rss == 0 || hwm == 0 {
        return Err(format!("pid {pid} 没有有效的 VmRSS/VmHWM"));
    }
    Ok((rss, hwm))
}

#[derive(Default)]
struct Sample {
    count: u64,
    peak_wal: u64,
    peak_disk: u64,
    peak_rss_kb: u64,
    peak_hwm_kb: u64,
}

impl Sample {
    fn observe(&mut self, s: &EngineStats, rss: u64, hwm: u64) {
        self.count += 1;
        self.peak_wal = self.peak_wal.max(s.wal_bytes);
        self.peak_disk = self.peak_disk.max(s.wal_disk_bytes);
        self.peak_rss_kb = self.peak_rss_kb.max(rss);
        self.peak_hwm_kb = self.peak_hwm_kb.max(hwm);
    }
}

struct Sampler {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<Result<Sample, String>>>,
}

impl Sampler {
    fn start(srv: &Server) -> Self {
        let mut c = srv.client();
        let pid = srv.pid();
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let handle = std::thread::spawn(move || {
            let mut sample = Sample::default();
            loop {
                let s = stats(&mut c);
                let (rss, hwm) = rss_kb(pid)?;
                sample.observe(&s, rss, hwm);
                if flag.load(Ordering::Relaxed) {
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Ok(sample)
        });
        Self {
            stop,
            handle: Some(handle),
        }
    }

    fn stop(&mut self) -> Sample {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .expect("sampler already stopped")
            .join()
            .expect("sampler panicked")
            .expect("sampler failed")
    }
}

/// 等自动档至少收掉一份新快照。Stats 请求本身会驱动事件循环的 poll，因此不需要
/// 再发无意义的数据 op 去「催」它。超时是失败，不是跳过。
fn wait_auto_checkpoint(srv: &Server, before: &EngineStats) -> EngineStats {
    let mut c = srv.client();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let s = stats(&mut c);
        if s.checkpoints > before.checkpoints && s.snap_seq > before.snap_seq {
            return s;
        }
        assert!(
            Instant::now() < deadline,
            "自动 checkpoint 没在 30s 内收口：before={before:?}, now={s:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_wal_below_threshold(srv: &Server) -> EngineStats {
    let mut c = srv.client();
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let s = stats(&mut c);
        if s.wal_bytes < AUTO_MIB * 1048576 {
            return s;
        }
        assert!(
            Instant::now() < deadline,
            "WAL 30s 内没落到自动阈值以下：{s:?}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn verify_rows(c: &mut Client, n: u64, round: u64, tail: bool) {
    assert_eq!(rows(c), n);
    let mut checked = 0u64;
    for start in (0..n).step_by(PIPE) {
        let end = (start + PIPE as u64).min(n);
        let ops: Vec<Op> = (start..end)
            .map(|i| Op::RowGet {
                table: TABLE.to_vec(),
                key: key(i),
            })
            .collect();
        let rs = c.pipeline(&ops).expect("verify pipeline");
        for (i, r) in (start..end).zip(rs) {
            let want_round = if tail && i == 0 { round + 1 } else { round };
            match r {
                Reply::Row(Some(row)) => assert_eq!(
                    row.cols,
                    cols(i, want_round),
                    "第 {i} 行不是第 {want_round} 轮内容"
                ),
                other => panic!("第 {i} 行读回 {other:?}"),
            }
            checked += 1;
        }
    }
    assert_eq!(checked, n);
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / 1048576.0
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn create_view(c: &mut Client, name: &[u8]) -> (u32, u64) {
    match c.call(&Op::CreateView { name: name.to_vec() }).expect("CreateView") {
        Reply::View { id, root } => (id, root),
        other => panic!("CreateView → {other:?}"),
    }
}

fn create_file(c: &mut Client, view: u32, name: &[u8]) -> u64 {
    match c
        .call(&Op::Create {
            view,
            parent: 1,
            name: name.to_vec(),
            mode: 0o644,
            uid: 1,
            gid: 1,
        })
        .expect("Create")
    {
        Reply::Node(n) => n.ino,
        other => panic!("Create → {other:?}"),
    }
}

fn create_dir(c: &mut Client, view: u32, name: &[u8]) -> u64 {
    match c
        .call(&Op::Mkdir {
            view,
            parent: 1,
            name: name.to_vec(),
            mode: 0o755,
            uid: 1,
            gid: 1,
        })
        .expect("Mkdir")
    {
        Reply::Node(n) => n.ino,
        other => panic!("Mkdir → {other:?}"),
    }
}

fn commit_ref(c: &mut Client, view: u32, ino: u64, data_ref: &[u8]) {
    assert!(matches!(
        c.call(&Op::Setattr {
            view,
            ino,
            valid: SA_DATA_REF,
            mode: 0,
            uid: 0,
            gid: 0,
            size: 0,
            atime: 0,
            mtime: 0,
            data_ref: data_ref.to_vec(),
            ref_bytes: 4096,
        }),
        Ok(Reply::Node(_))
    ));
}

fn set_lock(c: &mut Client, view: u32, ino: u64, owner: &[u8], lease_expiry: i64) {
    assert!(matches!(
        c.call(&Op::LockSet {
            view,
            ino,
            owner: owner.to_vec(),
            verb: 0,
            kind: 1,
            lease_expiry,
        }),
        Ok(Reply::Ok)
    ));
}

fn ledger(s: &EngineStats) -> (u64, u64, u64, u64, u64, u64) {
    (s.views, s.nodes, s.dentries, s.locks, s.refs, s.tables)
}

#[test]
fn last_posix_delete_cascades_locks_and_returns_ledger_to_baseline() {
    // 关掉周期 sweep，先把「删除级联」与「到期清扫」两件事分开：过期锁在
    // rmdir 之前必须仍在表里，删 inode 的那一刻才与永久锁一起消失。
    let srv = Server::start_extra("group", 1, &["--lock-sweep-ms", "0"]);
    let mut c = srv.client();
    let (view, _) = create_view(&mut c, b"posix-lock-cascade");
    let baseline = ledger(&stats(&mut c));
    let file = create_file(&mut c, view, b"f");
    let dir = create_dir(&mut c, view, b"d");
    commit_ref(&mut c, view, file, b"ref-f");
    assert!(matches!(
        c.call(&Op::Link { view, ino: file, parent: 1, name: b"alias".to_vec() }),
        Ok(Reply::Node(_))
    ));
    set_lock(&mut c, view, file, b"forever", 0);
    set_lock(&mut c, view, dir, b"already-expired", now_ms() - 1);

    let seeded = stats(&mut c);
    assert_eq!(
        (seeded.nodes, seeded.dentries, seeded.locks, seeded.refs),
        (baseline.1 + 2, baseline.2 + 3, 2, baseline.4 + 1)
    );
    assert!(matches!(
        c.call(&Op::Unlink { view, parent: 1, name: b"f".to_vec() }),
        Ok(Reply::NodeRef(_))
    ));
    let one_name_left = stats(&mut c);
    assert_eq!(one_name_left.nodes, seeded.nodes, "还有 alias，inode 不能提前删");
    assert_eq!(one_name_left.refs, seeded.refs, "还有 alias，ref 不能提前下账");
    assert_eq!(one_name_left.locks, 2, "还有 alias，两把锁都不能提前清");

    assert!(matches!(
        c.call(&Op::Unlink { view, parent: 1, name: b"alias".to_vec() }),
        Ok(Reply::NodeRef(ref r)) if r == b"ref-f"
    ));
    let last_name_gone = stats(&mut c);
    assert_eq!(last_name_gone.nodes, baseline.1 + 1, "只剩目录 inode");
    assert_eq!(last_name_gone.refs, baseline.4);
    assert_eq!(last_name_gone.locks, 1, "文件上的永久锁已级联，目录过期锁还在");

    assert!(matches!(
        c.call(&Op::Rmdir { view, parent: 1, name: b"d".to_vec() }),
        Ok(Reply::NodeRef(ref r)) if r.is_empty()
    ));
    let after = stats(&mut c);
    assert_eq!(ledger(&after), baseline, "nodes/dentries/locks/refs 必须精确回到基线");
    assert!(matches!(
        c.call(&Op::RefLookup { data_ref: b"ref-f".to_vec() }),
        Ok(Reply::Ref { count: 0, bytes: 0 })
    ));
    srv.graceful_shutdown();

    println!(
        "posix_lock_cascade baseline={baseline:?} seeded={:?} after={:?} errors=0",
        (seeded.nodes, seeded.dentries, seeded.locks, seeded.refs),
        ledger(&after)
    );
}

#[test]
fn disabled_auto_checkpoint_keeps_wal_until_explicit_checkpoint() {
    let n = env_u64("TREEDB_DELETE_ROWS", 40_000);
    let srv = Server::start_extra("group", 1, &["--auto-checkpoint-mb", "0"]);
    let mut c = srv.client();
    assert!(matches!(
        c.call(&Op::TableCreate {
            def: TableDef {
                name: TABLE.to_vec(),
                columns: vec![
                    ColumnDef { name: b"tag".to_vec(), flags: COL_INDEX },
                    ColumnDef { name: b"payload".to_vec(), flags: 0 },
                ],
            },
        }),
        Ok(Reply::Table(Some(_)))
    ));
    let before = stats(&mut c);
    let ops: Vec<Op> = (0..n).map(|i| put(i, 0)).collect();
    assert_eq!(execute(&mut c, &ops).expect("load without auto checkpoint"), n);
    let loaded = stats(&mut c);
    assert!(loaded.wal_bytes > AUTO_MIB * 1048576, "负载必须越过 1 MiB 阈值");
    std::thread::sleep(Duration::from_millis(300));
    let settled = stats(&mut c);
    assert_eq!(settled.checkpoints, before.checkpoints, "auto=0 不得自己落快照");
    assert_eq!(settled.snap_seq, before.snap_seq, "auto=0 不得推进快照水位");
    assert_eq!(settled.wal_bytes, loaded.wal_bytes, "auto=0 不得截断 WAL");

    assert!(matches!(c.call(&Op::Checkpoint), Ok(Reply::Ok)));
    let explicit = stats(&mut c);
    assert_eq!(explicit.checkpoints, before.checkpoints + 1);
    assert!(explicit.snap_seq > before.snap_seq);
    assert_eq!(explicit.wal_bytes, 0, "显式 checkpoint 必须收掉当前 WAL");
    srv.graceful_shutdown();
    println!(
        "auto_checkpoint_off rows={n} wal_bytes={} checkpoints={} snap_seq={} explicit_wal=0 errors=0",
        loaded.wal_bytes, settled.checkpoints, settled.snap_seq
    );
}

#[test]
fn posix_delete_and_small_tail_survive_sigkill_reopen() {
    let srv = Server::start_extra("group", 1, &["--auto-checkpoint-mb", "1"]);
    let mut c = srv.client();
    let (view, _) = create_view(&mut c, b"posix-crash");
    let baseline = ledger(&stats(&mut c));
    let file = create_file(&mut c, view, b"f");
    let dir = create_dir(&mut c, view, b"d");
    commit_ref(&mut c, view, file, b"crash-ref");
    set_lock(&mut c, view, file, b"forever", 0);
    assert!(matches!(c.call(&Op::Checkpoint), Ok(Reply::Ok)));

    assert_eq!(
        execute(
            &mut c,
            &[
                Op::Unlink { view, parent: 1, name: b"f".to_vec() },
                Op::Rmdir { view, parent: 1, name: b"d".to_vec() },
            ],
        )
        .expect("posix delete"),
        2
    );
    assert_eq!(ledger(&stats(&mut c)), baseline);
    let tail = create_file(&mut c, view, b"tail");
    assert!(matches!(
        c.call(&Op::Unlink { view, parent: 1, name: b"tail".to_vec() }),
        Ok(Reply::NodeRef(_))
    ));
    assert!(tail > file && tail > dir, "ino 必须单调分配");
    let before_crash = stats(&mut c);
    assert!(before_crash.wal_bytes > 0, "必须留下已 ack 的小尾巴");

    let (tmp, dir_path) = srv.sigkill_and_take_dir();
    let srv = Server::restart_at_workers(tmp, dir_path, "group", 1);
    let mut c = srv.client();
    let reopened = stats(&mut c);
    assert_eq!(ledger(&reopened), baseline, "重放后删除终态不能复活");
    assert_eq!(reopened.seq, before_crash.seq);
    assert_eq!(reopened.wal_bytes, before_crash.wal_bytes, "重开必须带上水位后的尾巴");
    for name in [b"f".as_slice(), b"d", b"tail"] {
        assert!(matches!(
            c.call(&Op::Lookup { view, parent: 1, name: name.to_vec() }),
            Ok(Reply::Err { code, .. }) if code == errno::ENOENT
        ));
    }
    assert!(matches!(
        c.call(&Op::RefLookup { data_ref: b"crash-ref".to_vec() }),
        Ok(Reply::Ref { count: 0, bytes: 0 })
    ));
    srv.graceful_shutdown();
    println!(
        "posix_sigkill_reopen baseline={baseline:?} seq={} wal_bytes={} errors=0",
        reopened.seq, reopened.wal_bytes
    );
}

#[test]
fn bench_posix_seed_and_delete_keep_fixed_workload() {
    let n = 1_000u64;
    let srv = Server::start_extra("group", 4, &["--auto-checkpoint-mb", "0"]);
    let seed = Command::new(env!("CARGO_BIN_EXE_treedb-bench"))
        .arg("--addr")
        .arg(&srv.addr)
        .args(["--bulk", "posixseed", "--rows"])
        .arg(n.to_string())
        .output()
        .expect("run treedb-bench posixseed");
    assert!(
        seed.status.success(),
        "posixseed failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&seed.stdout),
        String::from_utf8_lossy(&seed.stderr)
    );
    let stopped = srv.graceful_shutdown();
    assert!(stopped.status.success());
    let srv = Server::restart_at_workers(stopped.tmp, stopped.dir, "group", 4);
    let output = Command::new(env!("CARGO_BIN_EXE_treedb-bench"))
        .arg("--addr")
        .arg(&srv.addr)
        .args(["--bulk", "posixdelete", "--rows"])
        .arg(n.to_string())
        .args(["--conns", "4", "--pipeline", "64", "--sample-ms", "5"])
        .output()
        .expect("run treedb-bench posixdelete");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "posixdelete failed\nstdout:\n{stdout}\nstderr:\n{stderr}");
    let line = stdout.lines().find(|l| l.starts_with("bulk ")).expect("posix bulk summary");
    for needle in [
        format!("ops={}/{}", 2 * n, 2 * n),
        format!("records={}", 6 * n),
        format!("writes={}", 2 * n),
        format!("freed_refs={n}"),
        format!("verify_refs={n}"),
        "nodes_left=1 dentries_left=0 locks_left=0 refs_left=0".to_string(),
        "errors=0".to_string(),
    ] {
        assert!(line.contains(&needle), "bench 汇总缺 {needle}: {line}");
    }
    assert!(line.contains("lat_p50_us=") && line.contains("lat_p99_us="));
    let mut c = srv.client();
    assert_eq!(ledger(&stats(&mut c)), (1, 1, 0, 0, 0, 0));
    srv.graceful_shutdown();
}

#[test]
fn fixed_row_delete_storm_checkpoints_and_reopens() {
    let n = env_u64("TREEDB_DELETE_ROWS", 40_000);
    let srv = Server::start_extra("group", 1, &["--auto-checkpoint-mb", "1"]);
    let mut c = srv.client();
    seed(&mut c, n);
    let before = stats(&mut c);
    let (rss_before, hwm_before) = rss_kb(srv.pid()).unwrap();

    let ops: Vec<Op> = (0..n).map(delete).collect();
    let mut sampler = Sampler::start(&srv);
    let t = Instant::now();
    let done = execute(&mut c, &ops).expect("delete storm");
    let time_ms = t.elapsed().as_millis() as u64;
    let sampled = sampler.stop();
    let immediate = stats(&mut c);
    let after_auto = wait_auto_checkpoint(&srv, &before);
    let (rss_after, hwm_after) = rss_kb(srv.pid()).unwrap();

    assert_eq!(done, n, "删除 op 必须全部完成");
    assert_eq!(rows(&mut c), 0, "删完必须零行");
    assert_eq!(immediate.seq - before.seq, n, "每条真删除恰有一条 WAL 记录");
    assert_eq!(immediate.writes_total - before.writes_total, n);
    assert!(
        after_auto.checkpoints > before.checkpoints,
        "1 MiB 自动档没有新增 checkpoint"
    );
    assert!(after_auto.snap_seq > before.snap_seq);
    assert!(after_auto.snap_bytes > 0);
    assert!(
        sampled.peak_wal > after_auto.wal_bytes,
        "没看见 WAL 从峰值回落：peak={}, after={}",
        sampled.peak_wal,
        after_auto.wal_bytes
    );
    assert!(sampled.peak_wal > AUTO_MIB * 1048576, "峰值没越过触发阈值");
    assert!(sampled.peak_disk >= sampled.peak_wal);
    // checkpoint 可能在本轮采样停止后才收口；VmHWM 是进程终身峰值，因此终态
    // 可以高于窗口内采样值，但没有任何理由低于基线。
    assert!(hwm_after >= hwm_before);
    assert!(sampled.count > 1, "采样点太少，不能称持续采样");

    // 不再显式 checkpoint：等自动档把 WAL 收到阈值以下，再写两条小尾巴。这样重开
    // 必须同时用上自动快照与快照水位之后的记录，而不是只读一份完整快照。
    wait_wal_below_threshold(&srv);
    let tail_ops = vec![put(0, 99), delete(0)];
    assert_eq!(execute(&mut c, &tail_ops).expect("delete tail"), 2);
    let final_stats = stats(&mut c);
    assert!(final_stats.wal_bytes > 0 && final_stats.wal_bytes < AUTO_MIB * 1048576);
    // 优雅停服会主动 checkpoint（把尾巴清零）；这里 kill -9 才能验证已 ack 的
    // 小尾巴与自动快照组合恢复。
    let (tmp, dir) = srv.sigkill_and_take_dir();
    let srv = Server::restart_at_workers(tmp, dir, "group", 1);
    let mut c = srv.client();
    let reopened = stats(&mut c);
    assert_eq!(rows(&mut c), 0, "重启后删除终态复活了");
    assert!(reopened.snap_seq >= final_stats.snap_seq);
    assert!(reopened.seq >= final_stats.seq);
    assert!(reopened.wal_bytes > 0, "重启读数没带上水位之后的尾巴");
    srv.graceful_shutdown();

    println!(
        "delete_storm workers=1 rows={n} ops={done} time_ms={time_ms} records={} writes={} \
         syncs={} checkpoints={} wal_peak={} wal_after_auto={} wal_before_shutdown={} snap_seq={} \
         snap_bytes={} rss_kb={rss_before}/{rss_after} peak_rss_kb={} hwm_kb={hwm_before}/{hwm_after} \
         peak_hwm_kb={} retained_delta_kb={} samples={} reopen_rows=0 reopen_wal={} errors=0",
        immediate.seq - before.seq,
        immediate.writes_total - before.writes_total,
        immediate.syncs_total - before.syncs_total,
        after_auto.checkpoints - before.checkpoints,
        sampled.peak_wal,
        after_auto.wal_bytes,
        final_stats.wal_bytes,
        reopened.snap_seq,
        reopened.snap_bytes,
        sampled.peak_rss_kb,
        sampled.peak_hwm_kb,
        rss_after as i64 - rss_before as i64,
        sampled.count,
        reopened.wal_bytes,
    );
}

#[test]
fn repeated_delete_reinsert_churn_keeps_content_and_reports_rss() {
    let n = env_u64("TREEDB_DELETE_ROWS", 40_000);
    let cycles = env_u64("TREEDB_DELETE_CYCLES", 3);
    assert!(cycles >= 1, "TREEDB_DELETE_CYCLES 必须 >= 1");
    let srv = Server::start_extra("group", 8, &["--auto-checkpoint-mb", "1"]);
    let mut c = srv.client();
    seed(&mut c, n);
    let baseline = stats(&mut c);
    let (rss_baseline, hwm_baseline) = rss_kb(srv.pid()).unwrap();
    let mut peak_rss = rss_baseline;
    let mut peak_hwm = hwm_baseline;
    let mut peak_wal_all = 0u64;
    let mut total_checkpoints = 0u64;

    for round in 1..=cycles {
        let before = stats(&mut c);
        let (rss_before, hwm_before) = rss_kb(srv.pid()).unwrap();
        let mut ops = Vec::with_capacity((2 * n) as usize);
        ops.extend((0..n).map(delete));
        ops.extend((0..n).map(|i| put(i, round)));
        let mut sampler = Sampler::start(&srv);
        let t = Instant::now();
        let done = execute(&mut c, &ops).expect("churn");
        let time_ms = t.elapsed().as_millis() as u64;
        let sampled = sampler.stop();
        let immediate = stats(&mut c);
        let after_auto = wait_auto_checkpoint(&srv, &before);
        let (rss_after, hwm_after) = rss_kb(srv.pid()).unwrap();
        peak_rss = peak_rss.max(sampled.peak_rss_kb).max(rss_after);
        peak_hwm = peak_hwm.max(sampled.peak_hwm_kb).max(hwm_after);
        peak_wal_all = peak_wal_all.max(sampled.peak_wal);
        let checkpoints = after_auto.checkpoints - before.checkpoints;
        total_checkpoints += checkpoints;

        assert_eq!(done, 2 * n, "churn 每轮必须恰好 2N 个 op");
        assert_eq!(immediate.seq - before.seq, 2 * n);
        assert_eq!(immediate.writes_total - before.writes_total, 2 * n);
        assert_eq!(rows(&mut c), n);
        assert!(checkpoints > 0, "第 {round} 轮没触发自动 checkpoint");
        assert!(sampled.peak_wal > after_auto.wal_bytes);
        assert!(hwm_after >= hwm_before);
        verify_rows(&mut c, n, round, false);

        println!(
            "churn_cycle workers=8 round={round}/{cycles} rows={n} ops={done} time_ms={time_ms} \
             records={} writes={} syncs={} checkpoints={checkpoints} wal_peak={} \
             wal_after_auto={} snap_seq={} snap_bytes={} rss_kb={rss_before}/{rss_after} \
             peak_rss_kb={} peak_hwm_kb={} retained_delta_kb={} samples={} errors=0",
            immediate.seq - before.seq,
            immediate.writes_total - before.writes_total,
            immediate.syncs_total - before.syncs_total,
            sampled.peak_wal,
            after_auto.wal_bytes,
            after_auto.snap_seq,
            after_auto.snap_bytes,
            sampled.peak_rss_kb,
            sampled.peak_hwm_kb,
            rss_after as i64 - rss_before as i64,
            sampled.count,
        );
    }

    assert!(stats(&mut c).checkpoints - baseline.checkpoints >= cycles);
    // 只改一行作为尾巴：内容与行数都能单独通过，old snapshot / tail 混用才会错。
    wait_wal_below_threshold(&srv);
    assert_eq!(execute(&mut c, &[put(0, cycles + 1)]).expect("churn tail"), 1);
    let final_stats = stats(&mut c);
    assert!(final_stats.wal_bytes > 0 && final_stats.wal_bytes < AUTO_MIB * 1048576);
    let (tmp, dir) = srv.sigkill_and_take_dir();
    let srv = Server::restart_at_workers(tmp, dir, "group", 8);
    let mut c = srv.client();
    let reopened = stats(&mut c);
    assert!(reopened.snap_seq >= final_stats.snap_seq);
    assert!(reopened.wal_bytes > 0, "重启读数没带上 churn 尾巴");
    verify_rows(&mut c, n, cycles, true);
    let (rss_reopened, _) = rss_kb(srv.pid()).unwrap();
    srv.graceful_shutdown();

    println!(
        "churn_summary workers=8 rows={n} cycles={cycles} ops={} records={} \
         total_checkpoints={total_checkpoints} wal_peak_mib={:.3} snap_bytes={} \
         rss_kb={rss_baseline}/{rss_reopened} retained_delta_kb={} peak_rss_kb={peak_rss} \
         hwm_kb={hwm_baseline}/{peak_hwm} reopen_checked={n} reopen_wal={} errors=0",
        2 * n * cycles,
        reopened.seq - baseline.seq,
        mib(peak_wal_all),
        reopened.snap_bytes,
        rss_reopened as i64 - rss_baseline as i64,
        reopened.wal_bytes,
    );
}

#[test]
fn bench_churn_parallel_connections_keep_round_barriers() {
    let n = 1_000u64;
    let cycles = 4u64;
    let want = 2 * n * cycles;
    let srv = Server::start_extra("group", 8, &[]);
    let output = Command::new(env!("CARGO_BIN_EXE_treedb-bench"))
        .arg("--addr")
        .arg(&srv.addr)
        .args(["--bulk", "churn"])
        .arg("--rows")
        .arg(n.to_string())
        .arg("--cycles")
        .arg(cycles.to_string())
        .args(["--conns", "8", "--pipeline", "64", "--sample-ms", "5"])
        .output()
        .expect("run treedb-bench churn");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "bench churn failed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    let line = stdout
        .lines()
        .find(|l| l.starts_with("bulk "))
        .expect("bench bulk summary");
    for needle in [
        format!("ops={want}/{want}"),
        format!("records={want}"),
        format!("writes={want}"),
        format!("rows_left={n}"),
        "errors=0".to_string(),
    ] {
        assert!(line.contains(&needle), "bench 汇总缺 {needle}: {line}");
    }

    let mut c = srv.client();
    assert_eq!(bulk_rows(&mut c), n);
    for base in (0..n).step_by(500) {
        let end = (base + 500).min(n);
        let batch: Vec<Op> = (base..end)
            .map(|i| Op::RowGet {
                table: b"bulkkv".to_vec(),
                key: format!("b{i:08}").into_bytes(),
            })
            .collect();
        let rs = c.pipeline(&batch).expect("verify bench churn");
        for (i, r) in (base..end).zip(rs) {
            match r {
                Reply::Row(Some(row)) => assert_eq!(
                    row.cols,
                    vec![
                        format!("t{}", i % 16).into_bytes(),
                        format!("p{i}c{cycles}").into_bytes(),
                    ],
                    "第 {i} 行不是最后一轮"
                ),
                other => panic!("第 {i} 行读回 {other:?}"),
            }
        }
    }
    srv.graceful_shutdown();
}

fn bulk_rows(c: &mut Client) -> u64 {
    match c
        .call(&Op::RowCount {
            table: b"bulkkv".to_vec(),
            column: ROW_PK,
            value: Vec::new(),
        })
        .expect("bulk RowCount")
    {
        Reply::Count(n) => n,
        other => panic!("bulk RowCount → {other:?}"),
    }
}
