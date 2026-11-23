//! 恢复成本随状态规模的伸缩（§8 第 2 条）。
//!
//! 状态全内存，恢复 = 读快照 + 重放尾日志。默认档造 10 万节点（再写 2 万条尾巴）
//! 进回归板，钉的是「大状态恢复之后语义还对」；百万到千万档手动跑，钉的是耗时 /
//! 内存随节点数的线性度，数字就是 DESIGN §8 第 2 条引用的那一组：
//!
//! ```sh
//! cargo test --release --test scale_restore            # 10 万节点，进回归板
//! TREEDB_RESTORE_NODES=10000000 TREEDB_RESTORE_TAIL=100000 \
//!   cargo test --release --test scale_restore -- --ignored --nocapture
//! ```
//!
//! `TREEDB_RESTORE_DIR` 给大规模档指定数据目录（默认 `$TMPDIR/treedb-restore-scale`）；
//! 跑完会删掉自己建的那个子目录。

use std::path::{Path, PathBuf};
use std::time::Instant;
use treedb::engine::model::{Op, Reply};
use treedb::engine::{Durability, Engine};

const DIRS: usize = 32;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn ok(e: &mut Engine, op: &Op) -> Reply {
    match e.submit(op).expect("submit") {
        Reply::Err { code, msg } => panic!(
            "{op:?} → errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        r => r,
    }
}

/// `(VmRSS, VmHWM)`，单位 KiB。HWM 是进程峰值，能看见 checkpoint 编码那一笔。
fn rss_kb() -> (u64, u64) {
    let s = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    let (mut rss, mut hwm) = (0u64, 0u64);
    for l in s.lines() {
        if let Some(v) = l.strip_prefix("VmRSS:") {
            rss = v.split_whitespace().next().unwrap().parse().unwrap();
        } else if let Some(v) = l.strip_prefix("VmHWM:") {
            hwm = v.split_whitespace().next().unwrap().parse().unwrap();
        }
    }
    (rss, hwm)
}

fn mb(kb: u64) -> f64 {
    kb as f64 / 1024.0
}

/// 造一个视图 + 32 个目录 + `n` 个文件（名字 13 字节，接近 grovefs 的十六进制键）。
fn build(dir: &Path, n: usize) -> (Engine, u32, Vec<u64>, f64) {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let mut e = Engine::open(dir, Durability::Never).unwrap();
    let v = match ok(&mut e, &Op::CreateView { name: b"v".to_vec() }) {
        Reply::View { id, .. } => id,
        other => panic!("{other:?}"),
    };
    let mut dirs = Vec::with_capacity(DIRS);
    for d in 0..DIRS {
        match ok(
            &mut e,
            &Op::Mkdir {
                view: v,
                parent: 1,
                name: format!("d{d:02}").into_bytes(),
                mode: 0o755,
                uid: 1000,
                gid: 1000,
            },
        ) {
            Reply::Node(nd) => dirs.push(nd.ino),
            other => panic!("{other:?}"),
        }
    }
    let t = Instant::now();
    for i in 0..n {
        ok(
            &mut e,
            &Op::Create {
                view: v,
                parent: dirs[i % DIRS],
                name: format!("f{i:012x}").into_bytes(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );
    }
    let secs = t.elapsed().as_secs_f64();
    (e, v, dirs, secs)
}

/// 跑一档，返回给 stdout 的两行摘要。断言都在这儿，两个入口共用同一份检查。
fn run(dir: &Path, n: usize, tail: usize) -> String {
    let (mut e, v, dirs, insert) = build(dir, n);
    let (rss_ins, hwm_ins) = rss_kb();
    let st = e.snapshot_stats();
    let nodes = st.nodes;
    let wal = st.wal_bytes;
    assert_eq!(nodes, n as u64 + 1 + DIRS as u64, "插入后的节点数不对");

    // 同步 checkpoint：编码 + 写 + fsync + 收尾的 truncate_before 全算在里面。
    let t = Instant::now();
    e.checkpoint().unwrap();
    let ckpt = t.elapsed().as_secs_f64() * 1e3;
    let snap = e.snapshot_stats().snap_bytes;
    let (rss_ck, hwm_ck) = rss_kb();
    assert_eq!(e.snapshot_stats().wal_bytes, 0, "checkpoint 之后 WAL 逻辑长度应归零");
    drop(e);

    // 冷恢复：快照还没进页缓存。
    let t = Instant::now();
    let mut e = Engine::open(dir, Durability::Never).unwrap();
    let cold = t.elapsed().as_secs_f64() * 1e3;
    let (rss_open, hwm_open) = rss_kb();
    let st = e.snapshot_stats();
    assert_eq!(st.nodes, nodes, "快照恢复后节点数变了");
    assert_eq!(st.wal_bytes, 0, "纯快照恢复不该有尾巴");
    // 恢复不是只把数字填上：随机挑一个文件按名字走一次 lookup。
    let probe = format!("f{:012x}", n / 2);
    match ok(
        &mut e,
        &Op::Lookup {
            view: v,
            parent: dirs[(n / 2) % DIRS],
            name: probe.into_bytes(),
        },
    ) {
        Reply::Node(_) => {}
        other => panic!("lookup after restore: {other:?}"),
    }
    drop(e);

    // 热基线：快照已在页缓存，这一次只花解码 + 建表。和下面「快照 + 尾巴」同口径
    // 相比，差值才是重放的钱（拿冷恢复当基线会把读盘算进重放）。
    let t = Instant::now();
    let mut e = Engine::open(dir, Durability::Never).unwrap();
    let hot = t.elapsed().as_secs_f64() * 1e3;

    let t = Instant::now();
    for i in 0..tail {
        ok(
            &mut e,
            &Op::Create {
                view: v,
                parent: dirs[i % DIRS],
                name: format!("t{i:012x}").into_bytes(),
                mode: 0o644,
                uid: 1000,
                gid: 1000,
            },
        );
    }
    let tail_write = t.elapsed().as_secs_f64();
    let (rss_tail, hwm_tail) = rss_kb();
    drop(e);

    let t = Instant::now();
    let mut e = Engine::open(dir, Durability::Never).unwrap();
    let hot_tail = t.elapsed().as_secs_f64() * 1e3;
    let st = e.snapshot_stats();
    assert_eq!(st.nodes, nodes + tail as u64, "尾日志重放后节点数不对");
    drop(e);

    let replay = (hot_tail - hot).max(0.0);
    let line = format!(
        "[n={n:>9}] nodes={nodes} insert={insert:.2}s ({:.0} op/s) wal={:.0}MB \
         ckpt={ckpt:.0}ms snap={:.1}MB | tail_write={tail_write:.2}s open cold={cold:.0}ms hot={hot:.0}ms \
         ({:.0} nodes/s) hot+{tail}tail={hot_tail:.0}ms replay≈{replay:.0}ms \
         ({:.0} rec/s) | rss MB ins={:.0}/{:.0} ckpt={:.0}/{:.0} open={:.0}/{:.0} \
         tail={:.0}/{:.0} (rss/peak) B/node rss={:.0} snap={:.0}",
        nodes as f64 / insert,
        wal as f64 / 1048576.0,
        snap as f64 / 1048576.0,
        nodes as f64 / hot * 1000.0,
        if replay > 0.0 { tail as f64 / replay * 1000.0 } else { 0.0 },
        mb(rss_ins),
        mb(hwm_ins),
        mb(rss_ck),
        mb(hwm_ck),
        mb(rss_open),
        mb(hwm_open),
        mb(rss_tail),
        mb(hwm_tail),
        rss_ins as f64 * 1024.0 / nodes as f64,
        snap as f64 / nodes as f64,
    );
    let _ = std::fs::remove_dir_all(dir);
    line
}

/// 回归板这一条：10 万节点 + 2 万条尾巴走完整个「插入 → checkpoint → 两次恢复」，
/// 验的是大状态恢复之后名字、节点数、水位都对。
#[test]
fn restore_survives_a_hundred_thousand_nodes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("data");
    let n = env_usize("TREEDB_RESTORE_NODES", 100_000);
    let tail = env_usize("TREEDB_RESTORE_TAIL", 20_000);
    println!("{}", run(&dir, n, tail));
}

/// 百 MB 到 GB 级快照：手动跑，输出就是 §8 第 2 条引用的那几个数。
#[test]
#[ignore = "百万到千万节点：手动跑，数字供 DESIGN §8 第 2 条引用"]
fn restore_cost_scales_to_ten_million_nodes() {
    let base = std::env::var("TREEDB_RESTORE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("treedb-restore-scale"));
    std::fs::create_dir_all(&base).expect("restore dir");
    let scales: Vec<usize> = match std::env::var("TREEDB_RESTORE_NODES") {
        Ok(v) => vec![v.parse().expect("TREEDB_RESTORE_NODES")],
        Err(_) => vec![1_000_000, 3_000_000, 10_000_000],
    };
    let tail = env_usize("TREEDB_RESTORE_TAIL", 100_000);
    for n in scales {
        println!("{}", run(&base.join(format!("n{n}")), n, tail));
    }
}
