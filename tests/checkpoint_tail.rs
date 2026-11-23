//! 后台快照（§8 第 7 条）收尾那一轮在提交线程上的代价。
//!
//! 后台线程把「重放 + 编码 + 写盘」搬走了，但 `checkpoint_poll` 仍要在提交
//! 线程上把 WAL 逐帧读一遍、丢掉水位之前的那些记录（`truncate_before`）。
//! 这个读口曾经是一帧两次 `read` 系统调用：64 MiB / 88 万条记录要停 510 ms，
//! 尾巴留 32 MiB 时 2.7 s。缓冲之后同一条路径是 89 / 258 ms。
//!
//! ```text
//! cargo test --release --test checkpoint_tail                # 小规模正确性，进回归板
//! TREEDB_CKPT_MB=64 cargo test --release --test checkpoint_tail -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};
use treedb::engine::model::SA_SIZE;
use treedb::engine::{Durability, Engine, Op, Reply};

fn ok(e: &mut Engine, op: &Op) -> Reply {
    match e.submit(op).expect("submit") {
        Reply::Err { code, msg } => panic!("{op:?} → errno {code} ({})", String::from_utf8_lossy(&msg)),
        r => r,
    }
}

/// 一个视图 + 一个文件，返回 (view, ino)。
fn seed(e: &mut Engine) -> (u32, u64) {
    let v = match ok(e, &Op::CreateView { name: b"v".to_vec() }) {
        Reply::View { id, .. } => id,
        other => panic!("{other:?}"),
    };
    let ino = match ok(
        e,
        &Op::Create {
            view: v,
            parent: 1,
            name: b"f".to_vec(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ) {
        Reply::Node(nd) => nd.ino,
        other => panic!("{other:?}"),
    };
    (v, ino)
}

/// 反复改同一个文件的 size 把 WAL 推到 `target` 字节（状态只有 1 个节点，
/// 后台编快照的开销因此可以忽略，测的就是日志搬运）。返回 (wal 字节, 最后 size)。
fn fill(e: &mut Engine, v: u32, ino: u64, target: u64, ctr: &mut u64) -> (u64, u64) {
    while e.wal_bytes() < target {
        *ctr += 1;
        ok(
            e,
            &Op::Setattr {
                view: v,
                ino,
                valid: SA_SIZE,
                mode: 0,
                uid: 0,
                gid: 0,
                size: *ctr,
                atime: 0,
                mtime: 0,
                data_ref: Vec::new(),
                ref_bytes: 0,
            },
        );
    }
    (e.wal_bytes(), *ctr)
}

/// 派一份后台快照、写 `tail` 字节新记录、等它落定，返回收尾那一轮的毫秒数。
fn run_tail(e: &mut Engine, v: u32, ino: u64, tail: u64, ctr: &mut u64) -> f64 {
    let full = e.wal_bytes();
    e.sync_now().expect("sync_now");
    assert!(e.checkpoint_start().expect("checkpoint_start"), "没派出去");
    fill(e, v, ino, full + tail, ctr);
    let deadline = Instant::now() + Duration::from_secs(60);
    while !e.checkpoint_ready() {
        assert!(Instant::now() < deadline, "后台快照一直没结束");
        std::thread::sleep(Duration::from_millis(1));
    }
    let t = Instant::now();
    assert!(e.checkpoint_poll().expect("poll").is_some(), "这一次该落定");
    t.elapsed().as_secs_f64() * 1000.0
}

/// 尾巴必须原样留在新日志里：条数、seq、重启后的可见状态三者一起查。
#[test]
fn compaction_keeps_the_tail_and_reopens_cleanly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("data");
    std::fs::create_dir_all(&dir).unwrap();
    let mut e = Engine::open(&dir, Durability::Never).expect("open");
    let (v, ino) = seed(&mut e);
    let mut ctr = 0u64;
    fill(&mut e, v, ino, 4 << 20, &mut ctr);
    let tail_at = ctr;
    let _ = run_tail(&mut e, v, ino, 512 << 10, &mut ctr);
    assert!(ctr > tail_at, "尾巴一条都没写进去");
    let tail_bytes = e.wal_bytes();
    assert!(tail_bytes > 0, "收尾把尾巴也抹了");
    assert!(tail_bytes < 1 << 20, "尾巴不该有这么大：{tail_bytes}");
    let snap = e.snapshot_stats().snap_bytes;
    assert!(snap > 0, "后台快照没落盘");
    drop(e);

    // 不 checkpoint 直接重开：恢复路径 = 快照（水位）+ 剩下的尾巴。
    let mut e = Engine::open(&dir, Durability::Never).expect("reopen");
    let nd = match ok(
        &mut e,
        &Op::Lookup {
            view: v,
            parent: 1,
            name: b"f".to_vec(),
        },
    ) {
        Reply::Node(nd) => nd,
        other => panic!("{other:?}"),
    };
    assert_eq!(nd.size, ctr, "尾巴里的最后一次 setattr 没重放出来");
    assert_eq!(e.snapshot_stats().nodes, 2, "只有视图根 + 一个文件");
}

/// `--checkpoint-bytes` 默认 64 MiB 下的真实停顿时长；数字供 DESIGN §8 第 7 条引用。
#[test]
#[ignore = "64 MiB 触发一轮：手动跑，数字供 DESIGN §8 第 7 条② 引用"]
fn compaction_cost_at_the_default_threshold() {
    let mb = 1u64 << 20;
    let limit: f64 = std::env::var("TREEDB_CKPT_LIMIT_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1500.0);
    let base = std::env::temp_dir().join("treedb-ckpt-tail");
    for tail_mb in [0u64, 4, 32] {
        let dir = base.join(format!("t{tail_mb}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut e = Engine::open(&dir, Durability::Never).expect("open");
        let (v, ino) = seed(&mut e);
        let mut ctr = 0u64;
        let t = Instant::now();
        fill(&mut e, v, ino, 64 * mb, &mut ctr);
        let fill_ms = t.elapsed().as_secs_f64() * 1000.0;
        let poll_ms = run_tail(&mut e, v, ino, tail_mb * mb, &mut ctr);
        println!(
            "[64MB 触发 tail={tail_mb}MB] 写满={fill_ms:.0}ms 收尾那一轮={poll_ms:.1}ms \
             (尾巴 +{tail_mb}MB，日志现在 {} B)",
            e.wal_bytes(),
        );
        assert!(
            poll_ms < limit,
            "收尾停 {poll_ms:.1}ms，超过 {limit}ms 的回归线（缓冲搬运被退回去了？）"
        );
        drop(e);
        let _ = std::fs::remove_dir_all(&dir);
    }
    let _ = std::fs::remove_dir_all(&base);
}
