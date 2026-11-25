//! 日志传送与只读副本验收（DESIGN.md §6.6）。
//!
//! 六件事各自一条：主写→副本收敛、主没了副本仍可答读、gap 触发整份重装快照、
//! 副本拒写（引擎档与线协议档各一档）、lag 计数的可观测性、`treedb-standby`
//! 这个二进制端到端跑通。
//!
//! 最后五条是 fencing token（§6.6 第 3 条）：令牌自身的只升不降 / 副本侧对快照
//! 与批的闸门 / 提权后在线上拒旧主 / `--promote` 这个离线动作 / 真主与真副本的
//! 完整切主链。切主是这套东西唯一的存在理由，而它恰恰是平时跑不到的那条路径 ——
//! 所以单独一组，不混在追平用例里。
//!
//! 前五条刻意**不起副本进程**：主是真进程（走真网络、真 group commit），副本
//! 是测试进程里的 `Engine` + `Follower`。这样"追平逻辑对不对"与"两个进程怎么
//! 拼起来"分开失败。第六条才把 `treedb-standby` 拉起来，覆盖线协议上的
//! `EROFS` 与 `Stats` 里那组新字段的真实编解码。
//!
//! 跑法：`cargo test --test replication`

#[path = "common/mod.rs"]
mod common;

use common::{free_addr, test_key, Server};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use treedb::engine::core::{EngineStats, SNAP_FILE};
use treedb::engine::model::{
    errno, ColumnDef, Kind, Node, Op, Reply, TableDef, COND_NONE, MAX_BATCH_OPS, SA_MODE,
    WAL_FETCH_MAX,
};
use treedb::engine::wal::crc32;
use treedb::engine::{Durability, Engine, FENCE_FILE};
use treedb::error::Error;
use treedb::net::auth::Key;
use treedb::net::client::Client;
use treedb::net::replicate::{Follower, FollowerProgress, ReplicaCfg, PREFIX_PROBE_BUDGET};

// ---------------------------------------------------------------- 驱动脚手架

fn fcfg(primary: &str, interval_ms: u64, max_bytes: u32) -> ReplicaCfg {
    let mut c = ReplicaCfg::new(primary.parse().expect("primary addr"));
    c.interval_ms = interval_ms;
    c.max_bytes = max_bytes;
    // 测试里把超时压短：主宕机那一条用例不该在 SYN 重试上耗掉整个 test job。
    c.timeout_ms = 1_000;
    c
}

fn replica(dir: &Path) -> Engine {
    // 副本用 Always：追平回来的记录必须当场落盘，测试不想再等一轮周期 sync。
    Engine::open_replica(dir, Durability::Always).expect("open_replica")
}

fn okc(c: &mut Client, op: &Op) -> Reply {
    match c.call(op) {
        Ok(Reply::Err { code, msg }) => {
            panic!("{op:?} → errno {code} ({})", String::from_utf8_lossy(&msg))
        }
        Ok(r) => r,
        Err(e) => panic!("{op:?} → transport: {e}"),
    }
}

fn ok(e: &mut Engine, op: &Op) -> Reply {
    let (logs, r) = e.exec(op);
    assert!(logs.is_empty(), "{op:?} → 只读 op 不该产出日志");
    match r {
        Reply::Err { code, msg } => {
            panic!("{op:?} → errno {code} ({})", String::from_utf8_lossy(&msg))
        }
        r => r,
    }
}

fn node(r: Reply) -> Node {
    match r {
        Reply::Node(n) => n,
        other => panic!("expected Reply::Node, got {other:?}"),
    }
}

fn view_id(r: Reply) -> u32 {
    match r {
        Reply::View { id, root } => {
            assert_eq!(root, 1, "view root must be ino 1");
            id
        }
        other => panic!("expected Reply::View, got {other:?}"),
    }
}

fn stats(c: &mut Client) -> EngineStats {
    match c.call(&Op::Stats).expect("stats call") {
        Reply::Stats(s) => *s,
        other => panic!("expected Stats, got {other:?}"),
    }
}

/// 一棵能在副本上被逐条比对的小树：一个视图 + `n` 个文件 + 一个目录。
/// 返回视图 id；文件名形如 `f{i}`，内容靠 mode 区分，便于断言"是同一条记录"。
fn seed_primary(srv: &Server, tag: &str, n: u32) -> u32 {
    let mut c = srv.client();
    let vid = view_id(okc(&mut c, &Op::CreateView { name: tag.into() }));
    for i in 0..n {
        okc(
            &mut c,
            &Op::Create {
                view: vid,
                parent: 1,
                name: format!("f{i}").into_bytes(),
                mode: 0o640 + (i % 7),
                uid: 1000 + i,
                gid: 100,
            },
        );
    }
    okc(
        &mut c,
        &Op::Mkdir {
            view: vid,
            parent: 1,
            name: b"d".to_vec(),
            mode: 0o755,
            uid: 0,
            gid: 0,
        },
    );
    vid
}

/// 反复 tick 直到“问过主、lag 归零、不在下载快照”。
///
/// 判据里必须带 `primary_seq > 0`：一个从没联系上主的副本 lag 也是 0
/// （`replica_primary` 还是 0），只看 lag 会把它读成“已追平”。
///
/// 必须**先睡满一个间隔再 tick**：反过来的话，上一轮刚追平的副本会在
/// `interval_ms` 里被限频跳过那一次 tick，而 `lag == 0` 仍是上一轮的旧值——
/// 于是主刚刚接上去的那条写会被当成已追平。这个提前返回是写本轮用例时
/// 真踩到的，不是臆想出来的防御。
fn converge(f: &mut Follower, eng: &mut Engine, ctx: &str) -> FollowerProgress {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        std::thread::sleep(Duration::from_millis(3));
        f.tick(eng)
            .unwrap_or_else(|e| panic!("tick must never be fatal here: {e}"));
        let p = f.progress(eng);
        if p.primary_seq > 0 && p.lag == 0 && !p.downloading {
            return p;
        }
        if Instant::now() > deadline {
            panic!("replica never converged ({p:?})\n{ctx}");
        }
    }
}

fn lookup(eng: &mut Engine, vid: u32, name: &str) -> Node {
    node(ok(
        eng,
        &Op::Lookup {
            view: vid,
            parent: 1,
            name: name.into(),
        },
    ))
}

fn read_only_dir(tag: &str) -> (TempDir, PathBuf) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join(tag);
    (tmp, dir)
}

/// 停服后冷拷一份目录：连 `.lock` 与 `fence` 一起搬，锁本身随进程退出释放。
fn copy_tree(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).expect("create copy root");
    for entry in std::fs::read_dir(src).expect("read copy source") {
        let entry = entry.expect("copy entry");
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if entry.file_type().expect("copy file type").is_dir() {
            copy_tree(&from, &to);
        } else {
            std::fs::copy(&from, &to).expect("copy file");
        }
    }
}

fn fence_value(dir: &Path) -> u64 {
    let path = dir.join(FENCE_FILE);
    if !path.exists() {
        return 0;
    }
    std::fs::read_to_string(path)
        .expect("read fence")
        .trim()
        .parse()
        .expect("fence number")
}

// ------------------------------------------------------------------- 用例

/// 主上的写在副本上收敛，且逐条比对得出：同一个 ino、同一份属性。
#[test]
fn primary_writes_converge_at_the_replica() {
    let srv = Server::start("group");
    let vid = seed_primary(&srv, "rconv", 40);

    let (_tmp, dir) = read_only_dir("rep");
    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, 1 << 20));
    let p = converge(&mut f, &mut eng, &srv.log_text());
    assert!(!p.downloading, "converged副本不该还在下载快照");
    assert_eq!(p.lag, 0);
    assert_eq!(p.connected, true);
    // 41 个写 op（40 个 Create + 1 个 Mkdir）+ CreateView = 42 条日志，
    // 但一条 op 可能落不止一条记录，所以这里只断言下界。
    assert!(p.applied >= 42, "applied = {}，至少该有 42 条", p.applied);

    let mut pc = srv.client();
    for i in 0..40u32 {
        let name = format!("f{i}");
        let on_replica = lookup(&mut eng, vid, &name);
        let on_primary = node(okc(
            &mut pc,
            &Op::Lookup {
                view: vid,
                parent: 1,
                name: name.clone().into_bytes(),
            },
        ));
        assert_eq!(on_replica.ino, on_primary.ino, "{name}: ino 必须一致");
        assert_eq!(on_replica.mode, 0o640 + (i % 7));
        assert_eq!(on_replica.uid, 1000 + i);
        assert_eq!(on_replica.parent, 1);
    }
    let d = lookup(&mut eng, vid, "d");
    assert_eq!(d.kind, Kind::Dir, "Mkdir 也要传到副本");

    // 副本自己算的 readdir 与主一致：视图根下 41 条。
    match ok(
        &mut eng,
        &Op::ReadDir {
            view: vid,
            parent: 1,
            offset: 0,
            limit: 100,
        },
    ) {
        Reply::Dirents { inos, end, .. } => {
            assert_eq!(inos.len(), 41, "readdir 条目数与主不一致");
            assert!(end, "一页就该装完");
        }
        other => panic!("expected Dirents, got {other:?}"),
    }

    // 追平之后再写的第二批也要收敛，验证稳态（非冷启动）的 tick 也在工作。
    okc(
        &mut pc,
        &Op::Create {
            view: vid,
            parent: 1,
            name: b"late".to_vec(),
            mode: 0o600,
            uid: 7,
            gid: 7,
        },
    );
    let p = converge(&mut f, &mut eng, &srv.log_text());
    assert_eq!(lookup(&mut eng, vid, "late").uid, 7);
    assert_eq!(p.lag, 0);
}

/// 主开了接入认证时，副本必须握得上手才追得动（§4.4）。
///
/// 两个方向都要钉：带对密钥的副本收敛得与非认证拓扑一模一样；带错密钥的
/// 副本**永不收敛但也不退出**，而且失败必须落在可观测的计数里 —— 一个
/// 静默不追的副本比一个会报错的副本更坏，运维看 `--stats` 就该看见它坏了。
#[test]
fn a_replica_hands_shakes_with_an_authenticated_primary() {
    let srv = Server::start_auth("group", 4);
    let vid = seed_primary(&srv, "rauth", 20);

    let (_tmp, dir) = read_only_dir("rep-auth-ok");
    let mut eng = replica(&dir);
    let mut cfg = fcfg(&srv.addr, 1, 1 << 20);
    cfg.auth = Some(Arc::new(test_key()));
    let mut f = Follower::new(cfg);
    let p = converge(&mut f, &mut eng, &srv.log_text());
    assert_eq!(p.lag, 0);
    assert!(p.connected, "握过手的副本才算连上");
    assert_eq!(p.errors, 0, "对密钥不该抬 errors：{p:?}");
    // 内容与非认证副本同构：握手只建立身份，不改传送的任何字节。
    assert_eq!(lookup(&mut eng, vid, "f0").uid, 1000);
    assert_eq!(lookup(&mut eng, vid, "f19").uid, 1019);

    // 错密钥：永远连不上，但 tick 不是致命的，退避重试也不会把循环钉死。
    let (_tmp2, dir2) = read_only_dir("rep-auth-bad");
    let mut eng2 = replica(&dir2);
    let mut bad = fcfg(&srv.addr, 1, 1 << 20);
    bad.auth = Some(Arc::new(Key([0x9cu8; 32])));
    let mut f2 = Follower::new(bad);
    for _ in 0..40 {
        f2.tick(&mut eng2)
            .expect("错密钥不该把追平循环做成致命错误");
        std::thread::sleep(Duration::from_millis(3));
    }
    let p2 = f2.progress(&mut eng2);
    assert!(!p2.connected, "错密钥不算连上：{p2:?}");
    assert!(p2.errors > 0, "失败必须能在计数里看见：{p2:?}");
    assert_eq!(p2.applied, 0, "一个字节也不该被 apply");
    // 引擎仍然是只读副本身份：拒写靠的是 `replica` 旗标，不是“有没有连上主”。
    match ok(&mut eng2, &Op::ListViews) {
        Reply::Views(_) => {}
        other => panic!("副本仍该答读：{other:?}"),
    }
}

/// 主被 kill -9 之后，副本不退出、不报错，且继续用已有状态答读。
///
/// 这是"只读副本"最值钱的一刻，也是它最容易被做坏的一刻：把追平失败做成
/// 致命错误，卖点就变成反向卖点。
#[test]
fn the_replica_keeps_answering_reads_after_the_primary_dies() {
    let srv = Server::start("group");
    let vid = seed_primary(&srv, "rdying", 12);

    let (_tmp, dir) = read_only_dir("rep");
    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, 1 << 20));
    converge(&mut f, &mut eng, &srv.log_text());
    let before = lookup(&mut eng, vid, "f5").ino;

    // SIGKILL：不给主任何优雅退出的机会，副本连"主停了"这件事都不该知道。
    let (_tmp2, _pdir) = srv.sigkill_and_take_dir();
    let mut errs = 0;
    for _ in 0..8 {
        // tick 必须一直返回 Ok：连不上主是可恢复状态。
        f.tick(&mut eng).expect("主死了 tick 也不该致命");
        let p = f.progress(&mut eng);
        errs = p.errors;
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(errs > 0, "连不上主要能被 `replica_errors` 看见，got {errs}");
    assert_eq!(f.progress(&mut eng).connected, false, "断线后不该谎报已连接");

    // 已有的状态照旧可答读，且就是主死前那一份。
    assert_eq!(lookup(&mut eng, vid, "f5").ino, before);
    for i in 0..12u32 {
        assert_eq!(lookup(&mut eng, vid, &format!("f{i}")).uid, 1000 + i);
    }
    // lag 停在最后一次问到的水位上：它不会被悄悄清成 0。
    let p = f.progress(&mut eng);
    assert!(p.primary_seq >= 13, "主的水位该留着，got {}", p.primary_seq);
}

/// 主 checkpoint 把日志截掉之后，副本要能发现 gap 并整份重装快照。
///
/// 这一条同时是 `SnapFetch` 的分块用例：把 `max_bytes` 压到 4 KiB，
/// 快照必须多块才下得完。
#[test]
fn a_gap_reinstalls_the_snapshot_in_chunks() {
    let srv = Server::start("group");
    let vid = seed_primary(&srv, "rgap", 300);
    okc(&mut srv.client(), &Op::Checkpoint);
    let snap_len = match okc(&mut srv.client(), &Op::Stats) {
        Reply::Stats(s) => s.snap_bytes,
        other => panic!("expected Stats, got {other:?}"),
    };
    assert!(
        snap_len > 8192,
        "这条用例要测多块下载，快照至少得跨过两块：got {snap_len} B"
    );

    // 副本是全新目录：want_seq = 1 <= 主的 snap_seq，第一问就是 gap。
    let (_tmp, dir) = read_only_dir("rep");
    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, 4096));
    let mut chunks = 0usize;
    let deadline = Instant::now() + Duration::from_secs(20);
    let p = loop {
        std::thread::sleep(Duration::from_millis(3));
        f.tick(&mut eng).expect("tick must not be fatal");
        let p = f.progress(&mut eng);
        if p.downloading {
            chunks += 1;
        }
        if p.primary_seq > 0 && p.lag == 0 && !p.downloading {
            break p;
        }
        assert!(Instant::now() < deadline, "never converged: {p:?}");
    };
    assert!(chunks >= 2, "4 KiB 的块下 {snap_len} B 的快照不该只跑 {chunks} 块");
    assert!(p.resyncs >= 1, "gap 必须记进 `replica_resyncs`，got {}", p.resyncs);
    // gap 不是错误：两个数混在一起，运维就分不出哪一种该报警。
    assert_eq!(p.errors, 0, "gap 不该抬 `replica_errors`");
    // 装的必须是主那一代：快照原件落盘，所以能按字节比。
    let mine = std::fs::read(dir.join(SNAP_FILE)).expect("replica snapshot");
    let theirs = std::fs::read(srv.dir().join(SNAP_FILE)).expect("primary snapshot");
    assert_eq!(mine.len(), theirs.len(), "两份快照长度不同");
    assert_eq!(mine, theirs, "副本装的快照必须与主的逐字节相同");

    // 重装完之后仍然能继续追增量。
    let mut pc = srv.client();
    okc(
        &mut pc,
        &Op::Create {
            view: vid,
            parent: 1,
            name: b"after".to_vec(),
            mode: 0o644,
            uid: 42,
            gid: 42,
        },
    );
    converge(&mut f, &mut eng, &srv.log_text());
    assert_eq!(lookup(&mut eng, vid, "after").uid, 42);
    assert_eq!(lookup(&mut eng, vid, "f299").uid, 1299);
}

/// 副本拒写挡在引擎层，而不是只挡在网络层：内嵌用法也吃得到 EROFS。
///
/// 判据是"既非只读也非控制面"，所以 `Stats` / `Checkpoint` 仍然可用 —— 一个
/// 连自己状态都问不出来的副本没法监控。
#[test]
fn the_replica_refuses_writes_at_the_engine() {
    let (_tmp, dir) = read_only_dir("rep");
    let mut eng = replica(&dir);
    assert!(eng.is_replica());
    let seq0 = eng.apply_seq();

    // 真写：挑六个形状不同的，确认判据是“类别”而不是某个 op。
    // 最后一条是批量 op（0x007c）：它三档里落在「真写」，所以也必须被拒。
    let write_ops: Vec<Op> = vec![
        Op::CreateView { name: b"v".to_vec() },
        Op::Create {
            view: 1,
            parent: 1,
            name: b"x".to_vec(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        },
        Op::Setattr {
            view: 1,
            ino: 1,
            valid: treedb::engine::model::SA_MODE,
            mode: 0o600,
            uid: 0,
            gid: 0,
            size: 0,
            atime: 0,
            mtime: 0,
            data_ref: Vec::new(),
            ref_bytes: 0,
        },
        Op::LockSet {
            view: 1,
            ino: 1,
            owner: b"o".to_vec(),
            verb: 0,
            kind: 1,
            lease_expiry: 0,
        },
        Op::DropView { name: b"v".to_vec() },
        Op::TableClear { table: b"t".to_vec() },
    ];
    for op in &write_ops {
        let (logs, r) = eng.exec(op);
        match r {
            Reply::Err { code, msg } => {
                assert_eq!(code, errno::EROFS, "{op:?} → 期望 EROFS，got {code}");
                assert!(
                    String::from_utf8_lossy(&msg).contains("read-only replica"),
                    "{op:?} → 错误文案要说明它为何被拒"
                );
            }
            other => panic!("{op:?} → 副本上该被拒，got {other:?}"),
        }
        // 被拒的写一条日志也不能留下：否则 `EROFS` 只是“没应答”，而状态
        // 已经改了 —— 那比报错更坏。
        assert!(logs.is_empty(), "{op:?} → 被拒却产出了日志");
    }
    assert_eq!(eng.apply_seq(), seq0, "六个被拒的写不该推过水位");

    // 只读与控制面放行：一个连自己状态都问不出来的副本没法监控。
    match eng.exec(&Op::ListViews).1 {
        Reply::Views(v) => assert!(v.is_empty(), "空目录的新副本不该有视图"),
        other => panic!("ListViews 不该被拒，got {other:?}"),
    }
    match eng.exec(&Op::Stats).1 {
        Reply::Stats(s) => {
            assert!(s.replica, "Stats 必须自报副本身份");
            assert_eq!(s.replica_errors, 0);
        }
        other => panic!("Stats 不该被拒，got {other:?}"),
    }
    // `Checkpoint` 也在放行档里：副本的 WAL 只会因追平而增长，
    // 不让它自己收口就等于让它拖着一个与主同大的日志。
    match eng.exec(&Op::Checkpoint).1 {
        Reply::Ok => {}
        other => panic!("Checkpoint 不该被拒，got {other:?}"),
    }
}

/// `replica_lag` 必须真的能看见“还差多少”：一次 tick 吃不完的一批写，
/// 中途 lag > 0，追平后归零。
#[test]
fn the_lag_counter_moves_and_then_returns_to_zero() {
    let srv = Server::start("group");
    let vid = seed_primary(&srv, "rlag", 200);
    let (_tmp, dir) = read_only_dir("rep");
    let mut eng = replica(&dir);
    // 2 KiB / 轮：一批 200 多条记录绝不可能一次吃完。
    let mut f = Follower::new(fcfg(&srv.addr, 1, 2048));

    f.tick(&mut eng).expect("tick");
    let mid = f.progress(&mut eng);
    assert!(mid.applied > 0, "第一口就该吃到记录，got {:?}", mid.applied);
    assert!(
        mid.lag > 0,
        "一次没吃完时 lag 必须是正数，got {:?}（applied {}）",
        mid.lag,
        mid.applied
    );
    let p = converge(&mut f, &mut eng, &srv.log_text());
    assert_eq!(p.lag, 0, "追平后 lag 必须归零");
    assert!(lookup(&mut eng, vid, "f199").ino > 0);
}

/// 块长在追赶时自己放大，追平后退回 `--fetch-bytes`（它是下限，不是定值）。
///
/// 造一份「远大于 4 个块长」的积压 + 一个很小的块长，ramp 的触发条件与
/// 回落条件就都能在几轮里看到。三件事各自断言：途中出现过比配置值大的
/// 块长、峰值不越协议上限、收敛后回到配置值。
///
/// 用 `Op::Batch` 播种而不是逐条 `Create`：group commit 下一条串行 RPC 要
/// 等一次 fsync，几千条就耍到分钟级；批宽取协议上限 [`MAX_BATCH_OPS`]，
/// 128 次往返就能铺完。
#[test]
fn the_fetch_block_ramps_while_catching_up_and_returns_to_cfg() {
    const CFG: u32 = 32 * 1024;
    const N: u32 = 8_192;
    const PER: u32 = MAX_BATCH_OPS as u32;

    let srv = Server::start("group");
    let mut c = srv.client();
    let vid = view_id(okc(&mut c, &Op::CreateView { name: "rramp".into() }));
    for start in (0..N).step_by(PER as usize) {
        let ops = (start..start + PER)
            .map(|i| Op::Create {
                view: vid,
                parent: 1,
                name: format!("f{i}").into_bytes(),
                mode: 0o640,
                uid: 1000 + i,
                gid: 100,
            })
            .collect();
        okc(&mut c, &Op::Batch { ops });
    }

    let (_tmp, dir) = read_only_dir("rep");
    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, CFG));

    let mut max_seen = 0u32;
    let deadline = Instant::now() + Duration::from_secs(60);
    let p = loop {
        std::thread::sleep(Duration::from_millis(2));
        f.tick(&mut eng).expect("tick must never be fatal here");
        let p = f.progress(&mut eng);
        max_seen = max_seen.max(p.fetch_bytes);
        if p.primary_seq > 0 && p.lag == 0 && !p.downloading {
            break p;
        }
        assert!(
            Instant::now() < deadline,
            "replica never converged (max block {max_seen}, {p:?})\n{}",
            srv.log_text()
        );
    };

    assert!(
        max_seen > CFG,
        "积压是块长的很多倍，块长却一直停在配置值 {CFG}（峰值 {max_seen}）"
    );
    assert!(max_seen <= WAL_FETCH_MAX, "峰值 {max_seen} 越过了协议上限");
    assert_eq!(
        p.fetch_bytes, CFG,
        "追平后应当退回配置值，got {}（峰值 {max_seen}）",
        p.fetch_bytes
    );
    assert!(lookup(&mut eng, vid, &format!("f{}", N - 1)).ino > 0);
}

/// 快照下载段吃的是同一条规则。冷启动的 tick 数就等于块数（下一块之前不再等
/// 一个间隔），所以不翻倍的话 28 MB 的快照就是 28 个 tick，而传输本身在真链路上
/// 只占其中一小截。
///
/// 判据四条：下载途中出现过比配置值大的块长、峰值不越协议上限、块数明显少于
/// 「按配置值切」的块数、装完并追平后回到配置值。
#[test]
fn the_snapshot_chunk_ramps_while_downloading_and_returns_to_cfg() {
    const CFG: u32 = 8 * 1024;
    const N: u32 = 4_000;
    const PER: u32 = MAX_BATCH_OPS as u32;

    let srv = Server::start("group");
    let mut c = srv.client();
    let vid = view_id(okc(&mut c, &Op::CreateView { name: "rsnap".into() }));
    for start in (0..N).step_by(PER as usize) {
        let ops = (start..start + PER)
            .map(|i| Op::Create {
                view: vid,
                parent: 1,
                name: format!("f{i}").into_bytes(),
                mode: 0o640,
                uid: 1000 + i,
                gid: 100,
            })
            .collect();
        okc(&mut c, &Op::Batch { ops });
    }
    // 落一次快照：这一条要的是「整份下载」，不是 WAL 追赶。
    okc(&mut c, &Op::Checkpoint);
    let snap_len = match okc(&mut c, &Op::Stats) {
        Reply::Stats(s) => s.snap_bytes,
        other => panic!("expected Stats, got {other:?}"),
    };
    assert!(
        snap_len > 8 * CFG as u64,
        "快照得明显大于 8 个块长，触发条件才轮得到：got {snap_len} B"
    );

    let (_tmp, dir) = read_only_dir("rep");
    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, CFG));

    let mut max_seen = 0u32;
    let mut rounds = 0u64;
    let deadline = Instant::now() + Duration::from_secs(60);
    let p = loop {
        std::thread::sleep(Duration::from_millis(2));
        f.tick(&mut eng).expect("tick must never be fatal here");
        let p = f.progress(&mut eng);
        // 只看下载态的块长：装完之后紧接着的 WAL 追赶是另一段，不属这条用例。
        if p.downloading {
            rounds += 1;
            max_seen = max_seen.max(p.fetch_bytes);
        }
        if p.primary_seq > 0 && p.lag == 0 && !p.downloading {
            break p;
        }
        assert!(
            Instant::now() < deadline,
            "replica never converged (block {max_seen}, {rounds} rounds, {p:?})\n{}",
            srv.log_text()
        );
    };

    assert!(
        max_seen > CFG,
        "快照没下完还剩很多，块长却一直停在配置值 {CFG}（峰值 {max_seen}）"
    );
    assert!(max_seen <= WAL_FETCH_MAX, "峰值 {max_seen} 越过了协议上限");
    let fixed_rounds = snap_len.div_ceil(CFG as u64);
    assert!(
        rounds < fixed_rounds,
        "快照 {snap_len} B / 块长 {CFG} 该比按块长切少：{rounds} vs {fixed_rounds} 轮"
    );
    assert_eq!(
        p.fetch_bytes, CFG,
        "追平后应当退回配置值，got {}（峰值 {max_seen}）",
        p.fetch_bytes
    );
    assert!(lookup(&mut eng, vid, &format!("f{}", N - 1)).ino > 0);
}

/// 换代时已下的前缀能不能留着：`SnapFetch { len: 0 }` 是探针，回 `SnapPrefix`。
///
/// 判据只能是对内容说的：`prefix_crc` 必须等于快照文件前 `min(offset,total)`
/// 字节的 crc32，与副本自己算的那个逐位相同。越界（`offset >= total`）不是错，
/// 截到整份为止。
#[test]
fn a_snapshot_probe_reports_the_prefix_crc() {
    let srv = Server::start("group");
    seed_primary(&srv, "rprobe", 200);
    let mut c = srv.client();
    okc(&mut c, &Op::Checkpoint);
    let raw = std::fs::read(srv.dir().join(SNAP_FILE)).expect("primary snapshot");
    let total = raw.len() as u64;

    let half = total / 2;
    let beyond = total + 4096;
    for off in [0u64, 1, 4096, half, total, beyond] {
        match okc(&mut c, &Op::SnapFetch { offset: off, len: 0 }) {
            Reply::SnapPrefix {
                prefix_crc, total: t, ..
            } => {
                assert_eq!(t, total, "offset {off}: total 必须报整份长度");
                let n = (off as usize).min(raw.len());
                assert_eq!(
                    prefix_crc,
                    crc32(&raw[..n]),
                    "offset {off}: 前缀 crc 必须覆盖前 {n} B"
                );
            }
            other => panic!("len == 0 应回 SnapPrefix，得到 {other:?}"),
        }
    }
    // 同一个偏移的普通取块不变：探针只是 `len == 0` 这一档的意思变了。
    match okc(&mut c, &Op::SnapFetch { offset: half, len: 4096 }) {
        Reply::SnapChunk { bytes, total: t, .. } => {
            assert_eq!(t, total);
            assert_eq!(bytes, &raw[half as usize..half as usize + bytes.len()]);
        }
        other => panic!("len > 0 应回 SnapChunk，得到 {other:?}"),
    }
}

/// 换代时改动落在正文的哪一段。
#[derive(Clone, Copy, PartialEq)]
enum Change {
    /// 通用表的一行（快照编码的最后一段）：前缀整体可复用，探针一次就够。
    Tail,
    /// 中段某个节点：前半段逐字节相同、后半段分叉 —— 只有二分能留下前半段。
    Mid,
    /// 新增一个文件：`nodes` 条数变了，正文从很前面就分叉。
    Head,
}

/// 下载途中主换代：已下的前缀能留多长，由两份快照的实际公共前缀说了算。
///
/// 这条把改动钉在「主在下载期间又 checkpoint 了一次」那条路上：节点按
/// `(view, ino)` 排，`ino` 是入库顺序，所以改最后一个节点只动正文尾部、改中段
/// 节点只动中段，而新建文件会改 `nodes` 条数、从很前面就分叉。
///
/// 返回（进度, 换代前的快照, 副本拼出来的快照, 主换代后的快照）。
fn generation_change_mid_download(change: Change) -> (FollowerProgress, Vec<u8>, Vec<u8>, Vec<u8>) {
    const N: u32 = 4_000;
    const PER: u32 = MAX_BATCH_OPS as u32;

    let srv = Server::start("group");
    let mut c = srv.client();
    let vid = view_id(okc(
        &mut c,
        &Op::CreateView {
            name: match change {
                Change::Head => "rdrop",
                _ => "rkeep",
            }
            .into(),
        },
    ));
    for start in (0..N).step_by(PER as usize) {
        let ops = (start..start + PER)
            .map(|i| Op::Create {
                view: vid,
                parent: 1,
                name: format!("f{i}").into_bytes(),
                mode: 0o640,
                uid: 1000 + i,
                gid: 100,
            })
            .collect();
        okc(&mut c, &Op::Batch { ops });
    }
    // 一张通用表 + 一行：它在快照编码的最后一段（tables/rows 在 nodes 与
    // dentries 之后），所以「改这一行」是真正的尾部改动，与中段改节点分开。
    okc(
        &mut c,
        &Op::TableCreate {
            def: TableDef {
                name: b"tailtbl".to_vec(),
                columns: vec![ColumnDef {
                    name: b"v".to_vec(),
                    flags: 0,
                }],
            },
        },
    );
    okc(&mut c, &Op::RowPut {
        table: b"tailtbl".to_vec(),
        key: b"k".to_vec(),
        cols: vec![b"x".to_vec()],
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    });
    okc(&mut c, &Op::Checkpoint);
    let snap_len = match okc(&mut c, &Op::Stats) {
        Reply::Stats(s) => s.snap_bytes,
        other => panic!("expected Stats, got {other:?}"),
    };
    assert!(
        snap_len > 64 * 1024,
        "这条用例要在一份大快照中途换代：got {snap_len} B"
    );
    let old = std::fs::read(srv.dir().join(SNAP_FILE)).expect("primary snapshot (generation 0)");

    // 块长取整份的 1/4：`next_fetch_bytes` 只在「落后超过 4 个块」时才翻倍，
    // 于是这条用例固定走 4 个块、块长不变 —— 第 3 块结束时正好下了 3/4，
    // 中段的分叉点一定落在已经下过的范围里，而且还有一块没下完（不会刚好装完）。
    let cfg_bytes = ((snap_len / 4) as u32).clamp(1, 1 << 20);
    let (_tmp, dir) = read_only_dir("rep");
    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, cfg_bytes));

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        std::thread::sleep(Duration::from_millis(2));
        f.tick(&mut eng).expect("tick must not be fatal");
        let p = f.progress(&mut eng);
        if p.snap_off > snap_len * 2 / 3 {
            assert!(p.downloading, "下载提前装完了，没赶上换代：{p:?}");
            break;
        }
        assert!(Instant::now() < deadline, "下载没有推进：{p:?}");
    }

    match change {
        Change::Tail => {
            // 改写那一行的值：它在快照最后一段，前面一个字节都不动。
            okc(&mut c, &Op::RowPut {
                table: b"tailtbl".to_vec(),
                key: b"k".to_vec(),
                cols: vec![b"y".to_vec()],
                flags: 0,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            });
        }
        Change::Mid => {
            // 改中段那个节点：它前面那一截在快照里逐字节不动，后面也一样 ——
            // 整段 crc 必然对不上，但公共前缀停在这条记录上。
            let mid = node(okc(
                &mut c,
                &Op::Lookup {
                    view: vid,
                    parent: 1,
                    name: format!("f{}", N / 2).into(),
                },
            ))
            .ino;
            set_mode(&mut c, vid, mid);
        }
        Change::Head => {
            // 再加一个文件：`nodes` 的条数变了，正文从很前面就分叉。
            okc(
                &mut c,
                &Op::Create {
                    view: vid,
                    parent: 1,
                    name: b"zzz".to_vec(),
                    mode: 0o644,
                    uid: 7,
                    gid: 7,
                },
            );
        }
    }
    okc(&mut c, &Op::Checkpoint);

    let p = converge(&mut f, &mut eng, &srv.log_text());
    let mine = std::fs::read(dir.join(SNAP_FILE)).expect("replica snapshot");
    let theirs = std::fs::read(srv.dir().join(SNAP_FILE)).expect("primary snapshot");
    (p, old, mine, theirs)
}

fn set_mode(c: &mut Client, vid: u32, ino: u64) {
    okc(
        c,
        &Op::Setattr {
            view: vid,
            ino,
            valid: SA_MODE,
            mode: 0o600,
            uid: 0,
            gid: 0,
            size: 0,
            atime: 0,
            mtime: 0,
            data_ref: Vec::new(),
            ref_bytes: 0,
        },
    );
}

/// 两份快照第一个不同的字节（没有不同就是较短那份的长度）。
fn first_diff(a: &[u8], b: &[u8]) -> u64 {
    let n = a.len().min(b.len());
    let mut i = 0;
    while i < n && a[i] == b[i] {
        i += 1;
    }
    i as u64
}

/// 换代的改动落在正文末尾：已下的前缀逐字节相同，crc 对得上，一次探针就接着下。
#[test]
fn a_snapshot_generation_change_keeps_the_downloaded_prefix() {
    let (p, old, mine, theirs) = generation_change_mid_download(Change::Tail);
    let d = first_diff(&old, &theirs);
    assert!(
        d > old.len() as u64 * 3 / 4,
        "这条用例要尾部改动：d={d}，len={}",
        old.len()
    );
    assert!(
        p.snap_reused > 0,
        "换代之后一格前缀都没留下（restarts {}）",
        p.snap_restarts
    );
    assert!(
        p.snap_reused <= d,
        "留下的 {} B 越过了分叉点 {d}",
        p.snap_reused
    );
    assert_eq!(p.snap_restarts, 0, "可复用的前缀不该被判成整份重下");
    assert_eq!(p.snap_probes, 1, "整段对得上就不该再二分");
    assert_eq!(p.resyncs, 1, "换代不是新的一次重装");
    assert_eq!(mine, theirs, "拼出来的快照必须与主的逐字节相同");
}

/// 中段分叉：整段 crc 对不上，但前半段逐字节相同 —— 二分把前半段留下。
///
/// 断言不猜分叉点：从两份快照直接找第一个不同的字节，`snap_reused` 必须落在它
/// 之前（安全：绝不保留一个不同的字节）且离它不远（有效：二分确实把边界收到了
/// 分叉点附近，而不是退回 0）。
#[test]
fn a_snapshot_generation_change_keeps_the_common_prefix_mid_body() {
    let (p, old, mine, theirs) = generation_change_mid_download(Change::Mid);
    assert_eq!(mine, theirs, "拼出来的快照必须与主的逐字节相同");
    let d = first_diff(&old, &theirs);
    assert!(
        d > old.len() as u64 / 4,
        "这条用例要中段分叉：d={d}，len={}",
        old.len()
    );
    assert!(
        p.snap_reused <= d,
        "留下的 {} B 越过了分叉点 {d}",
        p.snap_reused
    );
    assert!(
        p.snap_reused * 4 >= d * 3,
        "二分没把边界收近：留了 {} B，分叉点在 {d}（probes {}）",
        p.snap_reused,
        p.snap_probes
    );
    assert_eq!(p.snap_restarts, 0, "留下了前缀就不算整份重下");
    assert!(
        p.snap_probes > 1 && p.snap_probes <= 1 + PREFIX_PROBE_BUDGET as u64,
        "探针次数越界：{}",
        p.snap_probes
    );
    assert_eq!(p.resyncs, 1, "换代不是新的一次重装");
}

/// 反面：换代改动落在正文开头（`nodes` 条数变了），公共前缀只有开头那一小截。
///
/// 二分在这里只该留下那个真实的公共头，且绝不允许越过分叉点 —— 宁可重下，
/// 也不拼一份谁都不认识的快照。
#[test]
fn a_snapshot_generation_change_with_a_different_prefix_keeps_only_the_head() {
    let (p, old, mine, theirs) = generation_change_mid_download(Change::Head);
    assert_eq!(mine, theirs, "拼出来的快照必须与主的逐字节相同");
    let d = first_diff(&old, &theirs);
    assert!(
        p.snap_reused <= d,
        "留下的 {} B 越过了分叉点 {d}",
        p.snap_reused
    );
    assert!(
        p.snap_reused < old.len() as u64 / 8,
        "正文从很前面就分叉，不该留下 {} B（整份 {} B）",
        p.snap_reused,
        old.len()
    );
    assert!(
        p.snap_probes <= 1 + PREFIX_PROBE_BUDGET as u64,
        "探针次数越界：{}",
        p.snap_probes
    );
    assert_eq!(p.resyncs, 1, "重下不算新的一次重装");
}

/// 端到端：`treedb-standby` 进程 + 真客户端。线协议上的 `EROFS` 与
/// `Stats` 里那组新字段的真实编解码只在这一组里被覆盖。
///
/// 两档事件循环跑的是**同一份断言**：副本的 tick 在单线程循环里住在轮末，
/// 在多 worker 循环里住在提交线程里 —— 只测一档等于把另一档当成不存在的代码。
fn standby_over_the_wire(workers: usize) {
    let srv = Server::start("group");
    let vid = seed_primary(&srv, &format!("rbinary{workers}"), 25);

    let tmp = tempfile::tempdir().expect("tempdir");
    let addr = free_addr();
    let log = tmp.path().join("standby.log");
    let sb = Standby::spawn(&srv.addr, &tmp.path().join("rep"), &addr, &log, workers);
    let mut sc = sb.client();

    // 写在线上一被拒：错误码来自引擎，但必须原样穿过帧。
    match sc
        .call(&Op::Create {
            view: vid,
            parent: 1,
            name: b"nope".to_vec(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        })
        .expect("call")
    {
        Reply::Err { code, msg } => {
            assert_eq!(code, errno::EROFS, "副本上的写必须 EROFS");
            assert!(
                String::from_utf8_lossy(&msg).contains("read-only replica"),
                "msg = {}",
                String::from_utf8_lossy(&msg)
            );
        }
        other => panic!("expected EROFS Err, got {other:?}"),
    }
    // 控制面在副本上仍然可用，否则没法监控它。
    match okc(&mut sc, &Op::Checkpoint) {
        Reply::Ok => {}
        Reply::Err { code, msg } if code == errno::EINVAL => {
            panic!("Checkpoint 不该被拒：{}", String::from_utf8_lossy(&msg))
        }
        r => {
            // `Checkpoint` 的应答类型不在 `Ok` 时也不算失败（不同版本可能带水位），
            // 但被 EROFS 拒掉就是回归。
            assert!(!matches!(r, Reply::Err { code, .. } if code == errno::EROFS));
        }
    }

    // 读要等到追平才看得见；这一步同时验证 Stats 的新字段能过线。
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut seen = None;
    loop {
        match okc(&mut sc, &Op::Stats) {
            Reply::Stats(s) => {
                assert!(s.replica, "standby 必须以副本身份报自己");
                if s.replica_primary_seq > 0 && s.replica_lag == 0 {
                    seen = Some((s.replica_applied, s.replica_resyncs));
                    break;
                }
            }
            other => panic!("expected Stats, got {other:?}"),
        }
        if Instant::now() > deadline {
            panic!("standby never caught up:\n{}", sb.log_text());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let (applied, _resyncs) = seen.expect("loop above breaks only after setting it");
    assert!(applied >= 26, "26 个写 op 该至少传 26 条记录，got {applied}");

    let n = match okc(
        &mut sc,
        &Op::Lookup {
            view: vid,
            parent: 1,
            name: b"f24".to_vec(),
        },
    ) {
        Reply::Node(n) => n,
        other => panic!("expected Node, got {other:?}"),
    };
    assert_eq!(n.uid, 1024);
    let mut pc = srv.client();
    let pn = match okc(
        &mut pc,
        &Op::Lookup {
            view: vid,
            parent: 1,
            name: b"f24".to_vec(),
        },
    ) {
        Reply::Node(n) => n,
        other => panic!("expected Node, got {other:?}"),
    };
    assert_eq!(n.ino, pn.ino, "两端读到的必须是同一个对象");

    // 主继续写，副本继续追：这条进程路径不是一次性的。
    okc(
        &mut pc,
        &Op::Create {
            view: vid,
            parent: 1,
            name: b"live".to_vec(),
            mode: 0o600,
            uid: 9,
            gid: 9,
        },
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        // 这里不能拿 `okc`：等追平的轮询里 ENOENT 是正常分支，而 `okc` 会把它
        // 当成失败 panic 掉。
        match sc
            .call(&Op::Lookup {
                view: vid,
                parent: 1,
                name: b"live".to_vec(),
            })
            .expect("call")
        {
            Reply::Node(n) => {
                assert_eq!(n.uid, 9, "追平回来的必须是主上那一条");
                break;
            }
            // ENOENT = 还没追平；其他错误码就是真坏了。
            Reply::Err { code: errno::ENOENT, .. } => {}
            Reply::Err { code, msg } => {
                panic!("live lookup → errno {code} ({})", String::from_utf8_lossy(&msg))
            }
            other => panic!("expected Node, got {other:?}"),
        }
        assert!(Instant::now() < deadline, "`live` never showed up:\n{}", sb.log_text());
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn the_standby_binary_serves_reads_on_the_single_thread_loop() {
    standby_over_the_wire(1);
}

#[test]
fn the_standby_binary_serves_reads_on_the_multi_worker_loop() {
    standby_over_the_wire(2);
}

/// 副本重启后靠自己的日志接上，不必重新拉一份快照。
///
/// 这是“副本自带 WAL”那个决定的全部回报：没它的话，每次重启都要先过一遍
/// 全量下载，而重启恰恰是副本最频繁的事件。
#[test]
fn a_replica_restart_resumes_from_its_own_log() {
    let srv = Server::start("group");
    let vid = seed_primary(&srv, "rrestart", 30);
    let (_tmp, dir) = read_only_dir("rep");

    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, 1 << 20));
    converge(&mut f, &mut eng, &srv.log_text());
    // 让副本自己也 checkpoint 一次：重启要走的恢复路径是“快照 + 日志尾巴”，
    // 不是一个空快照目录。主那边不 checkpoint，所以本地水位一直接得上。
    eng.checkpoint().expect("replica checkpoint");
    let snap_before = std::fs::metadata(dir.join(SNAP_FILE)).expect("replica snapshot").len();
    assert!(snap_before > 0, "checkpoint 之后本地快照必须存在");
    // 尾巴：checkpoint 之后主又写了一批，这些只在 WAL 里。
    let mut pc = srv.client();
    for i in 0..8u32 {
        okc(
            &mut pc,
            &Op::Create {
                view: vid,
                parent: 1,
                name: format!("t{i}").into_bytes(),
                mode: 0o644,
                uid: 3,
                gid: 3,
            },
        );
    }
    converge(&mut f, &mut eng, &srv.log_text());
    let ino_before = lookup(&mut eng, vid, "f7").ino;
    drop(f);
    drop(eng);

    // 重启：同一个目录，一个新的 `Follower`（进度计数也随之归零）。
    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, 1 << 20));
    let p = converge(&mut f, &mut eng, &srv.log_text());
    assert_eq!(p.resyncs, 0, "本地日志能接上号，不该走 gap 重装：{p:?}");
    assert_eq!(p.errors, 0, "重启后的第一轮拉取不该失败：{p:?}");
    assert_eq!(lookup(&mut eng, vid, "f7").ino, ino_before, "重放必须还原到同一个对象");
    for i in 0..8u32 {
        assert_eq!(lookup(&mut eng, vid, &format!("t{i}")).uid, 3, "尾巴上的记录也要在");
    }
    // 快照文件还在，且就是上次那一份：重启没引发重下。
    let snap_after = std::fs::metadata(dir.join(SNAP_FILE)).expect("replica snapshot").len();
    assert_eq!(snap_after, snap_before, "重启不该换掉本地快照那一代");

    // 主上的新写仍然追得到：水位接得上，不是停在重启那一刻。
    okc(
        &mut pc,
        &Op::Create {
            view: vid,
            parent: 1,
            name: b"post".to_vec(),
            mode: 0o644,
            uid: 5,
            gid: 5,
        },
    );
    let p = converge(&mut f, &mut eng, &srv.log_text());
    assert_eq!(p.resyncs, 0, "增量追平不该抬 resyncs");
    assert_eq!(lookup(&mut eng, vid, "post").uid, 5);
}

/// 启动参数的两道门：没有主不启动，`--primary == --listen` 不启动。
/// 都是退出码 2，且都不该把监听口开起来。
#[test]
fn the_standby_binary_refuses_a_nonsense_configuration() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("rep");
    let addr = free_addr();

    // 没给 --primary
    let st = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
        .args(["--listen", &addr, "--dir"])
        .arg(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn");
    assert_eq!(st.code(), Some(2), "缺 --primary 必须以用法错误退出");

    // 自己拉自己：会在第一次 gap 上死循环，表现为"lag 不动"
    let st = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
        .args(["--primary", &addr, "--listen", &addr, "--dir"])
        .arg(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn");
    assert_eq!(st.code(), Some(2), "--primary == --listen 必须被拒");
    assert!(
        !Client::ping(&addr),
        "被拒之后不该有任何东西占着 {addr}"
    );

    // fetch-bytes 超上限：主会回 EINVAL，副本没必要先问一遍才知道
    let st = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
        .args([
            "--primary", "127.0.0.1:1",
            "--listen", &addr,
            "--dir", &dir.to_string_lossy(),
            "--fetch-bytes", "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn");
    assert_eq!(st.code(), Some(2), "--fetch-bytes 0 必须被 check_cfg 挡下");
}

// ------------------------------------------------------------------ 子进程

/// 一个 `treedb-standby` 子进程。Drop 时确保回收，不留占端口的孤儿。
struct Standby {
    child: Option<Child>,
    addr: String,
    log: PathBuf,
    pid: u32,
}

impl Standby {
    fn spawn(primary: &str, dir: &Path, addr: &str, log: &Path, workers: usize) -> Standby {
        let out = Stdio::from(std::fs::File::create(log).expect("create standby log"));
        let err = Stdio::from(
            std::fs::OpenOptions::new()
                .append(true)
                .open(log)
                .expect("open standby log"),
        );
        let child = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
            .arg("--primary")
            .arg(primary)
            .arg("--dir")
            .arg(dir)
            .arg("--listen")
            .arg(addr)
            .arg("--interval-ms")
            .arg("2")
            .arg("--workers")
            .arg(workers.to_string())
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .spawn()
            .expect("spawn treedb-standby");
        let pid = child.id();
        let sb = Standby {
            child: Some(child),
            addr: addr.to_string(),
            log: log.to_path_buf(),
            pid,
        };
        let deadline = Instant::now() + Duration::from_secs(15);
        while !Client::ping(&sb.addr) {
            if Instant::now() > deadline {
                panic!("standby never became ready:\n{}", sb.log_text());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        sb
    }

    fn client(&self) -> Client {
        Client::connect(&self.addr, 0).expect("connect standby")
    }

    fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// 走真实 `Op::Shutdown`，让副本先 checkpoint 再释放目录锁。
    fn graceful_shutdown(mut self) -> std::process::ExitStatus {
        {
            let mut c = self.client();
            let _ = c.call(&Op::Shutdown);
        }
        let mut child = self.child.take().expect("child already reaped");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match child.try_wait().expect("try_wait standby") {
                Some(st) => {
                    assert!(st.success(), "standby exited {st:?}:\n{}", self.log_text());
                    return st;
                }
                None if Instant::now() > deadline => {
                    panic!("standby did not exit on Shutdown:\n{}", self.log_text())
                }
                None => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }
}

impl Drop for Standby {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

// ---------------------------------------------- fencing token（§6.6 第 3 条）

/// `submit` 版的成功断言：写 op 会产出日志，不能用只读那个 `ok`。
fn apply_ok(e: &mut Engine, op: &Op) -> Reply {
    match e.submit(op) {
        Ok(Reply::Err { code, msg }) => {
            panic!("{op:?} → errno {code} ({})", String::from_utf8_lossy(&msg))
        }
        Ok(r) => r,
        Err(e) => panic!("{op:?} → {e}"),
    }
}

/// 造一份可装的快照：一个视图 + `files` 个文件，checkpoint 后返回字节与视图 id。
///
/// 直接读 `state.snap` 而不是重新编码：副本真正收到的是主的文件原件，
/// 测试也该拿同一份字节，否则 `install_snapshot` 的 crc 尾巴校验就绕过去了。
fn snapshot_of(tag: &str, view: &str, files: u32) -> (TempDir, Vec<u8>, u32) {
    let (tmp, dir) = read_only_dir(tag);
    let (bytes, vid) = {
        // 必须用 Always：`Never` 走 `append_nodur`，`durable_seq()` 一直是 0，
        // checkpoint 于是写出一份水位为 0 的快照 —— 测不到「旧/新」这个维度。
        let mut e = Engine::open(&dir, Durability::Always).expect("open snapshot source");
        let vid = view_id(apply_ok(&mut e, &Op::CreateView { name: view.into() }));
        for i in 0..files {
            apply_ok(
                &mut e,
                &Op::Create {
                    view: vid,
                    parent: 1,
                    name: format!("f{i}").into_bytes(),
                    mode: 0o644,
                    uid: 1,
                    gid: 1,
                },
            );
        }
        e.checkpoint().expect("checkpoint");
        let bytes = std::fs::read(dir.join(SNAP_FILE)).expect("read state.snap");
        (bytes, vid)
    };
    (tmp, bytes, vid)
}

/// 令牌只升不降、跟着目录走、坏文件不许读成 0。
///
/// 三件事各自护着一个口子：只升不降防“提过权的目录又被旧主驱动”；落盘防
/// “崩溃后回到旧令牌”；坏文件报错防“一个坏字节就把 fencing 解除了”。
#[test]
fn the_fence_is_monotonic_persisted_and_never_defaults_to_zero() {
    let (_tmp, dir) = read_only_dir("fence-mono");
    {
        let mut e = Engine::open(&dir, Durability::Never).expect("open");
        assert_eq!(e.fence(), 0, "新目录从未被提升过，令牌是 0");
        assert_eq!(e.promote().expect("promote"), 1);
        assert_eq!(e.promote().expect("promote"), 2);
        assert_eq!(e.raise_fence(7).expect("raise"), 7);
        assert_eq!(e.raise_fence(3).expect("raise"), 7, "降令牌必须被忽略");
        assert_eq!(e.fence(), 7);
    }
    let path = dir.join(FENCE_FILE);
    assert_eq!(
        std::fs::read_to_string(&path).expect("fence file").trim(),
        "7",
        "令牌要落成一个人眼能 cat 的十进制"
    );
    {
        let e = Engine::open(&dir, Durability::Never).expect("reopen");
        assert_eq!(e.fence(), 7, "重启后令牌必须还在");
    }
    std::fs::write(&path, b"seven\n").expect("write");
    match Engine::open(&dir, Durability::Never) {
        Err(Error::Config(m)) => assert!(m.contains("fencing token"), "{m}"),
        Err(e) => panic!("坏令牌应是 Config 错，得到 {e}"),
        Ok(_) => panic!("坏令牌必须拒绝启动：读成 0 等于把 fencing 默默解除"),
    }
}

/// 副本侧的闸门：更大的令牌先落盘再吃数据，更小的令牌一个字都不许进。
///
/// 四档：
/// ① 令牌 1 的快照 → 接受，令牌落盘为 1；
/// ② 令牌 5 的**旧**快照 → 接受（权力优先于新旧，允许 seq 回退）；
/// ③ 同一令牌下的旧快照 → 拒收（这才是真的回滚）；
/// ④ 令牌 0 的批 → 拒收，令牌不上抬，状态一个字节不动。
#[test]
fn a_replica_obeys_the_fence_on_snapshots_and_batches() {
    let (_tmp_a, snap_big, vid) = snapshot_of("fence-src-a", "a", 3);
    let (_tmp_b, snap_small, _) = snapshot_of("fence-src-b", "b", 0);

    let (_rtmp, rdir) = read_only_dir("fence-rep");
    let mut r = replica(&rdir);
    assert_eq!(r.fence(), 0);

    // ① 令牌更大：接受，并且**先**把令牌落到盘上。
    let seq_big = r.install_snapshot(&snap_big, 1).expect("install at epoch 1");
    assert_eq!(r.fence(), 1);
    assert_eq!(
        std::fs::read_to_string(rdir.join(FENCE_FILE)).unwrap().trim(),
        "1",
        "吃掉数据之前令牌必须已经落地（否则崩一下就回到旧令牌了）"
    );
    lookup(&mut r, vid, "f0");

    // ② 令牌 5 > 本地 1：哪怕快照更旧也接受 —— 新主可能从别处提权，
    //    它的数据就是权威。这是“权力优先于新旧”这条例外的唯一形态。
    let seq_small = r
        .install_snapshot(&snap_small, 5)
        .expect("a higher token outranks a newer snapshot");
    assert!(
        seq_small < seq_big,
        "这一档要真的回退水位，否则测不到例外（big {seq_big} / small {seq_small}）"
    );
    assert_eq!(r.fence(), 5);
    match r.exec(&Op::Lookup {
        view: vid,
        parent: 1,
        name: b"f0".to_vec(),
    }) {
        (_, Reply::Err { code: errno::ENOENT, .. }) => {}
        (_, other) => panic!("回退后的状态应该是旧那份（没有 f0），得到 {other:?}"),
    }

    // ③ 同一令牌下回退：拒绝。
    let (_rtmp2, rdir2) = read_only_dir("fence-rep2");
    let mut r2 = replica(&rdir2);
    assert_eq!(r2.install_snapshot(&snap_big, 5).expect("install"), seq_big);
    match r2.install_snapshot(&snap_small, 5) {
        Err(Error::Io(m)) => assert!(m.contains("rewind"), "{m}"),
        other => panic!("同令牌下的旧快照必须被拒，得到 {other:?}"),
    }

    // ④ 令牌 0 的批：拒收，且不碰令牌、不碰状态。
    let before = r.state_snapshot();
    match r.replicate_records(1, &[b"whatever".to_vec()], 0) {
        Err(Error::Fence(_)) => {}
        other => panic!("旧令牌的批必须被拒，得到 {other:?}"),
    }
    assert_eq!(r.fence(), 5, "被拒的批不该把令牌改小或改大");
    assert_eq!(r.state_snapshot(), before, "被拒的批一个字节都不许进");
}

/// 提权之后，仍在追旧主的副本拒收它的批：不退出、不推进、错误计数抬起来。
///
/// 这是 fencing 在真实链路上的样子 —— 它不阻止旧主继续对**自己的**客户端
/// 服务（那是停掉旧主的运维动作），只保证**副本**不会把旧主的新写当成真相。
#[test]
fn a_promoted_replica_refuses_its_old_primary_over_the_wire() {
    let srv = Server::start("group");
    let vid = seed_primary(&srv, "fenced", 5);

    let (_tmp, dir) = read_only_dir("fenced-rep");
    let mut eng = replica(&dir);
    let mut f = Follower::new(fcfg(&srv.addr, 1, 1 << 20));
    converge(&mut f, &mut eng, &srv.log_text());
    assert_eq!(eng.fence(), 0, "追平本身不碰令牌");

    // 提权：这份目录成了更高的那一任。
    assert_eq!(eng.promote().expect("promote"), 1);

    // 旧主不知道已被换掉，照旧接写。
    let mut pc = srv.client();
    okc(
        &mut pc,
        &Op::Create {
            view: vid,
            parent: 1,
            name: b"ghost".to_vec(),
            mode: 0o644,
            uid: 9,
            gid: 9,
        },
    );

    let before = eng.state_snapshot();
    for _ in 0..30 {
        std::thread::sleep(Duration::from_millis(5));
        f.tick(&mut eng)
            .expect("fencing 不是致命错误：退出会让这份副本比旧主更不可用");
    }
    let p = f.progress(&mut eng);
    assert!(
        p.errors >= 1,
        "拒收必须体现在 replica_errors 上，否则监控看不出主配错了：{p:?}"
    );
    assert_eq!(eng.state_snapshot(), before, "旧主的批不许改本地状态");
    assert_eq!(eng.fence(), 1, "拒收不该把令牌再往上抬");
    // 仍在答读：这才是“不退出”那句的判据。
    assert_eq!(lookup(&mut eng, vid, "f0").uid, 1000);
}

/// `--promote` 是对目录的**离线**操作：不开端口、令牌 +1、打印下一步命令。
#[test]
fn the_promote_flag_bumps_the_fence_without_serving() {
    let (_tmp, dir) = read_only_dir("promote-cli");
    // 先当一次副本，让目录与令牌都是真实的。
    {
        let _e = replica(&dir);
    }
    let addr = free_addr();
    let out = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
        .args(["--promote", "--dir"])
        .arg(&dir)
        .args(["--listen", &addr])
        .output()
        .expect("spawn --promote");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "--promote failed: {}\n{stdout}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(stdout.contains("fence 0 -> 1"), "{stdout}");
    assert_eq!(
        std::fs::read_to_string(dir.join(FENCE_FILE)).unwrap().trim(),
        "1"
    );
    assert!(
        !Client::ping(&addr),
        "--promote 只改盘上的一个数，不该把监听口开起来"
    );

    // 再提一次：第 2 任。令牌只往前走，不能因为“提过了”就停在那里。
    let out = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
        .args(["--promote", "--dir"])
        .arg(&dir)
        .output()
        .expect("spawn --promote twice");
    assert!(out.status.success());
    assert!(String::from_utf8_lossy(&out.stdout).contains("fence 1 -> 2"));

    // 同时给 --primary 是自相矛盾的两件事：拒绝而不是默默忽略其中一个。
    let st = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
        .args(["--promote", "--primary", "127.0.0.1:1", "--dir"])
        .arg(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn");
    assert_eq!(st.code(), Some(2), "--promote 与 --primary 不能同时给");
    assert_eq!(
        std::fs::read_to_string(dir.join(FENCE_FILE)).unwrap().trim(),
        "2",
        "被拒的那一次不许改令牌"
    );
}

/// 完整切主链全部走真进程：真主 A → 真 standby B → 停 A/B → CLI promote →
/// 真新主 B → 真 standby C → 旧主 A 回魂被 C 拒收，而 C 仍答读。
///
/// 这条与脚本分工不同：脚本量吞吐并留独立日志，本条把关键顺序与失败面钉进
/// 每次 `cargo test`，防止「打印得像成功」但实际静默回滚的回归。
#[test]
fn a_real_standby_promotion_fences_a_resurrected_primary() {
    let old_primary = Server::start_workers("group", 2);
    let vid = seed_primary(&old_primary, "real-promote", 6);
    let a_before = stats(&mut old_primary.client());

    let (rep_tmp, rep_dir) = read_only_dir("real-promote-rep");
    let b_addr = free_addr();
    let b_log = rep_tmp.path().join("standby-b.log");
    let standby_b = Standby::spawn(&old_primary.addr, &rep_dir, &b_addr, &b_log, 2);
    let mut bc = standby_b.client();

    let deadline = Instant::now() + Duration::from_secs(20);
    let b_before = loop {
        let s = stats(&mut bc);
        assert!(s.replica, "standby 必须自报副本身份");
        if s.replica_primary_seq > 0
            && s.replica_lag == 0
            && s.nodes == a_before.nodes
            && s.seq == a_before.seq
        {
            break s;
        }
        assert!(Instant::now() < deadline, "B 未追平：{s:?}\n{}", standby_b.log_text());
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(fence_value(&rep_dir), 0, "追平不抬令牌");

    // 在线副本必须拒真写；失败不能改数据面水位。
    match bc.call(&Op::Create {
        view: vid,
        parent: 1,
        name: b"online-write".to_vec(),
        mode: 0o600,
        uid: 8,
        gid: 8,
    }) {
        Ok(Reply::Err { code: errno::EROFS, .. }) => {}
        other => panic!("副本在线写必须 EROFS，得到 {other:?}"),
    }
    let after_ro = stats(&mut bc);
    assert_eq!(after_ro.nodes, b_before.nodes);
    assert_eq!(after_ro.seq, b_before.seq);

    // B 活着时提升必须被目录锁挡住，并点名持锁进程。
    let locked = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
        .args(["--promote", "--dir"])
        .arg(&rep_dir)
        .output()
        .expect("spawn locked promote");
    let locked_err = String::from_utf8_lossy(&locked.stderr);
    assert_eq!(locked.status.code(), Some(1), "被锁提升应退出 1：{locked_err}");
    assert!(locked_err.contains("already locked"), "{locked_err}");
    assert!(
        locked_err.contains(&format!("pid {} holds it", standby_b.pid)),
        "{locked_err}"
    );
    assert_eq!(fence_value(&rep_dir), 0, "被锁提升不许改令牌");

    // 运维顺序：先停旧主，再停副本，最后离线提升副本目录。
    let stopped_a = old_primary.graceful_shutdown();
    assert!(stopped_a.status.success());
    let b_status = standby_b.graceful_shutdown();
    assert!(b_status.success());
    drop(bc);

    let promoted = Command::new(env!("CARGO_BIN_EXE_treedb-standby"))
        .args(["--promote", "--dir"])
        .arg(&rep_dir)
        .output()
        .expect("spawn promote");
    let promoted_out = String::from_utf8_lossy(&promoted.stdout);
    assert!(promoted.status.success(), "{}", String::from_utf8_lossy(&promoted.stderr));
    assert!(promoted_out.contains("fence 0 -> 1"), "{promoted_out}");
    assert_eq!(fence_value(&rep_dir), 1);

    // 冷拷一份已提升目录给 C；随后 B 起真服务端，初始水位必须原样保留。
    let c_dir = rep_tmp.path().join("real-promote-cold");
    copy_tree(&rep_dir, &c_dir);
    assert_eq!(fence_value(&c_dir), 1);
    let new_primary = Server::restart_at_workers(rep_tmp, rep_dir, "group", 2);
    let initial_new = stats(&mut new_primary.client());
    assert_eq!(initial_new.nodes, b_before.nodes);
    assert_eq!(initial_new.seq, b_before.seq);
    assert_eq!(fence_value(new_primary.dir()), 1);
    okc(
        &mut new_primary.client(),
        &Op::Create {
            view: vid,
            parent: 1,
            name: b"after-promote".to_vec(),
            mode: 0o600,
            uid: 11,
            gid: 11,
        },
    );
    let b_after = stats(&mut new_primary.client());
    assert!(b_after.seq > b_before.seq, "新主必须真的接收写");

    // 旧主回魂仍能对自己的客户端服务；fencing 不替代停机，也不会污染新主。
    let zombie = Server::restart_at_workers(stopped_a.tmp, stopped_a.dir, "group", 2);
    okc(
        &mut zombie.client(),
        &Op::Create {
            view: vid,
            parent: 1,
            name: b"ghost".to_vec(),
            mode: 0o644,
            uid: 9,
            gid: 9,
        },
    );
    assert!(stats(&mut zombie.client()).seq > a_before.seq);
    let untouched_new = stats(&mut new_primary.client());
    assert_eq!(untouched_new.nodes, b_after.nodes);
    assert_eq!(untouched_new.seq, b_after.seq);

    // 高令牌 C 追旧主：拒收、状态不动、错误可见，但已有数据仍可读。
    let c_addr = free_addr();
    let c_log = c_dir
        .parent()
        .expect("cold copy parent")
        .join("standby-c.log");
    let standby_c = Standby::spawn(&zombie.addr, &c_dir, &c_addr, &c_log, 2);
    let mut cc = standby_c.client();
    let deadline = Instant::now() + Duration::from_secs(20);
    let c_after = loop {
        let s = stats(&mut cc);
        if s.replica_errors > 0 && s.replica_lag > 0 {
            break s;
        }
        assert!(Instant::now() < deadline, "C 未拒旧主：{s:?}\n{}", standby_c.log_text());
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(c_after.nodes, b_before.nodes);
    assert_eq!(c_after.seq, b_before.seq);
    assert_eq!(fence_value(&c_dir), 1);
    let c_text = standby_c.log_text();
    assert!(c_text.contains("is fenced out"), "{c_text}");
    let old_file = node(okc(
        &mut cc,
        &Op::Lookup { view: vid, parent: 1, name: b"f0".to_vec() },
    ));
    assert_eq!(old_file.uid, 1000, "拒旧主后 C 必须继续答读");
    assert!(Client::ping(&c_addr), "拒旧主不能把副本进程打死");
}
