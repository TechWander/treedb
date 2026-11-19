//! 日志传送与只读副本验收（DESIGN.md §6.6）。
//!
//! 六件事各自一条：主写→副本收敛、主没了副本仍可答读、gap 触发整份重装快照、
//! 副本拒写（引擎档与线协议档各一档）、lag 计数的可观测性、`treedb-standby`
//! 这个二进制端到端跑通。
//!
//! 前五条刻意**不起副本进程**：主是真进程（走真网络、真 group commit），副本
//! 是测试进程里的 `Engine` + `Follower`。这样"追平逻辑对不对"与"两个进程怎么
//! 拼起来"分开失败。第六条才把 `treedb-standby` 拉起来，覆盖线协议上的
//! `EROFS` 与 `Stats` 里那组新字段的真实编解码。
//!
//! 跑法：`cargo test --test replication`

#[path = "common/mod.rs"]
mod common;

use common::{free_addr, Server};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use treedb::engine::core::SNAP_FILE;
use treedb::engine::model::{errno, Kind, Node, Op, Reply};
use treedb::engine::{Durability, Engine};
use treedb::net::client::Client;
use treedb::net::replicate::{Follower, FollowerProgress, ReplicaCfg};

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

    // 真写：挑五个形状不同的，确认判据是“类别”而不是某个 op。
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
    assert_eq!(eng.apply_seq(), seq0, "五个被拒的写不该推过水位");

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
        let sb = Standby {
            child: Some(child),
            addr: addr.to_string(),
            log: log.to_path_buf(),
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
}

impl Drop for Standby {
    fn drop(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}
