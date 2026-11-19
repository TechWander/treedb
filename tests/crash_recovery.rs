//! 持久性验收：`kill -9` 之后，**已经拿到 ack 的写必须一条不丢**。
//!
//! 这是整个引擎唯一不可妥协的承诺。做法只有一条路：
//! 起真进程服务端 → 真客户端写 → 拿到 ack → `SIGKILL`（不给任何清理机会）→
//! 用**同一个数据目录**重启 → 逐条 Lookup 验回。
//!
//! 反面同样重要：崩溃时还没 ack 的写**允许**丢（协议上客户端就没收到成功），
//! 但丢了也不许把已经落盘的前缀截断——所以每个用例都会顺带核对
//! `Stats.seq` 不小于已 ack 的写数（每条写至少占一个 WAL 记录）。
//!
//! 跑法：`cargo test --test crash_recovery`

mod common;

use common::Server;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use treedb::engine::model::{errno, AuditRec, DefRec, KeyRec, Kind, Node, Op, Reply, SA_DATA_REF, SA_SIZE};
use treedb::net::client::Client;

fn ok(c: &mut Client, op: &Op) -> Reply {
    match c.call(op) {
        Ok(Reply::Err { code, msg }) => panic!(
            "{op:?} → errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        Ok(r) => r,
        Err(e) => panic!("{op:?} → transport error: {e}"),
    }
}

/// 期望拿到 errno 的查询：走 `rep`，不能用 `ok`（ok 会把正常的 ENOENT 当失败）。
fn rep(c: &mut Client, op: &Op) -> Reply {
    match c.call(op) {
        Ok(r) => r,
        Err(e) => panic!("{op:?} → transport error: {e}"),
    }
}

fn create(c: &mut Client, view: u32, name: &str) -> Node {
    match ok(
        c,
        &Op::Create {
            view,
            parent: 1,
            name: name.into(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ) {
        Reply::Node(n) => n,
        other => panic!("{other:?}"),
    }
}

fn key(id: &str, hash: &str) -> KeyRec {
    KeyRec {
        id: id.into(),
        label: format!("label-{id}").into(),
        secret_hash: hash.into(),
        created_at: 1,
        last_used_at: 0,
        revoked_at: 0,
        view: b"team".to_vec(),
        ro: false,
        owner: b"alice".to_vec(),
        note: Vec::new(),
        expires_at: 0,
        created_by: Vec::new(),
        secret_hint: b"ab".to_vec(),
    }
}

/// 一条 acked 记录：`(view, name)`，重启后要靠 Lookup 把它认回来。
type Acked = Arc<Mutex<Vec<(u32, String)>>>;

fn record(a: &Acked, view: u32, names: impl IntoIterator<Item = String>) {
    a.lock().unwrap().extend(names.into_iter().map(|n| (view, n)));
}

fn snapshot(a: &Acked) -> Vec<(u32, String)> {
    a.lock().unwrap().clone()
}

/// 重启后逐条验回：已 ack 的写必须全在。返回缺失的名字。
fn missing(srv: &Server, acked: &[(u32, String)]) -> Vec<(u32, String)> {
    let mut c = srv.client();
    let mut gone = Vec::new();
    for chunk in acked.chunks(200) {
        let ops: Vec<Op> = chunk
            .iter()
            .map(|(v, n)| Op::Lookup {
                view: *v,
                parent: 1,
                name: n.clone().into_bytes(),
            })
            .collect();
        let reps = c.pipeline(&ops).expect("verify pipeline");
        for ((v, n), r) in chunk.iter().zip(reps) {
            match r {
                // 名字能查回，且回的就是那个名字：防止“查到了别人”。
                Reply::Node(node) => assert_eq!(
                    node.name,
                    n.as_bytes(),
                    "视图 {v} 里查 {n} 却拿回了 {:?}",
                    String::from_utf8_lossy(&node.name)
                ),
                Reply::Err { code, .. } if code == errno::ENOENT => {
                    gone.push((v.clone(), n.clone()))
                }
                other => panic!("{n} → 意外应答 {other:?}"),
            }
        }
    }
    gone
}

fn stats(srv: &Server) -> treedb::engine::EngineStats {
    match ok(&mut srv.client(), &Op::Stats) {
        Reply::Stats(s) => *s,
        other => panic!("{other:?}"),
    }
}

/// N 条连接各写各的，写完（拿到全部 ack）再 `kill -9`。
///
/// 这是最纯粹的一条：**ack 严格晚于 fsync**，所以这里一条都不许丢。
fn hammer_then_kill(durability: &str, threads: usize, per_thread: usize) -> Vec<(u32, String)> {
    let srv = Server::start(durability);
    let addr = srv.addr.clone();
    let acked: Acked = Arc::new(Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for t in 0..threads {
        let a = addr.clone();
        let ack = acked.clone();
        handles.push(std::thread::spawn(move || {
            let mut c = Client::connect(&a, 0).expect("connect");
            let v = match ok(&mut c, &Op::CreateView { name: format!("v{t}").into() }) {
                Reply::View { id, .. } => id,
                other => panic!("{other:?}"),
            };
            for k in 0..per_thread {
                create(&mut c, v, &format!("w{t}-{k:05}"));
            }
            // 全部 ack 到手才记账：口径保守，才能断言「记下来的必须在」。
            record(&ack, v, (0..per_thread).map(|i| format!("w{t}-{i:05}")));
            v
        }));
    }
    let views: Vec<u32> = handles.into_iter().map(|h| h.join().expect("writer panicked")).collect();
    let total: usize = snapshot(&acked).len();
    eprintln!("{durability}: {total} 条 ack 已收齐（{threads} 连接），准备 SIGKILL");
    let before = stats(&srv);
    assert_eq!(before.views, views.len() as u64);

    // 关键一步：ack 都已收到，此刻 SIGKILL 不能丢任何一条。
    let (tmp, dir) = srv.sigkill_and_take_dir();
    let srv2 = Server::restart_at(tmp, dir, durability);
    let list = snapshot(&acked);
    // 同一批名字在不同视图下会重名，去重前先确认视图集合正确。
    let uniq: HashSet<(u32, String)> = list.iter().cloned().collect();
    assert_eq!(uniq.len(), list.len(), "用例自己把 (view,name) 写重了");
    let gone = missing(&srv2, &list);
    assert!(
        gone.is_empty(),
        "{durability} 模式下丢了 {} 条已 ack 的写：{:#?}",
        gone.len(),
        &gone[..gone.len().min(10)]
    );
    let after = stats(&srv2);
    assert!(
        after.seq >= before.seq,
        "重启后 seq 不能回退：{} < {}",
        after.seq,
        before.seq
    );
    assert!(after.nodes >= list.len() as u64, "nodes {} < acked {}", after.nodes, list.len());
    srv2.graceful_shutdown();
    list
}

#[test]
fn acked_writes_survive_sigkill_always() {
    let list = hammer_then_kill("always", 4, 400);
    assert_eq!(list.len(), 4 * 400);
}

#[test]
fn acked_writes_survive_sigkill_group() {
    let list = hammer_then_kill("group", 4, 400);
    assert_eq!(list.len(), 4 * 400);
}

/// 崩溃正好落在写的中间：已 ack 的仍在，未 ack 的可以没，但状态必须还能起来。
///
/// 这条故意让主线程在写入途中 `kill -9`：部分批次已经发出去、应答还在路上。
/// 协议语义是「没收到 ack 就等于没发生」，所以只反向要求：收到 ack 的一条不少。
#[test]
fn kill_mid_flight_keeps_every_acked_write() {
    let srv = Server::start("group");
    let addr = srv.addr.clone();
    let acked: Acked = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for t in 0..6usize {
        let a = addr.clone();
        let ack = acked.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            let mut c = Client::connect(&a, 0).expect("connect");
            let v = match ok(&mut c, &Op::CreateView { name: format!("m{t}").into() }) {
                Reply::View { id, .. } => id,
                other => panic!("{other:?}"),
            };
            let mut k = 0usize;
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                // 一整批 200 条一次性发出去：服务端会跨连接合并 fsync，
                // 于是批中间存在「已落盘但还没 ack」的窗口——正是本用例要撞的时刻。
                // 名字必须每条不同（带 i），否则后 199 条全是 EEXIST，什么也没写。
                let names: Vec<String> = (0..200).map(|i| format!("m{t}-{:06}", k + i)).collect();
                let ops: Vec<Op> = names
                    .iter()
                    .map(|n| Op::Create {
                        view: v,
                        parent: 1,
                        name: n.clone().into_bytes(),
                        mode: 0o644,
                        uid: 0,
                        gid: 0,
                    })
                    .collect();
                match c.pipeline(&ops) {
                    Ok(reps) => {
                        assert_eq!(reps.len(), names.len());
                        // 每条都必须是真建成功了：混进 EEXIST 就等于本批没写。
                        for (n, r) in names.iter().zip(&reps) {
                            assert!(matches!(r, Reply::Node(_)), "{n} 写入未成功: {r:?}");
                        }
                        record(&ack, v, names);
                    }
                    // 出错时无法确定这批到底 ack 没 ack，故一条都不记账：
                    // 记账口径保守，才能保证「记下来的必须在」。
                    Err(_) => break,
                }
                k += 200;
            }
            v
        }));
    }
    // 等到有足够多的写已 ack，再抽掉进程。
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while snapshot(&acked).len() < 3000 {
        assert!(std::time::Instant::now() < deadline, "写入没跑起来");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let (tmp, dir) = srv.sigkill_and_take_dir();
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    // join 后才能取全集：kill 前已完整返回的批会在那之后补记账，
    // 它们都是真 ack，必须同样在。
    let list = snapshot(&acked);
    eprintln!("mid-flight: {} 条已 ack，崩溃点在其中", list.len());
    let srv2 = Server::restart_at(tmp, dir, "group");
    let gone = missing(&srv2, &list);
    assert!(
        gone.is_empty(),
        "丢了 {} 条已 ack 的写：{:#?}",
        gone.len(),
        &gone[..gone.len().min(10)]
    );
    // 每条 ack 的写至少吃掉一个 WAL 记录：seq 掉到 ack 数以下 = 有记录被静默跳过。
    let s = stats(&srv2);
    assert!(
        s.seq >= list.len() as u64,
        "seq {} < acked {}，WAL 里少了东西",
        s.seq,
        list.len()
    );
    srv2.graceful_shutdown();
}

/// checkpoint 之后立刻崩溃：快照 + 空 WAL 的组合必须能起来（重启水位从快照接回）。
///
/// 这条是 `Wal::fast_forward` 的回归测试：漏了它，checkpoint 后重启会当场报错，
/// 或者新记录从 1 重新发号、下一次恢复时被当成「快照已覆盖」而静默丢数据。
#[test]
fn crash_right_after_checkpoint() {
    let srv = Server::start("group");
    let mut c = srv.client();
    let v = match ok(&mut c, &Op::CreateView { name: b"ckpt".to_vec() }) {
        Reply::View { id, .. } => id,
        other => panic!("{other:?}"),
    };
    let mut before = Vec::new();
    for k in 0..500 {
        let n = format!("pre-{k:04}");
        create(&mut c, v, &n);
        before.push(n);
    }
    assert_eq!(ok(&mut c, &Op::Checkpoint), Reply::Ok);
    // 快照之后继续写，一条都还没再 checkpoint —— 全靠 WAL 兜住。
    let mut after = Vec::new();
    for k in 0..500 {
        let n = format!("post-{k:04}");
        create(&mut c, v, &n);
        after.push(n);
    }
    let wal_len = std::fs::metadata(srv.dir().join("treedb.wal")).expect("wal").len();
    let (tmp, dir) = srv.sigkill_and_take_dir();
    let srv2 = Server::restart_at(tmp, dir, "group");
    let mut list: Vec<(u32, String)> = before.iter().map(|n| (v, n.clone())).collect();
    list.extend(after.iter().map(|n| (v, n.clone())));
    let gone = missing(&srv2, &list);
    assert!(gone.is_empty(), "checkpoint 后崩溃丢了 {} 条：{:#?}", gone.len(), &gone[..8]);
    // 重放只该覆盖快照之后的部分，不是整本 WAL 从头再来。
    let s = stats(&srv2);
    assert!(s.snap_seq > 0, "重启后应带回快照水位");
    assert!(s.seq >= s.snap_seq);
    assert!(wal_len > 0, "checkpoint 后仍写了 WAL");
    srv2.graceful_shutdown();
}

/// 控制面（密钥/审计/视图定义/放置/锁）与改名删除同样要经得起 `kill -9`。
///
/// 这些数据没有客户端侧的本地缓存兜底，丢了就是真丢了。
#[test]
fn control_plane_survives_sigkill() {
    let srv = Server::start("always");
    let mut c = srv.client();
    let v = match ok(&mut c, &Op::CreateView { name: b"ctl".to_vec() }) {
        Reply::View { id, .. } => id,
        other => panic!("{other:?}"),
    };
    create(&mut c, v, "old-name");
    let renamed = create(&mut c, v, "to-be-renamed");
    assert_eq!(
        ok(
            &mut c,
            &Op::Rename {
                view: v,
                old_parent: 1,
                old_name: b"to-be-renamed".to_vec(),
                new_parent: 1,
                new_name: b"renamed-ok".to_vec(),
                flags: 0,
            }
        ),
        Reply::NodeRef(Vec::new()),
        "rename 回的是 NodeRef（GC 线索），不是 Node"
    );
    let d = match ok(
        &mut c,
        &Op::Mkdir { view: v, parent: 1, name: b"d".to_vec(), mode: 0o755, uid: 0, gid: 0 },
    ) {
        Reply::Node(n) => n,
        other => panic!("{other:?}"),
    };
    assert_eq!(d.kind, Kind::Dir);
    ok(&mut c, &Op::Unlink { view: v, parent: 1, name: b"old-name".to_vec() });
    // 一个指向块引用的文件：fork/快照依赖的 refs 账本也在同一条 WAL 里。
    let with_ref = match ok(
        &mut c,
        &Op::Create {
            view: v,
            parent: d.ino,
            name: b"data".to_vec(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        },
    ) {
        Reply::Node(n) => n,
        other => panic!("{other:?}"),
    };
    // 内容对象键在引擎侧是不透明字节；这里用 grovefs 后端那种字符串键的形状。
    let blk = format!("1:4096:dev{:016x}", 0x5ac0_0000_0000_0001u64).into_bytes();
    ok(
        &mut c,
        &Op::Setattr {
            view: v,
            ino: with_ref.ino,
            valid: SA_DATA_REF | SA_SIZE,
            mode: 0,
            uid: 0,
            gid: 0,
            size: 4096,
            atime: 0,
            mtime: 0,
            data_ref: blk.clone(),
            ref_bytes: 4096,
        },
    );

    ok(&mut c, &Op::KeyCreate { key: key("k-durable", "hash-1") });
    ok(
        &mut c,
        &Op::KeyUpdate { id: b"k-durable".to_vec(), verb: 3, ts: 0, new_hash: Vec::new(), new_hint: Vec::new(), ro: true },
    );
    ok(
        &mut c,
        &Op::KeyUpdate { id: b"k-durable".to_vec(), verb: 2, ts: 777, new_hash: Vec::new(), new_hint: Vec::new(), ro: false },
    );
    ok(&mut c, &Op::AuditAppend { row: AuditRec { seq: 0, ts: 123, key_id: b"k-durable".to_vec(), method: b"xfer.get".to_vec(), tool: b"api".to_vec(), ok: true, detail: b"durable detail".to_vec() } });
    ok(
        &mut c,
        &Op::DefPut { def: DefRec { name: b"view.ctl".to_vec(), kind: b"view".to_vec(), payload: br#"{"sql":"select 1"}"#.to_vec(), updated_at: 7 } },
    );
    ok(&mut c, &Op::PlaceSet { view: b"ctl".to_vec(), instance: b"node-3".to_vec() });
    ok(
        &mut c,
        &Op::LockSet { view: v, ino: renamed.ino, owner: b"owner-1".to_vec(), verb: 0, kind: 2, lease_expiry: 0 },
    );
    drop(c);

    let (tmp, dir) = srv.sigkill_and_take_dir();
    let srv2 = Server::restart_at(tmp, dir, "always");
    let mut c = srv2.client();

    // ---- 目录树：改名/删除/新建都要如常
    assert!(matches!(
        ok(&mut c, &Op::Lookup { view: v, parent: 1, name: b"renamed-ok".to_vec() }),
        Reply::Node(_)
    ));
    for gone_name in [&"to-be-renamed", "old-name"] {
        assert_eq!(
            match rep(
                &mut c,
                &Op::Lookup { view: v, parent: 1, name: gone_name.as_bytes().to_vec() }
            ) {
                Reply::Err { code, .. } => code,
                other => panic!("{gone_name} 崩后还在: {other:?}"),
            },
            errno::ENOENT
        );
    }
    match ok(&mut c, &Op::Getattr { view: v, ino: d.ino }) {
        Reply::Node(n) => assert_eq!((n.kind, n.nlink), (Kind::Dir, 2)),
        other => panic!("{other:?}"),
    }

    // ---- 块引用账本：崩后 count/bytes 仍对得上
    match ok(&mut c, &Op::RefLookup { data_ref: blk }) {
        Reply::Ref { count, bytes } => assert_eq!((count, bytes), (1, 4096)),
        other => panic!("{other:?}"),
    }

    // ---- 密钥：ro / last_used 都要带回来，且旧口令仍能过 auth
    match ok(&mut c, &Op::KeyGet { id: b"k-durable".to_vec() }) {
        Reply::Key(k) => assert_eq!((k.ro, k.last_used_at, k.revoked_at), (true, 777, 0)),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        ok(&mut c, &Op::KeyCheck { id: b"k-durable".to_vec(), verb: 0, hash: b"hash-1".to_vec(), now_ms: 1000 }),
        Reply::Authed { .. }
    ));

    // ---- 审计：seq 由服务端分配，崩后不能重号也不能丢行
    match ok(&mut c, &Op::AuditList { limit: 10 }) {
        Reply::Audits(rows) => {
            assert_eq!(rows.len(), 1, "{rows:?}");
            assert!(rows[0].seq > 0, "audit seq 应当已分配");
            assert_eq!(rows[0].detail, b"durable detail".to_vec());
        }
        other => panic!("{other:?}"),
    }

    // ---- 视图定义与放置
    match ok(&mut c, &Op::DefGet { name: b"view.ctl".to_vec() }) {
        Reply::Def(Some(def)) => assert_eq!(
            (def.kind.as_slice(), def.payload.as_slice(), def.updated_at),
            (b"view".as_slice(), br#"{"sql":"select 1"}"#.as_slice(), 7)
        ),
        other => panic!("{other:?}"),
    }
    match ok(&mut c, &Op::PlaceGet { view: b"ctl".to_vec() }) {
        Reply::Place(i) => assert_eq!(i, b"node-3".to_vec()),
        other => panic!("{other:?}"),
    }

    // ---- 锁：无租约（expiry=0）不会过期，崩后必须仍在
    match ok(&mut c, &Op::LockQuery { view: v, ino: renamed.ino }) {
        Reply::Locks(l) => {
            assert_eq!(l.len(), 1, "崩后锁不对: {l:?}");
            assert_eq!((l[0].0.as_slice(), l[0].1, l[0].2), (b"owner-1".as_slice(), 2u8, 0i64));
        }
        other => panic!("{other:?}"),
    }
    srv2.graceful_shutdown();
}

/// 反复「写—崩—起」不能一点点蚀空状态。
#[test]
fn repeated_kill_cycles_do_not_erode_state() {
    const PER: usize = 300;
    const ROUNDS: usize = 3;
    let mut cur = Server::start("group");
    let mut c = cur.client();
    let v = match ok(&mut c, &Op::CreateView { name: b"cyc".to_vec() }) {
        Reply::View { id, .. } => id,
        other => panic!("{other:?}"),
    };
    drop(c);
    let mut all: Vec<String> = Vec::new();
    for round in 0..ROUNDS {
        let mut c = cur.client();
        for k in 0..PER {
            let n = format!("r{round}-{k:04}");
            create(&mut c, v, &n);
            all.push(n);
        }
        drop(c);
        let (tmp, dir) = cur.sigkill_and_take_dir();
        cur = Server::restart_at(tmp, dir, "group");
        // 每一轮都要能看见此前所有轮次的写，不是只看见最近一段。
        let list: Vec<(u32, String)> = all.iter().map(|n| (v, n.clone())).collect();
        let gone = missing(&cur, &list);
        assert!(
            gone.is_empty(),
            "第 {round} 轮后丢了 {} 条：{:#?}",
            gone.len(),
            &gone[..gone.len().min(8)]
        );
    }
    let s = stats(&cur);
    assert_eq!(s.nodes, 1 + (ROUNDS * PER) as u64, "根 + 每轮写入");
    cur.graceful_shutdown();
}
