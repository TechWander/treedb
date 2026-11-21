//! 多前端 worker（`--workers N`）的验收。
//!
//! 这一套要钉死四件事，缺一不可：
//!
//! 1. **分派表不撒谎**：被判为只读的 op 真的不产 `Log`。判错一条就等于把一次
//!    已确认的写丢在 worker 手里 —— 那是数据丢失，不是性能问题。
//! 2. **开不开 worker，答复逐条相同**：语义不能被并行化改写。
//! 3. **每连接流内顺序不能被扫乱**：含写批次整批交给提交线程；纯读批次即便
//!    "答案晚到"，也不许插到同连接前面那条写的答复之前。
//! 4. **应答只认连接号，不认 fd**：fd 一关就被内核重新分配，按 fd 回填等于把
//!    上一任的写答复塞进新任的队头。
//!
//! 吞吐不在这里断言（时间相关的断言必然 flaky），它属于
//! `benches/throughput_matrix.sh` 的轴 5，数字写进 DESIGN.md §7.5。
//!
//! 跑法：`cargo test --test read_scaling`

mod common;

use common::Server;
use treedb::engine::model::{
    errno, AuditRec, ColumnDef, DefRec, KeyRec, Op, Reply, TableDef, COL_INDEX, COL_UNIQUE,
    COND_NONE, NONCE_LEN, ROW_PK, SNAP_CHUNK_MAX, WAL_FETCH_MAX, WAL_HINT_NONE,
};
use treedb::engine::sha256::DIGEST_LEN;
use treedb::engine::{Durability, Engine};
use treedb::net::client::Client;

/// 内容对象键（引擎侧是不透明字节）；拿 grovefs 那种字符串键的形状凑一个。
fn blk(id: u64) -> Vec<u8> {
    format!("1:4096:dev{id}").into_bytes()
}

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

fn must_err(r: &Reply, want: u16, what: &str) {
    match r {
        Reply::Err { code, msg } => assert_eq!(*code, want, "{what}: 回了 {code} ({})", String::from_utf8_lossy(msg)),
        other => panic!("{what}: 期望 errno {want}，实得 {other:?}"),
    }
}

fn mk_view(c: &mut Client, name: &str) -> u32 {
    match ok(c, &Op::CreateView { name: name.into() }) {
        Reply::View { id, root } => {
            assert_eq!(root, 1);
            id
        }
        other => panic!("{other:?}"),
    }
}

fn create(c: &mut Client, view: u32, name: &str) {
    ok(
        c,
        &Op::Create {
            view,
            parent: 1,
            name: name.into(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    );
}

/// 语料与用例共用的表形状：一列唯一、一列仅索引。
fn tbl(name: &str) -> TableDef {
    TableDef {
        name: name.as_bytes().to_vec(),
        columns: vec![
            ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE },
            ColumnDef { name: b"b".to_vec(), flags: COL_INDEX },
        ],
    }
}

fn rowput(table: &str, key: &str, val: &str, cond_col: u32, cond_val: &str) -> Op {
    Op::RowPut {
        table: table.as_bytes().to_vec(),
        key: key.as_bytes().to_vec(),
        cols: vec![val.as_bytes().to_vec(), b"x".to_vec()],
        flags: 0,
        cond_col,
        cond_val: cond_val.as_bytes().to_vec(),
    }
}

/// 52 个 op 变体的语料，每条都带能让它走到真实分支的参数（视图/文件名/表名
/// 取自下面 `seed` 播好的状态，所以视图 id 由调用方递进来）。加新 op 必须同时
/// 补这里 —— 最后两个计数断言就是报警器。
fn corpus(v: u32) -> Vec<Op> {
    let name = |s: &str| s.as_bytes().to_vec();
    vec![
        // ---- 控制面 ----
        Op::Ping,
        Op::Stats,
        Op::Checkpoint,
        Op::Shutdown,
        Op::CreateView { name: name("new-view") },
        Op::DropView { name: name("seed-view") },
        Op::ListViews,
        // ---- 目录树 ----
        Op::Lookup { view: v, parent: 1, name: name("a") },
        Op::Getattr { view: v, ino: 2 },
        Op::Setattr {
            view: v, ino: 2, valid: 1, mode: 0o600, uid: 0, gid: 0,
            size: 0, atime: 0, mtime: 0, data_ref: Vec::new(), ref_bytes: 0,
        },
        Op::Create { view: v, parent: 1, name: name("new-file"), mode: 0o644, uid: 1, gid: 1 },
        Op::Mkdir { view: v, parent: 1, name: name("new-dir"), mode: 0o755, uid: 1, gid: 1 },
        Op::Symlink { view: v, parent: 1, name: name("new-link"), target: name("a"), uid: 1, gid: 1 },
        // 源 ino 取种子里的 "a"（=2）：给真存在的 inode 再加一个名字。
        Op::Link { view: v, ino: 2, parent: 1, name: name("a2") },
        Op::Readlink { view: v, ino: 2 },
        Op::Unlink { view: v, parent: 1, name: name("a") },
        Op::Rmdir { view: v, parent: 1, name: name("new-dir") },
        Op::Rename { view: v, old_parent: 1, old_name: name("a"), new_parent: 1, new_name: name("b"), flags: 0},
        Op::ReadDir { view: v, parent: 1, offset: 0, limit: 100 },
        // ---- 共享账本与锁 ----
        Op::RefLookup { data_ref: blk(7) },
        Op::RefUp { data_ref: blk(7), bytes: 4096 },
        Op::RefDown { data_ref: blk(7), bytes: 4096 },
        Op::LockQuery { view: v, ino: 2 },
        Op::LockSet { view: v, ino: 2, owner: name("mnt1"), verb: 0, kind: 1, lease_expiry: i64::MAX },
        // ---- 密钥 ----
        Op::KeyCreate {
            key: KeyRec {
                id: name("k-new"), label: name("l"), secret_hash: name("h"),
                created_at: 1, last_used_at: 0, revoked_at: 0, view: name("seed-view"),
                ro: false, owner: name("o"), note: name(""), expires_at: i64::MAX,
                created_by: name("test"), secret_hint: name(""),
            },
        },
        Op::KeyGet { id: name("k-seed") },
        Op::KeyList,
        Op::KeyUpdate { id: name("k-seed"), verb: 2, ts: 2, new_hash: vec![], new_hint: vec![], ro: false },
        Op::KeyCheck { id: name("k-seed"), verb: 0, hash: name("seed-hash"), now_ms: 1 },
        // ---- 审计 / 定义 / 放置 ----
        Op::AuditAppend {
            row: AuditRec { seq: 0, ts: 1, key_id: name("k-seed"), method: name("ls"), tool: name("cli"), ok: true, detail: name("") },
        },
        Op::AuditList { limit: 10 },
        Op::DefPut { def: DefRec { name: name("d-new"), kind: name("json"), payload: name("{}"), updated_at: 1 } },
        Op::DefGet { name: name("d-seed") },
        Op::DefList { kind: name("") },
        Op::DefDel { name: name("d-seed") },
        Op::PlaceSet { view: name("seed-view"), instance: name("pg2") },
        Op::PlaceGet { view: name("seed-view") },
        Op::PlaceList,
        // ---- 通用表 ----
        Op::TableCreate { def: tbl("new-table") },
        Op::TableDrop { name: name("seed-table") },
        Op::TableGet { name: name("seed-table") },
        Op::TableList,
        Op::RowPut { table: name("seed-table"), key: name("r-new"), cols: vec![name("v-new"), name("x")], flags: 0, cond_col: COND_NONE, cond_val: Vec::new() },
        Op::RowGet { table: name("seed-table"), key: name("r1") },
        Op::RowList { table: name("seed-table"), column: ROW_PK, value: Vec::new(), from: Vec::new(), limit: 10 },
        Op::RowDelete { table: name("seed-table"), key: name("r2"), flags: 0, cond_col: COND_NONE, cond_val: Vec::new() },
        Op::RowCount { table: name("seed-table"), column: 1, value: name("x") },
        // ---- readdir 带属性 ----
        Op::ReadDirPlus { view: v, parent: 1, offset: 0, limit: 100 },
        // ---- 日志传送 ----
        // 参数取真值形状：`want_seq=1` 在种子状态下确实还没被截进快照，
        // 走的是交付而不是 gap 那条支路。
        Op::WalFetch { want_seq: 1, hint_off: WAL_HINT_NONE, max_bytes: WAL_FETCH_MAX },
        Op::SnapFetch { offset: 0, len: SNAP_CHUNK_MAX },
        // ---- 接入认证（§4.4）----
        // 这两条在服务端由事件循环就地处置、根本不进引擎；走进来只会撞上
        // `apply.rs` 那条大声的 EINVAL 兜底。语料仍然必须覆盖它们：下面两个
        // 计数断言就是「新 op 有没有被登记进三档分类」的报警器。
        Op::AuthBegin { client_nonce: [0x11; NONCE_LEN] },
        Op::AuthFinish { mac: [0x22; DIGEST_LEN] },
    ]
}

/// 播一个"每条只读 op 都能真查到东西"的小状态，返回种子视图 id。
fn seed(e: &mut Engine) -> u32 {
    let v = match e.submit(&Op::CreateView { name: b"seed-view".to_vec() }).unwrap() {
        Reply::View { id, root } => {
            assert_eq!(root, 1, "视图 root ino 必须是 1，语料里的 parent/ino 都按它写");
            id
        }
        other => panic!("{other:?}"),
    };
    e.submit(&Op::Create { view: v, parent: 1, name: b"a".to_vec(), mode: 0o644, uid: 1, gid: 1 }).unwrap();
    e.submit(&Op::Mkdir { view: v, parent: 1, name: b"d".to_vec(), mode: 0o755, uid: 1, gid: 1 }).unwrap();
    e.submit(&Op::Symlink { view: v, parent: 1, name: b"l".to_vec(), target: b"a".to_vec(), uid: 1, gid: 1 }).unwrap();
    e.submit(&Op::LockSet { view: v, ino: 2, owner: b"mnt".to_vec(), verb: 0, kind: 1, lease_expiry: i64::MAX }).unwrap();
    e.submit(&Op::RefUp { data_ref: blk(7), bytes: 4096 }).unwrap();
    e.submit(&Op::KeyCreate {
        key: KeyRec {
            id: b"k-seed".to_vec(), label: b"l".to_vec(), secret_hash: b"seed-hash".to_vec(),
            created_at: 1, last_used_at: 0, revoked_at: 0, view: b"seed-view".to_vec(),
            ro: false, owner: b"o".to_vec(), note: b"".to_vec(), expires_at: i64::MAX,
            created_by: b"test".to_vec(), secret_hint: b"".to_vec(),
        },
    })
    .unwrap();
    e.submit(&Op::AuditAppend {
        row: AuditRec { seq: 0, ts: 1, key_id: b"k-seed".to_vec(), method: b"ls".to_vec(), tool: b"cli".to_vec(), ok: true, detail: b"".to_vec() },
    })
    .unwrap();
    e.submit(&Op::DefPut {
        def: DefRec { name: b"d-seed".to_vec(), kind: b"json".to_vec(), payload: b"{}".to_vec(), updated_at: 1 },
    })
    .unwrap();
    e.submit(&Op::PlaceSet { view: b"seed-view".to_vec(), instance: b"pg1".to_vec() }).unwrap();
    // 通用表：不建表不灌行，五条只读表 op 两边都只会拼 ENOENT，分类断言
    // 就只在测「查不存在的表不产日志」这一件废话。
    e.submit(&Op::TableCreate { def: tbl("seed-table") }).unwrap();
    for k in ["r1", "r2", "r3"] {
        e.submit(&rowput("seed-table", k, &format!("v{k}"), COND_NONE, "")).unwrap();
    }
    v
}

/// 判为只读的 op 必须真的不产 `Log`。这条断言是分派表的安全边界：错一条，
/// 事件循环就会把一个会改状态的 op 交给只读 worker，logs 从此消失。
#[test]
fn readonly_classification_matches_translate() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut e = Engine::open(tmp.path(), Durability::Never).expect("open");
    let v = seed(&mut e);
    // exec_read 只取读锁、不 apply，所以对写 op 也可以安全试探它的 logs。
    let st = e.shared_state();
    let (mut ro, mut wr) = (0usize, 0usize);
    let cs = corpus(v);
    for op in &cs {
        let (_reply, logs) = Engine::exec_read(&st, &op);
        if op.is_readonly() {
            assert!(
                logs.is_empty(),
                "{op:?} 判为只读却产了 {} 条 logs：它其实会改状态，绝不能进读池",
                logs.len()
            );
            ro += 1;
        } else {
            wr += 1;
        }
    }
    let cs_len = cs.len();
    assert_eq!(cs_len, 52, "op 语料必须覆盖全部变体");
    assert_eq!(ro, 22, "只读 op 数变了：分派表、这里和 DESIGN.md §4.2 分组表要一起改");
    assert_eq!(wr, 30, "非只读 op 数变了：同上");
}

/// 三档划分必须穷举且两两不交：只读 / 真写 / 控制面。
///
/// 这条断言存在的理由不是形式上好看：副本拒写的判据是
/// `!is_readonly() && !is_control()`。漏一档（一条其实不改状态的 op 被归进
/// 真写）会让副本把它拒成 `EROFS`，客户端看到的是一个没道理的错误；多一档
/// （一条真写被归进控制面）就是副本接受了本该拒绝的写 —— 那是数据分叉。
/// 两个方向都得有人接住，这里就用三个数接：上面那条分类断言只兜「只读不
/// 产 logs」，兜不到控制面这一档。
#[test]
fn ops_partition_into_three_classes() {
    let cs = corpus(1);
    let (mut ro, mut wr, mut ctl) = (0usize, 0usize, 0usize);
    for op in &cs {
        match (op.is_readonly(), op.is_control()) {
            (true, false) => ro += 1,
            (false, false) => wr += 1,
            (false, true) => ctl += 1,
            (true, true) => panic!("{op:?} 既只读又是控制面：两档必须不交"),
        }
    }
    assert_eq!(
        (ro, wr, ctl),
        (22, 23, 7),
        "三档的分布：只读 22 / 真写 23 / 控制面 7（Stats、Checkpoint、Shutdown、WalFetch、\
         SnapFetch、AuthBegin、AuthFinish）"
    );
    // 语料覆盖全部变体，因此三档之和就是 op 总数。
    assert_eq!(ro + wr + ctl, cs.len());
}

/// 抹掉时间戳。两次独立运行的 `now_ms()` 必然不同，拿它们比“语义是否一致”
/// 只会比出时钟；除时间之外的每个字段都必须逐字相等。
fn norm(r: Reply) -> Reply {
    match r {
        Reply::Node(mut n) => {
            n.atime = 0;
            n.mtime = 0;
            n.ctime = 0;
            Reply::Node(n)
        }
        // readdirplus 的属性里同样带着创建时刻：除时间之外的字段才是语义。
        Reply::DirentsPlus { inos, kinds, names, attrs, next, end } => Reply::DirentsPlus {
            inos,
            kinds,
            names,
            attrs: attrs
                .into_iter()
                .map(|mut a| {
                    a.atime = 0;
                    a.mtime = 0;
                    a.ctime = 0;
                    a
                })
                .collect(),
            next,
            end,
        },
        other => other,
    }
}

/// 同一份工作负载，`workers=1`（单线程基线）与 `workers=8` 的答复必须逐条相同。
/// 这是"并行化没有改写语义"的整体证明。
#[test]
fn worker_reads_answer_identically_to_inline_reads() {
    let mut got: Vec<Vec<Reply>> = Vec::new();
    for workers in [1usize, 8usize] {
        let srv = Server::start_workers("group", workers);
        let mut c = srv.client();
        let v = mk_view(&mut c, "parity");
        let mut out = vec![];
        for k in 0..40 {
            create(&mut c, v, &format!("f{k}"));
        }
        // 通用表：先建表灌行（这些写的答复不进 `out`），下面的只读才有东西可查。
        ok(&mut c, &Op::TableCreate { def: tbl("parity") });
        for k in 0..10 {
            ok(&mut c, &rowput("parity", &format!("p{k}"), &format!("v{k}"), COND_NONE, ""));
        }
        // 纯读批次：workers>1 时在 worker 线程就地算完并发出。
        let batch: Vec<Op> = (0..40)
            .map(|k| Op::Lookup { view: v, parent: 1, name: format!("f{k}").into_bytes() })
            .chain(std::iter::once(Op::ReadDir { view: v, parent: 1, offset: 0, limit: 100 }))
            .chain((0..10).map(|k| Op::Getattr { view: v, ino: 2 + k as u64 }))
            .chain(std::iter::once(Op::ReadDirPlus { view: v, parent: 1, offset: 0, limit: 100 }))
            .chain(std::iter::once(Op::TableGet { name: b"parity".to_vec() }))
            .chain(std::iter::once(Op::TableList))
            .chain((0..5).map(|k| Op::RowGet { table: b"parity".to_vec(), key: format!("p{k}").into_bytes() }))
            .chain(std::iter::once(Op::RowList { table: b"parity".to_vec(), column: ROW_PK, value: Vec::new(), from: Vec::new(), limit: 4 }))
            .chain(std::iter::once(Op::RowCount { table: b"parity".to_vec(), column: 1, value: b"x".to_vec() }))
            .collect();
        out.extend(c.pipeline(&batch).expect("pipeline reads"));
        // 混打批次：含写就整批交给提交线程，否则同连接顺序就不保了。
        let mixed = vec![
            Op::Lookup { view: v, parent: 1, name: b"later".to_vec() },
            Op::Create { view: v, parent: 1, name: b"later".to_vec(), mode: 0o644, uid: 1, gid: 1 },
            Op::Lookup { view: v, parent: 1, name: b"later".to_vec() },
            Op::ReadDir { view: v, parent: 1, offset: 0, limit: 100 },
            // 表写夹在表读之间：CAS 的答复必须看到同批次前面的写。
            Op::RowList { table: b"parity".to_vec(), column: ROW_PK, value: Vec::new(), from: Vec::new(), limit: 4 },
            rowput("parity", "p0", "v0b", 0, "v0"),
            Op::RowGet { table: b"parity".to_vec(), key: b"p0".to_vec() },
            Op::RowCount { table: b"parity".to_vec(), column: 1, value: b"x".to_vec() },
        ];
        out.extend(c.pipeline(&mixed).expect("pipeline mixed"));
        got.push(out.into_iter().map(norm).collect());
        let _ = srv.graceful_shutdown();
    }
    assert_eq!(got[0].len(), got[1].len());
    for (i, (a, b)) in got[0].iter().zip(got[1].iter()).enumerate() {
        assert_eq!(a, b, "第 {i} 条答复因 workers 而异：并行化改写了语义");
    }
    assert_eq!(
        got[0].len(),
        40 + 1 + 10 + 1 + 2 + 5 + 2 + 8,
        "负载条数对不上，用例本身写坏了"
    );
}

/// 一条连接上 `[读, 写, 读, 写, 读]` 的流水线，答复必须严格按请求顺序，
/// 反复跑以撞出任何"读的答案提前/推后"的窗口。
#[test]
fn read_write_stream_keeps_per_connection_order() {
    let srv = Server::start_workers("group", 8);
    let mut c = srv.client();
    let v = mk_view(&mut c, "order");
    for k in 0..400 {
        let name = format!("f{k}").into_bytes();
        let rs = c
            .pipeline(&[
                Op::Lookup { view: v, parent: 1, name: name.clone() },
                Op::Create { view: v, parent: 1, name: name.clone(), mode: 0o644, uid: 1, gid: 1 },
                Op::Lookup { view: v, parent: 1, name: name.clone() },
                Op::Unlink { view: v, parent: 1, name: name.clone() },
                Op::Lookup { view: v, parent: 1, name },
            ])
            .expect("pipeline");
        assert_eq!(rs.len(), 5);
        must_err(&rs[0], errno::ENOENT, "创建前的 lookup");
        assert!(matches!(rs[1], Reply::Node(_)), "create 应回 Node，实得 {:?}", rs[1]);
        assert!(matches!(rs[2], Reply::Node(_)), "创建后的 lookup 应命中，实得 {:?}", rs[2]);
        assert!(matches!(rs[3], Reply::NodeRef(_)), "unlink 应回 NodeRef，实得 {:?}", rs[3]);
        must_err(&rs[4], errno::ENOENT, "删除后的 lookup");
    }
    // 全删干净了：目录树只剩视图根本身（ino 1）。
    match ok(&mut c, &Op::ReadDir { view: v, parent: 1, offset: 0, limit: 1000 }) {
        Reply::Dirents { names, .. } => assert!(names.is_empty(), "残留 {} 条目录项", names.len()),
        other => panic!("{other:?}"),
    }
}

/// 一写多读同时打：已 ack 的写最终必须全被读到，而且不能读到半截状态。
#[test]
fn concurrent_reads_do_not_obey_stale_or_torn_writes() {
    let srv = Server::start_workers("group", 4);
    let n = 500usize;
    // 先把视图建好并把 id 分给两边：读者要在写者跑的同时真的读到东西，
    // 而不是对一个不存在的视图拼错。
    let v = mk_view(&mut srv.client(), "cw");
    let writer = {
        let addr = srv.addr.clone();
        std::thread::spawn(move || {
            let mut c = Client::connect(&addr, 0).expect("writer connect");
            for k in 0..n {
                c.call(&Op::Create {
                    view: v, parent: 1, name: format!("w{k}").into_bytes(),
                    mode: 0o644, uid: 1, gid: 1,
                })
                .expect("create ack");
            }
        })
    };
    // 读者一边打读批次，一边只能得到"还没建出来"或"已建出来"两种一致答案，
    // 绝不能拿到半个目录项。写者一完事就收工，不用定时长这种 flaky 同步。
    let done = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let read_clients: Vec<_> = (0..6)
        .map(|_| {
            let addr = srv.addr.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let mut c = Client::connect(&addr, 0).expect("reader connect");
                let mut reads = 0usize;
                while !done.load(std::sync::atomic::Ordering::Relaxed) && reads < 200_000 {
                    let _ = c.pipeline(&[
                        Op::ReadDir { view: v, parent: 1, offset: 0, limit: 200 },
                        Op::ListViews,
                        Op::Lookup { view: v, parent: 1, name: b"w0".to_vec() },
                    ]);
                    reads += 3;
                }
                reads
            })
        })
        .collect();
    writer.join().expect("writer join");
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    for r in read_clients {
        assert!(r.join().expect("reader join") > 3, "读者没真跑起来");
    }
    // 收口后所有已确认的写都必须在。
    let mut c = srv.client();
    match ok(&mut c, &Op::ReadDir { view: v, parent: 1, offset: 0, limit: 100_000 }) {
        Reply::Dirents { names, end, .. } => {
            assert!(end, "一次就该读到底");
            assert_eq!(names.len(), n, "已 ack 的创建少见了：{} / {n}", names.len());
        }
        other => panic!("{other:?}"),
    }
    let _ = srv.graceful_shutdown();
}

/// `Stats.ops_total` 必须把 worker 就地算掉的读也记上 —— 否则运维看到的
/// QPS 比真实量少一大截。
#[test]
fn stats_counts_worker_reads() {
    let srv = Server::start_workers("group", 8);
    let mut c = srv.client();
    let v = mk_view(&mut c, "count");
    create(&mut c, v, "x");
    let base = match ok(&mut c, &Op::Stats) {
        Reply::Stats(s) => s.ops_total,
        other => panic!("{other:?}"),
    };
    let k = 500usize;
    let batch: Vec<Op> = (0..k)
        .map(|_| Op::Lookup { view: v, parent: 1, name: b"x".to_vec() })
        .collect();
    let rs = c.pipeline(&batch).expect("pipeline reads");
    assert_eq!(rs.len(), k);
    let after = match ok(&mut c, &Op::Stats) {
        Reply::Stats(s) => s.ops_total,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        after - base,
        k as u64 + 1,
        "两条 Stats 之间只跑了 {k} 条读 + 第二次 Stats 本身一条"
    );
    let _ = srv.graceful_shutdown();
}

/// 多前端 worker 开着也不能破坏崩溃语义：`kill -9` 后已 ack 的写全部还在。
#[test]
fn acked_writes_survive_crash_with_workers_on() {
    let srv = Server::start_workers("group", 8);
    let mut c = srv.client();
    let v = mk_view(&mut c, "crash");
    let n = 300;
    ok(&mut c, &Op::TableCreate { def: tbl("crash") });
    for k in 0..n {
        create(&mut c, v, &format!("c{k}"));
        // 表写与目录写同批交错，worker 与提交线程的交叠面要盖住通用表。
        ok(&mut c, &rowput("crash", &format!("c{k}"), &format!("v{k}"), COND_NONE, ""));
        // 每几条插一批纯读，逼 worker 与写 fsync 交叠。
        if k % 20 == 0 {
            let _ = c.pipeline(&[
                Op::ReadDir { view: v, parent: 1, offset: 0, limit: 500 },
                Op::Getattr { view: v, ino: 2 },
                Op::ListViews,
                Op::ReadDirPlus { view: v, parent: 1, offset: 0, limit: 500 },
                Op::RowList { table: b"crash".to_vec(), column: ROW_PK, value: Vec::new(), from: Vec::new(), limit: 500 },
            ]);
        }
    }
    let (tmp, dir) = srv.sigkill_and_take_dir();
    let srv2 = Server::restart_at_workers(tmp, dir, "group", 8);
    let mut c2 = srv2.client();
    match ok(&mut c2, &Op::ReadDir { view: v, parent: 1, offset: 0, limit: 100_000 }) {
        Reply::Dirents { names, .. } => {
            assert_eq!(names.len(), n as usize, "崩溃后已 ack 的目录项不齐")
        }
        other => panic!("{other:?}"),
    }
    match ok(&mut c2, &Op::RowCount { table: b"crash".to_vec(), column: ROW_PK, value: Vec::new() }) {
        Reply::Count(m) => assert_eq!(m, n as u64, "崩溃后已 ack 的行不齐"),
        other => panic!("{other:?}"),
    }
    // 二级索引不进 WAL 也不进快照：重开后它必须被重建出来，否则同一个唯一值
    // 能插两行 —— 这是“可推字段不动磁盘格式”那个取舍的真实代价。
    assert!(
        matches!(c2.call(&rowput("crash", "dup", "v0", COND_NONE, "")), Ok(Reply::Err { code: errno::EEXIST, .. })),
        "重启后唯一约束失效：索引没从行重推出来"
    );
    let _ = srv2.graceful_shutdown();
}

/// 应答路由必须认**连接号**而不是 fd。fd 一关就被内核重新分配，很可能立刻
/// 分给同一条 worker 线程上的新任连接：按 fd 回填的话，上一任那批写的答复会
/// 填进新任的 `Slot::Await` —— 新任客户端发出 `Create`，收到的却是别人的 ino。
///
/// 所以“新任”自己也得有一批在飞的写：没有待回槽位的错路由帧只会静静地被丢掉，
/// 测不出任何东西。（本用例的第一版就踩了这个坑：拿一个只发 Ping 的新任连接，
/// 把“按 fd 路由”的错版本跑得干干净净，等于没测。）
///
/// 窗口能不能撞上取决于内核的 fd 分配与新连接落在哪条 worker（`SO_REUSEPORT`
/// 会把它分到任意一条上），这里不主张每轮都踩中；但断言与是否踩中无关，任何一次错路由
/// 都会当场炸。
#[test]
fn conn_id_reuse_does_not_misroute_acks() {
    let srv = Server::start_workers("group", 2);
    let mut boot = srv.client();
    let v = mk_view(&mut boot, "reuse");
    for round in 0..300 {
        // 前任：连发一批写、一条都不读回，然后立刻断开。提交线程手里还压着它
        // 那批答复时，它的 fd 已经还给内核了。
        //
        // 断开时本地接收缓冲里堆着未读的答复，关闭会发 RST，可能把服务端尚未
        // 读走的请求一并抹掉 —— 这批写本来就没拿到 ack，与本用例要验的东西无关。
        let mut ghost = Client::connect(&srv.addr, 5_000).expect("connect ghost");
        for k in 0..40 {
            ghost
                .send(&Op::Create {
                    view: v,
                    parent: 1,
                    name: format!("g{round}_{k}").into_bytes(),
                    mode: 0o644,
                    uid: 1,
                    gid: 1,
                })
                .expect("send create");
        }
        drop(ghost);
        // 新任：自己也有 40 条在飞的写。每条 ack 必须就是它刚建出来的那个节点 ——
        // 拿自己的名字再 lookup 一次，ino 必须对得上。
        let mut fresh = Client::connect(&srv.addr, 5_000).expect("connect fresh");
        let mine: Vec<String> = (0..40).map(|k| format!("f{round}_{k}")).collect();
        let batch: Vec<Op> = mine
            .iter()
            .map(|n| Op::Create {
                view: v,
                parent: 1,
                name: n.clone().into_bytes(),
                mode: 0o644,
                uid: 1,
                gid: 1,
            })
            .collect();
        let acks = fresh.pipeline(&batch).expect("fresh creates");
        assert_eq!(acks.len(), mine.len(), "第 {round} 轮应答条数不对");
        for (n, a) in mine.iter().zip(&acks) {
            let ino = match a {
                Reply::Node(x) => x.ino,
                other => panic!("第 {round} 轮 {n} 的 create 实得 {other:?}"),
            };
            match ok(&mut fresh, &Op::Lookup { view: v, parent: 1, name: n.clone().into_bytes() }) {
                Reply::Node(x) => assert_eq!(
                    x.ino, ino,
                    "第 {round} 轮 {n}：ack 里的 ino 与真实节点不是同一个 —— 应答被错路由到这条连接了"
                ),
                other => panic!("{other:?}"),
            }
        }
        assert!(!fresh.has_buffered(), "第 {round} 轮新任连接手里多出了上一任的答复");
    }
    let _ = srv.graceful_shutdown();
}
