//! `Op::Batch` 原子批验收（DESIGN §5.1）。
//!
//! 五条主张，一条一个用例组：
//!
//! 1. **顺序可见**：批内后一条看得到前一条的写（`create` 完直接 `setattr` 那个新 ino）。
//! 2. **失败整批不落**：任何一条回 errno，真状态一个字段都不变（连父目录的
//!    `mtime`/`nlink` 与 `next_ino` 都不许动 —— 预演落在 `State` 的副本上）。
//! 3. **一条 WAL 记录**：整批只推进一个 seq；把这条记录从中间截断，重开后
//!    整批一条不剩，而不是「前几条还在」。这一条是「崩溃原子」的可执行定义。
//! 4. **拒绝面**：空批、超 `MAX_BATCH_OPS`、嵌套、控制面 op —— 结构化构造走
//!    `translate` 回 `EINVAL`，字节层面的越界/嵌套在解码就断连。
//! 5. **并发对抗**：上面四条都在单线程里跑，而批的原子性真正要防的是「多个连接同时
//!    在同一批行上做乐观锁」。这两条用真服务端、真 TCP、真 `fsync` 跑（见
//!    [`contended_cas_batches_never_lose_a_transfer`]），并且在 `workers=1` 与
//!    `workers=8` 两种前端形状上各跑一遍 —— 只测一种等于另一种裸奔（§11 第 7 条）。
//!
//! 跑法：`cargo test --test batch`

mod common;

use common::Server;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tempfile::TempDir;
use treedb::engine::core::WAL_FILE;
use treedb::engine::model::{
    errno, ColumnDef, Op, Reply, TableDef, COL_UNIQUE, COND_NONE, MAX_BATCH_OPS, PUT_IF_ABSENT,
    ROW_PK, SA_MODE, SA_UID,
};
use treedb::engine::{Durability, Engine, Rd, W};
use treedb::net::client::Client;

// ---------------------------------------------------------------- 驱动脚手架

struct Dir {
    tmp: TempDir,
}

impl Dir {
    fn new(tag: &str) -> Dir {
        let tmp = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(tmp.path().join(tag)).unwrap();
        Dir { tmp }
    }
    fn path(&self) -> PathBuf {
        self.tmp.path().join("treedb")
    }
    fn open(&self) -> Engine {
        Engine::open(&self.path(), Durability::Always).expect("engine open")
    }
    fn wal_path(&self) -> PathBuf {
        self.path().join(WAL_FILE)
    }
}

fn sub(e: &mut Engine, op: &Op) -> Reply {
    e.submit(op).unwrap_or_else(|err| panic!("{op:?} → submit error: {err}"))
}

fn ok(e: &mut Engine, op: &Op) -> Reply {
    match sub(e, op) {
        Reply::Err { code, msg } => panic!(
            "{op:?} → unexpected errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        r => r,
    }
}

/// 期望失败：回 `(errno, msg)`，供用例读下标与原因。
fn bad(e: &mut Engine, op: &Op) -> (u16, String) {
    match sub(e, op) {
        Reply::Err { code, msg } => (code, String::from_utf8_lossy(&msg).into_owned()),
        other => panic!("{op:?} → expected Reply::Err, got {other:?}"),
    }
}

fn mk_view(e: &mut Engine) -> u32 {
    match ok(e, &Op::CreateView { name: b"v".to_vec() }) {
        Reply::View { id, root } => {
            assert_eq!(root, 1);
            id
        }
        other => panic!("{other:?}"),
    }
}

fn mkdir(view: u32, name: &str) -> Op {
    Op::Mkdir {
        view,
        parent: 1,
        name: name.into(),
        mode: 0o755,
        uid: 1000,
        gid: 1000,
    }
}

fn create(view: u32, name: &str) -> Op {
    Op::Create {
        view,
        parent: 1,
        name: name.into(),
        mode: 0o644,
        uid: 1000,
        gid: 1000,
    }
}

fn lookup(view: u32, name: &str) -> Op {
    Op::Lookup {
        view,
        parent: 1,
        name: name.into(),
    }
}

fn getattr(e: &mut Engine, view: u32, ino: u64) -> treedb::engine::model::Node {
    match ok(e, &Op::Getattr { view, ino }) {
        Reply::Node(n) => n,
        other => panic!("{other:?}"),
    }
}

fn exists(e: &mut Engine, view: u32, name: &str) -> bool {
    !matches!(sub(e, &lookup(view, name)), Reply::Err { .. })
}

fn setattr(view: u32, ino: u64, valid: u32, mode: u32, uid: u32) -> Op {
    Op::Setattr {
        view,
        ino,
        valid,
        mode,
        uid,
        gid: 0,
        size: 0,
        atime: 0,
        mtime: 0,
        data_ref: Vec::new(),
        ref_bytes: 0,
    }
}

fn batch(ops: Vec<Op>) -> Op {
    Op::Batch { ops }
}

fn replies(r: Reply) -> Vec<Reply> {
    match r {
        Reply::Batch { replies } => replies,
        other => panic!("expected Reply::Batch, got {other:?}"),
    }
}

// ---------------------------------------------------------------- 顺序可见

/// 批内「先建后改」是合法的：`setattr` 要用 `create` 才分配的 ino（2），
/// 若预演没落在副本上、或副本没被前一条的 `apply` 推着走，这一条会 `ENOENT`。
#[test]
fn a_batch_sees_its_own_writes() {
    let dir = Dir::new("batch-sees-writes");
    let mut e = dir.open();
    let v = mk_view(&mut e);

    let seq0 = e.apply_seq();
    let out = replies(ok(
        &mut e,
        &batch(vec![
            create(v, "f"),
            setattr(v, 2, SA_MODE, 0o600, 0),
            setattr(v, 2, SA_UID, 0, 42),
            lookup(v, "f"),
        ]),
    ));
    assert_eq!(out.len(), 4, "应答与子 op 逐位对应");
    let seq1 = e.apply_seq();
    assert_eq!(
        seq1 - seq0,
        1,
        "四条子 op 只推进一个 seq：整批是一条 WAL 记录（seq {seq0} → {seq1}）"
    );

    // 第 1 条回的是 create 的成品：ino 2（根是 1）。
    match &out[0] {
        Reply::Node(n) => assert_eq!(n.ino, 2),
        other => panic!("{other:?}"),
    }
    // 第 3 条（lookup）看到的是前两条改完之后的样子 —— 顺序可见。
    match &out[3] {
        Reply::Node(n) => {
            assert_eq!(n.mode & 0o7777, 0o600, "lookup 要看到批内刚才那次 setattr");
            assert_eq!(n.uid, 42, "两次 setattr 都生效了（分别改不同字段）");
        }
        other => panic!("{other:?}"),
    }
    // 批外的读者看到的是同一个结果。
    let n = getattr(&mut e, v, 2);
    assert_eq!(n.mode & 0o7777, 0o600);
    assert_eq!(n.uid, 42);
}

// ---------------------------------------------------------------- 失败整批不落

/// 第二条撞名失败：第一条已经 `apply` 进副本了，真状态必须一个字段没动。
/// 判据取三处最容易漏的：目录的 `nlink`/`size`/`mtime`、`next_ino`、
/// 以及那两个名字本身。
#[test]
fn one_bad_op_leaves_the_whole_batch_unapplied() {
    let dir = Dir::new("batch-all-or-nothing");
    let mut e = dir.open();
    let v = mk_view(&mut e);
    ok(&mut e, &create(v, "a"));
    let before = getattr(&mut e, v, 1);
    let seq0 = e.apply_seq();

    let (code, msg) = bad(
        &mut e,
        &batch(vec![create(v, "b"), mkdir(v, "a"), create(v, "c")]),
    );
    assert_eq!(code, errno::EEXIST, "带上失败那条的 errno，不是含糊的「批失败」");
    assert!(msg.starts_with("batch op #1: "), "msg 要指认第几条：{msg}");
    assert_eq!(e.apply_seq(), seq0, "失败的批一条日志都不产");

    let after = getattr(&mut e, v, 1);
    assert_eq!(after.nlink, before.nlink, "父目录 nlink 不许被预演漏出来");
    assert_eq!(after.size, before.size, "父目录 size 同上");
    assert_eq!(after.mtime, before.mtime, "父目录 mtime 同上");
    assert_eq!(after.ctime, before.ctime, "父目录 ctime 同上");
    assert!(!exists(&mut e, v, "b"), "#0 的那次 create 不能留下");
    assert!(!exists(&mut e, v, "c"), "#2 虽然自己没报错，也得一起不落");
    assert!(exists(&mut e, v, "a"), "批之前的写当然还在");

    // ino 分配也没被吃掉：`a` 用了 2，下一个 create 拿到的还是 3。
    match ok(&mut e, &create(v, "d")) {
        Reply::Node(n) => assert_eq!(n.ino, 3, "失败批里预留的 ino 必须回收（本来就是预演）"),
        other => panic!("{other:?}"),
    }
}

/// 同一张表上的唯一约束也走同一条路：两条 `RowPut` 抢同一个唯一值，
/// 第二条 `EEXIST`，整批不落 —— 证明预演副本里的 `row_index` 是跟着
/// `apply` 实时维护的（否则第二条看不出撞车）。
#[test]
fn a_batch_on_the_generic_table_is_atomic_too() {
    let dir = Dir::new("batch-table");
    let mut e = dir.open();
    let def = TableDef {
        name: b"t".to_vec(),
        columns: vec![
            ColumnDef {
                name: b"a".to_vec(),
                flags: COL_UNIQUE,
            },
            ColumnDef {
                name: b"b".to_vec(),
                flags: 0,
            },
        ],
    };
    ok(&mut e, &Op::TableCreate { def });

    let put = |key: &str, a: &str, b: &str| Op::RowPut {
        table: b"t".to_vec(),
        key: key.as_bytes().to_vec(),
        cols: vec![a.as_bytes().to_vec(), b.as_bytes().to_vec()],
        flags: 0,
        cond_col: u32::MAX,
        cond_val: Vec::new(),
    };
    let count = |e: &mut Engine| match ok(e, &Op::RowCount { table: b"t".to_vec(), column: ROW_PK, value: Vec::new() }) {
        Reply::Count(n) => n,
        other => panic!("{other:?}"),
    };

    let (code, msg) = bad(&mut e, &batch(vec![put("k1", "x", "1"), put("k2", "x", "2")]));
    assert_eq!(code, errno::EEXIST);
    assert!(msg.contains("batch op #1"), "{msg}");
    assert_eq!(count(&mut e), 0, "撞唯一的那一批连第一行都不许留");

    // 换成不撞的取值，同一形状必须整批成功。
    ok(&mut e, &batch(vec![put("k1", "x", "1"), put("k2", "y", "2")]));
    assert_eq!(count(&mut e), 2);
}

// ---------------------------------------------------------------- 一条记录 + 崩溃原子

/// 扫描一段 WAL 字节，返回最后一条完好记录的 `(偏移, 整条长度)`。
///
/// 帧格式见 §6.1：`seq(8) crc(4) len(4) reserved(4)` + payload。遇到 `len == 0`
/// 就停 —— 那是预分配空洞，不是记录。
fn last_record(buf: &[u8]) -> Option<(usize, usize)> {
    const HDR: usize = 20;
    let mut off = 0usize;
    let mut last = None;
    while off + HDR <= buf.len() {
        let len = u32::from_le_bytes(buf[off + 12..off + 16].try_into().unwrap()) as usize;
        if len == 0 || off + HDR + len > buf.len() {
            break;
        }
        last = Some((off, HDR + len));
        off += HDR + len;
    }
    last
}

/// 整批的日志是**一条**记录：把它从 payload 中间截断，重开后整批一条不剩。
///
/// 若实现退回「一批多条记录」，同样这刀只会切掉最后那条子日志，
/// `x`/`y` 会留在状态里 —— 所以这条用例精确地钉住容器，而不是只钉住「写进去了」。
#[test]
fn a_batch_is_one_wal_record_that_a_torn_tail_drops_whole() {
    let dir = Dir::new("batch-torn");
    let v = {
        let mut e = dir.open();
        let v = mk_view(&mut e);
        let seq0 = e.apply_seq();
        replies(ok(
            &mut e,
            &batch(vec![mkdir(v, "x"), mkdir(v, "y"), create(v, "z")]),
        ));
        assert_eq!(e.apply_seq() - seq0, 1, "三条子 op 一条记录");
        let bytes = std::fs::read(dir.wal_path()).unwrap();
        let (off, len) = last_record(&bytes).expect("WAL 里至少有建视图与批两条记录");
        // 截在 payload 中间：头是完整的，CRC 覆盖的那段少了一半。
        let cut = off + len / 2;
        assert!(cut > off + 20, "得真的切在 payload 里");
        std::fs::File::options()
            .write(true)
            .open(dir.wal_path())
            .unwrap()
            .set_len(cut as u64)
            .unwrap();
        v
    };

    // 第一遍：不截断的那份 WAL 是好的（上面已经切了，所以这里只验恢复后的样子）。
    let mut e = dir.open();
    assert!(!exists(&mut e, v, "x"), "整批丢：x 不能留");
    assert!(!exists(&mut e, v, "y"), "整批丢：y 不能留");
    assert!(!exists(&mut e, v, "z"), "整批丢：z 不能留");
    // 截断点之前那条记录（建视图）照旧重放：坏尾只丢自己这一条。
    match ok(&mut e, &Op::ListViews) {
        Reply::Views(vs) => assert_eq!(vs.len(), 1, "前面的记录不该被牵连"),
        other => panic!("{other:?}"),
    }
    // 目录属性也回到建视图那一刻：没有 nlink +2 / size 变化的残留。
    let root = getattr(&mut e, v, 1);
    assert_eq!(root.nlink, 2, "只剩 root 自己 + 它的 '..'");
    assert_eq!(root.size, 0);
}

// ---------------------------------------------------------------- 只读批

/// 纯读批不产日志、不进提交线程（`is_readonly` 递归），应答与逐条发一致。
#[test]
fn a_read_only_batch_costs_no_log_and_answers_like_separate_calls() {
    let dir = Dir::new("batch-readonly");
    let mut e = dir.open();
    let v = mk_view(&mut e);
    ok(&mut e, &create(v, "f"));

    // 四条都读得到：批内一条失败就是整批失败，所以这里不留「读一下不存在的名字」。
    let ops = vec![lookup(v, "f"), getattr_op(v, 2), Op::ListViews, lookup(v, "f")];
    assert!(
        matches!(&batch(ops.clone()), Op::Batch { ops } if ops.iter().all(|o| o.is_readonly())),
        "四条都是只读，整批就该判只读"
    );

    let seq0 = e.apply_seq();
    let got = replies(ok(&mut e, &batch(ops.clone())));
    assert_eq!(e.apply_seq(), seq0, "纯读批一条日志都不产");

    // 逐条发，应答必须逐条相同。
    assert_eq!(got.len(), 4);
    for (i, op) in ops.iter().enumerate() {
        assert_eq!(got[i], sub(&mut e, op), "第 {i} 条应答与单独发不一致");
    }
    // 失败面：只读批里一条 ENOENT 也把整批抬成失败（读一下确认"名字在不在"
    // 必须能被调用方看见），而且不产日志。
    let seq1 = e.apply_seq();
    let (code, msg) = bad(&mut e, &batch(vec![lookup(v, "nope"), lookup(v, "f")]));
    assert_eq!(code, errno::ENOENT);
    assert!(msg.starts_with("batch op #0: "), "{msg}");
    assert_eq!(e.apply_seq(), seq1);
}

fn getattr_op(view: u32, ino: u64) -> Op {
    Op::Getattr { view, ino }
}

// ---------------------------------------------------------------- 拒绝面

/// 四类结构化拒绝 + 两条字节层面的拒绝。
///
/// 字节层面那两条落在**解码**里（`Codec` 错 = 断连）：先防 `Vec::with_capacity`
/// 的内存放大与递归爆栈，再谈 errno。这个分工与 `WalFetch` 那组「越界回 EINVAL、
/// 断帧回 Codec」是同一条界。
#[test]
fn batch_refuses_empty_oversize_nested_and_control_ops() {
    let dir = Dir::new("batch-refusals");
    let mut e = dir.open();
    let v = mk_view(&mut e);
    let seq0 = e.apply_seq();

    let (code, msg) = bad(&mut e, &batch(vec![]));
    assert_eq!(code, errno::EINVAL);
    assert_eq!(msg, "empty batch");

    let mut too_many: Vec<Op> = (0..=MAX_BATCH_OPS).map(|_| Op::ListViews).collect();
    assert_eq!(too_many.len(), MAX_BATCH_OPS + 1);
    let (code, msg) = bad(&mut e, &batch(too_many.clone()));
    assert_eq!(code, errno::EINVAL);
    assert!(msg.contains("MAX_BATCH_OPS"), "{msg}");

    let (code, msg) = bad(&mut e, &batch(vec![lookup(v, "f"), batch(vec![lookup(v, "f")])]));
    assert_eq!(code, errno::EINVAL);
    assert!(msg.contains("is itself a batch"), "{msg}");

    for ctl in [Op::Stats, Op::Checkpoint, Op::Shutdown] {
        let (code, msg) = bad(&mut e, &batch(vec![create(v, "x"), ctl.clone()]));
        assert_eq!(code, errno::EINVAL, "{ctl:?} 不许进批");
        assert!(msg.contains("control-plane"), "{msg}");
        assert!(!exists(&mut e, v, "x"), "{ctl:?} 被拒时整批不落");
    }

    // 边界那条要真的能过：正好 MAX_BATCH_OPS 条的只读批。
    too_many.truncate(MAX_BATCH_OPS);
    let out = replies(ok(&mut e, &batch(too_many)));
    assert_eq!(out.len(), MAX_BATCH_OPS);

    assert_eq!(e.apply_seq(), seq0, "上面这一串拒绝与只读批都不许碰 WAL");

    // 字节层面：65 条 → 解码就拒。
    let mut w = W::new();
    w.u16(0x007a);
    w.u32(MAX_BATCH_OPS as u32 + 1);
    w.u16(0x0001); // 一条 Ping 垫在最前面，够触发计数检查就行
    let bytes = w.finish();
    let err = Op::decode(&mut Rd::new(&bytes)).expect_err("超限的批要在解码里拒");
    assert!(format!("{err}").contains("MAX_BATCH_OPS"), "{err}");

    // 字节层面：嵌套 → 解码就拒（防爆栈）。
    let mut w = W::new();
    w.u16(0x007a);
    w.u32(1);
    w.u16(0x007a); // 子 op 又是批
    w.u32(0);
    let bytes = w.finish();
    let err = Op::decode(&mut Rd::new(&bytes)).expect_err("嵌套批要在解码里拒");
    assert!(format!("{err}").contains("nested batch"), "{err}");
}

// ---------------------------------------------------------------- 并发对抗

/// 余额的字节形状：十进制 ASCII。CAS 只比字节相等，不需要可排序。
fn amt(n: u32) -> Vec<u8> {
    n.to_string().into_bytes()
}

/// 无条件写 —— 建数据用（带条件的写对不存在的行必然 `EAGAIN`）。
fn set_raw(key: &str, n: u32) -> Op {
    Op::RowPut {
        table: b"bal".to_vec(),
        key: key.as_bytes().to_vec(),
        cols: vec![amt(n)],
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

/// 带 CAS 的写：要求这一列当前恰好是 `expect`。
fn set_cas(key: &str, n: u32, expect: u32) -> Op {
    Op::RowPut {
        table: b"bal".to_vec(),
        key: key.as_bytes().to_vec(),
        cols: vec![amt(n)],
        flags: 0,
        cond_col: 0,
        cond_val: amt(expect),
    }
}

fn row_get(key: &str) -> Op {
    Op::RowGet {
        table: b"bal".to_vec(),
        key: key.as_bytes().to_vec(),
    }
}

/// 期望成功的那类调用（建表、初始化）。失败时把 op 与 errno 一起打出来。
fn must(c: &mut Client, op: &Op) -> Reply {
    match c.call(op) {
        Ok(Reply::Err { code, msg }) => panic!(
            "{op:?} → errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        Ok(r) => r,
        Err(e) => panic!("{op:?} → transport error: {e}"),
    }
}

fn one_row(r: &Reply) -> u32 {
    match r {
        Reply::Row(Some(rec)) => String::from_utf8_lossy(&rec.cols[0])
            .parse()
            .unwrap_or_else(|e| panic!("余额不是十进制: {e}")),
        other => panic!("RowGet → {other:?}"),
    }
}

/// 用**一条只读批**读回一对余额。
///
/// 「没有半批可见」的可执行定义就是：任何一次采样都满足 `a + b == total`。
/// 只读批在同一次锁持有期内翻译两条 `RowGet`（apply.rs 的纯读批分支），所以
/// 它拿到的是一个切面，而不是两个各取各的时刻。
fn read_pair(c: &mut Client) -> (u32, u32) {
    match must(c, &batch(vec![row_get("A"), row_get("B")])) {
        Reply::Batch { replies } => {
            assert_eq!(replies.len(), 2);
            (one_row(&replies[0]), one_row(&replies[1]))
        }
        other => panic!("只读批 → {other:?}"),
    }
}

/// 原子性的两本账：`(seq, writes_total)`。
fn seq_writes(c: &mut Client) -> (u64, u64) {
    match must(c, &Op::Stats) {
        Reply::Stats(s) => (s.seq, s.writes_total),
        other => panic!("Stats → {other:?}"),
    }
}

/// 建 `bal(amt)`，把 `total` 全压在 A 上，返回落盘后的账本基准。
fn setup_bal(srv: &Server, total: u32) -> (u64, u64) {
    let mut c = srv.client();
    must(
        &mut c,
        &Op::TableCreate {
            def: TableDef {
                name: b"bal".to_vec(),
                columns: vec![ColumnDef {
                    name: b"amt".to_vec(),
                    flags: 0,
                }],
            },
        },
    );
    must(&mut c, &set_raw("A", total));
    must(&mut c, &set_raw("B", 0));
    seq_writes(&mut c)
}

/// 一笔转账 = 一条批 = 两个 CAS。`EAGAIN` 是乐观锁的正常代价，不是错误。
fn send_one(c: &mut Client, retries: &AtomicUsize) -> bool {
    let (a, b) = read_pair(c);
    assert!(a > 0, "本线程还有该转的额度，A 却已空 —— 钱被凭空造出来了");
    match c
        .call(&batch(vec![set_cas("A", a - 1, a), set_cas("B", b + 1, b)]))
        .expect("transport")
    {
        Reply::Batch { replies } => {
            assert_eq!(replies.len(), 2);
            assert!(
                replies.iter().all(|x| matches!(x, Reply::Row(Some(_)))),
                "批成功却有条目失败：{replies:?}"
            );
            true
        }
        Reply::Err { code, .. } if code == errno::EAGAIN => {
            retries.fetch_add(1, Ordering::Relaxed);
            false
        }
        other => panic!("转账批 → {other:?}"),
    }
}

/// N 个连接在同一对行上做乐观锁转账：钱不许多不许少，旁观者不许看到半批，
/// 账本上必须恰好「一笔转账 = 一条 WAL 记录」。
///
/// 前面四节都在线程 0 里跑，加在一起也证明不了一件事：**批与批之间的互斥靠
/// 的是那把写锁，不是运气**。若 `translate` 与 `apply` 之间让出过锁，两个转账
/// 批会各自基于同一份旧余额做预演，后落地的那笔把前一笔覆盖掉 —— 经典的丢失
/// 更新。它从统计上看不出来（`seq` 照样 +2、`writes_total` 照样 +2），只有
/// `(a, b)` 的和会少一块，所以断言必须落在钱上、并且要有独立采样者。
#[test]
fn contended_cas_batches_never_lose_a_transfer() {
    const THREADS: usize = 4;
    const PER: usize = 40;
    const TOTAL: u32 = (THREADS * PER) as u32;
    /// 单笔转账的重试上限：撞满说明活锁，而不是慢。
    const ATTEMPTS: usize = 50_000;

    // 两种前端形状各跑一遍：`workers=1` 时读写都排在主线程的事件循环里，
    // `workers=8` 时只读批被派到 worker 线程、与提交线程并行。只测一种，
    // 另一种就是裸奔（§11 第 7 条）。
    for workers in [1usize, 8] {
        let srv = Server::start_workers("group", workers);
        let (seq0, writes0) = setup_bal(&srv, TOTAL);
        let done = AtomicBool::new(false);
        let torn = AtomicUsize::new(0);
        let retries = AtomicUsize::new(0);

        std::thread::scope(|sc| {
            let watch = sc.spawn(|| {
                let mut c = srv.client();
                while !done.load(Ordering::Relaxed) {
                    let (a, b) = read_pair(&mut c);
                    if a + b != TOTAL {
                        torn.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            let sends: Vec<_> = (0..THREADS)
                .map(|_| {
                    sc.spawn(|| {
                        let mut c = srv.client();
                        for _ in 0..PER {
                            let mut moved = false;
                            for _ in 0..ATTEMPTS {
                                if send_one(&mut c, &retries) {
                                    moved = true;
                                    break;
                                }
                            }
                            assert!(moved, "workers={workers}：重试 {ATTEMPTS} 次仍转不出去，活锁");
                        }
                    })
                })
                .collect();
            for h in sends {
                h.join().unwrap();
            }
            done.store(true, Ordering::Relaxed);
            watch.join().unwrap();
        });

        let mut c = srv.client();
        let (a, b) = read_pair(&mut c);
        assert_eq!(
            (a, b),
            (0, TOTAL),
            "workers={workers}：转账数恰好等于总额度，A 必须清零、B 必须收满"
        );
        let (seq1, writes1) = seq_writes(&mut c);
        assert_eq!(
            seq1 - seq0,
            TOTAL as u64,
            "workers={workers}：{TOTAL} 笔转账必须恰好 {TOTAL} 条 WAL 记录，一条不多一条不少"
        );
        assert_eq!(
            writes1 - writes0,
            TOTAL as u64,
            "workers={workers}：被 `EAGAIN` 拒掉的批一个字段都不许改，也就一进不了写账"
        );
        assert_eq!(torn.load(Ordering::Relaxed), 0, "workers={workers}：旁观者看到过半批");
        // 一次冲突都没有，说明这压根不是竞争负载，前面那几条断言就只是重言式。
        assert!(
            retries.load(Ordering::Relaxed) > 0,
            "workers={workers}：没有一次 CAS 冲突，用例退化成了串行跑"
        );
    }
}

/// 8 个连接对同一个不存在的键抢 `PUT_IF_ABSENT`：恰好一个成功，其余 `EEXIST`，
/// 落盘的是赢家那一份。
///
/// 这条要防的是「存在性检查与写入之间让出了锁」：那样会有两个连接都看到键
/// 不在、都产出一条 `PutRow`，后写的静默覆盖先写的 —— 而 `PUT_IF_ABSENT` 的全部
/// 承诺就是「创建者唯一」。和丢失更新一样，`seq` 与 `writes_total` 都会 +2，
/// 只有值本身与创建者计数能揭穿它。
#[test]
fn contended_put_if_absent_elects_exactly_one_winner() {
    const RACERS: usize = 8;
    let srv = Server::start_workers("group", 4);
    let mut c = srv.client();
    must(
        &mut c,
        &Op::TableCreate {
            def: TableDef {
                name: b"bal".to_vec(),
                columns: vec![ColumnDef {
                    name: b"amt".to_vec(),
                    flags: 0,
                }],
            },
        },
    );
    let (seq0, writes0) = seq_writes(&mut c);

    let winner = AtomicUsize::new(usize::MAX);
    let created = AtomicUsize::new(0);
    let clashed = AtomicUsize::new(0);
    let go = AtomicBool::new(false);

    std::thread::scope(|sc| {
        // 线程要按值搬走 `i`，共享的那几个就得先借出来再 `move`，否则 `move`
        // 会试图把 `Server` 与几个原子量整个搬进每一个闭包。
        let (srv, go, winner, created, clashed) = (&srv, &go, &winner, &created, &clashed);
        let racer: Vec<_> = (0..RACERS)
            .map(|i| {
                sc.spawn(move || {
                    let mut c = srv.client();
                    // 都连上再同时发车：一边连一边写的话，先连的那个早就写完了。
                    while !go.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    match c
                        .call(&Op::RowPut {
                            table: b"bal".to_vec(),
                            key: b"k".to_vec(),
                            cols: vec![amt(i as u32)],
                            flags: PUT_IF_ABSENT,
                            cond_col: COND_NONE,
                            cond_val: Vec::new(),
                        })
                        .expect("transport")
                    {
                        Reply::Row(Some(_)) => {
                            assert_eq!(
                                winner.swap(i, Ordering::SeqCst),
                                usize::MAX,
                                "两个连接都声称自己是创建者"
                            );
                            created.fetch_add(1, Ordering::Relaxed);
                        }
                        Reply::Err { code, .. } if code == errno::EEXIST => {
                            clashed.fetch_add(1, Ordering::Relaxed);
                        }
                        other => panic!("PUT_IF_ABSENT → {other:?}"),
                    }
                })
            })
            .collect();
        go.store(true, Ordering::Release);
        for h in racer {
            h.join().unwrap();
        }
    });

    let mut c = srv.client();
    let won = winner.load(Ordering::SeqCst);
    assert_eq!(created.load(Ordering::Relaxed), 1, "{RACERS} 个抢同一个键，赢家必须恰好一个");
    assert_eq!(
        clashed.load(Ordering::Relaxed),
        RACERS - 1,
        "其余都要按 `EEXIST` 被拒，不能静默成功"
    );
    match must(&mut c, &row_get("k")) {
        Reply::Row(Some(rec)) => assert_eq!(
            rec.cols[0],
            amt(won as u32),
            "落盘的值必须是赢家那一份，不能是被后来者覆盖后的任意一份"
        ),
        other => panic!("RowGet → {other:?}"),
    }
    let (seq1, writes1) = seq_writes(&mut c);
    assert_eq!(seq1 - seq0, 1, "{RACERS} 个请求只该有一条记录落地");
    assert_eq!(writes1 - writes0, 1, "被拒的那一些不许进 writes_total");
}
