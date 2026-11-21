//! 离线 dump / load 的目录级验收：真造一份「所有表都有内容」的数据目录，
//! 导成文件、导进新目录、用 `Engine::open` 重开核对，再导一次比字节。
//!
//! 判据有两条，缺一不可：
//!
//! 1. **语义**：`restore` 之后重开出来的状态，与源目录关闭前的状态逐字段相等。
//! 2. **确定性**：从目标目录再导一次，字节与源 dump 完全相同 —— 如果导出顺序
//!    依赖 HashMap 迭代序或时间戳，这条会先响，而不是等到某次跨机搬运才露头。
//!
//! 跑法：`cargo test --test dump_restore`

use std::path::{Path, PathBuf};
use tempfile::TempDir;
use treedb::engine::model::{
    AuditRec, ColumnDef, DefRec, KeyRec, Kind, Node, Op, Reply, TableDef, COL_INDEX, COL_UNIQUE,
    COND_NONE, SA_DATA_REF,
};
use treedb::engine::{dump_dir, now_ms, restore_dump, Durability, Engine, State};

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
    /// 数据目录本身。`tag` 只影响临时根，目录名统一叫 treedb。
    fn path(&self) -> PathBuf {
        self.tmp.path().join("treedb")
    }
    /// 临时根：放过 dump 文件的地方（**不能**放进数据目录，那会让目标非空）。
    fn root(&self) -> &Path {
        self.tmp.path()
    }
    fn open(&self) -> Engine {
        Engine::open(&self.path(), Durability::Always).expect("engine open")
    }
}

fn ok(e: &mut Engine, op: &Op) -> Reply {
    match e.submit(op) {
        Ok(Reply::Err { code, msg }) => panic!(
            "{op:?} → unexpected errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        Ok(r) => r,
        Err(err) => panic!("{op:?} → submit error: {err}"),
    }
}

fn node(r: Reply) -> Node {
    match r {
        Reply::Node(n) => n,
        other => panic!("expected Reply::Node, got {other:?}"),
    }
}

fn mk_view(e: &mut Engine, name: &str) -> u32 {
    match ok(e, &Op::CreateView { name: name.into() }) {
        Reply::View { id, root } => {
            assert_eq!(root, 1);
            id
        }
        other => panic!("expected Reply::View, got {other:?}"),
    }
}

fn mk_file(e: &mut Engine, view: u32, parent: u64, name: &str) -> Node {
    node(ok(
        e,
        &Op::Create {
            view,
            parent,
            name: name.into(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ))
}

fn mk_dir(e: &mut Engine, view: u32, parent: u64, name: &str) -> Node {
    node(ok(
        e,
        &Op::Mkdir {
            view,
            parent,
            name: name.into(),
            mode: 0o755,
            uid: 1000,
            gid: 1000,
        },
    ))
}

fn blk(id: u64) -> Vec<u8> {
    format!("1:4096:dev{id}").into_bytes()
}

fn commit_ref(view: u32, ino: u64, data_ref: u64, bytes: u64) -> Op {
    Op::Setattr {
        view,
        ino,
        valid: SA_DATA_REF,
        mode: 0,
        uid: 0,
        gid: 0,
        size: 0,
        atime: 0,
        mtime: 0,
        data_ref: blk(data_ref),
        ref_bytes: bytes,
    }
}

fn key(id: &str) -> KeyRec {
    KeyRec {
        id: id.into(),
        label: format!("label-{id}").into(),
        secret_hash: b"hash-1".to_vec(),
        created_at: 7,
        last_used_at: 9,
        revoked_at: 0,
        view: b"team".to_vec(),
        ro: false,
        owner: b"alice".to_vec(),
        note: b"n".to_vec(),
        expires_at: 0,
        created_by: b"root".to_vec(),
        secret_hint: b"h".to_vec(),
    }
}

/// 每张表都放一条：漏掉哪张，`assert_eq!(back, before)` 会给出字段名。
fn build_rich(e: &mut Engine) -> (u32, u64) {
    let v = mk_view(e, "v");
    let dir = mk_dir(e, v, 1, "d");
    let a = mk_file(e, v, 1, "a");
    let b = mk_file(e, v, dir.ino, "b");
    ok(e, &commit_ref(v, a.ino, 100, 4096));
    ok(e, &commit_ref(v, b.ino, 100, 1024));
    // 硬链接：同 inode 两个名字（nlink / dentry 两条名字都要还原）。
    ok(e, &Op::Link { view: v, ino: a.ino, parent: dir.ino, name: b"b-alias".to_vec() });
    ok(
        e,
        &Op::Symlink { view: v, parent: 1, name: b"l".to_vec(), target: b"a".to_vec(), uid: 1, gid: 1 },
    );
    ok(
        e,
        &Op::LockSet {
            view: v,
            ino: a.ino,
            owner: b"o1".to_vec(),
            verb: 0,
            kind: 1,
            lease_expiry: now_ms() + 60_000,
        },
    );
    ok(e, &Op::KeyCreate { key: key("k1") });
    ok(
        e,
        &Op::AuditAppend {
            row: AuditRec {
                seq: 0,
                ts: 5,
                key_id: b"k1".to_vec(),
                method: b"POST".to_vec(),
                tool: b"fs_ls".to_vec(),
                ok: true,
                detail: b"d".to_vec(),
            },
        },
    );
    ok(
        e,
        &Op::DefPut {
            def: DefRec { name: b"ls".to_vec(), kind: b"tool".to_vec(), payload: b"x".to_vec(), updated_at: 3 },
        },
    );
    ok(e, &Op::PlaceSet { view: b"v".to_vec(), instance: b"node-1".to_vec() });
    ok(
        e,
        &Op::TableCreate {
            def: TableDef {
                name: b"t1".to_vec(),
                columns: vec![
                    ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE },
                    ColumnDef { name: b"b".to_vec(), flags: COL_INDEX },
                ],
            },
        },
    );
    for (k, a1, b1) in [("r1", "a1", "x"), ("r2", "a2", "y")] {
        ok(
            e,
            &Op::RowPut {
                table: b"t1".to_vec(),
                key: k.as_bytes().to_vec(),
                cols: vec![a1.as_bytes().to_vec(), b1.as_bytes().to_vec()],
                flags: 0,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            },
        );
    }
    (v, a.ino)
}

// ---------------------------------------------------------------- 用例

/// 源状态必须是「快照 + 增量日志」两段都在，否则这个用例测不到 dump 里
/// 「快照之后的尾巴」那半边。
#[test]
fn dump_then_restore_reproduces_the_directory_and_its_bytes() {
    let src = Dir::new("dump-src");
    let (v, before, durable): (u32, State, u64);
    {
        let mut e = src.open();
        v = build_rich(&mut e).0;
        ok(&mut e, &Op::Checkpoint);
        assert_eq!(e.wal_len(), 0, "同步 checkpoint 之后 WAL 应为空");
        // 快照之后再写两条：dump 必须走「快照 + 重放尾巴」，不能只看快照。
        mk_file(&mut e, v, 1, "after-snap");
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"l".to_vec() });
        before = e.state_snapshot();
        assert!(e.wal_len() > 0, "尾巴必须真的存在");
        let (snap_seq, wal_seq) = match ok(&mut e, &Op::Stats) {
            Reply::Stats(s) => (s.snap_seq, s.seq),
            other => panic!("{other:?}"),
        };
        durable = wal_seq;
        assert!(snap_seq > 0, "快照那半边也必须存在");
        assert!(snap_seq < durable, "快照不能已经盖到尾，否则测不到重放");
    }

    let dumped = dump_dir(&src.path()).expect("dump");
    assert_eq!(dumped.seq, durable, "水位必须等于磁盘上最后一条日志的 seq");
    assert!(dumped.logs.len() > 0);

    // 落成文件再读回：容器自身的 encode/decode 也要过一遍。
    let f = src.root().join("grove.dump");
    let n = dumped.write_file(&f).expect("write dump");
    assert_eq!(n as usize, dumped.encode().len());
    let back = treedb::engine::Dump::read_file(&f).expect("read dump");
    assert_eq!(back, dumped, "读回同一份 dump 必须逐字段相等");

    // 导进一个全新的目录。
    let dst = Dir::new("dump-dst");
    restore_dump(&dst.path(), &back).expect("restore");
    assert!(dst.path().join("state.snap").exists(), "导入必须落出 state.snap");
    assert!(!dst.path().join("treedb.wal").exists(), "导入不写 WAL");

    {
        let e2 = dst.open();
        assert_eq!(e2.state_snapshot(), before, "restore 后状态必须与源逐字段相等");
        let rec = e2.recovery();
        assert_eq!(rec.snap_seq, durable, "快照水位必须来自 dump 的 seq");
        assert_eq!(rec.wal_records, 0, "导入后没有任何日志可重放");
    }

    // 从目标目录再导一次：字节必须与源 dump 完全相同。必须在写新东西之前做，
    // 否则测到的是「新写有没有进 dump」，不再是「同一份状态导出两次」。
    let again = dump_dir(&dst.path()).expect("re-dump");
    assert_eq!(
        again.encode(),
        dumped.encode(),
        "dump → restore → dump 必须逐字节相同（导出顺序/计数器有漂移）"
    );
    assert_eq!(again, dumped);

    // 最后证明确实「接着能写」：水位计数器不能停在 0，否则新写会盖掉旧 seq。
    {
        let mut e3 = dst.open();
        let fresh = mk_file(&mut e3, v, 1, "post-restore");
        assert!(fresh.ino > 1);
        match ok(&mut e3, &Op::Stats) {
            Reply::Stats(s) => assert!(s.seq > durable, "新写必须在 dump 水位之后接着排"),
            other => panic!("{other:?}"),
        }
        ok(&mut e3, &Op::Checkpoint);
    }
}

/// fail-closed 的两道闸：非空目标、以及「服务正在跑」的目录。
#[test]
fn restore_and_dump_refuse_the_two_dangerous_cases() {
    let src = Dir::new("guard-src");
    let dumped;
    {
        let mut e = src.open();
        let v = mk_view(&mut e, "v");
        mk_file(&mut e, v, 1, "f");
        ok(&mut e, &Op::Checkpoint);
    }
    dumped = dump_dir(&src.path()).expect("dump");

    // 1) 目标目录里已有别人的东西：拒绝，且不覆盖、不合并。
    let busy = Dir::new("guard-busy");
    std::fs::create_dir_all(busy.path()).unwrap();
    std::fs::write(busy.path().join("treedb.wal"), b"someone else's log").unwrap();
    let err = restore_dump(&busy.path(), &dumped).expect_err("非空目标必须被拒");
    assert!(
        err.to_string().contains("refusing to restore"),
        "错误信息要能让人看懂为什么被拒：{err}"
    );
    assert_eq!(
        std::fs::read(busy.path().join("treedb.wal")).unwrap(),
        b"someone else's log",
        "拒绝之后一个字节都不许动"
    );
    assert!(!busy.path().join("state.snap").exists());

    // 2) 空目录（或已存在、只含 `.lock`）可以导入。
    let empty = Dir::new("guard-empty");
    std::fs::create_dir_all(empty.path()).unwrap();
    restore_dump(&empty.path(), &dumped).expect("空目标必须可导入");
    drop(Engine::open(&empty.path(), Durability::Always).expect("restored dir opens"));

    // 3) 引擎在跑时 dump 被锁挡下；关掉之后同一目录立即可导。
    let live = Dir::new("guard-live");
    let e = live.open();
    let err = dump_dir(&live.path()).expect_err("服务在跑时必须拒");
    assert!(
        err.to_string().contains("locked"),
        "错误信息要指出是目录被占：{err}"
    );
    drop(e);
    let after = dump_dir(&live.path()).expect("释放锁后必须可导");
    assert_eq!(after.seq, 0);
    assert!(after.logs.is_empty(), "空目录导出的重建序列应为空");
}

/// 让 `Kind` / `NodeAttr` 的 import 有真实用处，同时把「零票引用不进 dump」
/// 这条隐式契约钉在状态层：`state_to_logs` 自己会挡，但只有当状态里真的
/// 出现 `count == 0` 的行时才测得到。
#[test]
fn zero_ref_rows_are_not_emitted_and_kind_survives_the_roundtrip() {
    let d = Dir::new("dump-refs");
    let before: State;
    {
        let mut e = d.open();
        let v = mk_view(&mut e, "v");
        let f = mk_file(&mut e, v, 1, "f");
        let g = mk_file(&mut e, v, 1, "g");
        // 两个 inode 指同一个内容对象：账本 count 累计到 2。
        ok(&mut e, &commit_ref(v, f.ino, 7, 10));
        ok(&mut e, &commit_ref(v, g.ino, 7, 10));
        before = e.state_snapshot();
    }
    assert_eq!(before.refs[&blk(7)].count, 2);
    assert_eq!(before.nodes.values().next().map(|n| n.kind), Some(Kind::Dir));
    let dumped = dump_dir(&d.path()).expect("dump");
    // 状态里只应有 1 条 PutRef，而不含任何「删账本」的记录。
    let refs = dumped.logs.iter().filter(|l| matches!(l, treedb::engine::Log::PutRef { .. })).count();
    assert_eq!(refs, 1);
    let s = dumped.state();
    assert_eq!(s.refs, before.refs);
    assert_eq!(s.nodes, before.nodes, "Kind 必须从 PutNode 原样回来");
    assert_eq!(s.dentries, before.dentries, "冗余 Dent::kind 由节点推出，必须对上");
}

// ---------------------------------------------------------------- 真进程

/// 起真二进制，返回（退出码，stdout+stderr）。与 `tests/migrate.rs` 同一个写法：
/// 退出码是运维唯一能写进脚本的契约，不能只靠库里单测。
fn cli(bin: &str, args: &[&str]) -> (i32, String) {
    let out = std::process::Command::new(bin)
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn {bin}: {e}"));
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code().unwrap_or(-1), text)
}

/// 两个命令的真进程契约：0 = 完成，1 = 拒绝/出错，2 = 参数不全。
/// **每个失败态都要求目标目录一个字节不动** —— 这正是 fail-closed 的意思。
#[test]
fn cli_dump_and_load_hold_their_exit_codes() {
    let dump_bin = env!("CARGO_BIN_EXE_treedb-dump");
    let load_bin = env!("CARGO_BIN_EXE_treedb-load");

    // 源目录：两个视图 + 几个文件，然后 checkpoint（快照 + 空 WAL）。
    let src = Dir::new("cli-src");
    {
        let mut e = src.open();
        let v = mk_view(&mut e, "v");
        mk_dir(&mut e, v, 1, "d");
        mk_file(&mut e, v, 1, "a");
        mk_file(&mut e, v, 1, "b");
        mk_view(&mut e, "v2");
        ok(&mut e, &Op::Checkpoint);
    }
    let out_file = src.root().join("cli.dump");
    let out_s = out_file.to_str().unwrap().to_string();
    let dir_s = src.path().to_str().unwrap().to_string();

    // 缺少 --out → 2。
    let (code, text) = cli(dump_bin, &["--dir", &dir_s]);
    assert_eq!(code, 2, "{text}");
    assert!(text.contains("--out"), "{text}");

    // 导出 → 0，stdout 里要有 seq 与规模。
    let (code, text) = cli(dump_bin, &["--dir", &dir_s, "--out", &out_s]);
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("dump    = seq ") && text.contains("views 2"), "{text}");
    assert!(out_file.is_file(), "{text}");

    // 体检档：不给 --dir 就只报告，一个字节不写。
    let (code, text) = cli(load_bin, &["--in", &out_s]);
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("dry-run"), "{text}");

    // 导入到新目录 → 0；同一目录再来一次 → 1，且报「拒收」。
    let dst = src.root().join("cli-dst");
    let dst_s = dst.to_str().unwrap().to_string();
    let (code, text) = cli(load_bin, &["--in", &out_s, "--dir", &dst_s]);
    assert_eq!(code, 0, "{text}");
    assert!(dst.join("state.snap").is_file());
    assert!(!dst.join("treedb.wal").exists(), "导入不写 WAL：{text}");
    let snap_after_first = std::fs::read(dst.join("state.snap")).unwrap();
    let (code, text) = cli(load_bin, &["--in", &out_s, "--dir", &dst_s]);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("refusing to restore"), "{text}");
    assert_eq!(
        std::fs::read(dst.join("state.snap")).unwrap(),
        snap_after_first,
        "拒绝之后目标目录一个字节都不许动"
    );

    // 坏一个字节 → 1，且报 CRC 不符（不是解出一半就写盘）。
    let bad = src.root().join("cli-bad.dump");
    let mut raw = std::fs::read(&out_file).unwrap();
    raw[64] ^= 0xff;
    std::fs::write(&bad, &raw).unwrap();
    let (code, text) = cli(load_bin, &["--in", bad.to_str().unwrap()]);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("crc mismatch"), "{text}");

    // 服务在跑时导出 → 1，错误信息要点名「被锁」。
    let live = Dir::new("cli-live");
    let e = live.open();
    let (code, text) = cli(
        dump_bin,
        &[
            "--dir",
            live.path().to_str().unwrap(),
            "--out",
            live.root().join("x.dump").to_str().unwrap(),
        ],
    );
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("locked"), "{text}");
    drop(e);
    let (code, text) = cli(
        dump_bin,
        &[
            "--dir",
            live.path().to_str().unwrap(),
            "--out",
            live.root().join("x.dump").to_str().unwrap(),
        ],
    );
    assert_eq!(code, 0, "释放锁后必须可导：{text}");
}
