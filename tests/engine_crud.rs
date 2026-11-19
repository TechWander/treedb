//! 引擎功能验收：POSIX 目录树语义 + refs 账本 + 锁 + 控制面 + 崩溃恢复等价性。
//!
//! 全部走 [`Engine::submit`]（内嵌路径，`Durability::Always`），刻意**不经
//! 网络**：这样「语义对不对」与「传输对不对」分开失败，报错时可读。
//! 线协议与事件循环由 [`concurrency`] / `tests/crash_recovery.rs` 覆盖。
//!
//! 跑法：`cargo test --test engine_crud`

use std::path::PathBuf;
use tempfile::TempDir;
use treedb::engine::model::{
    errno, AuditRec, DefRec, KeyRec, Kind, Node, Op, Reply, RENAME_NOREPLACE, SA_DATA_REF, SA_MODE,
    SA_SIZE,
};
use treedb::engine::{now_ms, Durability, Engine, State};

// ---------------------------------------------------------------- 驱动脚手架

/// 一个数据目录 + 打开/重开它的入口。
struct Dir {
    tmp: TempDir,
}

impl Dir {
    fn new(tag: &str) -> Dir {
        let tmp = tempfile::tempdir().expect("tempdir");
        // 用例名进目录名：失败时能一眼看出是谁的现场。
        let d = tmp.path().join(tag);
        std::fs::create_dir_all(&d).unwrap();
        Dir { tmp }
    }
    fn path(&self) -> PathBuf {
        // 名字只用于报错时定位；实际数据目录统一用 tmp 下的 treedb。
        self.tmp.path().join("treedb")
    }
    fn open(&self) -> Engine {
        Engine::open(&self.path(), Durability::Always).expect("engine open")
    }
}

fn rep(e: &mut Engine, op: &Op) -> Reply {
    match e.submit(op) {
        Ok(r) => r,
        Err(err) => panic!("{op:?} → submit error: {err}"),
    }
}

/// 期望成功的 op；协议错误直接 panic 并带上 errno 名，省得每个用例手写 match。
fn ok(e: &mut Engine, op: &Op) -> Reply {
    match rep(e, op) {
        Reply::Err { code, msg } => panic!(
            "{op:?} → unexpected errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        r => r,
    }
}

fn err_code(e: &mut Engine, op: &Op) -> u16 {
    match rep(e, op) {
        Reply::Err { code, .. } => code,
        other => panic!("{op:?} → expected Reply::Err, got {other:?}"),
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
            assert_eq!(root, 1, "view root must be ino 1");
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

fn getattr(e: &mut Engine, view: u32, ino: u64) -> Node {
    node(ok(e, &Op::Getattr { view, ino }))
}

/// 内容对象键：引擎侧是一串**不透明字节**（`DataRef = Vec<u8>`）。测试里用这个
/// 函数把逻辑编号折成键，形状对齐 grovefs 后端真正存进 `node.data_ref` 的那种
/// 字符串键（`<size>:<bytes>:<dev>` 一类），而不是又回到一个数字。
fn blk(id: u64) -> Vec<u8> {
    format!("1:4096:dev{id}").into_bytes()
}

/// 只带一个 valid 位的 Setattr，其余字段全零。
fn setattr(view: u32, ino: u64, valid: u32) -> Op {
    Op::Setattr {
        view,
        ino,
        valid,
        mode: 0,
        uid: 0,
        gid: 0,
        size: 0,
        atime: 0,
        mtime: 0,
        data_ref: Vec::new(),
        ref_bytes: 0,
    }
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

fn refcount(e: &mut Engine, d: u64) -> (u64, u64) {
    match ok(e, &Op::RefLookup { data_ref: blk(d) }) {
        Reply::Ref { count, bytes } => (count, bytes),
        other => panic!("expected Reply::Ref, got {other:?}"),
    }
}

fn dir_names(e: &mut Engine, view: u32, parent: u64, offset: u64, limit: u32) -> (Vec<String>, u64, bool) {
    match ok(e, &Op::ReadDir { view, parent, offset, limit }) {
        Reply::Dirents { names, next, end, .. } => (
            names.iter().map(|n| String::from_utf8_lossy(n).into_owned()).collect(),
            next,
            end,
        ),
        other => panic!("expected Reply::Dirents, got {other:?}"),
    }
}

fn key(id: &str, hash: &str) -> KeyRec {
    KeyRec {
        id: id.into(),
        label: format!("label-{id}").into(),
        secret_hash: hash.into(),
        created_at: 0,
        last_used_at: 0,
        revoked_at: 0,
        view: b"team".to_vec(),
        ro: false,
        owner: b"alice".to_vec(),
        note: Vec::new(),
        expires_at: 0,
        created_by: Vec::new(),
        secret_hint: Vec::new(),
    }
}

// ---------------------------------------------------------------- 视图生命周期

#[test]
fn view_lifecycle_and_id_monotonicity() {
    let d = Dir::new("views");
    let mut e = d.open();

    let a = mk_view(&mut e, "alpha");
    let b = mk_view(&mut e, "beta");
    assert_ne!(a, b, "ids must be unique");
    assert_eq!(err_code(&mut e, &Op::CreateView { name: b"alpha".to_vec() }), errno::EEXIST);

    match ok(&mut e, &Op::ListViews) {
        Reply::Views(v) => {
            assert_eq!(v.len(), 2);
            assert_eq!(v[0].0, b"alpha");
            assert_eq!(v[1].0, b"beta");
        }
        other => panic!("{other:?}"),
    }

    // 视图里放东西，drop 必须级联清掉 node/dentry（否则是内存泄漏）。
    let _f = mk_file(&mut e, b, 1, "x");
    assert_eq!(ok(&mut e, &Op::DropView { name: b"beta".to_vec() }), Reply::Ok);
    assert_eq!(err_code(&mut e, &Op::DropView { name: b"beta".to_vec() }), errno::ENODEV);
    assert_eq!(err_code(&mut e, &Op::Getattr { view: b, ino: 1 }), errno::ENODEV);
    assert_eq!(
        err_code(&mut e, &Op::Lookup { view: b, parent: 1, name: b"x".to_vec() }),
        errno::ENODEV
    );

    let stats = match ok(&mut e, &Op::Stats) {
        Reply::Stats(s) => s,
        other => panic!("{other:?}"),
    };
    assert_eq!((stats.views, stats.nodes, stats.dentries), (1, 1, 0), "purge must cascade");

    // id 不复用：已释放视图的 id 不能再被发出去，否则旧句柄会写进新视图。
    let c = mk_view(&mut e, "gamma");
    assert!(c > b, "view ids must be monotonic, got {c} after {b}");
}

#[test]
fn view_name_validation_and_isolated_namespaces() {
    let d = Dir::new("namespaces");
    let mut e = d.open();
    let a = mk_view(&mut e, "a");
    let b = mk_view(&mut e, "b");

    assert_eq!(err_code(&mut e, &Op::CreateView { name: Vec::new() }), errno::ENOENT);
    assert_eq!(
        err_code(&mut e, &Op::CreateView { name: vec![b'a'; 256] }),
        errno::ENAMETOOLONG
    );

    // 同名文件在两个视图里互不干扰：dentry key 带 view，namespace 天然隔离。
    let fa = mk_file(&mut e, a, 1, "same");
    let fb = mk_file(&mut e, b, 1, "same");
    assert_eq!(fa.name, fb.name);
    ok(&mut e, &Op::Unlink { view: a, parent: 1, name: b"same".to_vec() });
    assert_eq!(err_code(&mut e, &Op::Lookup { view: a, parent: 1, name: b"same".to_vec() }), errno::ENOENT);
    assert!(matches!(
        ok(&mut e, &Op::Lookup { view: b, parent: 1, name: b"same".to_vec() }),
        Reply::Node(_)
    ));
}

// ---------------------------------------------------------------- 目录树 CRUD

#[test]
fn create_lookup_getattr_roundtrip() {
    let d = Dir::new("crud");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");

    let f = mk_file(&mut e, v, 1, "f");
    assert_eq!((f.ino, f.parent, f.kind, f.mode, f.uid, f.nlink), (2, 1, Kind::File, 0o644, 1000, 1));
    assert!(f.ctime > 0 && f.atime == f.ctime);

    // ino 视图内单调不复用：删了再建必须拿到新号（FUSE 句柄靠它区分）。
    ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"f".to_vec() });
    let f2 = mk_file(&mut e, v, 1, "f");
    assert!(f2.ino > f.ino, "ino reused: {} then {}", f.ino, f2.ino);

    // lookup 与 getattr 必须给出同一个 inode。
    let byname = node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"f".to_vec() }));
    assert_eq!(byname, f2);
    assert_eq!(getattr(&mut e, v, f2.ino), f2);

    // 模式位可改（chmod 走纯 SA_MODE，不动 size/data_ref）。
    let mut op = setattr(v, f2.ino, SA_MODE);
    if let Op::Setattr { mode, .. } = &mut op {
        *mode = 0o600;
    }
    assert_eq!(node(ok(&mut e, &op)).mode, 0o600);

    assert_eq!(err_code(&mut e, &Op::Getattr { view: v, ino: 9999 }), errno::ENOENT);
    assert_eq!(
        err_code(&mut e, &Op::Lookup { view: v, parent: 9999, name: b"x".to_vec() }),
        errno::ENOENT
    );
    // 名字里有 '/' 是协议级错误，不能让它进到 dentry key 里造出伪层级。
    assert_eq!(
        err_code(&mut e, &Op::Lookup { view: v, parent: 1, name: b"a/b".to_vec() }),
        errno::EINVAL
    );
}

#[test]
fn mkdir_rmdir_and_nlink_rules() {
    let d = Dir::new("mkdir");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");

    let root = getattr(&mut e, v, 1);
    assert_eq!((root.kind, root.nlink), (Kind::Dir, 2));

    let dir = mk_dir(&mut e, v, 1, "sub");
    assert_eq!(dir.nlink, 2, "新目录 nlink=2（. 与父项）");
    assert_eq!(getattr(&mut e, v, 1).nlink, 3, "父目录因 . 计数 +1");

    // 目录里再开目录，逐层验证 nlink。
    let inner = mk_dir(&mut e, v, dir.ino, "inner");
    assert_eq!(getattr(&mut e, v, dir.ino).nlink, 3);

    // Create 与 Mkdir 撞名一律 EEXIST，且不破坏已有项。
    assert_eq!(
        err_code(
            &mut e,
            &Op::Create { view: v, parent: 1, name: b"sub".to_vec(), mode: 0o644, uid: 0, gid: 0 }
        ),
        errno::EEXIST
    );
    assert_eq!(getattr(&mut e, v, dir.ino).nlink, 3, "撞名失败不能改动画像");

    // unlink 打目录 → EISDIR；rmdir 打文件 → ENOTDIR；rmdir 非空 → ENOTEMPTY。
    assert_eq!(err_code(&mut e, &Op::Unlink { view: v, parent: 1, name: b"sub".to_vec() }), errno::EISDIR);
    let f = mk_file(&mut e, v, 1, "f");
    assert_eq!(err_code(&mut e, &Op::Rmdir { view: v, parent: 1, name: b"f".to_vec() }), errno::ENOTDIR);
    assert_eq!(err_code(&mut e, &Op::Rmdir { view: v, parent: 1, name: b"sub".to_vec() }), errno::ENOTEMPTY);
    // '.' / '..' 不是真实 dentry（引擎不存合成项），必须 ENOENT 而不是碰到根。
    assert_eq!(err_code(&mut e, &Op::Rmdir { view: v, parent: 1, name: b".".to_vec() }), errno::ENOENT);
    assert_eq!(err_code(&mut e, &Op::Rmdir { view: v, parent: dir.ino, name: b"..".to_vec() }), errno::ENOENT);

    // 清空后 rmdir 成功，父目录 nlink 回落。
    ok(&mut e, &Op::Rmdir { view: v, parent: dir.ino, name: b"inner".to_vec() });
    assert_eq!(getattr(&mut e, v, dir.ino).nlink, 2);
    ok(&mut e, &Op::Rmdir { view: v, parent: 1, name: b"sub".to_vec() });
    assert_eq!(getattr(&mut e, v, 1).nlink, 2);
    assert_eq!(err_code(&mut e, &Op::Getattr { view: v, ino: inner.ino }), errno::ENOENT);
    assert_eq!(err_code(&mut e, &Op::Getattr { view: v, ino: dir.ino }), errno::ENOENT);
    assert_eq!(err_code(&mut e, &Op::Unlink { view: v, parent: 1, name: b"gone".to_vec() }), errno::ENOENT);
    let _ = f;
}

#[test]
fn view_root_cannot_be_reached_or_removed() {
    let d = Dir::new("rootbusy");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let dir = mk_dir(&mut e, v, 1, "d");
    ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"d".to_vec(), new_parent: 1, new_name: b"d2".to_vec(), flags: 0});
    // 根没有指向自己的 dentry，所以任何删除/改名路径都只能以 ENOENT 结束；
    // Unlink/Rmdir/Rename 里的 EBUSY 守卫是防「未来加了硬链接后误删根」。
    assert_eq!(err_code(&mut e, &Op::Rmdir { view: v, parent: dir.ino, name: b"..".to_vec() }), errno::ENOENT);
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"..".to_vec(), new_parent: 1, new_name: b"x".to_vec(), flags: 0}),
        errno::ENOENT
    );
    assert_eq!(getattr(&mut e, v, 1).kind, Kind::Dir, "根必须还在");
    assert_eq!(getattr(&mut e, v, 1).nlink, 3, "根 2 + 一个子目录 d2");
    assert_eq!(dir_names(&mut e, v, 1, 0, 10).0, vec!["d2"].to_vec(), "目录里只剩改名后的 d2");
}

#[test]
fn symlink_readlink_and_posix_override() {
    let d = Dir::new("symlink");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let t = mk_file(&mut e, v, 1, "target");

    let sl = node(ok(
        &mut e,
        &Op::Symlink {
            view: v,
            parent: 1,
            name: b"link".to_vec(),
            target: b"/target".to_vec(),
            uid: 1000,
            gid: 1000,
        },
    ));
    assert_eq!(sl.kind, Kind::Symlink);
    assert_eq!(sl.mode, 0o777, "symlink 固定 0777");
    assert_eq!(sl.link, b"/target");
    assert_eq!(sl.size, 7, "symlink 的 size = 目标串长度");

    match ok(&mut e, &Op::Readlink { view: v, ino: sl.ino }) {
        Reply::Node(n) => assert_eq!(n.link, b"/target"),
        other => panic!("{other:?}"),
    }
    // Readlink 打非 symlink → EINVAL（POSIX 里 readlink 只对链接合法）。
    assert_eq!(err_code(&mut e, &Op::Readlink { view: v, ino: t.ino }), errno::EINVAL);

    // POSIX：symlink() 覆盖同名旧项，旧文件的 refs 要跟着减。
    ok(&mut e, &commit_ref(v, t.ino, 500, 4096));
    assert_eq!(refcount(&mut e, 500), (1, 4096));
    let sl2 = node(ok(
        &mut e,
        &Op::Symlink {
            view: v,
            parent: 1,
            name: b"target".to_vec(),
            target: b"elsewhere".to_vec(),
            uid: 0,
            gid: 0,
        },
    ));
    assert!(sl2.ino > t.ino, "覆盖 = 换 inode，旧 inode 必须消失");
    assert_eq!(err_code(&mut e, &Op::Getattr { view: v, ino: t.ino }), errno::ENOENT);
    assert_eq!(refcount(&mut e, 500), (0, 0), "被覆盖文件的引用要归还");

    // 目标是目录时不覆盖：空 → EISDIR，非空 → ENOTEMPTY。
    let dir = mk_dir(&mut e, v, 1, "d");
    assert_eq!(
        err_code(
            &mut e,
            &Op::Symlink { view: v, parent: 1, name: b"d".to_vec(), target: b"x".to_vec(), uid: 0, gid: 0 }
        ),
        errno::EISDIR
    );
    mk_file(&mut e, v, dir.ino, "inside");
    assert_eq!(
        err_code(
            &mut e,
            &Op::Symlink { view: v, parent: 1, name: b"d".to_vec(), target: b"x".to_vec(), uid: 0, gid: 0 }
        ),
        errno::ENOTEMPTY
    );
}

#[test]
fn readdir_is_byte_ordered_and_pages_stably() {
    let d = Dir::new("readdir");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    for n in ["b", "a2", "a10", "A", "_z"] {
        mk_file(&mut e, v, 1, n);
    }
    // BTreeMap key 字典序 = POSIX readdir 的稳定序；'A'(0x41) < '_'(0x5f) < 'a' < 'b'。
    let (all, next, end) = dir_names(&mut e, v, 1, 0, 0);
    assert_eq!(all, vec!["A", "_z", "a10", "a2", "b"]);
    assert_eq!((next, end), (5, true));

    // 分页：offset 是「已返回条数」，逐页拼起来必须等于整目录，不重不漏。
    let mut paged: Vec<String> = Vec::new();
    let mut off = 0u64;
    for _ in 0..10 {
        let (names, next, end) = dir_names(&mut e, v, 1, off, 2);
        paged.extend(names);
        off = next;
        if end {
            break;
        }
    }
    assert_eq!(paged, all);

    // 非目录 → ENOTDIR；空目录 → 空页且 end。
    let some_file = 2u64; // 第一个创建的是 "b"
    assert_eq!(err_code(&mut e, &Op::ReadDir { view: v, parent: some_file, offset: 0, limit: 10 }), errno::ENOTDIR);
    let dir = mk_dir(&mut e, v, 1, "d");
    let (names, _, end) = dir_names(&mut e, v, dir.ino, 0, 10);
    assert!(names.is_empty() && end);
    // 视图不存在 → ENODEV，不能伪装成空目录。
    assert_eq!(err_code(&mut e, &Op::ReadDir { view: 999, parent: 1, offset: 0, limit: 10 }), errno::ENODEV);
}

// ---------------------------------------------------------------- rename

#[test]
fn rename_moves_and_overwrites() {
    let d = Dir::new("rename");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let src = mk_file(&mut e, v, 1, "src");
    let dst_dir = mk_dir(&mut e, v, 1, "dst");
    ok(&mut e, &commit_ref(v, src.ino, 700, 4096));

    // 跨目录改名：ino 不变，parent/name 变，旧名字消失。
    ok(&mut e, &Op::Rename {
        view: v,
        old_parent: 1,
        old_name: b"src".to_vec(),
        new_parent: dst_dir.ino,
        new_name: b"moved".to_vec(),
        flags: 0,
    });
    assert_eq!(err_code(&mut e, &Op::Lookup { view: v, parent: 1, name: b"src".to_vec() }), errno::ENOENT);
    let moved = node(ok(&mut e, &Op::Lookup { view: v, parent: dst_dir.ino, name: b"moved".to_vec() }));
    assert_eq!(moved.ino, src.ino, "rename 必须保持 ino");
    assert_eq!((moved.parent, moved.name.as_slice()), (dst_dir.ino, b"moved".as_slice()));
    assert_eq!(refcount(&mut e, 700), (1, 4096), "改名不动引用账本");

    // rename 到已存在的文件 = 原子替换，旧目标 inode 与引用一起消失。
    let other = mk_file(&mut e, v, 1, "other");
    ok(&mut e, &commit_ref(v, other.ino, 701, 1024));
    ok(&mut e, &Op::Rename {
        view: v,
        old_parent: dst_dir.ino,
        old_name: b"moved".to_vec(),
        new_parent: 1,
        new_name: b"other".to_vec(),
        flags: 0,
    });
    assert_eq!(err_code(&mut e, &Op::Getattr { view: v, ino: other.ino }), errno::ENOENT);
    assert_eq!(refcount(&mut e, 701), (0, 0));
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"other".to_vec() })).ino, src.ino);

    // 目录改名跨父目录时两边的 nlink 各自 ±1。
    let a = mk_dir(&mut e, v, 1, "a");
    let b = mk_dir(&mut e, v, 1, "b");
    // 根下现有 dst / a / b 三个子目录 → nlink = 2 + 3。
    assert_eq!((getattr(&mut e, v, 1).nlink, getattr(&mut e, v, a.ino).nlink), (5, 2));
    ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"a".to_vec(), new_parent: b.ino, new_name: b"a".to_vec(), flags: 0});
    assert_eq!(getattr(&mut e, v, 1).nlink, 4, "旧父目录 -1（5 = 2 + dst/a/b → 4 = 2 + dst/b）");
    assert_eq!(getattr(&mut e, v, b.ino).nlink, 3, "新父目录 +1");
    assert_eq!(getattr(&mut e, v, a.ino).parent, b.ino);
}

#[test]
fn unlink_and_rename_report_the_last_holder_key() {
    // GC 线索必须由引擎在同一个 op 里给出。grovefs 侧 unlink 的返回值直接驱动
    // `backend.remove(key)`：若改成客户端 Unlink 之后再 RefLookup 一次判断「还有
    // 没有别人持有」，中间那个窗口里别人可以重新引用同一个键，于是还在用的
    // 字节被删掉。
    let d = Dir::new("gc-hint");
    let mut e = d.open();
    let v = mk_view(&mut e, "gc");
    let a = mk_file(&mut e, v, 1, "a");
    let b = mk_file(&mut e, v, 1, "b");
    let keeper = mk_file(&mut e, v, 1, "keeper");

    // a、b 共用 900 号对象，keeper 独占 901。
    ok(&mut e, &commit_ref(v, a.ino, 900, 1024));
    ok(&mut e, &commit_ref(v, b.ino, 900, 1024));
    ok(&mut e, &commit_ref(v, keeper.ino, 901, 1024));
    assert_eq!(refcount(&mut e, 900), (2, 2048));

    // 还有别人持有 → 空串，调用方不许去删字节。
    assert_eq!(
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"a".to_vec() }),
        Reply::NodeRef(Vec::new())
    );
    assert_eq!(refcount(&mut e, 900), (1, 1024));

    // 最后一个持有者离开 → 键原样带回。
    assert_eq!(
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"b".to_vec() }),
        Reply::NodeRef(blk(900))
    );
    assert_eq!(refcount(&mut e, 900), (0, 0));

    // 没有内容的文件（data_ref 为空） unlink 回空串，不会把空串当成一个键。
    let empty = mk_file(&mut e, v, 1, "empty");
    let _ = empty;
    assert_eq!(
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"empty".to_vec() }),
        Reply::NodeRef(Vec::new())
    );

    // rename 覆盖目标：被覆盖对象的键走同一条通道回来；没覆盖任何东西时是空串。
    let over = mk_file(&mut e, v, 1, "over");
    ok(&mut e, &commit_ref(v, over.ino, 902, 1024));
    assert_eq!(
        ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"keeper".to_vec(), new_parent: 1, new_name: b"over".to_vec(), flags: 0 }),
        Reply::NodeRef(blk(902))
    );
    assert_eq!(refcount(&mut e, 902), (0, 0));
    assert_eq!(
        ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"over".to_vec(), new_parent: 1, new_name: b"final".to_vec(), flags: 0 }),
        Reply::NodeRef(Vec::new())
    );
    // 搬走自己不算丢弃内容：901 还在，由改名后的 inode 持有。
    assert_eq!(refcount(&mut e, 901), (1, 1024));

    // NOREPLACE：目标已存在必须失败，而且不能动现场。
    let x = mk_file(&mut e, v, 1, "x");
    let y = mk_file(&mut e, v, 1, "y");
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"x".to_vec(), new_parent: 1, new_name: b"y".to_vec(), flags: RENAME_NOREPLACE }),
        errno::EEXIST
    );
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"x".to_vec() })).ino, x.ino);
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"y".to_vec() })).ino, y.ino);
    // 目标不存在时 NOREPLACE 正常成功。
    ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"x".to_vec(), new_parent: 1, new_name: b"z".to_vec(), flags: RENAME_NOREPLACE });
    assert_eq!(err_code(&mut e, &Op::Lookup { view: v, parent: 1, name: b"x".to_vec() }), errno::ENOENT);
}

#[test]
fn rename_rejects_cycles_and_type_mismatches() {
    let d = Dir::new("rename_bad");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let outer = mk_dir(&mut e, v, 1, "outer");
    let inner = mk_dir(&mut e, v, outer.ino, "inner");
    let leaf = mk_dir(&mut e, v, inner.ino, "leaf");
    let f = mk_file(&mut e, v, 1, "f");
    let _ = leaf;

    // 目录进自身 / 进自己的子树 → EINVAL（否则目录图成环，遍历永不停）。
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"outer".to_vec(), new_parent: outer.ino, new_name: b"x".to_vec(), flags: 0}),
        errno::EINVAL
    );
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"outer".to_vec(), new_parent: inner.ino, new_name: b"x".to_vec(), flags: 0}),
        errno::EINVAL
    );
    // 自身改自身：POSIX 规定 no-op 成功，不是 EEXIST/EINVAL；GC 线索为空。
    assert_eq!(
        ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"outer".to_vec(), new_parent: 1, new_name: b"outer".to_vec(), flags: 0}),
        Reply::NodeRef(Vec::new())
    );
    // 文件改名进目录位置 → ENOTDIR；目录改名占文件位置 → EISDIR。
    let dir2 = mk_dir(&mut e, v, 1, "dir2");
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"dir2".to_vec(), new_parent: 1, new_name: b"f".to_vec(), flags: 0}),
        errno::EISDIR
    );
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"f".to_vec(), new_parent: 1, new_name: b"dir2".to_vec(), flags: 0}),
        errno::ENOTDIR
    );
    assert!(matches!(ok(&mut e, &Op::Getattr { view: v, ino: dir2.ino }), Reply::Node(_)));

    // 源不存在 / 父不是目录 / 根不可改名 / 视图不存在。
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"nope".to_vec(), new_parent: 1, new_name: b"x".to_vec(), flags: 0}),
        errno::ENOENT
    );
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: f.ino, old_name: b"x".to_vec(), new_parent: 1, new_name: b"y".to_vec(), flags: 0}),
        errno::ENOTDIR,
        "源父必须是目录"
    );
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"f".to_vec(), new_parent: f.ino, new_name: b"y".to_vec(), flags: 0}),
        errno::ENOTDIR,
        "目标父必须是目录"
    );
    assert_eq!(
        err_code(&mut e, &Op::Rename { view: 4242, old_parent: 1, old_name: b"f".to_vec(), new_parent: 1, new_name: b"y".to_vec(), flags: 0}),
        errno::ENODEV
    );
}

// ---------------------------------------------------------------- refs 账本

#[test]
fn data_ref_ledger_keeps_exact_byte_invariant() {
    let d = Dir::new("refs");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let a = mk_file(&mut e, v, 1, "a");
    let b = mk_file(&mut e, v, 1, "b");

    // 未登记的对象查出来是零行，而不是 ENOENT（GC 要能无脑查）。
    assert_eq!(refcount(&mut e, 100), (0, 0));

    ok(&mut e, &commit_ref(v, a.ino, 100, 4096));
    assert_eq!((getattr(&mut e, v, a.ino).data_ref, getattr(&mut e, v, a.ino).size), (blk(100), 4096));
    ok(&mut e, &commit_ref(v, b.ino, 100, 8192));
    // 不变量：bytes == Σ 持有者 size。
    assert_eq!(refcount(&mut e, 100), (2, 12288));

    // 只改 size 不动 ref：账本跟着补差。
    ok(&mut e, &Op::Setattr {
        view: v,
        ino: a.ino,
        valid: SA_SIZE,
        mode: 0,
        uid: 0,
        gid: 0,
        size: 1024,
        atime: 0,
        mtime: 0,
        data_ref: Vec::new(),
        ref_bytes: 0,
    });
    assert_eq!(refcount(&mut e, 100), (2, 9216), "12288 - 4096 + 1024");

    // 换对象：旧行减（按切换前 size），新行加。
    ok(&mut e, &commit_ref(v, a.ino, 200, 0));
    assert_eq!(refcount(&mut e, 100), (1, 8192));
    assert_eq!(refcount(&mut e, 200), (1, 1024));

    // 同一对象重复提交是 no-op，不能把 count 加两次。
    ok(&mut e, &commit_ref(v, a.ino, 200, 0));
    assert_eq!(refcount(&mut e, 200), (1, 1024));

    // 释放：把 ref 换成**空字节串**（引擎侧「空 = 未提交内容」），等价于减引用。
    // 写成字面 Setattr 而不走 `commit_ref`：后者的编号参数会被 `blk()` 折成一个
    // 真键，0 号也是对象，不是“没有对象”。
    ok(&mut e, &Op::Setattr {
        view: v,
        ino: a.ino,
        valid: SA_DATA_REF,
        mode: 0,
        uid: 0,
        gid: 0,
        size: 0,
        atime: 0,
        mtime: 0,
        data_ref: Vec::new(),
        ref_bytes: 0,
    });
    assert_eq!(refcount(&mut e, 200), (0, 0));
    assert!(getattr(&mut e, v, a.ino).data_ref.is_empty());

    // unlink 归还持有；归零必须删行（否则 stats.refs 只涨不落）。
    ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"b".to_vec() });
    assert_eq!(refcount(&mut e, 100), (0, 0));
    let stats = match ok(&mut e, &Op::Stats) {
        Reply::Stats(s) => s,
        other => panic!("{other:?}"),
    };
    assert_eq!(stats.refs, 0, "空账本行不能留在表里");

    // 管理面显式增减（GC 纠正历史估算用）。
    match ok(&mut e, &Op::RefUp { data_ref: blk(300), bytes: 7 }) {
        Reply::Ref { count, bytes } => assert_eq!((count, bytes), (1, 7)),
        other => panic!("{other:?}"),
    }
    // count 归零 = 整行作废：回包里的 bytes 也必须归零，不能给一个查不到的幻影值。
    match ok(&mut e, &Op::RefDown { data_ref: blk(300), bytes: 3 }) {
        Reply::Ref { count, bytes } => assert_eq!((count, bytes), (0, 0)),
        other => panic!("{other:?}"),
    }
    assert_eq!(refcount(&mut e, 300), (0, 0));
    // 行已经不存在，再减只能停在 0，不能减出负数。
    match ok(&mut e, &Op::RefDown { data_ref: blk(300), bytes: 99 }) {
        Reply::Ref { count, bytes } => assert_eq!((count, bytes), (0, 0)),
        other => panic!("{other:?}"),
    }

    // 一个 op 里同时改 size 与换 ref：同一行被碰两次，必须汇总后只写一行
    // （日志存的是整行绝对值，逐笔产出会互相覆盖）。
    let c = mk_file(&mut e, v, 1, "c");
    ok(&mut e, &commit_ref(v, c.ino, 600, 1000));
    assert_eq!(refcount(&mut e, 600), (1, 1000));
    ok(&mut e, &Op::Setattr {
        view: v,
        ino: c.ino,
        valid: SA_SIZE | SA_DATA_REF,
        mode: 0,
        uid: 0,
        gid: 0,
        size: 250,
        atime: 0,
        mtime: 0,
        data_ref: blk(601),
        ref_bytes: 0,
    });
    assert_eq!(refcount(&mut e, 600), (0, 0), "旧对象减账按切换前的 size");
    assert_eq!(refcount(&mut e, 601), (1, 250), "新对象按新 size 记账");
    assert_eq!(getattr(&mut e, v, c.ino).size, 250);
}

#[test]
fn setattr_on_missing_inode_and_dir_size() {
    let d = Dir::new("setattr");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let dir = mk_dir(&mut e, v, 1, "d");
    assert_eq!(
        err_code(&mut e, &setattr(99, 1, SA_MODE)),
        errno::ENODEV,
        "视图整个没了要回 ENODEV：FUSE 需要区分「视图被删」与「文件不见了」"
    );
    assert_eq!(err_code(&mut e, &setattr(v, 4242, SA_MODE)), errno::ENOENT);
    // 目录不能 truncate：EISDIR 是 grovefs 现有路径的行为，不能变。
    assert_eq!(err_code(&mut e, &setattr(v, dir.ino, SA_SIZE)), errno::EISDIR);
    // size=0 的显式 truncate 不报错，把文件清零。
    let f = mk_file(&mut e, v, 1, "f");
    ok(&mut e, &commit_ref(v, f.ino, 400, 4096));
    ok(&mut e, &Op::Setattr {
        view: v,
        ino: f.ino,
        valid: SA_SIZE,
        mode: 0,
        uid: 0,
        gid: 0,
        size: 0,
        atime: 0,
        mtime: 0,
        data_ref: Vec::new(),
        ref_bytes: 0,
    });
    assert_eq!(getattr(&mut e, v, f.ino).size, 0);
    assert_eq!(refcount(&mut e, 400), (1, 0), "truncate 到 0 后仍持有，但字节额为 0");
}

// ---------------------------------------------------------------- 锁

#[test]
fn locks_are_owner_scoped_and_lease_filtered() {
    let d = Dir::new("locks");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let f = mk_file(&mut e, v, 1, "f");

    assert_eq!(
        err_code(&mut e, &Op::LockSet { view: v, ino: 4242, owner: b"o".to_vec(), verb: 0, kind: 1, lease_expiry: 0 }),
        errno::ENOENT,
        "不能给不存在的 inode 上锁"
    );

    // 两个 owner 各自持锁，查询按 owner 给出 kind/lease。
    ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"a".to_vec(), verb: 0, kind: 1, lease_expiry: 0 });
    ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"b".to_vec(), verb: 0, kind: 2, lease_expiry: now_ms() + 60_000 });
    match ok(&mut e, &Op::LockQuery { view: v, ino: f.ino }) {
        Reply::Locks(l) => {
            assert_eq!(l.len(), 2);
            assert_eq!((l[0].0.as_slice(), l[0].1, l[0].2), (b"a".as_slice(), 1u8, 0i64));
            assert_eq!((l[1].0.as_slice(), l[1].1), (b"b".as_slice(), 2u8));
        }
        other => panic!("{other:?}"),
    }

    // 续约：同 owner 覆盖，不新增行。
    ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"a".to_vec(), verb: 0, kind: 0, lease_expiry: 0 });
    match ok(&mut e, &Op::LockQuery { view: v, ino: f.ino }) {
        Reply::Locks(l) => assert_eq!((l.len(), l[0].1), (2, 0), "续约是改 kind，不是加锁"),
        other => panic!("{other:?}"),
    }

    // 过期租约查不到：半开连接的锁不能永久挡住别人（grovefs 的教训）。
    ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"c".to_vec(), verb: 0, kind: 1, lease_expiry: now_ms() - 1 });
    let live: Vec<Vec<u8>> = match ok(&mut e, &Op::LockQuery { view: v, ino: f.ino }) {
        Reply::Locks(l) => l.into_iter().map(|(o, ..)| o).collect(),
        other => panic!("{other:?}"),
    };
    assert_eq!(live, vec![b"a".to_vec(), b"b".to_vec()]);

    // verb=1 释放该 owner 在此 inode 上的全部锁。
    ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"a".to_vec(), verb: 1, kind: 0, lease_expiry: 0 });
    let live: Vec<Vec<u8>> = match ok(&mut e, &Op::LockQuery { view: v, ino: f.ino }) {
        Reply::Locks(l) => l.into_iter().map(|(o, ..)| o).collect(),
        other => panic!("{other:?}"),
    };
    assert_eq!(live, vec![b"b".to_vec()]);

    // 锁按 (view,ino) 隔离，且 unlink 不遗留孤儿行。
    let v2 = mk_view(&mut e, "v2");
    let f2 = mk_file(&mut e, v2, 1, "f");
    match ok(&mut e, &Op::LockQuery { view: v2, ino: f2.ino }) {
        Reply::Locks(l) => assert!(l.is_empty(), "另一视图的锁不能串过来"),
        other => panic!("{other:?}"),
    }
    let stats_locks = |e: &mut Engine| match ok(e, &Op::Stats) {
        Reply::Stats(s) => s.locks,
        other => panic!("{other:?}"),
    };
    assert_eq!(stats_locks(&mut e), 2);
    ok(&mut e, &Op::DropView { name: b"v".to_vec() });
    assert_eq!(stats_locks(&mut e), 0, "drop view 必须连带清锁");
}

// ---------------------------------------------------------------- 控制面

fn fresh_key() -> KeyRec {
    key("k1", "hash-1")
}

#[test]
fn key_lifecycle_auth_revoke_rotate_expire() {
    let d = Dir::new("keys");
    let mut e = d.open();

    match ok(&mut e, &Op::KeyCreate { key: fresh_key() }) {
        Reply::Key(k) => assert_eq!(k.id, b"k1"),
        other => panic!("{other:?}"),
    }
    assert_eq!(err_code(&mut e, &Op::KeyCreate { key: fresh_key() }), errno::EEXIST);
    match ok(&mut e, &Op::KeyGet { id: b"k1".to_vec() }) {
        Reply::Key(k) => {
            assert_eq!(
                (k.label.as_slice(), k.ro, k.view.as_slice(), k.owner.as_slice()),
                (b"label-k1".as_slice(), false, b"team".as_slice(), b"alice".as_slice())
            );
            assert_eq!(k.revoked_at, 0);
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(err_code(&mut e, &Op::KeyGet { id: b"nope".to_vec() }), errno::ENOENT);

    // auth：口令对 → 回 label/ro/view/owner（FUSE 侧要按 view 决定挂哪个树）。
    match ok(&mut e, &Op::KeyCheck { id: b"k1".to_vec(), verb: 0, hash: b"hash-1".to_vec(), now_ms: now_ms() }) {
        Reply::Authed { label, ro, view, owner } => {
            assert_eq!(
                (label.as_slice(), ro, view.as_slice(), owner.as_slice()),
                (b"label-k1".as_slice(), false, b"team".as_slice(), b"alice".as_slice())
            );
        }
        other => panic!("{other:?}"),
    }
    // live 探测与 auth 分开：verb=1 只回 bool。
    assert_eq!(
        ok(&mut e, &Op::KeyCheck { id: b"k1".to_vec(), verb: 1, hash: b"hash-1".to_vec(), now_ms: now_ms() }),
        Reply::Live(true)
    );

    // 口令不符与 id 不存在必须同码同长，否则 id 可以被枚举出来。
    let wrong = match rep(&mut e, &Op::KeyCheck { id: b"k1".to_vec(), verb: 0, hash: b"bad".to_vec(), now_ms: now_ms() }) {
        Reply::Err { code, msg } => (code, msg),
        other => panic!("{other:?}"),
    };
    let missing = match rep(&mut e, &Op::KeyCheck { id: b"kghost".to_vec(), verb: 0, hash: b"bad".to_vec(), now_ms: now_ms() }) {
        Reply::Err { code, msg } => (code, msg),
        other => panic!("{other:?}"),
    };
    assert_eq!(wrong, missing);
    assert_eq!(wrong.0, errno::EADV);

    // revoke 后 auth 直接拒；revoke 时间戳落库可见。
    ok(&mut e, &Op::KeyUpdate { id: b"k1".to_vec(), verb: 0, ts: 111, new_hash: Vec::new(), new_hint: Vec::new(), ro: false });
    assert_eq!(err_code(&mut e, &Op::KeyCheck { id: b"k1".to_vec(), verb: 0, hash: b"hash-1".to_vec(), now_ms: now_ms() }), errno::EADV);
    match ok(&mut e, &Op::KeyGet { id: b"k1".to_vec() }) {
        Reply::Key(k) => assert_eq!(k.revoked_at, 111),
        other => panic!("{other:?}"),
    }

    // rotate：换新口令后旧 hash 失效；hint 同步更新。
    ok(&mut e, &Op::KeyUpdate { id: b"k1".to_vec(), verb: 1, ts: 0, new_hash: b"hash-2".to_vec(), new_hint: b"ab12".to_vec(), ro: false });
    assert_eq!(err_code(&mut e, &Op::KeyCheck { id: b"k1".to_vec(), verb: 0, hash: b"hash-1".to_vec(), now_ms: now_ms() }), errno::EADV);
    // 已撤销的 key 即使口令对也过不去（revoked 优先于 hash 比较）。
    assert_eq!(err_code(&mut e, &Op::KeyCheck { id: b"k1".to_vec(), verb: 0, hash: b"hash-2".to_vec(), now_ms: now_ms() }), errno::EADV);

    // 过期：expires_at 到了就拒，0 表示永不过期。
    ok(&mut e, &Op::KeyCreate {
        key: KeyRec { expires_at: 5000, ..fresh_key().with_id("k2") },
    });
    assert!(matches!(
        ok(&mut e, &Op::KeyCheck { id: b"k2".to_vec(), verb: 0, hash: b"hash-1".to_vec(), now_ms: 4999 }),
        Reply::Authed { .. }
    ));
    assert_eq!(err_code(&mut e, &Op::KeyCheck { id: b"k2".to_vec(), verb: 0, hash: b"hash-1".to_vec(), now_ms: 5000 }), errno::EADV);

    // touch 记 last_used；set_ro 翻只读位。
    ok(&mut e, &Op::KeyUpdate { id: b"k2".to_vec(), verb: 2, ts: 777, new_hash: Vec::new(), new_hint: Vec::new(), ro: false });
    ok(&mut e, &Op::KeyUpdate { id: b"k2".to_vec(), verb: 3, ts: 0, new_hash: Vec::new(), new_hint: Vec::new(), ro: true });
    match ok(&mut e, &Op::KeyGet { id: b"k2".to_vec() }) {
        Reply::Key(k) => assert_eq!((k.last_used_at, k.ro), (777, true)),
        other => panic!("{other:?}"),
    }
    assert_eq!(err_code(&mut e, &Op::KeyUpdate { id: b"k2".to_vec(), verb: 9, ts: 0, new_hash: Vec::new(), new_hint: Vec::new(), ro: false }), errno::EINVAL);
    assert_eq!(err_code(&mut e, &Op::KeyUpdate { id: b"ghost".to_vec(), verb: 0, ts: 1, new_hash: Vec::new(), new_hint: Vec::new(), ro: false }), errno::ENOENT);

    match ok(&mut e, &Op::KeyList) {
        Reply::Keys(v) => assert_eq!(v.len(), 2),
        other => panic!("{other:?}"),
    }
}

/// `key()` 的小糖：让用例能 `..fresh_key()` 复用其余字段。
trait WithId {
    fn with_id(self, id: &str) -> KeyRec;
}
impl WithId for KeyRec {
    fn with_id(mut self, id: &str) -> KeyRec {
        self.id = id.into();
        self
    }
}

#[test]
fn audit_defs_placement_roundtrip() {
    let d = Dir::new("control");
    let mut e = d.open();

    // seq=0 由引擎分配且单调；显式 seq 原样保留（导入历史数据用）。
    ok(&mut e, &Op::AuditAppend {
        row: AuditRec { seq: 0, ts: 1, key_id: b"k1".to_vec(), method: b"POST".to_vec(), tool: b"fs_ls".to_vec(), ok: true, detail: Vec::new() },
    });
    ok(&mut e, &Op::AuditAppend {
        row: AuditRec { seq: 0, ts: 2, key_id: b"k2".to_vec(), method: b"GET".to_vec(), tool: b"key_list".to_vec(), ok: false, detail: b"denied".to_vec() },
    });
    ok(&mut e, &Op::AuditAppend {
        row: AuditRec { seq: 900, ts: 3, key_id: b"k3".to_vec(), method: b"POST".to_vec(), tool: b"fs_rm".to_vec(), ok: true, detail: Vec::new() },
    });
    match ok(&mut e, &Op::AuditList { limit: 0 }) {
        Reply::Audits(v) => {
            assert_eq!(v.len(), 3);
            assert_eq!(v[0].seq, 900, "limit=0 取全量，最新在前");
            let mut auto: Vec<u64> = v.iter().map(|r| r.seq).filter(|s| *s < 900).collect();
            auto.sort_unstable();
            assert_eq!(auto, vec![1, 2], "自动 seq 必须递增不复用");
            assert_eq!(v[1].detail, b"denied");
        }
        other => panic!("{other:?}"),
    }
    match ok(&mut e, &Op::AuditList { limit: 2 }) {
        Reply::Audits(v) => assert_eq!(v.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![900, 2]),
        other => panic!("{other:?}"),
    }

    // 视图定义：按 kind 过滤，空 kind = 全量。
    for (n, k) in [("ls", b"tool".as_slice()), ("rm", b"tool"), ("intro", b"prompt")] {
        ok(&mut e, &Op::DefPut {
            def: DefRec { name: n.into(), kind: k.to_vec(), payload: format!("body-{n}").into(), updated_at: 0 },
        });
    }
    match ok(&mut e, &Op::DefList { kind: b"tool".to_vec() }) {
        Reply::Defs(v) => assert_eq!(v.len(), 2),
        other => panic!("{other:?}"),
    }
    match ok(&mut e, &Op::DefList { kind: Vec::new() }) {
        Reply::Defs(v) => assert_eq!(v.len(), 3),
        other => panic!("{other:?}"),
    }
    match ok(&mut e, &Op::DefGet { name: b"ls".to_vec() }) {
        Reply::Def(Some(x)) => {
            assert_eq!((x.payload.as_slice(), x.kind.as_slice()), (b"body-ls".as_slice(), b"tool".as_slice()));
            assert!(x.updated_at > 0, "updated_at=0 必须由引擎填当前时间");
        }
        other => panic!("{other:?}"),
    }
    // 不存在的 def 回 Def(None) 而不是 Err：读路径要能区分「没有」与「查错」。
    assert_eq!(ok(&mut e, &Op::DefGet { name: b"ghost".to_vec() }), Reply::Def(None));
    ok(&mut e, &Op::DefDel { name: b"ls".to_vec() });
    assert_eq!(ok(&mut e, &Op::DefGet { name: b"ls".to_vec() }), Reply::Def(None));
    assert_eq!(err_code(&mut e, &Op::DefDel { name: b"ls".to_vec() }), errno::ENOENT);

    // 放置表：视图名 → 实例，与视图生命周期无关（视图还没建就能先记归属）。
    assert_eq!(err_code(&mut e, &Op::PlaceGet { view: b"ghost".to_vec() }), errno::ENOENT);
    ok(&mut e, &Op::PlaceSet { view: b"ghost".to_vec(), instance: b"node-3".to_vec() });
    assert_eq!(ok(&mut e, &Op::PlaceGet { view: b"ghost".to_vec() }), Reply::Place(b"node-3".to_vec()));
    ok(&mut e, &Op::PlaceSet { view: b"other".to_vec(), instance: b"node-1".to_vec() });
    match ok(&mut e, &Op::PlaceList) {
        Reply::Places(v) => assert_eq!(v, vec![(b"ghost".to_vec(), b"node-3".to_vec()), (b"other".to_vec(), b"node-1".to_vec())]),
        other => panic!("{other:?}"),
    }
    ok(&mut e, &Op::PlaceSet { view: b"ghost".to_vec(), instance: b"node-9".to_vec() });
    assert_eq!(ok(&mut e, &Op::PlaceGet { view: b"ghost".to_vec() }), Reply::Place(b"node-9".to_vec()));
    assert_eq!(
        match ok(&mut e, &Op::PlaceList) { Reply::Places(v) => v.len(), other => panic!("{other:?}") },
        2,
        "改放置不能插出第二行"
    );
}

// ---------------------------------------------------------------- 崩溃恢复等价性

/// 造一份覆盖所有表的状态，返回 (视图 id, 文件 a 的 ino)。
fn build_state(e: &mut Engine) -> (u32, u64) {
    let v = mk_view(e, "v");
    let dir = mk_dir(e, v, 1, "d");
    let a = mk_file(e, v, 1, "a");
    let b = mk_file(e, v, dir.ino, "b");
    mk_file(e, v, 1, "c");
    ok(e, &commit_ref(v, a.ino, 100, 4096));
    ok(e, &commit_ref(v, b.ino, 100, 1024));
    ok(
        e,
        &Op::Symlink { view: v, parent: 1, name: b"l".to_vec(), target: b"a".to_vec(), uid: 1, gid: 1 },
    );
    ok(e, &Op::LockSet { view: v, ino: a.ino, owner: b"o1".to_vec(), verb: 0, kind: 1, lease_expiry: now_ms() + 10_000 });
    ok(e, &Op::KeyCreate { key: fresh_key() });
    ok(e, &Op::AuditAppend {
        row: AuditRec { seq: 0, ts: 5, key_id: b"k1".to_vec(), method: b"POST".to_vec(), tool: b"fs_ls".to_vec(), ok: true, detail: Vec::new() },
    });
    ok(e, &Op::DefPut {
        def: DefRec { name: b"ls".to_vec(), kind: b"tool".to_vec(), payload: b"x".to_vec(), updated_at: 0 },
    });
    ok(e, &Op::PlaceSet { view: b"v".to_vec(), instance: b"node-1".to_vec() });
    (v, a.ino)
}

fn assert_rich_state(s: &State, v: u32) {
    assert_eq!(s.views.len(), 1);
    // 根 + a + d + b + c + l = 6 个 inode，5 条 dentry（根没有 entry）。
    // 条数走 `dentry_count()`：`dentries` 是两层表，`.len()` 是目录数。
    assert_eq!(s.nodes.len(), 6, "nodes: {:?}", s.nodes.keys().collect::<Vec<_>>());
    assert_eq!(s.dentry_count(), 5);
    assert_eq!(s.refs.len(), 1);
    assert_eq!(s.refs[&blk(100)], treedb::engine::model::RefRec { count: 2, bytes: 5120 });
    assert_eq!(s.locks.len(), 1);
    assert_eq!(s.keys.len(), 1);
    assert_eq!(s.audit.len(), 1);
    assert_eq!(s.defs.len(), 1);
    assert_eq!(s.placement.len(), 1);
    assert!(s.view_by_id.contains_key(&v));
}

#[test]
fn reopen_replays_wal_into_identical_state() {
    let d = Dir::new("replay");
    let v;
    let a_ino;
    let before: State;
    {
        let mut e = d.open();
        let built = build_state(&mut e);
        v = built.0;
        a_ino = built.1;
        // 再来一波改名/删除，把 DelNode/DelDentry/DelLock 也压进 WAL。
        ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"a".to_vec(), new_parent: 2, new_name: b"a2".to_vec(), flags: 0});
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"c".to_vec() });
        ok(&mut e, &Op::LockSet { view: v, ino: a_ino, owner: b"o1".to_vec(), verb: 1, kind: 0, lease_expiry: 0 });
        // 建一个用完就丢的视图，让 PurgeView 也走一遍重放。
        let tmp = mk_view(&mut e, "tmp");
        mk_file(&mut e, tmp, 1, "junk");
        ok(&mut e, &Op::DropView { name: b"tmp".to_vec() });
        before = e.state_snapshot();
        assert!(before.dentry(v, 1, b"c").is_none());
        assert!(!before.views.contains_key(b"tmp".as_slice()));
    } // 这里 Engine 离开作用域就是普通 close，没有 checkpoint —— 全靠 WAL 重放。

    let mut e2 = d.open();
    assert_eq!(e2.state_snapshot(), before, "重放后的状态必须与崩溃前逐字段相等");
    // 基线 6 个 inode 减掉 unlink 的 c；dentry 5 减 1；PurgeView 不能留残留。
    assert_eq!((e2.state_snapshot().nodes.len(), e2.state_snapshot().dentry_count()), (5, 4));
    assert_eq!(e2.state_snapshot().views.len(), 1, "临时视图不能重放回来");
    assert!(e2.state_snapshot().locks.is_empty(), "释放锁必须从重放里体现");
    assert!(e2.state_snapshot().view_by_id.len() == 1, "反查表要跟着清");
    assert_eq!(
        node(ok(&mut e2, &Op::Lookup { view: v, parent: 2, name: b"a2".to_vec() })).name,
        b"a2",
        "改名必须从 WAL 里还原"
    );
    assert_eq!(err_code(&mut e2, &Op::Lookup { view: v, parent: 1, name: b"c".to_vec() }), errno::ENOENT);
    let rec = e2.recovery();
    assert!(rec.wal_records > 0, "没重放任何记录说明用例没测到东西");
    assert_eq!(rec.snap_seq, 0, "没做过 checkpoint，水位应为 0");
    assert!(rec.durable_seq > 0, "durable seq 必须对齐 WAL 尾部");
    // 重启后第一次 Stats 的 seq 不能回退，否则运维看不出 WAL 位置。
    match ok(&mut e2, &Op::Stats) {
        Reply::Stats(s) => assert_eq!(s.seq, rec.durable_seq),
        other => panic!("{other:?}"),
    }
}

#[test]
fn checkpoint_then_reopen_survives_wal_truncation() {
    let d = Dir::new("ckpt");
    let v;
    let before: State;
    {
        let mut e = d.open();
        v = build_state(&mut e).0;
        // checkpoint 会重置 WAL：此后一切状态只能来自快照。
        ok(&mut e, &Op::Checkpoint);
        assert!(e.wal_len() == 0, "checkpoint 后 WAL 必须被截断重写");
        // 快照之后再写几条，让「快照 + 增量 WAL」两段同时存在。
        mk_file(&mut e, v, 1, "after");
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"c".to_vec() });
        before = e.state_snapshot();
    }

    let mut e2 = d.open();
    assert_eq!(e2.state_snapshot(), before);
    let rec = e2.recovery();
    assert!(rec.snap_seq > 0, "快照水位必须带回来，否则会把旧记录再放一遍");
    assert!(rec.wal_records > 0, "快照之后的增量必须走重放");
    // 重启后 seq 不回退：运维靠它判断 WAL 位置。
    match ok(&mut e2, &Op::Stats) {
        Reply::Stats(s) => {
            assert!(s.seq >= rec.snap_seq);
            assert_eq!(s.snap_seq, rec.snap_seq);
        }
        other => panic!("{other:?}"),
    }
    assert_rich_state(&e2.state_snapshot(), v);
    // 快照后的增量不能因重放两次而翻倍：再开一次仍等价。
    assert_eq!(d.open().state_snapshot(), before);
}

#[test]
fn torn_wal_tail_is_discarded_not_fatal() {
    let d = Dir::new("torntail");
    let v;
    // 逻辑末尾，不等同文件长度：预分配后磁盘上还挂着一大片没写过的零。
    let good;
    {
        let mut e = d.open();
        v = mk_view(&mut e, "v");
        mk_file(&mut e, v, 1, "durable1");
        mk_file(&mut e, v, 1, "durable2");
        good = e.wal_len();
        assert!(good > 0);
    }
    // 模拟最后一条只写了半路就掉电：完整头 + 真实长度，但 CRC 不对。
    // （只截头的场景由 lib 层 `replay_skips_torn_tail` 覆盖，这里验另一条分支。）
    let wal = d.path().join("treedb.wal");
    let junk: Vec<u8> = [
        9999u64.to_le_bytes().as_slice(),
        0xdead_beefu32.to_le_bytes().as_slice(),
        4u32.to_le_bytes().as_slice(),
        0u32.to_le_bytes().as_slice(),
        b"junk".as_slice(),
    ]
    .concat();
    {
        use std::io::{Seek, SeekFrom, Write};
        // 不能 `append`：O_APPEND 会把垃圾送到预分配空洞末尾，扫描根本走不到
        // 那里，这个测就只在测「什么都没发生」。
        let mut f = std::fs::OpenOptions::new().write(true).open(&wal).unwrap();
        f.seek(SeekFrom::Start(good)).unwrap();
        f.write_all(&junk).unwrap();
        f.sync_all().unwrap();
    }

    let mut e = d.open();
    // 半条记录被截掉，之前 ack 的写一条不少。
    assert_eq!(e.wal_len(), good, "open 必须把坏尾截掉");
    let names = dir_names(&mut e, v, 1, 0, 10).0;
    assert_eq!(names, vec!["durable1", "durable2"]);
    // 截尾之后还能继续写，seq 不与坏尾重叠。
    mk_file(&mut e, v, 1, "post");
    assert_eq!(dir_names(&mut e, v, 1, 0, 10).0, vec!["durable1", "durable2", "post"]);
    assert!(e.wal_len() > good, "截尾后新记录必须真的落在逻辑末尾之后");
    let before = e.state_snapshot();
    drop(e);
    assert_eq!(d.open().state_snapshot(), before, "截尾后的引擎状态仍可完整恢复");
}

#[test]
fn dropped_view_id_is_never_reused_across_reopen() {
    let d = Dir::new("viewid");
    let first;
    {
        let mut e = d.open();
        first = mk_view(&mut e, "a");
        mk_view(&mut e, "b");
        ok(&mut e, &Op::DropView { name: b"b".to_vec() });
        ok(&mut e, &Op::Checkpoint);
    }
    // 此时 WAL 被 checkpoint 截成空文件：开不起来是回归级 bug。
    let mut e = d.open();
    let c = mk_view(&mut e, "c");
    assert_ne!(c, first);
    assert!(c > first, "next_view_id 必须随快照持久化，重启后不能从 0 重新发票");

    // 接上：空 WAL 上重新发号必须从快照水位之后接着走，否则这些记录
    // 会被下一次恢复当成「快照已覆盖」静默丢掉。
    let v = c;
    mk_file(&mut e, v, 1, "post-ckpt");
    let seq = match ok(&mut e, &Op::Stats) {
        Reply::Stats(s) => s.seq,
        other => panic!("{other:?}"),
    };
    assert!(seq > 0, "checkpoint 后的新写 seq 不能回到 0");
    let before = e.state_snapshot();
    drop(e);
    let mut e2 = d.open();
    assert_eq!(e2.state_snapshot(), before, "checkpoint 后的写同样不能丢");
    assert_eq!(dir_names(&mut e2, v, 1, 0, 10).0, vec!["post-ckpt"]);
}
