//! 引擎功能验收：POSIX 目录树语义 + refs 账本 + 锁 + 通用表 + 崩溃恢复等价性。
//!
//! 全部走 [`Engine::submit`]（内嵌路径，`Durability::Always`），刻意**不经
//! 网络**：这样「语义对不对」与「传输对不对」分开失败，报错时可读。
//! 线协议与事件循环由 [`concurrency`] / `tests/crash_recovery.rs` 覆盖。
//!
//! 跑法：`cargo test --test engine_crud`

use std::path::PathBuf;
use tempfile::TempDir;
use treedb::engine::model::{
    errno, ColumnDef, Kind, Node, NodeAttr, Op, Reply, TableDef,
    DEL_IF_EXISTS, COL_INDEX, COL_UNIQUE, COND_NONE, PUT_IF_ABSENT, PUT_IF_EXISTS, ROW_KEY_MAX,
    ROW_PK, RENAME_NOREPLACE, SA_DATA_REF, SA_GID, SA_MODE, SA_SIZE, SA_UID, TABLE_NAME_MAX,
};
use treedb::engine::{now_ms, Durability, Engine, Log, State, LOCK_SWEEP_BATCH};

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

// ---------------------------------------------------------------- 硬链接

/// `Op::Link` 的权责：两个名字一个 inode、`nlink` 记账、内容对象只在最后一个
/// 名字离开时下账。grovefs 的 `ln` 类断言在 treedb 后端从 SKIP 变 OK 靠的就是
/// 这里 —— 引擎侧唯一缺的就是「第二根 dentry」。
#[test]
fn hard_links_share_the_inode_and_keep_bytes_alive() {
    let d = Dir::new("link");
    let mut e = d.open();
    let v = mk_view(&mut e, "ln");
    let f = mk_file(&mut e, v, 1, "f");
    ok(&mut e, &commit_ref(v, f.ino, 900, 1024));

    // 第二个名字：同一个 ino、nlink 2、内容对象原样共享（不复制、不重复记账）。
    let n = node(ok(&mut e, &Op::Link { view: v, ino: f.ino, parent: 1, name: b"g".to_vec() }));
    assert_eq!((n.ino, n.nlink), (f.ino, 2));
    assert_eq!(n.data_ref, blk(900), "另一个名字共享同一份内容对象");
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"g".to_vec() })).ino, f.ino);
    assert_eq!(refcount(&mut e, 900), (1, 1024), "Link 不动引用账本");

    // 摘一个名字：inode 还在、nlink 1、字节仍被持有 —— 应答必须是空键，
    // 否则调用方会把还在用的对象当孤儿回收。
    assert_eq!(
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"f".to_vec() }),
        Reply::NodeRef(Vec::new())
    );
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 1);
    assert_eq!(refcount(&mut e, 900), (1, 1024));
    assert_eq!(
        err_code(&mut e, &Op::Lookup { view: v, parent: 1, name: b"f".to_vec() }),
        errno::ENOENT
    );

    // 最后一个名字离开时才把键带回来。
    assert_eq!(
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"g".to_vec() }),
        Reply::NodeRef(blk(900))
    );
    assert_eq!(refcount(&mut e, 900), (0, 0));

    // 重开引擎：nlink 与 dentry 都得从快照 / WAL 里回来，不是只活在内存里。
    drop(e);
    let mut e = d.open();
    let re = mk_file(&mut e, v, 1, "again");
    ok(&mut e, &Op::Link { view: v, ino: re.ino, parent: 1, name: b"again2".to_vec() });
    assert_eq!(getattr(&mut e, v, re.ino).nlink, 2, "重建后 nlink 必须是 2");
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"again2".to_vec() })).ino, re.ino);
}

/// 拒绝面逐条点名 errno：`link(2)` 没有 upsert、目录不可链接、父目录先验优先。
#[test]
fn link_refusals_name_the_posix_errno() {
    let d = Dir::new("link-err");
    let mut e = d.open();
    let v = mk_view(&mut e, "lne");
    let f = mk_file(&mut e, v, 1, "f");
    let dir = mk_dir(&mut e, v, 1, "d");
    let link = |name: &[u8], ino: u64, parent: u64| Op::Link {
        view: v,
        ino,
        parent,
        name: name.to_vec(),
    };

    assert_eq!(err_code(&mut e, &link(b"f", f.ino, 1)), errno::EEXIST, "同名就 EEXIST，含 ln f f");
    assert_eq!(err_code(&mut e, &link(b"x", dir.ino, 1)), errno::EPERM, "目录不可链接");
    assert_eq!(err_code(&mut e, &link(b"x", 9999, 1)), errno::ENOENT);
    assert_eq!(err_code(&mut e, &link(b"x", f.ino, f.ino)), errno::ENOTDIR, "父是文件");
    assert_eq!(err_code(&mut e, &link(b"x", f.ino, 9999)), errno::ENOENT, "父不存在");
    assert_eq!(err_code(&mut e, &link(b"a/b", f.ino, 1)), errno::EINVAL, "名字里的 '/' 是协议级错误");
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 1, "拒绝不许改动 inode 画像");
}

/// 覆盖「多链接 inode 的一个名字」时只减一票：另一个名字还能读，字节不下账。
/// 两条覆盖路径都要走一遍 —— `Rename` 的覆盖目标与 `Symlink` 的 upsert。
#[test]
fn overwriting_one_of_several_links_only_drops_one_vote() {
    let d = Dir::new("link-over");
    let mut e = d.open();
    let v = mk_view(&mut e, "lno");

    // f 与 g 是同一个 inode，内容对象 905 只被它持有一次。
    let f = mk_file(&mut e, v, 1, "f");
    ok(&mut e, &commit_ref(v, f.ino, 905, 4096));
    ok(&mut e, &Op::Link { view: v, ino: f.ino, parent: 1, name: b"g".to_vec() });
    assert_eq!(refcount(&mut e, 905), (1, 4096));

    // rename 覆盖 g：inode 还活着（f 这个名字还在），引用不许下账。
    let tmp = mk_file(&mut e, v, 1, "tmp");
    assert_eq!(
        ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"tmp".to_vec(), new_parent: 1, new_name: b"g".to_vec(), flags: 0 }),
        Reply::NodeRef(Vec::new())
    );
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 1);
    assert_eq!(refcount(&mut e, 905), (1, 4096), "另一个名字还指着它");
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"f".to_vec() })).ino, f.ino);
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"g".to_vec() })).ino, tmp.ino);

    // 再挂一次，然后用 symlink 的 upsert 覆盖其中一个名字。
    ok(&mut e, &Op::Link { view: v, ino: f.ino, parent: 1, name: b"h".to_vec() });
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 2);
    ok(&mut e, &Op::Symlink { view: v, parent: 1, name: b"h".to_vec(), target: b"t".to_vec(), uid: 1000, gid: 1000 });
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 1, "upsert 也只减一票");
    assert_eq!(refcount(&mut e, 905), (1, 4096));
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"h".to_vec() })).kind, Kind::Symlink);
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"f".to_vec() })).ino, f.ino);

    // 同一个 inode 的两个名字互相 rename：POSIX 要求成功且什么都不做。不挡
    // 的话，「被覆盖项只减一票」会与末尾那份按旧 nlink 写的 PutNode 打架，
    // 留下一个没有第二个名字却 nlink = 2 的 inode —— 此后它永远删不掉。
    ok(&mut e, &Op::Link { view: v, ino: f.ino, parent: 1, name: b"k".to_vec() });
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 2);
    assert_eq!(
        ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"f".to_vec(), new_parent: 1, new_name: b"k".to_vec(), flags: 0 }),
        Reply::NodeRef(Vec::new())
    );
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 2, "两个名字都在，票数不动");
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"f".to_vec() })).ino, f.ino);

    // 跨目录也是同一条：不是「先删旧名字再建新名字」。
    let sub = mk_dir(&mut e, v, 1, "sub");
    ok(&mut e, &Op::Link { view: v, ino: f.ino, parent: sub.ino, name: b"far".to_vec() });
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 3);
    assert_eq!(
        ok(&mut e, &Op::Rename { view: v, old_parent: 1, old_name: b"f".to_vec(), new_parent: sub.ino, new_name: b"far".to_vec(), flags: 0 }),
        Reply::NodeRef(Vec::new())
    );
    assert_eq!(getattr(&mut e, v, f.ino).nlink, 3);
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: 1, name: b"f".to_vec() })).ino, f.ino);
    assert_eq!(node(ok(&mut e, &Op::Lookup { view: v, parent: sub.ino, name: b"far".to_vec() })).ino, f.ino);

    // 三个名字逐个离开：前两个不下账，最后一个才把键带回来。
    assert_eq!(
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"f".to_vec() }),
        Reply::NodeRef(Vec::new())
    );
    assert_eq!(
        ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"k".to_vec() }),
        Reply::NodeRef(Vec::new())
    );
    assert_eq!(refcount(&mut e, 905), (1, 4096));
    assert_eq!(
        ok(&mut e, &Op::Unlink { view: v, parent: sub.ino, name: b"far".to_vec() }),
        Reply::NodeRef(blk(905))
    );
    assert_eq!(refcount(&mut e, 905), (0, 0));
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

/// 过期租约从「查询时被过滤」走到「真的被删掉」（§8 第 17 条）。
///
/// 读路径不能删（读 op 不许产生日志），所以回收只能由提交线程的周期
/// 任务发 `Log::DelLock`。这里验的是引擎那一半：只删过期的，不碰
/// `lease_expiry == 0`（不过期）与还没到期的行，而且删掉的写真的进了
/// WAL —— 重开后不会又看见它。
#[test]
fn expired_leases_are_reclaimed_and_stay_gone_after_restart() {
    let d = Dir::new("locksweep");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let f = mk_file(&mut e, v, 1, "f");

    ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"forever".to_vec(), verb: 0, kind: 1, lease_expiry: 0 });
    ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"live".to_vec(), verb: 0, kind: 1, lease_expiry: now_ms() + 60_000 });
    ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"gone".to_vec(), verb: 0, kind: 1, lease_expiry: now_ms() - 1 });

    let stats_locks = |e: &mut Engine| match ok(e, &Op::Stats) {
        Reply::Stats(s) => s.locks,
        other => panic!("{other:?}"),
    };
    assert_eq!(
        stats_locks(&mut e),
        3,
        "过期租约在查询时被过滤，但行还在表里 —— 这正是要回收的那三行"
    );

    assert_eq!(e.sweep_expired_locks(now_ms(), 1024).unwrap(), (3, 1));
    assert_eq!(stats_locks(&mut e), 2, "永久锁与未到期锁一行都不能少");
    // 再扫一次：没有可删的，也不应该动 WAL。
    assert_eq!(e.sweep_expired_locks(now_ms(), 1024).unwrap(), (2, 0));

    drop(e);
    let mut e = d.open();
    assert_eq!(
        stats_locks(&mut e),
        2,
        "DelLock 必须进了 WAL：重放之后过期的那行不应复活"
    );
}

/// 清扫按批走完整个表：每轮最多 `limit` 条，扫到尾巴就从头再来。
///
/// 这条钉的是「单轮代价与表大小无关」那个前提 —— 若哪天改成全表扫，
/// 这里的 `scanned <= limit` 会直接红，而不是等到线上出现一次慢轮。
#[test]
fn the_lock_sweep_walks_the_table_in_batches_and_terminates() {
    let d = Dir::new("locksweepbatches");
    let mut e = Engine::open(&d.path(), Durability::Never).expect("engine open");
    let v = mk_view(&mut e, "v");
    let f = mk_file(&mut e, v, 1, "f");

    let n = LOCK_SWEEP_BATCH * 2 + 100;
    let expired = now_ms() - 1;
    for i in 0..n {
        ok(
            &mut e,
            &Op::LockSet {
                view: v,
                ino: f.ino,
                owner: format!("o{i}").into_bytes(),
                verb: 0,
                kind: 1,
                lease_expiry: expired,
            },
        );
    }

    let (mut rounds, mut total) = (0usize, 0usize);
    loop {
        let (scanned, removed) = e.sweep_expired_locks(now_ms(), LOCK_SWEEP_BATCH).unwrap();
        rounds += 1;
        assert!(scanned <= LOCK_SWEEP_BATCH, "一轮不能看超过 limit 条");
        total += removed;
        if scanned == 0 {
            break;
        }
        assert!(rounds <= 8, "{n} 条锁应该在 4 轮内扫完");
    }
    assert_eq!(total, n, "每一条过期租约都必须被删掉一次");
    match ok(&mut e, &Op::Stats) {
        Reply::Stats(s) => assert_eq!(s.locks, 0),
        other => panic!("{other:?}"),
    }
}

/// 副本上这个周期任务必须是空操作：副本的 WAL 是主的镜像，本地发一句
/// `DelLock` 就会让自己与主的 seq 对不上，下一批 `WalFetch` 当场报 gap。
#[test]
fn the_lock_sweep_never_writes_on_a_replica() {
    let d = Dir::new("locksweepreplica");
    {
        let mut e = d.open();
        let v = mk_view(&mut e, "v");
        let f = mk_file(&mut e, v, 1, "f");
        ok(&mut e, &Op::LockSet { view: v, ino: f.ino, owner: b"gone".to_vec(), verb: 0, kind: 1, lease_expiry: now_ms() - 1 });
    }
    let mut r = Engine::open_replica(&d.path(), Durability::Always).expect("replica open");
    let before = r.snapshot_stats().seq;
    assert_eq!(r.sweep_expired_locks(now_ms(), LOCK_SWEEP_BATCH).unwrap(), (0, 0));
    assert_eq!(r.snapshot_stats().seq, before, "副本一条日志都不该产生");
    match ok(&mut r, &Op::Stats) {
        Reply::Stats(s) => assert_eq!(s.locks, 1, "过期行还在，等主来发 DelLock"),
        other => panic!("{other:?}"),
    }
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
    // 通用表：让表/行/级联删除也进重放与快照这两条恢复路径。
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
    for (k, a, b) in [("r1", "a1", "x"), ("r2", "a2", "x"), ("r3", "a3", "y")] {
        ok(
            e,
            &Op::RowPut {
                table: b"t1".to_vec(),
                key: k.as_bytes().to_vec(),
                cols: vec![a.as_bytes().to_vec(), b.as_bytes().to_vec()],
                flags: 0,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            },
        );
    }
    // 覆盖写 + 删行：`PutRow` 的旧值必须从索引摘掉，`DelRow` 同样要重放。
    ok(
        e,
        &Op::RowPut {
            table: b"t1".to_vec(),
            key: b"r2".to_vec(),
            cols: vec![b"a2b".to_vec(), b"x".to_vec()],
            flags: 0,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        },
    );
    ok(
        e,
        &Op::RowDelete {
            table: b"t1".to_vec(),
            key: b"r3".to_vec(),
            flags: 0,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        },
    );
    // 一张用完就丢的表，让 `DelTable` 级联也走一遍重放。
    ok(
        e,
        &Op::TableCreate {
            def: TableDef {
                name: b"t-gone".to_vec(),
                columns: vec![ColumnDef { name: b"c".to_vec(), flags: COL_UNIQUE }],
            },
        },
    );
    ok(
        e,
        &Op::RowPut {
            table: b"t-gone".to_vec(),
            key: b"g".to_vec(),
            cols: vec![b"v".to_vec()],
            flags: 0,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        },
    );
    ok(e, &Op::TableDrop { name: b"t-gone".to_vec() });
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
    assert!(s.view_by_id.contains_key(&v));
    // 通用表：级联后只剩一张活表，正向表同批摘掉；id 不回收。
    assert_eq!(s.tables.len(), 1, "只剩 t1：{:?}", s.tables.keys().collect::<Vec<_>>());
    assert_eq!(s.table_by_id.len(), 1);
    assert_eq!(s.next_table_id, 2, "drop 掉的 id 不能重新发票（与 view id 同规）");
    // `rows.len()` 是表数，`row_count()` 才是行数：3 写 1 删 = 2。
    assert_eq!(s.rows.len(), 1);
    assert_eq!(s.row_count(), 2);
    // 索引项必须与行严格对齐：2 行 × 2 个索引列。覆盖写的旧值、级联删掉的行
    // 若有一条残留，这里就会先响。
    assert_eq!(s.row_index.len(), 4, "索引残留/漏建：{:?}", s.row_index.keys().collect::<Vec<_>>());
    assert!(s.index_keys(0, 0, b"a2").is_empty(), "被覆盖的旧值必须从索引里摘掉");
    assert_eq!(s.index_keys(1, 0, b"v"), Vec::<Vec<u8>>::new(), "删表的行不能留在索引");
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
    // 先 drop 掉 e2：一个目录同时只能有一个活着的 `Engine`（`.lock` 上的 flock），
    // 而「再开一次」本来就是在模拟重启。
    drop(e2);
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

// ---------------------------------------------------------------- 落盘合批

/// `always` 与 `group` 都必须**一轮一次 fsync 覆盖本轮全部连接**。
///
/// 这颗钉子是 §7.9 那次改动的回归防线：旧实现里 `always` 按组逐个 `commit`，
/// 64 并发下第 k 条连接要排 k × fsync（机械盘上量到 p99 65–556 ms、吞吐只有
/// `group` 的约 1/6）。而两个档位的不变量本来就是同一句「ack 晚于覆盖该 op 的
/// fsync」，合批不破坏它 —— 所以这里断言的不是性能，而是「合批没吞记录」：
/// 四个组一次提交，重启后四个视图必须全在。
#[test]
fn both_durable_tiers_fsync_once_per_round_not_per_group() {
    for tier in [Durability::Always, Durability::Group] {
        let d = Dir::new("fsync-per-round");
        let mut e = Engine::open(&d.path(), tier).expect("engine open");
        let mut groups: Vec<Vec<Log>> = Vec::new();
        for i in 0..4 {
            let (logs, reply) = e.exec(&Op::CreateView {
                name: format!("v{i}").into_bytes(),
            });
            assert!(matches!(reply, Reply::View { .. }), "{tier:?}: {reply:?}");
            assert!(!logs.is_empty(), "{tier:?}: CreateView 必须产出日志");
            groups.push(logs);
        }
        let refs: Vec<&[Log]> = groups.iter().map(|g| g.as_slice()).collect();
        let before = e.snapshot_stats().syncs_total;
        e.commit_batch(&refs).expect("commit");
        let after = e.snapshot_stats();
        assert_eq!(
            after.syncs_total - before,
            1,
            "{tier:?}: 一轮四个组只能付一次 fsync（旧 `always` 就是在这里翻了 4 倍）"
        );
        drop(e);
        // 合批不能吞记录：重开必须看见全部四个视图。
        let mut reopened = Engine::open(&d.path(), tier).expect("reopen");
        assert_eq!(
            reopened.snapshot_stats().views,
            4,
            "{tier:?}: 一次 fsync 必须覆盖本轮所有组，一条不能少"
        );
    }
}

// ---------------------------------------------------------------- 通用表（§11 T2）

fn col(name: &str, flags: u32) -> ColumnDef {
    ColumnDef { name: name.as_bytes().to_vec(), flags }
}

/// 建一张 `(a 唯一, b 仅索引, c 无约束)` 的三列表，返回它的 def。
fn mk_table(e: &mut Engine, name: &str) -> TableDef {
    let def = TableDef {
        name: name.as_bytes().to_vec(),
        columns: vec![col("a", COL_UNIQUE), col("b", COL_INDEX), col("c", 0)],
    };
    match ok(e, &Op::TableCreate { def: def.clone() }) {
        Reply::Table(Some(x)) => assert_eq!(x, def, "建表要把成品回给调用方"),
        other => panic!("{other:?}"),
    }
    def
}

fn rowput_op(table: &str, key: &str, cols: &[&str], flags: u32, cond_col: u32, cond_val: &str) -> Op {
    Op::RowPut {
        table: table.as_bytes().to_vec(),
        key: key.as_bytes().to_vec(),
        cols: cols.iter().map(|s| s.as_bytes().to_vec()).collect(),
        flags,
        cond_col,
        cond_val: cond_val.as_bytes().to_vec(),
    }
}

fn put_row(e: &mut Engine, table: &str, key: &str, cols: &[&str], flags: u32, cond_col: u32, cond_val: &str) -> Reply {
    ok(e, &rowput_op(table, key, cols, flags, cond_col, cond_val))
}

fn err_rowput(e: &mut Engine, table: &str, key: &str, cols: &[&str], flags: u32, cond_col: u32, cond_val: &str) -> u16 {
    err_code(e, &rowput_op(table, key, cols, flags, cond_col, cond_val))
}

fn rowlist_op(table: &str, column: u32, value: &str, from: &str, limit: u32) -> Op {
    Op::RowList {
        table: table.as_bytes().to_vec(),
        column,
        value: value.as_bytes().to_vec(),
        from: from.as_bytes().to_vec(),
        limit,
    }
}

fn err_rowlist(e: &mut Engine, table: &str, column: u32, value: &str) -> u16 {
    err_code(e, &rowlist_op(table, column, value, "", 10))
}

fn row_count(e: &mut Engine, table: &str, column: u32, value: &str) -> u64 {
    match ok(e, &Op::RowCount { table: table.as_bytes().to_vec(), column, value: value.as_bytes().to_vec() }) {
        Reply::Count(n) => n,
        other => panic!("{other:?}"),
    }
}

fn row_keys(r: Reply) -> Vec<String> {
    match r {
        Reply::Rows { rows, .. } => rows
            .iter()
            .map(|x| String::from_utf8_lossy(&x.key).into_owned())
            .collect(),
        other => panic!("{other:?}"),
    }
}

/// 按游标翻完一段：每页 `page` 条，下一页的 `from` 就是本页最后一行的主键。
/// 只靠「主键序」推游标是不够的，这里要把不重不漏当街声明。
fn walk(e: &mut Engine, table: &str, column: u32, value: &str, page: u32) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut from = String::new();
    for _ in 0..1000 {
        let (keys, end) = match ok(e, &rowlist_op(table, column, value, &from, page)) {
            Reply::Rows { rows, end } => {
                if !end {
                    assert_eq!(rows.len() as u32, page, "没到底的页必须装满");
                }
                (rows.iter().map(|x| String::from_utf8_lossy(&x.key).into_owned()).collect::<Vec<_>>(), end)
            }
            other => panic!("{other:?}"),
        };
        let Some(last) = keys.last().cloned() else {
            assert!(end, "空页不能说「还没到底」");
            break;
        };
        for k in keys {
            assert!(!out.contains(&k), "翻页重复返回了 {k}");
            out.push(k);
        }
        from = last;
        if end {
            break;
        }
    }
    out
}

#[test]
fn table_shape_is_validated_and_names_can_be_reclaimed() {
    let d = Dir::new("tables");
    let mut e = d.open();

    // 形状校验先于一切：名字/列数/列名任一条不对都不配占一个表 id。
    assert_eq!(err_code(&mut e, &Op::TableCreate { def: TableDef { name: Vec::new(), columns: vec![col("a", 0)] } }), errno::EINVAL);
    assert_eq!(
        err_code(&mut e, &Op::TableCreate { def: TableDef { name: vec![b'x'; TABLE_NAME_MAX + 1], columns: vec![col("a", 0)] } }),
        errno::ENAMETOOLONG
    );
    assert_eq!(err_code(&mut e, &Op::TableCreate { def: TableDef { name: b"nocol".to_vec(), columns: Vec::new() } }), errno::EINVAL);
    assert_eq!(err_code(&mut e, &Op::TableCreate { def: TableDef { name: b"dup".to_vec(), columns: vec![col("a", 0), col("a", COL_UNIQUE)] } }), errno::EINVAL);
    assert_eq!(err_code(&mut e, &Op::TableCreate { def: TableDef { name: b"emptycol".to_vec(), columns: vec![col("", 0)] } }), errno::EINVAL);
    assert_eq!(e.snapshot_stats().tables, 0, "被拒的 def 不能留下半张表");

    mk_table(&mut e, "t");
    // 重名 = EEXIST，不是覆盖：两份列数不同的形状同时存在，行就没法解释了。
    assert_eq!(err_code(&mut e, &Op::TableCreate { def: TableDef { name: b"t".to_vec(), columns: vec![col("z", 0)] } }), errno::EEXIST);

    // 查不存在的表不是错误：回 `Table(None)`，客户端一个 op 就能判存在性。
    match ok(&mut e, &Op::TableGet { name: b"ghost".to_vec() }) {
        Reply::Table(None) => {}
        other => panic!("TableGet 缺表应回 Table(None)，实得 {other:?}"),
    }
    match ok(&mut e, &Op::TableList) {
        Reply::Tables(v) => assert_eq!(v.len(), 1, "TableList 要把 def 原样回齐：{v:?}"),
        other => panic!("{other:?}"),
    }
    // 不存在的表名在写侧也是 ENOENT，而不是凭空建一张。
    assert_eq!(err_rowput(&mut e, "nope", "k", &["a"], 0, COND_NONE, ""), errno::ENOENT);
    assert_eq!(err_code(&mut e, &Op::TableDrop { name: b"nope".to_vec() }), errno::ENOENT);
    assert_eq!(err_rowlist(&mut e, "nope", ROW_PK, ""), errno::ENOENT);

    put_row(&mut e, "t", "k1", &["a1", "x", "1"], 0, COND_NONE, "");
    ok(&mut e, &Op::TableDrop { name: b"t".to_vec() });
    // 表名可以回收（与视图名同理：名字是客户端的键，不是主键），但旧行不能跟着回来。
    mk_table(&mut e, "t");
    match ok(&mut e, &Op::RowGet { table: b"t".to_vec(), key: b"k1".to_vec() }) {
        Reply::Row(None) => {}
        other => panic!("重建的表不能带着旧行：{other:?}"),
    }
    assert_eq!(row_count(&mut e, "t", ROW_PK, ""), 0);
}

#[test]
fn row_writes_respect_bounds_constraints_and_conditions() {
    let d = Dir::new("rowwrite");
    let mut e = d.open();
    mk_table(&mut e, "t");

    // 主键 1..=上限：空主键会让「从头」与「从空键起」两种意思撞在一个写法上。
    assert_eq!(err_rowput(&mut e, "t", "", &["a1", "b1", "c1"], 0, COND_NONE, ""), errno::EINVAL);
    assert_eq!(
        err_rowput(&mut e, "t", &"k".repeat(ROW_KEY_MAX + 1), &["a1", "b1", "c1"], 0, COND_NONE, ""),
        errno::EINVAL
    );
    // 列数与 def 严格相等：少一列不是「那列留空」，是形状不符。
    assert_eq!(err_rowput(&mut e, "t", "k", &["a1"], 0, COND_NONE, ""), errno::EINVAL);
    assert_eq!(err_rowput(&mut e, "t", "k", &["a1", "b1", "c1", "d1"], 0, COND_NONE, ""), errno::EINVAL);

    put_row(&mut e, "t", "k1", &["a1", "x", "1"], 0, COND_NONE, "");
    put_row(&mut e, "t", "k2", &["a2", "x", "2"], 0, COND_NONE, "");
    // 唯一列撞值 → EEXIST；非唯一列（b 只索引）同值两行必须放行。
    assert_eq!(err_rowput(&mut e, "t", "k3", &["a1", "y", "3"], 0, COND_NONE, ""), errno::EEXIST);
    put_row(&mut e, "t", "k3", &["a3", "x", "3"], 0, COND_NONE, "");
    assert!(
        matches!(put_row(&mut e, "t", "k1", &["a1", "x", "9"], 0, COND_NONE, ""), Reply::Row(Some(_))),
        "同值覆盖写不算撞约束"
    );
    // 空字节是一列的合法值：唯一列上两行都取空值也算撞。
    put_row(&mut e, "t", "k4", &["", "x", ""], 0, COND_NONE, "");
    assert_eq!(err_rowput(&mut e, "t", "k5", &["", "z", ""], 0, COND_NONE, ""), errno::EEXIST, "空值也是值");

    // flags：两个互斥位同时给是客户端的错。
    assert_eq!(err_rowput(&mut e, "t", "k1", &["a9", "x", "1"], PUT_IF_ABSENT, COND_NONE, ""), errno::EEXIST);
    assert_eq!(err_rowput(&mut e, "t", "kk", &["a9", "x", "1"], PUT_IF_EXISTS, COND_NONE, ""), errno::ENOENT);
    assert_eq!(
        err_rowput(&mut e, "t", "kk", &["a9", "x", "1"], PUT_IF_ABSENT | PUT_IF_EXISTS, COND_NONE, ""),
        errno::EINVAL
    );

    // CAS：命中才写，不命中一律 EAGAIN（行不存在也算不命中）；列号越界是 EINVAL。
    assert_eq!(err_rowput(&mut e, "t", "k2", &["a2b", "x", "2"], 0, 0, "wrong"), errno::EAGAIN);
    assert_eq!(err_rowput(&mut e, "t", "k2", &["a2b", "x", "2"], 0, 7, ""), errno::EINVAL);
    assert_eq!(err_rowput(&mut e, "t", "knew", &["an", "x", "n"], 0, 0, "whatever"), errno::EAGAIN);
    put_row(&mut e, "t", "k2", &["a2b", "x", "2"], 0, 0, "a2");
    match ok(&mut e, &Op::RowGet { table: b"t".to_vec(), key: b"k2".to_vec() }) {
        Reply::Row(Some(r)) => assert_eq!(
            r.cols,
            vec![b"a2b".to_vec(), b"x".to_vec(), b"2".to_vec()],
            "CAS 命中就要写下去"
        ),
        other => panic!("{other:?}"),
    }
    match ok(&mut e, &Op::RowGet { table: b"t".to_vec(), key: b"no-k".to_vec() }) {
        Reply::Row(None) => {}
        other => panic!("RowGet 缺行应回 Row(None)，实得 {other:?}"),
    }

    // 删行：默认幂等（重发一个已成功的删除不该看到错误）。
    let del = |key: &str, flags: u32, cond_col: u32, cond_val: &str| Op::RowDelete {
        table: b"t".to_vec(),
        key: key.as_bytes().to_vec(),
        flags,
        cond_col,
        cond_val: cond_val.as_bytes().to_vec(),
    };
    assert!(matches!(ok(&mut e, &del("k4", 0, COND_NONE, "")), Reply::Ok));
    assert!(matches!(ok(&mut e, &del("k4", 0, COND_NONE, "")), Reply::Ok), "幂等是默认");
    assert_eq!(err_code(&mut e, &del("k4", DEL_IF_EXISTS, COND_NONE, "")), errno::ENOENT);
    assert_eq!(err_code(&mut e, &del("k3", 0, 0, "not-a3")), errno::EAGAIN);
    assert!(
        matches!(ok(&mut e, &Op::RowGet { table: b"t".to_vec(), key: b"k3".to_vec() }), Reply::Row(Some(_))),
        "条件不命中不能把行顺手删了"
    );

    // 删表之后行不能还在。
    ok(&mut e, &Op::TableDrop { name: b"t".to_vec() });
    assert_eq!(err_code(&mut e, &Op::RowGet { table: b"t".to_vec(), key: b"k1".to_vec() }), errno::ENOENT);
}

#[test]
fn row_scans_page_by_key_cursor_without_gaps_or_repeats() {
    let d = Dir::new("rowscan");
    let mut e = d.open();
    mk_table(&mut e, "t");
    // 25 行，主键定宽补零：字节序 = 数字序，期望值才能直接写死。
    for i in 0..25 {
        let (k, a) = (format!("k{i:02}"), format!("a{i:02}"));
        let b = if i % 2 == 0 { "even" } else { "odd" };
        put_row(&mut e, "t", &k, &[&a, b, "c"], 0, COND_NONE, "");
    }

    let want: Vec<String> = (0..25).map(|i| format!("k{i:02}")).collect();
    assert_eq!(walk(&mut e, "t", ROW_PK, "", 4), want, "主键游标翻页必须不重不漏");
    assert_eq!(walk(&mut e, "t", ROW_PK, "", 25), want, "一页恰好装完也得说清到底了");
    assert_eq!(walk(&mut e, "t", 1, "even", 4), (0..25).step_by(2).map(|i| format!("k{i:02}")).collect::<Vec<_>>());
    assert_eq!(walk(&mut e, "t", 1, "odd", 3), (1..25).step_by(2).map(|i| format!("k{i:02}")).collect::<Vec<_>>());
    // 主键当游标用在索引列上：同一值那一段里接着往下走。
    let mut from_one = row_keys(ok(&mut e, &rowlist_op("t", 1, "even", "", 1)));
    assert_eq!(from_one, vec!["k00".to_string()]);
    from_one = row_keys(ok(&mut e, &rowlist_op("t", 1, "even", &from_one[0], 2)));
    assert_eq!(from_one, vec!["k02".to_string(), "k04".to_string()]);

    // 计数：主键 = 表里存了多少行，索引列 = 命中数。
    assert_eq!(row_count(&mut e, "t", ROW_PK, ""), 25);
    assert_eq!(row_count(&mut e, "t", 1, "even"), 13);
    assert_eq!(row_count(&mut e, "t", 1, "none"), 0);

    // 没建索引的列（c）与越界列号都是 EINVAL，而不是静默扫全表。
    assert_eq!(err_rowlist(&mut e, "t", 2, "c"), errno::EINVAL);
    assert_eq!(err_rowlist(&mut e, "t", 9, ""), errno::EINVAL);
    assert_eq!(err_code(&mut e, &Op::RowCount { table: b"t".to_vec(), column: 2, value: b"c".to_vec() }), errno::EINVAL);

    // `limit = 0` 走默认页宽（与 readdir 同一套路），不是「不限」。整表 25 行一页就到底。
    assert_eq!(row_keys(ok(&mut e, &rowlist_op("t", ROW_PK, "", "", 0))).len(), 25);
    // 空表与不存在的列值都是空页 + end=true，不能让调用方多跑一页空扫才发现到底。
    match ok(&mut e, &rowlist_op("t", 1, "none", "", 10)) {
        Reply::Rows { rows, end } => assert!(rows.is_empty() && end),
        other => panic!("{other:?}"),
    }
    // 删到空：内层表不留，与 dentries 同规矩（否则 `rows.len()` 会谎报表数）。
    for i in 0..25 {
        let k = format!("k{i:02}");
        ok(&mut e, &Op::RowDelete { table: b"t".to_vec(), key: k.into_bytes(), flags: 0, cond_col: COND_NONE, cond_val: Vec::new() });
    }
    let s = e.state_snapshot();
    assert!(s.rows.is_empty(), "空内层表不能留在外层");
    assert!(s.row_index.is_empty(), "删行必须同时摘掉索引项");
}

fn rowrange_op(
    table: &str,
    column: u32,
    lo: &str,
    hi: &str,
    after_val: &str,
    after_key: &str,
    limit: u32,
) -> Op {
    Op::RowRange {
        table: table.as_bytes().to_vec(),
        column,
        lo: lo.as_bytes().to_vec(),
        hi: hi.as_bytes().to_vec(),
        after_val: after_val.as_bytes().to_vec(),
        after_key: after_key.as_bytes().to_vec(),
        limit,
    }
}

/// 区间读的一页：`(主键序, end)`。
fn range_page(
    e: &mut Engine,
    table: &str,
    column: u32,
    lo: &str,
    hi: &str,
    after_val: &str,
    after_key: &str,
    limit: u32,
) -> (Vec<String>, bool) {
    match ok(e, &rowrange_op(table, column, lo, hi, after_val, after_key, limit)) {
        Reply::Rows { rows, end } => (
            rows.iter()
                .map(|x| String::from_utf8_lossy(&x.key).into_owned())
                .collect(),
            end,
        ),
        other => panic!("{other:?}"),
    }
}

/// 按区间游标翻完一段。主键区间只需主键作游标；索引列区间要连列值一起带上
/// —— 同一个值可能跨页，只给主键回不到「这一页停在哪一段」。
fn walk_range(
    e: &mut Engine,
    table: &str,
    column: u32,
    lo: &str,
    hi: &str,
    page: u32,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let (mut av, mut ak) = (String::new(), String::new());
    for _ in 0..1000 {
        let (rows, end) = match ok(e, &rowrange_op(table, column, lo, hi, &av, &ak, page)) {
            Reply::Rows { rows, end } => {
                if !end {
                    assert_eq!(rows.len() as u32, page, "没到底的页必须装满");
                }
                (rows, end)
            }
            other => panic!("{other:?}"),
        };
        let Some(last) = rows.last() else {
            assert!(end, "空页不能说「还没到底」");
            break;
        };
        ak = String::from_utf8_lossy(&last.key).into_owned();
        av = if column == ROW_PK {
            String::new()
        } else {
            last.cols
                .get(column as usize)
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default()
        };
        for r in &rows {
            let k = String::from_utf8_lossy(&r.key).into_owned();
            assert!(!out.contains(&k), "翻页重复返回了 {k}");
            out.push(k);
        }
        if end {
            break;
        }
    }
    out
}

#[test]
fn row_range_by_key_is_half_open_and_keeps_the_bounds_when_paging() {
    let d = Dir::new("rowrange");
    let mut e = d.open();
    mk_table(&mut e, "t");
    for i in 0..25 {
        let (k, a) = (format!("k{i:02}"), format!("a{i:02}"));
        put_row(&mut e, "t", &k, &[&a, "c", "d"], 0, COND_NONE, "");
    }
    let keys = |a: usize, b: usize| (a..b).map(|i| format!("k{i:02}")).collect::<Vec<_>>();

    // 左闭右开：`lo` 那一行在里面，`hi` 那一行恰好在外面。
    assert_eq!(range_page(&mut e, "t", ROW_PK, "k05", "k15", "", "", 100), (keys(5, 15), true));
    // 两侧空 = 该侧无界。
    assert_eq!(range_page(&mut e, "t", ROW_PK, "", "", "", "", 100), (keys(0, 25), true));
    assert_eq!(range_page(&mut e, "t", ROW_PK, "k20", "", "", "", 100), (keys(20, 25), true));
    assert_eq!(range_page(&mut e, "t", ROW_PK, "", "k03", "", "", 100), (keys(0, 3), true));
    // 翻页不重不漏，末页说清到底。
    assert_eq!(walk_range(&mut e, "t", ROW_PK, "k05", "k20", 3), keys(5, 20));
    // 游标就是 `lo` 那一行：不含它自己。
    assert_eq!(range_page(&mut e, "t", ROW_PK, "k05", "k10", "", "k05", 100), (keys(6, 10), true));
    // 游标落在 `lo` 之前：仍从 `lo` 起，不能把前面那段带出来。
    assert_eq!(range_page(&mut e, "t", ROW_PK, "k10", "k12", "", "k02", 100), (keys(10, 12), true));
    // 游标越过上界 / `lo == hi` / `lo > hi`：一律空页 + 到底。这几条都在
    // `BTreeMap::range` 的 panic 条件上，不能用「肯定不会这么调」推掉。
    for (lo, hi, ak) in [
        ("k05", "k10", "k10"),
        ("k05", "k10", "k99"),
        ("k07", "k07", ""),
        ("k20", "k05", ""),
    ] {
        assert_eq!(
            range_page(&mut e, "t", ROW_PK, lo, hi, "", ak, 10),
            (Vec::new(), true),
            "lo={lo} hi={hi} after={ak} 应当是一页空 + 到底"
        );
    }
    // `limit = 0` 与 RowList 同一个默认页宽（1000），整段一页装完。
    assert_eq!(range_page(&mut e, "t", ROW_PK, "", "", "", "", 0), (keys(0, 25), true));
    // 拒绝路径与列举同规矩：没有表 ENOENT，没索引的列 / 越界列号 EINVAL。
    assert_eq!(err_code(&mut e, &rowrange_op("nope", ROW_PK, "", "", "", "", 10)), errno::ENOENT);
    assert_eq!(err_code(&mut e, &rowrange_op("t", 2, "", "", "", "", 10)), errno::EINVAL);
    assert_eq!(err_code(&mut e, &rowrange_op("t", 9, "", "", "", "", 10)), errno::EINVAL);
}

#[test]
fn index_range_walks_column_values_and_pages_inside_one_value() {
    let d = Dir::new("idxrange");
    let mut e = d.open();
    mk_table(&mut e, "t");
    // 30 行：`b` 列 even/odd 交替，`a` 列唯一（也建了索引）。定宽字符串让字节序 = 数字序。
    for i in 0..30 {
        let (k, a) = (format!("k{i:02}"), format!("a{i:02}"));
        let b = if i % 2 == 0 { "even" } else { "odd" };
        put_row(&mut e, "t", &k, &[&a, b, "c"], 0, COND_NONE, "");
    }
    let evens: Vec<String> = (0..30).step_by(2).map(|i| format!("k{i:02}")).collect();
    let odds: Vec<String> = (1..30).step_by(2).map(|i| format!("k{i:02}")).collect();

    // 区间落在**列值**上：`[even, f)` 只含 even —— `f` 排在 even 与 odd 之间，
    // 这正好说明上界挡的是整段「值」而不是「某一行」。
    assert_eq!(walk_range(&mut e, "t", 1, "even", "f", 4), evens);
    assert_eq!(walk_range(&mut e, "t", 1, "odd", "", 3), odds, "hi 空 = 上界无界，走到底");
    // 一个值内部按主键序；游标 `(值, 主键)` 要把同值的下一页接上。
    assert_eq!(range_page(&mut e, "t", 1, "even", "f", "", "", 4), (evens[0..4].to_vec(), false));
    assert_eq!(range_page(&mut e, "t", 1, "even", "f", "even", "k06", 3), (evens[4..7].to_vec(), false));
    assert_eq!(range_page(&mut e, "t", 1, "even", "f", "even", "k26", 3), (vec!["k28".to_string()], true));
    assert_eq!(
        range_page(&mut e, "t", 1, "even", "f", "even", "k28", 3),
        (Vec::new(), true),
        "游标落在该值最后一行之后：空页 + 到底，不能越过 `hi` 去碰下一个值"
    );
    // 游标的值落在 `lo` 之前：仍从 `lo` 那一段起，不能把前面带出来。
    assert_eq!(range_page(&mut e, "t", 1, "even", "f", "aaa", "k00", 10).0, evens[0..10].to_vec());
    // 游标的值到 `hi` 之后：空页 + 到底。
    assert_eq!(range_page(&mut e, "t", 1, "even", "f", "zzz", "k00", 10), (Vec::new(), true));
    // `lo > hi` 是空区间而不是错误；两侧空 = 全列按值序。
    assert_eq!(range_page(&mut e, "t", 1, "odd", "even", "", "", 100), (Vec::new(), true));
    let all = range_page(&mut e, "t", 1, "", "", "", "", 100);
    assert_eq!(all.0.len(), 30, "两侧空要走完这一列");
    assert!(all.1, "一页装完必须说清到底");
    // 跨值翻页：唯一列 a 的 `[a03, a09)`，一个值一行，页宽 2。
    assert_eq!(
        walk_range(&mut e, "t", 0, "a03", "a09", 2),
        (3..9).map(|i| format!("k{i:02}")).collect::<Vec<_>>()
    );
    // 拒绝路径：没有表 ENOENT，没索引的列 / 越界列号 EINVAL。
    assert_eq!(err_code(&mut e, &rowrange_op("nope", 1, "", "", "", "", 10)), errno::ENOENT);
    assert_eq!(err_code(&mut e, &rowrange_op("t", 2, "", "", "", "", 10)), errno::EINVAL);
    assert_eq!(err_code(&mut e, &rowrange_op("t", 9, "", "", "", "", 10)), errno::EINVAL);
}

#[test]
fn table_state_survives_checkpoint_and_reopen() {
    let d = Dir::new("tblreopen");
    let mut e = d.open();
    mk_table(&mut e, "keep");
    mk_table(&mut e, "gone");
    put_row(&mut e, "keep", "k1", &["a1", "x", "1"], 0, COND_NONE, "");
    put_row(&mut e, "gone", "g1", &["a2", "x", "2"], 0, COND_NONE, "");
    ok(&mut e, &Op::TableDrop { name: b"gone".to_vec() });
    // checkpoint 之后再写一条，逼「快照 + 增量 WAL」两段都带上表状态。
    ok(&mut e, &Op::Checkpoint);
    put_row(&mut e, "keep", "k2", &["a3", "y", "3"], 0, COND_NONE, "");
    let before = e.state_snapshot();
    drop(e);

    let mut e2 = d.open();
    assert_eq!(e2.state_snapshot(), before, "表/行/索引必须与关掉前逐字段相等");
    match ok(&mut e2, &Op::TableList) {
        Reply::Tables(v) => assert_eq!(
            v.iter().map(|t| String::from_utf8_lossy(&t.name).into_owned()).collect::<Vec<_>>(),
            vec!["keep".to_string()],
            "drop 掉的表不能重放回来"
        ),
        other => panic!("{other:?}"),
    }
    match ok(&mut e2, &Op::TableGet { name: b"keep".to_vec() }) {
        Reply::Table(Some(def)) => assert_eq!(def.columns.len(), 3, "def 要逐字段回来"),
        other => panic!("{other:?}"),
    }
    assert_eq!(row_count(&mut e2, "keep", ROW_PK, ""), 2);
    assert_eq!(row_keys(ok(&mut e2, &rowlist_op("keep", 1, "x", "", 10))), vec!["k1".to_string()]);
    // 二级索引不存磁盘：重开后唯一约束得照样拦得住，否则就是索引没重建。
    assert_eq!(err_rowput(&mut e2, "keep", "x1", &["a1", "z", "9"], 0, COND_NONE, ""), errno::EEXIST);
}

// ---------------------------------------------------------------- readdir 带属性（§11 #6）

#[test]
fn readdir_plus_answers_the_same_page_as_readdir_plus_getattr() {
    let d = Dir::new("rdplus");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    let a = mk_file(&mut e, v, 1, "a");
    let _dd = mk_dir(&mut e, v, 1, "d");
    ok(&mut e, &Op::Symlink { view: v, parent: 1, name: b"l".to_vec(), target: b"a".to_vec(), uid: 3, gid: 4 });
    // 把四条目录项拉成四种不同属性：改 mode/uid/gid/size、提交内容引用，
    // 这样平行的属性数组不是四份全零。
    ok(&mut e, &Op::Setattr {
        view: v, ino: a.ino, valid: SA_MODE | SA_UID | SA_GID | SA_SIZE, mode: 0o640, uid: 7, gid: 8,
        size: 4096, atime: 0, mtime: 0, data_ref: Vec::new(), ref_bytes: 0,
    });
    let f2 = mk_file(&mut e, v, 1, "f2");
    ok(&mut e, &commit_ref(v, f2.ino, 100, 4096));

    for (off, lim) in [(0u64, 100u32), (0, 2), (2, 2), (4, 2), (0, 1), (9, 10)] {
        let (p_inos, p_kinds, p_names, p_next, p_end) = match ok(&mut e, &Op::ReadDir { view: v, parent: 1, offset: off, limit: lim }) {
            Reply::Dirents { inos, kinds, names, next, end } => (inos, kinds, names.to_vec(), next, end),
            other => panic!("{other:?}"),
        };
        let (q_inos, q_kinds, q_names, q_attrs, q_next, q_end) = match ok(
            &mut e,
            &Op::ReadDirPlus { view: v, parent: 1, offset: off, limit: lim },
        ) {
            Reply::DirentsPlus { inos, kinds, names, attrs, next, end } => {
                assert_eq!(attrs.len(), inos.len(), "平行数组必须同长，少一项就是整页错位");
                assert_eq!(kinds.len(), inos.len());
                (inos, kinds, names.to_vec(), attrs, next, end)
            }
            other => panic!("{other:?}"),
        };
        let tag = format!("offset {off} limit {lim}");
        assert_eq!(p_inos, q_inos, "{tag}: 目录页本身变了");
        assert_eq!(p_kinds, q_kinds, "{tag}");
        assert_eq!(p_names, q_names, "{tag}");
        assert_eq!((p_next, p_end), (q_next, q_end), "{tag}: 分页游标与到底判定必须一致");
        // 属性逐条与 Getattr 对齐：这就是这一页省下那 N 次往返的全部内容。
        for (ino, attr) in q_inos.iter().zip(q_attrs.iter()) {
            assert_eq!(*attr, NodeAttr::of(&getattr(&mut e, v, *ino)), "{tag}: ino {ino} 的属性与 Getattr 不符");
        }
    }

    // 错误面与 readdir 同一条：多要属性不该看到不同的 errno。
    assert_eq!(err_code(&mut e, &Op::ReadDirPlus { view: v, parent: a.ino, offset: 0, limit: 10 }), errno::ENOTDIR);
    assert_eq!(err_code(&mut e, &Op::ReadDirPlus { view: 999, parent: 1, offset: 0, limit: 10 }), errno::ENODEV);
    // 空目录（只有根本身时 dentries 里没这一项）：空页 + end，不是报错。
    let sub = mk_dir(&mut e, v, 1, "sub");
    match ok(&mut e, &Op::ReadDirPlus { view: v, parent: sub.ino, offset: 0, limit: 10 }) {
        Reply::DirentsPlus { names, end, next, .. } => {
            assert!(names.is_empty() && end, "空目录要一次就说到底");
            assert_eq!(next, 0);
        }
        other => panic!("{other:?}"),
    }
    // 视图删了之后目录页也不该残留半条：与 readdir 一样回空。
    ok(&mut e, &Op::DropView { name: b"v".to_vec() });
    assert_eq!(err_code(&mut e, &Op::ReadDirPlus { view: v, parent: 1, offset: 0, limit: 10 }), errno::ENODEV);
}

// ---------------------------------------------------------------- Stats 的磁盘面（§11 #3）

#[test]
fn stats_reports_table_counts_and_both_disk_sizes() {
    let d = Dir::new("stdisk");
    let mut e = d.open();
    let wal_path = d.path().join("treedb.wal");
    let snap_path = d.path().join("state.snap");

    let st0 = e.snapshot_stats();
    assert_eq!(st0.proto_version, treedb::engine::PROTOCOL_VERSION, "Stats 要能报出对端协议版本");
    assert_eq!((st0.tables, st0.rows), (0, 0));
    assert_eq!(st0.snap_bytes, 0, "没做过 checkpoint 不该有快照长度");
    assert_eq!(st0.wal_disk_bytes, std::fs::metadata(&wal_path).unwrap().len(), "磁盘长度就是 st_size");
    // 两个数必须各说各的：预分配让文件比已写字节大，容量规划看大的那个。
    assert!(st0.wal_disk_bytes >= st0.wal_bytes, "逻辑长度不可能超过磁盘长度");

    mk_table(&mut e, "t");
    for i in 0..7 {
        let (k, a) = (format!("k{i}"), format!("a{i}"));
        put_row(&mut e, "t", &k, &[&a, "b", "c"], 0, COND_NONE, "");
    }
    // 表数与行数分开报：`tables` 是一张，`rows` 是七行（不是「有行的表数」）。
    let st = e.snapshot_stats();
    assert_eq!((st.tables, st.rows), (1, 7));
    assert!(st.wal_bytes > 0);

    ok(&mut e, &Op::Checkpoint);
    let st2 = e.snapshot_stats();
    assert!(st2.snap_bytes > 0, "checkpoint 后快照长度必须可查，否则恢复时间算不出账");
    assert_eq!(st2.snap_bytes, std::fs::metadata(&snap_path).unwrap().len());
    assert!(st2.checkpoints >= 1);
    // checkpoint 把 WAL 截断重写：逻辑长度回落，磁盘长度跟着文件走 ——
    // 断言的是「两个数都仍然自洽」，不是谁一定更大。
    let st3 = e.snapshot_stats();
    assert_eq!(st3.wal_disk_bytes, std::fs::metadata(&wal_path).unwrap().len());
    assert!(st3.wal_disk_bytes >= st3.wal_bytes);
    // 重开后计数与磁盘长度都得重新算出来，不能靠旧内存里的残值。
    drop(e);
    let mut e4 = d.open();
    let st4 = e4.snapshot_stats();
    assert_eq!((st4.tables, st4.rows), (1, 7));
    assert_eq!(st4.snap_bytes, std::fs::metadata(&snap_path).unwrap().len());
}

// ---- 2026-11-20：问题清单 F3 / F8 -------------------------------------------

/// 一个数据目录同时只能被一个 `Engine` 打开。
///
/// 没有这道闸时两个实例会各自重放同一份 WAL、各自 checkpoint、各自
/// `wal.reset`：谁后 rename 谁赢，先那个以为自己的写已落盘。锁是 `.lock`
/// 上的 `flock`，进程怎么死的都自动释放，所以这里第二段"重开能过"也是
/// 锁真放掉了的证据。
#[test]
fn a_data_dir_refuses_a_second_engine() {
    let d = Dir::new("lock_one_owner");
    let first = d.open();
    let err = Engine::open(&d.path(), Durability::Always)
        .err()
        .expect("同一目录的第二个实例必须被拒")
        .to_string();
    assert!(err.contains("already locked"), "{err}");
    assert!(
        err.contains(&std::process::id().to_string()),
        "错误里要带持锁 pid，运维才知道去找谁：{err}"
    );

    // 锁跟着 Engine 的生命周期走：drop 之后立刻可开。
    drop(first);
    let _again = d.open();
}

/// `unlink`/`rmdir` 与 `rename` 对"父 inode 不是目录"给同一个 errno。
///
/// POSIX 里路径前缀不是目录是 `ENOTDIR`，而且优先于"名字查不到"
/// （`ENOENT`）。改之前 unlink 直接查 dentry，于是 `unlink(file/child)` 回
/// `ENOENT` —— 同一个坏父 inode，rename 与 unlink 给出两个不同的答案。
#[test]
fn unlink_and_rmdir_report_enotdir_for_a_non_dir_parent() {
    let d = Dir::new("unlink_parent_kind");
    let mut e = d.open();
    let v = mk_view(&mut e, "alice");
    let file = node(ok(
        &mut e,
        &Op::Create { view: v, parent: 1, name: b"f".to_vec(), mode: 0o644, uid: 0, gid: 0 },
    ))
    .ino;

    assert_eq!(
        err_code(&mut e, &Op::Unlink { view: v, parent: file, name: b"x".to_vec() }),
        errno::ENOTDIR
    );
    assert_eq!(
        err_code(&mut e, &Op::Rmdir { view: v, parent: file, name: b"x".to_vec() }),
        errno::ENOTDIR
    );
    // 父 inode 根本不存在：仍然是 ENOENT（不是 ENOTDIR —— 没有东西可判类型）。
    assert_eq!(
        err_code(&mut e, &Op::Unlink { view: v, parent: 9_999, name: b"x".to_vec() }),
        errno::ENOENT
    );
    // 正常的 unlink 不受影响。
    ok(&mut e, &Op::Unlink { view: v, parent: 1, name: b"f".to_vec() });
}

// ------------------------------------------------- 旁路快照（§8 第 7 条）

/// 等到后台快照落定，返回它写进快照尾巴的水位。
fn settle_background(e: &mut Engine) -> u64 {
    for _ in 0..10_000 {
        if let Some(seq) = e.checkpoint_poll().expect("poll") {
            return seq;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("background snapshot never landed");
}

/// 停轮的代价必须与状态大小脱钩：派工只取水位 + 起线程。
///
/// 这个用例同时把最危险的那一格钉住 —— **取水位之后才 ack 的写**。它们不在
/// 这份快照里，只活在日志中；收尾若走 `wal.reset` 就会把它们一起抹掉，重启
/// 后两条都消失（内存里有、盘上没有，等于静默丢已确认的写）。
#[test]
fn background_snapshot_costs_a_dispatch_not_a_pause() {
    use std::time::Instant;
    const N: usize = 20_000;
    let d = Dir::new("bgpause");
    let mut e = Engine::open(&d.path(), Durability::Never).expect("open");
    let v = mk_view(&mut e, "v");
    for i in 0..N {
        ok(
            &mut e,
            &Op::Create {
                view: v,
                parent: 1,
                name: format!("f{i}").into(),
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
        );
    }
    e.sync_now().expect("sync");

    // 基准：同规模状态走一次停轮快照。
    let t = Instant::now();
    let sync_seq = e.checkpoint().expect("sync checkpoint");
    let paused = t.elapsed();

    // 再来一条写，好让旁路快照有活干。派工读数里混着**起线程的调度延迟**：机器
    // 被别人压满时一次 `thread::spawn` 能量到几十毫秒（实测 load 12 的一次跑出
    // paused=12.6 ms / dispatch=31.1 ms，断言假报警 —— 这条断言拦的是「派工变成
    // 停轮」，不是「这台机此刻很忙」）。所以取三次里的最小值（与 §7.15 那句
    // 「单次采样不配当基线」同一条规矩），而每派一份就收掉；两次派工之间必须
    // 先写一条 —— 水位没动时 `checkpoint_start` 直接回 `false`，那不给读数。
    let mut dispatch = std::time::Duration::MAX;
    for i in 0..3 {
        ok(
            &mut e,
            &Op::Create {
                view: v,
                parent: 1,
                name: format!("warm{i}").into_bytes(),
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
        );
        e.sync_now().expect("sync warm");
        let t = Instant::now();
        assert!(e.checkpoint_start().expect("start"), "第 {i} 次应当真的派出去");
        let d = t.elapsed();
        e.checkpoint_wait().expect("drain");
        dispatch = dispatch.min(d);
    }
    // 收尾那一句要拿它当「派工之后还在的节点」查回来。
    ok(
        &mut e,
        &Op::Create { view: v, parent: 1, name: b"after".to_vec(), mode: 0o644, uid: 0, gid: 0 },
    );
    e.sync_now().expect("sync");
    // 一条写可能落好几条日志（inode + 目录项 + …），所以这里不断言步长，
    // 只确认「水位确实往前走了」—— 派工取的就是它。
    let cut = e.snapshot_stats().seq;
    assert!(cut > sync_seq, "写几条新 op 之后水位必须往前走：{sync_seq} → {cut}");

    eprintln!("checkpoint pause vs dispatch @ {N} nodes: paused={paused:?} dispatch={dispatch:?}");
    assert!(
        dispatch * 5 < paused,
        "派工 {dispatch:?} 应当远小于停轮 {paused:?}（状态 {N} 条）"
    );
    // 在路上时第二次派工直接放弃：不让两份抢同一个 `state.snap.tmp`。
    assert!(e.checkpoint_start().expect("in flight"), "水位已往前走，这一份应当真派出去");
    assert!(!e.checkpoint_start().expect("second start"), "不许两份同时在编");

    // 取水位之后才 ack 的两条：不属于这份快照，但必须活过日志整理。
    for name in [b"tail1".to_vec(), b"tail2".to_vec()] {
        ok(
            &mut e,
            &Op::Create { view: v, parent: 1, name, mode: 0o644, uid: 0, gid: 0 },
        );
    }
    e.sync_now().expect("sync");

    assert_eq!(settle_background(&mut e), cut, "快照水位就是派工时取的那一个");
    assert_eq!(e.snapshot_stats().snap_seq, cut);
    drop(e);

    let mut e = Engine::open(&d.path(), Durability::Never).expect("reopen");
    for name in [
        "f0".to_string(),
        format!("f{}", N - 1),
        "after".to_string(),
        "tail1".to_string(),
        "tail2".to_string(),
    ] {
        let n = node(ok(
            &mut e,
            &Op::Lookup { view: v, parent: 1, name: name.clone().into_bytes() },
        ));
        assert_eq!(n.nlink, 1, "{name} 没活过旁路快照");
    }
}

/// 退出/显式 checkpoint 会先把在路上的那份收掉：两份抢同一个 tmp，而且
/// 收得晚的那份会把水位往回带。
#[test]
fn explicit_checkpoint_drains_the_background_one_first() {
    let d = Dir::new("bgdrain");
    let mut e = d.open();
    let v = mk_view(&mut e, "v");
    for i in 0..3 {
        ok(
            &mut e,
            &Op::Create {
                view: v,
                parent: 1,
                name: format!("n{i}").into(),
                mode: 0o644,
                uid: 0,
                gid: 0,
            },
        );
    }
    assert!(e.checkpoint_start().expect("start"));
    // 不等它跑完，直接走同步那条：开编前它自己会先 join。
    let seq = e.checkpoint().expect("sync checkpoint");
    assert_eq!(e.snapshot_stats().snap_seq, seq);
    drop(e);

    let mut e = Engine::open(&d.path(), Durability::Always).expect("reopen");
    for i in 0..3 {
        node(ok(
            &mut e,
            &Op::Lookup { view: v, parent: 1, name: format!("n{i}").into_bytes() },
        ));
    }
}
