//! 快照升级工具的验收（DESIGN.md §6.5）。
//!
//! 「v3 数据目录」是这样造出来的：用当前引擎把状态写全、`checkpoint`，再把快照
//! 文件按 v3 布局重写一遍（`State::save_v3` 就是那代布局的权威写出器，四张已删
//! 控制面表的计数段写 0）。这比去历史二进制里捞一个文件更好也不更差：字节由
//! 同一个编码器产生，只是多写了 v3 那 24 字节计数段，于是任何「迁移后状态不等」
//! 都只能来自迁移逻辑，不会来自样本本身。
//!
//! 三条不可让的判据贯穿全部用例：**状态逐字段相等**、**WAL 一个字节没动**、
//! **拒动的时候原文件真的没被动过**。
//!
//! 跑法：`cargo test --test migrate`

use std::path::PathBuf;
use tempfile::TempDir;
use treedb::engine::core::{
    migrate_snapshot, probe_snapshot, MigrationOutcome, SNAP_BACKUP, SNAP_FILE, WAL_FILE,
};
use treedb::engine::model::{
    errno, ColumnDef, Node, Op, Reply, TableDef, COND_NONE, COL_INDEX,
    COL_UNIQUE, SA_DATA_REF,
};
use treedb::engine::{Durability, Engine, W, SNAP_LEGACY_VERSION, SNAP_MAGIC, SNAP_TRAIL, SNAP_TRAIL_LEGACY, SNAP_VERSION};

// ---------------------------------------------------------------- 脚手架

struct Dir {
    tmp: TempDir,
}

impl Dir {
    fn new() -> Dir {
        Dir { tmp: tempfile::tempdir().expect("tempdir") }
    }
    fn path(&self) -> PathBuf {
        self.tmp.path().join("treedb")
    }
    fn open(&self) -> Engine {
        Engine::open(&self.path(), Durability::Always).expect("engine open")
    }
    fn snap(&self) -> PathBuf {
        self.path().join(SNAP_FILE)
    }
    fn wal(&self) -> PathBuf {
        self.path().join(WAL_FILE)
    }
    fn backup(&self) -> PathBuf {
        self.path().join(SNAP_BACKUP)
    }
    /// 不存在时给空 Vec：这些读全部只在「有没有被动过」的断言里用。
    fn bytes(&self, p: &PathBuf) -> Vec<u8> {
        std::fs::read(p).unwrap_or_default()
    }
    fn write(&self, p: &PathBuf, data: &[u8]) {
        std::fs::write(p, data).unwrap();
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

/// v3 那代布局装得下的全部段，每段都写点东西进去。
/// 返回 `(view, 文件 ino)` 给后面的语义面断言用。
fn seed(e: &mut Engine) -> (u32, u64) {
    let view = match ok(e, &Op::CreateView { name: b"alice".to_vec() }) {
        Reply::View { id, root } => {
            assert_eq!(root, 1);
            id
        }
        other => panic!("{other:?}"),
    };
    let dir = node(ok(
        e,
        &Op::Mkdir { view, parent: 1, name: b"d".to_vec(), mode: 0o755, uid: 1, gid: 1 },
    ))
    .ino;
    let f1 = node(ok(
        e,
        &Op::Create { view, parent: 1, name: b"a.txt".to_vec(), mode: 0o644, uid: 1, gid: 1 },
    ))
    .ino;
    node(ok(
        e,
        &Op::Create { view, parent: dir, name: b"b.txt".to_vec(), mode: 0o644, uid: 1, gid: 1 },
    ));
    // refs 账本：data_ref 是不透明字节，v2 起就是这个形状（v1 → v2 那次改的）。
    ok(
        e,
        &Op::Setattr {
            view,
            ino: f1,
            valid: SA_DATA_REF,
            mode: 0,
            uid: 0,
            gid: 0,
            size: 0,
            atime: 0,
            mtime: 0,
            data_ref: b"1:4096:dev7".to_vec(),
            ref_bytes: 4096,
        },
    );
    ok(
        e,
        &Op::LockSet { view, ino: f1, owner: b"o1".to_vec(), verb: 0, kind: 1, lease_expiry: 0 },
    );
    (view, f1)
}

/// checkpoint 一次，然后把快照文件重写成 v3 布局。返回那份 v3 字节。
///
/// checkpoint 之后 WAL 是空的，所以「旧二进制后来又写了」这类场景要由调用方在这
/// 之后继续写 —— 那几条只会落在 WAL 里，正是迁移最该保住的东西。
fn downgrade_to_v3(d: &Dir, e: &mut Engine) -> Vec<u8> {
    let seq = e.checkpoint().expect("checkpoint");
    let mut w = W::new();
    e.state_snapshot().save_v3(&mut w);
    let mut raw = w.finish();
    raw.extend_from_slice(&seq.to_le_bytes());
    d.write(&d.snap(), &raw);
    raw
}

// ---------------------------------------------------------------- 用例

/// 主路径：状态逐字段相等、WAL 不动、备份就是原件、重开走的是严格入口。
#[test]
fn migration_preserves_every_field_and_leaves_the_wal_alone() {
    let d = Dir::new();
    let mut e = d.open();
    let (view, f1) = seed(&mut e);
    let raw_v3 = downgrade_to_v3(&d, &mut e);

    // 假装旧二进制又接了几条写：它们只进 WAL，快照里没有。
    let late = node(ok(
        &mut e,
        &Op::Create { view, parent: 1, name: b"late.txt".to_vec(), mode: 0o600, uid: 2, gid: 2 },
    ));
    let expect = e.state_snapshot();
    drop(e);
    assert_eq!(expect.nodes.len(), 5, "脚手架规模变了就重算这条基准");

    let wal_before = d.bytes(&d.wal());
    let info = probe_snapshot(&d.path()).unwrap();
    assert_eq!(info.version, SNAP_LEGACY_VERSION);
    assert!(info.needs_migration() && !info.is_current());

    let out = migrate_snapshot(&d.path(), false).unwrap();
    let MigrationOutcome::Migrated { before, after, backup, seq, .. } = out else {
        panic!("expected a real migration, got {out:?}");
    };
    assert_eq!(before.version, SNAP_LEGACY_VERSION);
    assert_eq!(after.version, SNAP_VERSION);
    assert!(after.is_current() && !after.needs_migration());
    assert!(before.snap_seq > 0 && after.snap_seq == before.snap_seq, "水位必须原样");
    assert_eq!(seq, after.snap_seq);
    // 迁移不碰日志：这条断言是「已 ack 的写不会因迁移而丢」那句承诺的证据。
    assert_eq!(d.bytes(&d.wal()), wal_before, "WAL 被迁移动过了");
    assert_eq!(before.wal_disk_bytes, after.wal_disk_bytes);
    // 备份 = 原件逐字节，回滚因此只是一次 copy。
    assert_eq!(d.bytes(&backup), raw_v3);
    // 增量是负的：正文少 24 字节（四个已删控制面表的 u32 计数 + 一个 u64 序号），
    // 尾巴从 8 字节长到 16 字节（crc32 尾巴）。这条是 §6.5「v3 → v4 就是抽掉
    // 一段」那个前提的数值版：哪天 v3 写出侧没跟着改，这里会先炸。
    let body_v3 = raw_v3.len() - SNAP_TRAIL_LEGACY;
    assert_eq!(
        after.bytes as usize,
        body_v3 - 24 + SNAP_TRAIL,
        "v4 只该比 v3 少 24 字节正文、换 16 字节尾巴：{} -> {}",
        raw_v3.len(),
        after.bytes
    );
    // 迁移写出来的是**带 crc 的新尾巴**，而不是旧 8 字节尾巴的替身。
    let migrated = d.bytes(&d.snap());
    assert_eq!(&migrated[migrated.len() - 4..], &SNAP_MAGIC.to_le_bytes());

    // 重开走严格入口：能开门本身就是「新版能接这个目录」的判据。
    let mut e2 = d.open();
    assert_eq!(e2.state_snapshot(), expect, "迁移后重开的状态与崩溃前不等");
    let st = e2.state_snapshot();
    assert!(st.tables.is_empty() && st.rows.is_empty() && st.row_index.is_empty());
    assert_eq!(
        node(ok(&mut e2, &Op::Lookup { view, parent: 1, name: b"late.txt".to_vec() })),
        late,
        "只进过 WAL 的那条写没被迁移保住"
    );
    assert_eq!(node(ok(&mut e2, &Op::Getattr { view, ino: f1 })).data_ref, b"1:4096:dev7".to_vec());
    match ok(&mut e2, &Op::LockQuery { view, ino: f1 }) {
        Reply::Locks(v) => assert_eq!(v.len(), 1, "迁移前那条锁得还在"),
        other => panic!("{other:?}"),
    }
    match ok(&mut e2, &Op::RefLookup { data_ref: b"1:4096:dev7".to_vec() }) {
        Reply::Ref { count, bytes } => {
            assert_eq!((count, bytes), (1, 4096), "refs 账本必须逐字段搬过来")
        }
        other => panic!("{other:?}"),
    }
}

/// seq 尾巴必须照搬：它一旦被改写，下一次恢复要么把已覆盖的记录重放两遍，
/// 要么把没覆盖的当成已覆盖而丢掉（checkpoint 后 WAL 空着的那条路径尤其脆）。
#[test]
fn seq_watermark_survives_the_migration_and_numbering_continues() {
    let d = Dir::new();
    let mut e = d.open();
    let (view, _) = seed(&mut e);
    let seq = e.checkpoint().expect("checkpoint");
    let mut w = W::new();
    e.state_snapshot().save_v3(&mut w);
    let mut raw = w.finish();
    raw.extend_from_slice(&seq.to_le_bytes());
    d.write(&d.snap(), &raw);
    drop(e); // WAL 此刻是空的：迁移后发号只能从快照水位接回来。

    let out = migrate_snapshot(&d.path(), false).unwrap();
    let MigrationOutcome::Migrated { seq: got, after, .. } = out else { panic!("{out:?}") };
    assert_eq!(got, seq);
    assert_eq!(after.snap_seq, seq, "尾巴上的水位必须与旧文件逐位相同");

    let mut e2 = d.open();
    assert_eq!(e2.snap_seq(), seq);
    node(ok(
        &mut e2,
        &Op::Create { view, parent: 1, name: b"after1".to_vec(), mode: 0o644, uid: 0, gid: 0 },
    ));
    match ok(&mut e2, &Op::Stats) {
        Reply::Stats(s) => assert!(s.seq > seq, "新写必须接着旧水位发号，不能从 1 重发"),
        other => panic!("{other:?}"),
    }
    drop(e2);
    // 尾巴算错了的话，这次重开会以「seq 跳号」硬错收场（`d.open()` 直接 panic）。
    let e3 = d.open();
    let st = e3.state_snapshot();
    assert!(
        st.dentries.iter().flat_map(|(_, m)| m.keys()).any(|n| n == b"after1"),
        "重开后新写的那条不在了"
    );
    assert_eq!(st.nodes.len(), 5);
}

/// 升完再升一次必须无事可做，而且不能留下第二份备份、不能改一个字节。
#[test]
fn migration_is_idempotent_and_leaves_a_current_dir_untouched() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    downgrade_to_v3(&d, &mut e);
    let expect = e.state_snapshot();
    drop(e);

    let first = migrate_snapshot(&d.path(), false).unwrap();
    assert!(matches!(first, MigrationOutcome::Migrated { .. }), "{first:?}");
    let snap = d.bytes(&d.snap());
    let wal = d.bytes(&d.wal());
    let backup = d.bytes(&d.backup());
    assert!(!backup.is_empty(), "第一次迁移必须留下备份");

    let second = migrate_snapshot(&d.path(), false).unwrap();
    assert!(matches!(second, MigrationOutcome::AlreadyCurrent(_)), "{second:?}");
    assert_eq!(d.bytes(&d.snap()), snap, "第二次动手改了快照");
    assert_eq!(d.bytes(&d.wal()), wal);
    assert_eq!(d.bytes(&d.backup()), backup, "幂等重跑不该重写备份");

    let e2 = d.open();
    assert_eq!(e2.state_snapshot(), expect);
}

/// 认不认识的版本位一律拒动，而且拒得干净：文件没改，备份也没落。
#[test]
fn unknown_version_is_refused_without_touching_anything() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    let raw_v3 = downgrade_to_v3(&d, &mut e);
    drop(e);

    // 版本位换成 5：正文还是那份正文，所以「按 5 解释」会静默解错，必须拒。
    let mut bogus = raw_v3.clone();
    bogus[..4].copy_from_slice(&5u32.to_le_bytes());
    d.write(&d.snap(), &bogus);

    let info = probe_snapshot(&d.path()).unwrap();
    assert_eq!(info.version, 5);
    assert!(!info.decodable);
    assert!(info
        .note
        .clone()
        .unwrap()
        .contains("neither writes nor migrates"));
    assert!(!info.is_current() && !info.needs_migration());

    let err = migrate_snapshot(&d.path(), false).unwrap_err().to_string();
    assert!(err.contains("refusing to touch"), "{err}");
    assert_eq!(d.bytes(&d.snap()), bogus, "拒动的时候一个字节都不该改");
    assert!(!d.backup().exists(), "没动手就不该留下备份");
}

/// 正文与版本位不吻合（尾巴多一个字节）= 这不是它自称的那个文件。
/// 这条测的是 `State::load_for_migration` 那道「正文之后恰好 16 字节尾巴」的闸。
#[test]
fn body_that_does_not_match_its_version_is_refused() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    let mut raw_v3 = downgrade_to_v3(&d, &mut e);
    drop(e);
    raw_v3.push(0); // 尾巴变 17 字节
    d.write(&d.snap(), &raw_v3);

    let info = probe_snapshot(&d.path()).unwrap();
    assert_eq!(info.version, SNAP_LEGACY_VERSION);
    assert!(!info.decodable, "多出来的字节必须让严格解码失败");
    assert!(info.note.unwrap().contains("cannot be decoded"));

    let err = migrate_snapshot(&d.path(), false).unwrap_err().to_string();
    assert!(err.contains("refusing to touch"), "{err}");
    assert_eq!(d.bytes(&d.snap()), raw_v3);
    assert!(!d.backup().exists());
}

/// 备份是上一轮唯一可回滚的原件，默认不覆盖；要换得说 `--force`。
#[test]
fn existing_backup_is_not_clobbered_without_force() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    let raw_v3 = downgrade_to_v3(&d, &mut e);
    let expect = e.state_snapshot();
    drop(e);

    let junk = b"someone else's file".to_vec();
    d.write(&d.backup(), &junk);

    let err = migrate_snapshot(&d.path(), false).unwrap_err().to_string();
    assert!(err.contains("--force"), "{err}");
    assert_eq!(d.bytes(&d.backup()), junk, "不加 --force 就动了别人的文件");
    assert_eq!(d.bytes(&d.snap()), raw_v3, "被备份闸挡住时快照不该被动");

    let out = migrate_snapshot(&d.path(), true).unwrap();
    assert!(matches!(out, MigrationOutcome::Migrated { .. }), "{out:?}");
    assert_eq!(d.bytes(&d.backup()), raw_v3, "--force 就该把它换成原件");
    let e2 = d.open();
    assert_eq!(e2.state_snapshot(), expect);
}

/// 迁移后的目录不是「能开门就谢天谢地」的半成品：T2 那面通用表要能正常用，
/// `next_table_id` 从 0 起算也不能撞号。
#[test]
fn migrated_dir_can_use_the_generic_tables() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    downgrade_to_v3(&d, &mut e);
    drop(e);
    migrate_snapshot(&d.path(), false).unwrap();

    let mut e2 = d.open();
    let def = TableDef {
        name: b"kv".to_vec(),
        columns: vec![
            ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE },
            ColumnDef { name: b"b".to_vec(), flags: COL_INDEX },
        ],
    };
    match ok(&mut e2, &Op::TableCreate { def: def.clone() }) {
        Reply::Table(Some(x)) => assert_eq!(x, def),
        other => panic!("{other:?}"),
    }
    ok(
        &mut e2,
        &Op::RowPut {
            table: b"kv".to_vec(),
            key: b"r1".to_vec(),
            cols: vec![b"a1".to_vec(), b"x".to_vec()],
            flags: 0,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        },
    );
    // 唯一列撞一次：约束必须在迁移过的目录上照常生效。
    let code = match e2
        .submit(&Op::RowPut {
            table: b"kv".to_vec(),
            key: b"r2".to_vec(),
            cols: vec![b"a1".to_vec(), b"y".to_vec()],
            flags: 0,
            cond_col: COND_NONE,
            cond_val: Vec::new(),
        })
        .unwrap()
    {
        Reply::Err { code, .. } => code,
        other => panic!("expected an error reply, got {other:?}"),
    };
    assert_eq!(code, errno::EEXIST);

    let expect = e2.state_snapshot();
    assert_eq!(expect.tables.len(), 1);
    assert_eq!(expect.row_count(), 1);
    let seq = e2.checkpoint().unwrap();
    drop(e2);
    // 迁移过的目录做完 checkpoint 再重开：表与行必须原样回来，索引原样重推。
    let e3 = d.open();
    assert_eq!(e3.state_snapshot(), expect);
    assert_eq!(e3.snap_seq(), seq);
}

/// 空目录 / 只有 WAL 的目录：没有快照就没有可升的东西，这不是错误。
#[test]
fn fresh_dir_has_nothing_to_migrate() {
    let d = Dir::new();
    let info = probe_snapshot(&d.path()).unwrap();
    assert!(!info.present && !info.is_current() && !info.needs_migration());
    assert!(matches!(
        migrate_snapshot(&d.path(), false).unwrap(),
        MigrationOutcome::NoSnapshot(_)
    ));

    // 新目录直接能被当前二进制打开，且写进去的快照本来就是 v4。
    let mut e = d.open();
    seed(&mut e);
    e.checkpoint().unwrap();
    drop(e);
    assert_eq!(probe_snapshot(&d.path()).unwrap().version, SNAP_VERSION);
    assert!(matches!(
        migrate_snapshot(&d.path(), false).unwrap(),
        MigrationOutcome::AlreadyCurrent(_)
    ));
    assert!(!d.backup().exists());
}

// ---------------------------------------------------------------- CLI 面

/// 跑一把真的 `treedb-migrate`。上面那些用例测的是 API，这三条测的是运维
/// 实际看到的两个字：退出码与输出。脚本要拿它们做判断，所以两者都是契约。
fn cli(dir: &PathBuf, args: &[&str]) -> (i32, String) {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_treedb-migrate"))
        .arg("--dir")
        .arg(dir)
        .args(args)
        .output()
        .expect("spawn treedb-migrate");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code().unwrap_or(-1), text)
}

/// 不带 `--apply` 只看：必须 exit 10 并把下一步命令说出来，而且一个字节都
/// 不写。给 `--apply` 之后才真改，改完输出里要有备份路径与回滚命令。
#[test]
fn cli_reports_exit_10_before_applying_and_migrates_after() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    downgrade_to_v3(&d, &mut e);
    let expect = e.state_snapshot();
    drop(e);
    let snap = d.bytes(&d.snap());

    let (code, text) = cli(&d.path(), &[]);
    assert_eq!(code, 10, "{text}");
    assert!(text.contains("needs migration"), "{text}");
    assert!(text.contains("--apply"), "得把下一步命令写出来：{text}");
    assert!(text.contains(&format!("version     = {SNAP_LEGACY_VERSION}")), "{text}");
    assert_eq!(d.bytes(&d.snap()), snap, "不带 --apply 就是只读");
    assert!(!d.backup().exists());

    let (code, text) = cli(&d.path(), &["--apply"]);
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("migrated    = 3 -> 4"), "{text}");
    assert!(text.contains("wal         = untouched"), "{text}");
    assert!(text.contains("rollback    ="), "{text}");
    assert!(text.contains(SNAP_BACKUP), "备份路径要写在输出里：{text}");
    // 回滚提示必须把窗口写在脸上：说「拷回去就行」而不提 checkpoint 会抹掉
    // WAL，就是把丢数据藏在一句好看的话里（§6.5 末）。
    assert!(text.contains("checkpoint"), "回滚窗口要写在输出里：{text}");

    // 升完的目录现在走严格启动路径就能开门。
    let e2 = d.open();
    assert_eq!(e2.state_snapshot(), expect);

    // 再跑一次：无事可做、exit 0。
    let (code, text) = cli(&d.path(), &["--apply"]);
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("already on the current layout"), "{text}");
}

/// 认不认识的版本：exit 1 加一句拒动理由，不能报 0（脚本会以为已升完）、
/// 也不能报 10（那意味着加 --apply 能解决，而它不能）。
#[test]
fn cli_refuses_unknown_version_with_exit_1() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    let mut raw = downgrade_to_v3(&d, &mut e);
    drop(e);
    raw[..4].copy_from_slice(&5u32.to_le_bytes());
    d.write(&d.snap(), &raw);

    let (code, text) = cli(&d.path(), &["--apply"]);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("refusing to touch"), "{text}");
    assert_eq!(d.bytes(&d.snap()), raw, "拒动就得真的没动");
    assert!(!d.backup().exists());
}

/// `--help` 与空目录都是 exit 0；新目录本来就没有可升的东西，不该给运维
/// 一个需要解释的失败码。
#[test]
fn cli_help_and_fresh_dir_exit_zero() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_treedb-migrate"))
        .arg("--help")
        .output()
        .expect("spawn treedb-migrate --help");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(text.contains("treedb-migrate") && text.contains("--apply"), "{text}");

    let d = Dir::new();
    let (code, text) = cli(&d.path(), &["--apply"]);
    assert_eq!(code, 0, "{text}");
    assert!(text.contains("nothing to migrate"), "{text}");
}

// ---- 2026-11-20：快照尾巴补上 crc32（问题清单 F1）----------------------------
//
// 这三条钉住的是「快照坏了要在开门那一步大声失败」，以及「旧目录不必迁移
// 就能开」。它们不必走迁移工具，但属于同一件事：磁盘上的字节与它自称的
// 版本必须能被对上账。

/// checkpoint 之后快照带 16 字节尾巴：magic 在最后 4 字节，水位与前缀都在。
#[test]
fn checkpoint_writes_a_crc_trailer_and_reopens_cleanly() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    let seq = e.checkpoint().expect("checkpoint");
    let snap = d.bytes(&d.snap());
    assert_eq!(&snap[snap.len() - 4..], &SNAP_MAGIC.to_le_bytes(), "新尾巴必须自报家门");
    assert_eq!(
        u64::from_le_bytes(snap[snap.len() - 12..snap.len() - 4].try_into().unwrap()),
        seq,
        "尾巴里的水位必须就是 checkpoint 报的那个"
    );
    drop(e);
    // 能重开 = crc 也验过了（open 的第一步就是 split）。
    let mut e2 = d.open();
    assert_eq!(e2.snapshot_stats().snap_seq, seq);
}

/// 正文任何一个比特被写坏：开门必须失败，且错误消息里写的是 crc 而不是
/// 「解码失败」——运维看到前者会去查盘，看到后者会去查版本。
#[test]
fn corrupted_snapshot_body_is_refused_at_open() {
    let d = Dir::new();
    let mut e = d.open();
    seed(&mut e);
    e.checkpoint().expect("checkpoint");
    drop(e);

    let mut snap = d.bytes(&d.snap());
    let at = snap.len() / 2;
    snap[at] ^= 0x10;
    d.write(&d.snap(), &snap);

    let err = Engine::open(&d.path(), Durability::Always)
        .err()
        .expect("坏快照不该能开")
        .to_string();
    assert!(err.contains("crc32"), "要说清是校验不过：{err}");
    // 只读体检也要照同一个判据说，而且不许报"可迁移"。
    let info = probe_snapshot(&d.path()).unwrap();
    assert!(!info.decodable && !info.needs_migration());
    assert!(info.note.unwrap_or_default().contains("crc32"));
}

/// 旧尾巴（8 字节，没有 crc）照样能开：正文布局一个字节没变，所以这条
/// 补丁不需要迁移工具。下一次 checkpoint 会把它换成新尾巴。
#[test]
fn legacy_eight_byte_trailer_still_opens() {
    let d = Dir::new();
    let mut e = d.open();
    let (view, _) = seed(&mut e);
    let seq = e.checkpoint().expect("checkpoint");
    drop(e);

    // 把 16 字节尾巴降回 8 字节：正文 + seq，一个 crc 都不留。
    let snap = d.bytes(&d.snap());
    let body = &snap[..snap.len() - SNAP_TRAIL];
    let mut legacy = body.to_vec();
    legacy.extend_from_slice(&seq.to_le_bytes());
    d.write(&d.snap(), &legacy);

    let mut e2 = d.open();
    assert_eq!(e2.snapshot_stats().snap_seq, seq);
    // 状态真的在（不是"能开但空的"）：视图还在，文件还在。
    assert_eq!(e2.snapshot_stats().views, 1);
    match e2.submit(&Op::Lookup { view, parent: 1, name: b"a.txt".to_vec() }) {
        Ok(Reply::Node(n)) => assert_eq!(n.name, b"a.txt".to_vec()),
        other => panic!("legacy 尾巴重开后查不到文件：{other:?}"),
    }
    // 再 checkpoint 一次就该长回 16 字节。
    e2.checkpoint().expect("second checkpoint");
    let snap2 = d.bytes(&d.snap());
    assert_eq!(&snap2[snap2.len() - 4..], &SNAP_MAGIC.to_le_bytes());
}
