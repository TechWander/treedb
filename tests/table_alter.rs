//! 通用表「追加列」（`Op::TableAddColumn`，DESIGN §5.1）验收。
//!
//! 五条主张，一条一个用例：
//!
//! 1. **旧行被补齐**：新列的默认值是空字节，宽度校验跟着新 def 走；新列带
//!    索引时，补出来的空值会立刻出现在 `row_index` 里（两行都落进 `("", ...)`
//!    这一桶）。空串本来就是一个合法列值，所以这里不承诺「能区分补出来的空值
//!    与显式写入的空值」—— 那是这条 op 的语义边界，不是实现缺口。
//! 2. **拒绝面**：表不存在 / 列名空或超长或重名 / 已经 64 列 / `COL_UNIQUE`。
//!    失败一次，形状、行宽、`apply_seq` 全都不动。
//! 3. **一条 WAL 记录**：补列只推进一个 seq；把这条记录从 payload 中间截断，
//!    重开后形状与行宽一起退回旧值 —— 不存在「def 是 3 列、行还是 2 列」的
//!    中间态。
//! 4. **过 checkpoint 与重开**：状态逐字段相等（含 `row_index`），因为快照
//!    载入时 `index_row` 会按新 def 自然重建新增列的索引项。
//! 5. **dump → restore**：dump 里是「新 def + 补齐后的行」，重建出来的状态与
//!    源逐字段相等；格式与 `SNAP_VERSION` 一个字节都没动。
//!
//! 跑法：`cargo test --test table_alter`

use std::path::PathBuf;
use tempfile::TempDir;
use treedb::engine::core::WAL_FILE;
use treedb::engine::model::{
    errno, ColumnDef, Op, Reply, RowRec, TableDef, COL_INDEX, COL_UNIQUE, COND_NONE, MAX_COLUMNS,
    ROW_PK, TABLE_NAME_MAX,
};
use treedb::engine::{dump_dir, restore_dump, Durability, Engine, Log, State};

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

fn bad(e: &mut Engine, op: &Op) -> (u16, String) {
    match sub(e, op) {
        Reply::Err { code, msg } => (code, String::from_utf8_lossy(&msg).into_owned()),
        other => panic!("{op:?} → expected Reply::Err, got {other:?}"),
    }
}

fn b(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

fn col(name: &str, flags: u32) -> ColumnDef {
    ColumnDef {
        name: b(name),
        flags,
    }
}

fn def(name: &str, columns: Vec<ColumnDef>) -> TableDef {
    TableDef {
        name: b(name),
        columns,
    }
}

fn put(table: &str, key: &str, cols: &[&str]) -> Op {
    Op::RowPut {
        table: b(table),
        key: b(key),
        cols: cols.iter().map(|c| b(c)).collect(),
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

fn add(table: &str, column: ColumnDef) -> Op {
    Op::TableAddColumn {
        table: b(table),
        column,
    }
}

fn shape(e: &mut Engine, table: &str) -> Option<TableDef> {
    match ok(e, &Op::TableGet { name: b(table) }) {
        Reply::Table(d) => d,
        other => panic!("TableGet → {other:?}"),
    }
}

fn row(e: &mut Engine, table: &str, key: &str) -> Option<RowRec> {
    match ok(
        e,
        &Op::RowGet {
            table: b(table),
            key: b(key),
        },
    ) {
        Reply::Row(r) => r,
        other => panic!("RowGet → {other:?}"),
    }
}

fn keys(e: &mut Engine, table: &str, column: u32, value: &str) -> Vec<Vec<u8>> {
    match ok(
        e,
        &Op::RowList {
            table: b(table),
            column,
            value: b(value),
            from: Vec::new(),
            limit: 0,
        },
    ) {
        Reply::Rows { rows, .. } => rows.into_iter().map(|r| r.key).collect(),
        other => panic!("RowList → {other:?}"),
    }
}

fn count(e: &mut Engine, table: &str, column: u32, value: &str) -> u64 {
    match ok(
        e,
        &Op::RowCount {
            table: b(table),
            column,
            value: b(value),
        },
    ) {
        Reply::Count(n) => n,
        other => panic!("RowCount → {other:?}"),
    }
}

// ---------------------------------------------------------------- 补齐与索引

#[test]
fn add_column_widens_every_row_and_indexes_the_default_value() {
    let dir = Dir::new("alter-widen");
    let mut e = dir.open();
    ok(
        &mut e,
        &Op::TableCreate {
            def: def("kv", vec![col("a", COL_UNIQUE), col("b", COL_INDEX)]),
        },
    );
    ok(&mut e, &put("kv", "k1", &["x", "1"]));
    ok(&mut e, &put("kv", "k2", &["y", "2"]));

    let seq0 = e.apply_seq();
    let new_def = match ok(&mut e, &add("kv", col("c", COL_INDEX))) {
        Reply::Table(Some(d)) => d,
        other => panic!("追加列应回新形状，得到 {other:?}"),
    };
    assert_eq!(new_def.columns.len(), 3);
    assert_eq!(new_def.columns[2].name, b"c");
    assert_eq!(
        e.apply_seq() - seq0,
        1,
        "补列只产一条日志（一条 WAL 记录 = 崩溃时整张表一起在或一起不在）"
    );
    assert_eq!(
        shape(&mut e, "kv").as_ref(),
        Some(&new_def),
        "TableGet 必须回追加后的形状"
    );

    // 两条旧行都被补齐，补出来的是空字节。
    for k in ["k1", "k2"] {
        let r = row(&mut e, "kv", k).expect("旧行不能丢");
        assert_eq!(r.cols.len(), 3, "{k} 必须被补到新宽度");
        assert_eq!(r.cols[2], Vec::new(), "补出来的列值是空字节");
    }
    // 新列的索引项已经补上：两行都落在 ("c", "") 这一桶里，按主键序。
    assert_eq!(keys(&mut e, "kv", 2, ""), vec![b("k1"), b("k2")]);
    assert_eq!(count(&mut e, "kv", 2, ""), 2);

    // 宽度校验跟着新 def 走：2 列被拒、3 列能进。
    let (code, msg) = bad(&mut e, &put("kv", "k3", &["z", "3"]));
    assert_eq!(code, errno::EINVAL);
    assert!(msg.contains("3"), "报错要点出 def 现在是 3 列：{msg}");
    ok(&mut e, &put("kv", "k3", &["z", "3", "new"]));
    assert_eq!(keys(&mut e, "kv", 2, "new"), vec![b("k3")]);

    // 原有约束照旧：唯一列撞值仍 EEXIST，全表列举仍按主键序。
    assert_eq!(
        bad(&mut e, &put("kv", "k4", &["x", "4", "v"])).0,
        errno::EEXIST
    );
    assert_eq!(
        keys(&mut e, "kv", ROW_PK, ""),
        vec![b("k1"), b("k2"), b("k3")]
    );
}

// ---------------------------------------------------------------- 拒绝面

#[test]
fn add_column_refusals_leave_the_shape_alone() {
    let dir = Dir::new("alter-refuse");
    let mut e = dir.open();
    ok(
        &mut e,
        &Op::TableCreate {
            def: def("kv", vec![col("a", COL_UNIQUE), col("b", 0)]),
        },
    );
    ok(&mut e, &put("kv", "k1", &["x", "1"]));
    let def0 = shape(&mut e, "kv").unwrap();
    let seq0 = e.apply_seq();

    assert_eq!(bad(&mut e, &add("nope", col("c", 0))).0, errno::ENOENT);
    let (code, msg) = bad(&mut e, &add("kv", col("a", 0)));
    assert_eq!(code, errno::EINVAL, "重名列");
    assert!(msg.contains("already exists"), "{msg}");
    let (code, msg) = bad(&mut e, &add("kv", col("u", COL_UNIQUE)));
    assert_eq!(code, errno::EINVAL, "唯一列没法回填");
    assert!(msg.contains("COL_UNIQUE"), "{msg}");
    assert_eq!(bad(&mut e, &add("kv", col("", 0))).0, errno::EINVAL);
    assert_eq!(
        bad(&mut e, &add("kv", col(&"n".repeat(TABLE_NAME_MAX + 1), 0))).0,
        errno::ENAMETOOLONG
    );

    // 一次失败一个字节都不动：形状、行宽、水位全停在原处。
    assert_eq!(shape(&mut e, "kv").unwrap(), def0);
    assert_eq!(row(&mut e, "kv", "k1").unwrap().cols.len(), 2);
    assert_eq!(e.apply_seq(), seq0, "失败的追加列不产日志");

    // 64 列上限：正好 64 列的表再追加一列 → EINVAL。
    let cols: Vec<ColumnDef> = (0..MAX_COLUMNS).map(|i| col(&format!("c{i}"), 0)).collect();
    ok(
        &mut e,
        &Op::TableCreate {
            def: def("wide", cols),
        },
    );
    let seq_after_create = e.apply_seq();
    assert_eq!(bad(&mut e, &add("wide", col("one-more", 0))).0, errno::EINVAL);
    assert_eq!(e.apply_seq(), seq_after_create, "受限的追加列也不产日志");
}

// ---------------------------------------------------------------- 一条记录

/// 帧格式见 DESIGN §6.1：`seq(8) crc(4) len(4) reserved(4)` + payload。
/// `len == 0` 是预分配空洞，扫描到此为止。
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

/// 把追加列那条记录从 payload 中间截断：重开后形状与全部旧行一起退回 2 列。
///
/// 这条用例精确地钉住 `Log::AddColumn` 是**一条**记录：若实现退回「一条
/// `PutTable` + 每行一条 `PutRow`」，同样这一刀只会切掉后半截，重放出来就是
/// 「def 3 列、部分行 2 列」的非法中间态 —— 那正是这条日志存在的理由。
#[test]
fn add_column_is_one_record_and_a_torn_tail_keeps_the_old_shape() {
    let dir = Dir::new("alter-torn");
    {
        let mut e = dir.open();
        ok(
            &mut e,
            &Op::TableCreate {
                def: def("kv", vec![col("a", COL_UNIQUE), col("b", COL_INDEX)]),
            },
        );
        ok(&mut e, &put("kv", "k1", &["x", "1"]));
        ok(&mut e, &put("kv", "k2", &["y", "2"]));

        let seq0 = e.apply_seq();
        ok(&mut e, &add("kv", col("c", COL_INDEX)));
        assert_eq!(e.apply_seq() - seq0, 1, "补列只推进一个 seq");
        assert_eq!(row(&mut e, "kv", "k1").unwrap().cols.len(), 3);

        let bytes = std::fs::read(dir.wal_path()).unwrap();
        let (off, len) = last_record(&bytes).expect("WAL 里至少有建表/两行/追加列几条记录");
        let cut = off + len / 2;
        assert!(cut > off + 20, "得真的切在 payload 里");
        std::fs::File::options()
            .write(true)
            .open(dir.wal_path())
            .unwrap()
            .set_len(cut as u64)
            .unwrap();
    }

    let mut e = dir.open();
    let d = shape(&mut e, "kv").expect("表本身在更早的记录里，必须还在");
    assert_eq!(d.columns.len(), 2, "整条记录丢：形状必须停在追加之前");
    for k in ["k1", "k2"] {
        let r = row(&mut e, "kv", k).expect("旧行在更早的记录里，必须还在");
        assert_eq!(r.cols.len(), 2, "{k} 不许出现「def 2 列、行 3 列」的错位");
    }
    assert_eq!(row(&mut e, "kv", "k1").unwrap().cols[0], b("x"));
    // 新列既然不存在，按它列举就该被拒（不是空页）。
    assert_eq!(
        bad(
            &mut e,
            &Op::RowList {
                table: b("kv"),
                column: 2,
                value: Vec::new(),
                from: Vec::new(),
                limit: 0,
            }
        )
        .0,
        errno::EINVAL
    );
    // 旧索引照旧可用。
    assert_eq!(count(&mut e, "kv", 1, "1"), 1);
}

// ---------------------------------------------------------------- 快照与 dump

#[test]
fn widened_table_survives_checkpoint_and_dump_restore() {
    let src = Dir::new("alter-snap-src");
    let before: State;
    {
        let mut e = src.open();
        ok(
            &mut e,
            &Op::TableCreate {
                def: def("kv", vec![col("a", COL_UNIQUE), col("b", COL_INDEX)]),
            },
        );
        ok(&mut e, &put("kv", "k1", &["x", "1"]));
        ok(&mut e, &put("kv", "k2", &["y", "2"]));
        ok(&mut e, &add("kv", col("c", COL_INDEX)));
        ok(&mut e, &put("kv", "k3", &["z", "3", "pp"]));
        ok(&mut e, &Op::Checkpoint);
        assert_eq!(e.wal_len(), 0, "同步 checkpoint 之后 WAL 应为空");
        // 快照之后再写一行：dump 必须走「快照 + 重放尾巴」。
        ok(&mut e, &put("kv", "k4", &["w", "4", "qq"]));
        assert!(e.wal_len() > 0);
        before = e.state_snapshot();
    }

    // 重开：状态逐字段相等 —— 含 `row_index`（它不在快照里，只能由新 def
    // 重建出来；重建漏了新列，这条断言就会红）。
    {
        let mut e = src.open();
        assert_eq!(e.state_snapshot(), before, "checkpoint + 重开必须逐字段相等");
        assert_eq!(shape(&mut e, "kv").unwrap().columns.len(), 3);
        assert_eq!(count(&mut e, "kv", 2, ""), 2, "补出来的空值索引也要回来");
        assert_eq!(count(&mut e, "kv", 2, "pp"), 1);
        assert_eq!(count(&mut e, "kv", 2, "qq"), 1);
    }

    let dumped = dump_dir(&src.path()).expect("dump");
    let dst = Dir::new("alter-snap-dst");
    restore_dump(&dst.path(), &dumped).expect("restore");
    {
        let mut e = dst.open();
        assert_eq!(e.state_snapshot(), before, "dump → restore 必须逐字段相等");
        assert_eq!(keys(&mut e, "kv", 2, ""), vec![b("k1"), b("k2")]);
        assert_eq!(keys(&mut e, "kv", ROW_PK, ""), vec![b("k1"), b("k2"), b("k3"), b("k4")]);
        // 恢复出来的表还能接着追加（def 与行宽都在点上）。
        ok(&mut e, &add("kv", col("d", 0)));
        assert_eq!(row(&mut e, "kv", "k4").unwrap().cols.len(), 4);
    }

    // 导出的是「终态」而不是历史：那次追加列不增加日志条数，只是把同一条
    // `PutTable` 与四行 `PutRow` 写成新形状。
    let again = dump_dir(&dst.path()).expect("re-dump");
    assert_eq!(
        again.logs.len(),
        dumped.logs.len(),
        "dump 条数由终态决定，与「历史里发生过几次 alter」无关"
    );
    let tables: Vec<&Log> = again
        .logs
        .iter()
        .filter(|l| matches!(l, Log::PutTable { .. }))
        .collect();
    assert_eq!(tables.len(), 1);
    match tables[0] {
        Log::PutTable { def, .. } => assert_eq!(def.columns.len(), 4, "重导的 def 也必须是新形状"),
        other => panic!("只筛了 PutTable，却拿到 {other:?}"),
    }
}

// ---------------------------------------------------------------- 重放幂等

/// 同一条 `Log::AddColumn` 重放两次必须与重放一次等价：第二次列数差为 0，
/// 既不重复补行、也不把索引写乱。崩溃恢复路径上的「重复 apply」全靠这条。
#[test]
fn add_column_log_replays_idempotently() {
    let mut s = State::new();
    s.apply(&Log::PutTable {
        id: 0,
        def: def("kv", vec![col("a", COL_UNIQUE), col("b", COL_INDEX)]),
    });
    for (k, v) in [("k1", "x"), ("k2", "y")] {
        s.apply(&Log::PutRow {
            table: 0,
            row: RowRec {
                key: b(k),
                cols: vec![b(v), b("1")],
            },
        });
    }
    let alter = Log::AddColumn {
        id: 0,
        def: def(
            "kv",
            vec![col("a", COL_UNIQUE), col("b", COL_INDEX), col("c", COL_INDEX)],
        ),
    };
    s.apply(&alter);
    let once = s.clone();
    // 唯一列隐含索引，所以 a/b/c 三条都建了索引 × 两行 = 6 项。
    assert_eq!(s.row_index.len(), 6, "三条索引列 × 两行");
    assert_eq!(s.rows[&0][b"k1".as_slice()].cols.len(), 3);
    s.apply(&alter);
    assert_eq!(s, once, "重放第二次必须等价于一次");
}
