//! 每个子命令的实现：一条命令 → 一个（或一批）`Op` → 一段给人看的输出。
//!
//! 命名纪律是「子命令名与引擎语义一一对应」，因为这份 CLI 的读者是已经读过
//! DESIGN §5 的人：`row del-range` 就是 `RowRangeDelete`，`--if-absent` 就是
//! `PUT_IF_ABSENT`。不在名字上做包装（比如把 `table clear` 叫成 `truncate`），
//! 否则文档与命令行的说法会分叉，而分叉的代价是让人在排障时对照不上。
//!
//! 三条贯穿全文件的规矩：
//!
//! 1. **引擎是语义的唯一权威**。这里不预先拒绝引擎会拒绝的东西（比如给 `table
//!    add-column` 加 `:u` 标志）—— 让引擎回 `EINVAL`，把它的原话印出来。CLI 自己
//!    判一遍的结果和引擎不一致时，人就会相信错的那一句。
//! 2. **只读命令不写**。`table list` / `row get` / `stats` 走的是引擎里 `is_readonly`
//!    的那几条；任何会改状态的子命令都走提交线程（`--dir` 模式下是本进程的
//!    `commit_batch`），没有一条捷径直接动内存。
//! 3. **一句提示不毁一个字节流**。凡是把数据写到 stdout 的路径（`dump --out -`、
//!    `--json`），说明文字一律走 stderr。

use std::io::Write;

use crate::engine::model::errno;
use crate::engine::model::{
    ColumnDef, Op, Reply, RowRec, TableDef,
    COND_NONE, DEL_IF_EXISTS, MAX_BATCH_OPS, MAX_COLUMNS, PUT_IF_ABSENT, PUT_IF_EXISTS, RENAME_NOREPLACE,
    ROW_PK, TABLE_NAME_MAX,
};

use super::conn::{seq_probe, Fatal, Session};
use super::value;

/// 一次命令的落点。`out` 是数据，`err` 是解释与提示 —— 两条都必须活着到进程结束。
pub struct Ctx<'a> {
    pub s: &'a mut Session,
    pub out: &'a mut dyn Write,
    pub err: &'a mut dyn Write,
    /// `--json`：机器可读输出。
    pub json: bool,
    /// 写入后要不要额外发两次 `Stats` 去判断「同值消除」。`--no-seq-check` 关掉。
    pub seq_check: bool,
    /// REPL 里 `use <table>` 设的默认表；非交互模式永远是 `None`。
    pub table: Option<Vec<u8>>,
}

/// 退出码。与既有 bin 的口径一致（0 成功 / 1 失败 / 2 用法错），只多开一档：
/// `3` 表示 op 根本没被求值（连不上、目录被占、协议不同代），让脚本能把「引擎说不行」
/// 与「没地方去问引擎」分开处理。
pub const EX_OK: i32 = 0;
pub const EX_ENGINE: i32 = 1;
pub const EX_USAGE: i32 = 2;
pub const EX_TRANSPORT: i32 = 3;

/// errno 数值 → POSIX 名字。打印时带上它，运维看到的是「errno 17 (EEXIST)」而不是
/// 一个孤零零的 17。
pub fn errno_name(code: u16) -> &'static str {
    match code {
        errno::EPERM => "EPERM",
        errno::ENOENT => "ENOENT",
        errno::EIO => "EIO",
        errno::EBADF => "EBADF",
        errno::EAGAIN => "EAGAIN",
        errno::EACCES => "EACCES",
        errno::EBUSY => "EBUSY",
        errno::EEXIST => "EEXIST",
        errno::EXDEV => "EXDEV",
        errno::ENODEV => "ENODEV",
        errno::ENOTDIR => "ENOTDIR",
        errno::EISDIR => "EISDIR",
        errno::EINVAL => "EINVAL",
        errno::ENOSPC => "ENOSPC",
        errno::EROFS => "EROFS",
        errno::ENAMETOOLONG => "ENAMETOOLONG",
        errno::ENOTEMPTY => "ENOTEMPTY",
        errno::EADV => "EADV",
        errno::ESTALE => "ESTALE",
        _ => "?",
    }
}

/// 用法错误：把话说清该给什么，然后 2。
fn usage(ctx: &mut Ctx, msg: &str) -> i32 {
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":false,\"error\":{}}}",
            value::json_str(msg)
        );
    } else {
        let _ = writeln!(ctx.err, "usage error: {msg}");
    }
    EX_USAGE
}

/// 传输层/开局失败：3。
fn transport(ctx: &mut Ctx, e: Fatal) -> i32 {
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":false,\"fatal\":{}}}",
            value::json_str(&e.0)
        );
    } else {
        let _ = writeln!(ctx.err, "error: {}", e.0);
    }
    EX_TRANSPORT
}

/// 引擎回的 `Reply::Err`：1，并把 code 与 msg 原样印出来。
fn engine_err(ctx: &mut Ctx, code: u16, msg: &[u8]) -> i32 {
    let m = String::from_utf8_lossy(msg).into_owned();
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":false,\"errno\":{code},\"errno_name\":{},\"msg\":{}}}",
            value::json_str(errno_name(code)),
            value::json_str(&m)
        );
    } else {
        let _ = writeln!(
            ctx.err,
            "error: errno {code} ({}): {m}",
            errno_name(code)
        );
    }
    EX_ENGINE
}

/// 把一条应答按「要么成功打印、要么 engine_err」处理掉。
fn checked(ctx: &mut Ctx, reply: Reply) -> Result<Reply, i32> {
    match reply {
        Reply::Err { code, msg } => Err(engine_err(ctx, code, &msg)),
        other => Ok(other),
    }
}

/// 一个 op 的完整路径：发出去、传输层错就 3、语义错就 1。
fn call(ctx: &mut Ctx, op: &Op) -> Result<Reply, i32> {
    let r = ctx.s.call(op).map_err(|e| transport(ctx, e))?;
    checked(ctx, r)
}

/// 把 `Result<_, i32>` 拆成「出错就地返回那个码」。子命令里几乎每一句都在做这件事，写全
/// 样板会淹掉真正要读的那行判断；语义与 `?` 相同，区别只是宿主函数返回 `i32` 而不是
/// `Result` —— 退出码本来就是这里的错误类型，再包一层是给自己添麻烦。
macro_rules! rtry {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(rc) => return rc,
        }
    };
}

/// 写入成功后那句「有没有真的产生日志」。见 DESIGN §5.1 的同值消除一条。
///
/// 判据是 `writes_total` 的增量而不是 `seq`：一次 `RowPut` 值没变 → 零日志 → 不发
/// append、不 fsync、`seq` 与 `writes_total` 都不动。单看 `seq` 在并发下会被别人的写
/// 拱上去，从而把一次同值消除读成一次真写 —— 那两个数一起不动才是这条性质的原话。
///
/// 代价必须说清：这是**两次额外的 `Stats`**，在多连接的服务端上只保证「本命令看到的
/// 前后读数」，不保证中间没有别人的写。要准就 `--no-seq-check` 关掉，然后自己按
/// `stats` 的累计口径核对。
fn write_note(ctx: &mut Ctx, before: Option<(u64, u64)>, what: &str) {
    let Some((seq0, w0)) = before else { return };
    let Some((seq1, w1)) = seq_probe(ctx.s) else { return };
    if w1 > w0 {
        return; // 有日志，正常
    }
    let hint = if seq1 == seq0 { "seq 未前进" } else { "seq 被别人推走了，以 writes 为准" };
    let _ = writeln!(
        ctx.err,
        "note: {what} 未产生日志（同值消除/幂等空操作），{hint} (seq={seq1})"
    );
}

/// `--col` 的三种写法：`pk`、0 基序号、列名。
enum ColSpec {
    Pk,
    Num(u32),
    Name(String),
}

fn parse_col_spec(s: &str) -> Result<ColSpec, String> {
    if s.eq_ignore_ascii_case("pk") || s == "-1" {
        return Ok(ColSpec::Pk);
    }
    if let Ok(n) = s.parse::<u32>() {
        return Ok(ColSpec::Num(n));
    }
    if s.is_empty() {
        return Err("--col needs pk, an index, or a column name".to_string());
    }
    Ok(ColSpec::Name(s.to_string()))
}

/// 把 `ColSpec` 落成引擎要的 `column` 序号。名字这一档要多发一次 `TableGet`：
/// 列名→序号的映射只有引擎有（CLI 不自己存表形，否则改了列两边就分叉）。
fn resolve_col(ctx: &mut Ctx, table: &[u8], spec: &ColSpec) -> Result<u32, i32> {
    match spec {
        ColSpec::Pk => Ok(ROW_PK),
        ColSpec::Num(n) => Ok(*n),
        ColSpec::Name(n) => {
            let Reply::Table(Some(def)) = call(ctx, &Op::TableGet {
                name: table.to_vec(),
            })?
            else {
                let _ = writeln!(ctx.err, "error: no such table {}", value::render_bytes(table));
                return Err(EX_ENGINE);
            };
            let want = n.as_bytes();
            match def.columns.iter().position(|c| c.name == want) {
                Some(i) => Ok(i as u32),
                None => {
                    let names: Vec<String> = def
                        .columns
                        .iter()
                        .map(|c| String::from_utf8_lossy(&c.name).into_owned())
                        .collect();
                    let _ = writeln!(
                        ctx.err,
                        "error: table {} has no column {n} (columns: {})",
                        value::render_bytes(table),
                        names.join(", ")
                    );
                    Err(EX_ENGINE)
                }
            }
        }
    }
}

/// 表名：显式给了就用，否则用 REPL 里 `use` 过的那张。
fn table_arg(ctx: &mut Ctx, args: &[String], i: usize) -> Result<Vec<u8>, i32> {
    if let Some(a) = args.get(i) {
        return Ok(a.clone().into_bytes());
    }
    match &ctx.table {
        Some(t) => Ok(t.clone()),
        None => Err(usage(ctx, "missing table name (or `use <table>` first)")),
    }
}

/// `col[:flags]` 的解析。标志位只认 `i`(INDEX)/`u`(UNIQUE)，与引擎的两个常量一一对应。
fn parse_column(s: &str) -> Result<ColumnDef, String> {
    let (name, flags) = match s.split_once(':') {
        Some((n, f)) => {
            let mut flags = 0u32;
            for c in f.chars() {
                match c {
                    'i' => flags |= crate::engine::model::COL_INDEX,
                    'u' => flags |= crate::engine::model::COL_UNIQUE,
                    '-' => {}
                    other => {
                        return Err(format!(
                            "unknown column flag {other:?} (use `i` = COL_INDEX, `u` = COL_UNIQUE)"
                        ))
                    }
                }
            }
            (n, flags)
        }
        None => (s, 0),
    };
    if name.is_empty() {
        return Err("empty column name".to_string());
    }
    if name.len() > TABLE_NAME_MAX {
        return Err(format!("column name over {TABLE_NAME_MAX} bytes"));
    }
    Ok(ColumnDef {
        name: name.as_bytes().to_vec(),
        flags,
    })
}

/// 值参数表：任一参数按 [`value::parse_value`] 解释。
fn values(args: &[String], from: usize) -> Result<Vec<Vec<u8>>, String> {
    args[from..]
        .iter()
        .map(|a| value::parse_value(a))
        .collect()
}

/// 一个条件参数对（`--cond-col` / `--cond-val`）。没给 col 就是无条件。
struct Cond {
    col: u32,
    val: Vec<u8>,
}

impl Cond {
    fn none() -> Cond {
        Cond { col: COND_NONE, val: Vec::new() }
    }
}

/// 旗标表的一项：`(名字, 值)`，值 `None` 是裸开关（`--if-absent`），`Some` 是带值的
/// （`--cond-val x`）。`scan` 只负责拆形状，值是不是合法留给 `value::parse_value`，
/// 于是 `--eq=hex:00` 这类写法在两个旗标上的行为一致。
type Flag = (String, Option<String>);

/// 一个子命令的参数扫描。`bools` 是不带值的开关，`vals` 是带值的。
///
/// `--flag value` 与 `--flag=value` 两种写法都要吃：人从别处粘命令时两种都会遇到，
/// 而拒绝其中一种的报错（「unknown argument」）看起来像命令自己写错了。
fn scan(
    args: &[String],
    bools: &[&str],
    vals: &[&str],
) -> Result<(Vec<String>, Vec<Flag>), String> {
    let mut pos: Vec<String> = Vec::new();
    let mut flags: Vec<Flag> = Vec::new();
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if a == "--" {
            // 结束旗标解析，剩下的一个都不当开关。键可以是任意字节串，`--if-absent`
            // 这样形状的键是合法的（引擎只比字节）；没有这条出路，人就写不进那种行。
            pos.extend(args[i + 1..].iter().cloned());
            break;
        }
        if let Some(stripped) = a.strip_prefix("--") {
            let (name, inline) = match stripped.split_once('=') {
                Some((n, v)) => (n.to_string(), Some(v.to_string())),
                None => (stripped.to_string(), None),
            };
            // 存的是**不带横线**的名字：`get_val`/`has_flag` 的调用方一律写 `"lo"`、
            // `"if-absent"`，这里存 `--lo` 就会让每一次查询都落空 —— 而落空的形状是
            // 「无条件」，一次本该 CAS 的写会悄悄变成真写。报错文案才重新加横线。
            if bools.contains(&name.as_str()) {
                if inline.is_some() {
                    return Err(format!("--{name} takes no value"));
                }
                flags.push((name, None));
            } else if vals.contains(&name.as_str()) {
                let v = match inline {
                    Some(v) => v,
                    None => {
                        i += 1;
                        args.get(i).cloned().ok_or(format!("--{name} needs a value"))?
                    }
                };
                flags.push((name, Some(v)));
            } else {
                return Err(format!("unknown flag --{name}"));
            }
        } else {
            pos.push(a.clone());
        }
        i += 1;
    }
    Ok((pos, flags))
}

fn get_val(flags: &[Flag], name: &str) -> Option<String> {
    flags.iter().find(|(k, _)| k == name).and_then(|(_, v)| v.clone())
}

fn has_flag(flags: &[Flag], name: &str) -> bool {
    flags.iter().any(|(k, _)| k == name)
}

fn get_num<T: std::str::FromStr>(
    flags: &[Flag],
    name: &str,
    dflt: T,
) -> Result<T, String> {
    match get_val(flags, name) {
        None => Ok(dflt),
        Some(v) => v
            .parse::<T>()
            .map_err(|_| format!("--{name} wants a number, got {v:?}")),
    }
}

/// 条件对：`--cond-col` 可以给列名，此时要先查表形。
///
/// `pk` 这一档必须在这里当场拒掉，不能递给引擎去决定：编码层里 [`ROW_PK`] 与
/// [`COND_NONE`] 是同一个数（`u32::MAX`），而引擎的条件闸门写的是
/// `if cond_col != COND_NONE` —— 递过去的 `pk` 走的是「没有条件」那一条分支，
/// `--cond-val` 整个被忽略，一句本该 CAS 的覆盖写就静默成了裸写。主键 CAS 本身
/// 也是空命题（行就是按主键寻址的，比较 `key == key` 恒真），所以正确出口只有报错：
/// 存在性用 `--if-exists`/`--if-absent`，值用列序号或列名。
fn read_cond(
    ctx: &mut Ctx,
    table: &[u8],
    flags: &[Flag],
) -> Result<Cond, i32> {
    let Some(spec) = get_val(flags, "cond-col") else {
        if has_flag(flags, "cond-val") {
            return Err(usage(ctx, "--cond-val needs --cond-col"));
        }
        return Ok(Cond::none());
    };
    if has_flag(flags, "cond-val") && get_val(flags, "cond-val").is_none() {
        return Err(usage(ctx, "--cond-val needs a value"));
    }
    let spec = parse_col_spec(&spec).map_err(|e| usage(ctx, &e))?;
    if matches!(spec, ColSpec::Pk) {
        return Err(usage(
            ctx,
            "--cond-col cannot be pk: ROW_PK and COND_NONE are the same value, so the \
             engine reads it as \"no condition\" and writes unconditionally. Name a column \
             (index or name) for CAS, or use --if-exists / --if-absent for existence",
        ));
    }
    let col = resolve_col(ctx, table, &spec)?;
    let val = match get_val(flags, "cond-val") {
        Some(v) => value::parse_value(&v).map_err(|e| usage(ctx, &e))?,
        None => Vec::new(),
    };
    Ok(Cond { col, val })
}

// ---------------------------------------------------------------- 子命令入口

/// 唯一的分派点。`argv[0]` 是组名（`table` / `row` / `batch` / `stats` / …）。
pub fn run(ctx: &mut Ctx, argv: &[String]) -> i32 {
    let Some((head, rest)) = argv.split_first() else {
        return usage(ctx, "no command given");
    };
    let (group, verb): (String, Option<String>) = match head.as_str() {
        "table" | "row" => (head.clone(), rest.first().cloned()),
        other => (other.to_string(), None),
    };
    // 组命令把动词也消费掉，非组命令直接进 rest。
    let args: Vec<String> = match &verb {
        Some(v) if matches!(group.as_str(), "table" | "row") => rest[1..].to_vec(),
        _ => rest.to_vec(),
    };
    match (group.as_str(), verb.as_deref()) {
        ("table", Some("list")) => table_list(ctx, &args),
        ("table", Some("desc")) => table_desc(ctx, &args),
        ("table", Some("create")) => table_create(ctx, &args),
        ("table", Some("add-column")) => table_add_column(ctx, &args),
        ("table", Some("rename")) => table_rename(ctx, &args),
        ("table", Some("clear")) => table_clear(ctx, &args),
        ("table", Some("drop")) => table_drop(ctx, &args),
        ("table", None) => usage(ctx, "table needs list|desc|create|add-column|rename|clear|drop"),
        ("row", Some("get")) => row_get(ctx, &args),
        ("row", Some("put")) => row_put(ctx, &args),
        ("row", Some("del")) => row_del(ctx, &args),
        ("row", Some("del-range")) => row_del_range(ctx, &args),
        ("row", Some("list")) => row_list(ctx, &args),
        ("row", Some("range")) => row_range(ctx, &args),
        ("row", Some("count")) => row_count(ctx, &args),
        ("row", None) => usage(ctx, "row needs get|put|del|del-range|list|range|count"),
        ("batch", _) => batch(ctx, &args),
        ("stats", _) => stats(ctx, &args),
        ("ping", _) => {
            match call(ctx, &Op::Ping) {
                Ok(Reply::Pong) => {
                    let _ = writeln!(ctx.out, "pong");
                    EX_OK
                }
                Ok(other) => {
                    let _ = writeln!(ctx.err, "error: unexpected reply {other:?}");
                    EX_TRANSPORT
                }
                Err(rc) => rc,
            }
        }
        ("checkpoint", _) => {
            if !args.is_empty() {
                return usage(ctx, "checkpoint takes no argument");
            }
            control(ctx, &Op::Checkpoint, "checkpoint")
        }
        ("shutdown", _) => {
            if !args.is_empty() {
                return usage(ctx, "shutdown takes no argument");
            }
            control(ctx, &Op::Shutdown, "shutdown")
        }
        ("use", _) => use_table(ctx, &args),
        other => usage(
            ctx,
            &format!("unknown command {}{}", other.0, other.1.map(|v| format!(" {v}")).unwrap_or_default()),
        ),
    }
}

/// `use <table>` 只影响后续同会话里省略表名的命令（REPL 专用）。
fn use_table(ctx: &mut Ctx, args: &[String]) -> i32 {
    let Some(t) = args.first() else {
        ctx.table = None;
        let _ = writeln!(ctx.err, "default table cleared");
        return EX_OK;
    };
    // 当场核实它存在：把 `use` 的失败留在敲下它的那一刻，而不是三条命令之后一条
    // 莫名其妙的 ENOENT。
    match ctx.s.call(&Op::TableGet { name: t.clone().into_bytes() }) {
        Ok(Reply::Table(Some(def))) => {
            ctx.table = Some(def.name.clone());
            let _ = writeln!(ctx.out, "using table {}", value::render_bytes(&def.name));
            EX_OK
        }
        Ok(Reply::Table(None)) => {
            let _ = writeln!(ctx.err, "error: no such table {t}");
            EX_ENGINE
        }
        Ok(Reply::Err { code, msg }) => engine_err(ctx, code, &msg),
        Err(e) => transport(ctx, e),
        Ok(other) => {
            let _ = writeln!(ctx.err, "error: unexpected reply {other:?}");
            EX_TRANSPORT
        }
    }
}

// ------------------------------------------------------------------ 表：只读

fn table_list(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(args, &["rows"], &[]) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    if !pos.is_empty() {
        return usage(ctx, "table list takes no positional argument (maybe --rows?)");
    }
    let want_rows = has_flag(&flags, "rows");
    let Reply::Tables(defs) = rtry!(call(ctx, &Op::TableList)) else {
        let _ = writeln!(ctx.err, "error: unexpected reply");
        return EX_TRANSPORT;
    };
    if ctx.json {
        let mut items = Vec::with_capacity(defs.len());
        for d in &defs {
            let rows = if want_rows {
                rtry!(count_pk(ctx, &d.name))
            } else {
                None
            };
            items.push(table_json(d, rows));
        }
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"tables\":[{}]}}",
            items.join(",")
        );
        return EX_OK;
    }
    if defs.is_empty() {
        let _ = writeln!(ctx.out, "(no tables)");
        return EX_OK;
    }
    let width = defs.iter().map(|d| d.name.len()).max().unwrap_or(4).min(40);
    let _ = writeln!(
        ctx.out,
        "{}  {:>7}  {}",
        value::pad("TABLE", width),
        "COLUMNS",
        if want_rows { "ROWS  INDEXED COLUMNS" } else { "INDEXED COLUMNS" }
    );
    for d in &defs {
        let idx: Vec<String> = d
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.indexed())
            .map(|(i, c)| format!("{i}:{}", String::from_utf8_lossy(&c.name)))
            .collect();
        let rows = if want_rows {
            match count_pk(ctx, &d.name) {
                Ok(Some(n)) => format!("{n:<6}"),
                _ => "-".to_string(),
            }
        } else {
            String::new()
        };
        let _ = writeln!(
            ctx.out,
            "{}  {:>7}  {rows}{}",
            value::cell(&d.name, width),
            d.columns.len(),
            if idx.is_empty() { "—".to_string() } else { idx.join(" ") }
        );
    }
    EX_OK
}

fn table_desc(ctx: &mut Ctx, args: &[String]) -> i32 {
    // 与其余 `table *` 一样走 `table_arg`：REPL 里 `use` 过之后 `table desc` 不带名字
    // 也该能用，否则同一句 `row get` 省得下表名、`table desc` 省不下，规则看着像随手写的。
    let name = match table_arg(ctx, args, 0) {
        Ok(t) => t,
        Err(rc) => return rc,
    };
    let Reply::Table(def) = rtry!(call(ctx, &Op::TableGet { name: name.clone() })) else {
        let _ = writeln!(ctx.err, "error: unexpected reply");
        return EX_TRANSPORT;
    };
    let Some(def) = def else {
        let _ = writeln!(ctx.err, "error: no such table {}", value::render_bytes(&name));
        return EX_ENGINE;
    };
    let rows = count_pk(ctx, &def.name).ok().flatten();
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"table\":{}}}",
            table_json(&def, rows)
        );
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "table {}  columns {}  rows {}",
        value::render_bytes(&def.name),
        def.columns.len(),
        rows.map(|n| n.to_string()).unwrap_or_else(|| "?".into())
    );
    for (i, c) in def.columns.iter().enumerate() {
        let mut f = Vec::new();
        if c.unique() {
            f.push("UNIQUE");
        }
        if c.indexed() {
            f.push("INDEX");
        }
        let _ = writeln!(
            ctx.out,
            "  {i:>3}  {:<20} {}",
            String::from_utf8_lossy(&c.name),
            if f.is_empty() { "—".to_string() } else { f.join("|") }
        );
    }
    let _ = writeln!(
        ctx.out,
        "  pk  {:<20} ROW_PK = 主键本身（按字节序）；--col 可用，--cond-col 不可（与 COND_NONE 同值）",
        "(key)"
    );
    EX_OK
}

fn table_json(d: &TableDef, rows: Option<u64>) -> String {
    let cols: Vec<String> = d
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| {
            format!(
                "{{\"i\":{i},\"name\":{},\"index\":{},\"unique\":{}}}",
                value::json_name(&c.name),
                c.indexed(),
                c.unique()
            )
        })
        .collect();
    let r = rows.map(|n| n.to_string()).unwrap_or_else(|| "null".into());
    format!(
        "{{\"name\":{},\"columns\":[{}],\"rows\":{r}}}",
        value::json_name(&d.name),
        cols.join(",")
    )
}

/// 主键计数（`RowCount{column:ROW_PK}`，引擎侧是 O(1)）。
fn count_pk(ctx: &mut Ctx, name: &[u8]) -> Result<Option<u64>, i32> {
    match ctx.s.call(&Op::RowCount {
        table: name.to_vec(),
        column: ROW_PK,
        value: Vec::new(),
    }) {
        Ok(Reply::Count(n)) => Ok(Some(n)),
        Ok(_) => Ok(None),
        Err(e) => Err(transport(ctx, e)),
    }
}

// ------------------------------------------------------------------ 表：写

fn table_create(ctx: &mut Ctx, args: &[String]) -> i32 {
    if args.len() < 2 {
        return usage(ctx, "table create <name> <col>[:iu] <col>…");
    }
    let name = args[0].clone().into_bytes();
    let mut columns = Vec::with_capacity(args.len() - 1);
    for a in &args[1..] {
        match parse_column(a) {
            Ok(c) => columns.push(c),
            Err(e) => return usage(ctx, &e),
        }
    }
    if columns.len() > MAX_COLUMNS {
        return usage(ctx, &format!("a table has at most {MAX_COLUMNS} columns"));
    }
    let def = TableDef { name, columns };
    let before = if ctx.seq_check { seq_probe(ctx.s) } else { None };
    let Reply::Table(Some(def)) = rtry!(call(ctx, &Op::TableCreate { def })) else {
        return EX_ENGINE;
    };
    write_note(ctx, before, "table create");
    if ctx.json {
        let _ = writeln!(ctx.out, "{{\"ok\":true,\"table\":{}}}", table_json(&def, Some(0)));
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "created table {} with {} columns",
        value::render_bytes(&def.name),
        def.columns.len()
    );
    EX_OK
}

fn table_add_column(ctx: &mut Ctx, args: &[String]) -> i32 {
    if args.len() < 2 {
        return usage(ctx, "table add-column <name> <col>[:i]");
    }
    let name = args[0].clone().into_bytes();
    let column = match parse_column(&args[1]) {
        Ok(c) => c,
        Err(e) => return usage(ctx, &e),
    };
    let before = if ctx.seq_check { seq_probe(ctx.s) } else { None };
    // 引擎会拒 `COL_UNIQUE`（没有回填值），这里不代替它判：让那条 EINVAL 原样出来。
    if call(ctx, &Op::TableAddColumn { table: name.clone(), column: column.clone() }).is_err() {
        return EX_ENGINE;
    }
    write_note(ctx, before, "table add-column");
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"table\":{},\"column\":{{\"name\":{},\"index\":{},\"unique\":{}}}}}",
            value::json_name(&name),
            value::json_name(&column.name),
            column.indexed(),
            column.unique()
        );
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "added column {} to {}",
        value::render_bytes(&column.name),
        value::render_bytes(&name)
    );
    EX_OK
}

fn table_rename(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(args, &["noreplace"], &[]) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    if pos.len() != 2 {
        return usage(ctx, "table rename <from> <to> [--noreplace]");
    }
    let flags_bits = if has_flag(&flags, "noreplace") { RENAME_NOREPLACE } else { 0 };
    let from = pos[0].clone().into_bytes();
    let to = pos[1].clone().into_bytes();
    let before = if ctx.seq_check { seq_probe(ctx.s) } else { None };
    if call(ctx, &Op::TableRename { from: from.clone(), to: to.clone(), flags: flags_bits }).is_err() {
        return EX_ENGINE;
    }
    write_note(ctx, before, "table rename");
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"from\":{},\"to\":{},\"noreplace\":{}}}",
            value::json_name(&from),
            value::json_name(&to),
            flags_bits != 0
        );
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "renamed {} → {}{}",
        value::render_bytes(&from),
        value::render_bytes(&to),
        if flags_bits != 0 { " (noreplace)" } else { "" }
    );
    EX_OK
}

fn table_clear(ctx: &mut Ctx, args: &[String]) -> i32 {
    let Some(name) = args.first() else {
        return usage(ctx, "table clear <name>");
    };
    let name = name.clone().into_bytes();
    let Reply::Count(n) = rtry!(call(ctx, &Op::TableClear { table: name.clone() })) else {
        return EX_ENGINE;
    };
    // 本来就空 → 引擎零日志（`Count(0)`），这句不用 probe 也能说清，所以走专门分支。
    if n == 0 {
        let _ = writeln!(
            ctx.err,
            "note: {} 本来就是空表，未产生日志（seq 未前进）",
            value::render_bytes(&name)
        );
    }
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"table\":{},\"cleared\":{n}}}",
            value::json_name(&name)
        );
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "cleared {n} rows from {} (表名与形状保留)",
        value::render_bytes(&name)
    );
    EX_OK
}

fn table_drop(ctx: &mut Ctx, args: &[String]) -> i32 {
    let Some(name) = args.first() else {
        return usage(ctx, "table drop <name>");
    };
    let name = name.clone().into_bytes();
    let before = if ctx.seq_check { seq_probe(ctx.s) } else { None };
    if call(ctx, &Op::TableDrop { name: name.clone() }).is_err() {
        return EX_ENGINE;
    }
    write_note(ctx, before, "table drop");
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"dropped\":{}}}",
            value::json_name(&name)
        );
        return EX_OK;
    }
    let _ = writeln!(ctx.out, "dropped table {}", value::render_bytes(&name));
    EX_OK
}

// -------------------------------------------------------------------- 行

fn row_get(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(args, &[], &[]) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    let (table, at) = rtry!(split_table(ctx, &pos));
    let Some(k) = pos.get(at) else {
        return usage(ctx, "row get <table> <key>");
    };
    let key = match value::parse_value(k) {
        Ok(k) => k,
        Err(e) => return usage(ctx, &e),
    };
    let _ = flags;
    let Reply::Row(row) = rtry!(call(ctx, &Op::RowGet { table: table.clone(), key: key.clone() })) else {
        return EX_ENGINE;
    };
    let Some(row) = row else {
        // 「没有这一行」在引擎侧不是 errno（`Reply::Row(None)`），但它在脚本里正是
        // 要判的那件事，所以两种模式都给退出码 1。这是全套输出里唯一一个
        // `ok:true` 却非 0 的出口：`ok` 说的是「这一条走通了、引擎没回 errno」，
        // `found` 说的才是答案 —— 机器读 `found`，脚本读退出码，两边各有明确依据。
        if ctx.json {
            let _ = writeln!(
                ctx.out,
                "{{\"ok\":true,\"found\":false,\"table\":{},\"key\":{}}}",
                value::json_name(&table),
                value::json_value(&key)
            );
            return EX_ENGINE;
        }
        let _ = writeln!(ctx.err, "no such row");
        return EX_ENGINE;
    };
    let def = fetch_def(ctx, &table);
    print_row(ctx, def.as_ref(), &row);
    EX_OK
}

fn fetch_def(ctx: &mut Ctx, table: &[u8]) -> Option<TableDef> {
    match ctx.s.call(&Op::TableGet { name: table.to_vec() }) {
        Ok(Reply::Table(d)) => d,
        _ => None,
    }
}

/// 表名与后面位置参数的分界点（返回 `(表名, 下一个参数的下标)`）。
///
/// 没设默认表（非交互模式永远是这种）就是 `pos[0]` 当表名，一个 op 也不多花。设了默认表
/// （REPL 里 `use` 过）时，`pos[0]` 是表名还是键**由引擎说**：发一次 `TableGet`，命中就
/// 是表名，`Table(None)` 就当键。靠数参数个数猜是不行的：一张只有一个列的表与一个只有一个
/// 列值的键，在长度上根本分不出来。
fn split_table(ctx: &mut Ctx, pos: &[String]) -> Result<(Vec<u8>, usize), i32> {
    let Some(first) = pos.first() else {
        return match &ctx.table {
            Some(t) => Ok((t.clone(), 0)),
            None => Err(usage(ctx, "missing table name (or `use <table>` first)")),
        };
    };
    let cand = first.clone().into_bytes();
    let Some(def) = ctx.table.clone() else {
        return Ok((cand, 1));
    };
    match ctx.s.call(&Op::TableGet { name: cand.clone() }) {
        Ok(Reply::Table(Some(_))) => Ok((cand, 1)),
        // 不存在的表名 → 它是键。引擎这里的 `Table(None)` 不是错误（§5.1），所以
        // 不能走 `checked`。
        Ok(Reply::Table(None)) => Ok((def, 0)),
        Ok(Reply::Err { code, msg }) => Err(engine_err(ctx, code, &msg)),
        Err(e) => Err(transport(ctx, e)),
        Ok(other) => Err(transport(ctx, Fatal(format!("TableGet: unexpected reply {other:?}")))),
    }
}

fn row_put(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(
        args,
        &["if-absent", "if-exists"],
        &["cond-col", "cond-val"],
    ) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    let (table, at) = rtry!(split_table(ctx, &pos));
    let Some(k) = pos.get(at) else {
        return usage(
            ctx,
            "row put <table> <key> <col0> <col1>… [--if-absent|--if-exists] [--cond-col C --cond-val V]",
        );
    };
    let key = match value::parse_value(k) {
        Ok(k) => k,
        Err(e) => return usage(ctx, &e),
    };
    let cols = match values(&pos, at + 1) {
        Ok(c) => c,
        Err(e) => return usage(ctx, &e),
    };
    let mut bits = 0u32;
    if has_flag(&flags, "if-absent") {
        bits |= PUT_IF_ABSENT;
    }
    if has_flag(&flags, "if-exists") {
        bits |= PUT_IF_EXISTS;
    }
    // 两个 flag 同时给在引擎侧是 EINVAL。这里提前拦一次是因为这种写法几乎必然是
    // 手滑，而 EINVAL 的原文（"contradict"）对不读源码的人不如一句中文清楚。
    if bits == (PUT_IF_ABSENT | PUT_IF_EXISTS) {
        return usage(
            ctx,
            "--if-absent 与 --if-exists 互相矛盾（引擎按 EINVAL 拒）",
        );
    }
    // CAS 列名写错时必须停下来：默默退化成「无条件覆盖」是把一次本该安全的写变成真写，
    // 而这一条是用户显式要求的安全性。
    let cond = rtry!(read_cond(ctx, &table, &flags));
    let before = if ctx.seq_check { seq_probe(ctx.s) } else { None };
    let op = Op::RowPut {
        table: table.clone(),
        key: key.clone(),
        cols,
        flags: bits,
        cond_col: cond.col,
        cond_val: cond.val.clone(),
    };
    let Reply::Row(row) = rtry!(call(ctx, &op)) else {
        return EX_ENGINE;
    };
    write_note(ctx, before, "row put");
    if ctx.json {
        let def = fetch_def(ctx, &table);
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"table\":{},\"row\":{},\"flags\":\"{bits:x}\",\"cond_col\":{}}}",
            value::json_name(&table),
            row_json(def.as_ref(), row.as_ref()),
            cond_json(&cond)
        );
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "wrote {}{}{}",
        value::render_bytes(&table),
        if bits != 0 { format!(" [{}]", cond_desc(bits, &cond)) } else { String::new() },
        if row.is_some() { "" } else { " (no row back)" }
    );
    if let Some(r) = row {
        let def = fetch_def(ctx, &table);
        print_row(ctx, def.as_ref(), &r);
    }
    EX_OK
}

fn cond_json(cond: &Cond) -> String {
    if cond.col == COND_NONE {
        "null".to_string()
    } else {
        format!("{{\"col\":{},\"val\":{}}}", cond.col, value::json_value(&cond.val))
    }
}

fn cond_desc(bits: u32, cond: &Cond) -> String {
    let mut parts = Vec::new();
    if bits & PUT_IF_ABSENT != 0 {
        parts.push("PUT_IF_ABSENT".to_string());
    }
    if bits & PUT_IF_EXISTS != 0 {
        parts.push("PUT_IF_EXISTS".to_string());
    }
    if cond.col != COND_NONE {
        parts.push(format!("CAS col {}={}", cond.col, value::render_bytes(&cond.val)));
    }
    parts.join(", ")
}

fn row_del(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(args, &["if-exists"], &["cond-col", "cond-val"]) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    let (table, at) = rtry!(split_table(ctx, &pos));
    let Some(k) = pos.get(at) else {
        return usage(ctx, "row del <table> <key> [--if-exists] [--cond-col C --cond-val V]");
    };
    let key = match value::parse_value(k) {
        Ok(k) => k,
        Err(e) => return usage(ctx, &e),
    };
    let bits = if has_flag(&flags, "if-exists") { DEL_IF_EXISTS } else { 0 };
    let cond = rtry!(read_cond(ctx, &table, &flags));
    let before = if ctx.seq_check { seq_probe(ctx.s) } else { None };
    rtry!(call(
        ctx,
        &Op::RowDelete {
            table: table.clone(),
            key: key.clone(),
            flags: bits,
            cond_col: cond.col,
            cond_val: cond.val.clone(),
        },
    ));
    // 幂等分支（行本来就不存在）在这里显式说出来：引擎回的是 `Ok` 而不是 ENOENT，
    // 不说人就会以为删掉了一行。
    write_note(ctx, before, "row del");
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"table\":{},\"key\":{},\"if_exists\":{},\"cond\":{}}}",
            value::json_name(&table),
            value::json_value(&key),
            bits != 0,
            cond_json(&cond)
        );
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "deleted {} key {}",
        value::render_bytes(&table),
        value::render_bytes(&key)
    );
    EX_OK
}

fn row_del_range(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(args, &[], &["col", "lo", "hi", "max"]) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    let table = match table_arg(ctx, &pos, 0) {
        Ok(t) => t,
        Err(rc) => return rc,
    };
    let spec = get_val(&flags, "col").unwrap_or_else(|| "pk".to_string());
    let column = match parse_col_spec(&spec).map_err(|e| usage(ctx, &e)) {
        // 列名查不到就当场报错：退回到 `ROW_PK` 会把一条「扫不存在的索引」变成一次默默
        // 的全主键扫描，人看到的行数是对的，原因却是错的。
        Ok(c) => rtry!(resolve_col(ctx, &table, &c)),
        Err(rc) => return rc,
    };
    let lo = rtry!(parse_opt_bytes(ctx, &flags, "lo"));
    let hi = rtry!(parse_opt_bytes(ctx, &flags, "hi"));
    let max = match get_num(&flags, "max", 0u32) {
        Ok(m) => m,
        Err(e) => return usage(ctx, &e),
    };
    // `lo > hi` 在引擎侧是**空区间**而不是错误（mem.rs 那两道 `BTreeMap::range` 守卫）。
    // 这里提前说一句，是因为"删 0 行"与"参数写反了"在人眼里是两回事。
    if !lo.is_empty() && !hi.is_empty() && lo > hi {
        let _ = writeln!(
            ctx.err,
            "note: lo > hi 是空区间（引擎不报错），本次删 0 行"
        );
    }
    let Reply::Count(n) = rtry!(call(
        ctx,
        &Op::RowRangeDelete { table: table.clone(), column, lo: lo.clone(), hi: hi.clone(), max },
    )) else {
        return EX_ENGINE;
    };
    if n == 0 {
        let _ = writeln!(ctx.err, "note: 区间内没有行，未产生日志（seq 未前进）");
    }
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"table\":{},\"column\":{},\"lo\":{},\"hi\":{},\"max\":{max},\"deleted\":{n}}}",
            value::json_name(&table),
            col_json(column),
            value::json_value(&lo),
            value::json_value(&hi)
        );
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "deleted {n} rows from {} on {}",
        value::render_bytes(&table),
        col_text(column)
    );
    EX_OK
}

fn col_json(column: u32) -> String {
    if column == ROW_PK {
        "\"pk\"".to_string()
    } else {
        format!("{column}")
    }
}

fn col_text(column: u32) -> String {
    if column == ROW_PK {
        "主键 (ROW_PK)".to_string()
    } else {
        format!("column {column}")
    }
}

fn row_list(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(args, &[], &["col", "eq", "after", "limit"]) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    let table = match table_arg(ctx, &pos, 0) {
        Ok(t) => t,
        Err(rc) => return rc,
    };
    let spec = get_val(&flags, "col").unwrap_or_else(|| "pk".to_string());
    let column = match parse_col_spec(&spec).map_err(|e| usage(ctx, &e)) {
        Ok(c) => rtry!(resolve_col(ctx, &table, &c)),
        Err(rc) => return rc,
    };
    let value_eq = rtry!(parse_opt_bytes(ctx, &flags, "eq"));
    let from = rtry!(parse_opt_bytes(ctx, &flags, "after"));
    let limit = match get_num(&flags, "limit", 0u32) {
        Ok(n) => n,
        Err(e) => return usage(ctx, &e),
    };
    if column != ROW_PK && value_eq.is_empty() && get_val(&flags, "eq").is_none() {
        let _ = writeln!(
            ctx.err,
            "note: 索引列的等值列举没给 --eq = 匹配空值那一档（空串本身是合法列值）"
        );
    }
    let Reply::Rows { rows, end } = rtry!(call(
        ctx,
        &Op::RowList {
            table: table.clone(),
            column,
            value: value_eq.clone(),
            from,
            limit,
        },
    )) else {
        return EX_ENGINE;
    };
    pages_out(ctx, &table, &rows, end, "RowList", column, limit)
}

fn row_range(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(
        args,
        &[],
        &["col", "lo", "hi", "after-val", "after-key", "limit"],
    ) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    let table = match table_arg(ctx, &pos, 0) {
        Ok(t) => t,
        Err(rc) => return rc,
    };
    let spec = get_val(&flags, "col").unwrap_or_else(|| "pk".to_string());
    let column = match parse_col_spec(&spec).map_err(|e| usage(ctx, &e)) {
        Ok(c) => rtry!(resolve_col(ctx, &table, &c)),
        Err(rc) => return rc,
    };
    let lo = rtry!(parse_opt_bytes(ctx, &flags, "lo"));
    let hi = rtry!(parse_opt_bytes(ctx, &flags, "hi"));
    let after_val = rtry!(parse_opt_bytes(ctx, &flags, "after-val"));
    let after_key = rtry!(parse_opt_bytes(ctx, &flags, "after-key"));
    let limit = match get_num(&flags, "limit", 0u32) {
        Ok(n) => n,
        Err(e) => return usage(ctx, &e),
    };
    if !lo.is_empty() && !hi.is_empty() && lo > hi {
        let _ = writeln!(ctx.err, "note: lo > hi 是空区间，本页 0 行（引擎不报错）");
    }
    let Reply::Rows { rows, end } = rtry!(call(
        ctx,
        &Op::RowRange {
            table: table.clone(),
            column,
            lo: lo.clone(),
            hi: hi.clone(),
            after_val: after_val.clone(),
            after_key: after_key.clone(),
            limit,
        },
    )) else {
        return EX_ENGINE;
    };
    pages_out(ctx, &table, &rows, end, "RowRange", column, limit)
}

fn parse_opt_bytes(ctx: &mut Ctx, flags: &[Flag], name: &str) -> Result<Vec<u8>, i32> {
    match get_val(flags, name) {
        None => Ok(Vec::new()),
        Some(v) => value::parse_value(&v).map_err(|e| usage(ctx, &format!("--{name}: {e}"))),
    }
}

fn row_count(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(args, &[], &["col", "eq", "lo", "hi", "max"]) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    let table = match table_arg(ctx, &pos, 0) {
        Ok(t) => t,
        Err(rc) => return rc,
    };
    let spec = get_val(&flags, "col").unwrap_or_else(|| "pk".to_string());
    let column = match parse_col_spec(&spec).map_err(|e| usage(ctx, &e)) {
        Ok(c) => rtry!(resolve_col(ctx, &table, &c)),
        Err(rc) => return rc,
    };
    let has_range = get_val(&flags, "lo").is_some() || get_val(&flags, "hi").is_some();
    if has_range {
        // 引擎没有「区间计数」这一条 op：`range_count` 只在 `RowRangeDelete` 的应答里
        // 露面（那是删除的实际行数）。所以要数区间只能在 CLI 侧翻 `RowRange` 页。
        // 这条路的成本必须说清：O(命中数) 的扫描 + 每页一个 op，而不是引擎里的 O(1)。
        let lo = rtry!(parse_opt_bytes(ctx, &flags, "lo"));
        let hi = rtry!(parse_opt_bytes(ctx, &flags, "hi"));
        let max = match get_num(&flags, "max", 0usize) {
            Ok(m) => m,
            Err(e) => return usage(ctx, &e),
        };
        let mut n: usize = 0;
        let mut after_key = Vec::new();
        let mut after_val = Vec::new();
        loop {
            if max != 0 && n >= max {
                break;
            }
            let lim = if max == 0 { 1000 } else { (max - n).min(1000) } as u32;
            let Reply::Rows { rows, end } = rtry!(call(
                ctx,
                &Op::RowRange {
                    table: table.clone(),
                    column,
                    lo: lo.clone(),
                    hi: hi.clone(),
                    after_val: after_val.clone(),
                    after_key: after_key.clone(),
                    limit: lim,
                },
            )) else {
                return EX_ENGINE;
            };
            let got = rows.len();
            if let Some(last) = rows.last() {
                after_key = last.key.clone();
                if column != ROW_PK {
                    after_val = last.cols.get(column as usize).cloned().unwrap_or_default();
                }
            }
            n += got;
            if end || got == 0 {
                break;
            }
        }
        if ctx.json {
            let _ = writeln!(
                ctx.out,
                "{{\"ok\":true,\"table\":{},\"column\":{},\"lo\":{},\"hi\":{},\"count\":{n},\"method\":\"scan\"}}",
                value::json_name(&table),
                col_json(column),
                value::json_value(&lo),
                value::json_value(&hi)
            );
            return EX_OK;
        }
        let _ = writeln!(
            ctx.out,
            "{n} rows in {} on {} (区间计数走的是 RowRange 翻页，非引擎内建)",
            value::render_bytes(&table),
            col_text(column)
        );
        return EX_OK;
    }
    let value_eq = rtry!(parse_opt_bytes(ctx, &flags, "eq"));
    if get_val(&flags, "max").is_some() {
        let _ = writeln!(
            ctx.err,
            "note: RowCount 没有上限参数（等值计数本来就要数完）；--max 只在 --lo/--hi 那档有效"
        );
    }
    let Reply::Count(n) = rtry!(call(
        ctx,
        &Op::RowCount { table: table.clone(), column, value: value_eq.clone() },
    )) else {
        return EX_ENGINE;
    };
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"table\":{},\"column\":{},\"value\":{},\"count\":{n}}}",
            value::json_name(&table),
            col_json(column),
            value::json_value(&value_eq)
        );
        return EX_OK;
    }
    let _ = writeln!(
        ctx.out,
        "{n} rows in {} on {}",
        value::render_bytes(&table),
        if column == ROW_PK { "主键（全表，O(1)）".to_string() } else { format!("{} = {}", col_text(column), value::render_bytes(&value_eq)) }
    );
    EX_OK
}

/// 一页行的统一出口：人类模式是表格，`--json` 是一个对象。
fn pages_out(
    ctx: &mut Ctx,
    table: &[u8],
    rows: &[RowRec],
    end: bool,
    op: &str,
    column: u32,
    limit: u32,
) -> i32 {
    let def = fetch_def(ctx, table);
    if ctx.json {
        let items: Vec<String> = rows.iter().map(|r| row_json(def.as_ref(), Some(r))).collect();
        let cursor = rows.last().map(|r| value::json_value(&r.key));
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"op\":{},\"table\":{},\"column\":{},\"limit\":{limit},\"rows\":[{}],\"end\":{end},\"next_cursor\":{}}}",
            value::json_str(op),
            value::json_name(table),
            col_json(column),
            items.join(","),
            cursor.unwrap_or_else(|| "null".into())
        );
        return EX_OK;
    }
    print_table(ctx, def.as_ref(), rows);
    let _ = writeln!(
        ctx.err,
        "{} rows{}{}",
        rows.len(),
        if end { ", 到底了" } else { ", 还有 —— 下一页 --after " } ,
        if end {
            String::new()
        } else {
            rows.last()
                .map(|r| value::render_bytes(&r.key))
                .unwrap_or_default()
        }
    );
    EX_OK
}

fn row_json(def: Option<&TableDef>, row: Option<&RowRec>) -> String {
    match row {
        None => "null".to_string(),
        Some(r) => {
            let cols: Vec<String> = r
                .cols
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let name = def
                        .and_then(|d| d.columns.get(i))
                        .map(|c| String::from_utf8_lossy(&c.name).into_owned())
                        .unwrap_or_else(|| format!("col{i}"));
                    format!("{{\"i\":{i},\"name\":{},\"value\":{}}}", value::json_str(&name), value::json_value(c))
                })
                .collect();
            format!(
                "{{\"key\":{},\"cols\":[{}]}}",
                value::json_value(&r.key),
                cols.join(",")
            )
        }
    }
}

fn print_row(ctx: &mut Ctx, def: Option<&TableDef>, row: &RowRec) {
    if ctx.json {
        let _ = writeln!(ctx.out, "{{\"ok\":true,\"found\":true,\"row\":{}}}", row_json(def, Some(row)));
        return;
    }
    let _ = writeln!(ctx.out, "key  {}", value::render_bytes(&row.key));
    let width = def
        .map(|d| d.columns.iter().map(|c| c.name.len()).max().unwrap_or(4))
        .unwrap_or(6)
        .min(24);
    for (i, c) in row.cols.iter().enumerate() {
        let name = def
            .and_then(|d| d.columns.get(i))
            .map(|n| String::from_utf8_lossy(&n.name).into_owned())
            .unwrap_or_else(|| format!("col{i}"));
        let _ = writeln!(
            ctx.out,
            "  {}  {}",
            value::pad(&name, width),
            value::cell(c, value::ELIDE_AFTER)
        );
    }
}

fn print_table(ctx: &mut Ctx, def: Option<&TableDef>, rows: &[RowRec]) {
    if rows.is_empty() {
        let _ = writeln!(ctx.out, "(empty)");
        return;
    }
    let ncols = rows.iter().map(|r| r.cols.len()).max().unwrap_or(0);
    let mut heads = vec!["key".to_string()];
    for i in 0..ncols {
        heads.push(
            def.and_then(|d| d.columns.get(i))
                .map(|c| String::from_utf8_lossy(&c.name).into_owned())
                .unwrap_or_else(|| format!("col{i}")),
        );
    }
    let mut widths: Vec<usize> = heads.iter().map(|h| h.chars().count()).collect();
    let mut rendered: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for r in rows {
        let mut line = Vec::with_capacity(ncols + 1);
        line.push(value::cell(&r.key, 32));
        for i in 0..ncols {
            let v = r
                .cols
                .get(i)
                .map(|v| value::cell(v, 40))
                .unwrap_or_else(|| "—".to_string());
            line.push(v);
        }
        for (w, s) in widths.iter_mut().zip(line.iter()) {
            *w = (*w).max(s.chars().count());
        }
        rendered.push(line);
    }
    let _ = writeln!(
        ctx.out,
        "{}",
        heads
            .iter()
            .zip(widths.iter())
            .map(|(h, w)| value::pad(h, *w))
            .collect::<Vec<_>>()
            .join("  ")
    );
    for line in rendered {
        let _ = writeln!(
            ctx.out,
            "{}",
            line.iter()
                .zip(widths.iter())
                .map(|(s, w)| value::pad(s, *w))
                .collect::<Vec<_>>()
                .join("  ")
        );
    }
}

// ------------------------------------------------------------------ batch

fn batch(ctx: &mut Ctx, args: &[String]) -> i32 {
    let (pos, flags) = match scan(args, &[], &["file"]) {
        Ok(v) => v,
        Err(e) => return usage(ctx, &e),
    };
    let mut lines: Vec<String> = Vec::new();
    if let Some(f) = get_val(&flags, "file") {
        let text = if f == "-" {
            let mut buf = String::new();
            use std::io::Read;
            if let Err(e) = std::io::stdin().read_to_string(&mut buf) {
                return transport(ctx, Fatal(format!("read stdin: {e}")));
            }
            buf
        } else {
            match std::fs::read_to_string(&f) {
                Ok(t) => t,
                Err(e) => return transport(ctx, Fatal(format!("read {f}: {e}"))),
            }
        };
        lines.extend(text.lines().map(|l| l.to_string()));
    }
    // 位置参数里每条可以是一个命令（带空格就要引号），也可以用 `;` 分开多条。
    for a in &pos {
        for l in split_statements(a) {
            lines.push(l);
        }
    }
    lines.retain(|l| !l.trim().is_empty() && !l.trim_start().starts_with('#'));
    if lines.is_empty() {
        return usage(ctx, "batch needs sub-ops: batch \"row put t k v\" … | batch --file -");
    }
    let mut ops: Vec<Op> = Vec::with_capacity(lines.len());
    for (i, l) in lines.iter().enumerate() {
        let toks = match tokenize(l) {
            Ok(t) => t,
            Err(e) => return usage(ctx, &format!("batch line {}: {e}", i + 1)),
        };
        if toks.is_empty() {
            continue;
        }
        match compile(ctx, &toks) {
            Ok(Some(op)) => {
                if matches!(op, Op::Batch { .. }) {
                    return usage(ctx, &format!("batch line {}: 不许嵌套 Batch", i + 1));
                }
                if op_is_control(&op) {
                    return usage(ctx, &format!("batch line {}: 控制面 op 不能进批", i + 1));
                }
                ops.push(op);
            }
            Ok(None) => return usage(ctx, &format!("batch line {}: 不是数据 op", i + 1)),
            Err(rc) => return rc,
        }
    }
    if ops.is_empty() {
        return usage(ctx, "batch ended up empty");
    }
    if ops.len() > MAX_BATCH_OPS {
        return usage(ctx, &format!("a batch holds at most {MAX_BATCH_OPS} ops (MAX_BATCH_OPS), got {}", ops.len()));
    }
    let before = if ctx.seq_check { seq_probe(ctx.s) } else { None };
    let n = ops.len();
    let Reply::Batch { replies } = rtry!(call(ctx, &Op::Batch { ops })) else {
        // 任何一条回 Err 时整批不落，`call` 已经把那条 errno 与下标印出来了。
        return EX_ENGINE;
    };
    write_note(ctx, before, &format!("batch of {n}"));
    if ctx.json {
        let items: Vec<String> = replies
            .iter()
            .map(|r| match r {
                Reply::Ok => "\"ok\"".to_string(),
                Reply::Count(c) => format!("{{\"count\":{c}}}"),
                Reply::Row(row) => format!("{{\"row\":{}}}", row_json(None, row.as_ref())),
                Reply::Table(t) => format!(
                    "{{\"table\":{}}}",
                    t.as_ref().map(|d| table_json(d, None)).unwrap_or_else(|| "null".into())
                ),
                Reply::Rows { rows, .. } => format!("{{\"rows\":{}}}", rows.len()),
                Reply::Pong => "\"pong\"".to_string(),
                // 批内不可能出现 Err —— 任何一条失败都会在顶层先回一条带下标的 Err，整批
                // 不落。这一支只是把 match 穷举补齐，顺便保证真出现时输出仍是可读的。
                Reply::Err { code, msg } => format!(
                    "{{\"errno\":{code},\"errno_name\":{},\"msg\":{}}}",
                    value::json_str(errno_name(*code)),
                    value::json_str(&String::from_utf8_lossy(msg))
                ),
                other => format!("\"{other:?}\""),
            })
            .collect();
        let _ = writeln!(ctx.out, "{{\"ok\":true,\"batch\":[{}]}}", items.join(","));
        return EX_OK;
    }
    let _ = writeln!(ctx.out, "batch of {n} ops applied atomically (one WAL record):");
    for (i, r) in replies.iter().enumerate() {
        let _ = writeln!(ctx.out, "  {i:>2}  {}", reply_text(r));
    }
    EX_OK
}

fn reply_text(r: &Reply) -> String {
    match r {
        Reply::Ok => "Ok".to_string(),
        Reply::Pong => "Pong".to_string(),
        Reply::Count(n) => format!("Count({n})"),
        Reply::Row(Some(row)) => format!("Row({})", value::render_bytes(&row.key)),
        Reply::Row(None) => "Row(none)".to_string(),
        Reply::Table(Some(d)) => format!("Table({})", String::from_utf8_lossy(&d.name)),
        Reply::Table(None) => "Table(none)".to_string(),
        Reply::Rows { rows, end } => format!("Rows({} rows, end={end})", rows.len()),
        Reply::Views(v) => format!("Views({})", v.len()),
        Reply::Batch { replies } => format!("Batch({})", replies.len()),
        Reply::Err { code, msg } => format!(
            "Err errno {code} ({}) {}",
            errno_name(*code),
            String::from_utf8_lossy(msg)
        ),
        other => format!("{other:?}"),
    }
}

/// `Stats` / `Checkpoint` / `Shutdown` / 日志传送 / 握手这几条不进批：引擎按
/// `is_control` 拒（§5.1 那条「Checkpoint 会在批中间把 WAL 截掉」的理由）。CLI 在本地
/// 先认一遍，报错才有「哪条、为什么」的可读形状。
fn op_is_control(op: &Op) -> bool {
    matches!(
        op,
        Op::Ping
            | Op::Stats
            | Op::Checkpoint
            | Op::Shutdown
            | Op::WalFetch { .. }
            | Op::SnapFetch { .. }
            | Op::VersionBegin { .. }
            | Op::AuthBegin { .. }
            | Op::AuthFinish { .. }
            | Op::SecureBegin { .. }
    )
}

/// `checkpoint` / `shutdown`：HELP 里承诺过的两条控制面命令。
///
/// 引擎对它们的应答只有 `Reply::Ok`（`core.rs` 的 Checkpoint 分支；失败是 `EIO`），
/// 所以这里没有别的形状要渲染 —— 只把「成了」说清楚，外加一句不说不行的差别：
/// `--dir` 模式根本没有服务进程可停，敲 `shutdown` 的人必须知道自己停的是空气。
fn control(ctx: &mut Ctx, op: &Op, what: &str) -> i32 {
    let r = rtry!(call(ctx, op));
    if !matches!(r, Reply::Ok) {
        let _ = writeln!(ctx.err, "error: {what}: unexpected reply {r:?}");
        return EX_TRANSPORT;
    }
    if ctx.json {
        let _ = writeln!(ctx.out, "{{\"ok\":true,\"{what}\":true}}");
        return EX_OK;
    }
    let _ = writeln!(ctx.out, "{what}: ok");
    if what == "shutdown" && ctx.s.is_offline() {
        let _ = writeln!(
            ctx.err,
            "note: --dir 模式没有服务进程可停；要优雅关掉一个在跑的服务端，用 --addr"
        );
    }
    EX_OK
}

// ------------------------------------------------------------------ stats

fn stats(ctx: &mut Ctx, args: &[String]) -> i32 {
    if !args.is_empty() {
        return usage(ctx, "stats takes no argument");
    }
    let Reply::Stats(st) = rtry!(call(ctx, &Op::Stats)) else {
        let _ = writeln!(ctx.err, "error: unexpected reply to Stats");
        return EX_TRANSPORT;
    };
    let st = *st;
    if ctx.json {
        let _ = writeln!(
            ctx.out,
            "{{\"ok\":true,\"via\":{},\"views\":{},\"nodes\":{},\"dentries\":{},\"locks\":{},\"refs\":{},\"tables\":{},\"rows\":{},\"seq\":{},\"snap_seq\":{},\"wal_bytes\":{},\"wal_disk_bytes\":{},\"snap_bytes\":{},\"ops_total\":{},\"writes_total\":{},\"syncs_total\":{},\"sync_us_total\":{},\"checkpoints\":{},\"proto_version\":{},\"replica\":{},\"eliminated\":{}}}",
            value::json_str(ctx.s.via),
            st.views,
            st.nodes,
            st.dentries,
            st.locks,
            st.refs,
            st.tables,
            st.rows,
            st.seq,
            st.snap_seq,
            st.wal_bytes,
            st.wal_disk_bytes,
            st.snap_bytes,
            st.ops_total,
            st.writes_total,
            st.syncs_total,
            st.sync_us_total,
            st.checkpoints,
            st.proto_version,
            st.replica,
            // 同值消除的累计条数 = 收到的 op 里没落日志的那些。口径与 DESIGN §5.1 一致：
            // 这是一个可由两个计数器相减还原的观测值，不是另开的计数器。
            st.ops_total.saturating_sub(st.writes_total)
        );
        return EX_OK;
    }
    let avg = st.sync_us_total.checked_div(st.syncs_total).unwrap_or(0);
    let _ = writeln!(ctx.out, "via        = {}", ctx.s.via);
    let _ = writeln!(ctx.out, "views      = {}", st.views);
    let _ = writeln!(ctx.out, "nodes      = {}", st.nodes);
    let _ = writeln!(ctx.out, "dentries   = {}", st.dentries);
    let _ = writeln!(ctx.out, "locks      = {}", st.locks);
    let _ = writeln!(ctx.out, "refs       = {}", st.refs);
    let _ = writeln!(ctx.out, "tables     = {} (rows {})", st.tables, st.rows);
    let _ = writeln!(ctx.out, "seq        = {} (snapshot {})", st.seq, st.snap_seq);
    let _ = writeln!(
        ctx.out,
        "wal        = {} B logical, {} B on disk",
        st.wal_bytes, st.wal_disk_bytes
    );
    let _ = writeln!(ctx.out, "snapshot   = {} B", st.snap_bytes);
    let _ = writeln!(ctx.out, "protocol   = {}", st.proto_version);
    let _ = writeln!(
        ctx.out,
        "ops        = {} (writes {}, syncs {}, avg fsync {} us)",
        st.ops_total, st.writes_total, st.syncs_total, avg
    );
    let _ = writeln!(ctx.out, "eliminated = {} (ops with no log: same-value / idempotent)", st.ops_total.saturating_sub(st.writes_total));
    let _ = writeln!(ctx.out, "checkpoints= {}", st.checkpoints);
    if st.replica {
        let _ = writeln!(
            ctx.out,
            "replica    = yes (primary seq {} lag {} applied {} resyncs {} errors {})",
            st.replica_primary_seq, st.replica_lag, st.replica_applied, st.replica_resyncs,
            st.replica_errors
        );
    }
    EX_OK
}

// ------------------------------------------------------------------ 语句切分

/// 一行里按 `;` 切成多条语句，但引号里的分号不算，`batch { … ; … }` 块里的也不算。
///
/// 块是 HELP 里承诺过的形状，所以切句必须认它：不认的话 `batch { a ; b }` 会被切成两条，
/// a 因为不是独立命令报用法错，而 b **会作为一条普通命令真的执行掉** —— 一整批只落了
/// 一半，比直接报错糟得多。所以未闭合的花括号在 [`super::shell`] 里是语法错，不静默放过。
pub fn split_statements(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut q: Option<char> = None;
    let mut esc = false;
    // 花括号块的深度：块里的 `;` 是 batch 的分句，不是语句的结尾。只有 `batch { …` 那个
    // 形状才开块，引号里的花括号不算边界。
    let mut depth: usize = 0;
    let chars: Vec<char> = line.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if esc {
            cur.push(c);
            esc = false;
            i += 1;
            continue;
        }
        match c {
            '\\' if q == Some('\'') => {
                cur.push(c);
                esc = true;
            }
            '\\' => {
                cur.push(c);
                esc = true;
            }
            '"' | '\'' => {
                if q == Some(c) {
                    q = None;
                } else if q.is_none() {
                    q = Some(c);
                }
                cur.push(c);
            }
            // 开块只在「这条语句到此为止正好是 `batch`」时成立：一行里 `;` 可以分好几
            // 条语句，判据必须是当前这一段而不是整行。
            '{' if q.is_none() && depth == 0 && standalone(&chars, i) && batch_opens_block(&cur) => {
                depth += 1;
                cur.push(c);
            }
            '}' if q.is_none() && depth > 0 && standalone(&chars, i) => {
                depth -= 1;
                cur.push(c);
            }
            ';' if q.is_none() && depth == 0 => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
        i += 1;
    }
    out.push(cur);
    out
}

/// 这个花括号是否独立成词（前后是空白、分号或行的边界）。
fn standalone(cs: &[char], i: usize) -> bool {
    let edge = |c: char| c.is_whitespace() || c == ';';
    let left_ok = i == 0 || edge(cs[i - 1]);
    let right_ok = i + 1 >= cs.len() || edge(cs[i + 1]);
    left_ok && right_ok
}

/// 读到一个独立成词的 `{` 时，它前面那段是不是 `batch`。只有这种时候花括号才是块
/// 的边界 —— 否则一个值恰好是 `{` 的行（`row put t k {`）会把后面的 `;` 吞掉。
///
/// 两头都要 `trim`：一行里 `;` 可以分好几条语句，`use t; batch { … }` 的那种块前面
/// 就留着一个空格，只裁尾部会判成「不是 batch」，于是块从中间被切开。
fn batch_opens_block(cur: &str) -> bool {
    cur.trim() == "batch"
}

/// `batch { 子 op ; 子 op }` 的块形状：在**原文**上解，不在 token 上解 —— 引号只有在
/// 原文里才还留着，块里每条子 op 之后再各自 tokenize。不是这个形状就返回 `None`，
/// 由调用方按普通 `batch …` 处理。
///
/// 块必须写在一行里：REPL 没有「等闭合花括号」的续行机制，一个跨行的块只会以
/// `batch {` 结尾，那属于语法错，不该静默当成一条没写完的批。
pub fn batch_block(stmt: &str) -> Option<Vec<String>> {
    let s = stmt.trim();
    let rest = s.strip_prefix("batch")?.trim_start();
    let inner = rest.strip_prefix('{')?;
    let inner = inner.trim_end().strip_suffix('}')?;
    let mut subs: Vec<String> = Vec::new();
    for p in split_statements(inner) {
        let p = p.trim();
        if p.is_empty() || p.starts_with('#') {
            continue;
        }
        subs.push(p.to_string());
    }
    Some(subs)
}

/// 把一行切成参数，认单双引号。双引号里保留 `\xNN` 之类的转义（交给
/// [`value::parse_value`]），所以这里只管**分组**，不管反转义 —— 两层各做一件事，
/// 否则 `row put t k "a\nb"` 会被剥两次转义变成真换行。
pub fn tokenize(line: &str) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut q: Option<char> = None;
    let mut has = false;
    let mut esc = false;
    for c in line.chars() {
        if esc {
            cur.push(c);
            esc = false;
            continue;
        }
        match c {
            _ if q == Some('\'') && c == '\'' => q = None,
            _ if q.is_some() && Some(c) == q => q = None,
            '\\' => {
                cur.push(c);
                esc = true;
                has = true;
            }
            '"' => q = Some('"'),
            '\'' => {
                q = Some('\'');
                has = true;
            }
            c if c.is_whitespace() && q.is_none() => {
                if has {
                    out.push(std::mem::take(&mut cur));
                    has = false;
                }
            }
            c => {
                cur.push(c);
                has = true;
            }
        }
    }
    if q.is_some() {
        return Err("unterminated quote".to_string());
    }
    if has {
        out.push(cur);
    }
    Ok(out)
}

/// 把一串 token 编成一个 `Op`。这是 REPL 与 `batch` 共用的那一层。
///
/// 返回 `Ok(None)` = 这条命令不是 op（`help`、`connect`、`!`…），由调用方处置。
pub fn compile(ctx: &mut Ctx, toks: &[String]) -> Result<Option<Op>, i32> {
    let Some(head) = toks.first() else {
        return Ok(None);
    };
    let rest = &toks[1..];
    match (head.as_str(), rest.first().map(|s| s.as_str())) {
        // 位置参数与旗标的分界一律交给 `scan`：在 `rest[1]` 上直接拿表名会把
        // `row list --col x` 里的 `--col` 当成表名。
        ("row", Some("get")) => {
            let (pos, _) = scan(&rest[1..], &[], &[]).map_err(|e| usage(ctx, &e))?;
            let (table, at) = split_table(ctx, &pos)?;
            let Some(k) = pos.get(at) else {
                return Err(usage(ctx, "row get <table> <key>"));
            };
            let key = value::parse_value(k).map_err(|e| usage(ctx, &e))?;
            Ok(Some(Op::RowGet { table, key }))
        }
        ("row", Some("put")) => {
            let (pos, flags) =
                scan(&rest[1..], &["if-absent", "if-exists"], &["cond-col", "cond-val"])
                    .map_err(|e| usage(ctx, &e))?;
            let (table, at) = split_table(ctx, &pos)?;
            let Some(k) = pos.get(at) else {
                return Err(usage(ctx, "row put <table> <key> <col0> <col1>… [flags]"));
            };
            let key = value::parse_value(k).map_err(|e| usage(ctx, &e))?;
            let cols = values(&pos, at + 1).map_err(|e| usage(ctx, &e))?;
            let mut bits = 0u32;
            if has_flag(&flags, "if-absent") {
                bits |= PUT_IF_ABSENT;
            }
            if has_flag(&flags, "if-exists") {
                bits |= PUT_IF_EXISTS;
            }
            if bits == (PUT_IF_ABSENT | PUT_IF_EXISTS) {
                return Err(usage(ctx, "--if-absent 与 --if-exists 互相矛盾"));
            }
            let cond = read_cond(ctx, &table, &flags)?;
            Ok(Some(Op::RowPut {
                table,
                key,
                cols,
                flags: bits,
                cond_col: cond.col,
                cond_val: cond.val,
            }))
        }
        ("row", Some("del")) => {
            let (pos, flags) =
                scan(&rest[1..], &["if-exists"], &["cond-col", "cond-val"]).map_err(|e| usage(ctx, &e))?;
            let (table, at) = split_table(ctx, &pos)?;
            let Some(k) = pos.get(at) else {
                return Err(usage(ctx, "row del <table> <key> [--if-exists]"));
            };
            let key = value::parse_value(k).map_err(|e| usage(ctx, &e))?;
            let bits = if has_flag(&flags, "if-exists") { DEL_IF_EXISTS } else { 0 };
            let cond = read_cond(ctx, &table, &flags)?;
            Ok(Some(Op::RowDelete {
                table,
                key,
                flags: bits,
                cond_col: cond.col,
                cond_val: cond.val,
            }))
        }
        ("row", Some("del-range")) => {
            let (pos, flags) =
                scan(&rest[1..], &[], &["col", "lo", "hi", "max"]).map_err(|e| usage(ctx, &e))?;
            let table = table_arg(ctx, &pos, 0)?;
            let spec = get_val(&flags, "col").unwrap_or_else(|| "pk".to_string());
            let cspec = parse_col_spec(&spec).map_err(|e| usage(ctx, &e))?;
            let column = resolve_col(ctx, &table, &cspec)?;
            Ok(Some(Op::RowRangeDelete {
                table,
                column,
                lo: parse_opt_bytes(ctx, &flags, "lo")?,
                hi: parse_opt_bytes(ctx, &flags, "hi")?,
                max: get_num(&flags, "max", 0u32).map_err(|e| usage(ctx, &e))?,
            }))
        }
        ("row", Some("list")) => {
            let (pos, flags) =
                scan(&rest[1..], &[], &["col", "eq", "after", "limit"]).map_err(|e| usage(ctx, &e))?;
            let table = table_arg(ctx, &pos, 0)?;
            let spec = get_val(&flags, "col").unwrap_or_else(|| "pk".to_string());
            let cspec = parse_col_spec(&spec).map_err(|e| usage(ctx, &e))?;
            let column = resolve_col(ctx, &table, &cspec)?;
            Ok(Some(Op::RowList {
                table,
                column,
                value: parse_opt_bytes(ctx, &flags, "eq")?,
                from: parse_opt_bytes(ctx, &flags, "after")?,
                limit: get_num(&flags, "limit", 0u32).map_err(|e| usage(ctx, &e))?,
            }))
        }
        ("row", Some("range")) => {
            let (pos, flags) = scan(
                &rest[1..],
                &[],
                &["col", "lo", "hi", "after-val", "after-key", "limit"],
            )
            .map_err(|e| usage(ctx, &e))?;
            let table = table_arg(ctx, &pos, 0)?;
            let spec = get_val(&flags, "col").unwrap_or_else(|| "pk".to_string());
            let cspec = parse_col_spec(&spec).map_err(|e| usage(ctx, &e))?;
            let column = resolve_col(ctx, &table, &cspec)?;
            Ok(Some(Op::RowRange {
                table,
                column,
                lo: parse_opt_bytes(ctx, &flags, "lo")?,
                hi: parse_opt_bytes(ctx, &flags, "hi")?,
                after_val: parse_opt_bytes(ctx, &flags, "after-val")?,
                after_key: parse_opt_bytes(ctx, &flags, "after-key")?,
                limit: get_num(&flags, "limit", 0u32).map_err(|e| usage(ctx, &e))?,
            }))
        }
        ("row", Some("count")) => {
            let (pos, flags) =
                scan(&rest[1..], &[], &["col", "eq", "lo", "hi", "max"]).map_err(|e| usage(ctx, &e))?;
            let table = table_arg(ctx, &pos, 0)?;
            let spec = get_val(&flags, "col").unwrap_or_else(|| "pk".to_string());
            let cspec = parse_col_spec(&spec).map_err(|e| usage(ctx, &e))?;
            let column = resolve_col(ctx, &table, &cspec)?;
            // 批里只放引擎那条 `RowCount`（等值/全表）。区间计数要翻页，一个 op 装不下，
            // 所以在批的语境里明确拒绝，而不是悄悄换成一个更贵的形状。
            if get_val(&flags, "lo").is_some() || get_val(&flags, "hi").is_some() {
                let _ = writeln!(
                    ctx.err,
                    "error: 区间计数得翻页，不能进 batch；用 row count --lo/--hi 单发"
                );
                return Err(EX_USAGE);
            }
            Ok(Some(Op::RowCount {
                table,
                column,
                value: parse_opt_bytes(ctx, &flags, "eq")?,
            }))
        }
        ("table", Some("list")) => Ok(Some(Op::TableList)),
        ("table", Some("desc")) => {
            let (pos, _) = scan(&rest[1..], &[], &[]).map_err(|e| usage(ctx, &e))?;
            Ok(Some(Op::TableGet {
                name: table_arg(ctx, &pos, 0)?,
            }))
        }
        ("table", Some("create")) if rest.len() >= 2 => {
            let (pos, flags) = scan(&rest[1..], &[], &[]).map_err(|e| usage(ctx, &e))?;
            let _ = flags;
            if pos.len() < 2 {
                return Err(usage(ctx, "table create <name> <col>[:iu] …"));
            }
            let mut columns = Vec::new();
            for a in &pos[1..] {
                columns.push(parse_column(a).map_err(|e| usage(ctx, &e))?);
            }
            Ok(Some(Op::TableCreate {
                def: TableDef { name: pos[0].clone().into_bytes(), columns },
            }))
        }
        ("table", Some("add-column")) if rest.len() >= 3 => {
            let column = parse_column(&rest[2]).map_err(|e| usage(ctx, &e))?;
            Ok(Some(Op::TableAddColumn {
                table: rest[1].clone().into_bytes(),
                column,
            }))
        }
        ("table", Some("rename")) if rest.len() >= 3 => {
            let (pos, flags) = scan(&rest[1..], &["noreplace"], &[]).map_err(|e| usage(ctx, &e))?;
            if pos.len() != 2 {
                return Err(usage(ctx, "table rename <from> <to> [--noreplace]"));
            }
            Ok(Some(Op::TableRename {
                from: pos[0].clone().into_bytes(),
                to: pos[1].clone().into_bytes(),
                flags: if has_flag(&flags, "noreplace") { RENAME_NOREPLACE } else { 0 },
            }))
        }
        ("table", Some("clear")) if rest.len() >= 2 => Ok(Some(Op::TableClear {
            table: rest[1].clone().into_bytes(),
        })),
        ("table", Some("drop")) if rest.len() >= 2 => Ok(Some(Op::TableDrop {
            name: rest[1].clone().into_bytes(),
        })),
        ("batch", _) => Err(usage(ctx, "batch 不能嵌套进 batch")),
        ("row", _) | ("table", _) => Err(usage(
            ctx,
            &format!("incomplete row/table command: {}", rest.join(" ")),
        )),
        // help / connect / use / quit 这些不是 op，交给上层判。
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::conn::Opts;
    use crate::engine::core::Durability;

    /// 一份临时目录 + 连在它上面的会话 + 两条输出缓冲。
    ///
    /// 三件事必须活在同一个所有权里：`Ctx` 同时借 `s` 和两块缓冲，而 `TempDir` 必须比
    /// 持有它的引擎活得久（字段按声明顺序析构，所以 `_td` 放最后）。
    struct H {
        s: Session,
        out: Vec<u8>,
        err: Vec<u8>,
        seq_check: bool,
        _td: tempfile::TempDir,
    }

    impl H {
        fn open() -> H {
            let td = tempfile::tempdir().expect("tempdir");
            let o = Opts {
                dir: Some(td.path().to_path_buf()),
                // 测试判的是语义，不是掉电；never 档走的仍是同一条 WAL 写路径。
                durability: Durability::Never,
                ..Default::default()
            };
            let s = Session::open(&o).expect("offline session");
            H { s, out: Vec::new(), err: Vec::new(), seq_check: false, _td: td }
        }

        fn cmd(&mut self, json: bool, table: Option<&str>, argv: &[&str]) -> i32 {
            let v: Vec<String> = argv.iter().map(|s| s.to_string()).collect();
            let mut ctx = Ctx {
                s: &mut self.s,
                out: &mut self.out,
                err: &mut self.err,
                json,
                seq_check: self.seq_check,
                table: table.map(|t| t.as_bytes().to_vec()),
            };
            run(&mut ctx, &v)
        }

        fn run(&mut self, argv: &[&str]) -> i32 {
            self.cmd(false, None, argv)
        }

        /// 跑一条**应当成功**的命令，返回它的 stdout，并清空两块缓冲。
        /// 清空是重点：不清的话下一条断言读的是整段历史，`!o.contains("k3")` 这种
        /// 「这次不该出现」的判断会假失败，而假失败的排查最费人。
        fn ok(&mut self, argv: &[&str]) -> String {
            let rc = self.run(argv);
            let (o, e) = self.drain();
            assert_eq!(rc, EX_OK, "{}\nstdout: {o}\nstderr: {e}", argv.join(" "));
            o
        }

        /// REPL 里 `use t` 之后的形态：默认表已设。
        fn at(&mut self, table: &str, argv: &[&str]) -> i32 {
            self.cmd(false, Some(table), argv)
        }

        fn code(&mut self, op: &Op) -> Reply {
            self.s.call(op).expect("direct call must not fail at transport level")
        }

        fn out(&self) -> String {
            String::from_utf8_lossy(&self.out).into_owned()
        }

        fn err(&self) -> String {
            String::from_utf8_lossy(&self.err).into_owned()
        }

        fn drain(&mut self) -> (String, String) {
            let (o, e) = (self.out(), self.err());
            self.out.clear();
            self.err.clear();
            (o, e)
        }

        /// `people(name, age:i)` 加三行。几乎每条测试都要它，重复的 setup 会把真正
        /// 要读的断言挤下去。
        fn seed(&mut self) {
            assert_eq!(self.run(&["table", "create", "people", "name", "age:i"]), EX_OK);
            for (k, v) in [("alice", "30"), ("bob", "25"), ("carol", "44")] {
                assert_eq!(self.run(&["row", "put", "people", k, k, v]), EX_OK, "seed {k}");
            }
            self.drain();
        }
    }

    /// 15 个通用表 op 每一个都真的走通了 —— 这张表是清单，不是某条性质的证明。
    /// 哪天引擎加第十六条，这里会漏，靠 `tests/cli.rs` 与 DESIGN 的对照表补。
    #[test]
    fn every_table_op_has_a_command() {
        let mut h = H::open();
        // TableCreate
        let o = h.ok(&["table", "create", "widgets", "a", "b:i"]);
        assert!(o.contains("created table widgets"), "{o}");
        // TableList
        let o = h.ok(&["table", "list"]);
        assert!(o.contains("widgets"), "{o}");
        // TableGet
        let o = h.ok(&["table", "desc", "widgets"]);
        assert!(o.contains("columns 2"), "{o}");
        // RowPut
        for (k, v) in [("k1", "7"), ("k2", "8"), ("k3", "9")] {
            h.ok(&["row", "put", "widgets", k, "v", v]);
        }
        // RowGet
        let o = h.ok(&["row", "get", "widgets", "k1"]);
        assert!(o.contains("k1"), "{o}");
        // RowList：主键页
        let o = h.ok(&["row", "list", "widgets"]);
        assert!(o.contains("k1") && o.contains("k3"), "{o}");
        // RowList：索引列等值（引擎侧走 index_page）
        let o = h.ok(&["row", "list", "widgets", "--col", "b", "--eq", "7"]);
        assert!(o.contains("k1") && !o.contains("k3"), "{o}");
        // RowRange：主键区间
        let o = h.ok(&["row", "range", "widgets", "--lo", "k1", "--hi", "k4"]);
        assert!(o.contains("k1") && o.contains("k3"), "{o}");
        // RowRange：索引列区间（引擎侧走 index_range，`--hi` 是开区间）
        let o = h.ok(&["row", "range", "widgets", "--col", "1", "--lo", "7", "--hi", "9"]);
        assert!(o.contains("k1") && o.contains("k2") && !o.contains("k3"), "{o}");
        // RowCount：全表（引擎侧 O(1)）
        let o = h.ok(&["row", "count", "widgets"]);
        assert!(o.starts_with("3 rows") && o.contains("O(1)"), "{o}");
        // RowCount：索引列等值
        let o = h.ok(&["row", "count", "widgets", "--col", "b", "--eq", "8"]);
        assert!(o.starts_with("1 rows"), "{o}");
        // RowCount：区间 —— 引擎没有这条 op，是 CLI 翻 RowRange，所以必须自报方法
        let o = h.ok(&["row", "count", "widgets", "--lo", "k1", "--hi", "k4"]);
        assert!(o.starts_with("3 rows") && o.contains("非引擎内建"), "{o}");
        // Batch（一条 WAL 记录），其中的 RowDelete 得真的生效
        let o = h.ok(&["batch", "row put widgets k4 v 10", "row del widgets k1"]);
        assert!(o.contains("batch of 2"), "{o}");
        assert_eq!(
            h.code(&Op::RowGet { table: b"widgets".to_vec(), key: b"k1".to_vec() }),
            Reply::Row(None)
        );
        // TableAddColumn：只能追加到末尾
        h.ok(&["table", "add-column", "widgets", "c:i"]);
        // TableRename
        h.ok(&["table", "rename", "widgets", "gadgets"]);
        // RowRangeDelete：`--max` 提前截断，报的是实际删除数
        let o = h.ok(&["row", "del-range", "gadgets", "--lo", "k", "--hi", "k9", "--max", "2"]);
        assert!(o.contains("deleted 2 rows"), "{o}");
        // TableClear：留名字与形状，只删行
        let o = h.ok(&["table", "clear", "gadgets"]);
        assert!(o.contains("cleared 1 rows"), "{o}");
        let o = h.ok(&["table", "desc", "gadgets"]);
        assert!(o.contains("columns 3") && o.contains("rows 0"), "{o}");
        // TableDrop
        h.ok(&["table", "drop", "gadgets"]);
        let o = h.ok(&["table", "list"]);
        assert!(o.contains("(no tables)"), "{o}");
        // 可观测那两条：Ping 与 Stats
        let o = h.ok(&["ping"]);
        assert!(o.contains("pong"), "{o}");
        let o = h.ok(&["stats"]);
        assert!(o.contains("via") && o.contains("eliminated"), "{o}");
    }

    /// 四条关键错误路径：退出码都是「引擎回了 errno」那一档，且 POSIX 名字印出来。
    #[test]
    fn key_error_paths_keep_their_posix_names() {
        let mut h = H::open();
        h.seed();
        // EEXIST：--if-absent 打在已存在的键上。
        assert_eq!(h.run(&["row", "put", "people", "alice", "alice", "9", "--if-absent"]), EX_ENGINE);
        assert!(h.err().contains("EEXIST"), "{}", h.err());
        // ENOENT：--if-exists 打在不存在的键上。
        h.drain();
        assert_eq!(h.run(&["row", "put", "people", "nobody", "nobody", "1", "--if-exists"]), EX_ENGINE);
        assert!(h.err().contains("ENOENT"), "{}", h.err());
        // EAGAIN：CAS 条件不命中。
        h.drain();
        assert_eq!(
            h.run(&["row", "put", "people", "alice", "alice", "99", "--cond-col", "age", "--cond-val", "7"]),
            EX_ENGINE
        );
        assert!(h.err().contains("EAGAIN"), "{}", h.err());
        // 行没变：条件命中时应当成功。
        h.drain();
        assert_eq!(
            h.run(&["row", "put", "people", "alice", "alice", "99", "--cond-col", "age", "--cond-val", "30"]),
            EX_OK
        );
        // EINVAL：列数不对（引擎的原话，不替它改写成 CLI 自己的说法）。
        h.drain();
        assert_eq!(h.run(&["row", "put", "people", "dave", "onlyone"]), EX_ENGINE);
        assert!(h.err().contains("EINVAL"), "{}", h.err());
        // 同一个 errno 在 --json 里是可判的字段，不只藏在文案里。
        h.drain();
        assert_eq!(h.cmd(true, None, &["row", "put", "people", "alice", "x", "9", "--if-absent"]), EX_ENGINE);
        let (o, _) = h.drain();
        assert!(o.contains("\"errno\":17") && o.contains("\"errno_name\":\"EEXIST\""), "{o}");
    }

    /// 回归：`scan` 存的是不带横线的名字。存成 `--lo` 时每一项查询都落空，而落空的形状
    /// 是「无条件 / 全区间」—— 一条本该 CAS 的会变成真写，一条本该删 2 行的会删 3 行。
    #[test]
    fn flags_are_matched_by_their_bare_names() {
        let mut h = H::open();
        h.seed();
        // --if-absent 必须真的落到 PUT_IF_ABSENT 上：吞掉它这句话就会静默成功。
        assert_eq!(h.run(&["row", "put", "people", "alice", "alice", "30", "--if-absent"]), EX_ENGINE);
        assert!(h.err().contains("EEXIST"), "{}", h.err());
        // --if-exists 必须真的落到 PUT_IF_EXISTS 上：吞掉它这句会新建一行。
        h.drain();
        assert_eq!(h.run(&["row", "put", "people", "ghost", "ghost", "1", "--if-exists"]), EX_ENGINE);
        assert!(h.err().contains("ENOENT"), "{}", h.err());
        assert_eq!(h.code(&Op::RowGet { table: b"people".to_vec(), key: b"ghost".to_vec() }), Reply::Row(None));
        // --cond-col/--cond-val 必须真的组成 CAS。
        h.drain();
        assert_eq!(
            h.run(&["row", "put", "people", "alice", "alice", "99", "--cond-col", "age", "--cond-val", "1"]),
            EX_ENGINE
        );
        assert!(h.err().contains("EAGAIN"), "{}", h.err());
        // --col/--eq 必须走索引那一条：吞掉它这句变成按主键列举，三行全回来。
        h.drain();
        let (o, _) = { h.run(&["row", "list", "people", "--col", "age", "--eq", "30"]); h.drain() };
        assert!(o.contains("alice") && !o.contains("bob") && !o.contains("carol"), "{o}");
        // --limit 必须落到页宽上（表里此刻还是三行，一页只该回一行 + 表头）。
        assert_eq!(h.run(&["row", "list", "people", "--limit", "1"]), EX_OK);
        let (o, e) = h.drain();
        assert!(o.matches('\n').count() == 2 && o.contains("alice") && !o.contains("carol"), "{o}");
        assert!(e.contains("还有"), "{e}");
        // --lo/--hi/--max 必须落到 RowRangeDelete 上：吞掉它这句会清空整张表。
        assert_eq!(h.run(&["row", "del-range", "people", "--max", "1"]), EX_OK);
        let (o, _) = h.drain();
        assert!(o.contains("deleted 1 rows"), "{o}");
        // `[b, ca)` 只盖住 bob：`carol` 的首字母小于 `d`，用 `--hi d` 会把两张都圈进来。
        assert_eq!(h.run(&["row", "del-range", "people", "--lo", "b", "--hi", "ca"]), EX_OK);
        let (o, _) = h.drain();
        assert!(o.contains("deleted 1 rows"), "{o}");
        // 剩下的那一行必须还在：两次 --max/--lo 各删一行，谁都不该多删。
        assert_eq!(h.run(&["row", "count", "people"]), EX_OK);
        assert!(h.out().starts_with("1 rows"), "{}", h.out());
        h.drain();
    }

    /// 列名写错必须停下来。退回 `ROW_PK` 的话，人看到的行数照样是「对」的，原因是错的。
    #[test]
    fn a_bad_column_name_is_an_error_not_a_pk_fallback() {
        let mut h = H::open();
        h.seed();
        for argv in [
            vec!["row", "list", "people", "--col", "nosuch"],
            vec!["row", "range", "people", "--col", "nosuch"],
            vec!["row", "count", "people", "--col", "nosuch"],
            vec!["row", "del-range", "people", "--col", "nosuch"],
            vec!["row", "put", "people", "alice", "alice", "9", "--cond-col", "nosuch", "--cond-val", "1"],
        ] {
            assert_eq!(h.run(&argv), EX_ENGINE, "{argv:?}");
            let (_, e) = h.drain();
            assert!(e.contains("no column"), "{argv:?} -> {e}");
        }
        // 越界序号同样不放过：引擎按「条件指向不存在的列」回 EINVAL。
        assert_eq!(h.run(&["row", "count", "people", "--col", "9"]), EX_ENGINE);
        assert!(h.err().contains("EINVAL"), "{}", h.err());
        // `pk` 与 0 基序号是合法写法。
        h.drain();
        assert_eq!(h.run(&["row", "count", "people", "--col", "pk"]), EX_OK);
        assert_eq!(h.run(&["row", "count", "people", "--col", "1", "--eq", "30"]), EX_OK);
        // 没建索引的列也一样要拒 —— 退化成全表扫是把成本藏起来。
        assert_eq!(h.run(&["row", "count", "people", "--col", "0", "--eq", "alice"]), EX_ENGINE);
        assert!(h.err().contains("not indexed"), "{}", h.err());
    }

    /// 第四条同形坑：`--col pk` 合法，`--cond-col pk` 必须报错。两者差一个词，而后者
    /// 在编码层里与「不带条件」是同一个数（`ROW_PK == COND_NONE == u32::MAX`），递进引擎
    /// 会绕过条件闸门 —— 一句本该 CAS 的覆盖写会静默真的写下去，一句本该 CAS 的删除会
    /// 静默删掉一个不相干的行。这里同时钉住「报错」与「一个字节没动」，只钉前者
    /// 不够：退回到「递 COND_NONE」也能拿到用法错以外的任意形状。
    #[test]
    fn a_pk_cond_col_is_rejected_not_silently_unconditional() {
        let mut h = H::open();
        h.seed();
        // 对照组：不带条件的写法确实会写（否则下面的「没动」是假的）。
        assert_eq!(h.run(&["row", "put", "people", "alice", "alice", "77"]), EX_OK);
        h.drain();
        for argv in [
            // `pk` 与 `-1` 在 `parse_col_spec` 里是同一档。
            vec!["row", "put", "people", "alice", "alice", "99", "--cond-col", "pk", "--cond-val", "alice"],
            vec!["row", "put", "people", "alice", "alice", "99", "--cond-col", "-1", "--cond-val", "alice"],
            // 删除也一样：这一句如果递出去，alice 会被真的删掉。
            vec!["row", "del", "people", "alice", "--cond-col", "pk", "--cond-val", "alice"],
            // 批里的子 op 走同一个 `read_cond`，不能绕过这道闸门。
            vec!["batch", "row put people alice alice 99 --cond-col pk --cond-val alice"],
        ] {
            assert_eq!(h.run(&argv), EX_USAGE, "{argv:?} -> {}", h.err());
            let (_, e) = h.drain();
            assert!(e.contains("cannot be pk"), "{argv:?} -> {e}");
        }
        // 一个字节都没动：那一句 77 还在，且 alice 还活着。
        let o = h.ok(&["row", "get", "people", "alice"]);
        assert!(o.contains("77"), "{o}");
        // 列序号与列名依旧可用 —— 这一条拒的是 `pk`，不是整个条件位。
        assert_eq!(
            h.run(&["row", "put", "people", "alice", "alice", "99", "--cond-col", "1", "--cond-val", "30"]),
            EX_ENGINE
        );
        assert!(h.err().contains("EAGAIN"), "{}", h.err());
    }

    /// 默认表只吃掉「真的是表名」的那个参数（`split_table` 由引擎裁定）。
    #[test]
    fn default_table_absorbs_only_a_real_table_name() {
        let mut h = H::open();
        h.seed();
        // 省略表名：pos[0] 是键。
        assert_eq!(h.at("people", &["row", "get", "alice"]), EX_OK);
        let (o, _) = h.drain();
        assert!(o.contains("alice"), "{o}");
        assert_eq!(h.at("people", &["row", "put", "dave", "dave", "50"]), EX_OK);
        assert_eq!(h.at("people", &["row", "del", "dave"]), EX_OK);
        h.drain();
        // 显式给表名也照样对 —— 省略只是多一条路，不是换一条路。
        assert_eq!(h.at("people", &["row", "get", "people", "alice"]), EX_OK);
        h.drain();
        // 一个与表名同形的键：省略写法会把它当表名，这时必须停在用法错上，
        // 不能拿表名当键去查（那会回一个假的「没有这一行」）。
        assert_eq!(h.run(&["table", "create", "alice", "x"]), EX_OK);
        h.drain();
        assert_eq!(h.at("people", &["row", "get", "alice"]), EX_USAGE);
        // 写全就对了，这也是 HELP 里那句「键与表名撞形时写全」的依据。
        assert_eq!(h.at("people", &["row", "get", "people", "alice"]), EX_OK);
        h.drain();
        // 破坏性的 DDL 一律不吃默认表：省略名字必须是用法错。
        for argv in [vec!["table", "clear"], vec!["table", "drop"], vec!["table", "rename", "alice"]] {
            assert_eq!(h.at("people", &argv), EX_USAGE, "{argv:?}");
            h.drain();
        }
        // 默认表指向不存在的表时，`use` 那一步就该拒绝，而不是留个悬空的默认值。
        assert_eq!(h.run(&["use", "nosuchtable"]), EX_ENGINE);
        h.drain();
        assert_eq!(h.run(&["use", "people"]), EX_OK);
    }

    /// `compile`（REPL 与 batch 共用的那层）必须先把旗标分掉，再拿位置参数当表名。
    /// 早先它直接取 `rest[1]`，于是 `row list --col x` 把 `--col` 当表名，回的是 ENOENT。
    #[test]
    fn compile_never_takes_a_flag_for_a_table_name() {
        let mut h = H::open();
        h.seed();
        let compile_of = |h: &mut H, line: &str| -> (i32, Option<Op>) {
            let toks = tokenize(line).expect("tokenize");
            let mut ctx = Ctx {
                s: &mut h.s,
                out: &mut h.out,
                err: &mut h.err,
                json: false,
                seq_check: false,
                table: None,
            };
            match compile(&mut ctx, &toks) {
                Ok(op) => (EX_OK, op),
                Err(rc) => (rc, None),
            }
        };
        // 没默认表又不带名字：用法错（2），不是「--col 当成表名」后的 ENOENT（1）。
        let (rc, _) = compile_of(&mut h, "row list --col age");
        assert_eq!(rc, EX_USAGE);
        let (_, e) = h.drain();
        assert!(e.contains("missing table name"), "{e}");
        // 有了默认表，同一句编出来的 op 指的是 people，列是 age=1。
        let toks = tokenize("row list --col age").unwrap();
        let op = {
            let mut ctx = Ctx {
                s: &mut h.s,
                out: &mut h.out,
                err: &mut h.err,
                json: false,
                seq_check: false,
                table: Some(b"people".to_vec()),
            };
            compile(&mut ctx, &toks).expect("compile must succeed with a default table")
        };
        match op {
            Some(Op::RowList { table, column, limit, .. }) => {
                assert_eq!(table, b"people".to_vec());
                assert_eq!(column, 1);
                assert_eq!(limit, 0);
            }
            other => panic!("expected RowList, got {other:?}"),
        }
        // 同一条规矩用在 row range / count / del-range 上。
        for line in ["row range --col age --lo a --hi z", "row count --col age", "row del-range --lo a"] {
            let toks = tokenize(line).unwrap();
            let mut ctx = Ctx {
                s: &mut h.s,
                out: &mut h.out,
                err: &mut h.err,
                json: false,
                seq_check: false,
                table: Some(b"people".to_vec()),
            };
            let op = compile(&mut ctx, &toks)
                .unwrap_or_else(|rc| panic!("{line}: compile 回了 {rc}"));
            let t = match op {
                Some(Op::RowRange { table, .. }) => table,
                Some(Op::RowCount { table, .. }) => table,
                Some(Op::RowRangeDelete { table, .. }) => table,
                other => panic!("{line}: {other:?}"),
            };
            assert_eq!(t, b"people".to_vec(), "{line}");
        }
        h.drain();
    }

    /// 同值消除必须说出来：新旧行逐字节相同时引擎零日志、`seq` 不前进，不提示的话人
    /// 会以为「写成功了但计数器坏了」。
    #[test]
    fn same_value_write_says_it_produced_no_log() {
        let mut h = H::open();
        h.seed();
        h.seq_check = true;
        // 一模一样地重写一遍。
        assert_eq!(h.run(&["row", "put", "people", "alice", "alice", "30"]), EX_OK);
        let (_, e) = h.drain();
        assert!(e.contains("未产生日志") && e.contains("seq 未前进"), "{e}");
        // 真的改了就闭嘴。
        assert_eq!(h.run(&["row", "put", "people", "alice", "alice", "31"]), EX_OK);
        let (_, e) = h.drain();
        assert!(!e.contains("未产生日志"), "{e}");
        // 删不存在的行也是幂等空操作。
        assert_eq!(h.run(&["row", "del", "people", "nosuchkey"]), EX_OK);
        let (_, e) = h.drain();
        assert!(e.contains("未产生日志"), "{e}");
        // CAS 落空的 EAGAIN 不该被说成同值消除（它压根没写）。
        assert_eq!(
            h.run(&["row", "put", "people", "bob", "bob", "9", "--cond-col", "age", "--cond-val", "1"]),
            EX_ENGINE
        );
        let (_, e) = h.drain();
        assert!(e.contains("EAGAIN") && !e.contains("未产生日志"), "{e}");
        // 清空本来空的表：`Count(0)` 自己就能说明问题，不需要两次额外 Stats。
        assert_eq!(h.run(&["table", "create", "empty", "a"]), EX_OK);
        assert_eq!(h.run(&["table", "clear", "empty"]), EX_OK);
        let (_, e) = h.drain();
        assert!(e.contains("本来就是空表"), "{e}");
    }

    /// 批：一条 WAL 记录、失败整批不落，以及两条用量上的拒绝。
    #[test]
    fn batch_is_atomic_and_bounded() {
        let mut h = H::open();
        h.seed();
        assert_eq!(h.run(&["batch", "row put people dave dave 60", "row put people erin erin 70"]), EX_OK);
        let (o, _) = h.drain();
        assert!(o.contains("atomically"), "{o}");
        assert_eq!(h.run(&["row", "count", "people"]), EX_OK);
        assert!(h.out().starts_with("5 rows"), "{}", h.out());
        // 第二条撞 EEXIST → 整批不落，第一条也不许留下来。
        h.drain();
        assert_eq!(
            h.run(&["batch", "row put people frank frank 80", "row put people alice alice 30 --if-absent"]),
            EX_ENGINE
        );
        let (_, e) = h.drain();
        assert!(e.contains("batch op #1"), "{e}");
        assert_eq!(h.code(&Op::RowGet { table: b"people".to_vec(), key: b"frank".to_vec() }), Reply::Row(None));
        // 嵌套与超限都是用法错，不发 op。
        assert_eq!(h.run(&["batch", "batch row put people x x 1"]), EX_USAGE);
        h.drain();
        let mut big: Vec<String> = vec!["batch".to_string()];
        for i in 0..(MAX_BATCH_OPS + 1) {
            big.push(format!("row del people nosuch{i}"));
        }
        let big: Vec<&str> = big.iter().map(|s| s.as_str()).collect();
        assert_eq!(h.run(&big), EX_USAGE);
        assert!(h.err().contains("MAX_BATCH_OPS"), "{}", h.err());
        // 空批：引擎回 EINVAL，不替它改成 CLI 自己的说法。
        h.drain();
        assert_eq!(h.run(&["batch"]), EX_USAGE);
    }

    /// 块形状的切句。这两条纯粹函数不需要引擎，值得单列：`batch { a ; b }` 被切错的
    /// 代价是「b 真的执行掉了」，而那不是报错而是静默的数据变更。
    #[test]
    fn a_brace_block_is_not_split_on_semicolons() {
        // 整行就是一条批。
        assert_eq!(
            split_statements("batch { row put t k v ; row del t k2 }"),
            vec!["batch { row put t k v ; row del t k2 }"]
        );
        // 前面还有别的语句时也一样 —— 判据看的是当前这一段，写 `trim_end` 在这里就漏了。
        assert_eq!(split_statements("use t; batch { row put k1 v1 ; row del k0 }").len(), 2);
        assert_eq!(split_statements("stats; batch { row put t k v }; ping").len(), 3);
        // 不闭合的块：切句层把它当一句带块的语句（后面的 `;` 不拆），报错到 shell 那一层做。
        assert_eq!(split_statements("batch { row put t k v ; row del t k2").len(), 1);
        // 值里的 `{`、`}`、`;` 都不是块边界。
        assert_eq!(split_statements("row put t k {x").len(), 1);
        assert_eq!(split_statements(r#"row put t k "a;b""#).len(), 1);
        // 块内容按原文切开，引号还在（值层之后再各自剥一次）。
        assert_eq!(
            batch_block("batch { row put k1 \"a b\" ; row del k2 }"),
            Some(vec!["row put k1 \"a b\"".to_string(), "row del k2".to_string()])
        );
        assert_eq!(batch_block("batch row put k1 v1"), None);
        assert_eq!(batch_block("table create t a"), None);
    }

    /// `--json` 是那条无损出口：值不被截断、errno 是字段、stdout 上只有数据。
    #[test]
    fn json_output_is_machine_readable_and_keeps_stdout_clean() {
        let mut h = H::open();
        h.seed();
        assert_eq!(h.cmd(true, None, &["row", "get", "people", "alice"]), EX_OK);
        let (o, e) = h.drain();
        assert!(o.starts_with("{\"ok\":true") && o.ends_with("}\n"), "{o}");
        assert!(o.contains("\"utf8\":\"alice\""), "{o}");
        // 提示不进 stdout，否则 `--json | jq` 直接被一句中文打断。
        assert!(!e.contains("rows"), "{e}");
        // 不存在的行：退出码 1，`found:false` 给出答案。
        assert_eq!(h.cmd(true, None, &["row", "get", "people", "ghost"]), EX_ENGINE);
        let (o, _) = h.drain();
        assert!(o.contains("\"found\":false") && o.contains("\"ok\":true"), "{o}");
        // 二进制键值在 JSON 里必须完整，不被 120 字节的截断规则吃掉。
        let bin = vec![0u8, 0x01, 0xff, b'a'];
        assert!(matches!(
            h.code(&Op::RowPut {
                table: b"people".to_vec(),
                key: bin.clone(),
                cols: vec![bin.clone(), b"4000".to_vec()],
                flags: 0,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            }),
            Reply::Row(Some(_))
        ));
        h.drain();
        assert_eq!(h.cmd(true, None, &["row", "get", "people", "hex:0001ff61"]), EX_OK);
        let (o, _) = h.drain();
        assert!(o.contains("\"hex\":\"0001ff61\""), "{o}");
        assert!(o.contains("4000"), "{o}");
    }

    /// 值的前缀写法在子命令这一层真的生效，包括那个「空串是合法列值」的坑。
    #[test]
    fn value_syntax_reaches_the_engine_intact() {
        let mut h = H::open();
        h.seed();
        assert_eq!(h.run(&["row", "put", "people", "hex:00ff", "h:41424c", "1"]), EX_OK);
        let r = h.code(&Op::RowGet { table: b"people".to_vec(), key: vec![0x00, 0xff] });
        match r {
            Reply::Row(Some(row)) => assert_eq!(row.cols[0], b"ABL".to_vec()),
            other => panic!("{other:?}"),
        }
        // 转义：`\n` 是一个换行字节，不是两个字符。
        assert_eq!(h.run(&["row", "put", "people", "esc", "a\\nb", "2"]), EX_OK);
        let r = h.code(&Op::RowGet { table: b"people".to_vec(), key: b"esc".to_vec() });
        match r {
            Reply::Row(Some(row)) => assert_eq!(row.cols[0], b"a\nb".to_vec()),
            other => panic!("{other:?}"),
        }
        h.drain();
        // 空串列值：读回来必须是 `""` 而不是「没有值」。
        assert_eq!(h.run(&["row", "put", "people", "emp", "", "3"]), EX_OK);
        let (o, _) = { h.run(&["row", "get", "people", "emp"]); h.drain() };
        assert!(o.contains("emp"), "{o}");
        // 坏写法在发出任何 op 之前就被拦下（退出码 2，不是引擎的 1）。
        assert_eq!(h.run(&["row", "put", "people", "bad", "hex:zz", "4"]), EX_USAGE);
        assert_eq!(h.run(&["row", "range", "people", "--lo", "hex:qq"]), EX_USAGE);
        h.drain();
    }

    /// HELP 里写着 `checkpoint` / `shutdown`，那它们就不能是「unknown command」。
    /// 这条测的是文档与实现的一条边：HELP 承诺过的每一行都得有对应的分派。
    #[test]
    fn control_commands_answer_ok() {
        let mut h = H::open();
        let o = h.ok(&["checkpoint"]);
        assert!(o.contains("checkpoint: ok"), "{o}");
        // 这里不走 `ok()`：`ok()` 会把两块缓冲都清空，而这一条要判的正是 stderr 上那句。
        let rc = h.run(&["shutdown"]);
        assert_eq!(rc, EX_OK);
        assert!(h.out().contains("shutdown: ok"), "{}", h.out());
        // 离线会话停不出什么来，这句必须说：不然人会以为服务端退了。
        assert!(h.err().contains("没有服务进程可停"), "{}", h.err());
        h.drain();
        assert_eq!(h.run(&["checkpoint", "now"]), EX_USAGE);
        assert_eq!(h.run(&["shutdown", "force"]), EX_USAGE);
    }

    /// `--` 之后的东西一个都不解释成旗标：值里带 `--` 的键必须还能写进去。
    #[test]
    fn dash_dash_ends_flag_parsing() {
        let mut h = H::open();
        h.seed();
        assert_eq!(h.run(&["row", "put", "people", "--", "--if-absent", "x", "1"]), EX_OK);
        let r = h.code(&Op::RowGet { table: b"people".to_vec(), key: b"--if-absent".to_vec() });
        match r {
            Reply::Row(Some(row)) => assert_eq!(row.cols[1], b"1".to_vec()),
            other => panic!("expected the row whose key is `--if-absent`, got {other:?}"),
        }
        h.drain();
        assert_eq!(h.run(&["row", "get", "people", "--", "--if-absent"]), EX_OK);
    }
}
