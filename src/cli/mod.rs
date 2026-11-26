//! 面向人工操作的命令行入口：`treedb-cli`。
//!
//! 为什么要有它：引擎的 15 个通用表 op 一直只有两个出口 —— 写代码调
//! [`crate::net::Client`]，或者用 `treedb-bench` 打一个形状固定的表（表名硬编码、键由
//! nonce 生成、条件位摸不到）。于是「看一眼某张表里 `k` 这一行是什么」这种五分钟的运维
//! 动作，实际要付一次编译。DESIGN §11 把这条记成工具面缺口，这个模块就是来还的。
//!
//! 三条设计决定，都是被仓库里的既有约束逼出来的，不是偏好：
//!
//! 1. **不引依赖**。`tests/independence.rs` 门 2 把默认构建图的依赖钉在 `libc` +
//!    `thiserror`，所以参数解析、行编辑、补全、历史、JSON 输出全部自写 —— 与
//!    `sha256.rs` 那次同一个理由。
//! 2. **不新造语义**。每个子命令一对一映射到一个已有的 op，参数名沿用引擎常量
//!    （`--if-absent` = `PUT_IF_ABSENT`，`--noreplace` = `RENAME_NOREPLACE`，`pk` =
//!    `ROW_PK`）。CLI 不代替引擎判断任何一条它能判断的约束，只在引擎那句话对人类不够
//!    直白时补一句 note。
//! 3. **两条写路径都必须过 WAL**。连服务端走 `Client::call`（一个 op 一帧），离线走
//!    `Engine::submit`（同一个 translate + commit_batch）。没有任何一条捷径直接改内存
//!    —— 见 [`conn`] 模块开头那条对照。
//!
//! 输出面：人类模式是表格/对齐（值按可打印性自动在文本与 `hex:` 之间选，见
//! [`value`]），`--json` 是机器可读。数据走 stdout，解释与提示走 stderr，于是
//! `dump --out -` 那种「stdout 是字节流」的用法不会被一句 note 毁掉。

pub mod commands;
pub mod conn;
pub mod shell;
pub mod value;

use std::path::PathBuf;
use std::process::Command;

use crate::engine::core::Durability;
use crate::VERSION;

use commands::{EX_OK, EX_TRANSPORT, EX_USAGE};
use conn::{Opts, Session};

/// 顶层帮助。只留这一份全集：子命令的报错分支指回 `treedb-cli --help`，而不是各自
/// 抄一段 —— 抄出来的第二份一定会跟实现分叉（DESIGN §11 那条「散文里的数字是脆弱
/// 断言」在旗标表上同样成立）。
pub const HELP: &str = "\
treedb-cli — 通用表与引擎状态的人工操作入口（非交互子命令 + 交互式 shell）

连接（所有子命令统一；--addr 与 --dir 二选一，都不给 = 连 127.0.0.1:7777）：
  --addr HOST:PORT        连运行中的服务端，走线协议（Client::call）
  --dir PATH              独占打开数据目录，走 Engine::submit：同一个 translate、同一个
                          commit_batch、同样写 WAL，只是中间没有 socket。服务端正跑着
                          同一份目录时会被目录锁拒掉（退出码 3），不是各写各的
  --auth-key-file PATH    --addr 模式的接入密钥（§4.4）。服务端开了认证就必须给
  --timeout MS            连接/读写超时，默认 3000
  --durability NAME       --dir 模式的落盘档 always|group|never，默认 group
  --json                  机器可读输出（数据到 stdout，提示仍到 stderr）
  --no-seq-check          写入后不为「同值消除」额外发两次 Stats（省 2 个 op）

表（DDL 与只读）：
  table list [--rows]                        TableList（--rows 再对每张表发一次 RowCount）
  table desc <t>                             TableGet + 行数 + 索引标注
  table create <t> <col>[:iu] …              TableCreate    i=COL_INDEX u=COL_UNIQUE
  table add-column <t> <col>[:i]             TableAddColumn（只能追加；COL_UNIQUE 由引擎拒）
  table rename <from> <to> [--noreplace]     TableRename（默认替换，id 不变）
  table clear <t>                            TableClear（留名字与形状，只删行）
  table drop <t>                             TableDrop

行：
  row get <t> <key>                                       RowGet
  row put <t> <key> <c0> <c1>… [--if-absent|--if-exists]
          [--cond-col C --cond-val V]                     RowPut；C 是 0 基序号 / 列名（不能 pk）
  row del <t> <key> [--if-exists] [--cond-col C --cond-val V]  RowDelete
  row del-range <t> [--col C] [--lo A] [--hi B] [--max N]  RowRangeDelete → 报实际删除数
  row list <t> [--col C] [--eq V] [--after K] [--limit N]  RowList（limit 0 = 引擎默认页宽 1000）
  row range <t> [--col C] [--lo A] [--hi B]
          [--after-val V --after-key K] [--limit N]       RowRange
  row count <t> [--col C] [--eq V]                         RowCount
  row count <t> [--col C] --lo A --hi B [--max N]          区间计数走 CLI 翻 RowRange ——
          引擎没有区间计数 op，range_count 只在 RowRangeDelete 的应答里露面

原子批：
  batch \"row put t k v\" \"row del t k2\"    Batch（≤64 条，一条 WAL 记录，崩溃原子）
  batch --file F                            每行一条子 op，# 开头是注释；--file - 读 stdin
  batch { row put t k v ; row del t k2 }   块形状（交互 shell 与 `shell <语句>` 内联）：
          块里的 `;` 分句、不结束语句，所以整块必须写在一行里

可观测与运维：
  stats                                     Op::Stats（同值消除累计在 eliminated 那一行）
  checkpoint                                Op::Checkpoint
  shutdown                                  Op::Shutdown（优雅停服）
  ping                                      Op::Ping
  dump|load|pitr|migrate|backup …           原样转给那五个既有二进制，参数一个不改

交互：
  shell                                     进 REPL：连接参数只给一次，后续命令继承上下文
  use <t>                                   （REPL 内）设默认表，之后可省表名

参数与引号：
  --                        之后的 token 一个都不当旗标。键可以是任意字节串，叫
                            `--if-absent` 也合法；没有这条出路那种行就写不进去
  \"a b\" / 'a b'         引号只用来分组（值里有空格、分号、`--`），反转义统一交给
                            下面的值层 —— 两层各剥一次会把 `a\\nb` 变成真换行
  ;                         一行里分多条语句 —— 例外是 `batch { … ; … }`：块里的分号
                            属于这一条批，不结束语句（花括号只出现一边 = 用法错）

值的写法（进与出对称，见 src/cli/value.rs）：
  hex:00ff / h:00ff      十六进制
  @path                  整个文件的内容
  a\\nb、\\xNN、\\t、\\r、\\0、\\\\   文本加六个转义
  空串                   合法列值（引擎不拿空字节当「没值」）
  输出里可打印 UTF-8 直接印，否则 hex:，超 120 字节截断（要全量用 --json）

退出码：0 成功 / 1 引擎回了 errno / 2 用法错 / 3 连不上·目录被占·协议不同代 /
其余 = 被包装的子二进制自己的退出码原样传出（treedb-migrate 的 10 那一档也在其中）。
";

/// `treedb-cli` 的 main。返回退出码，由 `src/bin/cli.rs` 交给 `process::exit`。
///
/// 之所以是「返回码而不是直接 exit」：集成测试要能在同一个进程里跑完一条命令，拿到它
/// 的码与输出，不必为了验一个旗标拼错再 fork 一次。
pub fn main(argv: &[String]) -> i32 {
    match parse(argv) {
        Err(e) => {
            eprintln!("error: {e}\n\n{HELP}");
            EX_USAGE
        }
        Ok(Call::Help) => {
            println!("{HELP}");
            EX_OK
        }
        Ok(Call::Version) => {
            println!(
                "treedb-cli {} (protocol {}, snap {})",
                VERSION,
                crate::engine::PROTOCOL_VERSION,
                crate::engine::SNAP_VERSION
            );
            EX_OK
        }
        Ok(Call::Shell(opts, json, seq, rest)) => shell::drive(opts, json, seq, &rest),
        Ok(Call::Forward(which, rest)) => forward(&which, &rest),
        Ok(Call::Once(opts, json, seq, cmd)) => once(&opts, json, seq, &cmd),
    }
}

/// 解析出来的四种结局。
#[derive(Debug, PartialEq)]
enum Call {
    Help,
    Version,
    Shell(Opts, bool, bool, Vec<String>),
    Forward(String, Vec<String>),
    /// 一次性命令：全局参数 + `(命令名, 参数)`。
    Once(Opts, bool, bool, (String, Vec<String>)),
}

/// 全局参数的解析。
///
/// 两个刻意的设计：
///
/// 1. **全局旗标可以出现在命令名之前或之后**。`treedb-cli --dir D stats` 与
///    `treedb-cli stats --dir D` 都成立。让人先敲命令再想起来「哦还得说连哪」是常见
///    动作，而报一个「unknown flag --dir」只会让人怀疑自己拼错了。
/// 2. **包装子命令后面的参数一个不改**。`treedb-cli dump --dir D --out -` 里的 `--dir`
///    必须交给 `treedb-dump`，而不是被顶层当作自己的连接参数吃掉 —— 否则 HELP 里那句
///    「参数一个不改」就是假的。于是命令名一旦落到 `dump|load|pitr|migrate|backup`，
///    后面的 token 全部原样转出（包含 `-h`，那是那个二进制自己的帮助）。
fn parse(argv: &[String]) -> Result<Call, String> {
    let mut o = Opts::default();
    let mut json = false;
    let mut seq = true;
    let mut head: Option<String> = None;
    let mut args: Vec<String> = Vec::new();
    let mut verbatim = false;
    let mut i = 0;
    while i < argv.len() {
        let a = argv[i].clone();
        // `--flag=value` 先拆成 `--flag` + `value` 两步走同一段逻辑，省掉两套分支。
        let (key, inline) = match a.split_once('=') {
            Some((k, v)) if k.starts_with("--") => (k.to_string(), Some(v.to_string())),
            _ => (a.clone(), None),
        };
        if !verbatim {
            let global = matches!(
                key.as_str(),
                "--addr"
                    | "--dir"
                    | "--auth-key-file"
                    | "--timeout"
                    | "--durability"
                    | "--json"
                    | "--no-seq-check"
            );
            // 只在 `global` 分支里用：内联写法不再推指针，否则 `--dir=` 会吃掉下一个参数。
            macro_rules! take {
                () => {{
                    match &inline {
                        Some(v) => v.clone(),
                        None => {
                            i += 1;
                            argv.get(i)
                                .cloned()
                                .ok_or_else(|| format!("{key} needs a value"))?
                        }
                    }
                }};
            }
            if global {
                match key.as_str() {
                    "--addr" => o.addr = take!(),
                    "--dir" => o.dir = Some(PathBuf::from(take!())),
                    "--auth-key-file" => o.key_file = Some(PathBuf::from(take!())),
                    "--timeout" => {
                        let v = take!();
                        o.timeout_ms = v
                            .parse::<u64>()
                            .map_err(|_| format!("--timeout wants a number, got {v}"))?;
                    }
                    "--durability" => {
                        let v = take!();
                        o.durability = match v.as_str() {
                            "always" => Durability::Always,
                            "group" => Durability::Group,
                            "never" => Durability::Never,
                            other => {
                                return Err(format!("--durability wants always|group|never, got {other}"))
                            }
                        };
                    }
                    "--json" => json = true,
                    "--no-seq-check" => seq = false,
                    _ => unreachable!("global 列表与这里的分支是同一张表"),
                }
                i += 1;
                continue;
            }
            if head.is_none() {
                match key.as_str() {
                    "-h" | "--help" | "help" => return Ok(Call::Help),
                    "-V" | "--version" | "version" => return Ok(Call::Version),
                    _ => {}
                }
                if key.starts_with('-') {
                    return Err(format!("unknown flag {key}"));
                }
                verbatim = is_wrapper(&key);
                head = Some(key.clone());
                i += 1;
                continue;
            }
            // 命令之后的 `-h`：非包装命令就是「帮我看全局帮助」，包装命令在上面已经走
            // verbatim 分支了。
            if key == "-h" || key == "--help" {
                return Ok(Call::Help);
            }
        }
        args.push(a);
        i += 1;
    }
    let Some(h) = head else {
        return Ok(Call::Help);
    };
    if is_wrapper(&h) {
        return Ok(Call::Forward(h, args));
    }
    match h.as_str() {
        "shell" | "repl" => Ok(Call::Shell(o, json, seq, args)),
        _ => Ok(Call::Once(o, json, seq, (h, args))),
    }
}

/// 那五个「已有二进制，只做入口统一」的命令名。
fn is_wrapper(name: &str) -> bool {
    matches!(name, "dump" | "load" | "pitr" | "migrate" | "backup")
}

/// 一次性命令：建会话 → 交给 [`commands::run`]。
fn once(o: &Opts, json: bool, seq: bool, cmd: &(String, Vec<String>)) -> i32 {
    let mut sess = match Session::open(o) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {}", e.0);
            return EX_TRANSPORT;
        }
    };
    // 锁住两条流而不是 `stdout()` 本身：`commands` 那边的写入要的是 `&mut dyn Write`，
    // 而锁句柄活到本函数结束，正好覆盖一条命令的生命周期。
    let stdout = std::io::stdout();
    let stderr = std::io::stderr();
    let mut so = stdout.lock();
    let mut se = stderr.lock();
    let mut argv = Vec::with_capacity(cmd.1.len() + 1);
    argv.push(cmd.0.clone());
    argv.extend_from_slice(&cmd.1);
    let mut ctx = commands::Ctx {
        s: &mut sess,
        out: &mut so,
        err: &mut se,
        json,
        seq_check: seq,
        table: None,
    };
    commands::run(&mut ctx, &argv)
}

/// 把 `dump`/`load`/`pitr`/`migrate`/`backup` 原样转给同目录下的那个二进制。
///
/// 为什么是「转」而不是在自己进程里调 `engine::dump_dir`：那五个命令的参数、退出码与
/// 目录独占检查已经各自成文（DESIGN §6.9/§6.10/§6.11），在 CLI 里重写一遍就是给自己
/// 埋第二份要同步的实现。转发之后 `treedb-cli pitr` 与 `treedb-pitr` 永远同行为，而人
/// 只需要记一个二进制名。
fn forward(which: &str, args: &[String]) -> i32 {
    let bin = format!("treedb-{which}");
    let path = match sibling(&bin) {
        Some(p) => p,
        None => {
            eprintln!(
                "error: {bin} not found next to this binary.\n\
                 编一套 bins（`cargo build --release --bins`），或用 TREEDB_BIN_DIR 指到那一目录。"
            );
            return EX_TRANSPORT;
        }
    };
    let status = match Command::new(&path).args(args).status() {
        Ok(st) => st,
        Err(e) => {
            eprintln!("error: run {}: {e}", path.display());
            return EX_TRANSPORT;
        }
    };
    // 透传退出码：treedb-migrate 的 10（要 --apply）与 pitr 的 2（参数不全）都是有内容
    // 的信号，压成 1 就把信息丢了。
    match status.code() {
        Some(c) => c,
        None => {
            eprintln!("error: {} died on a signal", path.display());
            EX_TRANSPORT
        }
    }
}

/// 找同目录的兄弟二进制；`TREEDB_BIN_DIR` 是给「装到 /usr/bin 而脚本在别处」留的口子。
pub fn sibling(bin: &str) -> Option<PathBuf> {
    if let Ok(d) = std::env::var("TREEDB_BIN_DIR") {
        let p = PathBuf::from(d).join(bin);
        if p.is_file() {
            return Some(p);
        }
    }
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    let p = dir.join(bin);
    p.is_file().then_some(p)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(args: &[&str]) -> Vec<String> {
        args.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn global_flags_parse_before_and_after_the_command() {
        // 两种写法必须等价，否则人会在一行里试两遍。
        let a = parse(&v(&["--dir", "/tmp/x", "stats"])).unwrap_cmd();
        let b = parse(&v(&["stats", "--dir", "/tmp/x"])).unwrap_cmd();
        assert_eq!(a, b);
    }

    #[test]
    fn inline_equals_forms_parse() {
        match parse(&v(&["--addr=127.0.0.1:9", "ping"])).unwrap_cmd() {
            Call::Once(o, _, _, cmd) => {
                assert_eq!(o.addr, "127.0.0.1:9");
                assert_eq!(cmd.0, "ping");
            }
            other => panic!("{other:?}"),
        }
        match parse(&v(&["row", "put", "t", "k", "--if-absent"])).unwrap_cmd() {
            Call::Once(_, _, _, cmd) => {
                assert_eq!(cmd.0, "row");
                assert_eq!(cmd.1, v(&["put", "t", "k", "--if-absent"]));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn bad_usage_is_an_error_not_a_panic() {
        for bad in [
            v(&["--dir"]),
            v(&["--timeout", "x", "stats"]),
            v(&["--durability", "maybe", "stats"]),
            v(&["-Q", "stats"]),
        ] {
            assert!(parse(&bad).is_err(), "{bad:?} 必须报用法错");
        }
    }

    #[test]
    fn wrappers_and_shell_are_recognized_without_a_session() {
        // 这三个分支不该去连任何服务端：`dump` 在另一台机器上也能跑。
        assert!(matches!(parse(&v(&["dump", "--dir", "/tmp/d"])).unwrap_cmd(), Call::Forward(..)));
        assert!(matches!(parse(&v(&["shell", "--dir", "/tmp/d"])).unwrap_cmd(), Call::Shell(..)));
    }

    #[test]
    fn version_and_help_short_circuit() {
        assert!(matches!(parse(&v(&["-V"])).unwrap_cmd(), Call::Version));
        assert!(matches!(parse(&v(&[])).unwrap_cmd(), Call::Help));
        assert!(matches!(parse(&v(&["help"])).unwrap_cmd(), Call::Help));
    }

    trait Show {
        fn unwrap_cmd(self) -> Call;
    }
    impl Show for Result<Call, String> {
        fn unwrap_cmd(self) -> Call {
            match self {
                Ok(c) => c,
                Err(e) => panic!("parse error: {e}"),
            }
        }
    }
    // `Call` 的可比较性靠 `Opts: PartialEq`（测试要验证「前置与后置旗标等价」）。
}
