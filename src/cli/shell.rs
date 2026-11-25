//! 交互式 shell：一个自写的行编辑 + 历史 + 补全，加上「一条命令的上下文」。
//!
//! 为什么要它，而且为什么是**自写**：`tests/independence.rs` 门 2 把默认构建图的依赖钉
//! 在 `libc` + `thiserror`，`rustyline`/`reedline` 都进不来（要进来就得像 `sha256.rs`
//! 那次一样先改那道门再说）。而「方便易上手」这条要求的最小可用形态恰恰就是 REPL：连接
//! 参数给一次，后面十几条命令不用重拼 —— 非交互模式下每条命令都要重打 `--addr` 与表名，
//! 那正是这轮要消掉的成本。
//!
//! 四条实现取向：
//!
//! - **tty 与管道都当回事**。tty 走 raw 模式（方向键、历史、TAB 补全）；不是 tty 时退回
//!   按行读 stdin，于是 `printf '…\n' | treedb-cli shell --dir D` 是脚本模式，CI 里也测
//!   得到这个文件。两种模式跑同一套语句求值，不存在「交互能过、管道不过」。
//! - **一条失败不等于会话结束**。交互模式下语句报错后继续等下一条（一个 ENOENT 不该把
//!   人从 shell 里踢出去）；管道模式反过来 —— 第一条非零就停并把这个码交出去，因为脚本
//!   要的正是「中间错了就别再往下做」。
//! - **上下文只有两样**：连接（`connect` 可换）与默认表（`use` 可设）。刻意不做「当前
//!   视图 / 当前列 / 格式化选项」这类会话状态堆叠：状态越多，同一条命令在两次会话里的
//!   差别就越大，而排障时要的恰恰是可复现。
//! - **终端状态必须还原**。raw 模式由 [`RawMode`] 的 `Drop` 收尾，所以从任何一条路径
//!   离开（EOF、`return`、panic 之外的正常出口）都不会把人留在「敲什么都不回显」的终端
//!   里。那比任何功能缺失都糟。

use std::io::{BufRead, Read, Write};

use crate::engine::model::Op;

use super::commands::{
    self, split_statements, tokenize, EX_OK, EX_TRANSPORT, EX_USAGE,
};
use super::conn::{Opts, Session};
use super::{forward, HELP};

/// REPL 与管道脚本共用的入口。
pub fn drive(mut o: Opts, json: bool, seq_check: bool, args: &[String]) -> i32 {
    let mut i = 0;
    let mut inline: Vec<String> = Vec::new();
    while i < args.len() {
        match args[i].as_str() {
            "--script" => {}
            "-h" | "--help" => {
                println!("{HELP}");
                return EX_OK;
            }
            other if other.starts_with("--") => {
                eprintln!("error: shell: unknown flag {other}");
                return EX_USAGE;
            }
            // 位置参数 = 直接执行这几条语句，不进循环：给「在脚本里用 shell 的语句语法」
            // 留一条路，不必为了跑一句起一个交互进程。
            other => inline.push(other.to_string()),
        }
        i += 1;
    }
    let mut sess = match Session::open(&o) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {}", e.0);
            return EX_TRANSPORT;
        }
    };
    let tty = is_tty();
    // 不是 tty 就当脚本：一条错就停。显式 `--script` 在 tty 上也能进同一分支。
    let script_mode = !tty || args.iter().any(|a| a == "--script") || !inline.is_empty();
    let mut st = State {
        json,
        seq_check,
        table: None,
    };
    if !inline.is_empty() {
        let stdout = std::io::stdout();
        let stderr = std::io::stderr();
        let mut so = stdout.lock();
        let mut se = stderr.lock();
        let mut rc = EX_OK;
        for line in &inline {
            rc = exec_line(&mut sess, &mut o, &mut st, line, &mut so, &mut se);
            if rc != EX_OK {
                return rc;
            }
        }
        return rc;
    }
    let mut ed = Editor::new(tty);
    let mut last = EX_OK;
    loop {
        let prompt = prompt_of(&o, &st);
        let line = match ed.read_line(&prompt) {
            Some(l) => l,
            // Ctrl-D / EOF：干净收尾。
            None => break,
        };
        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }
        if quit_requested(&line) {
            ed.remember(&line);
            break;
        }
        ed.remember(&line);
        let stdout = std::io::stdout();
        let stderr = std::io::stderr();
        let mut so = stdout.lock();
        let mut se = stderr.lock();
        last = exec_line(&mut sess, &mut o, &mut st, &line, &mut so, &mut se);
        if script_mode && last != EX_OK {
            ed.save();
            return last;
        }
    }
    ed.save();
    last
}

/// 会话级的可变状态。见模块开头「上下文只有两样」。
pub struct State {
    pub json: bool,
    pub seq_check: bool,
    pub table: Option<Vec<u8>>,
}

fn prompt_of(o: &Opts, st: &State) -> String {
    let target = match &o.dir {
        Some(d) => format!("dir:{}", d.display()),
        None => format!("tcp:{}", o.addr),
    };
    match &st.table {
        Some(t) => format!("treedb[{target}]({})> ", String::from_utf8_lossy(t)),
        None => format!("treedb[{target}]> "),
    }
}

fn quit_requested(line: &str) -> bool {
    matches!(line.trim(), "quit" | "exit" | "\\q" | ".quit")
}

/// 一行（可能含 `;` 分隔的多条语句）求值，返回最后一条的退出码。
///
/// `quit` 之外的语义错误不会中断同一行里的后续语句 —— 交互 shell 里 `stats; row get t k`
/// 中前者失败不该让后者根本没跑，人会以为是自己第二条写错了。
fn exec_line(
    sess: &mut Session,
    o: &mut Opts,
    st: &mut State,
    line: &str,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    let mut rc = EX_OK;
    for stmt in split_statements(line) {
        let stmt = stmt.trim().to_string();
        if stmt.is_empty() || stmt.starts_with('#') {
            continue;
        }
        rc = exec_one(sess, o, st, &stmt, out, err);
    }
    rc
}

#[allow(clippy::too_many_arguments)]
fn exec_one(
    sess: &mut Session,
    o: &mut Opts,
    st: &mut State,
    line: &str,
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    let toks = match tokenize(line) {
        Ok(t) => t,
        Err(e) => {
            let _ = writeln!(err, "usage error: {e}");
            return EX_USAGE;
        }
    };
    let Some(head) = toks.first().cloned() else {
        return EX_OK;
    };
    let rest = &toks[1..];
    // 只有需要发 op 的分支才用 ctx；内置命令直接用 out/err。
    match head.as_str() {
        "help" | "\\h" => {
            let _ = writeln!(out, "{HELP}");
            EX_OK
        }
        "\\conninfo" => {
            let target = match &o.dir {
                Some(d) => format!("独占目录 {}", d.display()),
                None => format!("线协议 {}", o.addr),
            };
            let _ = writeln!(
                out,
                "via = {} ({target}); json = {}; seq_check = {}; default table = {}",
                sess.via,
                st.json,
                st.seq_check,
                st.table
                    .as_ref()
                    .map(|t| String::from_utf8_lossy(t).into_owned())
                    .unwrap_or_else(|| "—".into())
            );
            EX_OK
        }
        "connect" => connect(o, sess, rest, out, err),
        "use" => {
            let mut a = vec!["use".to_string()];
            a.extend_from_slice(rest);
            let mut ctx = ctx_of(sess, st, out, err);
            let rc = commands::run(&mut ctx, &a);
            // `use` 把新表写进了 ctx；这里取回，REPL 才有「之后可省表名」这件事。
            st.table = ctx.table.clone();
            rc
        }
        "\\dt" => command(sess, st, out, err, &["table".to_string(), "list".to_string()]),
        "\\d" => {
            let mut a = vec!["table".to_string(), "desc".to_string()];
            for r in rest {
                a.push(r.clone());
            }
            command(sess, st, out, err, &a)
        }
        "batch" => {
            // 两种入口共用 `commands::batch`：块形状 `batch { a ; b }` 在原文上解开成
            // 若干条子 op（引号只有在这时候还留着），普通形状则把 token 原样传下去。
            // 花括号只出现一边是语法错，不当「没写块」静默放过：那会让人以为提交了一
            // 批，实际只提交了块里第一句。
            let a: Vec<String> = match commands::batch_block(line) {
                Some(subs) => {
                    let mut v = vec!["batch".to_string()];
                    v.extend(subs);
                    v
                }
                None => {
                    let open = rest.first().map(|t| t == "{").unwrap_or(false);
                    let close = rest.last().map(|t| t == "}").unwrap_or(false);
                    if open != close {
                        let _ = writeln!(err, "usage error: batch 的花括号没闭合（块要写在一行里）");
                        return EX_USAGE;
                    }
                    let mut v = vec!["batch".to_string()];
                    v.extend_from_slice(rest);
                    v
                }
            };
            command(sess, st, out, err, &a)
        }
        "dump" | "load" | "pitr" | "migrate" | "backup" => forward(&head, rest),
        "stats" | "ping" | "checkpoint" | "shutdown" | "table" | "row" => {
            let mut a = vec![head.clone()];
            for r in rest {
                a.push(r.clone());
            }
            let rc = command(sess, st, out, err, &a);
            if head == "shutdown" && rc == EX_OK {
                let _ = writeln!(err, "note: 服务端已停，本会话的下一条命令会连不上");
            }
            rc
        }
        other => {
            let _ = writeln!(err, "usage error: unknown command {other}（help 看全表）");
            EX_USAGE
        }
    }
}

/// 组一个 [`commands::Ctx`] 并跑一条命令。
fn command(
    sess: &mut Session,
    st: &mut State,
    out: &mut dyn Write,
    err: &mut dyn Write,
    argv: &[String],
) -> i32 {
    let mut ctx = ctx_of(sess, st, out, err);
    commands::run(&mut ctx, argv)
}

/// 把一次会话的两条输出流绑成 [`commands::Ctx`]。
fn ctx_of<'a>(
    sess: &'a mut Session,
    st: &'a mut State,
    out: &'a mut dyn Write,
    err: &'a mut dyn Write,
) -> commands::Ctx<'a> {
    commands::Ctx {
        s: sess,
        out,
        err,
        json: st.json,
        seq_check: st.seq_check,
        table: st.table.clone(),
    }
}

/// `connect --addr A` / `connect --dir D [--auth-key-file P]`：换目标并立刻重连。
///
/// 换成功之前不动 `o`：连不上就保持原连接，否则人会站在一个「以为换了其实没换」的会话
/// 里，比报一次错糟得多。
fn connect(
    o: &mut Opts,
    sess: &mut Session,
    args: &[String],
    out: &mut dyn Write,
    err: &mut dyn Write,
) -> i32 {
    let mut next = o.clone();
    let mut i = 0;
    while i < args.len() {
        let (k, v) = match args[i].split_once('=') {
            Some((k, v)) => (k.to_string(), v.to_string()),
            None => {
                i += 1;
                let Some(v) = args.get(i) else {
                    let _ = writeln!(err, "usage error: {} needs a value", args[i - 1]);
                    return EX_USAGE;
                };
                (args[i - 1].clone(), v.clone())
            }
        };
        match k.as_str() {
            "--addr" => {
                next.addr = v;
                next.dir = None;
            }
            "--dir" => {
                next.dir = Some(std::path::PathBuf::from(v));
            }
            "--auth-key-file" => next.key_file = Some(std::path::PathBuf::from(v)),
            "--timeout" => {
                next.timeout_ms = match v.parse::<u64>() {
                    Ok(n) => n,
                    Err(_) => {
                        let _ = writeln!(err, "usage error: --timeout wants a number, got {v}");
                        return EX_USAGE;
                    }
                };
            }
            other => {
                let _ = writeln!(err, "usage error: connect: unknown flag {other}");
                return EX_USAGE;
            }
        }
        i += 1;
    }
    match Session::open(&next) {
        Ok(s) => {
            *sess = s;
            *o = next;
            let target = match &o.dir {
                Some(d) => format!("独占目录 {}", d.display()),
                None => format!("线协议 {}", o.addr),
            };
            let _ = writeln!(out, "connected: {target}");
            EX_OK
        }
        Err(e) => {
            let _ = writeln!(err, "error: connect failed, 原连接保持: {}", e.0);
            EX_TRANSPORT
        }
    }
}

// ---------------------------------------------------------------- 行编辑

/// 一个够用的行编辑器：插入/删除、光标移动、历史、TAB 补全。
pub struct Editor {
    tty: bool,
    buf: Vec<char>,
    cur: usize,
    hist: Vec<String>,
    hpos: usize,
    /// 非 tty 时持有的 stdin 行读取器。`Stdin` 只能被读一次，所以提前拿好。
    pipe: Option<std::io::BufReader<std::io::Stdin>>,
}

impl Editor {
    fn new(tty: bool) -> Editor {
        let hist = load_history();
        Editor {
            tty,
            buf: Vec::new(),
            cur: 0,
            hist,
            hpos: 0,
            pipe: if tty {
                None
            } else {
                Some(std::io::BufReader::new(std::io::stdin()))
            },
        }
    }

    /// 读一行。`None` = EOF，或用户在一空行上按了 Ctrl-D。
    fn read_line(&mut self, prompt: &str) -> Option<String> {
        if self.tty {
            self.read_line_raw(prompt)
        } else {
            let mut s = String::new();
            match self.pipe.as_mut()?.read_line(&mut s) {
                Ok(0) => None,
                Ok(_) => Some(s.trim_end_matches(['\n', '\r']).to_string()),
                Err(e) => {
                    eprintln!("error: stdin: {e}");
                    None
                }
            }
        }
    }

    fn read_line_raw(&mut self, prompt: &str) -> Option<String> {
        let mut guard = RawMode::enter();
        let mut stdin = std::io::stdin();
        loop {
            self.redraw(prompt);
            let b = read_byte(&mut stdin)?;
            match b {
                b'\n' | b'\r' => {
                    let line: String = self.buf.iter().collect();
                    self.buf.clear();
                    self.cur = 0;
                    self.hpos = self.hist.len();
                    return Some(line);
                }
                3 => {
                    // Ctrl-C：丢掉这一行，会话继续。ISIG 被 RawMode 关了，所以这个字节
                    // 真能到我们手里 —— 否则 Ctrl-C 会把整个 REPL 打死。
                    self.buf.clear();
                    self.cur = 0;
                    let _ = std::io::stderr().write_all(b"^C\n");
                }
                4 => {
                    // Ctrl-D：空行 = EOF；非空行 = 删掉光标下那个字符（与 bash 同规矩）。
                    if self.buf.is_empty() {
                        return None;
                    }
                    if self.cur < self.buf.len() {
                        self.buf.remove(self.cur);
                    }
                }
                26 => {
                    // Ctrl-Z：交回前台。挂起前先还原终端 —— 一个在 raw 模式里被 SIGSTOP
                    // 住的进程，恢复出来还是个坏终端。
                    guard.leave();
                    unsafe { libc::raise(libc::SIGTSTP) };
                    guard.reenter();
                }
                8 | 127 => {
                    if self.cur > 0 {
                        self.cur -= 1;
                        self.buf.remove(self.cur);
                    }
                }
                1 => self.cur = 0,
                5 => self.cur = self.buf.len(),
                11 => {
                    // Ctrl-K：删到行尾。
                    self.buf.truncate(self.cur);
                }
                21 => {
                    // Ctrl-U：删到光标前。
                    self.buf.drain(0..self.cur);
                    self.cur = 0;
                }
                23 => {
                    // Ctrl-W：删掉光标前那段空白加一个词。
                    while self.cur > 0 && self.buf[self.cur - 1].is_whitespace() {
                        self.buf.remove(self.cur - 1);
                        self.cur -= 1;
                    }
                    while self.cur > 0 && !self.buf[self.cur - 1].is_whitespace() {
                        self.buf.remove(self.cur - 1);
                        self.cur -= 1;
                    }
                }
                12 => {
                    let _ = std::io::stderr().write_all(b"\x1b[2J\x1b[H");
                }
                9 => self.complete(),
                27 => match read_byte(&mut stdin) {
                    Some(b'[') | Some(b'O') => match read_byte(&mut stdin) {
                        Some(b'A') => self.hist_prev(),
                        Some(b'B') => self.hist_next(),
                        Some(b'C') => self.cur = (self.cur + 1).min(self.buf.len()),
                        Some(b'D') => self.cur = self.cur.saturating_sub(1),
                        Some(b'H') => self.cur = 0,
                        Some(b'F') => self.cur = self.buf.len(),
                        Some(b'3') => {
                            // Delete：`ESC [ 3 ~`，把那个 `~` 也吃掉，否则它会被当成字符插入。
                            let mut tilde = [0u8; 1];
                            let _ = stdin.read(&mut tilde);
                            if self.cur < self.buf.len() {
                                self.buf.remove(self.cur);
                            }
                        }
                        Some(_) => {}
                        None => return None,
                    },
                    _ => {}
                },
                other => {
                    if let Some(c) = decode_char(other, &mut stdin) {
                        self.buf.insert(self.cur, c);
                        self.cur += 1;
                    }
                }
            }
        }
    }

    fn redraw(&self, prompt: &str) {
        let line: String = self.buf.iter().collect();
        let mut out = String::with_capacity(prompt.len() + line.len() + 16);
        out.push('\r');
        out.push_str("\x1b[K");
        out.push_str(prompt);
        out.push_str(&line);
        // 光标不在行尾就往左退。用相对定位而不是绝对列号：提示符长度随连接目标变，
        // 绝对列号要自己算提示符的显示宽度，那是错的温床。
        let back = line.chars().count() - self.cur;
        if back > 0 {
            out.push_str(&format!("\x1b[{back}D"));
        }
        let _ = std::io::stderr().write_all(out.as_bytes());
        let _ = std::io::stderr().flush();
    }

    fn hist_prev(&mut self) {
        if self.hist.is_empty() {
            return;
        }
        if self.hpos > 0 {
            self.hpos -= 1;
        }
        self.set_from_hist();
    }

    fn hist_next(&mut self) {
        if self.hpos < self.hist.len() {
            self.hpos += 1;
        }
        self.set_from_hist();
    }

    fn set_from_hist(&mut self) {
        // 走到「比最新一条更新」就是回到空行，与 shell 一致。
        let s = if self.hpos == self.hist.len() {
            String::new()
        } else {
            self.hist.get(self.hpos).cloned().unwrap_or_default()
        };
        self.buf = s.chars().collect();
        self.cur = self.buf.len();
    }

    /// TAB 补全。第三段之后不猜：那里该是表名，而表名只有服务端知道，`completion_set`
    /// 因此把那一档留成空表（要补表名就 `\dt` 看一眼，比在补全里塞一次往返更清楚）。
    fn complete(&mut self) {
        let line: String = self.buf.iter().collect();
        let (cands, replace_from) = completion_set(&line);
        let word = line.get(replace_from..).unwrap_or_default();
        let hits: Vec<String> = cands
            .iter()
            .filter(|c| c.starts_with(word) && *c != word)
            .cloned()
            .collect();
        match hits.len() {
            0 => {}
            1 => {
                let mut tail = hits[0][word.len()..].to_string();
                tail.push(' ');
                self.buf.extend(tail.chars());
                self.cur = self.buf.len();
            }
            _ => {
                let _ = std::io::stderr().write_all(format!("\n{}\n", hits.join("  ")).as_bytes());
            }
        }
    }

    fn remember(&mut self, line: &str) {
        if self.hist.last().map(|l| l == line).unwrap_or(false) {
            return;
        }
        self.hist.push(line.to_string());
        self.hpos = self.hist.len();
    }

    /// 尽力而为地落盘：失败不报 —— 「历史没存住」不该让一次运维动作看起来失败。
    fn save(&self) {
        let Some(p) = history_path() else { return };
        let keep = hist_keep(self.hist.len());
        let text: String = self.hist[keep..].iter().map(|l| format!("{l}\n")).collect();
        let _ = std::fs::write(&p, text);
    }
}

/// 历史文件里最多留多少条。
const HIST_MAX: usize = 500;

/// 落盘时保留的起点：只留最后 [`HIST_MAX`] 条。单独列出来是因为「上限」是这条规矩
/// 的全部内容，它得能被测到，而不是靠一句对常量的断言糊过去。
fn hist_keep(n: usize) -> usize {
    n.saturating_sub(HIST_MAX)
}

fn history_path() -> Option<std::path::PathBuf> {
    if let Ok(p) = std::env::var("TREEDB_CLI_HISTORY") {
        if p.is_empty() || p == "0" {
            return None;
        }
        return Some(std::path::PathBuf::from(p));
    }
    let home = std::env::var("HOME").ok()?;
    Some(std::path::PathBuf::from(home).join(".treedb_cli_history"))
}

fn load_history() -> Vec<String> {
    history_path()
        .and_then(|p| std::fs::read_to_string(p).ok())
        .map(|t| {
            t.lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| l.to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// 补全的候选集，以及「从第几个字节开始替换」。
///
/// 返回的下标是**字节**偏移，切的时候一律用 `str::get`/切片按字节走：按字符数当字节数
/// 用会在中文表名上切坏。
pub fn completion_set(line: &str) -> (Vec<String>, usize) {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    let start = if line.ends_with(' ') {
        line.len()
    } else {
        line.len() - tokens.last().map(|w| w.len()).unwrap_or(0)
    };
    let top: Vec<String> = [
        "table",
        "row",
        "batch",
        "stats",
        "ping",
        "checkpoint",
        "shutdown",
        "dump",
        "load",
        "pitr",
        "migrate",
        "backup",
        "connect",
        "use",
        "help",
        "quit",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();
    let table_verbs: Vec<String> = ["list", "desc", "create", "add-column", "rename", "clear", "drop"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let row_verbs: Vec<String> = ["get", "put", "del", "del-range", "list", "range", "count"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    match (tokens.first().copied(), tokens.len()) {
        (Some("table"), 1) => (table_verbs, start),
        (Some("row"), 1) => (row_verbs, start),
        (Some("table") | Some("row"), 2) if !line.ends_with(' ') => {
            if tokens[0] == "table" {
                (table_verbs, start)
            } else {
                (row_verbs, start)
            }
        }
        (None, 0) | (Some(_), 1) if line.ends_with(' ') => (top, start),
        (_, 1) => (top, start),
        _ => (Vec::new(), start),
    }
}

// ---------------------------------------------------------------- 终端模式

/// 进入/离开 raw 模式的守卫。
struct RawMode {
    original: libc::termios,
    active: bool,
}

impl RawMode {
    fn enter() -> RawMode {
        let mut t: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(0, &mut t) } != 0 {
            // 拿不到终端状态就不改它：宁可没有方向键，也不能把一个正常终端搞坏。
            return RawMode {
                original: t,
                active: false,
            };
        }
        let mut raw = t;
        // 关 ICANON 才能逐字符拿到方向键转义；关 ECHO 是因为回显由 `redraw` 自己管
        // （它要处理「在行中间插入」）；关 ISIG 是为了 Ctrl-C 能进我们的分支而不是把
        // REPL 打死 —— 代价是 Ctrl-Z 得自己实现，见按键表里那一档。
        raw.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG);
        raw.c_cc[libc::VMIN] = 1;
        raw.c_cc[libc::VTIME] = 0;
        let ok = unsafe { libc::tcsetattr(0, libc::TCSANOW, &raw) } == 0;
        RawMode {
            original: t,
            active: ok,
        }
    }

    fn leave(&mut self) {
        if self.active {
            unsafe { libc::tcsetattr(0, libc::TCSANOW, &self.original) };
            self.active = false;
        }
    }

    fn reenter(&mut self) {
        if !self.active {
            *self = RawMode::enter();
        }
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        self.leave();
    }
}

fn is_tty() -> bool {
    unsafe { libc::isatty(0) == 1 }
}

fn read_byte(stdin: &mut std::io::Stdin) -> Option<u8> {
    let mut b = [0u8; 1];
    match stdin.read(&mut b) {
        Ok(1) => Some(b[0]),
        Ok(_) => None,
        Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => read_byte(stdin),
        Err(_) => None,
    }
}

/// 按 UTF-8 前缀长度把一个字符读全。
///
/// 逐个字节当字符处理会把中文拆成乱码，而表名与列名恰恰可能是中文。
fn decode_char(first: u8, stdin: &mut std::io::Stdin) -> Option<char> {
    let need = utf8_len(first);
    if need == 0 {
        return None;
    }
    let mut buf = [first, 0, 0, 0];
    for slot in &mut buf[1..need] {
        *slot = read_byte(stdin)?;
    }
    std::str::from_utf8(&buf[..need]).ok()?.chars().next()
}

/// 首字节 → 这个字符一共几个字节；非法首字节（续字节）给 0。
fn utf8_len(b: u8) -> usize {
    match b {
        x if x < 0x80 => 1,
        x if (0xC0..0xE0).contains(&x) => 2,
        x if (0xE0..0xF0).contains(&x) => 3,
        x if (0xF0..0xF8).contains(&x) => 4,
        _ => 0,
    }
}

/// 一条语句编成一个 op（不发）。REPL 的 `batch` 分支与非交互的 `batch` 共用它。
pub fn compile_stmt(sess: &mut Session, st: &mut State, line: &str) -> Result<Option<Op>, i32> {
    let toks = tokenize(line).map_err(|_| EX_USAGE)?;
    if toks.is_empty() {
        return Ok(None);
    }
    // 两个 sink：`Ctx` 的 out/err 是两个独立的可变借用，同一个 sink 借两次过不了借用检查。
    let mut sink_out = std::io::sink();
    let mut sink_err = std::io::sink();
    let mut ctx = commands::Ctx {
        s: sess,
        out: &mut sink_out,
        err: &mut sink_err,
        json: st.json,
        seq_check: st.seq_check,
        table: st.table.clone(),
    };
    commands::compile(&mut ctx, &toks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::model::Reply;

    #[test]
    fn quits_are_recognized_without_a_server() {
        for l in ["quit", "exit", "\\q", ".quit"] {
            assert!(quit_requested(l), "{l}");
        }
        assert!(!quit_requested("stats"));
    }

    #[test]
    fn completion_follows_the_token_position() {
        let (c, at) = completion_set("ta");
        assert!(c.iter().any(|s| s == "table") && at == 0, "{c:?}");
        let (c, _) = completion_set("table");
        assert!(c.iter().any(|s| s == "create"), "{c:?}");
        let (c, _) = completion_set("row ");
        assert!(c.iter().any(|s| s == "del-range") || c.iter().any(|s| s == "row"), "{c:?}");
        let (c, _) = completion_set("table ");
        assert!(c.contains(&"create".to_string()), "{c:?}");
        // 第三个 token 之后不再猜命令名：那里该是表名，补全帮不上。
        let (c, _) = completion_set("row put ");
        assert!(c.is_empty(), "{c:?}");
    }

    #[test]
    fn prompt_shows_which_path_a_session_took() {
        let mut st = State {
            json: false,
            seq_check: true,
            table: None,
        };
        let o = Opts {
            dir: Some(std::path::PathBuf::from("/srv/db")),
            ..Default::default()
        };
        assert!(prompt_of(&o, &st).contains("dir:/srv/db"), "{}", prompt_of(&o, &st));
        st.table = Some(b"files".to_vec());
        assert!(prompt_of(&o, &st).contains("(files)"));
        let a = Opts::default();
        assert!(prompt_of(&a, &st).contains("tcp:127.0.0.1:7777"));
    }

    #[test]
    fn statements_split_around_quotes() {
        assert_eq!(split_statements("stats; ping"), vec!["stats", " ping"]);
        assert_eq!(
            split_statements(r#"row put t k "a;b""#),
            vec![r#"row put t k "a;b""#]
        );
    }

    #[test]
    fn a_brace_block_is_one_statement() {
        // HELP 承诺 `batch { … ; … }`：块里的 `;` 属于这一条批，不结束语句。两层都要钉住 ——
        // 语句切分不许在块中间断开，块形状要真的提交成**一个** Batch。
        assert_eq!(
            split_statements("batch { row put t k v ; row del t k2 }"),
            vec!["batch { row put t k v ; row del t k2 }"]
        );
        // 块外的 `;` 照旧分句；花括号不独立成词（是值的一部分）时不开块。
        assert_eq!(split_statements("stats; batch { row put t k v }; ping").len(), 3);
        assert_eq!(split_statements("row put t k {x").len(), 1);
        assert_eq!(split_statements(r#"row put t k "a;b""#).len(), 1);

        let dir = tempfile::tempdir().expect("tempdir");
        let mut o = Opts {
            dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let mut sess = Session::open(&o).expect("offline session");
        let mut st = State {
            json: false,
            seq_check: false,
            table: None,
        };
        let mut out: Vec<u8> = Vec::new();
        let mut err: Vec<u8> = Vec::new();
        let setup = "table create t a; row put t k0 v0";
        assert_eq!(
            exec_line(&mut sess, &mut o, &mut st, setup, &mut out, &mut err),
            EX_OK,
            "{}",
            String::from_utf8_lossy(&err)
        );
        out.clear();
        err.clear();
        // 建表与那一行写入各自一条日志。
        let seq = |sess: &mut Session| match sess.call(&Op::Stats) {
            Ok(Reply::Stats(st)) => st.seq,
            other => panic!("stats: {other:?}"),
        };
        let before = seq(&mut sess);
        assert_eq!(before, 2, "table create + row put = 两条日志");
        // 块 + `use` 的默认表：`row put k1 v1` 不写表名也要编得出来。
        let rc = exec_line(
            &mut sess,
            &mut o,
            &mut st,
            "use t; batch { row put k1 v1 ; row del k0 }",
            &mut out,
            &mut err,
        );
        assert_eq!(rc, EX_OK, "{}", String::from_utf8_lossy(&err));
        let o1 = String::from_utf8_lossy(&out).into_owned();
        assert!(o1.contains("batch of 2 ops"), "{o1}");
        // 一条 Batch = 一条日志：两条子 op 只让 seq 前进 1。
        assert_eq!(seq(&mut sess), before + 1, "{o1}");

        // 花括号只出现一边 = 语法错，不能静默只提交块里第一句。
        out.clear();
        err.clear();
        let rc = exec_line(
            &mut sess,
            &mut o,
            &mut st,
            "batch { row put k9 v9",
            &mut out,
            &mut err,
        );
        assert_eq!(rc, EX_USAGE, "{}", String::from_utf8_lossy(&out));
        assert!(
            String::from_utf8_lossy(&err).contains("没闭合"),
            "{}",
            String::from_utf8_lossy(&err)
        );
        assert_eq!(seq(&mut sess), before + 1, "半截块不许写进去");
    }

    #[test]
    fn utf8_first_byte_decides_the_char_length() {
        assert_eq!(utf8_len(b'a'), 1);
        assert_eq!(utf8_len("队".as_bytes()[0]), 3);
        assert_eq!(utf8_len("🙂".as_bytes()[0]), 4);
        // 续字节不是合法首字节：宁可丢弃这个字符，也不把它当成一个独立字符插进去。
        assert_eq!(utf8_len(0x80), 0);
    }

    #[test]
    fn history_is_opt_out_and_bounded() {
        // `TREEDB_CLI_HISTORY=0` 必须彻底不落盘：CI 里跑一轮 REPL 不该在 HOME 下留文件。
        std::env::set_var("TREEDB_CLI_HISTORY", "0");
        assert!(history_path().is_none());
        std::env::set_var("TREEDB_CLI_HISTORY", "/tmp/treedb-cli-test-history");
        assert_eq!(
            history_path(),
            Some(std::path::PathBuf::from("/tmp/treedb-cli-test-history"))
        );
        std::env::remove_var("TREEDB_CLI_HISTORY");
        // 「有上限」这件事住在 save 的切片起点里，所以测的是公式而不是常量本身 ——
        // 对常量做断言永远是真，等于没测。
        assert_eq!(hist_keep(0), 0);
        assert_eq!(hist_keep(HIST_MAX), 0);
        assert_eq!(hist_keep(HIST_MAX + 7), 7);
    }

    #[test]
    fn compile_stmt_goes_through_the_same_parser_as_batch() {
        // REPL 与 `batch` 共用 `commands::compile`：这条测的是「同一行两种入口编出同一个 op」。
        let dir = tempfile::tempdir().expect("tempdir");
        let o = Opts {
            dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let mut sess = Session::open(&o).expect("offline session");
        let mut st = State {
            json: false,
            seq_check: false,
            table: None,
        };
        let mut out: Vec<u8> = Vec::new();
        let mut err: Vec<u8> = Vec::new();
        let rc = command(
            &mut sess,
            &mut st,
            &mut out,
            &mut err,
            &["table", "create", "t", "a:i"]
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
        );
        assert_eq!(rc, EX_OK, "{}", String::from_utf8_lossy(&err));
        let op = compile_stmt(&mut sess, &mut st, "row put t k v").expect("compiles");
        assert!(
            matches!(op, Some(Op::RowPut { .. })),
            "{op:?}"
        );
        // 内置命令不是 op：交回 None，由调用方处置。
        assert!(compile_stmt(&mut sess, &mut st, "help")
            .expect("none")
            .is_none());
    }
}
