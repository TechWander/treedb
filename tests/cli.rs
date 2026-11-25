//! `treedb-cli` 的端到端用例：真二进制、真 argv、真退出码。
//!
//! 单元侧（`src/cli/commands.rs`）已经把每条子命令的语义逐条判过，这里判的是**打成
//! 二进制之后**那几件事，它们没法在进程内验：
//!
//! 1. 参数解析与退出码（0/1/2/3 + 被包装二进制的透传）；
//! 2. stdout 与 stderr 的分流 —— 一旦有人把提示打到 stdout，`--json | jq` 与
//!    `dump --out -` 就都坏了；
//! 3. `--addr`（线协议）与 `--dir`（离线独占）两条路径给出**同样的答案**。这一条是
//!    整个 CLI 的立身之本：两种模式必须只是「谁持有那个目录」的差别，不能是两套语义。
//!
//! 每个用例都自己起服务端或自己挑一份临时目录，互不共享状态。

mod common;

use common::Server;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

/// 跑一次 `treedb-cli`。`conn` 是连接参数（`--addr`/`--dir` 那一组），`args` 是命令本身。
fn cli(conn: &[&str], args: &[&str]) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_treedb-cli"));
    cmd.args(conn).args(args);
    cmd.stdin(Stdio::null())
        .output()
        .expect("run treedb-cli")
}

/// 连一个在跑的服务端。
fn at(addr: &str, args: &[&str]) -> Output {
    cli(&["--addr", addr], args)
}

/// 独占一份数据目录。
fn on(dir: &Path, args: &[&str]) -> Output {
    cli(
        &["--dir", dir.to_str().expect("utf-8 tempdir"), "--durability", "never"],
        args,
    )
}

fn code(o: &Output) -> i32 {
    o.status.code().expect("no signal")
}

fn so(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

fn se(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

/// 断言成功并返回 stdout。失败时把两条流一起带进 panic 信息 —— 排错时最想要的就是
/// 「它到底说了什么」，让人再去手跑一遍是浪费。
fn ok(o: &Output, what: &str) -> String {
    let (out, err) = (so(o), se(o));
    assert_eq!(
        code(o),
        0,
        "{what} 应当成功\nexit={}\nstdout: {out}\nstderr: {err}",
        o.status.code().unwrap_or(-1)
    );
    out
}

/// 一套固定的脚本，两种连接方式各跑一遍，返回每条命令的 stdout。
///
/// 顺序是有讲究的：每条都依赖前一条建好的状态，所以两边跑的是同一条时间线，比较结果
/// 才有意义。`stats`/`ping` 不在里面 —— 它们数的本来就是「这个进程/这个服务端看了多少」，
/// 两种模式必然不同（`conn.rs` 模块开头那条诚实的差别）。
fn walk(run: &dyn Fn(&[&str]) -> Output) -> Vec<(String, String)> {
    let script: Vec<Vec<&str>> = vec![
        vec!["table", "create", "t", "a", "b:i"],
        vec!["table", "list"],
        vec!["table", "desc", "t"],
        vec!["row", "put", "t", "k1", "v1", "7"],
        vec!["row", "put", "t", "k2", "v2", "8"],
        vec!["row", "get", "t", "k1"],
        vec!["row", "list", "t"],
        vec!["row", "list", "t", "--col", "b", "--eq", "8"],
        vec!["row", "range", "t", "--lo", "k1", "--hi", "k3"],
        vec!["row", "range", "t", "--col", "1", "--lo", "7", "--hi", "9"],
        vec!["row", "count", "t"],
        vec!["row", "count", "t", "--col", "b", "--eq", "7"],
        vec!["row", "count", "t", "--lo", "k1", "--hi", "k3"],
        vec!["batch", "row put t k3 v3 9", "row del t k1"],
        vec!["table", "add-column", "t", "c"],
        vec!["table", "rename", "t", "t2"],
        vec!["row", "del-range", "t2", "--lo", "k", "--hi", "k9", "--max", "1"],
        vec!["table", "clear", "t2"],
        vec!["table", "drop", "t2"],
        vec!["table", "list"],
    ];
    let mut got = Vec::new();
    for argv in script {
        let rc = run(&argv);
        let out = ok(&rc, &argv.join(" "));
        got.push((argv.join(" "), out));
    }
    got
}

/// 两种模式给出同样的答案 —— 这是「离线那条路没有捷径」的可执行表述。
#[test]
fn tcp_and_dir_modes_answer_identically() {
    let srv = Server::start("never");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().to_path_buf();

    let via_tcp = walk(&|argv| {
        let o = at(&srv.addr, argv);
        assert_eq!(code(&o), 0, "tcp: {}\n{}", argv.join(" "), se(&o));
        o
    });
    let via_dir = walk(&|argv| {
        let o = on(&dir, argv);
        assert_eq!(code(&o), 0, "dir: {}\n{}", argv.join(" "), se(&o));
        o
    });

    assert_eq!(via_tcp.len(), via_dir.len());
    for ((cmd, tcp), (_, dir_out)) in via_tcp.iter().zip(via_dir.iter()) {
        assert_eq!(&tcp, &dir_out, "命令 `{cmd}` 在两种模式下答案不同");
    }
}

/// 四条关键错误路径在真进程上还是那四个退出码；用法错与「没地方问引擎」分得很清。
#[test]
fn errno_and_exit_codes_survive_the_wire() {
    let srv = Server::start("never");
    let addr = srv.addr.clone();
    ok(&at(&addr, &["table", "create", "e", "a", "b:i"]), "create");
    ok(&at(&addr, &["row", "put", "e", "k", "v", "1"]), "put");

    // 1 = 引擎回了 errno，而且 errno 的 POSIX 名字在 stderr 里读得出来。
    for (argv, name) in [
        (vec!["row", "put", "e", "k", "k", "2", "--if-absent"], "EEXIST"),
        (vec!["row", "put", "e", "ghost", "g", "2", "--if-exists"], "ENOENT"),
        (
            vec!["row", "put", "e", "k", "k", "2", "--cond-col", "b", "--cond-val", "9"],
            "EAGAIN",
        ),
        (vec!["row", "put", "e", "k", "only-one-column"], "EINVAL"),
    ] {
        let o = at(&addr, &argv);
        assert_eq!(code(&o), 1, "{}", argv.join(" "));
        assert!(se(&o).contains(name), "{} -> {}", argv.join(" "), se(&o));
    }

    // 2 = 用法错，一个 op 都不该发出去。
    assert_eq!(code(&at(&addr, &["table", "nosuchverb"])), 2);
    assert_eq!(code(&at(&addr, &["row", "put", "e", "k", "hex:zz"])), 2);
    assert_eq!(code(&at(&addr, &["--nonsense"])), 2);
    // 3 = 没地方去问引擎。端口 1 上没有服务端。
    let o = cli(&["--addr", "127.0.0.1:1", "--timeout", "200"], &["ping"]);
    assert_eq!(code(&o), 3, "{}", se(&o));
    // 目录被在跑的服务端占着 → 也是 3，而不是「各写各的」。
    let o = on(srv.dir(), &["table", "list"]);
    assert_eq!(code(&o), 3, "{}", se(&o));
    let err = se(&o).to_lowercase();
    assert!(err.contains("lock") || err.contains("bus"), "{err}");
}

/// 同值消除的提示在两条路径上都成立 —— 它靠的是写前后两次 `Stats`，而 `Stats` 在
/// 线协议上是真 op，不是本地推断。
#[test]
fn same_value_write_is_reported_over_tcp() {
    let srv = Server::start("never");
    let addr = srv.addr.clone();
    ok(&at(&addr, &["table", "create", "s", "a"]), "create");
    ok(&at(&addr, &["row", "put", "s", "k", "v"]), "first put");
    let o = at(&addr, &["row", "put", "s", "k", "v"]);
    ok(&o, "same-value put");
    assert!(se(&o).contains("未产生日志"), "{}", se(&o));
    assert!(se(&o).contains("seq 未前进"), "{}", se(&o));
    // 真改了就闭嘴，否则这句提示会沦为噪音。
    let o = at(&addr, &["row", "put", "s", "k", "w"]);
    ok(&o, "real put");
    assert!(!se(&o).contains("未产生日志"), "{}", se(&o));
    // `--no-seq-check` 省下那两次 Stats：提示随之消失，数据照写。
    let o = cli(
        &["--addr", &addr, "--no-seq-check"],
        &["row", "put", "s", "k", "w"],
    );
    ok(&o, "put without seq check");
    assert!(!se(&o).contains("未产生日志"), "{}", se(&o));
    // 累计口径在 stats 里看得见。
    let st = ok(&at(&addr, &["stats"]), "stats");
    assert!(st.contains("eliminated"), "{st}");
}

/// `--json`：stdout 一行一个对象，提示全在 stderr，值不被截断。
#[test]
fn json_mode_keeps_stdout_machine_only() {
    let srv = Server::start("never");
    let addr = srv.addr.clone();
    let conn = ["--addr", addr.as_str(), "--json"];
    let j = |args: &[&str]| cli(&conn, args);

    ok(&j(&["table", "create", "g", "a", "b:i"]), "create");
    let long = format!("x{}", "y".repeat(300));
    ok(&j(&["row", "put", "g", "k", long.as_str(), "1"]), "put long");

    let o = j(&["row", "get", "g", "k"]);
    let out = ok(&o, "json row get");
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 1, "JSON 出口必须是一行: {out}");
    assert!(lines[0].starts_with("{\"ok\":true"), "{}", lines[0]);
    // 300 字节的值在人类模式会被截断，在 JSON 里必须完整。
    assert!(lines[0].contains(&"y".repeat(300)), "长值被截断了");
    // 二进制值走 hex，不掺不可打印字节。
    ok(&j(&["row", "put", "g", "hex:00ff", "a", "2"]), "put binary");
    let out = ok(&j(&["row", "get", "g", "hex:00ff"]), "json binary get");
    assert!(out.contains("\"hex\":\"00ff\""), "{out}");
    // 错误也是可判的字段。
    let o = j(&["row", "put", "g", "k", "k", "3", "--if-absent"]);
    assert_eq!(code(&o), 1);
    assert!(so(&o).contains("\"errno_name\":\"EEXIST\""), "{}", so(&o));
    // stats 的协议版本与 `--version` 说的那个是同一个数。
    let out = ok(&j(&["stats"]), "json stats");
    assert!(out.contains("\"proto_version\":8"), "{out}");
    let v = ok(&cli(&[], &["version"]), "version");
    assert!(v.contains("protocol 8"), "{v}");
}

/// 交互/脚本模式：连接参数只给一次，`use` 之后可省表名，多条语句共享上下文。
#[test]
fn shell_mode_carries_connection_and_context() {
    let srv = Server::start("never");
    let addr = srv.addr.clone();
    let o = cli(
        &["--addr", &addr],
        &[
            "shell",
            "table create sh a b:i",
            "use sh",
            "row put k1 v1 7",
            "row get k1",
            "row count",
        ],
    );
    let out = ok(&o, "shell inline");
    assert!(out.contains("using table sh"), "{out}");
    assert!(out.contains("k1"), "{out}");
    assert!(out.starts_with("created table sh"), "{out}");

    // 管道进来的脚本同样继承连接：这是「不写代码也能连续操作」的那一档。
    let mut child = Command::new(env!("CARGO_BIN_EXE_treedb-cli"))
        .args(["--addr", &addr, "shell"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn shell");
    {
        let stdin = child.stdin.as_mut().expect("stdin");
        stdin
            .write_all(b"use sh\nrow list\nrow del k1\nrow count\n")
            .expect("write");
    }
    let out = child.wait_with_output().expect("wait");
    let body = ok(&out, "shell via pipe");
    assert!(body.contains("k1"), "{body}");
    assert!(body.contains("0 rows"), "{body}");
}

/// 包装的五个命令：参数一个不改，退出码原样传出。
#[test]
fn wrappers_pass_arguments_and_exit_codes_through() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().to_path_buf();
    let dump = tmp.path().join("out.dump");

    ok(&on(&dir, &["table", "create", "w", "a", "b:i"]), "create");
    ok(&on(&dir, &["row", "put", "w", "k", "v", "1"]), "put");

    // `treedb-cli dump …` == `treedb-dump …`：`--dir` 归那个二进制，不被顶层吃掉。
    let o = cli(
        &[],
        &[
            "dump",
            "--dir",
            dir.to_str().unwrap(),
            "--out",
            dump.to_str().unwrap(),
        ],
    );
    ok(&o, "wrapped dump");
    assert!(dump.exists(), "dump 文件没出现: {}", so(&o));
    assert!(std::fs::metadata(&dump).expect("dump").len() > 0);

    // 体检式 load（只解码不写），参数一样是原样转过去。
    let o = cli(
        &[],
        &["load", "--in", dump.to_str().unwrap()],
    );
    ok(&o, "wrapped load --in");

    // 目标目录不能已存在或被占：这时透传的是那个二进制的 1，而不是 CLI 的 1 那一档含义。
    let bad = tmp.path().join("busy");
    std::fs::create_dir(&bad).expect("mkdir");
    ok(&on(&bad, &["table", "create", "occupied", "a"]), "seed busy dir");
    let o = cli(
        &[],
        &[
            "load",
            "--in",
            dump.to_str().unwrap(),
            "--dir",
            bad.to_str().unwrap(),
        ],
    );
    assert_eq!(code(&o), 1, "{} / {}", so(&o), se(&o));

    // 参数不全 → 那个二进制的 2 传出来，别压成 1。
    let o = cli(&[], &["dump", "--out"]);
    assert_eq!(code(&o), 2, "{}", se(&o));
    // 它的 `--help` 也是它自己的帮助，不是 CLI 的。
    let o = cli(&[], &["dump", "--help"]);
    let out = ok(&o, "wrapped dump --help");
    assert!(out.contains("treedb-dump"), "{out}");
    assert!(!out.contains("treedb-cli —"), "把 CLI 自己的 HELP 印出来了");
}

/// `--` 之后一个不当旗标：键叫 `--if-absent` 的行必须还能写进去、读出来。
#[test]
fn dash_dash_reaches_the_engine() {
    let srv = Server::start("never");
    let addr = srv.addr.clone();
    ok(&at(&addr, &["table", "create", "d", "a", "b:i"]), "create");
    ok(
        &at(&addr, &["row", "put", "d", "--", "--if-absent", "x", "1"]),
        "put a key that looks like a flag",
    );
    let out = ok(&at(&addr, &["row", "get", "d", "--", "--if-absent"]), "get it back");
    assert!(out.contains("--if-absent"), "{out}");
}

/// 一份目录被 CLI 独占写完之后，服务端重启必须能读出同样的数据 —— 这条钉住
/// 「CLI 没有绕过 WAL」在磁盘上的后果：它写的就是一条普通日志记录，与走网络的那些同格式。
#[test]
fn dir_mode_writes_a_real_wal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path().join("data");
    ok(&on(&dir, &["table", "create", "p", "a", "b:i"]), "create");
    ok(&on(&dir, &["row", "put", "p", "k1", "v1", "5"]), "put");
    let before = ok(&on(&dir, &["row", "get", "p", "k1"]), "read back offline");
    let after_put = ok(&on(&dir, &["row", "put", "p", "k2", "v2", "6"]), "second put");
    assert!(after_put.contains("wrote p"), "{after_put}");

    // 同一份目录交给服务端打开：CLI 写的那些必须原样还在。
    let srv = Server::restart_at(tmp, dir.clone(), "never");
    let out = at(&srv.addr, &["row", "get", "p", "k1"]);
    let after = ok(&out, "read through the server");
    assert_eq!(before, after, "CLI 写的行在服务端眼里长得不一样");
    let out = ok(&at(&srv.addr, &["row", "count", "p"]), "count through the server");
    assert!(out.starts_with("2 rows"), "{out}");
    // 落盘的就是那两个大家熟悉的名字，没有 CLI 专用的旁路文件。
    assert!(dir.join(treedb::engine::core::WAL_FILE).exists(), "{:?}", dir);
    assert!(!dir.join("treedb-cli").exists());
}
