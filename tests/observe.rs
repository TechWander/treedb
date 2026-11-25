//! 接入面的两个运维旋钮：`--max-conn`（连接上限）与
//! `--stats --format prometheus`（机器可读指标）。
//!
//! 为什么这两件事写在同一个文件里：它们回答的是同一个问题 —— **一个进程在被
//! 用坏的时候，能不能被提前发现**。前者是「别让一条不守规矩的客户端把 fd 吃光」，
//! 后者是「让看板看见水位、lag 与 fsync 成本」。两个都是运维面，都不该碰协议：
//! `PROTOCOL_VERSION` 一个字节没动，`Op` 表一条没加。
//!
//! 四条判据：
//!
//! 1. **上限真的生效**，且在册连接不受牵连（拒绝是逐条而不是整个 listener）；
//! 2. **腾出槽位后能重新接入** —— 上限不是「连满一次就永久拒绝」；
//! 3. **拒绝日志限速**（第 1 条打、之后每 1000 条才打）：连接风暴下每个一行
//!    会把盘写满；
//! 4. **指标与线上状态逐项相等**，且抓取失败时输出的是 `treedb_up 0` 而不是
//!    什么都不打；
//! 5. **停服会被确认**（`--workers > 1` 也要）：客户端拿不到 ack 时无法区分
//!    「另一个人的守护脚本顺手停了它」与「它自己跑了」。
//!
//! 跑法：`cargo test --release --test observe`

#[path = "common/mod.rs"]
mod common;

use common::{free_addr, wait_until, Server};
use std::time::{Duration, Instant};
use treedb::engine::core::EngineStats;
use treedb::engine::model::{Op, Reply};
use treedb::net::client::Client;

fn server_bin() -> &'static str {
    env!("CARGO_BIN_EXE_treedb-server")
}

/// 起真二进制并分开收 stdout / stderr：指标是 stdout，诊断是 stderr，
/// 混在一起就没法断言「抓取失败时 stdout 上恰好只有一行 `treedb_up 0`」。
fn run(args: &[&str]) -> (i32, String, String) {
    let out = std::process::Command::new(server_bin())
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn treedb-server: {e}"));
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// 把 exposition 文本拆成 (指标名, 值)；注释行顺带校验 HELP/TYPE 成对。
fn samples(text: &str) -> Vec<(String, f64)> {
    let mut rows = Vec::new();
    let mut pending_help: Option<String> = None;
    let mut pending_type: Option<String> = None;
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("# HELP ") {
            let name = rest.split_once(' ').expect("HELP 后要有说明").0.to_string();
            assert_eq!(pending_help, None, "{name}: 上一条 HELP 还没等到 TYPE");
            pending_help = Some(name);
            continue;
        }
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            let (name, kind) = rest.split_once(' ').expect("TYPE 后要有类型");
            assert!(matches!(kind, "gauge" | "counter"), "{name}: 类型 {kind:?}");
            assert_eq!(pending_help.as_deref(), Some(name), "{name}: HELP 与 TYPE 不同名");
            pending_help = None;
            pending_type = Some(name.to_string());
            continue;
        }
        assert!(!line.starts_with('#'), "不该有别的注释：{line}");
        let (name, value) = line.split_once(' ').expect("样本行必须是 `<name> <value>`");
        assert_eq!(pending_type.as_deref(), Some(name), "{name}: 样本前没有同名 TYPE");
        pending_type = None;
        let v = value
            .parse::<f64>()
            .unwrap_or_else(|e| panic!("{name} 的值 {value:?} 不是数字：{e}"));
        rows.push((name.to_string(), v));
    }
    assert_eq!(pending_help, None, "最后一条 HELP 没有 TYPE");
    assert_eq!(pending_type, None, "最后一条 TYPE 没有样本");
    rows
}

fn value_of(rows: &[(String, f64)], name: &str) -> f64 {
    rows.iter()
        .find(|(n, _)| n == name)
        .unwrap_or_else(|| panic!("缺指标 {name}"))
        .1
}

fn stats_via_client(srv: &Server) -> EngineStats {
    let mut c = srv.client();
    match c.call(&Op::Stats) {
        Ok(Reply::Stats(s)) => *s,
        other => panic!("expected Reply::Stats, got {other:?}"),
    }
}

// ---------------------------------------------------------------- --max-conn

/// 两种 worker 形状都要走一遍：上限判定在两条完全独立的事件循环里
/// （`serve_single` 用 `conns.len()`、`multi` 用全局原子），只测一种等于
/// 另一种裸奔。
#[test]
fn max_conn_bounds_the_registry_in_both_worker_shapes() {
    for workers in [1usize, 8] {
        run_cap(workers);
    }
}

fn run_cap(workers: usize) {
    let srv = Server::start_extra("group", workers, &["--max-conn", "2"]);
    let cap = 2;

    let mut a = Client::connect(&srv.addr, 2_000).expect("connect a");
    let mut b = Client::connect(&srv.addr, 2_000).expect("connect b");
    assert!(
        matches!(a.call(&Op::Ping), Ok(Reply::Pong)),
        "workers={workers}: 上限内的第 1 条连接必须能用"
    );
    assert!(
        matches!(b.call(&Op::Ping), Ok(Reply::Pong)),
        "workers={workers}: 上限内的第 2 条连接必须能用"
    );

    // 超额的每条都要被拒：TCP 层连得上（内核接受队列），但服务端在 accept
    // 之后立刻关闭，所以第一次往返一定是错误而不是 Pong。
    for i in cap..cap + 3 {
        let mut c = Client::connect_raw(&srv.addr, 2_000)
            .unwrap_or_else(|e| panic!("workers={workers}: 第 {i} 条 TCP 都连不上：{e}"));
        assert!(
            c.call(&Op::Ping).is_err(),
            "workers={workers}: 第 {i} 条超额连接被答话了（上限没生效）"
        );
    }

    // 拒绝不得牵连在册连接：这是「逐条拒」与「关 listener」的分界。
    assert!(
        matches!(a.call(&Op::Ping), Ok(Reply::Pong)),
        "workers={workers}: 拒绝新连接时在册连接被弄坏了"
    );

    // 日志限速：4 次拒绝只该留下 #1（限速口径是「第 1 条 + 每 1000 条」）。
    wait_until("拒绝日志出现", &srv, || {
        srv.log_text().contains("refused connection #1")
    });
    assert!(
        !srv.log_text().contains("refused connection #2"),
        "workers={workers}: 拒绝日志没有限速（每条一行会在风暴下写满盘）"
    );

    // 腾一个槽位：上限是「同时在册」，不是「累计」。
    drop(b);
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut took = false;
    while Instant::now() < deadline {
        if let Ok(mut d) = Client::connect(&srv.addr, 2_000) {
            if matches!(d.call(&Op::Ping), Ok(Reply::Pong)) {
                took = true;
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        took,
        "workers={workers}: 腾出槽位后新连接仍被拒：\n{}",
        srv.log_text()
    );
    assert!(
        matches!(a.call(&Op::Ping), Ok(Reply::Pong)),
        "workers={workers}: 整轮下来第 1 条连接必须始终可用"
    );
    drop(a);
    srv.graceful_shutdown();
}

// ---------------------------------------------------------------- 指标

/// `--stats --format prometheus` 的输出必须与同一实例 `Op::Stats` 的读数逐项
/// 相等 —— 不是「大致对」，是同一个数：指标层不允许再算一遍。
#[test]
fn prometheus_exposition_matches_the_wire_stats() {
    let srv = Server::start_workers("group", 2);
    let mut c = srv.client();
    match c.call(&Op::CreateView { name: "metrics".into() }) {
        Ok(Reply::View { id, .. }) => {
            for i in 0..3 {
                match c.call(&Op::Create {
                    view: id,
                    parent: 1,
                    name: format!("f{i}").into(),
                    mode: 0o644,
                    uid: 1000,
                    gid: 1000,
                }) {
                    Ok(Reply::Node(_)) => {}
                    other => panic!("create: {other:?}"),
                }
            }
        }
        other => panic!("create view: {other:?}"),
    }
    // 打一次 checkpoint：让 `snapshot_seq` / `snap_bytes` 从 0 变成真值，
    // 否则「贴错字段」与「本来就是 0」分不清。
    assert!(matches!(c.call(&Op::Checkpoint), Ok(Reply::Ok)));
    let s = stats_via_client(&srv);
    assert!(s.snap_seq > 0 && s.snap_bytes > 0, "前提不成立：{s:?}");
    assert!(s.nodes >= 4, "前提不成立：{s:?}");

    let (code, out, err) = run(&["--listen", &srv.addr, "--stats", "--format", "prometheus"]);
    assert_eq!(code, 0, "exit {code}\nstdout:\n{out}\nstderr:\n{err}");
    let rows = samples(&out);
    assert_eq!(value_of(&rows, "treedb_up"), 1.0, "抓取成功必须是 up=1");

    // 不随「又打了一次 Stats」而变化的量：必须逐字相等。
    for (name, want) in [
        ("treedb_views", s.views),
        ("treedb_nodes", s.nodes),
        ("treedb_dentries", s.dentries),
        ("treedb_locks", s.locks),
        ("treedb_refs", s.refs),
        ("treedb_tables", s.tables),
        ("treedb_table_rows", s.rows),
        ("treedb_seq", s.seq),
        ("treedb_snapshot_seq", s.snap_seq),
        ("treedb_wal_logical_bytes", s.wal_bytes),
        ("treedb_wal_disk_bytes", s.wal_disk_bytes),
        ("treedb_snapshot_bytes", s.snap_bytes),
        ("treedb_protocol_version", s.proto_version as u64),
        ("treedb_replica", 0),
    ] {
        assert_eq!(
            value_of(&rows, name) as u64,
            want,
            "{name} 与线上 Stats 不等（{s:?}）"
        );
    }
    // 会随 Stats 自身往里走的量：只要求单调不减（`--stats` 自己也是一个 op）。
    for (name, at_least) in [
        ("treedb_ops_total", s.ops_total),
        ("treedb_writes_total", s.writes_total),
        ("treedb_fsyncs_total", s.syncs_total),
        ("treedb_checkpoints_total", s.checkpoints),
    ] {
        assert!(
            value_of(&rows, name) as u64 >= at_least,
            "{name} 倒退了：{} < {at_least}",
            value_of(&rows, name)
        );
    }
    let fsync_secs = value_of(&rows, "treedb_fsync_seconds_total");
    let expect = s.sync_us_total as f64 / 1e6;
    assert!(
        (fsync_secs - expect).abs() < 1e-3 && fsync_secs >= expect - 1e-9,
        "fsync 秒数 {fsync_secs} 与 sync_us_total {expect} 对不上"
    );
    assert_eq!(err.trim(), "", "成功路径不该往 stderr 写东西：{err}");

    // 默认档（不给 --format）逐字保持原样：老脚本按 `key = value` 抓。
    let (code, out, _) = run(&["--listen", &srv.addr, "--stats"]);
    assert_eq!(code, 0);
    for line in [
        format!("views      = {}", s.views),
        format!("nodes      = {}", s.nodes),
        format!("seq        = {} (snapshot {})", s.seq, s.snap_seq),
        format!("snapshot   = {} B", s.snap_bytes),
        format!("protocol   = {}", s.proto_version),
    ] {
        assert!(out.contains(&line), "文本档缺这一行 {line:?}：\n{out}");
    }
    srv.graceful_shutdown();
}

/// 抓取失败仍然要给一个样本：面板上 `no data` 与 `down` 是两件事，
/// 而退出码是 shell 的判据（1，不是 2 —— 参数没错，是对方不在）。
#[test]
fn prometheus_scrape_says_down_when_nothing_listens() {
    let addr = free_addr();
    let (code, out, err) = run(&["--listen", &addr, "--stats", "--format", "prometheus"]);
    assert_eq!(code, 1, "连不上必须是 1：\nstdout:\n{out}\nstderr:\n{err}");
    let rows = samples(&out);
    assert_eq!(rows.len(), 1, "失败时只该有一行样本：\n{out}");
    assert_eq!(rows[0].0, "treedb_up");
    assert_eq!(rows[0].1, 0.0);
    assert!(!err.trim().is_empty(), "失败原因要留在 stderr 上");
}

/// 参数层契约：拼错的 `--format`、忘了 `--stats`、拼错的 `--max-conn`
/// 都必须是退出码 2，而不是「默默起一个服务端」或「默默当成不限」。
#[test]
fn bad_flags_exit_two_instead_of_starting_a_server() {
    let addr = free_addr();
    for args in [
        vec!["--listen", addr.as_str(), "--stats", "--format", "nope"],
        vec!["--listen", addr.as_str(), "--format", "prometheus"],
        vec!["--listen", addr.as_str(), "--max-conn", "abc"],
        vec!["--listen", addr.as_str(), "--max-conn", "-1"],
    ] {
        let (code, out, err) = run(&args);
        assert_eq!(code, 2, "{args:?} 应当是 usage error：\nstdout:\n{out}\nstderr:\n{err}");
        assert!(!out.contains("treedb_up"), "{args:?} 不该产出指标：\n{out}");
    }
}

/// `--shutdown` 必须拿到 ack，`--workers > 1` 也要。
///
/// 这条是回归：多 worker 下停位 `flag` 由提交线程**在回包之前**置位（它一看到
/// `Op::Shutdown` 就置），前端线程可能先看到停位就跳出循环，那个 ack 于是永远
/// 发不出去 —— 客户端只看到 EOF，`treedb-server --shutdown` 报
/// `shutdown: io: server closed`，而服务端其实已经乖乖停了。单线程路径没有这个
/// 窗口，所以两条形状都要验。
#[test]
fn cli_shutdown_is_acked_in_both_worker_shapes() {
    for workers in [1usize, 4] {
        let srv = Server::start_workers("group", workers);
        let (code, out, err) = run(&["--listen", &srv.addr, "--shutdown"]);
        assert_eq!(code, 0, "workers={workers} 退出码该是 0：stderr={err}");
        assert!(
            out.contains("shutdown acknowledged"),
            "workers={workers} 没拿到 ack：stdout={out:?} stderr={err:?}"
        );
        let st = srv.wait_exit();
        assert!(st.success(), "workers={workers} 确认停服后退出码 {st:?}");
    }
}
