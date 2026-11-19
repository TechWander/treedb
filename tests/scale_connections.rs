//! 连接轴验收：一个进程能不能扛住万级同时打开的连接，每条连接花多少内存。
//!
//! 这正是换掉 PG 的原始动机：PG 一条连接 = 一个 backend 进程 = 数 MB RSS，
//! 10 000 条连接单是 backend 就是几十 GB，而且 `max_connections` 默认才 100。
//! 这里验的是本引擎的结构主张：**1 个 fd + 几百字节用户态状态 / 连接**。
//!
//! 度量口径（全部来自 `/proc`，不靠估算）：
//!
//! | 指标 | 来源 |
//! |------|------|
//! | 服务端进程 RSS 增量 | `/proc/<pid>/status` VmRSS |
//! | 服务端 fd 数 | `/proc/<pid>/fd` 条目数 |
//! | 内核 socket 内存 | `/proc/net/sockstat` TCP mem（页） |
//!
//! 跑法：
//! ```text
//! cargo test --test scale_connections                          # 1k 连接快验
//! cargo test --test scale_connections -- --ignored --nocapture  # 10k 连接全量
//! TREEDB_CONNS=50000 cargo test --test scale_connections -- --ignored --nocapture
//! ```
//!
//! 上限提醒：到**同一个** `127.0.0.1:PORT` 的客户端 socket 数受本机临时端口范围
//! 限制（`net.ipv4.ip_local_port_range`，本机 32768–60999 ≈ 28 k）。要试 50 k
//! 就得把监听地址铺到多个 127.0.0.x 上，那是端口预算问题，不是引擎容量问题。

mod common;

use common::Server;
use std::sync::Arc;
use std::time::{Duration, Instant};
use treedb::engine::model::{Op, Reply};
use treedb::net::client::Client;

fn vm_rss_kb(pid: u32) -> u64 {
    let txt = std::fs::read_to_string(format!("/proc/{pid}/status")).expect("proc status");
    for line in txt.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse()
                .expect("VmRSS parse");
        }
    }
    0
}

fn fd_count(pid: u32) -> usize {
    std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|d| d.count())
        .unwrap_or(0)
}

/// `/proc/<pid>/limits` 里的 "Max open files"（soft, hard）。
fn open_file_limits(pid: u32) -> (u64, u64) {
    let txt = std::fs::read_to_string(format!("/proc/{pid}/limits")).expect("proc limits");
    for line in txt.lines() {
        if line.starts_with("Max open files") {
            // 形如 `Max open files   65536   65536   files`：后两个数字是 soft/hard，
            // 行尾还带单位字，所以只能按「取前两个能解析成数的 token」读。
            let nums: Vec<u64> = line
                .split_whitespace()
                .filter_map(|t| t.parse().ok())
                .collect();
            if nums.len() >= 2 {
                return (nums[0], nums[1]);
            }
        }
    }
    (0, 0)
}

/// `/proc/net/sockstat` 里 TCP 行的 mem（单位：页）。
///
/// 行格式是 `TCP: inuse 25 orphan 0 tw 8 alloc 55 mem 4`——`mem` 后面是空格不是
/// 冒号，按“下一个 token”取，别拿 `split("mem:")` 去找一个不存在的分隔符。
fn tcp_sock_pages() -> u64 {
    let txt = std::fs::read_to_string("/proc/net/sockstat").expect("sockstat");
    for line in txt.lines() {
        if line.starts_with("TCP: ") {
            let toks: Vec<&str> = line.split_whitespace().collect();
            for (i, t) in toks.iter().enumerate() {
                if *t == "mem" {
                    return toks.get(i + 1).and_then(|v| v.parse().ok()).unwrap_or(0);
                }
            }
        }
    }
    0
}

fn pct(v: &[u128], q: f64) -> u128 {
    if v.is_empty() {
        return 0;
    }
    let mut s = v.to_vec();
    s.sort_unstable();
    let i = ((s.len() as f64 - 1.0) * q).round() as usize;
    s[i.min(s.len() - 1)]
}

/// 起服务端、压 `n` 条常驻连接、每条 ping 一次并记延迟，返回统计。
///
/// 连接必须**一直开着**再 ping：开着才算并发连接数，ping 完就关只测了吞吐。
struct Loaded {
    conns: usize,
    rss_base_kb: u64,
    rss_loaded_kb: u64,
    fds_base: usize,
    fds_loaded: usize,
    ping_us: Vec<u128>,
    sock_pages_base: u64,
    sock_pages_loaded: u64,
}

fn load(durability: &str, n: usize, threads: usize) -> Loaded {
    let srv = Server::start_backlog(durability, 8192);
    let addr = Arc::new(srv.addr.clone());
    let pid = srv.pid();
    let (soft, hard) = open_file_limits(pid);
    eprintln!(
        "服务端 fd 上限 soft={soft} hard={hard}（要撑 {n} 条连接，soft 必须够）"
    );
    assert!(
        soft as usize > n + 1024,
        "服务端 RLIMIT_NOFILE 只有 {soft}，压不到 {n} 条连接：这是环境限制，不是引擎限制"
    );

    let rss_base_kb = vm_rss_kb(pid);
    let fds_base = fd_count(pid);
    let sock_pages_base = tcp_sock_pages();

    let per = n / threads;
    let mut handles = Vec::new();
    for t in 0..threads {
        let a = addr.clone();
        let want = if t == threads - 1 { n - per * (threads - 1) } else { per };
        handles.push(std::thread::spawn(move || {
            let mut held: Vec<Client> = Vec::with_capacity(want);
            let mut lat: Vec<u128> = Vec::with_capacity(want);
            let mut retries = 0u32;
            while held.len() < want {
                let r = Client::connect(&a, 3000);
                match r {
                    Ok(c) => held.push(c),
                    Err(e) => {
                        // 接受队列（backlog/somaxconn）满时 connect 会失败或超时。
                        // 这里退避重试，把「参数不够」和「引擎扛不住」区分开。
                        retries += 1;
                        assert!(retries < 2000, "连接 {want} 条时反复失败: {e}");
                        std::thread::sleep(Duration::from_millis(5));
                    }
                }
            }
            assert_eq!(held.len(), want);
            for c in &mut held {
                let t0 = Instant::now();
                match c.call(&Op::Ping) {
                    Ok(Reply::Pong) => {}
                    other => panic!("万级连接下 Ping 必须还能答: {other:?}"),
                }
                lat.push(t0.elapsed().as_micros());
            }
            (held, lat, retries)
        }));
    }
    let mut held_all: Vec<Client> = Vec::with_capacity(n);
    let mut ping_us: Vec<u128> = Vec::with_capacity(n);
    let mut retried = 0u32;
    for h in handles {
        let (mut v, lat, r) = h.join().expect("connector thread panicked");
        retried += r;
        held_all.append(&mut v);
        ping_us.extend(lat);
    }
    assert_eq!(held_all.len(), n);
    assert_eq!(ping_us.len(), n);
    if retried > 0 {
        eprintln!("注意：有 {retried} 次 connect 失败后重试才成功（接受队列压力）");
    }

    let fds_loaded = fd_count(pid);
    let rss_loaded_kb = vm_rss_kb(pid);
    let sock_pages_loaded = tcp_sock_pages();
    let conns = held_all.len();
    eprintln!(
        "常驻连接 {conns} 条 | RSS {} MiB → {} MiB | fd {} → {} | TCP sockmem {} → {} 页",
        rss_base_kb / 1024,
        rss_loaded_kb / 1024,
        fds_base,
        fds_loaded,
        sock_pages_base,
        sock_pages_loaded
    );
    // 上面全部度量都是在 `held_all` 还持有连接时做的；返回时连接自然关闭，
    // 服务端随后被 `Server::drop` SIGKILL，不会留下占端口的孤儿。
    Loaded {
        conns,
        rss_base_kb,
        rss_loaded_kb,
        fds_base,
        fds_loaded,
        ping_us,
        sock_pages_base,
        sock_pages_loaded,
    }
}

fn report(l: &Loaded) -> (f64, f64) {
    let per_conn_kb = (l.rss_loaded_kb.saturating_sub(l.rss_base_kb)) as f64 / l.conns as f64;
    let per_conn_fd = (l.fds_loaded.saturating_sub(l.fds_base)) as f64 / l.conns as f64;
    eprintln!(
        "RSS/conn = {per_conn_kb:.2} KiB | fd/conn = {per_conn_fd:.4} | \
         ping p50 = {} us p99 = {} us",
        pct(&l.ping_us, 0.50),
        pct(&l.ping_us, 0.99)
    );
    (per_conn_kb, per_conn_fd)
}

/// 1 000 条连接：随 `cargo test` 默认跑的快验，防止连接相关改动把
/// 「1 fd/连接 + 常数内存」这条结构性主张悄悄改坏。
#[test]
fn thousand_connections_cost_one_fd_and_a_sliver_of_rss() {
    let l = load("group", 1000, 8);
    let (kb_per_conn, fd_per_conn) = report(&l);
    assert!(
        fd_per_conn <= 1.05,
        "每条连接应当只占 1 个 fd，实测 {fd_per_conn}"
    );
    // 用户态每连接只多一个 Conn（百来字节）+ epoll item；给 8 KiB 的宽松上限，
    // 一旦有人给连接加了缓冲/线程，这里立刻红。
    assert!(
        kb_per_conn <= 8.0,
        "1k 连接下 RSS/conn {kb_per_conn:.2} KiB 超出预期"
    );
}

/// 万级连接全量验收：单进程 10 000 条常驻连接，全部可服务。
#[test]
#[ignore = "重压用例，手动跑：cargo test --test scale_connections -- --ignored --nocapture"]
fn ten_thousand_connections_in_one_process() {
    let n: usize = std::env::var("TREEDB_CONNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let l = load("group", n, 8);
    let (kb_per_conn, fd_per_conn) = report(&l);
    assert_eq!(l.conns, n);
    assert!(
        fd_per_conn <= 1.02,
        "{n} 条连接的 fd/conn 应当 ≈1，实测 {fd_per_conn}"
    );
    assert!(
        kb_per_conn <= 64.0,
        "{n} 条连接 RSS/conn {kb_per_conn:.2} KiB，超出「几 KB/连接」的设计承诺"
    );
    // p99 在万级连接下仍要是「一次系统调用」的量级，不能因为连接数上去就排队。
    let p99 = pct(&l.ping_us, 0.99);
    assert!(
        p99 < 50_000,
        "万级连接下 ping p99 = {p99} us，明显排队了"
    );
    let l_kb = kb_per_conn * (n as f64) / 1024.0;
    eprintln!(
        "结论：单进程 {n} 条连接，总增量 RSS ≈ {l_kb:.1} MiB（约 {kb_per_conn:.2} KiB/连接）"
    );
}
