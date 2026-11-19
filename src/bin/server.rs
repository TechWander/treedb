//! `treedb-server`：起一个元数据引擎实例，或对运行中的实例做状态查询。
//!
//! ```text
//! treedb-server --dir /data/treedb --listen 127.0.0.1:7777 [--durability group]
//! treedb-server --listen 127.0.0.1:7777 --stats          # 查询并退出
//! treedb-server --listen 127.0.0.1:7777 --shutdown       # 优雅停服
//! ```
//!
//! 信号：`SIGINT`/`SIGTERM` 走优雅退出路径（补 fsync + checkpoint 后返回），
//! 不是直接 kill —— 崩溃安全性由 `kill -9` 的验收测试专门覆盖。

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use treedb::engine::{Durability, Op, Reply};
use treedb::net::client::Client;
use treedb::net::server::{serve, ServerConfig};

static STOP: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);

extern "C" fn on_signal(_sig: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

fn parse_durability(s: &str) -> Durability {
    match s {
        "always" => Durability::Always,
        "never" => Durability::Never,
        _ => Durability::Group,
    }
}

fn print_stats(addr: &str) -> Result<(), String> {
    let mut c = Client::connect(addr, 3000).map_err(|e| e.to_string())?;
    match c.call(&Op::Stats).map_err(|e| e.to_string())? {
        Reply::Stats(s) => {
            println!("views      = {}", s.views);
            println!("nodes      = {}", s.nodes);
            println!("dentries   = {}", s.dentries);
            println!("locks      = {}", s.locks);
            println!("refs       = {}", s.refs);
            println!("keys       = {}", s.keys);
            println!("audit      = {}", s.audit);
            println!("seq        = {} (snapshot {})", s.seq, s.snap_seq);
            println!("wal        = {} B", s.wal_bytes);
            println!(
                "ops        = {} (writes {}, syncs {}, avg fsync {} us)",
                s.ops_total, s.writes_total, s.syncs_total,
                if s.syncs_total > 0 { s.sync_us_total / s.syncs_total } else { 0 }
            );
            println!("checkpoints= {}", s.checkpoints);
            Ok(())
        }
        Reply::Err { code, msg } => Err(format!("server error {code}: {}", String::from_utf8_lossy(&msg))),
        other => Err(format!("unexpected reply {other:?}")),
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut dir = PathBuf::from("/var/lib/treedb");
    let mut listen = "127.0.0.1:7777".to_string();
    let mut durability = "group".to_string();
    let mut sync_ms: i32 = 20;
    let mut auto_ckpt_mb: u64 = 64;
    let mut backlog: i32 = 4096;
    let mut workers: usize = 1;
    let mut stats_only = false;
    let mut shutdown_only = false;
    let mut pid_file: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        let next = |i: &mut usize| -> String {
            *i += 1;
            args.get(*i).cloned().unwrap_or_default()
        };
        match args[i].as_str() {
            "--dir" => dir = PathBuf::from(next(&mut i)),
            "--listen" => listen = next(&mut i),
            "--durability" => durability = next(&mut i),
            "--sync-ms" => sync_ms = next(&mut i).parse().unwrap_or(20),
            "--auto-checkpoint-mb" => auto_ckpt_mb = next(&mut i).parse().unwrap_or(64),
            "--backlog" => backlog = next(&mut i).parse().unwrap_or(4096),
            "--workers" => workers = next(&mut i).parse().unwrap_or(1).max(1),
            "--pid-file" => pid_file = Some(PathBuf::from(next(&mut i))),
            "--stats" => stats_only = true,
            "--shutdown" => shutdown_only = true,
            "-h" | "--help" => {
                println!("{}", HELP);
                return;
            }
            other => {
                eprintln!("unknown argument: {other}\n\n{HELP}");
                std::process::exit(2);
            }
        }
        i += 1;
    }

    if stats_only {
        if let Err(e) = print_stats(&listen) {
            eprintln!("stats: {e}");
            std::process::exit(1);
        }
        return;
    }
    if shutdown_only {
        match Client::connect(&listen, 3000).and_then(|mut c| c.call(&Op::Shutdown)) {
            Ok(Reply::Ok) => println!("shutdown acknowledged"),
            Ok(other) => eprintln!("unexpected reply: {other:?}"),
            Err(e) => eprintln!("shutdown: {e}"),
        }
        return;
    }

    let addr: SocketAddr = match listen.parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("bad --listen {listen}: {e}");
            std::process::exit(2);
        }
    };
    unsafe {
        libc::signal(libc::SIGINT, on_signal as *const () as libc::size_t);
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::size_t);
        // 半写连接不要拿 SIGPIPE 打死服务。
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
    if let Some(p) = &pid_file {
        let _ = std::fs::write(p, std::process::id().to_string());
    }

    let cfg = ServerConfig {
        listen: addr,
        dir: dir.clone(),
        durability: parse_durability(&durability),
        sync_ms,
        auto_checkpoint_bytes: auto_ckpt_mb * 1024 * 1024,
        backlog,
        workers,
    };
    eprintln!(
        "treedb-server pid={} listen={} dir={} durability={} backlog={} workers={}",
        std::process::id(),
        addr,
        dir.display(),
        durability,
        backlog,
        workers
    );
    match serve(&cfg, &STOP, &LIVE) {
        Ok(rep) => {
            eprintln!(
                "shutdown: recovered {} wal recs ({} ms, snap {}) | accepted {} peak {} | rounds {} worst {} us | ops {} writes {} syncs {} avg {} us | checkpoints {} | {} reads inline on {} workers, {} ops forwarded",
                rep.rec.wal_records,
                rep.rec.elapsed_ms,
                rep.rec.snap_seq,
                rep.accepted,
                rep.peak_conns,
                rep.rounds,
                rep.worst_round_us,
                rep.ops,
                rep.writes,
                rep.syncs,
                rep.avg_sync_us,
                rep.checkpoints,
                rep.reads_inline,
                rep.workers,
                rep.forwarded
            );
        }
        Err(e) => {
            // WAL fsync 失败等：状态可能已超前于磁盘，绝不能当成正常退出。
            eprintln!("treedb-server FATAL: {e}");
            std::process::exit(1);
        }
    }
}

const HELP: &str = "\
treedb-server — 元数据引擎服务端（POSIX 元数据语义，客户端无关）

  --dir PATH                 数据目录（WAL + 快照），默认 /var/lib/treedb
  --listen HOST:PORT         监听地址，默认 127.0.0.1:7777
  --durability NAME          group（默认，每轮一次 fsync）| always | never
  --backlog N                listen 接受队列长度，默认 4096（受 somaxconn 截断）
  --workers N                前端 worker 线程数，默认 1（= 单线程事件循环，
                             全部基线数字的档）。>1 时每条线程自己 accept、
                             自己收发，只读 op 就地并行算；写永远由唯一提交
                             线程定序，group commit 仍然跨连接合并成一次 fsync。
                             写多读少的负载不要开：并行只偷得到读。
  --sync-ms N                事件循环空闲唤醒间隔，默认 20
  --auto-checkpoint-mb N     WAL 超此大小自动快照，默认 64（0 = 关闭）
  --pid-file PATH            启动后写入 pid，便于测试脚本 kill -9
  --stats                    不启服务，连上去打印状态后退出
  --shutdown                 通知运行中的实例优雅停服
";
