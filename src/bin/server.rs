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
use std::sync::Arc;
use treedb::engine::{Durability, Op, Reply, PROTOCOL_VERSION};
use treedb::net::auth::Key;
use treedb::net::client::Client;
use treedb::net::server::{serve, ServerConfig};

static STOP: AtomicBool = AtomicBool::new(false);
static LIVE: AtomicUsize = AtomicUsize::new(0);

extern "C" fn on_signal(_sig: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

fn parse_durability(s: &str) -> Durability {
    // 解析器在引擎里（副本二进制共用同一份）：拼错不再是"静默降级成 group"，
    // 而是退出码 2。
    match Durability::parse(s) {
        Ok(d) => d,
        Err(e) => die(&e.to_string()),
    }
}

/// 参数错误一律退出码 2（POSIX 的 usage error），与运行期 FATAL 的 1 分开。
fn die(msg: &str) -> ! {
    eprintln!("{msg}\n\n{HELP}");
    std::process::exit(2);
}

/// 数值参数：缺值/不是数/越界都是启动错误。
/// 以前是 `.parse().unwrap_or(默认值)`，于是 `--workers abc` 会默默跑成 auto，
/// `--sync-ms 0` 与不给完全是两件事（前者关掉周期 sync），都不该靠猜。
fn num<T: std::str::FromStr>(flag: &str, raw: &str, default: T) -> T {
    if raw.is_empty() {
        return default;
    }
    match raw.parse() {
        Ok(v) => v,
        Err(_) => die(&format!("bad {flag} {raw:?}: expected a number")),
    }
}

fn print_stats(addr: &str, key: Option<&Key>) -> Result<(), String> {
    let mut c = Client::connect_auth(addr, 3000, key).map_err(|e| e.to_string())?;
    match c.call(&Op::Stats).map_err(|e| e.to_string())? {
        Reply::Stats(s) => {
            println!("views      = {}", s.views);
            println!("nodes      = {}", s.nodes);
            println!("dentries   = {}", s.dentries);
            println!("locks      = {}", s.locks);
            println!("refs       = {}", s.refs);
            println!("keys       = {}", s.keys);
            println!("audit      = {}", s.audit);
            println!("tables     = {} (rows {})", s.tables, s.rows);
            println!("seq        = {} (snapshot {})", s.seq, s.snap_seq);
            // 两个长度各说一件事：逻辑长度决定 checkpoint 何时触发，磁盘长度
            // 决定这块盘被占了多少（预分配的空洞差一个数量级）。
            println!(
                "wal        = {} B logical, {} B on disk",
                s.wal_bytes, s.wal_disk_bytes
            );
            println!("snapshot   = {} B", s.snap_bytes);
            println!("protocol   = {}", s.proto_version);
            println!(
                "ops        = {} (writes {}, syncs {}, avg fsync {} us)",
                s.ops_total, s.writes_total, s.syncs_total,
                if s.syncs_total > 0 { s.sync_us_total / s.syncs_total } else { 0 }
            );
            println!("checkpoints= {}", s.checkpoints);
            // 只在副本上打印：主那边这几个数永远是 0，多打几行只会让运维
            // 以为“0 lag”是一种健康状态。
            if s.replica {
                println!(
                    "replica    = yes (primary seq {} lag {} applied {} resyncs {} errors {})",
                    s.replica_primary_seq, s.replica_lag, s.replica_applied,
                    s.replica_resyncs, s.replica_errors
                );
            }
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
    let mut workers: usize = 0;
    let mut flush_pipeline = false;
    let mut stats_only = false;
    let mut shutdown_only = false;
    let mut pid_file: Option<PathBuf> = None;
    let mut auth_key_file: Option<PathBuf> = None;

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
            "--sync-ms" => sync_ms = num("--sync-ms", &next(&mut i), sync_ms),
            "--auto-checkpoint-mb" => {
                auto_ckpt_mb = num("--auto-checkpoint-mb", &next(&mut i), auto_ckpt_mb)
            }
            "--backlog" => backlog = num("--backlog", &next(&mut i), backlog),
            "--workers" => workers = num("--workers", &next(&mut i), workers),
            "--flush-pipeline" => flush_pipeline = true,
            "--pid-file" => pid_file = Some(PathBuf::from(next(&mut i))),
            "--auth-key-file" => auth_key_file = Some(PathBuf::from(next(&mut i))),
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

    // 缺值（`--dir` 后面没东西）也是错误：`next()` 会回空串，不能让
    // `--dir` 空着跑成默认目录。
    if dir.as_os_str().is_empty() {
        die("empty --dir");
    }

    // 0 = auto，与 [`ServerConfig::new`] 同一个解析函数：两处各写一套
    // 默认值的话，嵌入式用法与两行命令拉起的实例性能特征就会不一。
    if workers == 0 {
        workers = treedb::net::default_workers();
    }

    // 密钥先于一切加载：一个被要它认证、却因为密钥文件读不到而默默降级成
    // 不认证的实例，比拒接启动坏得多。`--stats` / `--shutdown` 也要用它，
    // 所以放在那两个分支之前。
    let auth: Option<Arc<Key>> = match &auth_key_file {
        Some(p) => match Key::load(p) {
            Ok(k) => Some(Arc::new(k)),
            Err(e) => {
                eprintln!("bad --auth-key-file: {e}");
                std::process::exit(2);
            }
        },
        None => None,
    };
    let auth_on = auth.is_some();

    if stats_only {
        if let Err(e) = print_stats(&listen, auth.as_deref()) {
            eprintln!("stats: {e}");
            std::process::exit(1);
        }
        return;
    }
    if shutdown_only {
        match Client::connect_auth(&listen, 3000, auth.as_deref())
            .and_then(|mut c| c.call(&Op::Shutdown))
        {
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
        flush_pipeline,
        // 这个二进制永远是主：副本入口是 `treedb-standby`，它把同一个
        // `serve` 配上 `replication` 就跑起来。两边各留一套 flag 解析的话，
        // 副本会拿到一份没人测过的默认值。
        replication: None,
        auth,
    };
    eprintln!(
        // 帧里没有版本位（§4.1），升级顺序又是硬约束，所以 banner 一开就把它摆出来：
        // 运维在两端对不上时第一眼就该看见。
        "treedb-server pid={} listen={} dir={} protocol={} durability={} backlog={} workers={} auth={}",
        std::process::id(),
        addr,
        dir.display(),
        PROTOCOL_VERSION,
        durability,
        backlog,
        workers,
        if auth_on { "on" } else { "off" }
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
  --durability NAME          group（默认，每轮一次 fsync）| always（与 group 同一个
                             落盘点：已 ack 的写必已落盘，不再按连接串 fsync）| never
  --backlog N                listen 接受队列长度，默认 4096（受 somaxconn 截断）
  --workers N                前端 worker 线程数，默认 0 = auto（按核数取
                             min(8, 核数/2)，至少 1）。=1 就是单线程事件
                             循环，也是全部历史基线数字的档。>1 时每条线程
                             自己 accept、自己收发，只读 op 就地并行算；写永远
                             由唯一提交线程定序，group commit 仍然跨连接合并成
                             一次 fsync。写多读少的负载可以显式收到 1：并行
                             只偷得到读，且每个 worker 要多占 3 个 fd。
  --sync-ms N                事件循环空闲唤醒间隔，默认 20
  --flush-pipeline           写轴流水线（§11 第 8 条）：提交线程只 append，
                             fdatasync 交给专用线程，落盘回执到了再 ack。
                             默认关：实测在 fsync 1.3–1.6 ms 的盘上是净负收益
                             （多一次线程往返，而 fsync 期间并没有可重叠的活），
                             只在该盘 fsync 占比高、或单个 op 的 translate 很重时
                             才值得打开。打开后 ack 语义不变（仍严格晚于落盘）。
  --auto-checkpoint-mb N     WAL 超此大小自动快照，默认 64（0 = 关闭）
  --pid-file PATH            启动后写入 pid，便于测试脚本 kill -9
  --auth-key-file PATH       启用接入认证（§4.4）：从文件读集群共享密钥，必须是
                             32 原始字节或 64 个十六进制字符（0/64 个零 = 没填的
                             模板，拒接）。不给这个 flag = 不认证，与 v4 逐字一致。
                             开了之后所有客户端（含 --stats / --shutdown）都得带
                             同一把密钥跑完握手才能发数据 op。
  --stats                    不启服务，连上去打印状态后退出（含表的行数、WAL 的
                             逻辑与磁盘两个长度、快照长度与协议版本）
  --shutdown                 通知运行中的实例优雅停服
";
