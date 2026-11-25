//! 一条连接的两个形状：连运行中的服务端，或者独占一份数据目录。
//!
//! 为什么要两种：`--addr` 是运维现实（服务在跑，人要往里看、要改一行），而
//! `--dir` 是「服务没起来 / 起不来 / 我只想离线核一条记录」那一档。两种都必须经过
//! **同一条写路径**：
//!
//! - `--addr` 走 [`crate::net::Client::call`]，一个 op 一帧，服务端做 translate →
//!   WAL append → fsync → ack。
//! - `--dir` 走 [`crate::engine::Engine::submit`]，同一个 `translate`、同一个
//!   `commit_batch`、同一次 fsync，只是中间没有 socket（`core.rs` 给这条路径的原话是
//!   「内嵌用法（不经网络）：一个 op 走完 stage → WAL → 返回 ack」）。
//!
//! 也就是说：**两种模式都不绕过 WAL**，区别只在「谁持有那个目录」。`Engine::open` 会
//! 拿 `.lock`（`DirLock::acquire`），服务端在跑时它直接失败并报目录被占 —— 这正是想要的
//! 行为：两个人同时往一份 WAL 里写会真把数据弄坏，绝不能静默地"各写各的"。
//!
//! 一个诚实的差别必须写在这儿：`--dir` 模式的 `Stats` 数的是**本进程**看到的
//! `ops_total`/`writes_total`/`syncs_total`（进程启动后从零开始累加），而 `--addr` 模式的
//! 那三个数是服务端自启动以来的累计。`seq`/`snap_seq`/`wal_bytes`/`tables`/`rows` 两边都是
//! 磁盘状态的真相，可以互相印证；那三个计数器不行。

use std::io::Write;
use std::path::PathBuf;

use crate::engine::core::{Durability, Engine};
use crate::engine::model::{Op, Reply};
use crate::net::auth::Key;
use crate::net::Client;

/// 从命令行收上来的连接参数。两种模式互斥，`addr` 是默认。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Opts {
    pub addr: String,
    pub dir: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub timeout_ms: u64,
    /// 只作用于 `--dir` 模式：一次 CLI 调用就是一条提交，默认与 `treedb-server` 的默认档
    /// 一致（`group`）。`always` 在同一条路径上（§7.9 那条注释），差别只在名字。
    pub durability: Durability,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            // 与 `treedb-server` / `treedb-bench` 的默认端口同一个值：三个入口指同一份
            // 默认配置，运维不需要记住"bench 是 7777 而 cli 是别的"。
            addr: "127.0.0.1:7777".to_string(),
            dir: None,
            key_file: None,
            timeout_ms: 3000,
            durability: Durability::Group,
        }
    }
}

/// 一个已经建好的会话。`Drop` 会释放目录锁（`--dir` 模式），所以它必须活到命令结束。
pub struct Session {
    mode: Mode,
    /// 本会话走的是哪条路，只用于打印头部提示与 JSON 里的 `via` 字段。
    pub via: &'static str,
}

// `Engine` 比 `Client` 大一个数量级（前者内联了整份内存态的句柄），所以装箱：
// `Mode`/`Session` 保持小尺寸，代价是每进程一次堆分配 —— 会话只建一次，买得起。
enum Mode {
    Net(Client),
    Off(Box<Engine>),
}

// `Client`/`Engine` 本身不导 `Debug`（前者持 socket 与缓冲，后者持整个内存态），所以
// 这里只打印足以定位「会话走的是哪条路」的信息。测试里 `expect_err` 需要 `Session: Debug`。
impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Session(via = {})", self.via)
    }
}

/// 一个致命错误的表达：与 [`Reply::Err`] 分开。
///
/// 分开是有内容的：`Reply::Err` 是「引擎收到了、按语义回你一个 errno」（EEXIST /
/// ENOENT / EAGAIN / EINVAL……），退出码 1；`Fatal` 是「这个 op 根本没被求值」——连不上、
/// 协议不同代、目录被别的实例占着，退出码 3。把两者混成一个字符串，脚本就没法区分
/// 「这行已经存在」与「服务端压根没起」。
#[derive(Debug)]
pub struct Fatal(pub String);

impl Session {
    /// 按 `Opts` 建会话。失败一律是 [`Fatal`]：这里没有引擎语义可言。
    pub fn open(o: &Opts) -> Result<Session, Fatal> {
        match &o.dir {
            None => {
                let key = match &o.key_file {
                    Some(p) => Some(Key::load(p).map_err(|e| Fatal(format!("--auth-key-file: {e}")))?),
                    None => None,
                };
                let c = Client::connect_auth(&o.addr, o.timeout_ms, key.as_ref())
                    .map_err(|e| Fatal(format!("connect {}: {e}", o.addr)))?;
                Ok(Session { mode: Mode::Net(c), via: "tcp" })
            }
            Some(d) => {
                let eng = Engine::open(d, o.durability)
                    .map_err(|e| Fatal(format!("open {}: {e}", d.display())))?;
                Ok(Session { mode: Mode::Off(Box::new(eng)), via: "dir" })
            }
        }
    }

    /// 发一个 op。`Ok(reply)` 包含语义错误（`Reply::Err`），`Err(Fatal)` 只表示传输层
    /// 或开局失败。
    pub fn call(&mut self, op: &Op) -> Result<Reply, Fatal> {
        match &mut self.mode {
            Mode::Net(c) => c.call(op).map_err(|e| Fatal(format!("call: {e}"))),
            Mode::Off(eng) => eng.submit(op).map_err(|e| Fatal(format!("submit: {e}"))),
        }
    }

    /// 会话是不是只走本地目录。交互式提示符要带它：同一个 `row put` 打在屏幕上，
    /// 人是连在别人跑着的服务端上，还是在自己独占的一份目录上，必须看得出来。
    pub fn is_offline(&self) -> bool {
        matches!(self.mode, Mode::Off(_))
    }
}

/// 一次 `Op::Stats` 的 `seq` 读数，给同值消除那道提示用（见 `commands::row_put`）。
///
/// 拿不到就返回 `None`，调用方据此**少说一句**而不是把命令判失败：这条探查是附加
/// 信息，它坏了不该让一次已经落盘的写入看起来失败了。
pub fn seq_probe(s: &mut Session) -> Option<(u64, u64)> {
    match s.call(&Op::Stats) {
        Ok(Reply::Stats(st)) => Some((st.seq, st.writes_total)),
        _ => None,
    }
}

/// 统一的落笔口：`println!` 走 stdout，而任何「这事没成」的解释走 stderr。
///
/// 之所以要在类型上强制：`treedb-cli dump --out -` 把 dump 本体写到 stdout，此时
/// 任何一句混进去的提示都会毁掉那份字节流。凡是往 stdout 写数据的路径，提示必须用这
/// 个函数送到 stderr。
pub fn note(err: &mut dyn Write, args: std::fmt::Arguments<'_>) -> std::io::Result<()> {
    writeln!(err, "{args}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_server_flag_table() {
        let o = Opts::default();
        // 这三个值必须与 src/bin/server.rs 的 HELP 逐字对得上，改那边要同时改这里。
        assert_eq!(o.addr, "127.0.0.1:7777");
        assert_eq!(o.dir, None);
        assert!(matches!(o.durability, Durability::Group));
    }

    #[test]
    fn offline_session_takes_the_dir_lock() {
        // 同一个目录开两次：第二次必须失败，而不是两个人各写各的 WAL。这条是
        // `Session::open` 唯一能被纯本地验证的性质，所以值得单独一个测试。
        let dir = tempfile::tempdir().expect("tempdir");
        let o = Opts {
            dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let _a = Session::open(&o).expect("first open");
        let e = Session::open(&o).expect_err("second open must fail");
        let msg = e.0.to_lowercase();
        assert!(
            msg.contains("lock") || msg.contains("bus") || msg.contains("占"),
            "第二个会话的失败原因没说是目录被占：{}",
            e.0
        );
    }

    #[test]
    fn unreachable_addr_is_fatal_not_semantic() {
        // 端口 1 上没有服务端。必须是 Fatal（退出码 3 那一档），不能是 Reply::Err。
        let o = Opts {
            addr: "127.0.0.1:1".to_string(),
            timeout_ms: 200,
            ..Default::default()
        };
        let e = Session::open(&o).expect_err("must not connect");
        assert!(e.0.contains("connect"), "{}", e.0);
    }
}
