//! 集成测试共用脚手架：起真进程 `treedb-server`、挑端口、等就绪、可控地杀掉它。
//!
//! 每个 `Server` 拥有自己的数据目录（`tempfile::TempDir`）与子进程，Drop 时
//! 保证回收，不会留下占着端口的孤儿进程。

// 这是**七个测试 crate 共用的**脚手架：每个 crate 只用到其中一部分（只有崩溃
// 用例要 `sigkill_and_take_dir`、只有连接风暴用例要 `start_backlog`）。不用
// `#[allow(dead_code)]` 的话，每加一个只被一个 crate 用到的方法，就会在其余
// 六个 crate 里各报一条 never used —— 那种噪声会让人习惯性无视编译警告。
#![allow(dead_code)]

use std::net::TcpListener;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use treedb::engine::sha256::DIGEST_LEN;
use treedb::net::auth::Key;
use treedb::net::client::Client;

/// 找一个没人用的 127.0.0.1 端口。绑定后立刻释放，交给服务端去占。
pub fn free_addr() -> String {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let a = l.local_addr().expect("local_addr");
    drop(l);
    a.to_string()
}

/// [`Server::start_auth`] 用的那把密钥。
///
/// 公开它不是为了“测试自己生成密钥”，而是为了让副本用例能在测试进程里
/// 拼出同一个 `ReplicaCfg::auth`。测试要的是可复现，不是强度：强度由
/// `Key::load` 的长度门禁与 `net::auth` 的单测负责。
pub fn test_key() -> Key {
    Key([0x5au8; DIGEST_LEN])
}

pub struct Server {
    pub addr: String,
    dir: PathBuf,
    child: Option<Child>,
    log: PathBuf,
    pid: u32,
    /// 本实例的接入密钥（`None` = 不认证）。它存在这里的理由不是方便，
    /// 是正确性：[`Server::wait_ready`] 与 [`Server::client`] 都要按它决定要不要
    /// 握手，否则开了认证的实例在探活那一步就永远不 ready，而报出来的错是
    /// “服务端没起来”。
    auth: Option<Key>,
    // 数据目录就住在这个 TempDir 里。用 `Option` 是因为 `Server` 实现了
    // `Drop`，字段不能直接搬出去；而崩溃恢复用例必须把目录交给重启的
    // 那个 `Server`，所以得能把它取出来。
    tmp: Option<TempDir>,
}

/// 停服后交还的现场：退出码 + 完整保留的数据目录。
///
/// `dir` 住在 `tmp` 里，想让它在重启后还存在，就必须自己拿着 `tmp`。
pub struct Stopped {
    pub status: std::process::ExitStatus,
    pub dir: PathBuf,
    pub tmp: TempDir,
}

impl Server {
    /// 在全新目录里起一个服务端。`durability` = `always|group|never`。
    pub fn start(durability: &str) -> Server {
        Server::start_backlog(durability, 4096)
    }

    /// 同上，但显式指定 `--workers`（前端 worker 线程数）。读扩展类用例走这里。
    pub fn start_workers(durability: &str, workers: usize) -> Server {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("data");
        let s = Server::spawn(tmp, dir, free_addr(), durability, 4096, workers, None);
        s.wait_ready();
        s
    }

    /// 起一个**启用了接入认证**的服务端（§4.4）。
    ///
    /// 密钥是 [`test_key`] 那个固定值：用例要的是可复现与可断言（“错密钥必
    /// 须被拒”需要一个已知值）。
    pub fn start_auth(durability: &str, workers: usize) -> Server {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("data");
        let key = test_key();
        let s = Server::spawn(tmp, dir, free_addr(), durability, 4096, workers, Some(key));
        s.wait_ready();
        s
    }

    /// 同上，但显式指定 `--backlog`：万级连接风暴用例需要把它拉大。
    pub fn start_backlog(durability: &str, backlog: i32) -> Server {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("data");
        let s = Server::spawn(tmp, dir, free_addr(), durability, backlog, 1, None);
        s.wait_ready();
        s
    }

    /// 起一个带额外 flag 的服务端（`--max-conn` 这类运行期旋钮）。
    ///
    /// 仍在参数形式上报 `(flag, value)` 对，而不是一个拼好的字符串：
    /// 用例里看得见自己到底改了什么，也不会绕过下面那套固定参数。
    pub fn start_extra(durability: &str, workers: usize, extra: &[&str]) -> Server {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("data");
        let s = Server::spawn_extra(
            tmp,
            dir,
            free_addr(),
            durability,
            4096,
            workers,
            None,
            extra,
        );
        s.wait_ready();
        s
    }

    /// 用**同一个数据目录**在新端口重启（崩溃恢复用例的核心动作）。
    pub fn restart_at(tmp: TempDir, dir: PathBuf, durability: &str) -> Server {
        Server::restart_at_addr(tmp, dir, free_addr(), durability)
    }

    /// 同上，但重启后也带多前端 worker：验证“并行读档下的写 ack”在崩溃路径
    /// 上同样成立。
    pub fn restart_at_workers(
        tmp: TempDir,
        dir: PathBuf,
        durability: &str,
        workers: usize,
    ) -> Server {
        let s = Server::spawn(tmp, dir, free_addr(), durability, 4096, workers, None);
        s.wait_ready();
        s
    }

    /// 用同一个数据目录 + **同一个地址**重启：优雅停服后端口必须能立刻重绑
    /// （std 的 `TcpListener::bind` 自带 SO_REUSEADDR，TIME_WAIT 不该挡道）。
    pub fn restart_at_addr(tmp: TempDir, dir: PathBuf, addr: String, durability: &str) -> Server {
        let s = Server::spawn(tmp, dir, addr, durability, 4096, 1, None);
        s.wait_ready();
        s
    }

    fn spawn(
        tmp: TempDir,
        dir: PathBuf,
        addr: String,
        durability: &str,
        backlog: i32,
        workers: usize,
        auth: Option<Key>,
    ) -> Server {
        Server::spawn_extra(tmp, dir, addr, durability, backlog, workers, auth, &[])
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_extra(
        tmp: TempDir,
        dir: PathBuf,
        addr: String,
        durability: &str,
        backlog: i32,
        workers: usize,
        auth: Option<Key>,
        extra: &[&str],
    ) -> Server {
        let log = tmp.path().join(format!("server-{addr}.log"));
        let pid_file = tmp.path().join("server.pid");
        let key_file = tmp.path().join("auth.key");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_treedb-server"));
        cmd.arg("--dir")
            .arg(&dir)
            .arg("--listen")
            .arg(&addr)
            .arg("--durability")
            .arg(durability)
            .arg("--backlog")
            .arg(backlog.to_string())
            .arg("--workers")
            .arg(workers.to_string())
            .arg("--pid-file")
            .arg(&pid_file);
        if let Some(k) = &auth {
            // 先落密钥文件、再起进程：`--auth-key-file` 读不到会直接 exit 2，
            // 那比“密钥文件没写成”与“服务起不来”混在一起好查。
            std::fs::write(&key_file, k.0).expect("write auth key");
            cmd.arg("--auth-key-file").arg(&key_file);
        }
        // 额外 flag 放在认证之后、环境变量之前：顺序不影响解析（每个 flag
        // 自己带值），但固定顺序才能让日志里的命令行可复现。
        cmd.args(extra);
        // 写轴流水线档（§11 第 8 条）。用环境变量而不是改 spawn 签名：
        // 用例只需要“同一套脚本在两条路径上都跑一遍”，而重启路径
        // （`restart_at*`）也走这里，所以两边自动一致。
        if std::env::var_os("TREEDB_TEST_FLUSH_PIPELINE").is_some() {
            cmd.arg("--flush-pipeline");
        }
        let out = Stdio::from(std::fs::File::create(&log).expect("create server log"));
        let child = cmd
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(Stdio::from(
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&log)
                    .expect("open server log"),
            ))
            .spawn()
            .expect("spawn treedb-server");
        let pid = child.id();
        Server { addr, dir, child: Some(child), log, pid, auth, tmp: Some(tmp) }
    }

    /// 轮询 Ping 直到服务端能答话。冷启动（含 WAL 重放）可能慢，给 15s。
    ///
    /// 走 `ping_auth` 而不是 `ping`：开了认证的实例对不握手的探测只会回
    /// EADV，那会被误读成“还没起来”，然后超时报错。
    pub fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if Client::ping_auth(&self.addr, self.auth.as_ref()) {
                return;
            }
            if Instant::now() > deadline {
                panic!("server at {} never became ready:\n{}", self.addr, self.log_text());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    pub fn client(&self) -> Client {
        Client::connect_auth(&self.addr, 0, self.auth.as_ref()).expect("connect")
    }

    /// 本实例的密钥。用例拿它去构造“自己管 socket”的连接（背压、错密钥、
    /// 故意不握手那些反例）；没开认证时是 `None`。
    pub fn auth_key(&self) -> Option<&Key> {
        self.auth.as_ref()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// 读回 `--pid-file`，用来验证 bin 真的把 pid 写对了（外部看护靠它 kill）。
    pub fn pid_from_file(&self) -> u32 {
        let p = self.tmp_path().join("server.pid");
        std::fs::read_to_string(&p)
            .unwrap_or_else(|e| panic!("read {}: {e}", p.display()))
            .trim()
            .parse()
            .expect("pid file is a number")
    }

    fn tmp_path(&self) -> &Path {
        self.tmp.as_ref().expect("temp dir already handed over").path()
    }

    /// `kill -9`：不给进程任何清理机会，然后 reap。
    /// 返回 `(TempDir, dir)`：目录必须被调用方握住，重启后才还在。
    pub fn sigkill_and_take_dir(mut self) -> (TempDir, PathBuf) {
        let mut child = self.child.take().expect("child already reaped");
        let pid = self.pid;
        // SIGKILL 而非 SIGTERM：SIGTERM 走的是优雅退出路径，测不到崩溃语义。
        let rc = unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        assert_eq!(rc, 0, "kill -9 {pid} failed");
        let st = child.wait().expect("wait after SIGKILL");
        assert!(
            st.signal() == Some(9),
            "server did not die from SIGKILL: {st:?}"
        );
        let tmp = self.tmp.take().expect("temp dir already handed over");
        (tmp, self.dir.clone())
    }

    /// 优雅停服：发 `Op::Shutdown`，等服务端自己退出并检查退出码。
    /// 只想验退码的用例直接丢掉返回值即可（数据目录随之一同清理）。
    pub fn graceful_shutdown(mut self) -> Stopped {
        let status = self.wait_for_exit();
        Stopped {
            status,
            dir: self.dir.clone(),
            tmp: self.tmp.take().expect("temp dir already handed over"),
        }
    }

    fn wait_for_exit(&mut self) -> std::process::ExitStatus {
        {
            let mut c = self.client();
            let _ = c.call(&treedb::engine::Op::Shutdown);
        }
        self.poll_exit()
    }

    /// 只等进程自己退出并检查退出码，**不**再发 `Op::Shutdown`。
    ///
    /// 停服由外部（`treedb-server --shutdown`）触发时用这个：那时端口已经关了，
    /// 走 [`Server::graceful_shutdown`] 会在 `connect` 上 panic，而用例要验的
    /// 恰恰是「停服被客户端确认」与「进程干净退出」这两件事。
    pub fn wait_exit(mut self) -> std::process::ExitStatus {
        self.poll_exit()
    }

    fn poll_exit(&mut self) -> std::process::ExitStatus {
        let mut child = self.child.take().expect("child already reaped");
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match child.try_wait().expect("try_wait") {
                Some(st) => {
                    assert!(
                        st.success(),
                        "non-zero exit after Shutdown: {st:?}\n{}",
                        self.log_text()
                    );
                    return st;
                }
                // `wait_timeout` 仍是 nightly，故自己轮询 try_wait。
                None if Instant::now() > deadline => {
                    panic!("server did not exit on Shutdown:\n{}", self.log_text())
                }
                None => std::thread::sleep(Duration::from_millis(20)),
            }
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // 用例中途 panic 也不能留孤儿：直接 SIGKILL + reap。
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// 轮询直到条件成立，失败即 panic（带服务端日志，省一轮复现）。
pub fn wait_until<F: FnMut() -> bool>(what: &str, srv: &Server, mut f: F) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !f() {
        if Instant::now() > deadline {
            panic!("condition never held: {what}\nserver log:\n{}", srv.log_text());
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}
