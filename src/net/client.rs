//! 同步客户端：一条 TCP 连接 = 一个 pipelined 会话。
//!
//! 故意做成阻塞式（`std::net`）：调用方那边的多线程 / FUSE 派发已经负责
//! 并发，客户端再引异步只会把依赖图搞脏。协议本身支持 pipeline，
//! [`Client::pipeline`] 就是「一次写多个请求、再一次读多个应答」，
//! 这是压满服务端 group commit 的正确姿势。

use crate::engine::codec::Rd;
use crate::engine::{encode_frame, Op, Reply, RxBuf, FRAME_HDR, MAX_FRAME};
use crate::error::{Error, Result};
use crate::net::auth::{self, Key};
use crate::net::record::Session;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub struct Client {
    stream: TcpStream,
    rx: RxBuf,
    /// 记录层（§4.5）。`None` = 这条线还是明文，
    /// 而 `send`/`recv` 的判据也只有这一个 `is_some`。
    secure: Option<Session>,
}

impl Client {
    /// 只建 TCP 连接，**不协商协议版本**。
    ///
    /// 这是给反例测试与跨代探针用的显式逃生口：正常调用方必须用
    /// [`Client::connect`] 或 [`Client::connect_auth`]。
    pub fn connect_raw(addr: &str, timeout_ms: u64) -> Result<Client> {
        let stream = TcpStream::connect(addr).map_err(|e| Error::Io(format!("connect {addr}: {e}")))?;
        if timeout_ms > 0 {
            let d = Duration::from_millis(timeout_ms);
            let _ = stream.set_read_timeout(Some(d));
            let _ = stream.set_write_timeout(Some(d));
        }
        let _ = stream.set_nodelay(true);
        Ok(Client {
            stream,
            rx: RxBuf::with_capacity(4096),
            secure: None,
        })
    }

    /// 连接并完成协议版本协商。`timeout=0` 表示不设超时（长尾 op 友好）。
    ///
    /// 协商失败（包括连到不认 `VersionBegin` 的旧服务端）直接返回错误，
    /// 不会退回未协商状态继续发数据 op。
    pub fn connect(addr: &str, timeout_ms: u64) -> Result<Client> {
        let mut c = Client::connect_raw(addr, timeout_ms)?;
        auth::version_handshake(&mut c)?;
        Ok(c)
    }

    /// 用已建好、已调优的 socket 构造**未协商**客户端。
    ///
    /// 给两类场景用：反例测试要验证服务端拒绝裸连接；调用方如果想自己管
    /// socket（复用、绑定、超时）也必须显式选择 [`Client::from_stream_auth`]
    /// 或 [`auth::version_handshake`] 才能进数据面。
    pub fn from_stream(stream: TcpStream) -> Client {
        let _ = stream.set_nodelay(true);
        Client {
            stream,
            rx: RxBuf::with_capacity(4096),
            secure: None,
        }
    }

    /// 连接 + 版本协商 + （给了密钥就）认证握手。
    ///
    /// `key = None` 与 [`Client::connect`] 等价；两者都先 fail-closed 地协商
    /// 协议版本，认证只是其后的可选步骤。
    pub fn connect_auth(addr: &str, timeout_ms: u64, key: Option<&Key>) -> Result<Client> {
        let mut c = Client::connect_raw(addr, timeout_ms)?;
        auth::handshake(&mut c, key)?;
        Ok(c)
    }

    /// [`Client::connect_auth`] 的不加密版：认证照跑，但不进记录层。
    ///
    /// 同样是逃生口而不是默认路径（理由见 [`Client::from_stream_auth_plain`]）：
    /// 代价实测需要「同一台服务端、同一份调用代码，只有加密开关不同」的 A/B，
    /// 而 `from_stream_*` 要调用方自备 socket（压测不会为了一个 flag 去接管
    /// socket 的调优）。服务端不 `--require-secure` 时这条线能正常拉起来。
    pub fn connect_auth_plain(addr: &str, timeout_ms: u64, key: Option<&Key>) -> Result<Client> {
        let mut c = Client::connect_raw(addr, timeout_ms)?;
        auth::handshake_plain(&mut c, key)?;
        Ok(c)
    }

    /// [`Client::from_stream`] 的握手版：自己调好 socket 之后也能协商版本并按需认证。
    ///
    /// `key = None` 也会完成版本协商；背压类用例因此既能调小缓冲区，又不会
    /// 因为绕开正常建连入口而被服务端正确拒绝。
    pub fn from_stream_auth(stream: TcpStream, key: Option<&Key>) -> Result<Client> {
        let mut c = Client::from_stream(stream);
        auth::handshake(&mut c, key)?;
        Ok(c)
    }

    /// 只跑认证握手、**不**协商记录层（保持明文）。
    ///
    /// 这是一个有意保留的逃生口，不是默认路径：它存在的意义是让「加密开着
    /// 与没开」能在同一份代码上 A/B（`benches/` 的代价实测就要这个对比），
    /// 以及让跨版本探针能拿明文看线。正常调用方用 [`Client::connect_auth`]。
    pub fn from_stream_auth_plain(stream: TcpStream, key: Option<&Key>) -> Result<Client> {
        let mut c = Client::from_stream(stream);
        auth::handshake_plain(&mut c, key)?;
        Ok(c)
    }

    /// 发一个 op 并等它的应答（一问一答）。
    pub fn call(&mut self, op: &Op) -> Result<Reply> {
        self.send(op)?;
        self.recv()
    }

    /// 只发不收：给 pipeline 用。
    pub fn send(&mut self, op: &Op) -> Result<()> {
        let mut w = crate::engine::W::new();
        op.encode(&mut w);
        self.put(&encode_frame(&w.finish()))
    }

    /// 把一段已经成形的帧字节送上连接。
    ///
    /// 切点就是 `self.secure`：它只能由 [`Client::arm_secure`] 在解码完
    /// `SecureOk` 之后置位，而服务端那侧的切点是「`SecureOk` 排进发送队列之后」。
    /// 两侧各自看得到同一个位置（都是那一帧的帧尾），所以不会出现一侧已封、
    /// 另一侧还按明文解。
    fn put(&mut self, blob: &[u8]) -> Result<()> {
        if self.secure.is_none() {
            return self
                .stream
                .write_all(blob)
                .map_err(|e| Error::Io(format!("send: {e}")));
        }
        // 密文要多一个局部缓冲：`seal` 得先算完整段才能发（标签在尾部），
        // 而 `write_all` 需要 `&self.stream`。明文热路径不付这笔钱。
        let mut sealed = Vec::with_capacity(blob.len() + 32);
        if !self
            .secure
            .as_mut()
            .expect("secure was just checked")
            .seal(blob, &mut sealed)
        {
            return Err(Error::Io(
                "record counters exhausted on this connection: reconnect (§4.5)".into(),
            ));
        }
        self.stream
            .write_all(&sealed)
            .map_err(|e| Error::Io(format!("send: {e}")))
    }

    /// 启客户端一侧的记录层，并把已经读进来的尾巴（那是记录）交给它。
    ///
    /// `recv` 一次能读 8 KiB，所解码完 `SecureOk` 时缓冲里很可能已经多了一段
    /// 字节。不把它们转给会话，下一轮就会拿密文去当明文帧头。
    pub(crate) fn arm_secure(&mut self, s: Session) -> Result<()> {
        let rest = self.rx.take_rest();
        self.secure = Some(s);
        if !rest.is_empty() {
            self.absorb(&rest)?;
        }
        Ok(())
    }

    /// 这条连接现在是不是密文。
    pub fn is_secure(&self) -> bool {
        self.secure.is_some()
    }

    /// 本端已发记录数（代价实测要拿它区分「真加密跑过」与「只是连上了」）。
    pub fn secure_records_sent(&self) -> u64 {
        self.secure.as_ref().map(|s| s.sent_records()).unwrap_or(0)
    }

    /// 收一个应答帧。内部缓冲里已有的帧优先，避免每次都 read。
    pub fn recv(&mut self) -> Result<Reply> {
        loop {
            if let Some(f) = self.rx.take_frame()? {
                return Reply::decode(&mut Rd::new(&f));
            }
            let mut buf = [0u8; 8192];
            match self.stream.read(&mut buf) {
                Ok(0) => return Err(Error::Io("server closed".into())),
                Ok(n) => self.absorb(&buf[..n])?,
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::Io(format!("recv: {e}"))),
            }
        }
    }

    /// 线上字节 → 明文帧：没加密就直接进 `rx`，加密就先验签解密。
    ///
    /// 返 `Err` 就是这条连接该丢了：标签验不过意味着两侧已经不在同一个
    /// 计数器上，继续读只会把乱字节当合法帧解下去（`record` 模块头第 2 条）。
    fn absorb(&mut self, more: &[u8]) -> Result<()> {
        match &mut self.secure {
            None => {
                self.rx.extend(more);
                Ok(())
            }
            Some(s) => {
                let mut plain = Vec::with_capacity(more.len());
                s.feed(more, &mut plain)
                    .map_err(|e| Error::Denied(format!("{e} (record layer, §4.5)")))?;
                self.rx.extend(&plain);
                Ok(())
            }
        }
    }

    /// 一次发、一次收。N 个请求在服务端会落进同一个 group commit。
    pub fn pipeline(&mut self, ops: &[Op]) -> Result<Vec<Reply>> {
        let mut blob: Vec<u8> = Vec::new();
        for op in ops {
            let mut w = crate::engine::W::new();
            op.encode(&mut w);
            blob.extend_from_slice(&encode_frame(&w.finish()));
        }
        self.put(&blob)
            .map_err(|e| Error::Io(format!("pipeline send: {e}")))?;
        let mut out = Vec::with_capacity(ops.len());
        for _ in 0..ops.len() {
            out.push(self.recv()?);
        }
        Ok(out)
    }

    /// 连接保活探测：能 Ping 通就算活。
    pub fn ping(addr: &str) -> bool {
        Client::ping_auth(addr, None)
    }

    /// 同 [`Client::ping`]，但对启用了认证的服务端要先握手才能探到活。
    ///
    /// 这个区分不是可有可无的：少了它，调用方会把“服务端正在启动”与
    /// “我根本没密钥”看成同一个结果（两个都是 `false`）。
    pub fn ping_auth(addr: &str, key: Option<&Key>) -> bool {
        match Client::connect_auth(addr, 2000, key) {
            Ok(mut c) => matches!(c.call(&Op::Ping), Ok(Reply::Pong)),
            Err(_) => false,
        }
    }

    /// 交给上层做「一次读多帧」时使用：缓冲里是否已有完整帧。
    pub fn has_buffered(&self) -> bool {
        self.rx.pending() >= FRAME_HDR && self.rx.peek_len().map(|l| l <= MAX_FRAME).unwrap_or(false)
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}
