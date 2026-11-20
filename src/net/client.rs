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
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub struct Client {
    stream: TcpStream,
    rx: RxBuf,
}

impl Client {
    /// 连接 `host:port`。`timeout=0` 表示不设超时（长尾 op 友好）。
    pub fn connect(addr: &str, timeout_ms: u64) -> Result<Client> {
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
        })
    }

    /// 用已建好、已调优的 socket 构造客户端。
    ///
    /// 给两类场景用：测试要先把缓冲区调小以触发服务端背压；调用方
    /// 如果想自己管 socket（复用、绑定、超时）也不必经本模块的 `connect`。
    pub fn from_stream(stream: TcpStream) -> Client {
        let _ = stream.set_nodelay(true);
        Client {
            stream,
            rx: RxBuf::with_capacity(4096),
        }
    }

    /// 连接 + （给了密钥就）握手。`key = None` 与 [`Client::connect`] 逐字等价。
    ///
    /// 为什么不把握手合进 `connect`：握手是两次往返，它会让“没打算认证”的
    /// 调用方白付两次 RTT，也会直接弄坏 v4 服务端。存在 [`auth::handshake`]
    /// 那一层，开关才能只落在真正需要它的地方。
    pub fn connect_auth(addr: &str, timeout_ms: u64, key: Option<&Key>) -> Result<Client> {
        let mut c = Client::connect(addr, timeout_ms)?;
        auth::handshake(&mut c, key)?;
        Ok(c)
    }

    /// [`Client::from_stream`] 的认证版：自己调好 socket 之后也能跑握手。
    ///
    /// 不另外提供一个 `from_stream` 是为了背压类用例：它们必须能把缓冲区调到
    /// 很小、又得能连上开了认证的服务端。
    pub fn from_stream_auth(stream: TcpStream, key: Option<&Key>) -> Result<Client> {
        let mut c = Client::from_stream(stream);
        auth::handshake(&mut c, key)?;
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
        self.stream
            .write_all(&encode_frame(&w.finish()))
            .map_err(|e| Error::Io(format!("send: {e}")))
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
                Ok(n) => self.rx.extend(&buf[..n]),
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::Io(format!("recv: {e}"))),
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
        self.stream
            .write_all(&blob)
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
