//! 传输层加密（记录层）验收（DESIGN.md §4.5）。
//!
//! `src/net/record.rs` 的单测证的是**会话本身**（分段无关、逐位篡改、重放、
//! 方向分域）。这里证的是**接线**：切点落在真实 TCP 上的正确位置，以及两条
//! worker 形状与 `--require-secure` 那档语义。
//!
//! 主张如下（编号与下面的用例对应，一个用例钉一条）：
//!
//! 1. 带密钥的建连必然已切到记录层，且超过一个记录的大值、流水线批在
//!    `workers=1` 与 `workers=8` 上都逐字节回得来（两条循环各自封帧）；副本线
//!    那两条只读 op（`WalFetch`/`SnapFetch`）过层后与明文线逐字节相同；
//! 2. **线上取不到明文**：夹在中间的代理看得见握手帧，却看不见切点之后的
//!    canary —— 而同一条代理在明文客户端下能看见它（对照组，缺了它「看不见」
//!    就等于「没采到」）；
//! 3. 线上翻转一个 bit ⇒ 断连 + 那条写**没有落库**（不是落了一个错值）；
//! 4. `--require-secure` 拒绝「已认证但明文」的连接，报的是缺 SecureBegin
//!    那一拍，而不是缺版本协商；
//! 5. 没开 `--require-secure` 的 v9 服务端仍然容忍明文客户端（升级窗口）；
//! 6. 服务端把 `SecureOk.suite` 改成 0（或旧服务端不认识 op tag）时，带密钥的
//!    客户端必须拒绝降级，而不是静默走明文。
//!
//! 跑法：`cargo test --test record`

#[path = "common/mod.rs"]
mod common;

use common::Server;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;
use treedb::engine::model::{
    errno, ColumnDef, Op, Reply, TableDef, COND_NONE, SNAP_CHUNK_MAX, WAL_FETCH_MAX, WAL_HINT_NONE,
};
use treedb::net::auth::Key;
use treedb::net::client::Client;

/// op/reply 的 tag（`model.rs`）。这里写死数值而不是从 crate 取：取证代理要靠
/// 它在**线上**定位切点，而这两个数一旦变了，本文件那些「明文帧结构」的假设
/// 就全废了 —— 让它们在这里当场失效，比让代理悄悄找不到目标好。
const OP_SECURE_BEGIN: u16 = 0x0093;
const REPLY_SECURE_OK: u16 = 0x0193;
/// 帧头（`len:u32` 小端）与记录头同宽。
const HDR: usize = 4;
/// 切点之后第一条记录里被翻位的字节：`4` 字节记录长度头再往后第 2 个密文字节。
const CT_BYTE: usize = HDR + 1;

const TBL: &str = "wire";

fn ok(c: &mut Client, op: &Op) -> Reply {
    match c.call(op) {
        Ok(Reply::Err { code, msg }) => panic!(
            "{op:?} → errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        Ok(r) => r,
        Err(e) => panic!("{op:?} → transport error: {e}"),
    }
}

/// 一列不透明字节的通用表，够装 canary 与大值。
fn table(name: &str) -> Op {
    Op::TableCreate {
        def: TableDef {
            name: name.as_bytes().to_vec(),
            columns: vec![ColumnDef {
                name: b"b".to_vec(),
                flags: 0,
            }],
        },
    }
}

fn put(key: &str, v: &[u8]) -> Op {
    Op::RowPut {
        table: TBL.as_bytes().to_vec(),
        key: key.as_bytes().to_vec(),
        cols: vec![v.to_vec()],
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }
}

fn get(key: &str) -> Op {
    Op::RowGet {
        table: TBL.as_bytes().to_vec(),
        key: key.as_bytes().to_vec(),
    }
}

/// 一行现在的值（没有就 `None`）。
fn row(c: &mut Client, key: &str) -> Option<Vec<u8>> {
    match ok(c, &get(key)) {
        Reply::Row(Some(rec)) => Some(rec.cols[0].clone()),
        Reply::Row(None) => None,
        other => panic!("expected Row, got {other:?}"),
    }
}

/// 一段可辨识、不可与随机数混淆的载荷。
fn payload(seed: u8, n: usize) -> Vec<u8> {
    (0..n).map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed)).collect()
}

fn has(hay: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && hay.windows(needle.len()).any(|w| w == needle)
}

/// 只跑 CHAP、不协商记录层的客户端（A/B 的明文那一侧）。
///
/// 它是 `Client::from_stream_auth_plain` 存在的理由：「加密开着与没开」必须在
/// 同一份代码上比，而不是拿两个不同的客户端去比。
fn plain_client(srv: &Server) -> Client {
    let sock = TcpStream::connect(&srv.addr).expect("connect");
    // 4 MiB 的应答在调试构建里能慢到十秒级，但不能没有上限：一个卡死的用例
    // 应该失败，而不是一直占着整个测试套。
    let _ = sock.set_read_timeout(Some(Duration::from_secs(60)));
    let _ = sock.set_write_timeout(Some(Duration::from_secs(60)));
    Client::from_stream_auth_plain(sock, srv.auth_key()).expect("plain handshake")
}

/// 从 seq 1 起把主当前的 WAL 全部拉回来（拼接后的原始记录字节）。
fn wal_batch(c: &mut Client) -> Vec<u8> {
    let mut out = Vec::new();
    let mut want = 1u64;
    let mut hint = WAL_HINT_NONE;
    // 上限只是防「主永远不交空批」把用例钉死；正常一轮就到尾。
    for _ in 0..64 {
        match ok(c, &Op::WalFetch { want_seq: want, hint_off: hint, max_bytes: WAL_FETCH_MAX }) {
            Reply::WalBatch { records, base_seq, next_off, durable_seq, gap, .. } => {
                assert!(!gap, "从 seq 1 拉一份还没 checkpoint 过的 WAL，不该断档");
                if records.is_empty() {
                    break;
                }
                for r in &records {
                    out.extend_from_slice(r);
                }
                want = base_seq + records.len() as u64;
                hint = next_off;
                if want > durable_seq {
                    break;
                }
            }
            other => panic!("WalFetch 应回 WalBatch，得到 {other:?}"),
        }
    }
    out
}

/// 整份快照的字节。
fn snap_all(c: &mut Client) -> Vec<u8> {
    let mut out = Vec::new();
    let mut off = 0u64;
    loop {
        match ok(c, &Op::SnapFetch { offset: off, len: SNAP_CHUNK_MAX }) {
            Reply::SnapChunk { bytes, total, .. } => {
                if bytes.is_empty() {
                    assert_eq!(off, total, "空块只能出现在末尾：已到 {off}, total {total}");
                    break;
                }
                out.extend_from_slice(&bytes);
                off += bytes.len() as u64;
                if off >= total {
                    break;
                }
            }
            other => panic!("SnapFetch 应回 SnapChunk，得到 {other:?}"),
        }
    }
    out
}

// ---------------------------------------------------------------- 取证代理

/// 代理要对哪一边动手。
#[derive(Clone, Copy)]
enum Tamper {
    /// 只转发与取证。
    None,
    /// c2s：切点之后第一条记录的第二个密文字节翻一位。
    C2sFirstRecordByte,
    /// s2c：把 `SecureOk` 的 suite 改成 0（= 服务端明确拒绝加密）。
    S2cDowngradeSuite,
}

/// 一个方向上的动作：取证 + 是否动手 + 动哪个字节。
#[derive(Clone, Copy)]
enum Mode {
    Observe,
    TamperC2s,
    TamperS2cDowngrade,
}

impl Mode {
    fn acts(&self) -> bool {
        !matches!(self, Mode::Observe)
    }

    /// 在已累积的字节里找动手目标（绝对下标）。`None` = 还没收全，下批再找。
    ///
    /// 找法是按**明文帧结构**走，不是搜字节序列：`93 01 01` 这种三字节模式
    /// 完全可能出现在 `Challenge` 的 32 字节随机数里，一旦撞上就是往随机数上
    /// 动手，然后整条用例空跑还以为成功了。
    fn target(&self, acc: &[u8]) -> Option<usize> {
        match self {
            Mode::Observe => None,
            // 切点 = `SecureBegin` 帧的下一字节 = 第一条记录长度头的起点。
            Mode::TamperC2s => first_frame(acc, OP_SECURE_BEGIN).map(|(_, end)| end + CT_BYTE),
            // 应答负载 = `tag:u16 ‖ suite:u8`，suite 在起点 + 2。
            Mode::TamperS2cDowngrade => first_frame(acc, REPLY_SECURE_OK).map(|(start, _)| start + 2),
        }
    }

    fn apply(&self, byte: u8) -> u8 {
        match self {
            Mode::TamperC2s => byte ^ 0x01,
            Mode::TamperS2cDowngrade => 0,
            Mode::Observe => byte,
        }
    }
}

/// 从流的开头按 `len:u32 ‖ payload(tag:u16 …)` 走明文帧，返回第一个负载以
/// `tag` 开头的帧的 `(负载起点, 帧尾之后)`。没收全就 `None`（下批再试）。
fn first_frame(buf: &[u8], tag: u16) -> Option<(usize, usize)> {
    let mut at = 0usize;
    loop {
        if at + HDR > buf.len() {
            return None;
        }
        let len = u32::from_le_bytes(buf[at..at + HDR].try_into().unwrap()) as usize;
        // 任何 op/reply 至少带一个 tag；带不下的东西不是帧，别再去走它。
        if len < 2 {
            return None;
        }
        let end = at + HDR + len;
        if end > buf.len() {
            return None;
        }
        if u16::from_le_bytes([buf[at + HDR], buf[at + HDR + 1]]) == tag {
            return Some((at + HDR, end));
        }
        at = end;
    }
}

/// 一条夹在客户端与服务端中间的 TCP 代理。
///
/// 它存在的理由只有一个：C 承诺的是「**线上**的字节是密文」，那句话只能在两
/// 条真实连接中间取证，读不到 socket 内部就等于没证。
struct Proxy {
    addr: String,
    c2s: Arc<Mutex<Vec<u8>>>,
    s2c: Arc<Mutex<Vec<u8>>>,
    hit: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Proxy {
    fn start(upstream: &str, tamper: Tamper) -> Proxy {
        let ls = TcpListener::bind("127.0.0.1:0").expect("proxy bind");
        let addr = ls.local_addr().unwrap().to_string();
        let (c2s, s2c, hit, stop) = (
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let up = upstream.to_string();
        let (a1, a2, h1, h2, st) = (
            Arc::clone(&c2s),
            Arc::clone(&s2c),
            Arc::clone(&hit),
            Arc::clone(&hit),
            Arc::clone(&stop),
        );
        let thread = std::thread::spawn(move || {
            // 一条连接两个泵线程；accept 循环直到 `stop`（`Drop` 会连一次把它叫醒）。
            for inc in ls.incoming() {
                if st.load(Ordering::Relaxed) {
                    break;
                }
                let Ok(client) = inc else { break };
                let Ok(server) = TcpStream::connect(&up) else { continue };
                let (c_out, s_out) = match (client.try_clone(), server.try_clone()) {
                    (Ok(a), Ok(b)) => (a, b),
                    _ => continue,
                };
                let (m_c2s, m_s2c) = match tamper {
                    Tamper::None => (Mode::Observe, Mode::Observe),
                    Tamper::C2sFirstRecordByte => (Mode::TamperC2s, Mode::Observe),
                    Tamper::S2cDowngradeSuite => (Mode::Observe, Mode::TamperS2cDowngrade),
                };
                // 先就地克隆再 `move` 进去：直接写 `Arc::clone(&a1)` 会把 `a1` 本身
                // 搬进闭包，下一轮 accept 就没有了。
                let (cap_c, hit_c) = (Arc::clone(&a1), Arc::clone(&h1));
                let (cap_s, hit_s) = (Arc::clone(&a2), Arc::clone(&h2));
                std::thread::spawn(move || pump(client, server, cap_c, hit_c, m_c2s));
                // 回程反接：src 必须是服务端的 socket。两个泵同向接的话，请求
                // 能过去、应答回不来，表象是客户端超时而不是「代理写错了」。
                std::thread::spawn(move || pump(s_out, c_out, cap_s, hit_s, m_s2c));
            }
        });
        Proxy {
            addr,
            c2s,
            s2c,
            hit,
            stop,
            thread: Some(thread),
        }
    }

    /// 已经流过的字节（`(客户端→服务端, 服务端→客户端)`）。
    fn captured(&self) -> (Vec<u8>, Vec<u8>) {
        (
            self.c2s.lock().unwrap().clone(),
            self.s2c.lock().unwrap().clone(),
        )
    }

    /// 代理是否真的动过手。断连类用例必须先确认这一条，否则「没出错」可能只是
    /// 「没篡改」。
    fn hit(&self) -> bool {
        self.hit.load(Ordering::SeqCst)
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // 叫醒可能还堵在 `accept` 的那个线程。
        let _ = TcpStream::connect(&self.addr);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

fn pump(mut src: TcpStream, mut dst: TcpStream, cap: Arc<Mutex<Vec<u8>>>, hit: Arc<AtomicBool>, mode: Mode) {
    let mut acc: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 16 * 1024];
    let mut done = !mode.acts();
    loop {
        let n = match src.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let mut chunk: Vec<u8> = buf[..n].to_vec();
        if !done {
            acc.extend_from_slice(&chunk);
            if let Some(t) = mode.target(&acc) {
                let base = acc.len() - chunk.len();
                // 目标必须落在**这一批**里才改得动：帧可能正被 TCP 切开，那时
                // 先原样转发，等凑齐那批再改（改晚了就漏了切点）。
                if t >= base && t < acc.len() {
                    chunk[t - base] = mode.apply(chunk[t - base]);
                    hit.store(true, Ordering::SeqCst);
                    done = true;
                }
            }
        }
        // 取证记的是转发出去的字节：那才是对面真正收到的东西。
        if dst.write_all(&chunk).is_err() {
            break;
        }
        let _ = dst.flush();
        cap.lock().unwrap().extend_from_slice(&chunk);
    }
    let _ = dst.shutdown(std::net::Shutdown::Both);
}

// ---------------------------------------------------------------- 用例

/// 1：加密连接上的大值与流水线在两种 worker 形状上都逐字节回来。
///
/// 两条循环各自封帧（`workers=1` 走 server.rs 的主线程，`workers>1` 的写在
/// 提交线程、封帧仍在主线程 `pump`），所以切点与计数器各有一套接线要验。
#[test]
fn an_encrypted_connection_round_trips_big_values_on_both_worker_shapes() {
    for workers in [1usize, 8] {
        let srv = Server::start_auth("group", workers);
        let mut c = srv.client();
        assert!(
            c.is_secure(),
            "workers={workers}: 带密钥的建连必须已经切到记录层"
        );
        ok(&mut c, &table(TBL));

        // 一条 200 KB 的值：超过 `MAX_PLAINTEXT`（64 KiB），回程必然是多个记录，
        // 而帧边界与记录边界不必对齐 —— 收侧要能把它们重新拼成原帧。
        let big = payload(0x31, 200_000);
        ok(&mut c, &put("big", &big));
        assert_eq!(
            row(&mut c, "big"),
            Some(big.clone()),
            "workers={workers}: 跨记录的大值没原样回来"
        );

        // 流水线：一批发过去、按序收回来，大小值混排（批里既有单记录帧也有多记录帧）。
        let sizes = [40usize, 100_000, 1, 65_536, 65_537, 16];
        let ops: Vec<Op> = sizes
            .iter()
            .enumerate()
            .map(|(i, n)| put(&format!("p{i}"), &payload(i as u8, *n)))
            .collect();
        let reps = c.pipeline(&ops).expect("pipeline");
        assert_eq!(reps.len(), ops.len(), "workers={workers}: 批应答数不对");
        assert!(
            reps.iter().all(|r| !matches!(r, Reply::Err { .. })),
            "workers={workers}: 批里有失败应答 {reps:?}"
        );
        for (i, n) in sizes.iter().enumerate() {
            assert_eq!(
                row(&mut c, &format!("p{i}")),
                Some(payload(i as u8, *n)),
                "workers={workers} 批内第 {i} 条（{n} 字节）回读不符"
            );
        }

        assert!(
            c.secure_records_sent() >= 2,
            "workers={workers}: 记录计数说明它其实没在加密跑：{}",
            c.secure_records_sent()
        );
    }
}

/// 1b：副本线那两条只读 op（`WalFetch`/`SnapFetch`）过记录层之后逐字节不变。
///
/// 一个 `WalBatch`/`SnapChunk` 帧能到 4 MiB，也就是 64 个记录 —— 全协议里没有
/// 比它更吃「记录边界与帧边界不必对齐」这一条的形状：拼接错一个字节，副本
/// 就会把 crc 当坏记录丢掉。
///
/// 基准选「同一条服务端上的明文连接」而不是内存里的期望值：两条线唯一的差别
/// 就是有没有加密，那所有差异都只能来自记录层。
#[test]
fn wal_and_snapshot_fetches_come_back_byte_identical_through_the_record_layer() {
    let srv = Server::start_auth("group", 4);
    let mut seed = srv.client();
    assert!(seed.is_secure(), "播种的连接自己就得是密文");
    ok(&mut seed, &table("blobs"));
    let val = vec![b'q'; 4096];
    for i in 0..300 {
        ok(
            &mut seed,
            &Op::RowPut {
                table: b"blobs".to_vec(),
                key: format!("k{i:04}").into_bytes(),
                cols: vec![val.clone()],
                flags: 0,
                cond_col: COND_NONE,
                cond_val: Vec::new(),
            },
        );
    }

    let wal_sec = wal_batch(&mut srv.client());
    let wal_plain = wal_batch(&mut plain_client(&srv));
    assert!(
        wal_sec.len() > 64 * 1024,
        "WAL 批要跨过多个记录才算数：{} B",
        wal_sec.len()
    );
    assert_eq!(wal_sec, wal_plain, "同一段 WAL，加密线与明文线拿到的必须完全一样");

    // 快照得先落盘才拉得到：`SnapFetch` 读的是 `state.snap`。
    ok(&mut seed, &Op::Checkpoint);
    drop(seed);
    let snap_sec = snap_all(&mut srv.client());
    let snap_plain = snap_all(&mut plain_client(&srv));
    assert!(
        snap_sec.len() > 64 * 1024,
        "快照要跨过多个记录才算数：{} B",
        snap_sec.len()
    );
    assert_eq!(snap_sec, snap_plain, "整份快照经记录层传输后不能差一个字节");
}

/// 2：切点之后线上取不到 canary，而同一条代理在明文客户端下取得到。
///
/// 后半条不是锦上添花：没有它，「看不见」与「没采到」长得一模一样。
#[test]
fn the_wire_hides_payloads_after_the_cut_but_shows_them_before_it() {
    let srv = Server::start_auth("group", 1);
    let key = srv.auth_key().expect("auth server has a key").clone();
    // 表用一条正常连接先建好：代理那条连接要留给取证。
    let mut setup = srv.client();
    ok(&mut setup, &table(TBL));
    drop(setup);

    let canary: &[u8] = b"CANARY-8f31b0c2-plaintext-probe";

    // A：加密客户端。
    let p = Proxy::start(&srv.addr, Tamper::None);
    {
        let mut c = Client::connect_auth(&p.addr, 5000, Some(&key)).expect("secure connect via proxy");
        ok(&mut c, &put("hidden", canary));
        assert_eq!(row(&mut c, "hidden").as_deref(), Some(canary), "往返要先对，否则「看不见」可能只是「没写进去」");
    }
    let (c2s, s2c) = p.captured();
    let cut = first_frame(&c2s, OP_SECURE_BEGIN).expect("the proxy must see the plaintext SecureBegin frame").1;
    let s2c_end = first_frame(&s2c, REPLY_SECURE_OK).expect("and the plaintext SecureOk reply").1;
    assert!(
        c2s.len() > cut + 20,
        "切点之后必须确实有流量：cut {cut}, total {}",
        c2s.len()
    );
    assert!(
        s2c.len() > s2c_end + 20,
        "回程也一样：s2c 切点 {s2c_end}, total {}",
        s2c.len()
    );
    assert!(
        !has(&c2s, canary) && !has(&s2c, canary),
        "canary 出现在明文中：c2s {} B, s2c {} B",
        c2s.len(),
        s2c.len()
    );
    drop(p);

    // B：只握手不加密的客户端 —— 同一条代理必须看得见它。
    let p = Proxy::start(&srv.addr, Tamper::None);
    {
        let sock = TcpStream::connect(&p.addr).expect("connect proxy");
        let mut c = Client::from_stream_auth_plain(sock, Some(&key)).expect("plain handshake");
        assert!(!c.is_secure(), "对照组必须是明文连接");
        ok(&mut c, &put("visible", canary));
    }
    let (c2s, _) = p.captured();
    assert!(
        has(&c2s, canary),
        "明文客户端下同一条代理看不见 canary（{} B），说明取证本身是坏的 —— A 的断言不成立",
        c2s.len()
    );
    drop(p);
}

/// 3：线上翻一个 bit ⇒ 断连，且那条写没有落库。
///
/// 「拿到 Err」与「拿到一个错的应答」是这件事的两面，缺一面都不算 AEAD：
/// 只断连但值写坏了是完整性失效，值没写坏却返回了成功是可用性失效。
#[test]
fn a_tampered_record_closes_the_connection_and_the_write_never_lands() {
    let srv = Server::start_auth("group", 1);
    let key = srv.auth_key().expect("auth server has a key").clone();
    let mut setup = srv.client();
    ok(&mut setup, &table(TBL));
    drop(setup);

    let p = Proxy::start(&srv.addr, Tamper::C2sFirstRecordByte);
    let mut c = Client::connect_auth(&p.addr, 5000, Some(&key)).expect("secure connect via proxy");
    let r = c.call(&put("tampered", b"value-that-must-not-land"));
    assert!(
        r.is_err(),
        "被改过的记录只能以断连收场，拿到任何应答都说明服务端把错字节当成了合法帧：{r:?}"
    );
    assert!(
        c.call(&get("tampered")).is_err(),
        "验不过之后同一条连接不该还能问出东西"
    );
    drop(c);
    assert!(p.hit(), "代理没找到切点，这条用例是空跑");
    drop(p);

    let mut v = srv.client();
    assert_eq!(
        row(&mut v, "tampered"),
        None,
        "半个被篡改的记录绝不能变成一行"
    );
    // 服务端本身没坏：别的连接照常。
    assert!(matches!(ok(&mut v, &Op::Ping), Reply::Pong));
}

/// 4：`--require-secure` 拦「已认证但明文」，并且文案指得准。
#[test]
fn require_secure_refuses_an_authenticated_plaintext_connection() {
    for workers in [1usize, 4] {
        let srv = Server::start_auth_extra("group", workers, &["--require-secure"]);
        let key: &Key = srv.auth_key().expect("auth server has a key");

        let sock = TcpStream::connect(&srv.addr).expect("connect");
        let mut plain = Client::from_stream_auth_plain(sock, Some(key)).expect("CHAP alone must still succeed");
        match plain.call(&Op::Ping).expect("a refused data op still answers") {
            Reply::Err { code, msg } => {
                let m = String::from_utf8_lossy(&msg);
                assert_eq!(code, errno::EADV, "workers={workers}");
                assert!(
                    m.contains("SecureBegin"),
                    "workers={workers}: 这条连接缺的是记录层那一拍，文案不能指向别处：{m}"
                );
                assert!(m.contains("§4.5"), "拒绝文案要指到文档那一节：{m}");
            }
            other => panic!("workers={workers}: require-secure 却答了 {other:?}"),
        }

        // 连版本都没协商的连接仍然报「VersionBegin」—— 三档文案必须互相区分得开。
        let mut raw = Client::connect_raw(&srv.addr, 3000).expect("connect raw");
        match raw.call(&Op::Ping).expect("raw call") {
            Reply::Err { msg, .. } => assert!(
                String::from_utf8_lossy(&msg).contains("VersionBegin"),
                "workers={workers}: 未协商版本却被当成「缺加密」：{}",
                String::from_utf8_lossy(&msg)
            ),
            other => panic!("workers={workers}: 裸连接不该有 {other:?}"),
        }

        // 而正常客户端（自动带 SecureBegin）在这台服务端上完全照常。
        let mut c = srv.client();
        assert!(c.is_secure(), "workers={workers}");
        ok(&mut c, &table(TBL));
        let big = payload(0x77, 150_000);
        ok(&mut c, &put("r", &big));
        assert_eq!(row(&mut c, "r"), Some(big), "workers={workers}");
    }
}

/// 4b：`--require-secure` 没有密钥就是配置错误，必须在参数阶段 die。
#[test]
fn require_secure_without_a_key_is_refused_at_startup() {
    // 走 `start_extra`（auth = None）：期望进程直接退出，`wait_ready` 会带着
    // 日志一起 panic。这里手工起，是为了断言那句文案而不是断言 panic。
    let tmp = tempfile::tempdir().expect("tempdir");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_treedb-server"))
        .args([
            "--dir",
            tmp.path().join("data").to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
            "--require-secure",
        ])
        .output()
        .expect("spawn treedb-server");
    assert!(!out.status.success(), "没有密钥也起得来的话，服务端会永远拒读者");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("--auth-key-file"),
        "报错要指出缺的是密钥：{err}"
    );
}

/// 5：没开 `--require-secure` 的 v9 服务端仍服务明文客户端（升级窗口）。
#[test]
fn a_default_server_still_serves_a_plaintext_client() {
    for workers in [1usize, 4] {
        let srv = Server::start_auth("group", workers);
        let key: &Key = srv.auth_key().expect("auth server has a key");
        let sock = TcpStream::connect(&srv.addr).expect("connect");
        let mut plain = Client::from_stream_auth_plain(sock, Some(key)).expect("plain handshake");
        ok(&mut plain, &table(TBL));
        ok(&mut plain, &put("p", b"plaintext is still fine here"));
        assert_eq!(
            row(&mut plain, "p").as_deref(),
            Some(&b"plaintext is still fine here"[..]),
            "workers={workers}: 默认配置下明文数据面必须照常"
        );

        // 同机同时服务两种客户端：这条是「可选后续阶段」的实际含义 —— 升级期间
        // 新旧客户端混跑，而不是必须一次换完。
        let mut sec = srv.client();
        assert!(sec.is_secure(), "workers={workers}");
        assert_eq!(
            row(&mut sec, "p").as_deref(),
            Some(&b"plaintext is still fine here"[..]),
            "workers={workers}: 明文客户端写的，加密客户端要能读到"
        );
    }
}

/// 6：`SecureOk.suite` 被改成 0 ⇒ 客户端拒绝降级，而不是静默走明文。
#[test]
fn a_downgraded_secureok_is_not_silently_accepted() {
    let srv = Server::start_auth("group", 1);
    let key = srv.auth_key().expect("auth server has a key").clone();

    let p = Proxy::start(&srv.addr, Tamper::S2cDowngradeSuite);
    let e = match Client::connect_auth(&p.addr, 5000, Some(&key)) {
        Ok(_) => panic!("服务端明确回了 suite:0，客户端不能当没事继续"),
        Err(e) => e,
    };
    let text = e.to_string();
    assert!(
        text.contains("suite") || text.contains("plaintext"),
        "报错要点明这是降级被拒：{text}"
    );
    assert!(p.hit(), "代理没改到 suite 字节，这条用例是空跑");
    drop(p);

    // 一个拒绝降级的客户端没把服务端弄坏：正常连接照常。
    let mut c = srv.client();
    assert!(matches!(ok(&mut c, &Op::Ping), Reply::Pong));
}

/// 记录层的每字节代价（手动跑，release）：
///
/// ```text
/// cargo test --release --test record -- --ignored --nocapture
/// ```
///
/// 为什么不只靠 `benches/secure_cost.sh`：脚本那个口径里混着 TCP、事件循环、
/// group commit，加密只占其中一小块，两臂差值一旦落进轮间抖动就什么都结论不了。
/// 这里把 `seal`/`feed` 单独拎出来，与同尺寸的纯拷贝放在同一轮里比 —— 量的是
/// 「多加一层记录层要占多少内存带宽」。那个倍数才是「代价可接受」这句话的依据：
/// 记录层是纯字节搬运的活，只要它比 memcpy 贵出的倍数远小于「加密线要省下的
/// 那条安全论证」，结论就成立，而端到端脚本给不出这个分辨率。
///
/// 形状按真实接收侧来：一次 `feed` 喂进去的是 socket 一批读到的字节（这里取
/// 128 KiB），不是整条流。把 64 MiB 一次喂进去会让内部缓冲自己分配一遍，那笔钱
/// 生产环境里不存在。
#[test]
#[ignore = "release 测量用例，手动跑：cargo test --release --test record -- --ignored --nocapture"]
fn record_layer_cost_per_byte() {
    use std::hint::black_box;
    use std::time::Instant;
    use treedb::net::record::Session;

    const TOTAL: usize = 64 << 20; // 64 MiB 一批：再小计时噪声就盖过差异
    const READ_BATCH: usize = 128 << 10; // 一次 feed 的字节数（≈ 一次 socket 读）
    let key = common::test_key();
    let (cn, sn) = ([0x11u8; 32], [0x22u8; 32]);

    println!(
        "=== 记录层每字节代价（release，{:>3} MiB/档，机器核数 {}） ===",
        TOTAL >> 20,
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0)
    );
    for &chunk in &[4096usize, 16 * 1024, 64 * 1024] {
        let rounds = TOTAL / chunk;
        let pt = payload(3, chunk);
        let mib = |d: Instant| (TOTAL as f64 / (1 << 20) as f64) / d.elapsed().as_secs_f64();

        // 基准：同尺寸纯拷贝。它是这一档的地板 —— 记录层不可能比它更快，
        // 报「加密只比拷贝慢 1.4×」而不报拷贝，读者无从判断绝对值好不好。
        let mut dst = vec![0u8; chunk];
        let t = Instant::now();
        for _ in 0..rounds {
            dst.copy_from_slice(black_box(&pt));
            black_box(&dst);
        }
        let copy = mib(t);

        // 单向封：一条记录一次 `seal`，与发送侧每帧一次调用同形。
        let mut enc = Session::for_server(&key, &cn, &sn);
        let mut rec = Vec::with_capacity(chunk + 32);
        let t = Instant::now();
        for _ in 0..rounds {
            rec.clear();
            assert!(enc.seal(black_box(&pt), &mut rec), "计数器不该在 {} 轮内耗尽", rounds);
            black_box(&rec);
        }
        let seal = mib(t);

        // 封 + 解：一条连接本来就要同时跑两个方向，这个数才是「线上多付的钱」。
        let mut enc = Session::for_server(&key, &cn, &sn);
        let mut dec = Session::for_client(&key, &cn, &sn);
        let mut batch: Vec<u8> = Vec::with_capacity(READ_BATCH);
        let mut plain = Vec::with_capacity(chunk + 32);
        let t = Instant::now();
        for _ in 0..rounds {
            assert!(enc.seal(black_box(&pt), &mut batch));
            // 攒够一批再喂：`feed` 的调用频率 ≈ socket 读的频率，不是记录的频率。
            if batch.len() >= READ_BATCH {
                plain.clear();
                dec.feed(&batch, &mut plain).expect("本实现自己封的必须验得过");
                // 一批里至少得回来一条记录的明文，否则这个循环在空跑，
                // 而下面的 MiB/s 会好看得毫无意义。
                assert!(plain.len() >= chunk, "喂了 {} B 却没解出东西", batch.len());
                black_box(&plain);
                batch.clear();
            }
        }
        if !batch.is_empty() {
            plain.clear();
            dec.feed(&batch, &mut plain).expect("尾批同样要验得过");
        }
        let both = mib(t);

        // 测完必须还正确一次：计时循环跑偏成「什么都没做」的话，上面的 MiB/s
        // 会好看得很，而这份表就全是假的。整条 round-trip 逐字节复核。
        let mut e2 = Session::for_server(&key, &cn, &sn);
        let mut d2 = Session::for_client(&key, &cn, &sn);
        let mut w = Vec::new();
        assert!(e2.seal(&pt, &mut w));
        let mut back = Vec::new();
        d2.feed(&w, &mut back).expect("spot check must verify");
        assert_eq!(back, pt, "{chunk} B 档的 round-trip 不再是原字节");

        println!(
            "  {:>6} B/记录  拷贝 {:>7.0} MiB/s   封 {:>7.0} MiB/s（{:.2}× 拷贝）   封+解 {:>7.0} MiB/s（{:.2}×）",
            chunk,
            copy,
            seal,
            copy / seal,
            both,
            copy / both,
        );
    }
}
