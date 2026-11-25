//! 多前端 worker + 唯一提交线程：`workers > 1` 时接管事件循环。
//!
//! ## 要解决的到底是什么
//!
//! 单线程循环在写上是对的（一次 commit 就把一条核跑满，group commit 还能把
//! N 次 fsync 摊成一次），但读不对。而读不对的原因还不是 CPU —— 实测 64 连接
//! `lookup`：
//!
//! - pipeline=32（真一次往返 32 个请求）：**70.7 万 ops/s**，单线程。
//! - pipeline=1（一问一答）：**9.6 万 ops/s**，同一份 CPU 活儿。
//!
//! 差 7 倍，而两次之间唯一的区别是**每连接一次 read + 一次 write 系统调用被
//! 串行化在同一条线程上**。先试过把 `translate` 搬到只读工作池：大回包
//! （readdir）确实快 3.8×，小 op 反而慢 3–7×，因为省下的计算抵不过多出来的
//! 一次线程间递交，而 syscall 仍然全压在主线程。所以要补的是**前端**：让每条
//! 线程自己收自己的包。
//!
//! ## 切法
//!
//! ```text
//!  worker 0 ─┐ 自己的 SO_REUSEPORT 监听 + 自己的 epoll + 属于自己的连接
//!  worker 1 ─┤   纯读批次：就地 translate + 编码 + 发出（读锁，N 核并行）
//!  worker N ─┘   含写批次：整批 Forwarded ──► committer（唯一持有
//!                                              Engine/WAL/stats 的线程）
//!     ◄── Answer（成品帧，按连接顺序）──── 一轮一次 commit_batch = 一次 fsync
//! ```
//!
//! - **写仍然只有一个定序者**，就是 committer。worker 碰不到 WAL、碰不到
//!   stats、碰不到 `apply`，所以「全局定序」这条不变量与单线程时一字不差。
//! - 只读 op 就地执行，于是读并行度和 syscall 并行度是同一个 N，不需要再单独
//!   搞计算池。
//! - group commit 不丢：committer 每轮把**所有**信箱一次排干再收口。
//! - `workers = 1` 时根本不走这个文件，走 [`crate::net::server::serve_single`]。但两档
//!   **共用** `Conn`/`Slot`/帧编解码/`apply` 这层构件，所以单线程档的代码并非从未变过：
//!   `Conn::rearm`、`Conn::push_frame`、读循环短读就 `break` 这几项两边都吃得到。拿
//!   `workers=1` 的历史基线格子做差之前，先同格重跑一次 A/B。
//!
//! ## 同连接流内顺序（这个文件里最容易写错的东西）
//!
//! 协议没有 request id，所以每连接的应答必须严格按请求顺序。规则：一条连接只要
//! 还有 `Slot::Await`（已转发、committer 未回话），它后续**任何** op 都必须继续
//! 转发，绝不允许就地算。否则就地算出来的读会插到前面那条写的答复之前 —— 客户
//! 端看到的就是「我前一次写还没确认，后一次读已经看不到它」。反过来，一条干净
//! 连接上就地算的读也不可能看到更晚的写：那个写根本还没被转发出去。
//!
//! 应答路由用**从不复用的连接号**而不是 fd：fd 会在关连接后被内核立刻重新分配，
//! 可能分到另一条 worker 线程，而提交线程手里那条批还在排队 —— 按 fd 回填会把
//! 上一任的写答复塞进新任的队头。
//!
//! ## 成本
//!
//! 每多一条 worker 多 3 个 fd（1 张监听套接字 + 上/下行各 1 个 eventfd），与
//! 连接数无关；`workers = 1` 时一个都不多，所以 `fd/conn = 1.0000` 那条结构性
//! 主张在默认档原样成立。

use crate::engine::codec::Rd;
use crate::engine::mem::{Log, State};
use crate::engine::model::errno;
use crate::engine::{reply_frame, Engine, Op, Reply};
use crate::error::{Error, Result};
use crate::net::mail::Mail;
use crate::net::replicate::Follower;
use crate::net::server::{
    close, epoll_create, epoll_set, flush_dirty, last_os, open_engine, tune_conn, Conn, Report,
    ServerConfig, Slot, Staged, MAX_BACKLOG, MAX_EVENTS, READ_CHUNK,
};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{ErrorKind, Read};
use std::net::{SocketAddr, TcpListener};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// `Answer.conn` 取这个值时表示「committer 收工了，你也可以退了」。
const BYE: u64 = u64::MAX;

/// worker → committer：一条连接上按序到达的一批 op。整批移交，不拆开。
struct Forwarded {
    conn: u64,
    ops: Vec<Op>,
}

/// committer → worker：上面那批 op 的成品应答帧，顺序与 `ops` 严格一致。
struct Answer {
    conn: u64,
    frames: Vec<Vec<u8>>,
}

/// 跨线程共享的计数。只有这几个数需要原子；状态本体仍然只有一把 `RwLock`。
#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    peak: AtomicU64,
    /// 全局同时在册连接数（所有 worker 之和），bin 的健康探测读它。
    live: AtomicUsize,
    /// 在 worker 线程就地算完的只读 op 数。
    inline_reads: AtomicU64,
    /// 整批交给 committer 的 op 数（写 + 排在写之后的读）。
    forwarded: AtomicU64,
    /// 因 `max_conn` 被接受后立刻关掉的连接数（所有 worker 之和）。
    refused: AtomicU64,
}

/// 一条 worker 线程启动时需要的东西。
///
/// 监听套接字不在这里：主线程已经 bind 好、随参数交给线程（`guarded_worker`）。
/// 历史上这里还挂过 `listen`/`backlog` 两个字段，自从 bind 上移之后就没人读过，
/// 已清掉。
///
/// 以前它是 `Copy`（几个整数）；加了 `auth` 之后只能降到 `Clone` —— 一个
/// `Arc` 多一次计数，在线程启动时付一次，不在数据面上。
#[derive(Clone)]
struct WorkerCfg {
    idx: usize,
    sync_ms: i32,
    /// 同时在册连接数上限，`0` = 不限。每一条 worker 都要它：accept 是各自
    /// 干的，上限必须拿全局原子计数判，不能按线程算。
    max_conn: usize,
    /// 接入认证密钥（§4.4）。`None` = 不认证，与 `ServerConfig::auth` 同语义。
    auth: Option<Arc<crate::net::auth::Key>>,
}

/// 起 `cfg.workers` 条前端线程，当前线程当 committer。
pub(crate) fn serve(
    cfg: &ServerConfig,
    stop: &AtomicBool,
    live_out: &AtomicUsize,
) -> Result<Report> {
    let n = cfg.workers.max(2);
    let mut engine = open_engine(cfg)?;
    let state = engine.shared_state();
    let ops_ctr = engine.attach_external_ops();
    let flag = Arc::new(AtomicBool::new(false));
    let cnt = Arc::new(Counters::default());
    let errs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    // 死信信箱：worker 出错时要能把阻塞在 epoll 里的 committer 叫醒，
    // 否则"少了一条前端线程"这件事会一直没人知道。
    let dead: Arc<Mail<()>> = Mail::new();

    let mut ins: Vec<Arc<Mail<Forwarded>>> = Vec::with_capacity(n);
    let mut outs: Vec<Arc<Mail<Answer>>> = Vec::with_capacity(n);
    for _ in 0..n {
        ins.push(Mail::new());
        outs.push(Mail::new());
    }
    // 监听套接字全部在这里先 bind 好再分给线程：线程里 bind 会有一小段
    // "端口已就绪但 worker 还没起来"的窗口，而 bind 失败（端口被占）必须
    // 在起线程之前就干净地报错。
    let mut listeners = Vec::with_capacity(n);
    for _ in 0..n {
        listeners.push(reuseport_listener(cfg.listen, cfg.backlog)?);
    }

    let mut handles = Vec::with_capacity(n);
    let mut spawn_err: Option<Error> = None;
    for (i, l) in listeners.into_iter().enumerate() {
        let arg = WorkerCfg {
            idx: i,
            sync_ms: cfg.sync_ms,
            max_conn: cfg.max_conn,
            auth: cfg.auth.clone(),
        };
        let (st, oc, fl, ct, er, mi, mo, dd) = (
            state.clone(),
            ops_ctr.clone(),
            flag.clone(),
            cnt.clone(),
            errs.clone(),
            ins[i].clone(),
            outs[i].clone(),
            dead.clone(),
        );
        let built = std::thread::Builder::new()
            .name(format!("treedb-worker-{i}"))
            .spawn(move || guarded_worker(arg, l, st, oc, fl, ct, er, mi, mo, dd));
        match built {
            Ok(h) => handles.push(h),
            Err(e) => {
                spawn_err = Some(Error::Io(format!("spawn worker {i}: {e}")));
                break;
            }
        }
    }
    if let Some(e) = spawn_err {
        // 起不齐就不凑合跑：先把已经起来的叫停回收，再把错误吐给 bin。
        flag.store(true, Ordering::Relaxed);
        for m in outs.iter().take(handles.len()) {
            m.send(Answer {
                conn: BYE,
                frames: vec![],
            });
        }
        for h in handles.drain(..) {
            let _ = h.join();
        }
        return Err(e);
    }

    // committer 不需要看任何连接 fd，它只看 N 个上行 eventfd + 死信。
    let res = committer(cfg, &mut engine, &ins, &outs, &dead, stop, &flag, &cnt, live_out);

    // 无论 committer 是正常收工还是报错，都得把 worker 叫回来：不能让它们攥着
    // 连接 fd 空转，更不能让 join 挂在一条已经退出的线程上等一个永远不会来的
    // 应答。先置位再发 BYE，顺序反了也不影响正确性（两边都会各自复检）。
    flag.store(true, Ordering::Relaxed);
    for m in &outs {
        m.send(Answer {
            conn: BYE,
            frames: vec![],
        });
    }
    for h in handles.drain(..) {
        let _ = h.join();
    }
    live_out.store(0, Ordering::Relaxed);

    let mut rep = res?;
    // worker 的错误必须让 bin 以非零码退出：少一条前端线程 = 一部分连接从此
    // 没人收包，那比整个进程死掉更难诊断。
    let first = {
        let mut g = errs.lock().unwrap_or_else(|p| p.into_inner());
        std::mem::take(&mut *g).into_iter().next()
    };
    if let Some(msg) = first {
        return Err(Error::Io(format!("frontend worker: {msg}")));
    }
    rep.workers = n;
    rep.reads_inline = cnt.inline_reads.load(Ordering::Relaxed);
    rep.forwarded = cnt.forwarded.load(Ordering::Relaxed);
    rep.accepted = cnt.accepted.load(Ordering::Relaxed);
    rep.refused = cnt.refused.load(Ordering::Relaxed);
    rep.peak_conns = cnt.peak.load(Ordering::Relaxed) as usize;
    Ok(rep)
}

/// worker 线程的入口：任何 panic 和 Err 都得登记 + 叫醒 committer。
///
/// 不兜住的话，一条前端线程静默死掉，它手里的连接从此没人收包，而进程还"看着
/// 像在正常服务"。
fn guarded_worker(
    w: WorkerCfg,
    listener: TcpListener,
    state: Arc<RwLock<State>>,
    ops_ctr: Arc<AtomicU64>,
    flag: Arc<AtomicBool>,
    cnt: Arc<Counters>,
    errs: Arc<Mutex<Vec<String>>>,
    m_in: Arc<Mail<Forwarded>>,
    m_out: Arc<Mail<Answer>>,
    dead: Arc<Mail<()>>,
) {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        worker_loop(&w, listener, &state, &ops_ctr, &flag, &cnt, &m_in, &m_out)
    }));
    let msg = match r {
        Ok(Ok(())) => return,
        Ok(Err(e)) => e.to_string(),
        Err(_) => format!("worker {} panicked", w.idx),
    };
    errs.lock()
        .unwrap_or_else(|p| p.into_inner())
        .push(format!("worker {}: {msg}", w.idx));
    flag.store(true, Ordering::Relaxed);
    dead.send(());
}

/// 一条前端线程的事件循环。
///
/// `w` 按引用传：它现在含一个 `Arc`，搬进 `worker_loop` 会让下面那句
/// 「panic 了要报 worker 序号」拿不到值。
fn worker_loop(
    w: &WorkerCfg,
    listener: TcpListener,
    state: &Arc<RwLock<State>>,
    ops_ctr: &Arc<AtomicU64>,
    flag: &AtomicBool,
    cnt: &Arc<Counters>,
    m_in: &Arc<Mail<Forwarded>>,
    m_out: &Arc<Mail<Answer>>,
) -> Result<()> {
    listener.set_nonblocking(true)?;
    let lfd = listener.as_raw_fd();
    let ep = epoll_create()?;
    epoll_set(ep, lfd, false)?;
    let wfd = m_out.fd();
    epoll_set(ep, wfd, false)?;

    let mut conns: HashMap<i32, Conn> = HashMap::new();
    let mut by_id: HashMap<u64, i32> = HashMap::new();
    // 号段按 worker 编号错开，两条线程永远不会发出同一个连接号。
    let mut next_id: u64 = (w.idx as u64) << 48;
    let mut events: Vec<libc::epoll_event> = vec![unsafe { std::mem::zeroed() }; MAX_EVENTS];
    let mut buf = vec![0u8; READ_CHUNK];
    let mut dirty: Vec<i32> = Vec::new();
    let mut bye = false;

    while !bye && !flag.load(Ordering::Relaxed) {
        // 本线程没有任何周期性活儿要干（fsync 归 committer），超时只用来
        // 复检停位。sync_ms<=0 时不能真的无限阻塞，否则信号停服叫不回它。
        let timeout = if w.sync_ms > 0 { w.sync_ms } else { 200 };
        let n = unsafe { libc::epoll_wait(ep, events.as_mut_ptr(), events.len() as i32, timeout) };
        if n < 0 {
            let e = last_os();
            if e.contains("Interrupted") {
                continue;
            }
            return Err(Error::Io(format!("epoll_wait: {e}")));
        }
        dirty.clear();

        for i in 0..n as usize {
            let fd = events[i].u64 as i32;
            let bits = events[i].events as u32;

            // ---- committer 的答话 ----
            if fd == wfd {
                for a in m_out.drain() {
                    if a.conn == BYE {
                        bye = true;
                        continue;
                    }
                    if let Some(f) = fill_answer(&mut conns, &by_id, a) {
                        dirty.push(f);
                    }
                }
                continue;
            }

            // ---- 新连接 ----
            if fd == lfd {
                loop {
                    match listener.accept() {
                        Ok((stream, _addr)) => {
                            // **先加再判超**，不能读-比-加：多条 worker 同时
                            // accept 时，两条线程会同时看到 `max-1` 而各收一条，
                            // 实际上限就变成 max + 并发 worker 数。代价是下面
                            // 两条失败路径要把这个名额退回去。
                            let now = cnt.live.fetch_add(1, Ordering::Relaxed) + 1;
                            if w.max_conn > 0 && now > w.max_conn {
                                cnt.live.fetch_sub(1, Ordering::Relaxed);
                                let r = cnt.refused.fetch_add(1, Ordering::Relaxed) + 1;
                                crate::net::server::note_refusal(r, w.max_conn);
                                drop(stream);
                                continue;
                            }
                            if stream.set_nonblocking(true).is_err() || tune_conn(&stream).is_err() {
                                cnt.live.fetch_sub(1, Ordering::Relaxed);
                                continue;
                            }
                            let id = next_id;
                            next_id += 1;
                            let c = Conn::new(stream, id);
                            let nfd = c.fd;
                            epoll_set(ep, nfd, false)?;
                            by_id.insert(id, nfd);
                            conns.insert(nfd, c);
                            cnt.accepted.fetch_add(1, Ordering::Relaxed);
                            cnt.peak.fetch_max(now as u64, Ordering::Relaxed);
                        }
                        // WouldBlock（backlog 已空）或 EMFILE：留给下一轮。
                        Err(_) => break,
                    }
                }
                continue;
            }

            if !conns.contains_key(&fd) {
                continue;
            }

            // ---- 可读：搬进 rx ----
            if bits & (libc::EPOLLIN as u32 | libc::EPOLLHUP as u32 | libc::EPOLLERR as u32) != 0 {
                let mut eof = false;
                loop {
                    let room = conns
                        .get(&fd)
                        .map(|c| c.backlog() < MAX_BACKLOG)
                        .unwrap_or(false);
                    if !room {
                        // 背压：把读端从 epoll 掩码里摘掉，否则电平触发的
                        // EPOLLIN 会让 worker 线程每轮空转（同 serve_single）。
                        if let Some(c) = conns.get_mut(&fd) {
                            c.hold_read();
                        }
                        break;
                    }
                    match conns.get_mut(&fd) {
                        Some(c) => match c.stream.read(&mut buf) {
                            Ok(0) => {
                                eof = true;
                                break;
                            }
                            // 同 serve_single：读不满整块就收工，省掉每 op 一次
                            // 只能拿 EAGAIN 的 recvfrom（电平触发不会丢数据）。
                            Ok(m) => {
                                let full = m == buf.len();
                                c.rx.extend(&buf[..m]);
                                if !full {
                                    break;
                                }
                            }
                            Err(e)
                                if matches!(
                                    e.kind(),
                                    ErrorKind::WouldBlock | ErrorKind::Interrupted
                                ) =>
                            {
                                break;
                            }
                            Err(_) => {
                                eof = true;
                                break;
                            }
                        },
                        None => break,
                    }
                }
                if eof {
                    drop_conn(&mut conns, &mut by_id, ep, fd, cnt);
                    continue;
                }
            }

            // ---- 拆帧 + 解码 ----
            let mut frames: Vec<Result<Vec<u8>>> = Vec::new();
            loop {
                match conns.get_mut(&fd) {
                    Some(c) => match c.rx.take_frame() {
                        Ok(Some(p)) => frames.push(Ok(p)),
                        Ok(None) => break,
                        Err(e) => {
                            frames.push(Err(e));
                            break;
                        }
                    },
                    None => break,
                }
            }
            let mut ops: Vec<Op> = Vec::with_capacity(frames.len());
            let mut bad: Option<Reply> = None;
            // 这批帧里有没有握手 op，或者这条连接还没放行。记在解码循环里而
            // 不是事后重新遍历一遍 `any()`，是为了让已放行的热路径不额外遍历。
            let mut needs_gate = conns
                .get(&fd)
                .map(|c| !c.auth.allows_data())
                .unwrap_or(true);
            for f in frames {
                let payload = match f {
                    Ok(p) => p,
                    Err(e) => {
                        bad = Some(Reply::err(errno::EINVAL, e.to_string()));
                        break;
                    }
                };
                match Op::decode(&mut Rd::new(&payload)) {
                    Ok(o) => {
                        needs_gate |= crate::net::auth::is_handshake(&o);
                        ops.push(o);
                    }
                    Err(e) => {
                        bad = Some(Reply::err(errno::EINVAL, e.to_string()));
                        break;
                    }
                }
            }
            if let Some(r) = bad {
                // 坏帧只处分闯祸的连接：先把错误应答挂进它的队尾，再关掉。
                if let Some(c) = conns.get_mut(&fd) {
                    c.out.push_back(Slot::Filled(reply_frame(&r)));
                }
                drop_conn(&mut conns, &mut by_id, ep, fd, cnt);
                continue;
            }
            if ops.is_empty() {
                // 纯 `EPOLLOUT`（上一个回包把 socket 写满了，这一轮只是「能写了」的
                // 通知）也会落到这里，因为这类事件本来就没有可读字节。早期实现在
                // 这里直接 `continue`：写出那一块被跳过，用户态剩下的半个回包就再也
                // 发不出去 —— 电平触发的 `EPOLLOUT` 还会每轮都来，事件循环变成空转。
                // 实测形状：跨机拉 1 MiB 快照块时副本只能拿到前 ~110 KiB，之后一直
                // 读到 `--timeout-ms` 超时（同机 loopback 因为缓冲区一次能吃下整帧，
                // 根本没走过这条路径，所以一直没有暴露）。
                flush_on_write(&mut conns, &mut by_id, ep, fd, bits, cnt)?;
                continue;
            }

            // ---- 握手：在路由之前就地答掉（§4.4）----
            //
            // 两条不变量决定了这里的形状：
            // 1. 握手既不读也不写状态，不该占用提交线程 —— 必须就地答；
            // 2. `out` 是严格 FIFO 而协议没有 request id，所以只有「最后一条握手
            //    op 之后」的数据 op 能进下面的路由块；夹在它前面的那些只能回
            //    EAGAIN 让客户端重发（它们还没被碰过，重发安全）。
            // 单线程那条循环逐条顺序执行，天然没有这个问题。
            let mut route: Vec<Op> = ops;
            if needs_gate {
                let auth_key = w.auth.as_deref();
                let cut = route
                    .iter()
                    .rposition(crate::net::auth::is_handshake)
                    .map(|i| i + 1)
                    .unwrap_or(0);
                let mut inline: Vec<Reply> = Vec::new();
                // head 先算：它里面的握手 op 会推进闸门，tail 的放行判据必须
                // 等它跑完才成立。
                let open = match conns.get_mut(&fd) {
                    Some(c) => {
                        for op in &route[..cut] {
                            inline.push(
                                c.auth
                                    .on_op(auth_key, op)
                                    .unwrap_or_else(crate::net::auth::Gate::straddled),
                            );
                        }
                        c.auth.allows_data()
                    }
                    None => false,
                };
                if open {
                    if cut > 0 {
                        // 把 head 从待路由的批里摘掉（tail 一定不含握手 op）。
                        route = route.split_off(cut);
                    }
                } else {
                    if let Some(c) = conns.get_mut(&fd) {
                        for _ in cut..route.len() {
                            inline.push(c.auth.denied());
                        }
                    }
                    route.clear();
                }
                if !inline.is_empty() {
                    if let Some(c) = conns.get_mut(&fd) {
                        for r in &inline {
                            if !c.push_frame(r) {
                                c.out.push_back(Slot::Filled(reply_frame(r)));
                            }
                        }
                    }
                }
            }

            // ---- 路由：就地算，还是整批交给 committer ----
            // `route` 为空 = 这一批全被握手层就地答完了，不能 `continue`（那会
            // 跳过下面的 EPOLLOUT / 标脏处理，就地答的帧就没人发）。
            if !route.is_empty() {
                let has_write = route.iter().any(|o| !o.is_readonly());
                let waiting = conns.get(&fd).map(|c| c.queued > 0).unwrap_or(false);
                let conn_id = conns.get(&fd).map(|c| c.id).unwrap_or(0);
                if has_write || waiting {
                    if let Some(c) = conns.get_mut(&fd) {
                        for _ in 0..route.len() {
                            c.out.push_back(Slot::Await);
                        }
                        c.queued += route.len();
                    }
                    cnt.forwarded
                        .fetch_add(route.len() as u64, Ordering::Relaxed);
                    m_in.send(Forwarded {
                        conn: conn_id,
                        ops: route,
                    });
                } else {
                    let mut broken: Option<usize> = None;
                    if let Some(c) = conns.get_mut(&fd) {
                        for (k, op) in route.iter().enumerate() {
                            let (reply, logs) = Engine::exec_read(state, op);
                            if !logs.is_empty() {
                                // 只读 op 却产出了 logs —— 分派表错了。在这里丢掉
                                // logs 等于丢一次应当被确认的写，宁可不 ack。
                                broken = Some(k);
                                break;
                            }
                            if !c.push_frame(&reply) {
                                c.out.push_back(Slot::Filled(reply_frame(&reply)));
                            }
                        }
                    }
                    if let Some(k) = broken {
                        return Err(Error::Io(format!(
                            "internal: op #{k} dispatched as read-only produced logs"
                        )));
                    }
                    let k = route.len() as u64;
                    // 计数只进原子句柄，`stats` 本体仍然只有 committer 碰得到。
                    ops_ctr.fetch_add(k, Ordering::Relaxed);
                    cnt.inline_reads.fetch_add(k, Ordering::Relaxed);
                }
            }

            if !flush_on_write(&mut conns, &mut by_id, ep, fd, bits, cnt)? {
                continue;
            }
            if conns
                .get(&fd)
                .map(|c| !c.out.is_empty() || c.tx_pos < c.tx.len())
                .unwrap_or(false)
            {
                dirty.push(fd);
            }
        }

        for closed in flush_dirty(&mut conns, ep, &dirty)? {
            by_id.remove(&closed);
            cnt.live.fetch_sub(1, Ordering::Relaxed);
        }
    }

    // 退出前把 committer 的答话再收一遍。
    //
    // `flag` 是它在**回包之前**置的（`accept_batch` 里一看到 `Op::Shutdown`
    // 就置了），所以本线程完全可能先看到停位、跳出循环，而那个 ack 那时还
    // 在路上 —— 客户端只看到 EOF，`treedb-server --shutdown` 报
    // `shutdown: io: server closed`。先等一拍 BYE 再把信箱排干，让已经算好的
    // 成帧落进槽位、走下面统一写出。等不到（committer 崩了）也不拖住停服：
    // 上限 100 ms，比一次 fsync 的周期还短。
    if !bye {
        let _ = wait_readable(wfd, 100);
    }
    for a in m_out.drain() {
        if a.conn != BYE {
            let _ = fill_answer(&mut conns, &by_id, a);
        }
    }

    // 退出前尽力把已经算出来的应答发出去。`Op::Shutdown` 的 ack 就是在这里
    // 送达的：上面那次排空把它填进了槽位，本线程已经跳出循环，所以不在轮末
    // 写出路径里。
    let all: Vec<i32> = conns.keys().copied().collect();
    for closed in flush_dirty(&mut conns, ep, &all)? {
        by_id.remove(&closed);
        cnt.live.fetch_sub(1, Ordering::Relaxed);
    }
    unsafe { libc::close(ep) };
    Ok(())
}

/// 处理一条连接的 `EPOLLOUT`：接着写、写完把掩码对齐。
///
/// 返回 `false` 表示连接在这一步被关掉（写错误），调用方应立即 `continue`。
/// 之所以抽成一个函数：这条尾巴在两个地方需要 —— 正常事件处理的末尾，以及
/// 「本轮没有可读字节」的分支（那就是纯 `EPOLLOUT`，跳过它的大回包会永远
/// 停在用户态）。
fn flush_on_write(
    conns: &mut HashMap<i32, Conn>,
    by_id: &mut HashMap<u64, i32>,
    ep: i32,
    fd: i32,
    bits: u32,
    cnt: &Arc<Counters>,
) -> Result<bool> {
    if bits & libc::EPOLLOUT as u32 == 0 {
        return Ok(true);
    }
    if let Some(c) = conns.get_mut(&fd) {
        if c.flush().is_err() {
            drop_conn(conns, by_id, ep, fd, cnt);
            return Ok(false);
        }
        c.rearm(ep)?;
    }
    Ok(true)
}

/// 把 committer 交回的一批成品帧按序填进该连接的 `Slot::Await`。
///
/// 不需要帧 id：同连接的转发与应答都是 FIFO，第 k 个答复必然属于第 k 个
/// `Await` 槽。返回这条连接的 fd，调用方据此标脏。
fn fill_answer(
    conns: &mut HashMap<i32, Conn>,
    by_id: &HashMap<u64, i32>,
    ans: Answer,
) -> Option<i32> {
    let fd = match by_id.get(&ans.conn) {
        Some(f) => *f,
        // 连接已经关了：答案作废。里面的写仍然已经 fsync —— 那是"已落盘但
        // 没 ack"，与单线程时同一时刻客户端断连的结果完全一致。
        None => return None,
    };
    let c = conns.get_mut(&fd)?;
    let mut left = ans.frames.into_iter();
    for slot in c.out.iter_mut() {
        if matches!(slot, Slot::Await) {
            match left.next() {
                Some(f) => *slot = Slot::Filled(f),
                None => break,
            }
        }
    }
    debug_assert!(
        left.next().is_none(),
        "committer 多发了一条应答：转发计数与槽位数不一致"
    );
    // 直接按队列重算，不拿 `-= 消耗数`：宁可每轮多扫一遍短队列，也不要一个
    // 会越算越偏的计数器（算偏的后果是这条连接从此永久串行化）。
    c.queued = c.out.iter().filter(|s| matches!(s, Slot::Await)).count();
    Some(fd)
}

/// 关连接：摘 fd、摘 id、减全局在册数。
fn drop_conn(
    conns: &mut HashMap<i32, Conn>,
    by_id: &mut HashMap<u64, i32>,
    ep: i32,
    fd: i32,
    cnt: &Arc<Counters>,
) {
    if let Some(id) = close(conns, ep, fd) {
        by_id.remove(&id);
        cnt.live.fetch_sub(1, Ordering::Relaxed);
    }
}

/// flusher 落完一次盘的回执。`seq` 是提交线程 append 时给的上界，
/// `us` 是这次 `fdatasync` 的实际耗时（折进 `avg fsync`）。
struct SyncDone {
    seq: u64,
    us: u64,
    err: Option<String>,
}

/// 专用的 fsync 线程：提交线程只 `append`，落盘全在这里。
///
/// ## 为什么值得多一条线程
///
/// `group` 档下一次 `fdatasync` 在机械盘上是毫秒级。旧实现里提交线程阻塞在
/// 它里面，那一窗口既不接 worker 的转发也不发 ack，于是所有连接排在同一只
/// fsync 外面等（§7.13 group 档 64 并发 p99 24–29 ms 就是这支队伍的长度）。
/// 拆出去之后：提交线程 append 完就跑下一轮（接转发 + translate），fsync
/// 在另一条线程上跑，重叠部分白赚。
///
/// ## ack 没有因此提前
///
/// 提交线程 append 完只把「fsync 到哪个 seq」交给本线程，应答帧挂在一个
/// `Flight` 上。只有 `SyncDone` 回到提交线程、且 `seq >= flight.target` 之后
/// 才发那一批帧。「已确认的写不丢」这条承诺一个字没变。
struct Flusher {
    /// 本实例是否真的走流水线。`false` 时本结构依然存在（提交线程不为它
    /// 做任何事），但所有 stage 走 `Pending::flush_now` 的同步路径。
    enabled: bool,
    jobs: Arc<Mail<u64>>,
    done: Arc<Mail<SyncDone>>,
    /// fsync 用的 fd。checkpoint 收口会换 inode，所以这里要能换。
    fd: Arc<Mutex<File>>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

impl Flusher {
    fn spawn(fd: File, enabled: bool) -> Result<Arc<Flusher>> {
        let jobs: Arc<Mail<u64>> = Mail::new();
        let done: Arc<Mail<SyncDone>> = Mail::new();
        let cell = Arc::new(Mutex::new(fd));
        let (j, d, c) = (jobs.clone(), done.clone(), cell.clone());
        let h = std::thread::Builder::new()
            .name("treedb-flusher".into())
            .spawn(move || flusher_loop(j, d, c))
            .map_err(|e| Error::Io(format!("spawn flusher: {e}")))?;
        Ok(Arc::new(Flusher {
            enabled,
            jobs,
            done,
            fd: cell,
            handle: Mutex::new(Some(h)),
        }))
    }

    /// 提交一次「fsync 到 `seq`」。多次提交会被 flusher 合并成一次落盘。
    fn submit(&self, seq: u64) {
        self.jobs.send(seq);
    }

    /// 完成通知的 fd（挂进 committer 的 epoll）。
    fn fd(&self) -> i32 {
        self.done.fd()
    }

    fn take_done(&self) -> Vec<SyncDone> {
        self.done.drain()
    }

    /// 收口后换 fd：`truncate_before` / `reset` 把日志换成了新 inode。
    fn retarget(&self, f: File) {
        *self.fd.lock().unwrap_or_else(|e| e.into_inner()) = f;
    }

    /// 停线程：送一个 0 哨兵（seq 从 1 起，0 不会和真任务撞）。
    fn stop(&self) {
        self.jobs.send(0);
        if let Some(h) = self
            .handle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            let _ = h.join();
        }
    }
}

/// flusher 线程主体：阻塞在 jobs 的 eventfd 上，每次醒来把队列里的目标 seq
/// 取最大值、只落一次盘、发一个 `SyncDone`。
///
/// 合并是必需的：提交线程每轮都会 submit，而一次 fsync 天然覆盖它之前写完的
/// 全部字节，逐条 fsync 等于把刚赚到的摊薄又还回去。
fn flusher_loop(jobs: Arc<Mail<u64>>, done: Arc<Mail<SyncDone>>, fd: Arc<Mutex<File>>) {
    let mut pfd = libc::pollfd {
        fd: jobs.fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let rc = unsafe { libc::poll(&mut pfd, 1, -1) };
        if rc < 0 {
            let e = last_os();
            if e.contains("Interrupted") {
                continue;
            }
            done.send(SyncDone {
                seq: 0,
                us: 0,
                err: Some(format!("flusher poll: {e}")),
            });
            return;
        }
        let mut target = 0u64;
        let mut stop = false;
        for t in jobs.drain() {
            if t == 0 {
                stop = true;
            } else if t > target {
                target = t;
            }
        }
        if target > 0 {
            let t0 = std::time::Instant::now();
            let r = {
                let f = fd.lock().unwrap_or_else(|e| e.into_inner());
                f.sync_data()
            };
            let us = t0.elapsed().as_micros() as u64;
            match r {
                Ok(()) => done.send(SyncDone {
                    seq: target,
                    us,
                    err: None,
                }),
                Err(e) => {
                    done.send(SyncDone {
                        seq: target,
                        us,
                        err: Some(format!("fdatasync wal: {e}")),
                    });
                    return;
                }
            }
        }
        if stop {
            return;
        }
    }
}

/// 等一个 fd 可读（最多 `ms` 毫秒）。返回 false = 超时。
fn wait_readable(fd: i32, ms: i32) -> Result<bool> {
    let mut pfd = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        let rc = unsafe { libc::poll(&mut pfd, 1, ms) };
        if rc < 0 {
            let e = last_os();
            if e.contains("Interrupted") {
                continue;
            }
            return Err(Error::Io(format!("poll flusher: {e}")));
        }
        return Ok(rc > 0);
    }
}

/// committer 本轮攥着的活儿：一批待 fsync 的写 + 每个已转发 op 的应答去向。
enum Entry {
    /// 就地算出来的读，帧已就绪。
    Ready(Vec<u8>),
    /// 写：下标指向 `Pending::staged`，fsync 成功后才有帧。
    Staged(usize),
}

struct Flight {
    /// 这一批 append 到的最后一条 seq；fsync 覆盖到它才允许 ack。
    /// 0 = 这批不需要落盘（纯读批次，或 `never` 档）。
    target: u64,
    batches: Vec<(usize, u64, Vec<Entry>)>,
    staged: Vec<Staged>,
}

#[derive(Default)]
struct Pending {
    staged: Vec<Staged>,
    /// (worker 下标, 连接号, 每个 op 的应答)，顺序即提交顺序。
    batches: Vec<(usize, u64, Vec<Entry>)>,
    /// 已经 append、等落盘回执才能 ack 的批次。target 单调递增，所以永远是
    /// 从队头开始出队。
    inflight: VecDeque<Flight>,
}

impl Pending {
    fn is_empty(&self) -> bool {
        self.staged.is_empty() && self.batches.is_empty()
    }

    fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    /// 把本轮已 stage 的写 append 进 WAL，并把应答目标挂成一个 `Flight`。
    ///
    /// 返回本次 append 到的最后一条 seq（`None` = 没有日志或不需要落盘）。
    /// 调用方拿它调 [`Flusher::submit`]；**不要在这里等盘** —— 等盘的地方
    /// 在 [`Pending::settle`]，它只负责把已经落了盘的批次放出去。
    fn stage(&mut self, engine: &mut Engine) -> Result<Option<u64>> {
        if self.is_empty() {
            return Ok(None);
        }
        let groups: Vec<&[Log]> = self.staged.iter().map(|s| s.logs.as_slice()).collect();
        let last = engine.stage_batch(&groups)?;
        // `never` 档：append 就是全部，不进 flusher（否则等于把 never 档
        // 变成 group 档，性能特征与文档承诺同时失效）。
        let target = match last {
            Some(seq) if engine.durable_writes() => seq,
            _ => 0,
        };
        let batches = std::mem::take(&mut self.batches);
        let staged = std::mem::take(&mut self.staged);
        self.inflight.push_back(Flight {
            target,
            batches,
            staged,
        });
        Ok(if target > 0 { Some(target) } else { None })
    }

    /// 收掉 flusher 的回执，把已经落盘的批次按原顺序送回各自 worker。
    ///
    /// **ack 严格晚于 fsync**：一次 `fdatasync` 报错就是「内存超前于磁盘」，
    /// 此时任何应答都是谎报，故直接抛出 Err 让 bin 终止进程。
    ///
    /// 返回本次消掉的回执数（调用方用它判断「这一等到底有没有进展」）。
    fn settle(
        &mut self,
        engine: &mut Engine,
        outs: &[Arc<Mail<Answer>>],
        fl: &Flusher,
    ) -> Result<usize> {
        let dones = fl.take_done();
        let n = dones.len();
        let mut hi = 0u64;
        for d in dones {
            if let Some(e) = d.err {
                return Err(Error::Io(e));
            }
            if d.seq > hi {
                hi = d.seq;
            }
            engine.note_synced(d.seq, d.us);
        }
        while let Some(f) = self.inflight.front() {
            if f.target == 0 || f.target <= hi {
                let f = self.inflight.pop_front().expect("front just checked");
                Self::deliver(f, outs);
            } else {
                break;
            }
        }
        Ok(n)
    }

    fn deliver(f: Flight, outs: &[Arc<Mail<Answer>>]) {
        let mut staged = f.staged;
        for (wi, conn, entries) in f.batches {
            let mut frames = Vec::with_capacity(entries.len());
            for e in entries {
                frames.push(match e {
                    Entry::Ready(fr) => fr,
                    Entry::Staged(k) => std::mem::take(&mut staged[k].frame),
                });
            }
            outs[wi].send(Answer { conn, frames });
        }
    }

    /// 同步档：一次 `commit_batch` 覆盖本轮全部已 stage 的写，落盘后立刻回包。
    ///
    /// 语义与流水线档的 ack 完全一致（ack 严格晚于 fsync），只是等盘的那一段
    /// 由提交线程自己站着。这是默认档 —— 在 fsync 1.3–1.6 ms 的盘上，把这段
    /// 挑到另一条线程反而多付 150 µs 的线路往返，实测净负（见
    /// `ServerConfig::flush_pipeline`）。
    fn flush_now(&mut self, engine: &mut Engine, outs: &[Arc<Mail<Answer>>]) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }
        let groups: Vec<&[Log]> = self.staged.iter().map(|s| s.logs.as_slice()).collect();
        engine.commit_batch(&groups)?;
        let batches = std::mem::take(&mut self.batches);
        let staged = std::mem::take(&mut self.staged);
        Self::deliver(
            Flight {
                target: 0,
                batches,
                staged,
            },
            outs,
        );
        Ok(())
    }

    /// 阻塞到 in-flight 全部 ack。checkpoint 收口、显式 Checkpoint、Shutdown
    /// 与退出前用——这些动作要么会换 WAL 的 inode，要么必须在 ack 之后才能
    /// 报告状态，所以不能带着未落盘的批次跑。
    fn drain_all(
        &mut self,
        engine: &mut Engine,
        outs: &[Arc<Mail<Answer>>],
        fl: &Flusher,
    ) -> Result<()> {
        if !fl.enabled {
            return self.flush_now(engine, outs);
        }
        // 先把攒在 `pending` 里还没 append 的那一轮提交出去：主循环只在
        // in-flight 为空时 stage，所以到这里的调用方（Checkpoint / Shutdown /
        // 收口 / 退出）手里一定可能有未 stage 的写。
        if !self.is_empty() {
            if let Some(seq) = self.stage(engine)? {
                fl.submit(seq);
            }
        }
        self.settle(engine, outs, fl)?;
        let mut idle_s = 0u64;
        while !self.inflight.is_empty() {
            if !wait_readable(fl.fd(), 1_000)? {
                // 1 s 没回执不算错（机械盘上队列满时 fsync 真能拉到秒级），
                // 但 60 s 不见一只就必须当 flusher 已死：继续等等于把进程
                // 卡在一条永远不会来的通知上，还不如退出去让 keeper 拉起。
                idle_s += 1;
                if idle_s >= 60 {
                    return Err(Error::Io(
                        "flusher did not report within 60s: refusing to ack un-synced writes"
                            .into(),
                    ));
                }
                continue;
            }
            idle_s = 0;
            self.settle(engine, outs, fl)?;
        }
        Ok(())
    }

    /// 最少等回一个落盘回执。现阶段主循环把在飞数限为 1，本函数是给
    /// 「以后想放宽到 N 个在飞」留的接口与测试钩子；不调它就是零成本。
    #[allow(dead_code)]
    fn wait_one(
        &mut self,
        engine: &mut Engine,
        outs: &[Arc<Mail<Answer>>],
        fl: &Flusher,
    ) -> Result<()> {
        let before = self.inflight.len();
        while self.inflight.len() >= before {
            if !wait_readable(fl.fd(), 1_000)? {
                continue;
            }
            self.settle(engine, outs, fl)?;
        }
        Ok(())
    }
}

/// 处理一条转发出的批次。`wi` 是它的来路，答案原路送回。
fn accept_batch(
    fwd: Forwarded,
    wi: usize,
    engine: &mut Engine,
    pend: &mut Pending,
    outs: &[Arc<Mail<Answer>>],
    fl: &Flusher,
    cnt: &Arc<Counters>,
    flag: &AtomicBool,
) -> Result<bool> {
    let mut shutdown = false;
    let mut entries = Vec::with_capacity(fwd.ops.len());
    for op in &fwd.ops {
        // checkpoint / shutdown 会动 WAL，必须先把已经 stage 的写落盘，
        // 否则快照水位会跑到磁盘前面。
        if matches!(op, Op::Checkpoint | Op::Shutdown) {
            pend.drain_all(engine, outs, fl)?;
        }
        let (logs, reply) = engine.exec(op);
        let frame = reply_frame(&reply);
        if logs.is_empty() {
            entries.push(Entry::Ready(frame));
        } else {
            pend.staged.push(Staged { logs, frame });
            entries.push(Entry::Staged(pend.staged.len() - 1));
        }
        if matches!(op, Op::Shutdown) {
            shutdown = true;
            flag.store(true, Ordering::Relaxed);
        }
    }
    if !entries.is_empty() {
        cnt.forwarded.fetch_add(entries.len() as u64, Ordering::Relaxed);
        pend.batches.push((wi, fwd.conn, entries));
    }
    Ok(shutdown)
}

/// 唯一提交线程：收齐所有 worker 的批次 → 定序执行 → 一次 fsync → 原路送回。
fn committer(
    cfg: &ServerConfig,
    engine: &mut Engine,
    ins: &[Arc<Mail<Forwarded>>],
    outs: &[Arc<Mail<Answer>>],
    dead: &Arc<Mail<()>>,
    stop: &AtomicBool,
    flag: &AtomicBool,
    cnt: &Arc<Counters>,
    live_out: &AtomicUsize,
) -> Result<Report> {
    let ep = epoll_create()?;
    for m in ins.iter() {
        epoll_set(ep, m.fd(), false)?;
    }
    epoll_set(ep, dead.fd(), false)?;

    let mut rep = Report::default();
    rep.rec = engine.recovery();
    rep.workers = cfg.workers;
    // 副本追平（§6.6）。`Follower` 只能在 committer 里构造：apply 需要
    // `&mut Engine`，而 `Engine` 只活在这一条线程上 —— 拿 `None` 时下面那句
    // tick 就是一次指针判空，主那侧的代码路径与改造之前逐字一致。
    let mut follower = cfg
        .replication
        .as_ref()
        .map(|c| Follower::new(c.clone()));
    let mut events: Vec<libc::epoll_event> = vec![unsafe { std::mem::zeroed() }; MAX_EVENTS];
    let mut pend = Pending::default();
    // 落盘线程（§11 第 8 条剩下的那一半）：提交线程只 append，`fdatasync`
    // 在它自己那条线程上跑。fd 是 dup 出来的，与提交线程的写共享同一批页；
    // 完成通知通过一只 eventfd 挂进本循环的 epoll。
    let fl = Flusher::spawn(engine.wal_sync_fd()?, cfg.flush_pipeline)?;
    epoll_set(ep, fl.fd(), false)?;
    // Never 档的周期 sync 计时基准；Group/Always 的 fsync 已经在 flusher
    // 里按语义付过了。
    let mut last_sync = Instant::now();
    // 过期租约清扫的节拍（§8 第 17 条）。清扫只能在这里：它是一条写
    // （`Log::DelLock`），而写永远只在这条线程上定序。
    let mut last_sweep = Instant::now();

    while !stop.load(Ordering::Relaxed) && !flag.load(Ordering::Relaxed) {
        let t0 = Instant::now();
        // 有活儿时靠 eventfd 醒，空闲时这个超时只用于复检 `stop`（信号停服）
        // 和跑周期任务。它绝不会被拿来等提交线程自己的答案 —— 那是单线程
        // 循环时代码池版踩过的坑：拿 20 ms 等 µs 级的活儿。
        let timeout = if cfg.sync_ms > 0 { cfg.sync_ms } else { 200 };
        let n = unsafe { libc::epoll_wait(ep, events.as_mut_ptr(), events.len() as i32, timeout) };
        if n < 0 {
            let e = last_os();
            if e.contains("Interrupted") {
                continue;
            }
            unsafe { libc::close(ep) };
            return Err(Error::Io(format!("epoll_wait: {e}")));
        }
        rep.rounds += 1;
        // 上一轮 append 的那一批：回执到了就 ack。先于接收新活儿 —— 这是
        // 客户端看到「前一次写已确认」的唯一时机，也是流水线赚到的那段重叠。
        pend.settle(engine, outs, &fl)?;
        // 不看哪几个 fd 到了：**全排干**。group commit 的意义就是把这一轮
        // 里所有 worker 的写并成一次 fsync，只排干被唤醒的那条等于把摊薄
        // 比例砍到 1/N。
        for (wi, m) in ins.iter().enumerate() {
            for fwd in m.drain() {
                if accept_batch(fwd, wi, engine, &mut pend, outs, &fl, cnt, flag)? {
                    // Shutdown 的 ack 必须先送回 worker，否则客户端会挂在那儿。
                    pend.drain_all(engine, outs, &fl)?;
                }
            }
        }
        if !dead.is_empty() {
            let _ = dead.drain();
        }
        if fl.enabled {
            // stage 一轮：append 完立刻交回控制权。但**同一时刻只允许一个 fsync
            // 在飞**：上一只 `fdatasync` 没回来之前，本轮收到的写先攒在
            // `pending` 里，等回执到了再跟后面的写一起 append。
            //
            // 不这么做（每轮都 stage + submit）会把批次切碎：fsync 期间
            // 进来的写各自挂一个 flight，排在下一次 fsync 后面，结果是「fsync
            // 次数变多、每次覆盖变少、尾部多等一整个周期」—— 实测本机 QPS 反而
            // -13%。攒到一个周期的范围再提交，单次 fsync 的覆盖度才不低于旧实现。
            if pend.inflight_len() == 0 {
                if let Some(seq) = pend.stage(engine)? {
                    fl.submit(seq);
                }
            }
        } else {
            // 同步档（默认）：与改造前逐字一致的路径 —— 一次 `commit_batch`
            // 覆盖本轮全部已 stage 的写，落盘后立刻回包。
            pend.flush_now(engine, outs)?;
        }
        live_out.store(cnt.live.load(Ordering::Relaxed), Ordering::Relaxed);

        if cfg.sync_ms > 0 && last_sync.elapsed() >= Duration::from_millis(cfg.sync_ms as u64) {
            engine.sync_now()?;
            last_sync = Instant::now();
        }
        // 旁路快照（§8 第 7 条）：收一份已完成的 + 判要不要派新的，两件都是
        // 常数时间。这里不能走 stop-the-round 的那条：一条慢轮会把所有连接
        // 的 ack 一起拖长，而那正是自动档要避免的。
        //
        // 收口会把 WAL 换成新 inode（`truncate_before`），所以先把 in-flight
        // 的 fsync 排空、收完之后再让 flusher 换 fd —— 否则后续 fsync 全刷到
        // 一个已经被 rename 走掉的旧 inode 上，新记录永不落盘而 ack 照发。
        if engine.checkpoint_ready() {
            pend.drain_all(engine, outs, &fl)?;
            engine.checkpoint_poll()?;
            fl.retarget(engine.wal_sync_fd()?);
        }
        if cfg.auto_checkpoint_bytes > 0 {
            // 只要 WAL 尺寸。这里不能走 `snapshot_stats()`：它除了判尺寸还会
            // 顺手折一次 worker 的 op 计数，而折计数只在有人真问数时才值钱
            // （`Op::Stats` 与退出收口两处），每轮折一次等于白拿一次读锁，
            // 而且 dentry 条数在两层表下是 O(目录数)。
            if engine.wal_bytes() > cfg.auto_checkpoint_bytes {
                engine.checkpoint_start()?;
            }
        }
        if let Some(f) = follower.as_mut() {
            f.tick(engine)?;
        }
        // 过期租约回收（§8 第 17 条）：读路径只过滤、不回收（读 op 不许产生
        // 日志），回收放在这条唯一的写线程上，用已有的 `Log::DelLock`。
        //
        // 它在 flusher 档里会自己 `commit_batch`（一次同步 fsync），与在飞的
        // 那次 fsync 不冲突：同一个文件上两条 fsync 只是多付一次，不会把
        // 已 ack 的写置于磁盘外。而这种时候只在扫到了东西时才发生。
        if cfg.lock_sweep_ms > 0
            && last_sweep.elapsed() >= Duration::from_millis(cfg.lock_sweep_ms as u64)
        {
            engine.sweep_expired_locks(crate::engine::now_ms(), crate::engine::LOCK_SWEEP_BATCH)?;
            last_sweep = Instant::now();
        }
        let us = t0.elapsed().as_micros() as u64;
        if us > rep.worst_round_us {
            rep.worst_round_us = us;
        }
    }

    // 停位落下之后，信箱里可能还压着已经收进内存的 op。这些请求的字节早就属于
    // 我们了：不回等于让客户端挂在半路上，而回了就必须真落盘 —— 所以再收一轮
    // 才走。
    for (wi, m) in ins.iter().enumerate() {
        for fwd in m.drain() {
            let _ = accept_batch(fwd, wi, engine, &mut pend, outs, &fl, cnt, flag)?;
        }
    }
    if fl.enabled {
        if let Some(seq) = pend.stage(engine)? {
            fl.submit(seq);
        }
    }
    // 退出前把最后一批落定：这些请求的字节早就是我们的了，不回等于让客户端
    // 挂在半路上，而回了就必须真落盘。
    pend.drain_all(engine, outs, &fl)?;
    fl.stop();

    // 优雅退出：落一次快照，把下次重启的重放窗口压到最短。
    // 与单线程路径同一条口径：不吞错，退出码要反映磁盘是不是真的干净。
    let shutdown = engine
        .sync_now()
        .and_then(|_| engine.checkpoint().map(|_| ()));
    let s = engine.snapshot_stats();
    rep.ops = s.ops_total;
    rep.writes = s.writes_total;
    rep.syncs = s.syncs_total;
    rep.checkpoints = s.checkpoints;
    rep.avg_sync_us = if s.syncs_total > 0 {
        s.sync_us_total / s.syncs_total
    } else {
        0
    };
    rep.replica = s.replica;
    rep.replica_applied = s.replica_applied;
    rep.replica_lag = s.replica_lag;
    rep.replica_resyncs = s.replica_resyncs;
    rep.replica_errors = s.replica_errors;
    live_out.store(cnt.live.load(Ordering::Relaxed), Ordering::Relaxed);
    unsafe { libc::close(ep) };
    shutdown?;
    Ok(rep)
}

/// 建一张 `SO_REUSEPORT` 监听套接字。
///
/// 必须手工 `socket` + `setsockopt` + `bind`：std 的 `TcpListener::bind` 给不了
/// `SO_REUSEPORT`，而这个标志**只能在 bind 之前**设 —— 内核是在 bind 那一刻决定
/// 要不要把新套接字并入已有的 reuseport 簇，bind 完再补是加不进去的。
fn reuseport_listener(addr: SocketAddr, backlog: i32) -> Result<TcpListener> {
    // 必须是 `CString`：Rust 的 `String` 不保证 NUL 结尾，直接把 `as_ptr()` 交给
    // `getaddrinfo`，它会读过缓冲区边界接着找终止符 —— 端口串后面碰巧跟着个
    // 非零字节时，glibc 就把它当服务名去查 /etc/services，报 EAI_SERVICE。
    let host = std::ffi::CString::new(addr.ip().to_string()).expect("ip has no interior NUL");
    let port = std::ffi::CString::new(addr.port().to_string()).expect("port has no interior NUL");
    let mut hints: libc::addrinfo = unsafe { std::mem::zeroed() };
    hints.ai_family = if addr.is_ipv4() {
        libc::AF_INET
    } else {
        libc::AF_INET6
    };
    hints.ai_socktype = libc::SOCK_STREAM;
    hints.ai_flags = libc::AI_PASSIVE;
    let mut res: *mut libc::addrinfo = std::ptr::null_mut();
    let rc = unsafe { libc::getaddrinfo(host.as_ptr(), port.as_ptr(), &hints, &mut res) };
    if rc != 0 {
        let why = unsafe {
            std::ffi::CStr::from_ptr(libc::gai_strerror(rc)).to_string_lossy()
        };
        return Err(Error::Io(format!("getaddrinfo {addr}: {why}")));
    }
    let on = 1 as libc::c_int;
    let sz = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
    let mut bound: i32 = -1;
    let mut err = String::new();
    let mut p = res;
    while !p.is_null() {
        let ai = unsafe { *p };
        // SOCK_NONBLOCK：worker 的 accept 循环靠 EAGAIN 收尾，不用另设 O_NONBLOCK。
        let fd = unsafe { libc::socket(ai.ai_family, libc::SOCK_STREAM | libc::SOCK_CLOEXEC | libc::SOCK_NONBLOCK, 0) };
        if fd < 0 {
            err = last_os();
            p = ai.ai_next;
            continue;
        }
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEADDR,
                &on as *const _ as *const libc::c_void,
                sz,
            );
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &on as *const _ as *const libc::c_void,
                sz,
            );
        }
        if unsafe { libc::bind(fd, ai.ai_addr, ai.ai_addrlen) } == 0 {
            bound = fd;
            break;
        }
        err = last_os();
        unsafe { libc::close(fd) };
        p = ai.ai_next;
    }
    unsafe { libc::freeaddrinfo(res) };
    if bound < 0 {
        // 同地址同端口已经被别的进程占着。注意 `SO_REUSEPORT` 的反面：
        // 对面**也**开了这个标志时内核会允许并列 bind，届时流量被两边分摊
        // —— 单实例假设得靠部署侧保证（见 DESIGN.md §8）。
        return Err(Error::Io(format!("bind reuseport {addr}: {err}")));
    }
    if backlog > 0 && unsafe { libc::listen(bound, backlog) } != 0 {
        let e = last_os();
        unsafe { libc::close(bound) };
        return Err(Error::Io(format!("listen backlog {backlog}: {e}")));
    }
    Ok(unsafe { TcpListener::from_raw_fd(bound) })
}
