//! 线协议 + 事件循环的服务端到端验收：请求经真 TCP、真 epoll、真 fsync。
//!
//! [`engine_crud`](super::engine_crud) 验的是「语义对不对」，这里验的是
//! 「把它接在网络上当服务跑，还成不成立」：帧边界、每连接应答顺序、
//! 跨连接隔离、大应答背压、坏帧只杀违规者、停服退码。
//!
//! 跑法：`cargo test --test net_protocol`

mod common;

use common::Server;
use std::io::{Read, Write};
use std::net::TcpStream;
use treedb::engine::model::{
    errno, ColumnDef, Op, Reply, TableDef, COL_INDEX, COL_UNIQUE, COND_NONE, Kind, Node, ROW_PK,
};
use treedb::engine::{encode_frame, RxBuf, FRAME_HDR, MAX_FRAME};
use treedb::net::client::Client;

fn call(c: &mut Client, op: &Op) -> Reply {
    match c.call(op) {
        Ok(r) => r,
        Err(e) => panic!("{op:?} → transport error: {e}"),
    }
}

fn ok(c: &mut Client, op: &Op) -> Reply {
    match call(c, op) {
        Reply::Err { code, msg } => panic!(
            "{op:?} → errno {code} ({})",
            String::from_utf8_lossy(&msg)
        ),
        r => r,
    }
}

fn as_node(r: Reply) -> Node {
    match r {
        Reply::Node(n) => n,
        other => panic!("expected Node, got {other:?}"),
    }
}

fn mk_view(c: &mut Client, name: &str) -> u32 {
    match ok(c, &Op::CreateView { name: name.into() }) {
        Reply::View { id, root } => {
            assert_eq!(root, 1);
            id
        }
        other => panic!("{other:?}"),
    }
}

fn create(c: &mut Client, view: u32, name: &str) -> Node {
    as_node(ok(
        c,
        &Op::Create {
            view,
            parent: 1,
            name: name.into(),
            mode: 0o644,
            uid: 1000,
            gid: 1000,
        },
    ))
}

#[test]
fn tcp_crud_roundtrip() {
    let srv = Server::start("always");
    let mut c = srv.client();

    let v = mk_view(&mut c, "v");
    let f = create(&mut c, v, "f");
    assert_eq!((f.ino, f.kind, f.mode), (2, Kind::File, 0o644));
    assert_eq!(as_node(call(&mut c, &Op::Lookup { view: v, parent: 1, name: b"f".to_vec() })), f);
    assert_eq!(as_node(call(&mut c, &Op::Getattr { view: v, ino: f.ino })), f);
    match call(&mut c, &Op::ReadDir { view: v, parent: 1, offset: 0, limit: 10 }) {
        Reply::Dirents { names, inos, end, .. } => {
            assert_eq!(names.to_vec(), vec![b"f".to_vec()]);
            assert_eq!(inos, vec![f.ino]);
            assert!(end);
        }
        other => panic!("{other:?}"),
    }
    // 协议错误码必须是裸 POSIX errno：FUSE 侧要能原样回给内核。
    match call(&mut c, &Op::Lookup { view: v, parent: 1, name: b"ghost".to_vec() }) {
        Reply::Err { code, .. } => assert_eq!(code, errno::ENOENT),
        other => panic!("{other:?}"),
    }
    assert_eq!(call(&mut c, &Op::Ping), Reply::Pong);
    // unlink 的应答是 GC 线索：空串 = 没有对象失去最后一个持有者。
    assert_eq!(
        ok(&mut c, &Op::Unlink { view: v, parent: 1, name: b"f".to_vec() }),
        Reply::NodeRef(Vec::new())
    );
    assert_eq!(
        match call(&mut c, &Op::Lookup { view: v, parent: 1, name: b"f".to_vec() }) {
            Reply::Err { code, .. } => code,
            other => panic!("{other:?}"),
        },
        errno::ENOENT
    );
    srv.graceful_shutdown();
}

/// 一条连接上 pipeline 一批混合 op：应答必须与请求一一对应、顺序一致。
///
/// 这是本协议没有 request id 的代价所在——服务端 `Slot::Pending` 槽位若回填
/// 错位，这里就会看到「第 k 个应答不是第 k 个请求的结果」。
#[test]
fn pipeline_keeps_per_connection_order() {
    let srv = Server::start("group");
    let mut c = srv.client();
    let v = mk_view(&mut c, "pipe");

    const N: usize = 300;
    let mut ops: Vec<Op> = Vec::new();
    for k in 0..N {
        ops.push(Op::Create {
            view: v,
            parent: 1,
            name: format!("p{k}").into(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        });
        // 夹一条读 op：它不进 WAL，走的是另一条 push 路径，
        // 混在同一批里才能验两条路径的相对顺序。
        ops.push(Op::Lookup {
            view: v,
            parent: 1,
            name: format!("p{k}").into(),
        });
    }
    let reps = c.pipeline(&ops).expect("pipeline");
    assert_eq!(reps.len(), ops.len());
    for (k, r) in reps.iter().enumerate() {
        let n = match r {
            Reply::Node(n) => n.clone(),
            other => panic!("reply {k}: {other:?}"),
        };
        // 奇数下标是 Lookup p{k/2}，不是 p{k}：应答必须落在自己的请求上。
        assert_eq!(
            n.name,
            format!("p{}", k / 2).into_bytes(),
            "应答错位：第 {k} 个回复不是第 {k} 个请求的结果"
        );
        if k % 2 == 0 {
            assert!(n.ino > 1, "create 应当拿到新 ino，{k}");
        } else {
            // lookup 必须回与上一条 create 相同的节点，否则就是槽位错位。
            match &reps[k - 1] {
                Reply::Node(prev) => assert_eq!(&n, prev, "第 {k} 个 lookup 与自己的 create 不同"),
                other => panic!("reply {}: {other:?}", k - 1),
            }
        }
    }
    // 同一连接继续可用，且状态与批量写一致。
    match ok(&mut c, &Op::ReadDir { view: v, parent: 1, offset: 0, limit: 0 }) {
        Reply::Dirents { names, end, .. } => {
            assert_eq!(names.len(), N);
            assert!(end);
        }
        other => panic!("{other:?}"),
    }
    srv.graceful_shutdown();
}

/// 8 条连接各写各的视图：跨连接并发不能串数据。
#[test]
fn concurrent_connections_do_not_crosstalk() {
    let srv = Server::start("group");
    let addr = srv.addr.clone();
    let mut handles = Vec::new();
    for t in 0..8usize {
        let a = addr.clone();
        handles.push(std::thread::spawn(move || {
            let mut c = Client::connect(&a, 0).expect("connect");
            let v = match ok(&mut c, &Op::CreateView { name: format!("t{t}").into() }) {
                Reply::View { id, .. } => id,
                other => panic!("{other:?}"),
            };
            for k in 0..100u32 {
                let n = create(&mut c, v, &format!("t{t}-f{k}"));
                assert_eq!(n.parent, 1);
            }
            // 只读自己的视图：看到别家的文件就是串了。
            match ok(&mut c, &Op::ReadDir { view: v, parent: 1, offset: 0, limit: 0 }) {
                Reply::Dirents { names, .. } => {
                    assert_eq!(names.len(), 100, "线程 {t} 目录条目数不对");
                    let mine = format!("t{t}-f").into_bytes();
                    assert!(names.iter().all(|n| n.starts_with(&mine)), "{names:?}");
                }
                other => panic!("{other:?}"),
            }
            // 别家视图里的名字在自己这里必须是 ENOENT，不是「碰巧能查到」。
            // 名字不带 '/'：带 '/' 在单组件 lookup 里是 EINVAL，测不到隔离。
            let other_view = (t + 1) % 8;
            match call(&mut c, &Op::Lookup { view: v, parent: 1, name: format!("t{other_view}-f0").into_bytes() }) {
                Reply::Err { code, .. } => assert_eq!(code, errno::ENOENT),
                r => panic!("串到别家视图了: {r:?}"),
            }
            v
        }));
    }
    let vids: Vec<u32> = handles.into_iter().map(|h| h.join().expect("thread panicked")).collect();
    assert_eq!(vids.len(), 8);
    // 8 个视图 id 互不相同，且服务端总数就是 8。
    let mut sorted = vids.clone();
    sorted.sort_unstable();
    sorted.dedup();
    assert_eq!(sorted.len(), 8, "视图 id 撞号: {vids:?}");
    match ok(&mut srv.client(), &Op::Stats) {
        Reply::Stats(s) => {
            assert_eq!(s.views, 8);
            // 8 根 + 8×100 文件
            assert_eq!(s.nodes, 8 * 101);
        }
        other => panic!("{other:?}"),
    }
    srv.graceful_shutdown();
}

/// 把接收缓冲压到 `bytes`：客户端不读 + 对端窗口小，服务端才会真的一致写不下。
///
/// `TcpStream::set_recv_buffer_size` 在 std 里不是 Unix 固有方法，直接走 setsockopt。
fn shrink_rcvbuf(s: &TcpStream, bytes: i32) {
    use std::os::unix::io::AsRawFd;
    let v = bytes;
    let rc = unsafe {
        libc::setsockopt(
            s.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &v as *const i32 as *const libc::c_void,
            std::mem::size_of_val(&v) as libc::socklen_t,
        )
    };
    assert_eq!(rc, 0, "SO_RCVBUF: {}", std::io::Error::last_os_error());
}

/// 大应答（远超 socket 缓冲）必须完整流出去：这条路径走 `tx`/`tx_pos` +
/// EPOLLOUT 续写，是事件循环里最容易「少写一截」的地方。
///
/// 客户端把接收缓冲压到 8 KiB 且中途不读，逼服务端一轮写不完，从而真的
/// 挂上 EPOLLOUT；应答体积远大于两个缓冲，否则一次 write 全推完就测不到背压。
#[test]
fn large_readdir_response_streams_out_fully() {
    let srv = Server::start("group");
    let mut c = srv.client();
    let v = mk_view(&mut c, "big");

    const N: usize = 20_000;
    // 分批 pipeline 写入，避免一次 2 万帧把接收侧也撑爆（那测的是另一件事）。
    let mut ops: Vec<Op> = Vec::new();
    for k in 0..N {
        ops.push(Op::Create {
            view: v,
            parent: 1,
            name: format!("file-{k:05}").into(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        });
        if ops.len() == 2000 {
            let reps = c.pipeline(&ops).expect("warmup pipeline");
            assert!(reps.iter().all(|r| matches!(r, Reply::Node(_))));
            ops.clear();
        }
    }
    assert!(ops.is_empty(), "批量没切干净");

    let raw = TcpStream::connect(&srv.addr).expect("connect");
    shrink_rcvbuf(&raw, 8 * 1024);
    let mut slow = Client::from_stream(raw);
    let reps = slow
        .pipeline(&[Op::ReadDir { view: v, parent: 1, offset: 0, limit: N as u32 }])
        .expect("big readdir");
    match &reps[0] {
        Reply::Dirents { names, inos, kinds, end, next } => {
            assert_eq!(names.len(), N, "大应答被截断了");
            assert_eq!(inos.len(), N);
            assert!(kinds.iter().all(|k| *k == Kind::File.code()));
            assert!(*end);
            assert_eq!(*next, N as u64);
            // 字节序：file-00000 .. file-019999。
            assert_eq!(names[0], b"file-00000".to_vec());
            assert_eq!(names[N - 1], format!("file-{:05}", N - 1).into_bytes());
        }
        other => panic!("expected Dirents, got {other:?}"),
    }
    // 同一条被背压拖过的连接之后还能正常干活。
    assert_eq!(call(&mut slow, &Op::Ping), Reply::Pong);
    srv.graceful_shutdown();
}

/// 坏帧（长度越上限）只能打死违规的那条连接，其他连接与服务端本体不受影响。
#[test]
fn oversized_frame_kills_only_the_offender() {
    let srv = Server::start("always");
    let good = srv.client();
    let mut bad = TcpStream::connect(&srv.addr).expect("connect");

    // 一个合法得离谱的头：len = u32::MAX-1，远超 MAX_FRAME。
    let mut hdr = [0u8; 4 + 2];
    hdr[..4].copy_from_slice(&(u32::MAX - 1).to_le_bytes());
    bad.write_all(&hdr).expect("send bogus header");
    let mut probe = [0u8; 16];
    // 服务端要么直接关连接，要么回一条 EINVAL 再关；两种都算合规，
    // 但绝不能把这条连接的脏状态留给别人。
    let _ = bad.read(&mut probe);

    let mut g = good;
    assert_eq!(call(&mut g, &Op::Ping), Reply::Pong);
    let v = mk_view(&mut g, "survivor");
    create(&mut g, v, "still-here");
    // 违规者已经没了：再写会拿到 EPIPE/EOF。
    let mut dead = TcpStream::connect(&srv.addr).expect("reconnect");
    dead.write_all(&encode_frame(&[0xFF, 0xFF])).expect("send unknown op tag");
    let mut buf = [0u8; 64];
    let n = dead.read(&mut buf).unwrap_or(0);
    if n > 0 {
        // 若回错了才关，也应答必须是 EINVAL。
        let mut rx = RxBuf::with_capacity(64);
        rx.extend(&buf[..n]);
        let frame = rx.take_frame().expect("frame").expect("complete frame");
        match Reply::decode(&mut treedb::engine::codec::Rd::new(&frame)) {
            Ok(Reply::Err { code, .. }) => assert_eq!(code, errno::EINVAL),
            other => panic!("expected EINVAL, got {other:?}"),
        }
    }
    assert_eq!(call(&mut g, &Op::Ping), Reply::Pong, "违规连接不能影响健康连接");
    srv.graceful_shutdown();
}

/// 分帧边界：请求被拆成任意小块送去，服务端必须攒齐才应答。
#[test]
fn fragmented_request_is_reassembled() {
    let srv = Server::start("always");
    let mut raw = TcpStream::connect(&srv.addr).expect("connect");
    let mut w = treedb::engine::W::new();
    Op::CreateView { name: b"frag".to_vec() }.encode(&mut w);
    let frame = encode_frame(&w.finish());
    assert!(frame.len() > 6);

    // 先送 3 字节（连帧头都不全），再一字节一字节地喂。
    raw.write_all(&frame[..3]).expect("partial header");
    raw.flush().expect("flush");
    std::thread::sleep(std::time::Duration::from_millis(120));
    for b in &frame[3..] {
        raw.write_all(&[*b]).expect("byte");
        raw.flush().expect("flush byte");
    }

    let mut rx = RxBuf::with_capacity(256);
    let mut buf = [0u8; 256];
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Some(p) = rx.peek_len() {
            if rx.pending() >= p as usize + FRAME_HDR {
                break;
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("服务端没有把碎帧拼完");
        }
        let n = raw.read(&mut buf).expect("read");
        assert!(n > 0, "连接被提前关闭");
        rx.extend(&buf[..n]);
    }
    let payload = rx.take_frame().unwrap().unwrap();
    match Reply::decode(&mut treedb::engine::codec::Rd::new(&payload)).expect("decode") {
        Reply::View { id, root } => assert_eq!((id, root), (0, 1)),
        other => panic!("{other:?}"),
    }
    srv.graceful_shutdown();
}

/// `--stats` / `--shutdown` 两条运维路径 + checkpoint 真把 WAL 截短。
#[test]
fn ops_stats_checkpoint_shutdown_over_tcp() {
    let srv = Server::start("always");
    let mut c = srv.client();
    assert_eq!(srv.pid_from_file(), srv.pid(), "--pid-file 必须写对，运维靠它");

    let v = mk_view(&mut c, "ops");
    for k in 0..200 {
        create(&mut c, v, &format!("o{k}"));
    }
    let disk_len = std::fs::metadata(srv.dir().join("treedb.wal")).expect("wal exists").len();
    assert!(disk_len > 0);

    let wal_before = match ok(&mut c, &Op::Stats) {
        Reply::Stats(s) => {
            assert_eq!((s.views, s.nodes, s.dentries), (1, 201, 200));
            assert!(s.writes_total >= 201, "writes {}", s.writes_total);
            assert!(s.syncs_total > 0, "always 模式每条写都要 fsync");
            // checkpoint 阈值按 WAL 逻辑长度算，所以 stats 必须报逻辑长度。
            // 磁盘上那个文件因为预分配只会 >= 它（多出来的是没写过的空洞），
            // 反过来报磁盘长度就会让阈值永远晚一个粒度才触发。
            assert!(
                s.wal_bytes > 0 && s.wal_bytes <= disk_len,
                "stats.wal_bytes 要落在 (0, 磁盘长度 {disk_len}] 内，实得 {}",
                s.wal_bytes
            );
            // §11 #3：两个长度并列报出，磁盘那个必须就是 st_size（运维拿它算空间）。
            assert_eq!(s.wal_disk_bytes, disk_len, "wal_disk_bytes 就是 st_size");
            assert_eq!(s.snap_bytes, 0, "没 checkpoint 过不该有快照长度");
            assert_eq!((s.tables, s.rows), (0, 0), "没建过通用表");
            assert_eq!(s.proto_version, treedb::engine::PROTOCOL_VERSION, "协议版本要能隔着 TCP 查到");
            assert!(s.seq >= s.snap_seq, "seq 水位不能落后于快照");
            s.wal_bytes
        }
        other => panic!("{other:?}"),
    };

    assert_eq!(ok(&mut c, &Op::Checkpoint), Reply::Ok);
    // 快照落盘后日志被截成空文件：逻辑长度归零（磁盘上余下的全是预留空洞）。
    match ok(&mut c, &Op::Stats) {
        Reply::Stats(s) => {
            assert_eq!(s.wal_bytes, 0, "checkpoint 后 WAL 逻辑长度应归零（截断前 {wal_before}）");
            assert_eq!(
                s.snap_bytes,
                std::fs::metadata(srv.dir().join("state.snap")).unwrap().len(),
                "恢复时间 = 读快照 + 重放增量，两个数都得报"
            );
        }
        other => panic!("{other:?}"),
    }
    assert!(srv.dir().join("state.snap").exists(), "快照文件必须落盘");
    // checkpoint 之后仍可读写，且 seq 接着涨不回退。
    create(&mut c, v, "post-ckpt");
    match ok(&mut c, &Op::Stats) {
        Reply::Stats(s) => assert_eq!(s.nodes, 202),
        other => panic!("{other:?}"),
    }
    let snap_seq = match ok(&mut c, &Op::Stats) {
        Reply::Stats(s) => s.snap_seq,
        other => panic!("{other:?}"),
    };
    assert!(snap_seq > 0);
    // 停服走的是优雅路径：退出码 0。
    let st = srv.graceful_shutdown();
    assert!(st.status.success());
}

/// 端口复用：同一个地址上「起—停—再起」要能立刻重绑（TIME_WAIT 不挡道）。
#[test]
fn restart_on_same_addr_after_graceful_shutdown() {
    let srv = Server::start("always");
    let addr = srv.addr.clone();
    let mut c = srv.client();
    let v = mk_view(&mut c, "keepme");
    create(&mut c, v, "durable");
    drop(c);
    // `graceful_shutdown` 会把数据目录连同它的 TempDir 一起交回来：
    // 不把 tmp 握住，重启时目录早已被清空。
    let stopped = srv.graceful_shutdown();
    assert!(stopped.status.success());

    let srv2 = Server::restart_at_addr(stopped.tmp, stopped.dir, addr.clone(), "always");
    let mut c2 = srv2.client();
    match ok(&mut c2, &Op::ListViews) {
        Reply::Views(vv) => assert_eq!(vv.len(), 1, "重启后视图必须还在"),
        other => panic!("{other:?}"),
    }
    assert!(matches!(
        ok(&mut c2, &Op::Lookup { view: 0, parent: 1, name: b"durable".to_vec() }),
        Reply::Node(_)
    ));
    // 复用同地址重启后仍然可写。
    let v2 = mk_view(&mut c2, "second");
    create(&mut c2, v2, "post-restart");
    srv2.graceful_shutdown();
}

/// MAX_FRAME 是协议层的硬上限，必须与实现里的常量一致（写错一位就是
/// 客户端与服务端对帧大小的理解不同步）。
#[test]
fn max_frame_constant_is_sane() {
    assert_eq!(MAX_FRAME, 8 * 1024 * 1024);
    // 帧头 4 字节 + payload 不能溢出成小帧。
    let f = encode_frame(&[7u8; 5]);
    assert_eq!(f.len(), 9);
    assert_eq!(&f[..4], &5u32.to_le_bytes());
}

/// 通用表与 readdirplus 走真 TCP。它们在 `engine_crud` 里验的是语义，这里验
/// 的是那几条新 tag 的帧编解码、分页游标与大应答背压。
#[test]
fn generic_table_and_readdir_plus_over_tcp() {
    let srv = Server::start("group");
    let mut c = srv.client();
    let v = mk_view(&mut c, "tbl");

    let def = TableDef {
        name: b"t".to_vec(),
        columns: vec![
            ColumnDef { name: b"a".to_vec(), flags: COL_UNIQUE },
            ColumnDef { name: b"b".to_vec(), flags: COL_INDEX },
        ],
    };
    match ok(&mut c, &Op::TableCreate { def: def.clone() }) {
        Reply::Table(Some(x)) => assert_eq!(x, def, "def 要帧往返无损"),
        other => panic!("{other:?}"),
    }
    let val = |k: usize| Op::RowPut {
        table: b"t".to_vec(),
        key: format!("k{k:05}").into_bytes(),
        cols: vec![format!("a{k:05}").into_bytes(), if k % 2 == 0 { b"even".to_vec() } else { b"odd".to_vec() }],
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    };
    const N: usize = 1000;
    let mut ops: Vec<Op> = Vec::new();
    for k in 0..N {
        ops.push(val(k));
        if ops.len() == 200 {
            let reps = c.pipeline(&ops).expect("rowput pipeline");
            assert!(reps.iter().all(|r| matches!(r, Reply::Row(Some(_)))), "行写经网络失败");
            ops.clear();
        }
    }
    assert!(ops.is_empty(), "批量没切干净");

    // 唯一约束经网络也必须拦得住：客户端不能只信自己手里那份索引。
    match c.call(&Op::RowPut {
        table: b"t".to_vec(),
        key: b"dup".to_vec(),
        cols: vec![b"a00000".to_vec(), b"x".to_vec()],
        flags: 0,
        cond_col: COND_NONE,
        cond_val: Vec::new(),
    }) {
        Ok(Reply::Err { code: errno::EEXIST, .. }) => {}
        other => panic!("唯一约束经网络失效：{other:?}"),
    }
    // CAS 不命中：EAGAIN 是一条 errno，不是丢帧。
    match c.call(&Op::RowPut {
        table: b"t".to_vec(),
        key: b"k00001".to_vec(),
        cols: vec![b"zz".to_vec(), b"odd".to_vec()],
        flags: 0,
        cond_col: 0,
        cond_val: b"not-a00001".to_vec(),
    }) {
        Ok(Reply::Err { code: errno::EAGAIN, .. }) => {}
        other => panic!("CAS 条件经网络失效：{other:?}"),
    }

    // 主键游标翻页：每页 37 条翻到底，不重不漏（37 不整除 1000，尾页能推住）。
    let mut seen: Vec<String> = Vec::new();
    let mut from: Vec<u8> = Vec::new();
    loop {
        match ok(&mut c, &Op::RowList { table: b"t".to_vec(), column: ROW_PK, value: Vec::new(), from, limit: 37 }) {
            Reply::Rows { rows, end } => {
                if end {
                    assert!(rows.len() <= 37);
                } else {
                    assert_eq!(rows.len(), 37, "没到底的页必须装满");
                }
                let last = rows.last().map(|r| r.key.clone()).unwrap_or_default();
                for r in &rows {
                    let k = String::from_utf8_lossy(&r.key).into_owned();
                    assert!(!seen.contains(&k), "翻页经网络重复返回了 {k}");
                    seen.push(k);
                }
                if end {
                    break;
                }
                from = last;
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(seen.len(), N, "翻页经网络漏了行");
    assert_eq!(seen.first().unwrap(), "k00000");
    assert_eq!(seen.last().unwrap(), &format!("k{:05}", N - 1));
    // 索引列等值列举与计数。
    match ok(&mut c, &Op::RowCount { table: b"t".to_vec(), column: 1, value: b"even".to_vec() }) {
        Reply::Count(n) => assert_eq!(n as usize, N / 2),
        other => panic!("{other:?}"),
    }

    // 大应答：1000 行一页取完远大于两个套接字缓冲，客户端读侧必须自己接回来。
    let raw = TcpStream::connect(&srv.addr).expect("connect");
    shrink_rcvbuf(&raw, 8 * 1024);
    let mut slow = Client::from_stream(raw);
    let reps = slow
        .pipeline(&[Op::RowList {
            table: b"t".to_vec(), column: ROW_PK, value: Vec::new(), from: Vec::new(), limit: N as u32,
        }])
        .expect("big rowlist");
    match &reps[0] {
        Reply::Rows { rows, end } => {
            assert_eq!(rows.len(), N, "大应答被截断了");
            assert!(*end);
            assert_eq!(rows[0].cols, vec![b"a00000".to_vec(), b"even".to_vec()]);
        }
        other => panic!("expected Rows, got {other:?}"),
    }
    assert_eq!(call(&mut slow, &Op::Ping), Reply::Pong, "被背压拖过的连接之后还能干活");

    // readdirplus：与 readdir 同一页，只是属性随条目一起过线。
    let mut ops: Vec<Op> = Vec::new();
    for k in 0..500 {
        ops.push(Op::Create {
            view: v,
            parent: 1,
            name: format!("n{k:03}").into_bytes(),
            mode: 0o644,
            uid: 0,
            gid: 0,
        });
    }
    let reps = c.pipeline(&ops).expect("create pipeline");
    assert_eq!(reps.len(), 500);
    let plain = match ok(&mut c, &Op::ReadDir { view: v, parent: 1, offset: 0, limit: 500 }) {
        Reply::Dirents { inos, kinds, names, next, end } => (inos, kinds, names.to_vec(), next, end),
        other => panic!("{other:?}"),
    };
    let plus = match ok(&mut c, &Op::ReadDirPlus { view: v, parent: 1, offset: 0, limit: 500 }) {
        Reply::DirentsPlus { inos, kinds, names, attrs, next, end } => {
            assert_eq!(attrs.len(), inos.len(), "平行数组经网络错位了");
            (inos, kinds, names.to_vec(), attrs, next, end)
        }
        other => panic!("{other:?}"),
    };
    assert_eq!(plain.0, plus.0);
    assert_eq!(plain.1, plus.1);
    assert_eq!(plain.2, plus.2);
    assert_eq!((plain.3, plain.4), (plus.4, plus.5));
    assert!(
        plus.3.iter().all(|a| a.mode == 0o644 && a.uid == 0 && a.nlink == 1),
        "属性经网络丢了字段：{:?}",
        plus.3[0]
    );
    assert_eq!(plus.2[0], b"n000".to_vec());
    let _ = srv.graceful_shutdown();
}
