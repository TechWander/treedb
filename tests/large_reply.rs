//! 回归：**应答比 socket 缓冲区大**时，多 worker 路径必须把剩下的字节写完。
//!
//! 两条路的分叉在这里：`workers = 1` 走 `serve_single`，每条事件的尾巴无条件
//! 经过「EPOLLOUT 就写」；`workers > 1` 走 `multi::worker_loop`，那条尾巴在两个
//! 位置都要经过 —— 正常事件处理的末尾，以及「本轮没有可读字节」的分支。
//!
//! 曾经后者直接 `continue`：`EPOLLOUT` 事件本来就不带可读字节，于是整类事件被
//! 吃掉，用户态剩下的半个回包再也发不出去；又因为 epoll 是电平触发的，同一个
//! `EPOLLOUT` 每轮都会回来，事件循环跟着空转。同机 loopback 上缓冲区经常一次
//! 吃得下整帧，所以这条路径长期没被走过；跨机拉 1 MiB 快照块时副本只能收到前
//! ~110 KiB，然后一路读到 `--timeout-ms` 超时。
//!
//! 这里不靠网络形状：**连着发四个满块请求、发完才开始读**，四份 4 MiB 的应答
//! 一共 16 MiB，必然超过内核发送缓冲区（`net.ipv4.tcp_wmem` 上限默认 4 MiB），
//! 所以 `write` 一定会短写返回 `WouldBlock`，剩下的字节只能靠 `EPOLLOUT` 续写。
//!
//! 跑法：`cargo test --test large_reply`

mod common;

use std::net::TcpStream;
use std::time::Duration;
use common::Server;
use treedb::engine::model::{ColumnDef, Op, Reply, COND_NONE, SNAP_CHUNK_MAX};
use treedb::net::client::Client;

/// 每行的值宽度：64 KiB，够大但不是巨型对象（帧上限 8 MiB）。
const VALUE: usize = 64 * 1024;
/// 行数：快照必须显著大于 `SNAP_CHUNK_MAX`，否则要不到满块。
const ROWS: usize = 80;

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

/// 带读超时的连接：真回归时用例应当**失败**，而不是把 CI 挂死。
fn timed_client(addr: &str) -> Client {
    let s = TcpStream::connect(addr).expect("connect");
    // 真回归时是**永久**卡住，不是慢：8 s 足够把「没写完」与「写得慢」分开。
    s.set_read_timeout(Some(Duration::from_secs(8))).unwrap();
    s.set_write_timeout(Some(Duration::from_secs(8))).unwrap();
    Client::from_stream_auth(s, None).expect("手写连接上的握手")
}

#[test]
fn full_chunk_crosses_a_full_socket() {
    // workers = 4：只有多前端那条路径有「空 ops 提前 continue」的分支。
    let s = Server::start_workers("group", 4);
    let mut c = s.client();
    ok(
        &mut c,
        &Op::TableCreate {
            def: treedb::engine::model::TableDef {
                name: b"blobs".to_vec(),
                columns: vec![ColumnDef {
                    name: b"v".to_vec(),
                    flags: 0,
                }],
            },
        },
    );
    let val = vec![b'x'; VALUE];
    for i in 0..ROWS {
        ok(
            &mut c,
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
    // 快照要落在盘上才拉得到：SnapFetch 读的是 `state.snap`。
    ok(&mut c, &Op::Checkpoint);
    drop(c);

    // 新连接：sndbuf 从默认初值起步，4 MiB 的应答一定装不下。
    let mut c2 = timed_client(&s.addr);
    let total = match ok(
        &mut c2,
        &Op::SnapFetch {
            offset: 0,
            len: SNAP_CHUNK_MAX,
        },
    ) {
        Reply::SnapChunk { total, .. } => total,
        other => panic!("SnapFetch 应回 SnapChunk，得到 {other:?}"),
    };
    assert!(
        total > SNAP_CHUNK_MAX as u64,
        "快照只有 {total} B，这一格没测到大回包（预期 > {SNAP_CHUNK_MAX} B）"
    );

    // 同一块再取一次，确认上一条连接的状态没有把续写能力搞坏。
    match ok(
        &mut c2,
        &Op::SnapFetch {
            offset: SNAP_CHUNK_MAX as u64,
            len: SNAP_CHUNK_MAX,
        },
    ) {
        Reply::SnapChunk { bytes, .. } => assert!(!bytes.is_empty(), "第二块不该为空"),
        other => panic!("{other:?}"),
    }

    // 关键一格：四个满块请求**一起发出去**，读完才轮到客户端收。
    // 16 MiB 的应答必须排进同一连接的待发队列，`flush` 中途一定撞上 `WouldBlock`
    // —— 能不能把剩下的写完，就看 `EPOLLOUT` 那条路通不通。
    let ops = vec![
        Op::SnapFetch {
            offset: 0,
            len: SNAP_CHUNK_MAX,
        };
        4
    ];
    let replies = c2.pipeline(&ops).expect("四个满块请求的往返");
    assert_eq!(replies.len(), ops.len());
    for (i, r) in replies.iter().enumerate() {
        match r {
            Reply::SnapChunk { bytes, .. } => assert_eq!(
                bytes.len() as u32,
                SNAP_CHUNK_MAX,
                "第 {i} 块应当满块，实得 {} B",
                bytes.len()
            ),
            other => panic!("第 {i} 块得到 {other:?}"),
        }
    }

    // 收尾要干净：快照写完后 wal 仍可推进（避免只测了读而留下半死状态）。
    ok(&mut c2, &Op::Ping);
}
