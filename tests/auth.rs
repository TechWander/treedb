//! 接入认证验收（DESIGN.md §4.4）。
//!
//! 这里只测**线协议上的握手形状**：MAC 的数学性质、闸门的五档语义、密钥文件
//! 的长度门禁都在 `net::auth` 与 `engine::sha256` 的单测里，不重复。
//!
//! 这些用例各自钉一件事（1 与 5 在同一条里：拦截与放行本来就是同一个闸门
//! 的两面，拆成两条只会多起四个服务端）：
//!
//! 1. 没握手的连接**一个数据 op 也拿不到**，两种 worker 形状都得拦（多 worker
//!    那条路径的握手是分在 head 段就地答的，最容易漏）；
//! 2. 错密钥被拒之后连接不锁死，换对密钥能重头握手，而**上一拍的 MAC 重放不上**；
//! 3. 带密钥的客户端遇到没配密钥的服务端必须**拒绝降级**，而不是"就当没事"；
//! 4. 没配密钥的服务端对认证请求回一次 `authed: false`，带密钥客户端必须拒绝降级；
//! 5. 版本协商后，开认证与不开认证的数据面都能正常读写；
//! 6. 握手 op 与数据 op 挤在同一批帧里时，**应答次序不能被打乱**；
//! 7. 不认证的两种 worker 形状也必须先协商版本，错版本不能放行；
//! 8. 客户端必须拒绝伪造服务端返回的错版本 `VersionOk`。
//!
//! 跑法：`cargo test --test auth`

#[path = "common/mod.rs"]
mod common;

use common::{test_key, Server};
use treedb::engine::model::errno;
use treedb::engine::sha256::DIGEST_LEN;
use treedb::engine::{Op, Reply, NONCE_LEN, PROTOCOL_VERSION};
use treedb::net::auth::Key;
use treedb::net::client::Client;

fn err_code(r: &Reply) -> Option<u16> {
    match r {
        Reply::Err { code, .. } => Some(*code),
        _ => None,
    }
}

/// 1 + 5：未握手的连接拿不到任何东西；握完之后写读照常。
///
/// 两种 worker 形状各跑一遍是有理由的：`workers = 1` 走单线程循环里那条
/// 逐条拦在引擎之前的 `if`，`workers > 1` 走 worker 里按「最后一条握手 op」
/// 切 head/tail 的那段。两条路径的拦截点不是同一处代码。
#[test]
fn data_ops_need_a_handshake_on_both_worker_shapes() {
    for workers in [1usize, 8] {
        let srv = Server::start_auth("group", workers);

        // 故意不协商版本也不认证：正常 `connect` 现在会先协商版本。
        let mut raw = Client::connect_raw(&srv.addr, 3000).expect("connect raw");
        for op in [
            Op::Ping,
            Op::ListViews,
            Op::Stats,
            Op::CreateView { name: b"nope".to_vec() },
        ] {
            match raw.call(&op) {
                Ok(Reply::Err { code, msg }) => {
                    assert_eq!(
                        code,
                        errno::EADV,
                        "workers={workers} 的 {op:?} 拿到了 {code}，不是 EADV"
                    );
                    assert!(
                        String::from_utf8_lossy(&msg).contains("§4.4"),
                        "拒绝文案得指到文档那一节：{}",
                        String::from_utf8_lossy(&msg)
                    );
                }
                Ok(other) => panic!("workers={workers} 未握手却拿到了 {other:?}"),
                Err(e) => panic!("workers={workers} 未握手不该断连：{e}"),
            }
        }

        // 同一条连接上补一次握手，之前被拦的那个 op 就该正常答。
        let key = srv.auth_key().expect("auth server has a key").clone();
        let mut c = Client::connect_auth(&srv.addr, 0, Some(&key)).expect("handshake");
        assert!(matches!(c.call(&Op::Ping), Ok(Reply::Pong)));
        let created = c
            .call(&Op::CreateView { name: format!("v{workers}").into_bytes() })
            .expect("call");
        match created {
            Reply::View { id, root } => {
                assert_eq!(root, 1);
                let got = c
                    .call(&Op::Create {
                        view: id,
                        parent: 1,
                        name: b"f".to_vec(),
                        mode: 0o644,
                        uid: 1,
                        gid: 1,
                    })
                    .expect("create");
                assert!(matches!(got, Reply::Node(_)), "写路径要通：{got:?}");
                let back = c
                    .call(&Op::Lookup { view: id, parent: 1, name: b"f".to_vec() })
                    .expect("lookup");
                assert!(matches!(back, Reply::Node(_)), "读路径要通：{back:?}");
            }
            other => panic!("expected View, got {other:?}"),
        }
    }
}

/// 未开认证也必须先协商版本；两种 worker 形状各跑一遍。
#[test]
fn an_open_server_requires_version_before_data_on_both_worker_shapes() {
    for workers in [1usize, 8] {
        let srv = Server::start_workers("group", workers);
        let mut raw = Client::connect_raw(&srv.addr, 3000).expect("connect raw");
        for op in [Op::Ping, Op::Stats, Op::ListViews] {
            match raw.call(&op).expect("raw call") {
                Reply::Err { code, msg } => {
                    assert_eq!(code, errno::EADV, "workers={workers}");
                    assert!(
                        String::from_utf8_lossy(&msg).contains("VersionBegin"),
                        "workers={workers} 的拒绝文案没点明版本协商：{}",
                        String::from_utf8_lossy(&msg)
                    );
                }
                other => panic!("workers={workers} 未协商版本却拿到了 {other:?}"),
            }
        }
        assert!(matches!(
            raw.call(&Op::VersionBegin {
                client_proto: PROTOCOL_VERSION,
            }),
            Ok(Reply::VersionOk { server_proto }) if server_proto == PROTOCOL_VERSION
        ));
        assert!(matches!(raw.call(&Op::Ping), Ok(Reply::Pong)));
        match raw.call(&Op::Stats).expect("stats") {
            Reply::Stats(s) => assert_eq!(s.proto_version, PROTOCOL_VERSION),
            other => panic!("同代非认证连接应能读 Stats：{other:?}"),
        }
    }
}

/// 错版本必须被明确拒绝，且拒绝后连接仍在 `VersionNeeded`。
#[test]
fn a_protocol_mismatch_is_rejected_on_both_worker_shapes() {
    let wrong = PROTOCOL_VERSION + 1;
    for workers in [1usize, 8] {
        let srv = Server::start_workers("group", workers);
        let mut raw = Client::connect_raw(&srv.addr, 3000).expect("connect raw");
        match raw
            .call(&Op::VersionBegin { client_proto: wrong })
            .expect("wrong version")
        {
            Reply::Err { code, msg } => {
                assert_eq!(code, errno::EADV, "workers={workers}");
                let msg = String::from_utf8_lossy(&msg);
                assert!(msg.contains(&format!("client {wrong}")), "{msg}");
                assert!(msg.contains(&format!("server {PROTOCOL_VERSION}")), "{msg}");
            }
            other => panic!("workers={workers} 错版本不该被接受：{other:?}"),
        }
        assert!(matches!(
            raw.call(&Op::Ping),
            Ok(Reply::Err { code, .. }) if code == errno::EADV
        ));
        // 错版本不锁死连接：改回同代仍可协商并放行。
        assert!(matches!(
            raw.call(&Op::VersionBegin {
                client_proto: PROTOCOL_VERSION,
            }),
            Ok(Reply::VersionOk { server_proto }) if server_proto == PROTOCOL_VERSION
        ));
        assert!(matches!(raw.call(&Op::Ping), Ok(Reply::Pong)));
    }
}

/// 客户端也必须核对 VersionOk：错版本应答不能静默降级成未协商连接。
#[test]
fn a_client_refuses_a_mismatched_version_reply() {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake server");
    let addr = listener.local_addr().expect("addr").to_string();
    let wrong = PROTOCOL_VERSION + 1;
    let peer = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept");
        let mut hdr = [0u8; 4];
        stream.read_exact(&mut hdr).expect("read frame header");
        let len = u32::from_le_bytes(hdr) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).expect("read VersionBegin");
        assert!(matches!(
            treedb::engine::Op::decode(&mut treedb::engine::Rd::new(&payload)),
            Ok(Op::VersionBegin { client_proto }) if client_proto == PROTOCOL_VERSION
        ));
        let mut w = treedb::engine::W::new();
        Reply::VersionOk {
            server_proto: wrong,
        }
        .encode(&mut w);
        stream
            .write_all(&treedb::engine::encode_frame(&w.finish()))
            .expect("write wrong VersionOk");
    });
    let err = match Client::connect(&addr, 3000) {
        Ok(_) => panic!("错版本应答不应让建连成功"),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        msg.contains(&format!("peer speaks protocol {wrong}")),
        "错误没点明对端版本：{msg}"
    );
    assert!(msg.contains(&format!("this client {PROTOCOL_VERSION}")), "{msg}");
    peer.join().expect("fake server join");
}

/// 2：错密钥 → 被拒但不锁死；重拿挑战后旧 MAC 重放不上；换对密钥能过。
#[test]
fn wrong_key_is_rejected_and_a_replay_does_not_work() {
    let srv = Server::start_auth("group", 4);
    let right = srv.auth_key().expect("key").clone();
    let wrong = Key([0x11u8; DIGEST_LEN]);
    let mut c = Client::connect(&srv.addr, 3000).expect("connect");

    // 用错密钥认真走一遍：服务端必须算出不一样的 MAC。
    let cn = [0xa1u8; NONCE_LEN];
    let sn = match c.call(&Op::AuthBegin { client_nonce: cn }).expect("begin") {
        Reply::Challenge { server_nonce, .. } => server_nonce,
        other => panic!("expected Challenge, got {other:?}"),
    };
    let bad_mac = wrong.client_mac(&cn, &sn);
    match c.call(&Op::AuthFinish { mac: bad_mac }).expect("finish") {
        Reply::Err { code, msg } => {
            assert_eq!(code, errno::EADV);
            assert!(
                String::from_utf8_lossy(&msg).contains("bad handshake mac"),
                "{}",
                String::from_utf8_lossy(&msg)
            );
        }
        other => panic!("错的 MAC 不该被接受：{other:?}"),
    }

    // 被拒不等于断连，也不等于锁死：这条连接还能继续说话。
    assert!(matches!(
        c.call(&Op::Ping),
        Ok(Reply::Err { code, .. }) if code == errno::EADV
    ));

    // 重拿一次挑战。上一拍的 MAC 此刻必须失效：它算给旧的 `server_nonce`，
    // 而新挑战的随机数不一样 —— 这正是"抓一次线路也带不到下一条连接"的形状。
    let sn2 = match c.call(&Op::AuthBegin { client_nonce: cn }).expect("begin 2") {
        Reply::Challenge { server_nonce, .. } => server_nonce,
        other => panic!("expected Challenge, got {other:?}"),
    };
    assert_ne!(sn, sn2, "两次挑战的随机数必须不同");
    assert!(
        matches!(
            c.call(&Op::AuthFinish { mac: bad_mac }).expect("replay"),
            Reply::Err { code, .. } if code == errno::EADV
        ),
        "上一拍的 MAC 被重放接受了"
    );
    // 连"用对密钥但算给旧挑战"的 MAC 也不能过 —— 绑的是这一拍的随机数。
    let stale = right.client_mac(&cn, &sn);
    assert!(
        matches!(
            c.call(&Op::AuthFinish { mac: stale }).expect("stale"),
            Reply::Err { code, .. } if code == errno::EADV
        ),
        "算给旧挑战的 MAC 被接受了"
    );

    // 第三次：拿当前挑战 + 对密钥，必须过，而且过完就能读数据。
    let (cn3, sn3) = match c.call(&Op::AuthBegin { client_nonce: [0xb7; NONCE_LEN] }).expect("3") {
        Reply::Challenge { server_nonce, .. } => ([0xb7u8; NONCE_LEN], server_nonce),
        other => panic!("expected Challenge, got {other:?}"),
    };
    match c
        .call(&Op::AuthFinish { mac: right.client_mac(&cn3, &sn3) })
        .expect("finish 3")
    {
        Reply::AuthOk { server_mac, authed } => {
            assert!(authed);
            // 双向认证的另一半：客户端也该能验掉服务端。
            assert_eq!(server_mac, right.server_mac(&cn3, &sn3));
        }
        other => panic!("expected AuthOk, got {other:?}"),
    }
    assert!(matches!(c.call(&Op::Ping), Ok(Reply::Pong)));
}

/// 3：带密钥的客户端遇到没配密钥的服务端 —— 拒绝降级，不是"算了先用着"。
#[test]
fn a_keyed_client_refuses_to_downgrade_against_an_open_server() {
    let srv = Server::start_workers("group", 4);
    let key = test_key();
    let e = match Client::connect_auth(&srv.addr, 3000, Some(&key)) {
        Ok(_) => panic!("带密钥的客户端绝不该在未认证的服务端上握手成功"),
        Err(e) => e,
    };
    let msg = e.to_string();
    assert!(
        msg.contains("no auth key configured"),
        "错误要说清是「对面没开认证」：{msg}"
    );
    // 同地址不带密钥照样能用：被拒的是这次握手，不是这个端口。
    assert!(Client::ping(&srv.addr), "未认证的客户端不该被牵连");
}

/// 4：没配密钥的服务端把握手当一次空转 —— 一次往返、明确回 `authed: false`。
///
/// 这条是"v5 客户端连得上 v4 形状的服务端"的依据：握手 op 不是错误，
/// 老服务端答得来，只是答"这里没事"。
#[test]
fn an_open_server_answers_a_handshake_with_one_round_trip() {
    let srv = Server::start_workers("group", 4);
    let mut c = Client::connect(&srv.addr, 3000).expect("connect");
    match c.call(&Op::AuthBegin { client_nonce: [0x33; NONCE_LEN] }).expect("begin") {
        Reply::AuthOk { authed: false, .. } => {}
        other => panic!("未启用认证的服务端该回 authed:false，拿到 {other:?}"),
    }
    // 空转不改变任何东西：数据面照旧。
    assert!(matches!(c.call(&Op::Ping), Ok(Reply::Pong)));
}

/// 版本协商、认证与数据 op 挤在同一批帧里时，**应答次序不能被打乱**。
///
/// 协议没有 request id，`out` 是严格 FIFO：多 worker 路径因此把最后一条握手
/// op 之前的数据 op 就地回掉，只路由其后的数据。这里钉住三帧顺序：未协商的
/// Ping 被拒、VersionBegin 回 VersionOk、AuthBegin 回 Challenge。
#[test]
fn a_straddled_batch_keeps_the_reply_order() {
    let srv = Server::start_auth("group", 8);
    let mut c = Client::connect_raw(&srv.addr, 3000).expect("connect raw");
    let cn = [0x44u8; NONCE_LEN];
    let rs = c
        .pipeline(&[
            Op::Ping,
            Op::VersionBegin {
                client_proto: PROTOCOL_VERSION,
            },
            Op::AuthBegin { client_nonce: cn },
        ])
        .expect("pipeline");
    assert_eq!(rs.len(), 3);
    assert_eq!(err_code(&rs[0]), Some(errno::EADV), "握手前的 Ping 必须被拒");
    assert!(
        matches!(&rs[1], Reply::VersionOk { server_proto } if *server_proto == PROTOCOL_VERSION),
        "第二条应答必须是 VersionOk：{:?}",
        rs[1]
    );
    let sn = match &rs[2] {
        Reply::Challenge { server_nonce, proto } => {
            assert_eq!(*proto, treedb::engine::PROTOCOL_VERSION);
            *server_nonce
        }
        other => panic!("第三条应答必须是这一批里那条 AuthBegin 的挑战：{other:?}"),
    };

    let key = srv.auth_key().expect("key").clone();
    match c.call(&Op::AuthFinish { mac: key.client_mac(&cn, &sn) }).expect("finish") {
        Reply::AuthOk { authed: true, .. } => {}
        other => panic!("expected AuthOk, got {other:?}"),
    }
    // 那个被就地拒掉的 Ping：现在原样重发，必须正常答。
    assert!(matches!(c.call(&Op::Ping), Ok(Reply::Pong)));
}
