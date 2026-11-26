//! 记录层与**独立实现**的交叉验证（DESIGN.md §4.5）。
//!
//! `src/net/chacha20poly1305.rs` 的 RFC 8439 向量钉的是 ChaCha20 与 Poly1305 两
//! 个零件，`src/net/record.rs` 的单测钉的是会话自己的自洽性。两者合起来仍然漏掉
//! 一类失效：**装配错**。长度头进不进 AAD、nonce 是 8 字节大端还是 4+8、HKDF 的
//! salt 用哪一层、`MAX_PLAINTEXT` 之外那条零长记录 —— 任何一个装错都能让本实现
//! 自己封自己解完全对得上，却与世界上任何其他实现不互通。那正是「升级窗口里旧
//! 客户端连不上新服务端」的形状，只在跨实现对照里才暴露。
//!
//! 所以下面这些常量不是手写的，是 `python3 benches/ximpl_chachapoly.py` 用
//! `cryptography`（独立的 ChaCha20-Poly1305 + HKDF-SHA256 实现）算出来冻在这里的。
//! 冻结而不是测试时现算：本仓库的测试不许依赖解释器与第三方包（依赖门只管发布
//! 构建，但「装不上的测试」同样等于没测）。
//!
//! 改了 `record.rs` 的派生链或记录形状就必须重跑那个脚本并同步这里的常量 —— 这条
//! 断言失败时的正确动作是**问自己为什么改了线上形状**，不是改常量。
//!
//! 跑法：`cargo test --test secure_channel`

use treedb::engine::sha256::DIGEST_LEN;
use treedb::engine::NONCE_LEN;
use treedb::net::auth::Key;
use treedb::net::chacha20poly1305::hkdf_sha256;
use treedb::net::record::Session;

// ---- 由 benches/ximpl_chachapoly.py 生成（python3 + cryptography 2.8）----
const KEY_HEX: &str = "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a";
const CN_HEX: &str = "030a11181f262d343b424950575e656c737a81888f969da4abb2b9c0c7ced5dc";
const SN_HEX: &str = "5b66717c87929da8b3bec9d4dfeaf5000b16212c37424d58636e79848f9aa5b0";
const K_CLI_HEX: &str = "f25ec2c4b874a74611a6f1c7da2545ab86cbc8a6522def02da7ea41b37b2e2e0";
const K_SRV_HEX: &str = "99687086056fbbb64f28e4add1f8f97ecedab9ff312a2ace0da35d553abf5356";
const SALT_HEX: &str = "fd5d3b11aeca7a7211d6d85e1693d43595c9d28f37c3493a2f96e4596a195258";
const K_CLI_64_HEX: &str = "f25ec2c4b874a74611a6f1c7da2545ab86cbc8a6522def02da7ea41b37b2e2e02c16e105245aea4d59c00e1b1b5274d24a03876ed829820f361c513c5f22fff2";

// SMALL: 4 段明文 -> 4 个记录, 313 B 线上字节
const SMALL_RECORDS: usize = 4;
const SMALL_PT0: &str = "07";
const SMALL_PT1: &str = "31506f8eadcceb0a29486786a5c4e30221405f7e9dbcdbfa19385776";
const SMALL_PT2: &str = "c8e70625446382a1c0dffe1d3c5b7a99b8d7f61534537291b0cfee0d2c4b6a89a8c7e60524436281a0bfdefd1c3b5a7998b7d6f51433527190afceed0c2b4a69";
const SMALL_PT3: &str = "032241607f9ebddcfb1a39587796b5d4f31231506f8eadcceb0a29486786a5c4e30221405f7e9dbcdbfa1938577695b4d3f211304f6e8daccbea0928476685a4c3e201203f5e7d9cbbdaf91837567594b3d2f1102f4e6d8cabcae90827466584a3c2e1001f3e5d7c9bbad9f81736557493b2d1f00f2e4d6c8baac9e80726456483a2c1e0ff1e3d5c7b9ab9d8";
const SMALL_WIRE: &str = "1100000043f2d4c672f1ae6e8fbada5958cf5bb4e52c0000009142133bbf4e365f6db78f0b08e07087a72b7911d1e3715fd50514f296de6ec4813b51db2bdb3bdaecee589750000000f7408db7d9d7c94f86aa9c291201d04f1a89fbaaa4bdd6240ae34bbe250ccc7ec5cfc0969be17e58ba12bce7279b4c3e37eb209d5c524a9489723b3489a5961d3cc84ccef455d3a20b33f31013fe33f19c000000d1439967eaebb98f378150cc9a48c057a1449cd8a04d99fbe0e51e033781a0d401adf463be8e598f5fc74279426bcd112cdf8d49bdb5de1d00465c1b4de5e85a6ddf1dcbb8b1ab897fd8cb266b433b4465239fdd4879eb149735142cb7ca13df3e3ebd4506566c94e3c66e094e798cacc4c532f145bb576f16352eac6ad03efb3a7428c918a2db0613b5ee8f2efc5912602c61fafcdc069e85c91f74";
const SMALL_CLI_WIRE: &str = "110000009b079714962d3641a2d22b7d30d9733ab72c0000003976df5751009f965909c57e8643bad5537b0dd76a87bc8d6a7aa448f2b739bd2e090f319bc53f155878f05150000000128eb3b0b93305ac3c9c364a7ca3e686a8f8d726f92ffd2613cabc2cd78806e4493d7c0b158faeb15ff0c95532b5d5dfe0f648e6b27e400c2060c1028e8c135fcc88aeb361b2f894626f48d5e55e8b0a9c000000e7201accfaedd001fe2900c5a5f225c1eff25adbc65ce962cd619e6f00b0b20443357040fd3d06b58a3fb53b63ed2dad8e33324be8aded0ce2ffe9a8fb15763ca73a63258fff2ca13322693fca606dbe4e4ff2e33666a68651b6e4a2117a05cd66c9742df50ffc927a91a0bedd36212a8a91217612463682892b1badc8ce4dea7971583d2db1d58f4bbb71ec6172cd263869ace8c9824aaefce62a3c";

// TAIL: 1 段明文 -> 1 个记录, 105 B 线上字节（85 字节 = 一个整块 + 21，钉零填充长度）
const TAIL_RECORDS: usize = 1;
const TAIL_PT0: &str = "0b2a496887a6c5e4032241607f9ebddcfb1a39587796b5d4f31231506f8eadcceb0a29486786a5c4e30221405f7e9dbcdbfa1938577695b4d3f211304f6e8daccbea0928476685a4c3e201203f5e7d9cbbdaf91837";
const TAIL_WIRE: &str = "650000004fa182b8fbfe59601abf0bd0bc998e9bae383aa98546656f389c7694e8df03a5301f12866e1ec161a4d660457b16a9dc25a770b7a6a97637ee5f308d556a9481277a135c7058d3aa3b8a7b9402011177169aae7a19dcc0f8c7a809c357a77b4ab455e232dc";
const TAIL_CLI_WIRE: &str = "65000000978f32728445de13172dbacc0c1f79c8e4b665c65d3c0e61837fb2af0cd256a9cf0caddb0335001be16d2fa3a4889497e9e3721336343185f6306d61b1fd0719183711b49607adc53c769973901d1a49df0c400a2c04fecd50df5f3a2111544ef9e564daae";

// MULTI: 2 段明文（64 KiB 与 64 KiB+1）-> 3 个记录, 131133 B。
// 太长，不整段贴进来：比 sha256 与记录数（这两条加起来与逐字节比同等强度）。
const MULTI_RECORDS: usize = 3;
const MULTI_PT0_SHA: &str = "1c08d58eda4e79bc57fecffb83d6466bdf89106c0759c8367bc4c548f4c17db4";
const MULTI_PT1_SHA: &str = "f7ee654e9f1f16cae8bd4afa7c101579fd0099ee89a01bbc4c5a02c4e344b63a";
const MULTI_WIRE_SHA: &str = "721b4fec9022e1179221477f836b9700376342ee10b40862885ee998e08adb89";
const MULTI_CLI_WIRE_SHA: &str = "a127e981e46ad52aa71232d2fb41437ecb3f92e497cb809a4ded2a1f84b1e98d";

/// 派生参数，逐字抄自 `src/net/record.rs` 与 `chacha20poly1305.rs`。
///
/// 这里**故意**重新写一遍而不是从 crate 里取常量：本文件要钉的就是「线上形状
/// 是这一套」。哪天 `record.rs` 改了域分隔串，取来的新值会让断言继续通过，那这
/// 份交叉验证就白写了。
const EXTRACT_SALT: &[u8] = b"treedb-record-v1";
const INFO_SALT: &[u8] = b"treedb-record-v1-salt";
const INFO_CLI: &[u8] = b"treedb-record-v1-c2s-key";
const INFO_SRV: &[u8] = b"treedb-record-v1-s2c-key";
const IKM_PREFIX: &[u8] = b"treedb-secure-v1";

fn hx(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
        .collect()
}

fn arr32(s: &str) -> [u8; 32] {
    let v = hx(s);
    assert_eq!(v.len(), 32, "向量长度");
    let mut a = [0u8; 32];
    a.copy_from_slice(&v);
    a
}

fn key() -> Key {
    Key(arr32(KEY_HEX))
}

/// (cn, sn)：`Session::for_*` 要的是引用，数组在这里现取。
fn nonces() -> ([u8; NONCE_LEN], [u8; NONCE_LEN]) {
    (arr32(CN_HEX), arr32(SN_HEX))
}

fn small_pts() -> Vec<Vec<u8>> {
    vec![hx(SMALL_PT0), hx(SMALL_PT1), hx(SMALL_PT2), hx(SMALL_PT3)]
}

fn multi_pts() -> Vec<Vec<u8>> {
    // 递推与 Python 侧 `pattern()` 一致：`(i * 31 + seed) & 0xff`。
    let one = |n: usize, seed: u8| -> Vec<u8> {
        (0..n).map(|i| ((i * 31) as u8).wrapping_add(seed)).collect()
    };
    vec![one(65_536, 5), one(65_537, 6)]
}

fn sha256_hex(data: &[u8]) -> String {
    // 引擎自带 SHA-256（`engine::sha256`），拿来摘要长向量正好，不必为了测试
    // 再引一个 crate。
    let d = treedb::engine::sha256::sha256(data);
    d.iter().map(|b| format!("{b:02x}")).collect()
}

/// HKDF-SHA256 的派生链逐段对表（含 64 字节输出那一格）。
#[test]
fn the_key_schedule_matches_an_independent_implementation() {
    let (cn, sn) = nonces();
    let mut ikm = IKM_PREFIX.to_vec();
    ikm.extend_from_slice(&cn);
    ikm.extend_from_slice(&sn);

    let mut salt = [0u8; DIGEST_LEN];
    hkdf_sha256(EXTRACT_SALT, &ikm, INFO_SALT, &mut salt);
    assert_eq!(hex(salt.as_slice()), SALT_HEX, "extract 层的 salt 不一致");

    let mut cli = [0u8; 32];
    let mut srv = [0u8; 32];
    hkdf_sha256(&salt, &key().0, INFO_CLI, &mut cli);
    hkdf_sha256(&salt, &key().0, INFO_SRV, &mut srv);
    assert_eq!(hex(cli.as_slice()), K_CLI_HEX, "c2s 方向密钥不一致");
    assert_eq!(hex(srv.as_slice()), K_SRV_HEX, "s2c 方向密钥不一致");
    assert_ne!(cli, srv, "两条 info 必须给出两把不同的密钥（分域）");

    // 64 字节 = T(1) ‖ T(2)：轮次字节写错时前 32 字节仍对，只有这里会炸。
    let mut long = [0u8; 64];
    hkdf_sha256(&salt, &key().0, INFO_CLI, &mut long);
    assert_eq!(hex(&long), K_CLI_64_HEX, "hkdf_expand 的多轮拼接不一致");
    assert_eq!(&long[..32], &cli[..], "T(1) 必须与 32 字节请求的输出相同");
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// 服务端方向：本实现封出的字节必须与独立实现**逐字节**相同。
///
/// 这一条同时钉住了 nonce 编码、AAD 取值、长度头的字节序与标签位置 —— 它们任
/// 一处不同，线上字节就不同。
#[test]
fn sealed_records_are_byte_identical_to_the_reference() {
    let (mut srv, _cli) = {
        let (cn, sn) = nonces();
        (
            Session::for_server(&key(), &cn, &sn),
            Session::for_client(&key(), &cn, &sn),
        )
    };
    let pts = small_pts();
    let mut wire = Vec::new();
    for p in &pts {
        assert!(srv.seal(p, &mut wire), "seal must succeed");
    }
    assert_eq!(srv.sent_records() as usize, SMALL_RECORDS, "记录数就是 nonce 序列");
    assert_eq!(hex(&wire), SMALL_WIRE, "服务端方向的线上字节与参照实现不同");

    // 85 字节 = 一个整块 + 21：零填充那段是 ChaCha20 最容易装配错的地方。
    let (cn, sn) = nonces();
    let mut s2 = Session::for_server(&key(), &cn, &sn);
    let mut wire = Vec::new();
    assert!(s2.seal(&hx(TAIL_PT0), &mut wire));
    assert_eq!(s2.sent_records() as usize, TAIL_RECORDS);
    assert_eq!(hex(&wire), TAIL_WIRE);
}

/// 客户端方向同一批明文必须换成另一把密钥、另一段字节（分域生效）。
#[test]
fn the_client_direction_uses_the_other_key() {
    let (cn, sn) = nonces();
    let mut cli = Session::for_client(&key(), &cn, &sn);
    let mut wire = Vec::new();
    for p in small_pts() {
        assert!(cli.seal(&p, &mut wire));
    }
    assert_eq!(hex(&wire), SMALL_CLI_WIRE);
    let mut t = Session::for_client(&key(), &cn, &sn);
    let mut wire = Vec::new();
    assert!(t.seal(&hx(TAIL_PT0), &mut wire));
    assert_eq!(hex(&wire), TAIL_CLI_WIRE);
    assert_ne!(SMALL_CLI_WIRE, SMALL_WIRE, "两方向同密钥的话参照实现早就报出不同了");
}

/// 独立实现封的字节，本实现要能解开并还原全部明文。
///
/// 方向要选对：`SMALL_WIRE` 是参照侧用 **s2c** 密钥封的，能解它的本实现一侧是
/// 客户端（它的接收方向正是 s2c 密钥）。
#[test]
fn the_implementation_opens_the_reference_stream() {
    let (cn, sn) = nonces();
    let mut cli = Session::for_client(&key(), &cn, &sn);
    let mut plain = Vec::new();
    cli.feed(&hx(SMALL_WIRE), &mut plain).expect("reference stream must verify");
    let want: Vec<u8> = small_pts().concat();
    assert_eq!(plain, want, "解出来的明文字节流必须与参照侧逐字节相同");
    assert_eq!(cli.pending(), 0, "整批喂完不该留下行");

    let mut cli = Session::for_client(&key(), &cn, &sn);
    let mut plain = Vec::new();
    cli.feed(&hx(TAIL_WIRE), &mut plain).expect("tail must verify");
    assert_eq!(plain, hx(TAIL_PT0));
}

/// 长载荷（> `MAX_PLAINTEXT`）：切成几个记录、拼回来是什么，两边也必须一致。
///
/// 长向量比摘要而不是比字节：131 KiB 的十六进制贴进源码会把这份表变成噪声，
/// 而 sha256 相等与逐字节相等在这里等价（要防的是形状不一致，不是哈希碰撞）。
#[test]
fn oversized_payloads_chunk_the_same_way() {
    let pts = multi_pts();
    for (i, p) in pts.iter().enumerate() {
        assert_eq!(sha256_hex(p), [MULTI_PT0_SHA, MULTI_PT1_SHA][i], "第 {i} 段明文本身与参照侧不同形");
    }
    let (cn, sn) = nonces();
    let mut srv = Session::for_server(&key(), &cn, &sn);
    let mut wire = Vec::new();
    for p in &pts {
        assert!(srv.seal(p, &mut wire));
    }
    assert_eq!(srv.sent_records() as usize, MULTI_RECORDS, "64 KiB + 64 KiB+1 必须是 3 个记录");
    assert_eq!(sha256_hex(&wire), MULTI_WIRE_SHA);
    // 长向量也要能反向解开（参照侧用 s2c 密钥封的 → 客户端一侧来解）。
    let mut cli = Session::for_client(&key(), &cn, &sn);
    let mut plain = Vec::new();
    cli.feed(&wire, &mut plain).expect("long stream must verify");
    assert_eq!(plain, pts.concat(), "跨记录拼接必须逐字节还原");

    let (cn, sn) = nonces();
    let mut cli = Session::for_client(&key(), &cn, &sn);
    let mut wire = Vec::new();
    for p in &pts {
        assert!(cli.seal(p, &mut wire));
    }
    assert_eq!(sha256_hex(&wire), MULTI_CLI_WIRE_SHA);
}

/// 按 `len:u32` 把一条完整的参照流切成记录，返回每条的 `(起点, 含头总长)`。
///
/// 这个走法本身也是信息：参照侧产出的字节能被「长度头 ‖ 密文 ‖ 标签」这套框架
/// 解释器整段走完（不留尾、不越界），说明冻结的向量和记录形状是自洽的。
fn records(wire: &[u8]) -> Vec<(usize, usize)> {
    let mut v = Vec::new();
    let mut off = 0;
    while off < wire.len() {
        assert!(off + 4 <= wire.len(), "偏移 {off} 处连长度头都放不下");
        let l = u32::from_le_bytes([wire[off], wire[off + 1], wire[off + 2], wire[off + 3]]) as usize;
        assert!(l >= 16, "偏移 {off} 处的记录长度 {l} 比标签还短");
        v.push((off, 4 + l));
        off += 4 + l;
    }
    v
}

/// 参照侧产出的**任何**一字节被改过，本实现都必须拒（而不是解出错明文）。
///
/// 这一条不是重复 `record.rs` 的单测：那里的密文是本实现自己封的，可能恰好与
/// 某个错误的装配同构；这里改的是别人算的字节。
///
/// 判据是「只有坏记录之前的记录能出去」，不是「一个字节都不许出去」：记录层是
/// 流式的，`feed` 一次喂进四个记录时，头三个验真无误的本来就该交付，第四个验
/// 不过才返回 `Err`（调用方随即断连）。要求全空会把这条正确的流语义判成失败。
#[test]
fn a_modified_reference_record_is_rejected() {
    let (cn, sn) = nonces();
    let wire = hx(SMALL_WIRE);
    let pts = small_pts();
    let recs = records(&wire);
    assert_eq!(recs.len(), SMALL_RECORDS, "参照流的记录数必须与表一致");
    assert_eq!(
        // 元组第二个数是「含 4 字节长度头的总长」，而长度头字段本身只含
        // 16 字节标签，所以明文长 = 总长 - 4 - 16。
        recs.iter().map(|(_, l)| l - 20).collect::<Vec<_>>(),
        pts.iter().map(|p| p.len()).collect::<Vec<_>>(),
        "每条记录的长度头必须与实际明文长度一致"
    );
    // 五个选位各有分工：0 = 首个记录的长度头，4/9 = 它的密文，20 = 它的标签末
    // 字节，312 = 整条流的最后一字节（= 第 4 个记录的标签末字节）。
    for at in [0usize, 4, 9, 20, 312] {
        let bad = recs
            .iter()
            .position(|(s, l)| at < s + l)
            .expect("选位要在帧内");
        let mut w = wire.clone();
        w[at] ^= 0x40;
        let mut cli = Session::for_client(&key(), &cn, &sn);
        let mut plain = Vec::new();
        let r = cli.feed(&w, &mut plain);
        assert!(r.is_err(), "偏移 {at}（第 {bad} 个记录）被动过，必须验不过");
        assert_eq!(
            plain,
            pts[..bad].concat(),
            "偏移 {at}：只能吐出坏记录之前那些验真过的明文，一个字节都不能多"
        );
    }
}
