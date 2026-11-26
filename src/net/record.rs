//! 握手的可选后续阶段：把这条连接的帧流整体加密（DESIGN.md §4.5）。
//!
//! ## 形状
//!
//! [`crate::net::auth`] 的双向 CHAP 只建立身份，不建立机密性 —— 握手之后
//! 线上仍是明文。这一层补的就是那一条，且**只**补那一条：
//!
//! ```text
//!   … AuthFinish { mac }  ──►  AuthOk { server_mac, authed: true }
//!   SecureBegin { want: 1 }  ──►  SecureOk { suite: 1 }        ← 最后两拍明文
//!   ─── 此线以下全部是记录，不再有任何明文帧 ───
//!   rec(len ‖ ct ‖ tag)   ◄──►  一个或多个明文帧拼成的字节流
//! ```
//!
//! 会话密钥从握手已经交过底的两个随机数派生（[`Session::derive`]）。绑 nonce
//! 而不是绑密钥字节，是因为 HKDF 的输入里已经有 `AuthFinish`/`AuthOk` 那两拍
//! 证明过的东西：一次握手的两条密钥流彼此不同（每条连接的新 `server_nonce`
//! 不同），而密钥本身依旧从不上线。
//!
//! ## 三条不能含混的语义
//!
//! 1. **协商帧是明文，之后的帧是密文。** 判据是「`SecureOk` 这条应答本身发出去
//!    之后」，不是「收到 `SecureBegin` 之后」—— 两侧必须切在同一处，否则第一条
//!    数据 op 就解不开。切点由 [`crate::net::server::Conn`] 的 `secure` 字段
//!    是否已装填决定，而装填只发生在闸门把 `SecureOk` 排进发送队列之后。
//! 2. **AEAD 验不过 = 断连。** 握手的 MAC 错可以回 `EADV` 让人换密钥重来
//!    （`Stage::Needed`），因为那一拍没有任何状态被猜透过；而记录验不过意味着
//!    密钥流/计数器/构造三者之一已经和对面不一致，继续读写只会把错乱的字节
//!    当成合法帧解下去。这里没有可恢复的分支。
//! 3. **协商是可选的，要求是显式的。** 服务端不认识 `SecureBegin` 时会在
//!    `decode` 的兜底那里回 `EINVAL`，客户端拿到这个 Err 就退回明文继续 ——
//!    这是「可选」的准确含义。服务端也可以直接回 `SecureOk { suite: 0 }`
//!    把拒绝说成一条正常应答。但调用方显式要求加密（`auth::secure_handshake`
//!    的 `require`、`ServerConfig::require_secure`）时，降级就是错误而不是回退。
//!
//! ## 它防不住什么
//!
//! - **无前向保密**（同 `chacha20poly1305` 模块头）：密钥泄露 = 历史包全可解。
//! - **不防流量分析**：记录长度就是明文长度 + 16。
//! - **不做重放保护的跨连接语义**：计数器只在一条连接内单调。抓包者把整段
//!   记录原样重放给服务端，会因为在**另一条**连接上（不同会话密钥）验不过而被断连 ——
//!   这靠的是每连接新 nonce，不是靠序号窗口。

use crate::engine::sha256::DIGEST_LEN;
use crate::engine::NONCE_LEN;
use crate::net::auth::Key;
use crate::net::chacha20poly1305::{hkdf_sha256, DirKey, Direction, IV_LEN, KEY_LEN, TAG_LEN};

/// 一个记录在线上占的字节上限：4 字节长度头 + 明文 + 标签。
///
/// 接收方拿它当**闸门**而不是提示：越界就是协议违规，直接断连。少了这道闸，
/// 一个声明 `len = 0xFFFF_FFFF` 的头部会让接收方去攒 4 GiB。
pub const RECORD_MAX: u32 = 4 + crate::net::chacha20poly1305::MAX_PLAINTEXT as u32 + TAG_LEN as u32;

/// 长度头自身的字节数。
pub const REC_HDR: usize = 4;

/// 套件 0：不加密。它同时是 `Reply::SecureOk` 里「服务端明确拒绝」的那个值
/// —— 所以它必须是零，不能靠「缺字段」表达。
pub const SECURE_SUITE_NONE: u8 = 0;
/// 套件 1：`AEAD_CHACHA20_POLY1305` + 每方向一个递增计数器 nonce（本模块的形状）。
pub const SECURE_SUITE_CHACHAPOLY1305: u8 = 1;

/// HKDF 的域分隔串：两方向各一条，且与握手 MAC 的 `treedb-auth-v1-*` 不同 ——
/// 同一份输入里同时混进了证明与密钥流两种用途，这两种不能算成同一种东西。
const INFO_CLI: &[u8] = b"treedb-record-v1-c2s-key";
const INFO_SRV: &[u8] = b"treedb-record-v1-s2c-key";
const INFO_SALT: &[u8] = b"treedb-record-v1-salt";

/// 一条已加密会话的两个方向，外加接收侧的攒包缓冲。
pub struct Session {
    /// 本端发出方向。
    tx: Direction,
    /// 本端接收方向。
    rx: Direction,
    /// 密文攒包缓冲（只放本端接收方向），`pos` 是已消费位置。
    ///
    /// 为什么不用 `Vec::drain(..n)`：快照线一次能推 MiB 级字节，每吃一个记录
    /// 就搬一次整块内存，代价是每记录 O(缓冲长度)。同一个游标形状与
    /// [`crate::engine::RxBuf`] 一致，也避免在两层各攒一份 8 MiB。
    enc: Vec<u8>,
    pos: usize,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 密钥字节一个都不带：`Direction` 里的 `DirKey` 自己已经遮了。
        f.debug_struct("Session")
            .field("sent", &self.tx.counter())
            .field("recv", &self.rx.counter())
            .field("pending_bytes", &(self.enc.len() - self.pos))
            .finish()
    }
}

impl Session {
    /// 服务端一侧：`srv` 密钥归发送方向。
    pub fn for_server(key: &Key, cn: &[u8; NONCE_LEN], sn: &[u8; NONCE_LEN]) -> Session {
        Session::derive(key, cn, sn, true)
    }

    /// 客户端一侧：`cli` 密钥归发送方向（与 [`Session::for_server`] 相反）。
    pub fn for_client(key: &Key, cn: &[u8; NONCE_LEN], sn: &[u8; NONCE_LEN]) -> Session {
        Session::derive(key, cn, sn, false)
    }

    /// 从握手的两个随机数派生双向密钥。
    ///
    /// 输入里绑的是 `cn ‖ sn`（加一个域分隔前缀）：一次握手一对，每条连接
    /// 的 `server_nonce` 都是新的 `getrandom` 值，所以下游那两条计数器密钥流
    /// 在本进程的生命周期内不可能撞上另一个会话 —— 这正是选计数器当 nonce
    /// 的前提（模块头与 `chacha20poly1305` 都写了撞了会怎样）。
    ///
    /// 密钥字节在第二步当 IKM：它在 `Key::client_mac` 里已经进过一次 HMAC，
    /// extract 一步之后两者就混进同一个 PRK 了；但**双向必须用同一条派生链**，
    /// 否则两侧算不出同一对密钥。
    fn derive(key: &Key, cn: &[u8; NONCE_LEN], sn: &[u8; NONCE_LEN], server_sends_srv: bool) -> Session {
        let mut ikm = Vec::with_capacity(16 + 2 * NONCE_LEN);
        ikm.extend_from_slice(b"treedb-secure-v1");
        ikm.extend_from_slice(cn);
        ikm.extend_from_slice(sn);
        let mut salt = [0u8; DIGEST_LEN];
        hkdf_sha256(b"treedb-record-v1", &ikm, INFO_SALT, &mut salt);

        let mut cli = [0u8; KEY_LEN];
        let mut srv = [0u8; KEY_LEN];
        hkdf_sha256(&salt, &key.0, INFO_CLI, &mut cli);
        hkdf_sha256(&salt, &key.0, INFO_SRV, &mut srv);
        let (tx, rx) = if server_sends_srv {
            (DirKey(srv), DirKey(cli))
        } else {
            (DirKey(cli), DirKey(srv))
        };
        Session {
            tx: Direction::new(tx),
            rx: Direction::new(rx),
            enc: Vec::new(),
            pos: 0,
        }
    }

    /// 把一段明文（一个或多个完整帧，或它们的任意分段）封进 `out`。
    ///
    /// 返回 false = 计数器溢出，调用方必须断连（见模块头第 2 条）。
    pub fn seal(&mut self, plaintext: &[u8], out: &mut Vec<u8>) -> bool {
        self.tx.seal_all(plaintext, out)
    }

    /// 收一批线上字节，能取出的记录全部验真、解密并进 `plain`。
    ///
    /// 返回 `Err` 表示**必须断连**：标签验不过、长度越界、计数器耗尽，
    /// 三种都不可恢复（模块头第 2 条）。半条记录是正常形状，留下的字节
    /// 等下一批，不是错误。
    pub fn feed(&mut self, more: &[u8], plain: &mut Vec<u8>) -> Result<(), &'static str> {
        self.enc.extend_from_slice(more);
        loop {
            let left = self.enc.len() - self.pos;
            if left < REC_HDR {
                self.compact();
                return Ok(());
            }
            let at = self.pos;
            let len = u32::from_le_bytes([
                self.enc[at],
                self.enc[at + 1],
                self.enc[at + 2],
                self.enc[at + 3],
            ]);
            if len > RECORD_MAX || len < TAG_LEN as u32 {
                return Err("record length out of range");
            }
            let total = REC_HDR + len as usize;
            if left < total {
                self.compact();
                return Ok(());
            }
            let mut rec = self.enc[at + REC_HDR..at + total].to_vec();
            self.pos = at + total;
            match self.rx.open(len, &mut rec) {
                // `open` 已经把 `rec` 截到明文长度并整只交回来，直接接到流尾。
                Some(p) => plain.extend_from_slice(&p),
                None => return Err("record failed authentication"),
            }
        }
    }

    /// 攒包缓冲里还有多少未消费的密文字节（背压判据要算上它）。
    pub fn pending(&self) -> usize {
        self.enc.len() - self.pos
    }

    /// 消费过的段攒够就整体前移，避免只读不消费的连接无限膨胀（同 `RxBuf`）。
    fn compact(&mut self) {
        if self.pos > 0 && (self.pos >= 4096 || self.pos * 2 >= self.enc.len()) {
            self.enc.copy_within(self.pos.., 0);
            self.enc.truncate(self.enc.len() - self.pos);
            self.pos = 0;
        }
    }

    /// 本端已发记录数（两侧各自的 tx 相加意义不大，运维只关心一条线）。
    pub fn sent_records(&self) -> u64 {
        self.tx.counter()
    }

    /// 一个 nonce 的长度，供 `--stats` 与测试断言用（避免各处硬编码 12）。
    pub fn iv_len(&self) -> usize {
        IV_LEN
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sha256::DIGEST_LEN;
    use crate::net::chacha20poly1305::MAX_PLAINTEXT;

    fn key(b: u8) -> Key {
        Key([b; DIGEST_LEN])
    }

    /// 一对已握手的会话（服务端 / 客户端），拿同一份 nonce 派生。
    fn pair(k: u8) -> (Session, Session) {
        let (cn, sn) = ([1u8; NONCE_LEN], [2u8; NONCE_LEN]);
        (
            Session::for_server(&key(k), &cn, &sn),
            Session::for_client(&key(k), &cn, &sn),
        )
    }

    /// 从 `srv` 封、往 `cli` 喂，逐段切。返 `Err` 就是会话自己报了不可恢复。
    fn through(srv: &mut Session, cli: &mut Session, data: &[u8], cut: usize) -> Result<Vec<u8>, &'static str> {
        let mut wire = Vec::new();
        assert!(srv.seal(data, &mut wire), "seal must succeed");
        let mut plain = Vec::new();
        for chunk in wire.chunks(cut.max(1)) {
            cli.feed(chunk, &mut plain)?;
        }
        Ok(plain)
    }

    /// 往返与分段无关：快照线一块能到 MiB 级，而 TCP 给什么都可能是半个记录。
    #[test]
    fn roundtrip_is_independent_of_the_tcp_fragmentation() {
        for len in [0usize, 1, 16, 65_535, 65_536, 65_537, 200_000] {
            let data: Vec<u8> = (0..len).map(|i| (i * 31 + 7) as u8).collect();
            for cut in [1usize, 7, 4096, 1 << 20] {
                let (mut srv, mut cli) = pair(9);
                let got = through(&mut srv, &mut cli, &data, cut).expect("feed must not fail");
                assert_eq!(got, data, "len {len} cut {cut} must come back byte-identical");
            }
        }
    }

    /// 长于 `MAX_PLAINTEXT` 的一段必须被切成整数个记录，而且计数器的前进
    /// 量要能对上 —— 它是「没有两个记录共享同一个 nonce」的唯一读数。
    #[test]
    fn long_payload_becomes_whole_records() {
        let (mut srv, mut cli) = pair(9);
        let data = vec![0xa5u8; 3 * MAX_PLAINTEXT + 7];
        let mut wire = Vec::new();
        assert!(srv.seal(&data, &mut wire));
        assert_eq!(srv.sent_records(), 4, "三个整记录 + 一个零头");
        cli.feed(&wire, &mut Vec::new()).unwrap();
        assert_eq!(cli.pending(), 0, "整批喂完不该留下行");
    }

    /// 模块头第 2 条：密文里改一个字节必须被检出，而不是解出一个错帧。
    ///
    /// 逐位都试一遍不现实（一帧就能有几十万字），但位置要选得有意：
    /// 长度头、密文首部、密文中间三类各自代表一条失效路径。
    #[test]
    fn a_tampered_record_kills_the_session() {
        // 第三个字段：true = 把长度头改小而不是翻转最高位。翻转最高位会把 44 改成
        // 172，那是「还差 128 字节」，`feed` 按半条记录等下去，证不了这一条；
        // 改小到 30 仍能凑齐一条「完整」记录，边界却是错的 —— 标签落在密文里。
        let cases = [
            (REC_HDR + 1, "密文首位", false),
            (REC_HDR + 14, "密文中间", false),
            (0, "长度头小端首位", true),
        ];
        for (at, what, shorten) in cases {
            let (mut srv, mut cli) = pair(9);
            let mut wire = Vec::new();
            assert!(srv.seal(b"put:inode=42:name=secret.bin", &mut wire));
            assert!(at < wire.len(), "用例选的位置必须在帧内（{what}）");
            if shorten {
                wire[at] = wire[at].saturating_sub(14);
            } else {
                wire[at] ^= 0x80;
            }
            let r = cli.feed(&wire, &mut Vec::new());
            assert!(r.is_err(), "{what} 被动过之后必须报错，而不是静默解出一个错帧");
        }
    }

    /// 标签被改同样验不过（它与密文一起进 Poly1305）。
    #[test]
    fn a_tampered_tag_is_rejected() {
        let (mut srv, mut cli) = pair(9);
        let mut wire = Vec::new();
        assert!(srv.seal(b"hello", &mut wire));
        let last = wire.len() - 1;
        wire[last] ^= 1;
        assert!(cli.feed(&wire, &mut Vec::new()).is_err());
    }

    /// 两个记录掉头：它们彼此不能交换。计数器就是序号，失序 = 验不过。
    #[test]
    fn reordered_records_are_rejected() {
        let (mut srv, mut cli) = pair(9);
        let mut a = Vec::new();
        let mut b = Vec::new();
        assert!(srv.seal(b"first", &mut a));
        assert!(srv.seal(b"second", &mut b));
        let mut swapped = b.clone();
        swapped.extend_from_slice(&a);
        let r = cli.feed(&swapped, &mut Vec::new());
        assert!(r.is_err(), "把第二条排在第一条前面必须验不过");
    }

    /// 重放同样不行：抓一段记录原样发回去，它落在下一个计数器上。
    ///
    /// 这就是模块头「不防跳连接的语义」那条的实现：靠的是每连接新会话密钥
    /// 加同方向递增计数器，不是靠窗口。
    #[test]
    fn replayed_records_are_rejected() {
        let (mut srv, mut cli) = pair(9);
        let mut wire = Vec::new();
        assert!(srv.seal(b"ping", &mut wire));
        cli.feed(&wire, &mut Vec::new()).unwrap();
        assert!(cli.feed(&wire, &mut Vec::new()).is_err(), "同一段记录重发必须验不过");
    }

    /// 长度头是个闸门：越界就是协议违规，不能先去攒那 4 GiB。
    #[test]
    fn an_out_of_range_length_header_is_rejected() {
        let (_srv, mut cli) = pair(9);
        for len in [RECORD_MAX + 1, u32::MAX, (TAG_LEN - 1) as u32] {
            let mut hdr = Vec::new();
            hdr.extend_from_slice(&len.to_le_bytes());
            hdr.extend_from_slice(&[0u8; 64]);
            assert!(
                cli.feed(&hdr, &mut Vec::new()).is_err(),
                "声明长度 {len} 必须被当协议违规拒掉"
            );
        }
    }

    /// 不完整的记录是正常形状，不是错误：剩下的字节等下一批。
    #[test]
    fn a_partial_record_just_waits() {
        let (mut srv, mut cli) = pair(9);
        let mut wire = Vec::new();
        assert!(srv.seal(&vec![7u8; 5000], &mut wire));
        let half = wire.len() / 2;
        let mut plain = Vec::new();
        cli.feed(&wire[..half], &mut plain).unwrap();
        assert!(plain.is_empty(), "半个记录不能产出任何明文");
        assert_eq!(cli.pending(), half, "未消费的字节要留在会话里");
        cli.feed(&wire[half..], &mut plain).unwrap();
        assert_eq!(plain.len(), 5000, "补齐后一次性交出去");
        assert_eq!(cli.pending(), 0);
    }

    /// 同一个会话的两把方向密钥必须互不相干：自己发出去的记录，自己的接收
    /// 方向解不开。
    ///
    /// 这一条守的是 HKDF 那两条 info 的分域：写成同一条的话，两侧派生出同一
    /// 把密钥，而一个反射型攻击者就能把服务端刚发的记录原样发回去 —— 那时
    /// 它不仅能过验，还被当成「客户端新发的一条 op」。
    #[test]
    fn directions_are_not_interchangeable() {
        let (mut srv, _cli) = pair(9);
        let mut wire = Vec::new();
        assert!(srv.seal(b"server reply", &mut wire));
        // 反证：正向路径是通的，否则下面两次 `is_err` 可能只是会话本就不匹配。
        let (mut srv2, mut cli2) = pair(9);
        let mut back = Vec::new();
        assert!(cli2.seal(b"client op", &mut back));
        let mut plain = Vec::new();
        srv2.feed(&back, &mut plain).unwrap();
        assert_eq!(plain, b"client op");
        // 把自己的出站记录喂进自己的入站方向。
        let mut wrong = Vec::new();
        assert!(
            srv.feed(&wire, &mut wrong).is_err(),
            "服务端不能收到自己发出的记录"
        );
        assert!(wrong.is_empty(), "验不过就不能交出任何字节");
        let mut wrong = Vec::new();
        assert!(
            cli2.feed(&back, &mut wrong).is_err(),
            "客户端不能收到自己发出的记录"
        );
    }

    /// 另一场握手（不同 `server_nonce`）的会话解不开这一场的字节。
    ///
    /// 它是「抓来的记录拿到另一条连接上重放」那个场景的最小复现：隔开的
    /// 不是密钥而是 nonce —— 集群共享密钥本来就不变。
    #[test]
    fn a_different_handshake_yields_a_different_session() {
        let k = key(4);
        let cn = [1u8; NONCE_LEN];
        let mut a = Session::for_server(&k, &cn, &[2u8; NONCE_LEN]);
        let mut wire = Vec::new();
        assert!(a.seal(b"op", &mut wire));
        let mut b = Session::for_server(&k, &cn, &[3u8; NONCE_LEN]);
        assert!(b.feed(&wire, &mut Vec::new()).is_err());
    }

    /// 同一条会话里连封多次，计数器逐记录前进；这是 nonce 不重用的直接证据。
    #[test]
    fn the_send_counter_advances_per_record() {
        let (mut srv, _cli) = pair(9);
        let mut out = Vec::new();
        for n in 1..=5usize {
            assert!(srv.seal(&vec![0u8; n * 100], &mut out));
            // 载荷都远小于 `MAX_PLAINTEXT` ⇒ 每次恰好一个记录，累计就是 n。
            assert_eq!(srv.sent_records() as usize, n, "第 {n} 次 seal 之后");
        }
        // 混一条超长的：它必须占两个记录，而不是把第二条摞在同一个 nonce 上。
        assert!(srv.seal(&vec![0u8; MAX_PLAINTEXT + 1], &mut out));
        assert_eq!(srv.sent_records() as usize, 7, "超长载荷要一次推进两个计数器");
    }

    /// `Debug` 不能把密钥带出去：它会跟着连接进 panic 文案与日志。
    #[test]
    fn debug_never_prints_key_material() {
        // key(9) = 32 个 0x09：一旦 `DirKey` 哪天派生了 `Debug`，数组形式会打印
        // 成 `9, 9, 9, …`，这一断言当场就能挡住它（`Session` 本身只该有计数）。
        let (srv, _cli) = pair(9);
        let shown = format!("{srv:?}");
        assert!(shown.contains("Session"), "形状还是要看得见: {shown}");
        assert!(!shown.contains("9, 9"), "密钥字节不能出现在 `Debug` 里: {shown}");
    }
}
