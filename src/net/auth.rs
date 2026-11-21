//! 接入认证：一份集群共享密钥 + 双向挑战握手（DESIGN.md §4.4）。
//!
//! ## 它是什么形状
//!
//! 两次往返的 CHAP，跑在既有的帧协议上，**不是** TLS 的简化版：
//!
//! ```text
//! 客户端                                 服务端
//!   │  AuthBegin { client_nonce }          │
//!   ├───────────────────────────────────►  │ 生成 server_nonce（getrandom）
//!   │  Challenge { server_nonce, proto }   │
//!   │ ◄────────────────────────────────────┤
//!   │  AuthFinish { HMAC(key, cn‖sn) }     │ 常数时间比对自己的 HMAC
//!   ├───────────────────────────────────►  │
//!   │  AuthOk { HMAC(key, sn‖cn) }         │
//!   │ ◄────────────────────────────────────┤
//! ```
//!
//! 密钥**从不上线**，两个方向各自交出「我知道密钥」的证明；线路上抓到的
//! `AuthFinish` 换一条连接重放必然失效，因为服务端每次都发新的 `server_nonce`。
//!
//! ## 它在哪一层被处置
//!
//! **握手不进引擎**：它既不读状态也不写状态，所以由事件循环就地回答（单线程循环
//! 与多 worker 两条路径共用这里的 [`Gate`]）。这也意味着它不占提交线程，
//! 认证开着时 group commit 的合并度一字不变。
//!
//! ## 它防不住什么（写清楚而不是等使用者自己发现）
//!
//! - **不防窃听**：握手之后的每一个字节仍是明文。这条握手只建立身份，
//!   不建立机密性；要加密得另做（§11 第 4 条）。
//! - **不做授权**：过了握手的连接与今天一样拥有全部 52 个 op。它是
//!   「一把钥匙开整扇门」，不是租户模型。
//! - **不防字典攻击**：密钥必须是 32 字节随机值。从口令派生的话，抓一次
//!   握手就能离线试口令 —— [`Key::load`] 因此在长度上不留余地。
//! - **不证明「是当前那个主」**：它证明对端持有密钥。脑裂后旧主同样持有密钥，
//!   照样能应答握手；fencing token（v0.4.0，§6.6）让副本认得出被提掉权的旧主并
//!   拒收，但它不阻止旧主对自己的客户端继续服务。

use crate::engine::model::errno;
use crate::engine::sha256::{ct_eq, hmac_sha256, DIGEST_LEN};
use crate::engine::{Op, Reply, NONCE_LEN, PROTOCOL_VERSION};
use crate::error::{Error, Result};

/// MAC 输入的域分隔串：客户端方向与服务端方向各一条。
///
/// 两条不同、且两个随机数在输入里的顺序也相反 —— 否则一份证明可以原样
/// 搬到另一个方向去用。
const CTX_CLIENT: &[u8] = b"treedb-auth-v1-cli";
const CTX_SERVER: &[u8] = b"treedb-auth-v1-srv";

/// 一份集群共享密钥。
///
/// `Debug` 故意只印长度：这东西会跟着 `ServerConfig` 进日志与 panic 文案，
/// 把字节带出去等于把密钥写进运维日志。
#[derive(Clone, PartialEq, Eq)]
pub struct Key(pub [u8; DIGEST_LEN]);

impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Key({DIGEST_LEN} bytes, redacted)")
    }
}

impl Key {
    /// 从文件读密钥：**32 原始字节**，或 **64 个十六进制字符**。
    ///
    /// 尾随空白只在十六进制那一档忽略（编辑器与 `echo` 都会留一个 `\n`）；
    /// 32 原始字节那一档一个字节都不可剔，理由见下面的注释。
    ///
    /// 两种形状都给是因为运维现实：`head -c 32 /dev/urandom > key` 与
    /// `openssl rand -hex 32 > key` 都在被人用。但**长度只认这两个值** ——
    /// 口令派生（"把 'hunter2' 补齐 32 字节"）在这里直接拒绝，理由见本模块开头
    /// 「不防字典攻击」那一条。
    pub fn load(path: &std::path::Path) -> Result<Key> {
        let raw = std::fs::read(path).map_err(|e| Error::Config(format!("read {}: {e}", path.display())))?;
        // 先按「恰好 32 原始字节」判，而且**不修剪**，这两个动作的顺序不能倒。
        // 随机密钥的最后一个字节完全可能正好是个 ASCII 空白（四个候选值
        // ⇒ 每 64 把里有 1 把），先剥空白再比长度就会让 `head -c 32 /dev/urandom`
        // 隔三差五地交出一把「长度不对」的密钥，而运维看不出自己做了什么错事。
        if raw.len() == DIGEST_LEN {
            // 全是空白就不可能是随机密钥（概率是 4/256 的 32 次方），只能是有人
            // 交了一个空模板。它长度合法，所以上面那条「不剔空白」的规则保不住
            // 它，得单独拒 —— 否则等于把零强度密钥当有效密钥收进来。
            if raw.iter().all(|b| b.is_ascii_whitespace()) {
                return Err(Error::Config(format!(
                    "{}: auth key is all whitespace, not a random 32-byte key",
                    path.display()
                )));
            }
            let mut k = [0u8; DIGEST_LEN];
            k.copy_from_slice(&raw);
            return finish(path, k, false);
        }
        // 剩下的都是文本形状：尾随换行/空白是正常写法（编辑器与 shell 都会留），
        // 先剥掉再看长度。
        let end = raw
            .iter()
            .rposition(|b| !b.is_ascii_whitespace())
            .map(|i| i + 1)
            .unwrap_or(0);
        let body = &raw[..end];
        if body.len() == DIGEST_LEN {
            let mut k = [0u8; DIGEST_LEN];
            k.copy_from_slice(body);
            return finish(path, k, false);
        }
        if body.len() == DIGEST_LEN * 2 && body.iter().all(|b| b.is_ascii_hexdigit()) {
            let mut k = [0u8; DIGEST_LEN];
            for (i, pair) in body.chunks(2).enumerate() {
                let hi = (pair[0] as char).to_digit(16).expect("checked hexdigit");
                let lo = (pair[1] as char).to_digit(16).expect("checked hexdigit");
                k[i] = (hi << 4 | lo) as u8;
            }
            return finish(path, k, true);
        }
        Err(Error::Config(format!(
            "{}: auth key must be 32 raw bytes or 64 hex chars, got {end}",
            path.display()
        )))
    }

    /// 握手 MAC 的输入：域分隔串 ‖ 线协议版本 ‖ 两个随机数。
    ///
    /// 版本绑进 MAC 是免费的，但不做会留坑：同一份密钥在两个协议版本之间
    /// 不该能互相顶替（v5 的握手字段与 v6 的不是一套字节）。
    fn mac_input(ctx: &[u8], a: &[u8; NONCE_LEN], b: &[u8; NONCE_LEN]) -> Vec<u8> {
        let mut m = Vec::with_capacity(ctx.len() + 2 + 2 * NONCE_LEN);
        m.extend_from_slice(ctx);
        m.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
        m.extend_from_slice(a);
        m.extend_from_slice(b);
        m
    }

    /// 客户端该交回的答案。
    pub fn client_mac(&self, cn: &[u8; NONCE_LEN], sn: &[u8; NONCE_LEN]) -> [u8; DIGEST_LEN] {
        hmac_sha256(&self.0, &Self::mac_input(CTX_CLIENT, cn, sn))
    }

    /// 服务端该交回的答案（双向认证那一半）。两个随机数的顺序与上面相反。
    pub fn server_mac(&self, cn: &[u8; NONCE_LEN], sn: &[u8; NONCE_LEN]) -> [u8; DIGEST_LEN] {
        hmac_sha256(&self.0, &Self::mac_input(CTX_SERVER, sn, cn))
    }
}

/// 三种形状共用的最后一道检查。`hex` 只用来挑文案：全零的十六进制串几乎
/// 一定是没填的模板（`0000…`），而全零的 32 原始字节只能是有人手工写出来的，
/// 两种都要拦，但报错时该说清是哪一种。
fn finish(path: &std::path::Path, k: [u8; DIGEST_LEN], hex: bool) -> Result<Key> {
    if k.iter().all(|b| *b == 0) {
        return Err(Error::Config(format!(
            "{}: auth key is all zero ({})",
            path.display(),
            if hex { "hex" } else { "raw bytes" }
        )));
    }
    Ok(Key(k))
}

/// 一段密码学意义的随机数：`getrandom(2)`，不用 `/dev/urandom`（那要 fd，
/// 而握手发生在 accept 之后、每连接一次，不该多占 fd 也不该依赖文件系统）。
fn random32() -> Result<[u8; NONCE_LEN]> {
    let mut b = [0u8; NONCE_LEN];
    let rc = unsafe { libc::getrandom(b.as_mut_ptr().cast(), b.len(), 0) };
    if rc != b.len() as libc::ssize_t {
        let e = std::io::Error::last_os_error();
        return Err(Error::Io(format!("getrandom: {e}")));
    }
    Ok(b)
}

/// 这条 op 是不是握手 op（两条路径的分派都判它）。
pub fn is_handshake(op: &Op) -> bool {
    matches!(op, Op::AuthBegin { .. } | Op::AuthFinish { .. })
}

/// 一条连接的握手进度。放在 [`crate::net::server::Conn`] 里，每连接一份。
#[derive(Clone, Debug)]
pub enum Stage {
    /// 本实例没启用认证：任何 op 直通，握手也只是回一句「这里没事」。
    Off,
    /// 启用了认证，这条连接还没开始握手。
    Needed,
    /// 已发挑战，等 `AuthFinish`。两个随机数都得留着，MAC 是算给它们的。
    Awaiting { cn: [u8; NONCE_LEN], sn: [u8; NONCE_LEN] },
    /// 过了。
    Ready,
}

/// 服务端一侧的握手闸门。
#[derive(Debug)]
pub struct Gate {
    pub stage: Stage,
}

impl Gate {
    pub fn new(required: bool) -> Gate {
        Gate {
            stage: if required { Stage::Needed } else { Stage::Off },
        }
    }

    /// 这条连接现在能不能走数据面。
    pub fn allows_data(&self) -> bool {
        matches!(self.stage, Stage::Off | Stage::Ready)
    }

    /// 「没握手就发数据 op」的标准应答。两条事件循环路径共用这一条，
    /// 免得一边的文案与另一边的 errno 对不上。
    pub fn denied() -> Reply {
        Reply::err(
            errno::EADV,
            "authentication required: run the handshake first (§4.4)",
        )
    }

    /// 「数据 op 与握手挤在同一批帧里，且排在最后一条握手 op 之前」的标准应答。
    ///
    /// 多 worker 路径只有「整批就地答」与「整批交提交线程」两个出口，而
    /// 协议没有 request id —— 夹在最后一条握手 op 之前的数据 op 无论走哪个
    /// 出口都会把应答次序打乱。回 EAGAIN 是唯一不丢语义的选择：这条 op 还
    /// 根本没被碰过，重发一次即可。
    pub fn straddled() -> Reply {
        Reply::err(
            errno::EAGAIN,
            "op shares a batch with a handshake: retransmit it after the handshake (§4.4)",
        )
    }

    /// 处置一条已解码的 op。
    ///
    /// `Some(reply)` = 握手层已经把它答完了，调用方**只准**把帧推回连接、不得
    /// 再交给引擎；`None` = 放行。判据只有一条：`allows_data()`。
    pub fn on_op(&mut self, key: Option<&Key>, op: &Op) -> Option<Reply> {
        match op {
            Op::AuthBegin { client_nonce } => Some(self.begin(key, client_nonce)),
            Op::AuthFinish { mac } => Some(self.finish(key, mac)),
            _ => {
                if self.allows_data() {
                    None
                } else {
                    Some(Self::denied())
                }
            }
        }
    }

    fn begin(&mut self, key: Option<&Key>, cn: &[u8; NONCE_LEN]) -> Reply {
        let Some(_) = key else {
            // 服务端没配密钥：握手照答，一次往返结束。带密钥的客户端会自己
            // 把这个 `authed = false` 判成降级（见 [`handshake`]）。
            self.stage = Stage::Off;
            return Reply::AuthOk {
                server_mac: [0u8; DIGEST_LEN],
                authed: false,
            };
        };
        // 挑战值本身不掺密钥（它是要明文送出去的），所以这里不用 `key`；
        // 掺密钥的是下一步的 [`Gate::finish`]。
        match random32() {
            Ok(sn) => {
                self.stage = Stage::Awaiting { cn: *cn, sn };
                Reply::Challenge {
                    server_nonce: sn,
                    proto: PROTOCOL_VERSION,
                }
            }
            // 取不到随机数 = 这台机器本身出了问题。报 EAGAIN 让客户端重试，
            // 而不是静默放行 —— 也不能拿时间戳、fd 号那种可猜值当挑战值。
            Err(e) => {
                self.stage = Stage::Needed;
                Reply::err(errno::EAGAIN, format!("no entropy for handshake: {e}"))
            }
        }
    }

    fn finish(&mut self, key: Option<&Key>, mac: &[u8; DIGEST_LEN]) -> Reply {
        let (cn, sn) = match &self.stage {
            Stage::Awaiting { cn, sn } => (*cn, *sn),
            _ => {
                return Reply::err(
                    errno::EADV,
                    "AuthFinish without a pending challenge: send AuthBegin first (§4.4)",
                )
            }
        };
        let Some(key) = key else {
            // 走到这里只能是随机数曾经取不到（`begin` 里进的 Awaiting 必有密钥）。
            self.stage = Stage::Needed;
            return Reply::err(errno::EAGAIN, "no auth key configured");
        };
        if ct_eq(&key.client_mac(&cn, &sn), mac) {
            self.stage = Stage::Ready;
            Reply::AuthOk {
                server_mac: key.server_mac(&cn, &sn),
                authed: true,
            }
        } else {
            // MAC 错不关连接、也不锁死：回到「还没握手」，可以换个密钥重头再来。
            // 但这一拍的挑战值作废 —— 抓来的 MAC 想在下一条连接上重放，
            // 必须重新拿一次挑战，而那条连接的 server_nonce 不一样。
            self.stage = Stage::Needed;
            Reply::err(errno::EADV, "bad handshake mac")
        }
    }
}

/// 客户端一侧跑完整次握手。
///
/// `key = None` 时**一个字节都不发**：保持与旧客户端逐字一致的行为，这样
/// 「带认证的开关关掉」等于这条改动不存在。
///
/// 两处刻意 fail-closed：
/// - 服务端回了 `AuthOk { authed: false }` 而客户端带了密钥 → 报错。否则
///   一个中间人可以只回一句「这里没事」就把认证整个摘掉（降级攻击）。
/// - `Challenge.proto` 与本端不一致 → 报错。MAC 里也绑了版本、本来就会算错，
///   但"版本不符"比"MAC 不对"是准确得多的诊断信息。
pub fn handshake(c: &mut crate::net::client::Client, key: Option<&Key>) -> Result<()> {
    let Some(key) = key else {
        return Ok(());
    };
    let cn = random32()?;
    match c.call(&Op::AuthBegin { client_nonce: cn })? {
        Reply::Challenge { server_nonce, proto } => {
            if proto != PROTOCOL_VERSION {
                return Err(Error::Denied(format!(
                    "peer speaks protocol {proto}, this client {PROTOCOL_VERSION}"
                )));
            }
            let mac = key.client_mac(&cn, &server_nonce);
            match c.call(&Op::AuthFinish { mac })? {
                Reply::AuthOk { server_mac, authed: true } => {
                    if ct_eq(&key.server_mac(&cn, &server_nonce), &server_mac) {
                        Ok(())
                    } else {
                        Err(Error::Denied(
                            "peer failed the reverse proof: it does not hold this key (§4.4)"
                                .into(),
                        ))
                    }
                }
                Reply::AuthOk { authed: false, .. } => Err(Error::Denied(
                    "server has no auth key configured, but the client demanded one".into(),
                )),
                Reply::Err { code, msg } => Err(Error::Denied(format!(
                    "handshake rejected: errno {code}: {}",
                    String::from_utf8_lossy(&msg)
                ))),
                other => Err(Error::Denied(format!("unexpected handshake reply: {other:?}"))),
            }
        }
        Reply::AuthOk { authed: false, .. } => Err(Error::Denied(
            "server has no auth key configured, but the client demanded one".into(),
        )),
        Reply::Err { code, msg } => Err(Error::Denied(format!(
            "handshake refused: errno {code}: {} (a pre-v5 server does not know op tag 0x0090)",
            String::from_utf8_lossy(&msg)
        ))),
        other => Err(Error::Denied(format!(
            "unexpected reply to AuthBegin: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(b: u8) -> Key {
        Key([b; DIGEST_LEN])
    }

    #[test]
    fn macs_are_directional_and_nonce_bound() {
        let (k, cn, sn) = (key(1), [2u8; NONCE_LEN], [3u8; NONCE_LEN]);
        let (cm, sm) = (k.client_mac(&cn, &sn), k.server_mac(&cn, &sn));
        assert_ne!(cm, sm, "两个方向必须给出不同的证明");
        assert_ne!(cm, k.client_mac(&cn, &[4u8; NONCE_LEN]), "服务端随机数必须进 MAC");
        assert_ne!(cm, k.client_mac(&[5u8; NONCE_LEN], &sn), "客户端随机数必须进 MAC");
        assert_ne!(cm, key(9).client_mac(&cn, &sn), "换密钥必须换结果");
    }

    /// 闸门的四条语义：未握手拦数据、错 MAC 回到起点、对 MAC 放行、
    /// 抓来的 MAC 换连接重放不上。
    #[test]
    fn gate_gates() {
        let k = key(7);
        let mut g = Gate::new(true);
        assert!(!g.allows_data());
        // 没握手就想读数据：EADV，而且不是断连（客户端还能去握手）。
        match g.on_op(Some(&k), &Op::Ping) {
            Some(Reply::Err { code, .. }) => assert_eq!(code, errno::EADV),
            other => panic!("expected EADV, got {other:?}"),
        }
        // 没有待决挑战就交 MAC：同样 EADV，不能蒙混成 Ready。
        assert!(matches!(
            g.on_op(Some(&k), &Op::AuthFinish { mac: [0u8; DIGEST_LEN] }),
            Some(Reply::Err { .. })
        ));
        assert!(!g.allows_data());

        // 正常一轮。
        let cn = [0x11u8; NONCE_LEN];
        let sn = match g.on_op(Some(&k), &Op::AuthBegin { client_nonce: cn }) {
            Some(Reply::Challenge { server_nonce, proto }) => {
                assert_eq!(proto, PROTOCOL_VERSION);
                server_nonce
            }
            other => panic!("expected challenge, got {other:?}"),
        };
        // MAC 错：回到 Needed，挑战作废。
        assert!(matches!(
            g.on_op(Some(&k), &Op::AuthFinish { mac: [0u8; DIGEST_LEN] }),
            Some(Reply::Err { code, .. }) if code == errno::EADV
        ));
        assert!(matches!(g.stage, Stage::Needed));
        // 重放这一拍的 MAC 已经不可能了：必须重拿挑战，而 nonce 每次都新。
        assert!(matches!(
            g.on_op(Some(&k), &Op::AuthFinish { mac: k.client_mac(&cn, &sn) }),
            Some(Reply::Err { .. })
        ));

        let sn2 = match g.on_op(Some(&k), &Op::AuthBegin { client_nonce: cn }) {
            Some(Reply::Challenge { server_nonce, .. }) => server_nonce,
            other => panic!("expected challenge, got {other:?}"),
        };
        assert_ne!(sn, sn2, "两条挑战必须不同（否则抓一次就能重放）");
        assert!(matches!(
            g.on_op(Some(&k), &Op::AuthFinish { mac: k.client_mac(&cn, &sn2) }),
            Some(Reply::AuthOk { authed: true, .. })
        ));
        assert!(g.allows_data());
        assert!(g.on_op(Some(&k), &Op::Ping).is_none(), "放行后就该交给引擎");
    }

    /// 没启用认证时握手不是错误而是一次空转：这条是 v4 客户端之外唯一的
    /// 「总是握手」客户端能连通老形状服务端的依据。
    #[test]
    fn gate_without_key_is_open() {
        let mut g = Gate::new(false);
        assert!(g.allows_data());
        assert!(g.on_op(None, &Op::Ping).is_none());
        assert!(matches!(
            g.on_op(None, &Op::AuthBegin { client_nonce: [0u8; NONCE_LEN] }),
            Some(Reply::AuthOk { authed: false, .. })
        ));
        // 空转之后依然放行（`Off` 不是一个需要重新获得的档位）。
        assert!(g.on_op(None, &Op::Stats).is_none());
    }

    /// 密钥文件的拒绝形状：太短（口令）、非十六进制、全零模板。
    #[test]
    fn key_load_is_strict() {
        let dir = tempfile::tempdir().expect("tempdir");
        let p = |n: &str| dir.path().join(n);

        std::fs::write(p("raw"), [7u8; 32]).expect("write raw");
        assert_eq!(Key::load(&p("raw")).unwrap(), key(7));

        // 原始字节那一档**不能先剔空白**：0x0a / 0x20 / 0x09 / 0x0d 都是合法的
        // 密钥末尾字节，而随机密钥每 64 把就有 1 把落在它们上。这条是本轮
        // 被真密钥踩出来后才补的（一次 20 000 条建连的跑批里，服务端直接
        // 不肯起）。
        for tail in [0x0au8, 0x20, 0x09, 0x0d] {
            let mut b = [0x5cu8; 32];
            b[31] = tail;
            std::fs::write(p("rawtail"), b).expect("write raw tail");
            assert_eq!(
                Key::load(&p("rawtail")).unwrap().0[31],
                tail,
                "末尾是 {tail:#04x} 的 32 原始字节密钥必须原样收下"
            );
        }
        // 32 个空白字节那一把不放过：它长度合法（上面那条「不剔空白」的规则
        // 因此不能把它当普通文本处理），但强度为零，只能是空模板。
        std::fs::write(p("blank"), [b' '; 32]).expect("write blank");
        assert!(
            Key::load(&p("blank")).is_err(),
            "全空白的 32 字节不是密钥"
        );

        std::fs::write(p("hex"), b"ab".repeat(32)).expect("write hex");
        assert_eq!(Key::load(&p("hex")).unwrap().0[..2], [0xab, 0xab]);

        // 尾随换行是正常写法，必须容忍。
        std::fs::write(p("hexnl"), [b"ab".repeat(32).as_slice(), b"\n"].concat()).expect("write");
        assert!(Key::load(&p("hexnl")).is_ok());

        std::fs::write(p("short"), b"hunter2").expect("write short");
        assert!(Key::load(&p("short")).is_err(), "口令派生明确拒绝");
        std::fs::write(p("zeros"), [b'0'; 64]).expect("write zeros");
        assert!(Key::load(&p("zeros")).is_err(), "全零多半是没填的模板");
        std::fs::write(p("not_hex"), [b'z'; 64]).expect("write z");
        assert!(Key::load(&p("not_hex")).is_err());
        assert!(Key::load(&p("missing")).is_err());
    }

    /// `Debug` 不能把密钥字节带出去：它会跟着 `ServerConfig` 进日志。
    #[test]
    fn key_debug_does_not_leak_bytes() {
        let s = format!("{:?}", key(0xde));
        assert!(!s.contains("de"), "{s}");
        assert!(s.contains("redacted"), "{s}");
    }
}
