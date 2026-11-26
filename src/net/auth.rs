//! 接入认证：协议版本协商 + 一份集群共享密钥 + 双向挑战握手（DESIGN.md §4.4）。
//!
//! ## 它是什么形状
//!
//! 先做一次版本协商，再跑两次往返的 CHAP；它们都跑在既有的帧协议上，
//! **不是** TLS 的简化版：
//!
//! ```text
//! 客户端                                 服务端
//!   │  VersionBegin { client_proto }       │
//!   ├───────────────────────────────────►  │
//!   │  VersionOk { server_proto }          │
//!   │ ◄────────────────────────────────────┤
//!   │  AuthBegin { client_nonce }          │
//!   ├───────────────────────────────────►  │ 生成 server_nonce（getrandom）
//!   │  Challenge { server_nonce, proto }   │
//!   │ ◄────────────────────────────────────┤
//!   │  AuthFinish { HMAC(key, cn‖sn) }     │ 常数时间比对自己的 HMAC
//!   ├───────────────────────────────────►  │
//!   │  AuthOk { HMAC(key, sn‖cn) }         │
//!   │ ◄────────────────────────────────────┤
//!   │  SecureBegin { want: 1 }             │ 可选后续阶段（§11 第 4 条）
//!   ├───────────────────────────────────►  │ 从 cn‖sn 派生双向记录密钥
//!   │  SecureOk { suite: 1 }               │
//!   │ ◄────────────────────────────────────┤
//!   │  ── 此线以下整条帧流走记录层 ──       │
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
//! - **认证本身不防窃听**：只跑完握手时，之后的每个字节仍是明文。它建立的是
//!   身份，不是机密性；机密性由 [`crate::net::record`] 那一层补上（本模块只负责
//!   派生它要用的密钥材料），而 `--require-secure` 决定「没补上算不算放行」。
//! - **不做授权**：过了握手的连接与今天一样拥有全部 45 个 op。它是
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
use crate::net::record::{
    Session, SECURE_SUITE_CHACHAPOLY1305, SECURE_SUITE_NONE,
};

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
///
/// `SecureBegin` 也算：它同样既不读状态也不写状态，必须在闸门就地答完，
/// 不能进引擎 —— 进引擎就拿不到那对 nonce，而且它一旦跨过提交线程就会
/// 把「`SecureOk` 在发送队列里的位置」这个切点信息丢掉。
pub fn is_handshake(op: &Op) -> bool {
    matches!(
        op,
        Op::VersionBegin { .. }
            | Op::AuthBegin { .. }
            | Op::AuthFinish { .. }
            | Op::SecureBegin { .. }
    )
}

/// 一条连接的握手进度。放在 [`crate::net::server::Conn`] 里，每连接一份。
#[derive(Clone, Debug)]
pub enum Stage {
    /// 新连接：还没报版本，任何数据 op 与认证 op 都不得放行。
    VersionNeeded,
    /// 本实例没启用认证：版本协商完成后任何 op 直通。
    Off,
    /// 版本已协商，启用了认证，这条连接还没开始握手。
    Needed,
    /// 已发挑战，等 `AuthFinish`。两个随机数都得留着，MAC 是算给它们的。
    Awaiting { cn: [u8; NONCE_LEN], sn: [u8; NONCE_LEN] },
    /// 过了。
    Ready,
}

/// 服务端一侧的握手闸门。
///
/// 它同时是「这条连接到底是不是密文」的唯一真值来源：记录层会话就放在这里，
/// 而不是放在 [`crate::net::server::Conn`] 里。两个地方各存一份 `Option<Session>`
/// 的话，`allows_data()` 与写侧的封存判据就有可能对不上 —— 那正是最贵的错：
/// 一侧发密文、另一侧按明文解。
#[derive(Debug)]
pub struct Gate {
    pub stage: Stage,
    /// 握手交过底的那对随机数，`SecureBegin` 拿它派生记录密钥。
    ///
    /// 派生一次就 `take()` 掉。留着它可以再走一遍同样的派生，得到同一对密钥，
    /// 而两个方向的记录计数器会从 0 重跑 —— 那是密钥流重用，比不加密更糟。
    nonces: Option<([u8; NONCE_LEN], [u8; NONCE_LEN])>,
    /// 已协商、等发送队列走到切点才真正启用的记录层。
    ///
    /// 为什么不直接启用：`SecureOk` 这一帧本身必须明文出去，而它前面可能还排着
    /// 等 fsync 的应答（客户端完全可以把一个写 op 与 `SecureBegin` 挤进同一批）。
    /// 在收到 `SecureBegin` 的那一刻就装填，会把那条更早的应答加密掉，而对侧此时
    /// 还没看到 `SecureOk`，它会拿明文帧头去解密文。所以闸门只说「该装填了」，
    /// 真装填发生在 `Conn` 把 `SecureOk` 排进队列之后（`Slot::Arm`）。
    pub pending: Option<Session>,
    /// 已启用的记录层。`Some` 之后写侧封帧、读侧解记录。
    pub secure: Option<Session>,
    /// `--require-secure`：认证过了但没加密的连接也不许走数据面。
    pub require_secure: bool,
}

impl Gate {
    /// 新连接的闸门。`require_secure` 来自 `ServerConfig`，它不能单独开启：
    /// 没有密钥就派不出记录密钥，所以上层（`bin/server.rs`）必须拦住
    /// 「`--require-secure` 但没 `--auth-key-file`」这种配置。
    pub fn new(require_secure: bool) -> Gate {
        Gate {
            stage: Stage::VersionNeeded,
            nonces: None,
            pending: None,
            secure: None,
            require_secure,
        }
    }

    /// 这条连接现在能不能走数据面。
    ///
    /// `require_secure` 那一半判据不能省：否则它就是「只在报错文案里要求加密」。
    pub fn allows_data(&self) -> bool {
        match self.stage {
            Stage::Off | Stage::Ready => !self.require_secure || self.secure.is_some(),
            _ => false,
        }
    }

    /// 「没握手就发数据 op」的标准应答。两条事件循环路径共用这一条，
    /// 免得一边的文案与另一边的 errno 对不上。
    ///
    /// 第三档（身份已证明、记录层还没启）单独给一句：运维拿 `EADV` + 文案
    /// 才能区分「客户端忘了握手」与「服务端开了 `--require-secure` 而客户端
    /// 不支持加密」—— 两者在只有两档文案时长得一模一样。
    ///
    /// 判据看 `stage` 而不看 `nonces`：`secure_begin` 会把 nonce 取走，从那一刻
    /// 到 `Conn` 真正装填会话之间有一个短窗口，拿 `nonces.is_some()` 当「已证明」
    /// 会在这个窗口里错报成「还没握手」。
    pub fn denied(&self) -> Reply {
        let msg = if matches!(self.stage, Stage::VersionNeeded) {
            "protocol version negotiation required: send VersionBegin first (§4.4)"
        } else if self.require_secure
            && self.secure.is_none()
            && matches!(self.stage, Stage::Off | Stage::Ready)
        {
            "transport encryption required: send SecureBegin before any data op (§4.5)"
        } else {
            "authentication required: run the handshake first (§4.4)"
        };
        Reply::err(errno::EADV, msg)
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
            Op::VersionBegin { client_proto } => Some(self.version(key, *client_proto)),
            Op::AuthBegin { client_nonce } => {
                if matches!(self.stage, Stage::VersionNeeded) {
                    Some(self.denied())
                } else {
                    Some(self.begin(key, client_nonce))
                }
            }
            Op::AuthFinish { mac } => {
                if matches!(self.stage, Stage::VersionNeeded) {
                    Some(self.denied())
                } else {
                    Some(self.finish(key, mac))
                }
            }
            Op::SecureBegin { want } => Some(self.secure_begin(key, *want)),
            _ => {
                if self.allows_data() {
                    None
                } else {
                    Some(self.denied())
                }
            }
        }
    }

    fn version(&mut self, key: Option<&Key>, client_proto: u16) -> Reply {
        if !matches!(self.stage, Stage::VersionNeeded) {
            return Reply::err(
                errno::EADV,
                "protocol version was already negotiated on this connection (§4.4)",
            );
        }
        if client_proto != PROTOCOL_VERSION {
            return Reply::err(
                errno::EADV,
                format!(
                    "protocol version mismatch: client {client_proto}, server {PROTOCOL_VERSION} (§4.4)"
                ),
            );
        }
        self.stage = if key.is_some() { Stage::Needed } else { Stage::Off };
        Reply::VersionOk {
            server_proto: PROTOCOL_VERSION,
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
            // 这对 nonce 随后可能被 `secure_begin` 拿去派生记录密钥。它就是本次
            // 握手的指纹：下一条连接必有新的 `server_nonce`，所以同一个密钥
            // 在全集群不会派生出第二对同样的记录密钥。
            self.nonces = Some((cn, sn));
            Reply::AuthOk {
                server_mac: key.server_mac(&cn, &sn),
                authed: true,
            }
        } else {
            // MAC 错不关连接、也不锁死：回到「还没握手」，可以换个密钥重头再来。
            // 但这一拍的挑战值作废 —— 抓来的 MAC 想在下一条连接上重放，
            // 必须重新拿一次挑战，而那条连接的 server_nonce 不一样。
            self.stage = Stage::Needed;
            self.nonces = None;
            Reply::err(errno::EADV, "bad handshake mac")
        }
    }

    /// 处置 `SecureBegin`：握手完成后的可选阶段（§11 第 4 条）。
    ///
    /// 三种回答的形状不同，不是同一个东西的三种写法：
    /// - `SecureOk { suite: 0 }` —— 正常的「我不支持/不同意这个套件」，客户端
    ///   可以退回明文继续（`require` 在客户端侧才把它变成错误）。
    /// - `Reply::Err` —— 协议违规：身份还没证明，或在同一条连接上二次协商。
    ///   后者必须是错误而不能是 `suite: 0`，否则客户端会把「服务端已经封好了」
    ///   误读成「服务端不同意」，从此一侧密文、一侧明文。
    /// - `Some(session)` 进了 `pending` —— 真正的装填由 `Conn` 在排完
    ///   `SecureOk` 之后做（见 `Gate::pending` 上的注释）。
    fn secure_begin(&mut self, key: Option<&Key>, want: u8) -> Reply {
        if !matches!(self.stage, Stage::Ready) {
            return self.denied();
        }
        if self.secure.is_some() || self.pending.is_some() {
            self.pending = None;
            return Reply::err(
                errno::EADV,
                "the record layer was already negotiated on this connection (§4.5)",
            );
        }
        if want != SECURE_SUITE_CHACHAPOLY1305 {
            return Reply::SecureOk {
                suite: SECURE_SUITE_NONE,
            };
        }
        let Some(key) = key else {
            // `Stage::Ready` 只能从带密钥的 `finish` 走到。这里拿到 `None`
            // 只能是配置在握手中途变了：报错，不能默默退回明文。
            return Reply::err(
                errno::EADV,
                "no auth key configured: record keys cannot be derived (§4.5)",
            );
        };
        let Some((cn, sn)) = self.nonces.take() else {
            // 上面 `secure.is_some()` 已经拦住主路；这里只是兜底：宁可得个
            // 没有密钥可派生的错，也不要拿旧 nonce 重跑一遍 HKDF。
            return Reply::err(
                errno::EADV,
                "no handshake nonces left to derive record keys from (§4.5)",
            );
        };
        self.pending = Some(Session::for_server(key, &cn, &sn));
        Reply::SecureOk {
            suite: SECURE_SUITE_CHACHAPOLY1305,
        }
    }
}

/// 客户端一侧的完整建连握手：版本 + CHAP + 记录层。
///
/// `key = None` 只做版本协商：没密钥就派不出记录密钥，而认证开关不能关掉
/// 跨代静默错读的闸门，所以那一档无论如何都要跑。
///
/// 三处刻意 fail-closed：
/// - 服务端回了 `AuthOk { authed: false }` 而客户端带了密钥 → 报错。否则
///   一个中间人可以只回一句「这里没事」就把认证整个摘掉（降级攻击）。
/// - `Challenge.proto` 与本端不一致 → 报错。MAC 里也绑了版本、本来就会算错，
///   但"版本不符"比"MAC 不对"是准确得多的诊断信息。
/// - `SecureOk { suite: 0 }`（或旧服务端回 `EINVAL`）而客户端带了密钥 → 同样
///   报错。判据与上一条完全同构：降级不能是静默的（`record` 模块头第 3 条）。
pub fn handshake(c: &mut crate::net::client::Client, key: Option<&Key>) -> Result<()> {
    version_handshake(c)?;
    let Some(key) = key else {
        return Ok(());
    };
    let (cn, sn) = handshake_plain_auth(c, key)?;
    secure_handshake(c, key, &cn, &sn, true).map(|_| ())
}

/// 只跑版本 + CHAP，**不**协商记录层（线保持明文）。
///
/// 它是代价实测与跨版本探针用的逃生口：「加密开 vs 关」要在同一份代码上
/// A/B 才有可比性，而不是拿两个不同的客户端去比。默认路径是 [`handshake`]。
pub fn handshake_plain(c: &mut crate::net::client::Client, key: Option<&Key>) -> Result<()> {
    version_handshake(c)?;
    let Some(key) = key else {
        return Ok(());
    };
    handshake_plain_auth(c, key).map(|_| ())
}

/// CHAP 本体，成功后交出那对随机数（记录层要拿它派密钥）。
fn handshake_plain_auth(
    c: &mut crate::net::client::Client,
    key: &Key,
) -> Result<([u8; NONCE_LEN], [u8; NONCE_LEN])> {
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
                        Ok((cn, server_nonce))
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

/// CHAP 之后的可选阶段：协商并启用记录层。
///
/// 返 `Ok(true)` = 这条线已是密文；`Ok(false)` = 对方不同意且 `require = false`，
/// 调用方自己决定要不要继续跑明文。
///
/// 切点在这里说清楚，因为它是全协议最容易写错的一拍：**发送侧在 `SecureBegin`
/// 帧写出去的那一刻就切**（所以下面 `arm_secure` 之前的那次 `call` 是最后一帧
/// 明文），**接收侧在解码出 `SecureOk` 的那一刻切**。两个位置在线上是同一个点，
/// 所以不需要任何序号去对齐。
pub fn secure_handshake(
    c: &mut crate::net::client::Client,
    key: &Key,
    cn: &[u8; NONCE_LEN],
    sn: &[u8; NONCE_LEN],
    require: bool,
) -> Result<bool> {
    let refused = |why: String| {
        if require {
            Err(Error::Denied(why))
        } else {
            Ok(false)
        }
    };
    match c.call(&Op::SecureBegin {
        want: SECURE_SUITE_CHACHAPOLY1305,
    })? {
        Reply::SecureOk { suite } if suite == SECURE_SUITE_CHACHAPOLY1305 => {
            c.arm_secure(Session::for_client(key, cn, sn))?;
            Ok(true)
        }
        Reply::SecureOk { suite } => refused(format!(
            "peer declined the record layer (suite {suite}): the wire stays plaintext (§4.5)"
        )),
        Reply::Err { code, msg } => refused(format!(
            "secure negotiation refused: errno {code}: {} (a pre-v9 server does not know op tag 0x0093)",
            String::from_utf8_lossy(&msg)
        )),
        other => Err(Error::Denied(format!(
            "unexpected reply to SecureBegin: {other:?}"
        ))),
    }
}

/// 所有正常连接的建连第一步：版本协商。
///
/// 旧服务端不认识 `0x0092` 时必须报错退出，绝不能退回「先发数据试试」。
pub fn version_handshake(c: &mut crate::net::client::Client) -> Result<()> {
    match c.call(&Op::VersionBegin {
        client_proto: PROTOCOL_VERSION,
    })? {
        Reply::VersionOk { server_proto } => {
            if server_proto != PROTOCOL_VERSION {
                return Err(Error::Denied(format!(
                    "peer speaks protocol {server_proto}, this client {PROTOCOL_VERSION}"
                )));
            }
            Ok(())
        }
        Reply::Err { code, msg } => Err(Error::Denied(format!(
            "version handshake refused: errno {code}: {} (a pre-v8 server does not know op tag 0x0092)",
            String::from_utf8_lossy(&msg)
        ))),
        other => Err(Error::Denied(format!(
            "unexpected reply to VersionBegin: {other:?}"
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
        let mut g = Gate::new(false);
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

        // 正常一轮：先协商版本，再进入认证。
        assert!(matches!(
            g.on_op(
                Some(&k),
                &Op::VersionBegin {
                    client_proto: PROTOCOL_VERSION,
                },
            ),
            Some(Reply::VersionOk { server_proto }) if server_proto == PROTOCOL_VERSION
        ));
        assert!(matches!(g.stage, Stage::Needed));
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

    /// 没启用认证时版本协商完成后数据面直通；显式 AuthBegin 仍是一次空转。
    #[test]
    fn gate_without_key_is_open() {
        let mut g = Gate::new(false);
        assert!(!g.allows_data());
        assert!(matches!(
            g.on_op(None, &Op::Ping),
            Some(Reply::Err { code, .. }) if code == errno::EADV
        ));
        assert!(matches!(
            g.on_op(
                None,
                &Op::VersionBegin {
                    client_proto: PROTOCOL_VERSION,
                },
            ),
            Some(Reply::VersionOk { server_proto }) if server_proto == PROTOCOL_VERSION
        ));
        assert!(g.allows_data());
        assert!(g.on_op(None, &Op::Ping).is_none());
        assert!(matches!(
            g.on_op(None, &Op::AuthBegin { client_nonce: [0u8; NONCE_LEN] }),
            Some(Reply::AuthOk { authed: false, .. })
        ));
        // 空转之后依然放行（`Off` 不是一个需要重新获得的档位）。
        assert!(g.on_op(None, &Op::Stats).is_none());
    }

    /// 错版本必须 fail-closed，而且不能顺手把闸门打开。
    #[test]
    fn gate_rejects_a_protocol_mismatch() {
        let mut g = Gate::new(false);
        let wrong = PROTOCOL_VERSION + 1;
        match g.on_op(None, &Op::VersionBegin { client_proto: wrong }) {
            Some(Reply::Err { code, msg }) => {
                assert_eq!(code, errno::EADV);
                let msg = String::from_utf8_lossy(&msg);
                assert!(msg.contains(&format!("client {wrong}")), "{msg}");
                assert!(msg.contains(&format!("server {PROTOCOL_VERSION}")), "{msg}");
            }
            other => panic!("错版本不该拿到 VersionOk：{other:?}"),
        }
        assert!(!g.allows_data());
        assert!(matches!(g.stage, Stage::VersionNeeded));
        assert!(matches!(
            g.on_op(None, &Op::Ping),
            Some(Reply::Err { code, .. }) if code == errno::EADV
        ));
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
