//! `AEAD_CHACHA20_POLY1305`（RFC 8439）与 HKDF-SHA256：握手之后那条加密栈用的原语。
//!
//! ## 为什么是自写
//!
//! 依赖门（`tests/independence.rs` 门 2）只放行 `libc` + `thiserror` 进默认构建图。
//! 门本身允许 optional 依赖挂 feature，但这里没有需要许可的理由：这两个算法的
//! 规范形状就是「20 轮置换 + 一次模 2^130−5 的求值」，没有分支表、没有平台内核
//! 指令，自写约 300 行换来的是**一条不随 feature 组合变化的实现路径**——
//! 而 §4.4 的握手已经是同一条纪律下的产物（`sha256.rs` 同理）。
//!
//! ## 为什么挑 ChaCha20-Poly1305 而不是 AES-GCM
//!
//! - 无查表 ⇒ 没有 cache-timing 侧信道，纯软件实现下这条比"常数时间"更容易守住；
//! - Poly1305 是 \(\varepsilon\)-almost-universal 的 MAC，伪造概率 \(2^{-102}\) 量级，
//!   且与 ChaCha20 共用同一份密钥扩展，一份块函数服务两件事；
//! - RFC 8439 给了可直接照抄的官方向量（本模块 `tests` 逐字取规范文本），
//!   而 AES-GCM 要正确就得谈硬件加速与子密钥，那是另一个依赖问题。
//!
//! ## 它防不住什么
//!
//! - **无前向保密**：会话密钥是握手两个随机数的 HKDF 派生物，握手用的密钥一经
//!   泄露，抓来的历史包全部可解。要前向保密得加临时 DH（X25519），那是另一件事。
//! - **不防流量分析**：记录长度就是明文帧长度 + 16，谁在什么时候发了多大一帧仍然可见。
//! - **nonce 重用即全崩**：同一方向上两个记录用同一个计数器值 = 密钥流重用
//!   （明文异或泄露）+ Poly1305 一次性密钥重用（伪造键可恢复）。所以这里
//!   **没有**"重用就退回去"的分支：计数器溢出直接断连，见 [`Direction::seal`]。

use crate::engine::sha256::{ct_eq, hmac_sha256, DIGEST_LEN};

/// Poly1305 一次性密钥 / ChaCha20 密钥长度。
pub const KEY_LEN: usize = 32;
/// RFC 8439 的 96-bit nonce。
pub const IV_LEN: usize = 12;
/// Poly1305 标签长度。
pub const TAG_LEN: usize = 16;

/// ChaCha 状态里那 16 个字（4 常量 + 8 密钥 + 2 计数 + 4 随机数）。
type State = [u32; 16];

/// 1 << n 循环左移（n < 32）。编译器把 `rotate_left` 降成单条 rotate 指令。
#[inline(always)]
fn rotl32(v: u32, n: u32) -> u32 {
    v.rotate_left(n)
}

/// 小端读一个字。
#[inline]
fn le32(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

/// ChaCha 列舍入函数（RFC 8439 §2.1.1）。
#[inline(always)]
fn quarter_round(x: &mut State, a: usize, b: usize, c: usize, d: usize) {
    x[a] = x[a].wrapping_add(x[b]);
    x[d] = rotl32(x[d] ^ x[a], 16);
    x[c] = x[c].wrapping_add(x[d]);
    x[b] = rotl32(x[b] ^ x[c], 12);
    x[a] = x[a].wrapping_add(x[b]);
    x[d] = rotl32(x[d] ^ x[a], 8);
    x[c] = x[c].wrapping_add(x[d]);
    x[b] = rotl32(x[b] ^ x[c], 7);
}

/// 把 key / counter / nonce 摊进初始状态（§2.3 的 setup 形状）。
///
/// `counter` 是 32 位：本模块所有调用方都从 0 或 1 起，且一帧最多
/// `MAX_PLAINTEXT` 字节，永远走不到回绕；但形状必须照抄规范 —— 位置 12 是
/// 计数器、13–15 是 nonce 的后 12 字节，写反了就是另一个算法。
fn init(key: &[u8; KEY_LEN], counter: u32, nonce: &[u8; IV_LEN]) -> State {
    let mut x = [0u32; 16];
    x[0] = 0x6170_7865;
    x[1] = 0x3320_646e;
    x[2] = 0x7962_2d32;
    x[3] = 0x6b20_6574;
    for i in 0..8 {
        x[4 + i] = le32(&key[i * 4..]);
    }
    x[12] = counter;
    x[13] = le32(&nonce[0..4]);
    x[14] = le32(&nonce[4..8]);
    x[15] = le32(&nonce[8..12]);
    x
}

/// 64 字节密钥流块：双重舍入 10 遍（= 20 轮），再把输入加回去（§2.3）。
fn block_out(x: &mut State) {
    for _ in 0..10 {
        // 列舍入
        quarter_round(x, 0, 4, 8, 12);
        quarter_round(x, 1, 5, 9, 13);
        quarter_round(x, 2, 6, 10, 14);
        quarter_round(x, 3, 7, 11, 15);
        // 对角舍入
        quarter_round(x, 0, 5, 10, 15);
        quarter_round(x, 1, 6, 11, 12);
        quarter_round(x, 2, 7, 8, 13);
        quarter_round(x, 3, 4, 9, 14);
    }
}

/// 一个密钥流块（`out.len() == 64`）。
pub fn chacha20_block(key: &[u8; KEY_LEN], counter: u32, nonce: &[u8; IV_LEN], out: &mut [u8]) {
    debug_assert_eq!(out.len(), 64);
    let mut x = init(key, counter, nonce);
    let inp = x;
    block_out(&mut x);
    for i in 0..16 {
        let v = x[i].wrapping_add(inp[i]);
        out[i * 4..i * 4 + 4].copy_from_slice(&v.to_le_bytes());
    }
}

/// ChaCha20 流加密（§2.4）。异或运算，加解密同一函数；`counter` 由调用方给定，
/// AEAD 那条路固定从 1 起，0 号块留给 Poly1305 的一次性密钥。
pub fn chacha20_xor(key: &[u8; KEY_LEN], counter: u32, nonce: &[u8; IV_LEN], data: &mut [u8]) {
    let mut block = [0u8; 64];
    let mut ctr = counter;
    let mut off = 0usize;
    while off < data.len() {
        chacha20_block(key, ctr, nonce, &mut block);
        let n = (data.len() - off).min(64);
        for i in 0..n {
            data[off + i] ^= block[i];
        }
        off += n;
        ctr = ctr.wrapping_add(1);
    }
}

/// Poly1305 累加器的 limb 宽度：五个 26 位字 = 130 位，正好是模数
/// \(2^{130}-5\) 的宽度。下面每一处移位（6/12/18/8 与 26/20/14）都是按这个
/// 宽度推出来的 —— 改宽度就得同时改 [`Poly1305::new`] 与
/// [`Poly1305::process_block`] 的拼接，两处共用同一个掩码才不会写歪。
const LIMB: u32 = 26;
const LIMB_MASK: u32 = (1 << LIMB) - 1;

/// Poly1305（§2.5）：32 字节一次性密钥 + 任意长消息 → 16 字节标签。
///
/// 累加器用 26 位五个字表示，模 \(2^{130}-5\) 由「最高 limb 溢出那一截
/// 乘 5 折回最低 limb」实现（2^130 ≡ 5）。选这个形状而不是 128 位整数版
/// 是因为它没有平台依赖：`u128` 版要两次 128×64 乘法拼 130 位，正确性
/// 全靠手工进位，而 26 位 limb 版的每一步都能在纸上验。
/// r 的钳位（§2.5.1）在 [`Poly1305::new`] 里做，调用方不需要预先处理。
struct Poly1305 {
    r: [u32; 5],
    s: [u32; 4],
    h: [u32; 5],
    buf: [u8; 16],
    buflen: usize,
}

impl Poly1305 {
    fn new(key: &[u8; KEY_LEN]) -> Poly1305 {
        // §2.5.1 的钳位 `r &= 0x0ffffffc0ffffffc0ffffffc0fffffff`，其中 r 是把
        // key 前 16 字节**按小端**读出的 128 位整数。拆成 32 位字就是：字 0 掩
        // 0x0fffffff、字 1..3 掩 0x0ffffffc —— 与规范给的逐字节写法
        // (`r[3]&=15; r[4]&=252; …`) 逐位等价，挑字面形式只因为少一层字节索引。
        let w = [
            le32(&key[0..4]) & 0x0fff_ffff,
            le32(&key[4..8]) & 0x0fff_fffc,
            le32(&key[8..12]) & 0x0fff_fffc,
            le32(&key[12..16]) & 0x0fff_fffc,
        ];
        // 再切成 26 位的五个 limb：`limb_i` 取 r 的第 [26i, 26i+26) 位。拼接形状
        // 与 [`Poly1305::process_block`] 完全一致 —— 两处写歪一处就是另一个算法。
        let r = [
            w[0] & LIMB_MASK,
            ((w[1] << 6) | (w[0] >> 26)) & LIMB_MASK,
            ((w[2] << 12) | (w[1] >> 20)) & LIMB_MASK,
            ((w[3] << 18) | (w[2] >> 14)) & LIMB_MASK,
            (w[3] >> 8) & LIMB_MASK,
        ];
        let s = [
            le32(&key[16..20]),
            le32(&key[20..24]),
            le32(&key[24..28]),
            le32(&key[28..32]),
        ];
        Poly1305 {
            r,
            s,
            h: [0; 5],
            buf: [0; 16],
            buflen: 0,
        }
    }

    /// 吃一个 16 字节块。`hibit` 是那块末尾补的那个 1（最后一块按实际长度补）。
    fn process_block(&mut self, m: &[u8], hibit: u32) {
        debug_assert_eq!(m.len(), 16);
        let t0 = le32(&m[0..4]);
        let t1 = le32(&m[4..8]);
        let t2 = le32(&m[8..12]);
        let t3 = le32(&m[12..16]);
        let h = self.h;
        self.h[0] = h[0] + (t0 & LIMB_MASK);
        self.h[1] = h[1] + (((t1 << 6) | (t0 >> 26)) & LIMB_MASK);
        self.h[2] = h[2] + (((t2 << 12) | (t1 >> 20)) & LIMB_MASK);
        self.h[3] = h[3] + (((t3 << 18) | (t2 >> 14)) & LIMB_MASK);
        self.h[4] = h[4] + ((t3 >> 8) | hibit);
        self.mul()
    }

    /// h = h * r mod (2^130 − 5)。
    ///
    /// 关键的一步是 `s1..s4 = r[i] * 5`：把乘出来的高位 limb 折回低位时，
    /// 2^130 ≡ 5 (mod 2^130−5)，所以乘 5 而不是乘 2^130。漏掉这一折就是模错。
    fn mul(&mut self) {
        let h = self.h;
        let r = self.r;
        let s1 = r[1] * 5;
        let s2 = r[2] * 5;
        let s3 = r[3] * 5;
        let s4 = r[4] * 5;
        // 每一项最多 26 位 × 26 位 = 52 位，五个相加再 56 位，u64 装得下且不留悬念。
        let d0 = (h[0] as u64) * r[0] as u64
            + (h[1] as u64) * s4 as u64
            + (h[2] as u64) * s3 as u64
            + (h[3] as u64) * s2 as u64
            + (h[4] as u64) * s1 as u64;
        let d1 = (h[0] as u64) * r[1] as u64
            + (h[1] as u64) * r[0] as u64
            + (h[2] as u64) * s4 as u64
            + (h[3] as u64) * s3 as u64
            + (h[4] as u64) * s2 as u64;
        let d2 = (h[0] as u64) * r[2] as u64
            + (h[1] as u64) * r[1] as u64
            + (h[2] as u64) * r[0] as u64
            + (h[3] as u64) * s4 as u64
            + (h[4] as u64) * s3 as u64;
        let d3 = (h[0] as u64) * r[3] as u64
            + (h[1] as u64) * r[2] as u64
            + (h[2] as u64) * r[1] as u64
            + (h[3] as u64) * r[0] as u64
            + (h[4] as u64) * s4 as u64;
        let d4 = (h[0] as u64) * r[4] as u64
            + (h[1] as u64) * r[3] as u64
            + (h[2] as u64) * r[2] as u64
            + (h[3] as u64) * r[1] as u64
            + (h[4] as u64) * r[0] as u64;

        // 折叠 + 进位传播。
        let mut c = d0 >> 26;
        self.h[0] = (d0 as u32) & LIMB_MASK;
        let d1 = d1 + c;
        c = d1 >> 26;
        self.h[1] = (d1 as u32) & LIMB_MASK;
        let d2 = d2 + c;
        c = d2 >> 26;
        self.h[2] = (d2 as u32) & LIMB_MASK;
        let d3 = d3 + c;
        c = d3 >> 26;
        self.h[3] = (d3 as u32) & LIMB_MASK;
        let d4 = d4 + c;
        self.h[4] = (d4 as u32) & LIMB_MASK;
        // `d4 >> 26` 就是 ≥ 2^130 的那一截，乘 5 折回 h[0]（2^130 ≡ 5 mod 2^130−5）。
        // 注意量级：d4 本身到得 2^56，所以这个 carry 是 30 位而不是 2 位 —— 折回
        // 必须全程走 u64，在 u32 里算（或者先 `& LIMB_MASK` 再加）会直接丢掉高位，
        // 而丢的那一截是 5·2^26 的整数倍，不是可以忽略的零头。
        let e = self.h[0] as u64 + (d4 >> 26) * 5;
        self.h[0] = (e & LIMB_MASK as u64) as u32;
        // 折回可能把 h[0] 顶出 26 位，溢出的那一截（≤ 2^10）正常进位给 h[1]。
        self.h[1] += (e >> LIMB) as u32;
    }

    fn update(&mut self, mut m: &[u8]) {
        if self.buflen > 0 {
            let want = 16 - self.buflen;
            let n = want.min(m.len());
            self.buf[self.buflen..self.buflen + n].copy_from_slice(&m[..n]);
            self.buflen += n;
            m = &m[n..];
            if self.buflen == 16 {
                // `buf` 是 `[u8; 16]`（Copy）：先取快照再喂，否则 `process_block`
                // 的 `&mut self` 会和 `&self.buf` 撞借用规则。
                let block = self.buf;
                self.process_block(&block, 1 << 24);
                self.buflen = 0;
            } else {
                return;
            }
        }
        // 只给 `finish` 留不足一块的尾巴：剩 16 字节也得当整块吃掉，否则
        // `finish` 会把它误当成需要填充的尾块（hibit 错一个位，标签全偏）。
        while m.len() >= 16 {
            self.process_block(&m[..16], 1 << 24);
            m = &m[16..];
        }
        if !m.is_empty() {
            self.buf[..m.len()].copy_from_slice(m);
            self.buflen = m.len();
        }
    }

    fn finish(mut self) -> [u8; TAG_LEN] {
        if self.buflen > 0 {
            self.buf[self.buflen] = 1;
            self.buf[self.buflen + 1..].fill(0);
            let block = self.buf;
            self.process_block(&block, 0);
        }
        // 最终约化分三步，形状照抄 26 位 limb 的参考实现（poly1305-donna-32）。
        //
        // 1) 把 h 彻底进位干净。`mul` 末尾往 h[0] 里补的那一下（乘 5 折回）可能
        //    让 h[1] 顶出 26 位；不进干净就没法比大小。
        let mut h = self.h;
        let mut c = h[1] >> 26;
        h[1] &= LIMB_MASK;
        h[2] += c;
        c = h[2] >> 26;
        h[2] &= LIMB_MASK;
        h[3] += c;
        c = h[3] >> 26;
        h[3] &= LIMB_MASK;
        h[4] += c;
        c = h[4] >> 26;
        h[4] &= LIMB_MASK;
        // 2^130 ≡ 5：溢出的一截折回最低 limb 要乘 5，再补一次进位。
        h[0] += c * 5;
        c = h[0] >> 26;
        h[0] &= LIMB_MASK;
        h[1] += c;

        // 2) 候选 g = h − p = h + 5 − 2^130（前四个 limb 加 5 逐级进位，最后一个
        //    limb 减掉 2^26 那一格，就是减 2^130）。
        let mut g = [0u32; 5];
        g[0] = h[0].wrapping_add(5);
        c = g[0] >> 26;
        g[0] &= LIMB_MASK;
        g[1] = h[1].wrapping_add(c);
        c = g[1] >> 26;
        g[1] &= LIMB_MASK;
        g[2] = h[2].wrapping_add(c);
        c = g[2] >> 26;
        g[2] &= LIMB_MASK;
        g[3] = h[3].wrapping_add(c);
        c = g[3] >> 26;
        g[3] &= LIMB_MASK;
        g[4] = h[4].wrapping_add(c).wrapping_sub(1 << 26);

        // 3) g[4] 的最高位是借位 ⇒ h < p ⇒ 留 h；没借 ⇒ h ≥ p ⇒ 留 g。用掩码选而
        //    不写 if：与 `ct_eq` 同一条纪律，不把大小关系放进控制流。
        let take_g = (g[4] >> 31).wrapping_sub(1);
        let keep_h = !take_g;
        let f = [
            (g[0] & take_g) | (h[0] & keep_h),
            (g[1] & take_g) | (h[1] & keep_h),
            (g[2] & take_g) | (h[2] & keep_h),
            (g[3] & take_g) | (h[3] & keep_h),
            (g[4] & take_g) | (h[4] & keep_h),
        ];
        // h + s（mod 2^128），然后按小端吐出 16 字节标签。26 位 limb 先拼回 32 位字。
        let lo = [
            f[0] | (f[1] << 26),
            (f[1] >> 6) | (f[2] << 20),
            (f[2] >> 12) | (f[3] << 14),
            (f[3] >> 18) | (f[4] << 8),
        ];
        let mut tag = [0u32; 4];
        let mut c: u64 = 0;
        for i in 0..4 {
            let v = lo[i] as u64 + self.s[i] as u64 + c;
            c = v >> 32;
            tag[i] = v as u32;
        }
        // mod 2^128：最高位的进位丢掉（规范里就是「取 128 个最低位」）。
        let mut out = [0u8; TAG_LEN];
        for i in 0..4 {
            out[i * 4..i * 4 + 4].copy_from_slice(&tag[i].to_le_bytes());
        }
        out
    }
}

/// 独立 Poly1305（密钥就是那 32 字节，不做 ChaCha 派生）。
pub fn poly1305_mac(key: &[u8; KEY_LEN], msg: &[u8]) -> [u8; TAG_LEN] {
    let mut ctx = Poly1305::new(key);
    ctx.update(msg);
    ctx.finish()
}

/// `AEAD_CHACHA20_POLY1305` 加密：返回 `密文 ‖ 标签`（标签在最后 16 字节）。
///
/// §2.8.1 的顺序是「先加密再算 MAC」（encrypt-then-MAC），且 MAC 覆盖的是
/// 密文而不是明文 —— 反过来做会让解密前的完整性判定失去意义。
pub fn aead_seal(
    key: &[u8; KEY_LEN],
    nonce: &[u8; IV_LEN],
    aad: &[u8],
    plaintext: &[u8],
) -> Vec<u8> {
    let otk = one_time_key(key, nonce);
    let mut out = plaintext.to_vec();
    chacha20_xor(key, 1, nonce, &mut out);
    let tag = mac_over(&otk, aad, &out);
    out.extend_from_slice(&tag);
    out
}

/// 只验标签，不解密。给「就地解密」那条路用（见 `record::Session::feed`）。
pub fn aead_verify_tag(
    key: &[u8; KEY_LEN],
    nonce: &[u8; IV_LEN],
    aad: &[u8],
    ciphertext: &[u8],
) -> bool {
    if ciphertext.len() < TAG_LEN {
        return false;
    }
    let body = &ciphertext[..ciphertext.len() - TAG_LEN];
    let got = &ciphertext[ciphertext.len() - TAG_LEN..];
    ct_eq(&mac_over(&one_time_key(key, nonce), aad, body), got)
}

/// 解密并验真。标签不对返回 `None`，**不返回任何明文**（宁可让调用方断连，
/// 也不把一个未认证的字节串交出去）。比对用常数时间。
pub fn aead_open(
    key: &[u8; KEY_LEN],
    nonce: &[u8; IV_LEN],
    aad: &[u8],
    ciphertext: &[u8],
) -> Option<Vec<u8>> {
    if !aead_verify_tag(key, nonce, aad, ciphertext) {
        return None;
    }
    let mut out = ciphertext[..ciphertext.len() - TAG_LEN].to_vec();
    chacha20_xor(key, 1, nonce, &mut out);
    Some(out)
}

/// 0 号块的输出就是 Poly1305 的一次性密钥（§2.6）。
fn one_time_key(key: &[u8; KEY_LEN], nonce: &[u8; IV_LEN]) -> [u8; KEY_LEN] {
    let mut block = [0u8; 64];
    chacha20_block(key, 0, nonce, &mut block);
    let mut otk = [0u8; KEY_LEN];
    otk.copy_from_slice(&block[..KEY_LEN]);
    otk
}

/// MAC 覆盖的构造串：`aad ‖ pad16 ‖ ct ‖ pad16 ‖ len(aad) ‖ len(ct)`（§2.8）。
///
/// 两个长度都是 64 位小端。它们不是装饰：少了长度，`aad = "" , ct = "ab"`
/// 与 `aad = "ab", ct = ""` 会拼出同一个串 —— 长度混淆是 AEAD 构造的经典死法。
fn mac_over(otk: &[u8; KEY_LEN], aad: &[u8], ct: &[u8]) -> [u8; TAG_LEN] {
    let mut ctx = Poly1305::new(otk);
    ctx.update(aad);
    ctx.update(&[0u8; 16][..pad16(aad.len())]);
    ctx.update(ct);
    ctx.update(&[0u8; 16][..pad16(ct.len())]);
    ctx.update(&(aad.len() as u64).to_le_bytes());
    ctx.update(&(ct.len() as u64).to_le_bytes());
    ctx.finish()
}

/// §2.8 的 `pad16`：补到 16 的倍数，**已经对齐时补 0 字节**（不是补一整块）。
fn pad16(n: usize) -> usize {
    (16 - (n % 16)) % 16
}

/// HKDF-SHA256 的 extract 一步（RFC 5869 §2.2）。
fn hkdf_extract(salt: &[u8], ikm: &[u8]) -> [u8; DIGEST_LEN] {
    hmac_sha256(salt, ikm)
}

/// HKDF-SHA256 的 expand（§2.3）。`okm.len()` 上限 255·32 = 8 160 字节。
fn hkdf_expand(prk: &[u8; DIGEST_LEN], info: &[u8], okm: &mut [u8]) {
    assert!(okm.len() <= 255 * DIGEST_LEN, "HKDF 输出长度越界");
    let mut prev = [0u8; DIGEST_LEN];
    let mut prev_len = 0usize;
    let mut done = 0usize;
    for i in 1..=255u8 {
        // T(i) = HMAC(PRK, T(i-1) ‖ info ‖ i)；T(0) 是空串。
        let mut input = Vec::with_capacity(prev_len + info.len() + 1);
        input.extend_from_slice(&prev[..prev_len]);
        input.extend_from_slice(info);
        input.push(i);
        let t = hmac_sha256(prk, &input);
        let n = (okm.len() - done).min(DIGEST_LEN);
        okm[done..done + n].copy_from_slice(&t[..n]);
        done += n;
        if done == okm.len() {
            return;
        }
        prev = t;
        prev_len = DIGEST_LEN;
    }
    unreachable!("上面的循环覆盖 1..=255 全部块数");
}

/// `HKDF-SHA256(salt, ikm, info)` 一步到位。
pub fn hkdf_sha256(salt: &[u8], ikm: &[u8], info: &[u8], okm: &mut [u8]) {
    let prk = hkdf_extract(salt, ikm);
    hkdf_expand(&prk, info, okm);
}

/// 一个方向上的记录密钥。会话两侧各一把，互不相干。
#[derive(Clone, PartialEq, Eq)]
pub struct DirKey(pub [u8; KEY_LEN]);

impl std::fmt::Debug for DirKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 与 `auth::Key` 同一条纪律：这东西会跟着连接进日志与 panic 文案。
        write!(f, "DirKey({KEY_LEN} bytes, redacted)")
    }
}

/// 派生参数：一个记录能装的明文字节上限。
///
/// 64 KiB 是两头的折中：再小，快照线（`SnapFetch` 一块常为 MiB 级）要把一帧
/// 拆成上百个记录，每记录 4 + 16 的开销与每记录一次 Poly1305 初始化叠上去；
/// 再大，一个记录就是一个内存尖峰，且单记录丢包重传的代价跟着涨。
/// 必须 ≤ `MAX_FRAME + TAG_LEN` 那侧能装下的形状，见 `record.rs` 的闸门。
pub const MAX_PLAINTEXT: usize = 64 * 1024;

/// 一条已加密连接的一个方向。
///
/// 计数器而不是随机 IV：同一个密钥下两个记录用同一个值 = 密钥流重用
/// （两份明文的异或直接泄露）+ Poly1305 一次性密钥重用（伪造键可代数恢复），
/// 那是整个构造最坏的单点故障。计数器单调、可判溢出、不依赖 `getrandom`
/// 在热路径上成功；随机 IV 则要在每记录处撞 96 位的生日界。
pub struct Direction {
    key: DirKey,
    next: u64,
}

impl Direction {
    pub fn new(key: DirKey) -> Direction {
        Direction { key, next: 0 }
    }

    /// 已发/已收记录数。运维读数：`--stats` 之外没有别的地方能看到它。
    pub fn counter(&self) -> u64 {
        self.next
    }

    /// 第 `n` 条记录的 nonce。大端编码保证字典序与数值序一致 ——
    /// 任何非零前缀都严格大于零前缀，回绕因此不可能被误当成长度相同的不同值。
    fn nonce(n: u64) -> [u8; IV_LEN] {
        let mut iv = [0u8; IV_LEN];
        iv[..8].copy_from_slice(&n.to_be_bytes());
        iv
    }

    /// 把一个明文帧封成线上字节：`len:u32(小端，含标签) ‖ 密文 ‖ 标签`。
    ///
    /// 长度字段进 AAD：它没被加密，但必须被认证 —— 否则一个中间人改长度
    /// 就能让接收方按错误的边界切下一段，而标签照样对得上自己那截。
    ///
    /// 返回 `None` 只有一种可能：计数器溢出或明文越界。溢出时**不能**封
    /// 第二遍（那就是密钥流重用），调用方必须断连。
    pub fn seal(&mut self, plaintext: &[u8]) -> Option<Vec<u8>> {
        if plaintext.len() > MAX_PLAINTEXT || self.next == u64::MAX {
            return None;
        }
        let iv = Self::nonce(self.next);
        self.next += 1;
        let mut aad = Vec::with_capacity(4);
        aad.extend_from_slice(&((plaintext.len() + TAG_LEN) as u32).to_le_bytes());
        let body = aead_seal(&self.key.0, &iv, &aad, plaintext);
        let mut out = Vec::with_capacity(4 + body.len());
        out.extend_from_slice(&aad);
        out.extend_from_slice(&body);
        Some(out)
    }

    /// 把任意长的一段明文切成记录流（一帧 8 MiB 的回包就是 128 个记录）。
    ///
    /// **记录边界不必对齐帧边界**：接收方把解密出的明文按顺序接进同一条
    /// 明文字节流，帧自己的 `len:u32` 头才是边界。这是这里能随意切段的理由，
    /// 也是服务端缓冲能保持有界的原因（不必攒够一整帧才发）。
    pub fn seal_all(&mut self, plaintext: &[u8], out: &mut Vec<u8>) -> bool {
        for chunk in plaintext.chunks(MAX_PLAINTEXT) {
            match self.seal(chunk) {
                Some(rec) => out.extend_from_slice(&rec),
                None => return false,
            }
        }
        true
    }

    /// 吃一个完整记录（`rec` 是 `len` 之后的字节，含标签），验真并就地解密。
    ///
    /// `wire_len` 是线上那 4 字节声明的长度，一并进 AAD。
    /// 标签不对 ⇒ `None`：不交任何字节。
    pub fn open(&mut self, wire_len: u32, rec: &mut Vec<u8>) -> Option<Vec<u8>> {
        if self.next == u64::MAX || rec.len() < TAG_LEN {
            return None;
        }
        let iv = Self::nonce(self.next);
        self.next += 1;
        let mut aad = [0u8; 4];
        aad.copy_from_slice(&wire_len.to_le_bytes());
        if !aead_verify_tag(&self.key.0, &iv, &aad, rec) {
            return None;
        }
        // 标签对得上才动手：复用同一份密钥流把字节翻回明文，
        // 避免先分配一份密文大小的拷贝再丢掉（记录最多 64 KiB + 16）。
        let n = rec.len() - TAG_LEN;
        chacha20_xor(&self.key.0, 1, &iv, &mut rec[..n]);
        rec.truncate(n);
        Some(std::mem::take(rec))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hx(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex"))
            .collect()
    }
    fn key(s: &str) -> [u8; KEY_LEN] {
        let v = hx(s);
        let mut k = [0u8; KEY_LEN];
        k.copy_from_slice(&v);
        k
    }
    fn iv(s: &str) -> [u8; IV_LEN] {
        let v = hx(s);
        let mut n = [0u8; IV_LEN];
        n.copy_from_slice(&v);
        n
    }

    /// RFC 8439 §2.1.1：四个独立整数的列舍入向量。
    /// 规范给的是不带状态的四个数，这里垫零到 16 字（`quarter_round` 只碰
    /// 下标 0..4，多出来的格子必须保持不动 —— 一同验）。
    #[test]
    fn quarter_round_vector() {
        let mut x: State = [0u32; 16];
        x[0] = 0x1111_1111;
        x[1] = 0x0102_0304;
        x[2] = 0x9b8d_6f43;
        x[3] = 0x0123_4567;
        quarter_round(&mut x, 0, 1, 2, 3);
        assert_eq!(x[0], 0xea2a92f4, "a");
        assert_eq!(x[1], 0xcb1cf8ce, "b");
        assert_eq!(x[2], 0x4581472e, "c");
        assert_eq!(x[3], 0x5881c4bb, "d");
        assert_eq!(&x[4..], &[0u32; 12][..], "其余格子不该动");
    }

    /// §2.2.1：对完整 16 字状态只跑一个 QUARTERROUND(2, 7, 8, 13)。
    /// 这一条的价值全在「其余 12 个字必须原封不动」—— 索引写错就会动到邻居。
    #[test]
    fn state_quarter_round_vector() {
        let inp: State = [
            0x879531e0, 0xc5ecf37d, 0x516461b1, 0xc9a62f8a, 0x44c20ef3, 0x3390af7f, 0xd9fc690b,
            0x2a5f714c, 0x53372767, 0xb00a5631, 0x974c541a, 0x359e9963, 0x5c971061, 0x3d631689,
            0x2098d9d6, 0x91dbd320,
        ];
        let mut x = inp;
        quarter_round(&mut x, 2, 7, 8, 13);
        assert_eq!(x[2], 0xbdb886dc);
        assert_eq!(x[7], 0xcfacafd2);
        assert_eq!(x[8], 0xe46bea80);
        assert_eq!(x[13], 0xccc07c79);
        for i in [0usize, 1, 3, 4, 5, 6, 9, 10, 11, 12, 14, 15] {
            assert_eq!(x[i], inp[i], "第 {i} 个字不该被碰到");
        }
    }

    /// §2.3.2：完整块函数（密钥 + nonce + 计数 1）的 64 字节输出。
    /// 形状上分三拍验证：初始状态、20 轮后的状态、加回输入后的最终字，
    /// 这样失败时能直接说出是 `init` 歪了还是 `block_out` 歪了。
    #[test]
    fn block_function_vector() {
        let k = key("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
        let n = iv("000000090000004a00000000");
        let want_setup: State = [
            0x61707865, 0x3320646e, 0x79622d32, 0x6b206574, 0x03020100, 0x07060504, 0x0b0a0908,
            0x0f0e0d0c, 0x13121110, 0x17161514, 0x1b1a1918, 0x1f1e1d1c, 0x00000001, 0x09000000,
            0x4a000000, 0x00000000,
        ];
        assert_eq!(init(&k, 1, &n), want_setup, "初始状态摆放");
        // 20 轮后（还没加回输入）的状态。
        let want_rounds: State = [
            0x837778ab, 0xe238d763, 0xa67ae21e, 0x5950bb2f, 0xc4f2d0c7, 0xfc62bb2f, 0x8fa018fc,
            0x3f5ec7b7, 0x335271c2, 0xf29489f3, 0xeabda8fc, 0x82e46ebd, 0xd19c12b4, 0xb04e16de,
            0x9e83d0cb, 0x4e3c50a2,
        ];
        let mut x = init(&k, 1, &n);
        block_out(&mut x);
        assert_eq!(x, want_rounds, "20 轮后的状态");
        // 加回输入之后的最终字（小端串行化）。
        let want = hx(
            "10f1e7e4d13b5915500fdd1fa32071c4c7d1f4c733c068030422aa9ac3d46c4e\
             d2826446079faa0914c2d705d98b02a2b5129cd1de164eb9cbd083e8a2503c4e",
        );
        let mut out = [0u8; 64];
        chacha20_block(&k, 1, &n, &mut out);
        assert_eq!(out.to_vec(), want, "块输出串行化");
    }

    /// §2.4.2：整段密钥流 + 加密结果。这是"流"这一层的完整闭环。
    /// 计数从 1 起（规范给的密钥流就是块 1 开始的），块 0 归 Poly1305 的一次性密钥。
    #[test]
    fn chacha20_cipher_vector() {
        let k = key("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
        let n = iv("000000000000004a00000000");
        let pt = b"Ladies and Gentlemen of the class of '99: \
                   If I could offer you only one tip for the future, sunscreen would be it.";
        let want_ks = hx(
            "224f51f3401bd9e12fde276fb8631ded8c131f823d2c06e27e4fcaec9ef3cf788a3b0aa372600a\
             92b57974cded2b9334794cba40c63e34cdea212c4cf07d41b769a6749f3f630f4122cafe28ec4d\
             c47e26d4346d70b98c73f3e9c53ac40c5945398b6eda1a832c89c167eacd901d7e2bf363",
        );
        assert_eq!(pt.len(), 114, "向量里的明文就是 114 字节，长度错则整段错位");
        let mut ks = vec![0u8; want_ks.len()];
        chacha20_xor(&k, 1, &n, &mut ks);
        assert_eq!(ks, want_ks, "密钥流必须逐字相等");
        let want_ct = hx(
            "6e2e359a2568f98041ba0728dd0d6981e97e7aec1d4360c20a27afccfd9fae0bf91b65c5524733\
             ab8f593dabcd62b3571639d624e65152ab8f530c359f0861d807ca0dbf500d6a6156a38e088a22\
             b65e52bc514d16ccf806818ce91ab77937365af90bbf74a35be6b40b8eedf2785e42874d",
        );
        // 密钥流与密文必须自洽：ct = pt ^ ks。两个官方向量彼此对不上时，这一条
        // 会先响，而不是等下面的 `aead_vector` 才间接暴露。
        let selfconsistent: Vec<u8> = pt.iter().zip(&want_ks).map(|(p, s)| p ^ s).collect();
        assert_eq!(&selfconsistent[..], &want_ct[..], "密文必须等于明文 XOR 同一段密钥流");
        let mut ct = pt.to_vec();
        chacha20_xor(&k, 1, &n, &mut ct);
        assert_eq!(ct, want_ct);
        // 异或形状：再跑一次回到明文。
        chacha20_xor(&k, 1, &n, &mut ct);
        assert_eq!(ct, pt.to_vec());
    }

    /// §2.5.2：独立 Poly1305 向量（34 字节消息 ⇒ 跨块 + 带部分高位）。
    #[test]
    fn poly1305_vector() {
        let k = key("85d6be7857556d337f4452fe42d506a80103808afb0db2fd4abff6af4149f51b");
        let msg = b"Cryptographic Forum Research Group";
        let want = hx("a8061dc1305136c6c22b8baf0c0127a9");
        assert_eq!(poly1305_mac(&k, msg).to_vec(), want);
    }

    /// §2.8.2：完整 AEAD 向量，含 AAD。
    #[test]
    fn aead_vector() {
        let k = key("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f");
        let n = iv("070000004041424344454647");
        let aad = hx("50515253c0c1c2c3c4c5c6c7");
        let pt = b"Ladies and Gentlemen of the class of '99: \
                   If I could offer you only one tip for the future, sunscreen would be it.";
        let want_ct = hx(
            "d31a8d34648e60db7b86afbc53ef7ec2a4aded51296e08fea9e2b5a736ee62d63dbea45e8ca967\
             1282fafb69da92728b1a71de0a9e060b2905d6a5b67ecd3b3692ddbd7f2d778b8c9803aee32809\
             1b58fab324e4fad675945585808b4831d7bc3ff4def08e4b7a9de576d26586cec64b6116",
        );
        let want_tag = hx("1ae10b594f09e26a7e902ecbd0600691");
        let out = aead_seal(&k, &n, &aad, pt);
        assert_eq!(out.len(), pt.len() + TAG_LEN);
        assert_eq!(out[..pt.len()], want_ct[..]);
        assert_eq!(out[pt.len()..], want_tag[..]);
        assert_eq!(aead_open(&k, &n, &aad, &out).unwrap(), pt.to_vec());
    }

    /// 一次性密钥：§2.6.2 的 0 号块输出（它同时就是 Poly1305 的密钥）。
    #[test]
    fn one_time_key_vector() {
        let k = key("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f");
        let n = iv("000000000001020304050607");
        let want = hx("8ad5a08b905f81cc815040274ab29471a833b637e3fd0da508dbb8e2fdd1a646");
        assert_eq!(one_time_key(&k, &n).to_vec(), want);
    }

    /// 空明文一格：我们的栈里最短的记录就是它（`seal(&[])`）。
    /// 期望值不凭记忆写 —— 与纯 Python 参照实现对跑（见 tests/secure_channel.rs）。
    #[test]
    fn empty_plaintext_round_trips() {
        let k = key(&"22".repeat(32));
        let n = iv(&"33".repeat(12));
        let out = aead_seal(&k, &n, b"ctx", b"");
        assert_eq!(out.len(), TAG_LEN);
        assert_eq!(aead_open(&k, &n, b"ctx", &out).unwrap(), b"");
        assert!(aead_open(&k, &n, b"ctxy", &out).is_none(), "AAD 变必须验不过");
    }

    /// 篡改检测：改一个比特的密文、改标签、截短、加尾巴，四种都得是 None。
    #[test]
    fn any_tamper_is_rejected() {
        let k = key(&"44".repeat(32));
        let n = iv(&"55".repeat(12));
        let aad = b"header";
        for len in [0usize, 1, 15, 16, 17, 63, 64, 65, 255, 256, 4096] {
            let m = vec![0xa7u8; len];
            let sealed = aead_seal(&k, &n, aad, &m);
            assert_eq!(aead_open(&k, &n, aad, &sealed).unwrap(), m);
            // 翻密文一个比特
            let mut bad = sealed.clone();
            if !bad.is_empty() {
                bad[0] ^= 1;
            }
            assert!(aead_open(&k, &n, aad, &bad).is_none(), "len {len}");
            // 翻标签一个比特
            let mut bad = sealed.clone();
            let t = bad.len() - 1;
            bad[t] ^= 0x80;
            assert!(aead_open(&k, &n, aad, &bad).is_none(), "len {len} tag");
            // 截掉标签一字节 / 少一字节
            assert!(aead_open(&k, &n, aad, &sealed[..sealed.len() - 1]).is_none());
            // 尾巴多一字节
            let mut bad = sealed.clone();
            bad.push(0);
            assert!(aead_open(&k, &n, aad, &bad).is_none(), "len {len} extend");
        }
    }

    /// `pad16` 的对齐语义：已对齐补 0，不是补一整块。这一条错了 MAC 就整体偏。
    #[test]
    fn padding_is_to_multiple_not_full_block() {
        assert_eq!(pad16(0), 0);
        assert_eq!(pad16(1), 15);
        assert_eq!(pad16(16), 0);
        assert_eq!(pad16(17), 15);
        assert_eq!(pad16(32), 0);
    }

    /// 记录流的两条形状：长于 [`MAX_PLAINTEXT`] 的一段必须被切成整数个记录，
    /// 且接收方按长度头重新切回来之后逐字节等于原明文。
    #[test]
    fn record_stream_splits_and_rejoins() {
        let k = DirKey([0x5a; KEY_LEN]);
        let mut tx = Direction::new(k.clone());
        let mut rx = Direction::new(k);
        for len in [MAX_PLAINTEXT - 1, MAX_PLAINTEXT, MAX_PLAINTEXT + 1, 3 * MAX_PLAINTEXT + 7] {
            let msg: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            let mut wire = Vec::new();
            assert!(tx.seal_all(&msg, &mut wire), "len {len} 封不上");
            // 记录数 = 向上取整；每记录 = 4 + 明文段 + 16。
            let want = len.div_ceil(MAX_PLAINTEXT);
            let mut got = Vec::new();
            let mut off = 0usize;
            let mut n = 0usize;
            while off < wire.len() {
                let l = u32::from_le_bytes(wire[off..off + 4].try_into().unwrap()) as usize;
                let mut rec = wire[off + 4..off + 4 + l].to_vec();
                match rx.open(l as u32, &mut rec) {
                    Some(p) => got.extend_from_slice(&p),
                    None => panic!("len {len} 第 {n} 个记录验不过"),
                }
                off += 4 + l;
                n += 1;
            }
            assert_eq!(n, want, "len {len} 记录数");
            assert_eq!(got, msg, "len {len} 重接不等");
        }
    }

    /// HKDF 的形状自检：不同 info 必须给出不同输出，且长度可切。
    /// 数值基准放在 `tests/secure_channel.rs` 与 Python 参照实现对跑。
    #[test]
    fn hkdf_info_and_length_bind() {
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        let mut c = [0u8; 64];
        hkdf_sha256(b"salt", b"ikm", b"cli", &mut a);
        hkdf_sha256(b"salt", b"ikm", b"srv", &mut b);
        hkdf_sha256(b"salt", b"ikm", b"cli", &mut c);
        assert_ne!(a, b, "域分隔串必须进 HKDF");
        assert_eq!(&c[..32], &a[..], "expand 必须前缀稳定（T(1) 与长度无关）");
        // 64 字节 = T(1) ‖ T(2)，两段必须不同（i 进 HMAC 的那一位）
        assert_ne!(&c[..32], &c[32..]);
    }
}
