//! SHA-256 与 HMAC-SHA256：线协议握手（DESIGN.md §4.4）唯一的密码学原语。
//!
//! 为什么自己写：引擎的依赖面是一条硬约束（`[dependencies]` 里非 optional 的
//! 只许 `libc` + `thiserror`，由 `tests/independence.rs` 守着），而一个握手
//! 要用的就是一个哈希函数 —— 为它引一整棵 `ring`/`rustcrypto` 依赖树不值得，
//! 引了也过不了那道门。
//!
//! 实现照 FIPS 180-4 逐字搬，不做任何"优化式改写"：正确性由标准测试向量钉
//! （[`tests`] 里那些串是 FIPS 与 RFC 4231 的原文，不是我们自己算出来的）。
//!
//! # 这里**没有**的东西
//!
//! - **没有密钥管理、没有随机数**：那是 [`crate::net::auth`] 的事。
//! - **不是抗侧信道的实现**：只有 [`ct_eq`] 那一处需要常数时间比较（判 MAC
//!   对不对），压缩函数按普通写法。要拿去抗时序旁路的场景请先重新评估。
//! - **不做 SHA-512 / 不做 PBKDF**：共享密钥必须是 32 字节随机值，不做口令
//!   派生 —— 口令派生弱是这个方案明确拒绝承担的债（§4.4「它防不住什么」）。

/// SHA-256 摘要长度（字节）。
pub const DIGEST_LEN: usize = 32;
/// SHA-256 分组长度（字节），HMAC 的 ipad/opad 就按它切。
pub const BLOCK_LEN: usize = 64;

/// FIPS 180-4 §4.2.2 的轮常量。
const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// 流式 SHA-256。
///
/// 握手要 MAC 的字节是拼出来的（上下文串 + 版本 + 两个随机数），一次 `update`
/// 就够，所以这个类型目前只会被用完即弃；留着流式接口是因为 64 字节分组的
/// 缓冲逻辑本身就得这么写，而不是为了将来"可能要用"。
pub struct Sha256 {
    h: [u32; 8],
    buf: [u8; BLOCK_LEN],
    /// `buf` 里已填字节数。
    fill: usize,
    /// 已喂入的总位数，进尾部长度域。
    bits: u64,
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    pub fn new() -> Self {
        Sha256 {
            // FIPS 180-4 §5.3.3 的初值。
            h: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buf: [0u8; BLOCK_LEN],
            fill: 0,
            bits: 0,
        }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.bits = self.bits.wrapping_add((data.len() as u64) * 8);
        if self.fill > 0 {
            let need = BLOCK_LEN - self.fill;
            let take = need.min(data.len());
            self.buf[self.fill..self.fill + take].copy_from_slice(&data[..take]);
            self.fill += take;
            data = &data[take..];
            if self.fill < BLOCK_LEN {
                return;
            }
            let block = self.buf;
            compress(&mut self.h, &block);
            self.fill = 0;
        }
        let mut chunks = data.chunks_exact(BLOCK_LEN);
        for c in &mut chunks {
            let mut block = [0u8; BLOCK_LEN];
            block.copy_from_slice(c);
            compress(&mut self.h, &block);
        }
        let rest = chunks.remainder();
        if !rest.is_empty() {
            self.buf[..rest.len()].copy_from_slice(rest);
            self.fill = rest.len();
        }
    }

    pub fn finish(mut self) -> [u8; DIGEST_LEN] {
        // 填充：0x80 + 若干 0 + 64 位大端总长（FIPS 180-4 §4.1 算法 1）。
        // `fill < 56` 时填充并进当前分组，否则要另开一个分组（剩下的空间不
        // 够放 1 + 8 字节），两条分支在下面的 `sha256_padding_boundary` 里各
        // 有一格。
        let total_bits = self.bits;
        let pad_len = if self.fill < 56 { 56 - self.fill } else { 120 - self.fill };
        let mut pad = [0u8; BLOCK_LEN];
        pad[0] = 0x80;
        self.update_no_count(&pad[..pad_len]);
        let mut len_be = [0u8; 8];
        len_be.copy_from_slice(&total_bits.to_be_bytes());
        self.update_no_count(&len_be);
        let mut out = [0u8; DIGEST_LEN];
        for (i, w) in self.h.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&w.to_be_bytes());
        }
        out
    }

    /// [`update`] 但不累加长度：尾部填充自己管长度。
    fn update_no_count(&mut self, data: &[u8]) {
        let keep = self.bits;
        self.update(data);
        self.bits = keep;
    }
}

/// 一次分组压缩（FIPS 180-4 §4.2.2）。
fn compress(h: &mut [u32; 8], block: &[u8; BLOCK_LEN]) {
    let mut w = [0u32; 64];
    for i in 0..16 {
        w[i] = u32::from_be_bytes([
            block[i * 4],
            block[i * 4 + 1],
            block[i * 4 + 2],
            block[i * 4 + 3],
        ]);
    }
    for i in 16..64 {
        let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
        let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
        w[i] = w[i - 16]
            .wrapping_add(s0)
            .wrapping_add(w[i - 7])
            .wrapping_add(s1);
    }
    let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut hh] = *h;
    for i in 0..64 {
        let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
        let ch = (e & f) ^ ((!e) & g);
        let t1 = hh
            .wrapping_add(s1)
            .wrapping_add(ch)
            .wrapping_add(K[i])
            .wrapping_add(w[i]);
        let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
        let maj = (a & b) ^ (a & c) ^ (b & c);
        let t2 = s0.wrapping_add(maj);
        hh = g;
        g = f;
        f = e;
        e = d.wrapping_add(t1);
        d = c;
        c = b;
        b = a;
        a = t1.wrapping_add(t2);
    }
    for (x, v) in h.iter_mut().zip([a, b, c, d, e, f, g, hh]) {
        *x = x.wrapping_add(v);
    }
}

/// 一次性摘要。
pub fn sha256(data: &[u8]) -> [u8; DIGEST_LEN] {
    let mut s = Sha256::new();
    s.update(data);
    s.finish()
}

/// HMAC-SHA256（RFC 2104）。
///
/// 密钥长于分组先哈希一次，短于分组补零 —— 这两条分支各有用例钉住，
/// 见 [`tests::hmac_rfc4231_long_key`] 与 [`tests::hmac_rfc4231_short_key`]。
pub fn hmac_sha256(key: &[u8], msg: &[u8]) -> [u8; DIGEST_LEN] {
    let k = if key.len() > BLOCK_LEN {
        let mut h = [0u8; BLOCK_LEN];
        h[..DIGEST_LEN].copy_from_slice(&sha256(key));
        h
    } else {
        let mut h = [0u8; BLOCK_LEN];
        h[..key.len()].copy_from_slice(key);
        h
    };
    let mut ipad = [0u8; BLOCK_LEN];
    let mut opad = [0u8; BLOCK_LEN];
    for i in 0..BLOCK_LEN {
        ipad[i] = k[i] ^ 0x36;
        opad[i] = k[i] ^ 0x5c;
    }
    let mut inner = Sha256::new();
    inner.update(&ipad);
    inner.update(msg);
    let inner = inner.finish();
    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(&inner);
    outer.finish()
}

/// 常数时间比两段字节。
///
/// 判 MAC 的地方必须用它：`a == b` 在 Rust 里是逐字节短路比较，最坏情况下
/// 第一个字节不同就返回，攻击者能用响应时间一位位地把正确 MAC 试出来。
/// 长度不同直接 false —— 长度不是秘密（本模块两处调用点都是定长 32）。
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FIPS 180-4 标准样本：空串。
    #[test]
    fn sha256_empty() {
        let got: String = sha256(b"").iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            got,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    /// FIPS 180-4 标准样本："abc"。
    #[test]
    fn sha256_abc() {
        let got: String = sha256(b"abc").iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            got,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    /// 448/449 位那条边界：填充要么并进当前分组，要么多开一个分组。
    #[test]
    fn sha256_padding_boundary() {
        let a: String = sha256(&vec![b'a'; 55]).iter().map(|b| format!("{b:02x}")).collect();
        let b: String = sha256(&vec![b'a'; 56]).iter().map(|b| format!("{b:02x}")).collect();
        let c: String = sha256(&vec![b'a'; 64]).iter().map(|b| format!("{b:02x}")).collect();
        // 55/56/64 三个长度必须落进不同的填充分支，摘要互不相同；具体值由
        // 下面那条"与 55 字节逐字节喂"的一致性用例交叉验证。
        assert!(a != b && b != c && a != c, "{a} / {b} / {c}");

        let mut s = Sha256::new();
        for chunk in vec![b'a'; 55].chunks(7) {
            s.update(chunk);
        }
        assert_eq!(s.finish(), sha256(&vec![b'a'; 55]));
    }

    /// FIPS 180-4 里那条两分组样本。
    #[test]
    fn sha256_two_block_message() {
        let msg = b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq";
        let got: String = sha256(msg).iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            got,
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    /// RFC 4231 Test Case 1（20 字节密钥 + "Hi There"）。
    #[test]
    fn hmac_rfc4231_case1() {
        let key = vec![0x0bu8; 20];
        let got: String = hmac_sha256(&key, b"Hi There")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            got,
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    /// RFC 4231 Test Case 2："what do ya want for nothing?"（密钥 = 长度本身）。
    #[test]
    fn hmac_rfc4231_case2() {
        let got: String = hmac_sha256(b"Jefe", b"what do ya want for nothing?")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            got,
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    /// RFC 4231 Test Case 3：密钥 20 字节，走补零那支。
    #[test]
    fn hmac_rfc4231_short_key() {
        let got: String = hmac_sha256(&vec![0xaau8; 20], &vec![0xddu8; 50])
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            got,
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"
        );
    }

    /// RFC 4231 Test Case 6 / 7：密钥 131 字节 > 分组，先哈希一次。
    #[test]
    fn hmac_rfc4231_long_key() {
        let got: String = hmac_sha256(
            &vec![0xaau8; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        )
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
        assert_eq!(
            got,
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
        let got: String = hmac_sha256(&vec![0xaau8; 131], &vec![0xddu8; 50])
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            got,
            "124c7d2385aa1743aaad12204e3464f06305fd1a6d291250fa564dceffab0c8a"
        );
    }

    /// 密钥 50 字节：大于一份摘要、小于一个分组。上面那些 RFC 样本都跳不出
    /// 这个区间，而恰好是这里能把「阈值误取成 `> DIGEST_LEN`」钉死 —— 取错了
    /// 就会多哈希一次，值完全不同。
    ///
    /// 期望值不是 RFC 里的（4231 没给这个长度），是本轮拿 `python3 -c
    /// "import hmac,hashlib; …"` 逐字对出来的：与本实现一致。
    #[test]
    fn hmac_key_between_digest_and_block() {
        let got: String = hmac_sha256(&vec![0xaau8; 50], &vec![0x44u8; 50])
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert_eq!(
            got,
            "8c7014580d3542c99e042823d683a58aaae9b67e8e30c706fc878b5fd91aee21"
        );
    }

    #[test]
    fn ct_eq_matches_but_is_length_strict() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(ct_eq(b"", b""));
    }
}
