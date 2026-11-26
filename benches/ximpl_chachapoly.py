#!/usr/bin/env python3
"""treedb 记录层的跨实现参照：用独立的 ChaCha20-Poly1305 / HKDF 实现重算一遍
线上字节，并把结果打印成可以直接贴进 `tests/secure_channel.rs` 的常量表。

为什么需要它：`src/net/chacha20poly1305.rs` 的 RFC 8439 向量钉的是 ChaCha20 与
Poly1305 两个**零件**，而「零件都对、装配错」是这类自写实现最典型的失效 ——
长度头进不进 AAD、nonce 的大端小端、HKDF 的 salt 用哪一层、计数器从 0 还是 1
起步，任何一个装错都能让本实现自己封自己解完全自洽，却与世界上任何其他实现
不互通。那正是「升级窗口里旧客户端连不上新服务端」的形状，只在跨实现对照里
才暴露。

用法：
    python3 benches/ximpl_chachapoly.py            # 打印 Rust 常量表
    python3 benches/ximpl_chachapoly.py --selftest # 只做本脚本与 cryptography 的自检

依赖：python3 + `cryptography`（开发机上的东西，**不是** crate 依赖 —— 依赖门
只管发布构建，这里产出的东西是冻结进测试的常量）。
"""

import hashlib
import sys

from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend

# cryptography 2.x 的 `HKDF` 构造器要显式 backend（3.1+ 才变成可选），而它家的
# AEAD 类反而不收 `backend` 参数 —— 两个都按 2.x 的形状写，新版照跑。
BE = default_backend()

# ---- 与 src/net/record.rs 逐字对应的形状 -------------------------------
# 改任何一条都必须同步改那边，并重新生成 `tests/secure_channel.rs` 的表。
PREFIX = b"treedb-secure-v1"
EXTRACT_SALT = b"treedb-record-v1"
INFO_SALT = b"treedb-record-v1-salt"
INFO_CLI = b"treedb-record-v1-c2s-key"
INFO_SRV = b"treedb-record-v1-s2c-key"
TAG_LEN = 16
REC_HDR = 4
# 一个记录能装的明文字节上限（`chacha20poly1305::MAX_PLAINTEXT`）。切片规则本身
# 是规格的一部分，不是密码学 —— 所以这里照抄，而 ChaCha20/Poly1305/HKDF 三个零件
# 全部拿独立实现算。
MAX_PLAINTEXT = 64 * 1024
KEY = bytes([0x5A]) * 32          # == tests/common::test_key()
CN = bytes([(i * 7 + 3) & 0xFF for i in range(32)])   # client_nonce
SN = bytes([(i * 11 + 91) & 0xFF for i in range(32)])  # server_nonce


def hkdf(salt, ikm, info, length=32):
    # HKDF-SHA256 的 extract+expand 一步，与 `hkdf_sha256` 同参数顺序。
    return HKDF(algorithm=hashes.SHA256(), length=length, salt=salt,
                info=info, backend=BE).derive(ikm)


def dir_keys(key, cn, sn):
    """返回 (c2s 密钥, s2c 密钥)，与服务端/客户端两侧的派生链一致。"""
    ikm = PREFIX + cn + sn
    salt = hkdf(EXTRACT_SALT, ikm, INFO_SALT)
    return hkdf(salt, key, INFO_CLI), hkdf(salt, key, INFO_SRV)


def nonce(n):
    """第 n 条记录的 nonce：8 字节大端计数器 ‖ 4 字节零。"""
    return n.to_bytes(8, "big") + b"\x00\x00\x00\x00"


def seal(key, n, plaintext):
    """一条记录：`len:u32(小端，含标签) ‖ 密文 ‖ 标签`，长度头进 AAD。"""
    aad = (len(plaintext) + TAG_LEN).to_bytes(REC_HDR, "little")
    body = ChaCha20Poly1305(key).encrypt(nonce(n), plaintext, aad)
    return aad + body


def open_record(key, n, wire):
    l = int.from_bytes(wire[:REC_HDR], "little")
    return ChaCha20Poly1305(key).decrypt(nonce(n), wire[REC_HDR:REC_HDR + l],
                                         wire[:REC_HDR])


def pattern(n, seed):
    """与 Rust 侧 `payload()` 同一条递推，免得两边构造的明文不同形。"""
    return bytes(((i * 31 + seed) & 0xFF) for i in range(n))


def stream(key, plaintexts):
    """从计数器 0 起连续封若干段明文 = 一条真实连接上的一段字节。

    超过 `MAX_PLAINTEXT` 的那一段先按上限切片，与 `Direction::seal_all` 同一条
    规则；不这样做的话两边记录数都对不上（而记录数就是 nonce 序列）。
    """
    out = b""
    n = 0
    for pt in plaintexts:
        # 空载荷不占记录（`chunks` 给零段），与 Rust 侧同形。
        for chunk in [pt[i:i + MAX_PLAINTEXT] for i in range(0, len(pt), MAX_PLAINTEXT)]:
            out += seal(key, n, chunk)
            n += 1
    return out


def reassemble(key, plaintexts):
    """封一整段流再逐记录走回去，返回 `(线上字节, 重接的明文, 记录数)`。

    “逐记录走回去”这一步不是为了产出什么常量，而是为了**自检**：参照侧自己
    得先自洽（封得回、按长度头能切回原帧），否则冻进测试的基准本身就是坏的。
    """
    wire = stream(key, plaintexts)
    plain = b""
    off = 0
    n = 0
    while off < len(wire):
        l = int.from_bytes(wire[off:off + REC_HDR], "little")
        plain += open_record(key, n, wire[off:off + REC_HDR + l])
        off += REC_HDR + l
        n += 1
    return wire, plain, n


# 冻结进测试的三组数据。尺寸选得有理由：
#  - SMALL：远小于一个记录，能整段贴进源码逐字节比。
#  - TAIL：正好跨过一个 64 字节块边界（1 个完整块 + 21 字节），零填充那条最容易错。
#  - MULTI：超过 MAX_PLAINTEXT，必须被切成整数个记录。
SMALL = [pattern(1, 7), pattern(28, 0x31), pattern(64, 200), pattern(140, 3)]
TAIL = [pattern(85, 11)]
MULTI = [pattern(65_536, 5), pattern(65_537, 6)]


def main():
    if "--selftest" in sys.argv:
        # 参照侧自己得先自洽：封了再解要回来，且跨记录流要能重接。
        k_cli, k_srv = dir_keys(KEY, CN, SN)
        for name, pts in (("SMALL", SMALL), ("TAIL", TAIL), ("MULTI", MULTI)):
            wire, plain, n = reassemble(k_srv, pts)
            assert plain == b"".join(pts), name
            assert n >= len(pts), (name, n, len(pts))
        print("selftest ok: %d records over %d streams"
              % (len(SMALL) + len(TAIL) + len(MULTI), 3))
        return 0

    k_cli, k_srv = dir_keys(KEY, CN, SN)
    print("// 由 benches/ximpl_chachapoly.py 生成（python3 + cryptography）。")
    print("const KEY_HEX: &str = \"%s\";" % KEY.hex())
    print("const CN_HEX: &str = \"%s\";" % CN.hex())
    print("const SN_HEX: &str = \"%s\";" % SN.hex())
    print("const K_CLI_HEX: &str = \"%s\";" % k_cli.hex())
    print("const K_SRV_HEX: &str = \"%s\";" % k_srv.hex())
    print("const SALT_HEX: &str = \"%s\";"
          % hkdf(EXTRACT_SALT, PREFIX + CN + SN, INFO_SALT).hex())
    # 64 字节输出 = T(1) ‖ T(2)：它钉的是 `hkdf_expand` 里那个轮次字节 `i`，
    # 写错成常量就会得出不等于 RFC 5869 的拼接（而两边自洽、单看 32 字节看不出来）。
    ikm = PREFIX + CN + SN
    salt64 = hkdf(EXTRACT_SALT, ikm, INFO_SALT)
    print("const K_CLI_64_HEX: &str = \"%s\";"
          % hkdf(salt64, KEY, INFO_CLI, 64).hex())

    for name, pts in (("SMALL", SMALL), ("TAIL", TAIL), ("MULTI", MULTI)):
        wire, plain, n = reassemble(k_srv, pts)
        assert plain == b"".join(pts)
        print("// %s: %d 段明文 -> %d 个记录, %d B 线上字节"
              % (name, len(pts), n, len(wire)))
        print("const %s_RECORDS: usize = %d;" % (name, n))
        for i, p in enumerate(pts):
            print("const %s_PT%d_LEN: usize = %d;" % (name, i, len(p)))
            if len(p) <= 256:
                print("const %s_PT%d: &str = \"%s\";" % (name, i, p.hex()))
            print("// %s_PT%d sha256 = %s"
                  % (name, i, hashlib.sha256(p).hexdigest()))
        print("const %s_WIRE_SHA: &str = \"%s\";"
              % (name, hashlib.sha256(wire).hexdigest()))
        if len(wire) <= 1024:
            print("const %s_WIRE: &str = \"%s\";" % (name, wire.hex()))
        # 反向：客户端方向封、服务端方向解（两条 info 不能算成同一种东西）。
        cli_wire = stream(k_cli, pts)
        print("const %s_CLI_WIRE_SHA: &str = \"%s\";"
              % (name, hashlib.sha256(cli_wire).hexdigest()))
        if len(cli_wire) <= 1024:
            print("const %s_CLI_WIRE: &str = \"%s\";"
                  % (name, cli_wire.hex()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
