//! 字节的入口与出口：命令行上的一个参数怎么变成 `Vec<u8>`，一行怎么变回给人看的字。
//!
//! 为什么这块要单独立一个文件：引擎的列值是**不透明字节**（§11 T2），它不知道也
//! 不关心一列里装的是名字、时间戳还是摘要。于是一个面向人的 CLI 必须自己回答两件
//! 事，而且两件都不能含糊：
//!
//! 1. **进**：运维在键盘上打不出一个 `0x00`，但可以表达它。这里的规则是「前缀决定
//!    解释方式」——`hex:` 整段按十六进制读，`@path` 整段是文件内容，其余按文本读并
//!    认 `\xNN`/`\n`/`\t`/`\0`/`\\` 五个转义。文本里不认的 `\` 序列一律报错而不是
//!    原样留下：静默吞掉一个写错的转义，等于把运维以为存进去的字节和真正存进去的
//!    字节分成两件事，而事后从 `RowGet` 里看不出是哪一件。
//! 2. **出**：一列值可能是文本也可能是二进制。默认「可打印 UTF-8 就当文本印，否则印
//!    `hex:`」，判据见 [`render_bytes`]。这条判据是单向的 —— 文本与「恰好可打印的二
//!    进制字节」不可区分（引擎不存类型），所以这里只承诺「印出来的文本按 [`parse_value`]
//!    再读回去能得到同一串字节」，不承诺能还原出当初那种写法。
//!
//! 两者合起来构成一个往返律，被 `value::tests::round_trip_*` 钉住：
//! `parse_value(&render_bytes(v)) == v` 对任意**没被截断**的 `v` 成立。唯一的例外是
//! 二进制值超过 [`ELIDE_AFTER`] 那一档 —— 屏幕上本来就没放全，自然粘不回去，要无损
//! 就用 `--json`（`json_value` 两条都不截）。运维照着屏幕上的输出重敲一条命令，不该
//! 改变内容；只有「一个十万字节的 payload」这一种情况除外，而那时人本来就该看 `--json`。

use std::path::Path;

/// 解析一个人手打的字节值。
///
/// 规则（按前缀，大小写不敏感）：
/// - `hex:` / `h:` —— 后面必须是偶数个十六进制字符，允许空格分隔（`hex:0a 1f`）。
/// - `@path` —— 整个文件内容。给的是目录或读不到就报错，不退化成空值。
/// - 整体被一对 `'…'` 或 `"…"` 包住 —— 剥掉引号后再走下面的转义规则。这一条是
///   为了**进出对称**：[`render_bytes`] 对含空白的文本会印成 Rust 风格的 `"a b"`，对
///   空串会印成 `''`，人在终端上选中粘回去必须得到同一串字节。
/// - 其余 —— 按 UTF-8 文本读，认 `\xNN`、`\n`、`\r`、`\t`、`\0`、`\\`、`\"`、`\'`。
///   空串是合法值（引擎那边空 `Vec<u8>` 本来就是一列的合法取值）。
pub fn parse_value(s: &str) -> Result<Vec<u8>, String> {
    if let Some(rest) = strip_prefix_ci(s, "hex:") {
        return parse_hex(rest);
    }
    if let Some(rest) = strip_prefix_ci(s, "h:") {
        return parse_hex(rest);
    }
    if let Some(path) = s.strip_prefix('@') {
        if path.is_empty() {
            return Err("`@` needs a path after it".to_string());
        }
        return std::fs::read(Path::new(path)).map_err(|e| format!("read {path}: {e}"));
    }
    if let Some(inner) = unquote(s) {
        return unescape(inner);
    }
    unescape(s)
}

/// 整对引号 → 内部。不配对（只一端有、或长度 < 2）返 `None`，走字面量那条路。
fn unquote(s: &str) -> Option<&str> {
    let b = s.as_bytes();
    if b.len() >= 2 && (b[0] == b'\'' || b[0] == b'"') && b[0] == b[b.len() - 1] {
        Some(&s[1..s.len() - 1])
    } else {
        None
    }
}

/// 十六进制段的长度上限：一帧最多 [`crate::engine::MAX_FRAME`]，而这里是给人用的
/// 命令行，超过 1 MiB 的十六进制参数只可能是把整个文件误粘进了终端。给它一个上界，
/// 报错也比让内核收到一个撑爆帧的请求好。
const HEX_MAX: usize = 1 << 20;

fn parse_hex(s: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(s.len() / 2);
    let mut cur: Option<u8> = None;
    for ch in s.chars() {
        if ch.is_whitespace() {
            continue;
        }
        let d = ch
            .to_digit(16)
            .ok_or_else(|| format!("hex value has a non-hex character {ch:?}"))?;
        match cur.take() {
            None => cur = Some(d as u8),
            Some(hi) => out.push((hi << 4) | d as u8),
        }
        if out.len() > HEX_MAX {
            return Err(format!("hex value over {HEX_MAX} bytes"));
        }
    }
    if cur.is_some() {
        return Err("hex value has an odd number of digits".to_string());
    }
    Ok(out)
}

fn strip_prefix_ci<'a>(s: &'a str, pre: &str) -> Option<&'a str> {
    // 用 `get` 而不是直接切片：前缀长度完全可能落在一个多字节字符中间（值是任意 UTF-8），
    // 直接切会在「解析用户输入」这条路上 panic。`..pre.len()` 能取到就说明是字符边界，
    // 下面那个索引才安全。
    let head = s.get(..pre.len())?;
    if head.eq_ignore_ascii_case(pre) {
        Some(&s[pre.len()..])
    } else {
        None
    }
}

fn unescape(s: &str) -> Result<Vec<u8>, String> {
    let mut out = Vec::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c != '\\' {
            push_char(&mut out, c);
            continue;
        }
        match it.next() {
            None => return Err("value ends with a lone backslash".to_string()),
            Some('n') => out.push(b'\n'),
            Some('r') => out.push(b'\r'),
            Some('t') => out.push(b'\t'),
            Some('0') => out.push(0),
            Some('\\') => out.push(b'\\'),
            // 两个引号转义存在的理由同上：`{:?}` 会把值里的 `"` 写成 `\"`，粘回来
            // 必须还是那个 `"`。
            Some('"') => out.push(b'"'),
            Some('\'') => out.push(b'\''),
            Some('x') => {
                let hi = hex_digit(it.next().ok_or("empty \\x escape")?)?;
                let lo = hex_digit(it.next().ok_or("short \\x escape: want two digits")?)?;
                out.push((hi << 4) | lo);
            }
            Some(o) => {
                return Err(format!(
                    "unknown escape \\{o} (recognized: \\xNN \\n \\r \\t \\0 \\\\ \" \')"
                ))
            }
        }
    }
    Ok(out)
}

fn hex_digit(c: char) -> Result<u8, String> {
    c.to_digit(16)
        .map(|d| d as u8)
        .ok_or_else(|| format!("\\x escape has a non-hex character {c:?}"))
}

fn push_char(out: &mut Vec<u8>, c: char) {
    let mut buf = [0u8; 4];
    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
}

/// 这串字节是不是「可以原样给人看」的文本：合法 UTF-8，且没有一个控制字符（ASCII
/// 空白之外的）或 DEL。
///
/// 为什么控制字符算不可看：一个 `0x01` 混在文本里终端上什么都看不见，运维会把它读
/// 成「这一列是空的」。宁可整列换成 `hex:`，让人一眼看出它不是文本。
pub fn is_text(v: &[u8]) -> bool {
    match std::str::from_utf8(v) {
        Err(_) => false,
        Ok(s) => s.chars().all(|c| !c.is_control() || c == ' ' || c == '\t'),
    }
}

/// 给人看的形状：文本直接印（含空白时加引号），否则 `hex:`。
///
/// 超过 [`ELIDE_AFTER`] 字节就截断并标注剩多少 —— 一次 `RowList` 印十万字节的
/// payload 会把终端刷爆，而人真正要看的是开头那点。要全量就 `--json`。
pub const ELIDE_AFTER: usize = 120;

pub fn render_bytes(v: &[u8]) -> String {
    if is_text(v) {
        let s = String::from_utf8_lossy(v);
        if s.is_empty() {
            return "''".to_string();
        }
        if s.chars().all(|c| !c.is_whitespace()) {
            return s.into_owned();
        }
        return format!("{s:?}");
    }
    let take = v.len().min(ELIDE_AFTER);
    let mut out = String::with_capacity(take * 2 + 8);
    out.push_str("hex:");
    for b in &v[..take] {
        out.push_str(&format!("{b:02x}"));
    }
    if v.len() > take {
        out.push_str(&format!("…(+{} B)", v.len() - take));
    }
    out
}

/// 值在 JSON 里的样子：一律 `{"hex":…,"utf8":…|null}` 一项。
///
/// 不做「是文本就印成字符串」那种省事写法，理由与 [`render_bytes`] 的单向性同一条：
/// 机器读者要能无歧义地拿回原始字节。UTF-8 文本里混一个 `0x00` 时，`utf8` 字段是不
/// 合法的 JSON 字符串，所以这里给的是「可打印文本或 null」，而 `hex` 永远准。
pub fn json_value(v: &[u8]) -> String {
    let text = if is_text(v) {
        json_str(&String::from_utf8_lossy(v))
    } else {
        "null".to_string()
    };
    format!("{{\"hex\":\"{}\",\"utf8\":{}}}", hex_of(v), text)
}

pub fn hex_of(v: &[u8]) -> String {
    let mut s = String::with_capacity(v.len() * 2);
    for b in v {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// 一个可能含任意字节的名字在 JSON 里怎么放：能当文本就文本，否则加 `hex:` 前缀。
///
/// 与 [`json_value`] 的区别是这里必须落在**一个**字符串里（键名与表名没有地方挂第二
/// 个字段），所以前缀承担了「这不是字面量」的信号。
pub fn json_name(v: &[u8]) -> String {
    if is_text(v) {
        json_str(&String::from_utf8_lossy(v))
    } else {
        json_str(&format!("hex:{}", hex_of(v)))
    }
}

/// 最小的 JSON 字符串转义：控制字符全走 `\u00XX`，引号反斜杠走常规写法。
///
/// 不引 serde 是依赖门（`tests/independence.rs` 门 2）逼的，也让这个函数能被单测逐
/// 字节钉住。
pub fn json_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// 表格单元格里的一格：太宽就截，保持列对齐。
pub fn cell(v: &[u8], width: usize) -> String {
    let s = render_bytes(v);
    let w = s.chars().count();
    if w <= width {
        return s;
    }
    let mut cut: String = s.chars().take(width.saturating_sub(1)).collect();
    cut.push('…');
    cut
}

/// 按最长内容对齐的一列（末尾不补空格，行尾多余空白对 `less` 与 diff 都不友好）。
pub fn pad(s: &str, width: usize) -> String {
    let w = s.chars().count();
    if w >= width {
        s.to_string()
    } else {
        format!("{s}{}", " ".repeat(width - w))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_each_prefix() {
        assert_eq!(parse_value("abc").unwrap(), b"abc");
        assert_eq!(parse_value("").unwrap(), b"");
        assert_eq!(parse_value("hex:00ff").unwrap(), vec![0, 255]);
        assert_eq!(parse_value("HEX:0A ff").unwrap(), vec![10, 255]);
        assert_eq!(parse_value("h:2f").unwrap(), vec![b'/']);
        assert_eq!(parse_value(r"a\nb").unwrap(), b"a\nb");
        assert_eq!(parse_value(r"\x00\\x").unwrap(), vec![0, b'\\', b'x']);
        assert_eq!(parse_value(r"tab\there").unwrap(), b"tab\there");
    }

    #[test]
    fn rejects_malformed_values() {
        // 报错而不是猜：每一种都必须留下可执行的提示。
        assert!(parse_value("hex:0").is_err(), "奇数位");
        assert!(parse_value("hex:zz").is_err(), "非十六进制");
        assert!(parse_value(r"a\qb").is_err(), "未知转义");
        assert!(parse_value(r"tail\").is_err(), "结尾孤立反斜杠");
        assert!(parse_value(r"\x0").is_err(), "\\x 只给一位");
        assert!(parse_value("@").is_err(), "@ 后面没路径");
        assert!(parse_value("@/definitely/not/here/treedb").is_err(), "文件不存在");
    }

    #[test]
    fn renders_text_and_hex() {
        assert_eq!(render_bytes(b"hello"), "hello");
        assert_eq!(render_bytes(b""), "''");
        assert_eq!(render_bytes(b"two words"), "\"two words\"");
        assert_eq!(render_bytes(&[0, 1, 2]), "hex:000102");
        // 控制字符必须换成 hex：留在文本里就等于看不见。
        assert_eq!(render_bytes(b"a\x01b"), "hex:610162");
        // 空格与制表算可看，换行不算（它会把一行拆成两行）。
        assert!(is_text(b"a b"));
        assert!(!is_text(b"a\nb"));
        // 非法 UTF-8 一定是 hex。
        assert!(!is_text(&[0xff, 0xfe]));
    }

    #[test]
    fn round_trip_printable_hex_and_text() {
        // 往返律：屏幕上看到的字，按本文件的规则再敲一遍必须得回原字节。
        // 只收「能印全」的那些：二进制超 ELIDE_AFTER 会被省略，那一档由
        // `elides_long_values` 单独测（它测的正是「不许假装能粘回去」）。
        let cases: Vec<Vec<u8>> = vec![
            b"".to_vec(),
            b"abc".to_vec(),
            b"two words".to_vec(),
            vec![0],
            vec![0xde, 0xad, 0xbe, 0xef],
            (0..=ELIDE_AFTER as u8 - 1).collect(),
            "非ASCII文本".as_bytes().to_vec(),
            b"it's \"quoted\"".to_vec(),
        ];
        for v in cases {
            let got = parse_value(&render_bytes(&v)).unwrap_or_else(|e| {
                panic!("render_bytes({v:?}) = {} 读不回去: {e}", render_bytes(&v))
            });
            assert_eq!(got, v, "往返不一致：{}", render_bytes(&v));
        }
    }

    #[test]
    fn elides_long_values() {
        let v = vec![7u8; ELIDE_AFTER + 300];
        let s = render_bytes(&v);
        assert!(s.starts_with("hex:") && s.contains("(+300 B)"), "{s}");
        // 被截断的那一档**故意**不满足往返律：屏幕上没放全的字节不可能敲回来。
        // 报错而不是默默给一截 —— 后者会让运维以为存进去的就是屏幕上那 120 字节。
        assert!(parse_value(&s).is_err(), "截断后的渲染必须读不回去: {s}");
        // 无损出口是 --json。
        assert_eq!(
            json_value(&v),
            format!("{{\"hex\":\"{}\",\"utf8\":null}}", "07".repeat(v.len()))
        );
        assert!(cell(b"abcdefghij", 5).ends_with('…'));
        assert_eq!(pad("ab", 5), "ab   ");
    }

    #[test]
    fn json_shapes_are_escaped() {
        assert_eq!(json_str("a\"b\\c\n"), "\"a\\\"b\\\\c\\n\"");
        assert_eq!(json_str("\u{1}"), "\"\\u0001\"");
        assert_eq!(json_name(b"ok"), "\"ok\"");
        assert_eq!(json_name(&[0xff]), "\"hex:ff\"");
        assert_eq!(json_value(b"ab"), "{\"hex\":\"6162\",\"utf8\":\"ab\"}");
        assert_eq!(json_value(&[0xff]), "{\"hex\":\"ff\",\"utf8\":null}");
    }
}
