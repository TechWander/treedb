//! 视图名 ↔ PG schema 名的编解码。
//!
//! 编码规则与 `grovefs::pg_store::schema_for` / `grovefs::control::schema_name_for`
//! **字节一致**：`grovefs_v_` + 视图名的 lowercase hex。理由：
//!
//! 1. **互解**：treedb 与 grovefs 主进程可能同时打开同一份 PG，schema 名必须
//!    是同一个纯函数，否则会出现"同名不同 schema"的孤儿。
//! 2. **注入安全**：hex 只输出 `[0-9a-f]`，永不需要 quote，也永不能通过视图
//!    名里的引号/点伪造。
//! 3. **长度约束**：PG 标识符上限 63 字节，前缀 10 + 2 字节/字符 → 视图名最长
//!    25 字节；超出直接拒绝（与原实现同一约束）。
//!
//! treedb 独立提供这份实现，是为了让 crate 可以脱离 grovefs 单测；接入时
//! 上层可以直接把 `grovefs::pg_store::schema_for` 的输出传给本模块的解码器，
//! 反之亦然。

/// 视图名最长 25 字节（PG 63 字节标识符减去 `grovefs_v_` 前缀 10 字节再除以 2）。
pub const VIEW_NAME_MAX: usize = 25;

/// schema 名前缀，与原实现一致。
pub const SCHEMA_PREFIX: &str = "grovefs_v_";

/// refs 账本 schema 名，与 `pg_store::PG_REFS_SCHEMA` 一致。
pub const REFS_SCHEMA: &str = "grovefs_refs";

/// 控制面 schema 名，与 `control::CONTROL_SCHEMA` 一致。
pub const CONTROL_SCHEMA: &str = "grovefs_control";

/// 视图名合法字符（简化版）：非空、长度 ≤ 25、不含路径分隔。真实规则在
/// grovefs 的 `layout::valid_view_name`，treedb 独立开发期用这份即可。
fn valid_view_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= VIEW_NAME_MAX && !s.contains('/') && !s.contains('\0')
}

/// 视图名 → schema 名（`grovefs_v_<hex>`）。非法名返回 `None`。
pub fn encode(view: &str) -> Option<String> {
    if !valid_view_name(view) {
        return None;
    }
    let mut hex = String::with_capacity(view.len() * 2);
    for b in view.as_bytes() {
        hex.push_str(&format!("{b:02x}"));
    }
    Some(format!("{SCHEMA_PREFIX}{hex}"))
}

/// schema 名 → 视图名。非本工具链产物返回 `None`，避免把外部同名前缀
/// schema 误认成可挂载视图。
pub fn decode(schema: &str) -> Option<String> {
    let hex = schema.strip_prefix(SCHEMA_PREFIX)?;
    if hex.is_empty() || hex.len() % 2 != 0 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks(2) {
        let s = std::str::from_utf8(pair).ok()?;
        bytes.push(u8::from_str_radix(s, 16).ok()?);
    }
    let view = String::from_utf8(bytes).ok()?;
    valid_view_name(&view).then_some(view)
}

/// 事务级 schema 切换 SQL：`SET LOCAL search_path TO <schema>, grovefs_refs`。
/// 双 schema 是为让 fork/graft 在同一事务里既访问视图树又访问 refs 账本，
/// 与原 `pg_store::ensure_schema` 里跨 schema 引用一致。
///
/// 走 `SET LOCAL` 而非 `SET`：只在当前事务内有效，COMMIT/ROLLBACK 自动失效，
/// 归还到池时不需要 RESET，也不会污染下一个借用者。
pub fn set_local_path(schema: &str) -> String {
    format!("SET LOCAL search_path TO \"{schema}\", {REFS_SCHEMA}")
}

/// 只访问控制面（api_keys/mcp_calls/view_defs/view_placement）的事务：把
/// 路径固定到 grovefs_control，避免用户视图 schema 干扰。
pub fn set_local_control() -> String {
    format!("SET LOCAL search_path TO {CONTROL_SCHEMA}")
}

/// DDL 用 `CREATE SCHEMA IF NOT EXISTS` + 目标 schema 的表；调用方按 SQL 数组
/// 传入即可，与原 grovefs 的 DDL_LOCK_CLASS 序列化配合。
pub fn ensure_schema_ddl(schema: &str) -> Vec<String> {
    vec![
        format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\""),
        format!("CREATE TABLE IF NOT EXISTS \"{schema}\".node (
            id BIGSERIAL PRIMARY KEY,
            parent_id BIGINT,
            name TEXT NOT NULL,
            is_dir BIGINT NOT NULL DEFAULT 0,
            data_ref TEXT,
            link_target TEXT,
            mode BIGINT NOT NULL DEFAULT 0,
            uid BIGINT NOT NULL DEFAULT 0,
            gid BIGINT NOT NULL DEFAULT 0,
            mtime BIGINT NOT NULL DEFAULT 0,
            atime BIGINT NOT NULL DEFAULT 0,
            ctime BIGINT NOT NULL DEFAULT 0,
            ino BIGINT NOT NULL DEFAULT 0
        )"),
        format!("CREATE UNIQUE INDEX IF NOT EXISTS nn_{schema}
                 ON \"{schema}\".node(parent_id, name)"),
        format!("CREATE INDEX IF NOT EXISTS np_{schema}
                 ON \"{schema}\".node(parent_id)"),
        format!("CREATE TABLE IF NOT EXISTS \"{schema}\".locks (
            id BIGSERIAL PRIMARY KEY,
            ino BIGINT NOT NULL,
            owner TEXT NOT NULL,
            kind TEXT NOT NULL,
            lease_expiry BIGINT NOT NULL
        )"),
        format!("CREATE TABLE IF NOT EXISTS \"{schema}\".ctrl (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_ascii() {
        let s = encode("alice").unwrap();
        assert_eq!(s, format!("{SCHEMA_PREFIX}616c696365"));
        assert_eq!(decode(&s).as_deref(), Some("alice"));
    }

    #[test]
    fn round_trip_utf8() {
        let s = encode("队a").unwrap();
        assert_eq!(decode(&s).as_deref(), Some("队a"));
    }

    #[test]
    fn rejects_bad_names() {
        assert!(encode("").is_none());
        assert!(encode("a/b").is_none());
        assert!(encode(&"x".repeat(VIEW_NAME_MAX + 1)).is_none());
        assert!(encode(&"x".repeat(VIEW_NAME_MAX)).is_some());
    }

    #[test]
    fn decode_rejects_foreign() {
        assert!(decode("public").is_none());
        assert!(decode(SCHEMA_PREFIX).is_none());
        assert!(decode("grovefs_v_zz").is_none());
        assert!(decode("grovefs_v_616").is_none()); // 奇数长度
    }

    #[test]
    fn set_local_targets_view_and_refs_schema() {
        let sql = set_local_path("grovefs_v_616c696365");
        assert!(sql.starts_with("SET LOCAL search_path TO \""));
        assert!(sql.contains(", grovefs_refs"));
    }
}
