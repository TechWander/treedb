//! 本地持久化：SQLite 镜像 + 快照 + 工具时间戳。
//!
//! treedb 的容灾靠这份：PG 是唯一权威，但一份本地 SQLite 镜像承担
//! "PG 不可达时 hot path 不熄火"。镜像只放**控制面**（api_keys/mcp_calls/
//! view_defs/view_placement），视图树 schema 太大不划算镜像。
//!
//! - [`Mirror`]：`<mirror_dir>/control_mirror.db`，WAL mode。
//! - [`snapshot`]：把 api_keys 表 VACUUM INTO 到带时间戳的文件，供离线恢复。
//! - [`now_ms`]：crate 内共享的毫秒时间戳（不引 `Instant`，因为要落盘）。

pub mod mirror;

pub use mirror::Mirror;

/// crate 内共享的 epoch 毫秒戳，避免每个模块各自实现。
pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// ISO 8601 (RFC 3339) 本地时间戳，落盘/日志统一用这个格式。
pub fn now_iso() -> String {
    chrono::Local::now().to_rfc3339()
}

/// 快照文件名：`control-YYYYMMDDTHHMMSS.mirror.db`。
pub fn snapshot_name(ts_ms: u64) -> String {
    let secs = (ts_ms / 1000) as i64;
    let when = std::time::UNIX_EPOCH
        + std::time::Duration::from_secs(secs.max(0) as u64);
    let dt = chrono::DateTime::<chrono::Local>::from(when);
    format!(
        "control-{}.mirror.db",
        dt.format("%Y%m%dT%H%M%S")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn now_ms_monotonicish() {
        let a = now_ms();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let b = now_ms();
        assert!(b >= a);
    }

    #[test]
    fn snapshot_name_shape() {
        let s = snapshot_name(now_ms());
        assert!(s.starts_with("control-"));
        assert!(s.ends_with(".mirror.db"));
        assert_eq!(s.len(), "control-".len() + 15 + ".mirror.db".len());
    }
}
