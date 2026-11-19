//! 本地 SQLite 镜像：控制面四表的 read-only 副本。
//!
//! ## 表结构
//!
//! - `api_keys`：与 `grovefs_control.api_keys` 列一一对应，id 做 PRIMARY KEY
//!   方便 UPDATE 走索引。
//! - `mcp_calls_buffer`：审计行本地缓冲，PG 恢复后由上层 flush 到 PG，
//!   flush 成功后本地删除。
//! - `view_defs`：命名视图定义（lens/alias/fork 记录）。
//! - `view_placement`：视图 → 实例名映射（treedb 独立开发时不真做 placement
//!   写入，只镜像已存在行）。
//! - `mirror_meta`：一个 K/V 表记录最后同步戳，方便重启时判断落后多少。
//!
//! ## 一致性
//!
//! 所有 `write_through_*` 方法都是**幂等**的 `INSERT OR REPLACE` 或
//! `UPDATE`；重复应用一条 CREATE 不会破坏状态。这让上层在 PG 提交后异步
//! 镜像、失败重试时不用维护"是否已镜像"标志。
//!
//! ## 隔离
//!
//! `Mirror` 内部 `Mutex<Connection>`，`Send + Sync`。SQLite 的 WAL mode
//! 允许单写多读；多线程共享同一份镜像不会 SQLITE_BUSY。

use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};

/// 本地控制面镜像。
pub struct Mirror {
    conn: parking_lot::Mutex<Connection>,
    dir: PathBuf,
}

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS api_keys (
    id           TEXT PRIMARY KEY,
    label        TEXT NOT NULL,
    secret_hash  TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    last_used_at TEXT,
    revoked_at   TEXT,
    scope        TEXT NOT NULL DEFAULT 'any',
    view_name    TEXT,
    ro           INTEGER NOT NULL DEFAULT 0,
    owner        TEXT,
    note         TEXT,
    expires_at   TEXT,
    created_by   TEXT,
    secret_hint  TEXT
);
CREATE TABLE IF NOT EXISTS mcp_calls_buffer (
    seq         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT NOT NULL,
    key_id      TEXT,
    key_label   TEXT,
    method      TEXT NOT NULL,
    tool        TEXT NOT NULL,
    ok          INTEGER NOT NULL,
    detail      TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS view_defs (
    name    TEXT PRIMARY KEY,
    kind    TEXT NOT NULL,
    payload TEXT NOT NULL,
    updated TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS view_placement (
    view      TEXT PRIMARY KEY,
    instance  TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mirror_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
";

impl Mirror {
    /// 打开或创建 `dir/control_mirror.db`，幂等建表。dir 不存在会创建。
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, crate::error::Error> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)
            .map_err(|e| crate::Error::Mirror(format!("{}: {e}", dir.display())))?;
        let path = dir.join("control_mirror.db");
        let conn = Connection::open(&path)
            .map_err(|e| crate::Error::Mirror(format!("{}: {e}", path.display())))?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA busy_timeout=5000;",
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        conn.execute_batch(SCHEMA)
            .map_err(|e| crate::Error::Mirror(format!("schema: {e}")))?;
        Ok(Self {
            conn: parking_lot::Mutex::new(conn),
            dir,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    // ---- api_keys write-through ---------------------------------------

    /// 密钥创建/更新的 write-through（幂等 REPLACE）。列顺序与
    /// `grovefs_control.api_keys` 一一对应。
    #[allow(clippy::too_many_arguments)]
    pub fn upsert_key(
        &self,
        id: &str,
        label: &str,
        secret_hash: &str,
        created_at: &str,
        last_used_at: Option<&str>,
        revoked_at: Option<&str>,
        scope: &str,
        view_name: Option<&str>,
        ro: bool,
        owner: Option<&str>,
        note: Option<&str>,
        expires_at: Option<&str>,
        created_by: Option<&str>,
        secret_hint: Option<&str>,
    ) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "INSERT OR REPLACE INTO api_keys
             (id,label,secret_hash,created_at,last_used_at,revoked_at,scope,
              view_name,ro,owner,note,expires_at,created_by,secret_hint)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
            params![
                id,
                label,
                secret_hash,
                created_at,
                last_used_at,
                revoked_at,
                scope,
                view_name,
                ro as i32,
                owner,
                note,
                expires_at,
                created_by,
                secret_hint
            ],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    pub fn revoke_key(&self, id: &str, revoked_at: &str) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "UPDATE api_keys SET revoked_at=?2 WHERE id=?1",
            params![id, revoked_at],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    pub fn set_ro(&self, id: &str, ro: bool) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "UPDATE api_keys SET ro=?2 WHERE id=?1",
            params![id, ro as i32],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    pub fn touch_last_used(&self, id: &str, ts: &str) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "UPDATE api_keys SET last_used_at=?2 WHERE id=?1",
            params![id, ts],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    /// fallback 认证：读取一行验证 hash 匹配、未 revoked、未过期。返回
    /// `None` 表示本地也没有这行（等价 PG 也不认）；`Some(outcome)` 携带
    /// 具体拒绝原因或成功后的属性。
    pub fn authenticate_fallback(
        &self,
        id: &str,
        secret_hash: &str,
        now_ms: i64,
    ) -> Result<Option<FallbackOutcome>, crate::Error> {
        let c = self.conn.lock();
        let mut stmt = c
            .prepare(
                "SELECT secret_hash, revoked_at, expires_at, ro, view_name, owner, scope, label
                 FROM api_keys WHERE id=?1",
            )
            .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        let row = stmt
            .query_row(params![id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, Option<String>>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, i32>(3)? != 0,
                    r.get::<_, Option<String>>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, String>(6)?,
                    r.get::<_, String>(7)?,
                ))
            })
            .ok();
        let Some((hash, revoked_at, expires_at, ro, view, owner, scope, label)) = row else {
            return Ok(None);
        };
        if hash != secret_hash {
            return Ok(Some(FallbackOutcome::BadSecret));
        }
        if revoked_at.is_some() {
            return Ok(Some(FallbackOutcome::Revoked));
        }
        if let Some(raw) = &expires_at {
            match chrono::DateTime::parse_from_rfc3339(raw) {
                Ok(dt) if dt.timestamp_millis() > now_ms => {}
                _ => return Ok(Some(FallbackOutcome::Expired)),
            }
        }
        Ok(Some(FallbackOutcome::Ok {
            label,
            ro,
            view,
            owner,
            scope,
        }))
    }

    /// 列出所有未撤销的 key id（诊断/审计快照用）。
    pub fn live_key_ids(&self) -> Result<Vec<String>, crate::Error> {
        let c = self.conn.lock();
        let mut stmt = c
            .prepare("SELECT id FROM api_keys WHERE revoked_at IS NULL ORDER BY id")
            .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        let ids = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| crate::Error::Mirror(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(ids)
    }

    // ---- audit buffer --------------------------------------------------

    pub fn buffer_audit(
        &self,
        ts: &str,
        key_id: Option<&str>,
        key_label: Option<&str>,
        method: &str,
        tool: &str,
        ok: bool,
        detail: &str,
    ) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "INSERT INTO mcp_calls_buffer (ts,key_id,key_label,method,tool,ok,detail)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![ts, key_id, key_label, method, tool, ok as i32, detail],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    /// 拉一批待 flush 的审计行；调用方拿到 seq 用于回写删除。
    #[allow(clippy::type_complexity)]
    pub fn pending_audit(
        &self,
        limit: usize,
    ) -> Result<Vec<(i64, String, Option<String>, Option<String>, String, String, bool, String)>, crate::Error>
    {
        let c = self.conn.lock();
        let mut stmt = c
            .prepare(
                "SELECT seq, ts, key_id, key_label, method, tool, ok, detail
                 FROM mcp_calls_buffer ORDER BY seq LIMIT ?1",
            )
            .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        let rows = stmt
            .query_map(params![limit as i64], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, String>(5)?,
                    r.get::<_, i32>(6)? != 0,
                    r.get::<_, String>(7)?,
                ))
            })
            .map_err(|e| crate::Error::Mirror(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(rows)
    }

    pub fn delete_audit_below_seq(&self, seq: i64) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "DELETE FROM mcp_calls_buffer WHERE seq <= ?1",
            params![seq],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    // ---- view_defs / placement -----------------------------------------

    pub fn upsert_def(
        &self,
        name: &str,
        kind: &str,
        payload: &str,
    ) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "INSERT OR REPLACE INTO view_defs (name,kind,payload,updated)
             VALUES (?1,?2,?3,?4)",
            params![name, kind, payload, crate::persist::now_iso()],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    pub fn get_def(&self, name: &str) -> Result<Option<(String, String)>, crate::Error> {
        let c = self.conn.lock();
        let mut stmt = c
            .prepare("SELECT kind, payload FROM view_defs WHERE name=?1")
            .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        let row = stmt
            .query_row(params![name], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .ok();
        Ok(row)
    }

    pub fn upsert_placement(
        &self,
        view: &str,
        instance: &str,
    ) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "INSERT OR REPLACE INTO view_placement (view,instance,updated_at)
             VALUES (?1,?2,?3)",
            params![view, instance, crate::persist::now_iso()],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    // ---- meta ----------------------------------------------------------

    pub fn set_meta(&self, key: &str, value: &str) -> Result<(), crate::Error> {
        let c = self.conn.lock();
        c.execute(
            "INSERT OR REPLACE INTO mirror_meta (key,value) VALUES (?1,?2)",
            params![key, value],
        )
        .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        Ok(())
    }

    pub fn get_meta(&self, key: &str) -> Result<Option<String>, crate::Error> {
        let c = self.conn.lock();
        let mut stmt = c
            .prepare("SELECT value FROM mirror_meta WHERE key=?1")
            .map_err(|e| crate::Error::Mirror(e.to_string()))?;
        let v = stmt
            .query_row(params![key], |r| r.get::<_, String>(0))
            .ok();
        Ok(v)
    }

    /// 快照：`VACUUM INTO <dir>/snapshot-<ts>.db`。返回新文件路径。
    pub fn snapshot(&self) -> Result<PathBuf, crate::Error> {
        let name = crate::persist::snapshot_name(crate::persist::now_ms());
        let path = self.dir.join(&name);
        let c = self.conn.lock();
        c.execute("VACUUM INTO ?1", params![path.to_str().unwrap_or("snapshot.db")])
            .map_err(|e| crate::Error::Mirror(format!("vacuum into: {e}")))?;
        Ok(path)
    }

    /// 保留最近 N 份快照，其它删除。sweeper 每次快照后调用。
    pub fn prune_snapshots(&self, keep: usize) -> Result<usize, crate::Error> {
        if keep == 0 {
            return Ok(0);
        }
        let mut files: Vec<_> = std::fs::read_dir(&self.dir)
            .map_err(|e| crate::Error::Mirror(e.to_string()))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("control-") && n.ends_with(".mirror.db"))
            })
            .collect();
        files.sort();
        files.reverse(); // 名字含时间戳，字典序倒排即最新在前
        let mut removed = 0;
        for f in files.iter().skip(keep) {
            if std::fs::remove_file(f).is_ok() {
                removed += 1;
            }
        }
        Ok(removed)
    }
}

/// `authenticate_fallback` 的可能结果。
#[derive(Debug, Clone)]
pub enum FallbackOutcome {
    /// hash 匹配、未撤销、未过期。
    Ok {
        label: String,
        ro: bool,
        view: Option<String>,
        owner: Option<String>,
        scope: String,
    },
    BadSecret,
    Revoked,
    Expired,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> tempfile::TempDir {
        tempfile::tempdir().expect("tmpdir")
    }

    #[test]
    fn upsert_and_fallback_round_trip() {
        let d = tmp();
        let m = Mirror::open(d.path()).expect("open");
        m.upsert_key(
            "abc123",
            "test key",
            "hash-of-secret",
            "2026-01-01T00:00:00+00:00",
            None,
            None,
            "any",
            Some("alice"),
            false,
            Some("alice"),
            None,
            None,
            None,
            Some("ab..cd"),
        )
        .expect("upsert");
        let now = chrono::Utc::now().timestamp_millis() + 1_000;
        let outcome = m
            .authenticate_fallback("abc123", "hash-of-secret", now)
            .expect("query")
            .expect("row exists");
        match outcome {
            FallbackOutcome::Ok {
                label,
                view,
                owner,
                scope,
                ..
            } => {
                assert_eq!(label, "test key");
                assert_eq!(view.as_deref(), Some("alice"));
                assert_eq!(owner.as_deref(), Some("alice"));
                assert_eq!(scope, "any");
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[test]
    fn fallback_bad_secret() {
        let d = tmp();
        let m = Mirror::open(d.path()).unwrap();
        m.upsert_key(
            "id1", "l", "right", "2026-01-01T00:00:00Z", None, None, "any",
            None, false, None, None, None, None, None,
        )
        .unwrap();
        let out = m
            .authenticate_fallback("id1", "wrong", i64::MAX)
            .unwrap()
            .unwrap();
        assert!(matches!(out, FallbackOutcome::BadSecret));
    }

    #[test]
    fn fallback_revoked() {
        let d = tmp();
        let m = Mirror::open(d.path()).unwrap();
        m.upsert_key(
            "id1", "l", "h", "2026-01-01T00:00:00Z", None, Some("2026-06-01T00:00:00Z"),
            "any", None, false, None, None, None, None, None,
        )
        .unwrap();
        let out = m.authenticate_fallback("id1", "h", i64::MAX).unwrap().unwrap();
        assert!(matches!(out, FallbackOutcome::Revoked));
    }

    #[test]
    fn fallback_expired() {
        let d = tmp();
        let m = Mirror::open(d.path()).unwrap();
        m.upsert_key(
            "id1", "l", "h", "2026-01-01T00:00:00Z", None, None, "any",
            None, false, None, None, Some("2026-02-01T00:00:00Z"), None, None,
        )
        .unwrap();
        // now = 三月
        let out = m.authenticate_fallback("id1", "h", i64::MAX).unwrap().unwrap();
        assert!(matches!(out, FallbackOutcome::Expired));
    }

    #[test]
    fn audit_buffer_round_trip() {
        let d = tmp();
        let m = Mirror::open(d.path()).unwrap();
        for i in 0..5 {
            m.buffer_audit("2026-01-01T00:00:00Z", Some("k"), Some("lbl"), "tools/call", &format!("t{i}"), true, "ok")
                .unwrap();
        }
        let rows = m.pending_audit(3).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].7, "ok");
        m.delete_audit_below_seq(rows[2].0).unwrap();
        let rest = m.pending_audit(10).unwrap();
        assert_eq!(rest.len(), 2);
    }

    #[test]
    fn idempotent_upsert() {
        let d = tmp();
        let m = Mirror::open(d.path()).unwrap();
        m.upsert_key(
            "dup",
            "first",
            "hash",
            "2026-01-01T00:00:00Z",
            None,
            None,
            "any",
            None,
            false,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        m.upsert_key(
            "dup",
            "second",
            "hash",
            "2026-01-01T00:00:00Z",
            None,
            None,
            "any",
            None,
            false,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let ids = m.live_key_ids().unwrap();
        assert_eq!(ids.len(), 1);
        let out = m
            .authenticate_fallback("dup", "hash", i64::MAX)
            .unwrap()
            .unwrap();
        match out {
            FallbackOutcome::Ok { label, .. } => assert_eq!(label, "second"),
            _ => panic!("wrong outcome"),
        }
    }

    #[test]
    fn snapshot_prunes_old() {
        let d = tmp();
        let m = Mirror::open(d.path()).unwrap();
        let _p1 = m.snapshot().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let _p2 = m.snapshot().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(1100));
        let _p3 = m.snapshot().unwrap();
        let removed = m.prune_snapshots(2).unwrap();
        assert_eq!(removed, 1);
    }
}
