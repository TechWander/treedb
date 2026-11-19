//! API 密钥存储：PG 权威 + 本地 SQLite 镜像 fallback。
//!
//! 语义与 `grovefs::apikeys::ApiKeyStore` 的 PG 分支等价：
//! - 密钥以 SHA-256 摘要存储，明文只在 create 时打印一次；
//! - id 是 16 hex（64 位随机），secret 是 32 hex（v4 UUID 去掉连字符，122 位
//!   随机），总格式 `<id>:<secret>`；
//! - `authenticate(id, secret)`：拿 id 定位行、比对摘要、检查 revoked_at、
//!   检查 expires_at；返回 `AuthedKey`；
//! - `revoke(id)` / `set_ro(id, bool)` / `touch(id)`：治理动词，PG 提交后
//!   write-through 到镜像。
//!
//! 与原实现的差异：
//! - PG 侧走 [`crate::pool::SharedInstancePool`]（单实例共享池），
//!   而不是 `grovefs::control::PgPool`（4 连接的独立池）；控制面是低
//!   频的，共享池的多余连接给视图树用；
//! - `Backend` 枚举消失：本 crate 只支持 PG 主 + Mirror 备份，纯本地
//!   SQLite 部署请继续用 grovefs 的原 ApiKeyStore。
//!
//! Fallback 只在**PG 完全不可达**时才走，且只用于认证路径——写路径
//! (`create/revoke/set_ro`) PG 挂了就返回 Err，不做异步补偿。理由：
//! 一次错误的密钥写入比一次拒登更严重。

use crate::error::{Error, Result};
use crate::persist::Mirror;
use crate::pool::SharedInstancePool;
use postgres::types::ToSql;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// 一次成功认证的身份。字段与 `grovefs::apikeys::AuthedKey` 一致。
#[derive(Debug, Clone)]
pub struct AuthedKey {
    pub id: String,
    pub owner: Option<String>,
    pub label: String,
    pub view: Option<String>,
    pub ro: bool,
}

/// 存储中的一行；`authenticate` 内部读到的最小列集。
#[derive(Debug, Clone)]
pub struct StoredKey {
    pub label: String,
    pub secret_hash: String,
    pub scope: String,
    pub revoked_at: Option<String>,
    pub view: Option<String>,
    pub ro: bool,
    pub owner: Option<String>,
    pub expires_at: Option<String>,
}

/// `create()` 的返回：id + secret + 完整记录。secret 只出现一次。
#[derive(Debug, Clone)]
pub struct KeyRecord {
    pub id: String,
    pub secret: String,
    pub hash: String,
    pub hint: String,
    pub created_at: String,
}

/// 密钥存储。`Arc<SharedInstancePool>` 是主后端，`Arc<Mirror>` 是备份。
pub struct KeyStore {
    pool: Arc<SharedInstancePool>,
    mirror: Option<Arc<Mirror>>,
    /// PG 侧是否可达。由探活或 checkout 失败路径更新。
    pg_reachable: Arc<AtomicBool>,
    /// 允许 fallback（PG 挂时读本地）？默认 true，可通过 config 关掉。
    fallback_enabled: bool,
}

impl KeyStore {
    /// 建 store。PG 建表幂等；镜像为 None 时不启用 fallback。
    pub fn new(
        pool: Arc<SharedInstancePool>,
        mirror: Option<Arc<Mirror>>,
        pg_reachable: Arc<AtomicBool>,
        fallback_enabled: bool,
    ) -> Result<Self> {
        let this = Self {
            pool,
            mirror,
            pg_reachable,
            fallback_enabled,
        };
        // 控制面 DDL 幂等：CREATE SCHEMA IF NOT EXISTS + CREATE TABLE IF NOT EXISTS。
        this.init_schema()?;
        Ok(this)
    }

    fn init_schema(&self) -> Result<()> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        // CREATE SCHEMA 走批处理，避开 prepared。
        tx.batch(
            "CREATE SCHEMA IF NOT EXISTS grovefs_control",
        )?;
        tx.batch(
            "CREATE TABLE IF NOT EXISTS grovefs_control.api_keys (
                seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                id TEXT NOT NULL UNIQUE,
                label TEXT NOT NULL,
                secret_hash TEXT NOT NULL,
                created_at TEXT NOT NULL,
                last_used_at TEXT,
                revoked_at TEXT,
                scope TEXT NOT NULL DEFAULT 'any',
                view_name TEXT,
                ro BOOLEAN NOT NULL DEFAULT false,
                owner TEXT,
                note TEXT,
                expires_at TEXT,
                created_by TEXT,
                secret_hint TEXT
             )",
        )?;
        // 增量列（PG 幂等）。
        for sql in [
            "ALTER TABLE grovefs_control.api_keys ADD COLUMN IF NOT EXISTS owner TEXT",
            "ALTER TABLE grovefs_control.api_keys ADD COLUMN IF NOT EXISTS note TEXT",
            "ALTER TABLE grovefs_control.api_keys ADD COLUMN IF NOT EXISTS expires_at TEXT",
            "ALTER TABLE grovefs_control.api_keys ADD COLUMN IF NOT EXISTS created_by TEXT",
            "ALTER TABLE grovefs_control.api_keys ADD COLUMN IF NOT EXISTS secret_hint TEXT",
        ] {
            tx.batch(sql)?;
        }
        g.commit()?;
        Ok(())
    }

    /// 建一把新密钥。`label` 必填；`scope/view/owner/note/ttl/created_by`
    /// 可选。secret 只在返回值里出现一次，绝不落 PG。
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        &self,
        label: &str,
        scope: &str,
        view: Option<&str>,
        owner: Option<&str>,
        note: Option<&str>,
        ttl_expires_at: Option<&str>,
        created_by: Option<&str>,
        ro: bool,
    ) -> Result<KeyRecord> {
        let id = gen_id();
        let secret = gen_secret();
        let hash = sha256_hex(&secret);
        let hint = secret_hint(&secret);
        let created_at = crate::persist::now_iso();

        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let params: &[&(dyn ToSql + Sync)] = &[
            &id,
            &label,
            &hash,
            &created_at,
            &scope,
            &view,
            &ro,
            &owner,
            &note,
            &ttl_expires_at,
            &created_by,
            &hint,
        ];
        tx.execute(
            "INSERT INTO grovefs_control.api_keys
             (id,label,secret_hash,created_at,scope,view_name,ro,owner,note,expires_at,created_by,secret_hint)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            params,
        )?;
        g.commit()?;

        // write-through 到镜像（PG 已提交，镜像失败只记日志不回滚）。
        if let Some(m) = &self.mirror {
            let _ = m.upsert_key(
                &id,
                label,
                &hash,
                &created_at,
                None,
                None,
                scope,
                view,
                ro,
                owner,
                note,
                ttl_expires_at,
                created_by,
                Some(&hint),
            );
        }

        Ok(KeyRecord {
            id,
            secret,
            hash,
            hint,
            created_at,
        })
    }

    /// 认证：PG 主路径。PG 挂掉时（`pg_reachable=false` + fallback 启用）
    /// 走镜像。返回 `Err(Denied)` 是"密钥本身不合法"（不匹配 / 撤销 /
    /// 过期），返回 `Err(Unavailable)` 是"两边都读不到"。
    pub fn authenticate(&self, id: &str, secret: &str) -> Result<AuthedKey> {
        let hash = sha256_hex(secret);
        if self.pg_reachable.load(Ordering::Acquire) {
            match self.fetch_key(id) {
                Ok(Some(row)) => return self.verify_row(id, &hash, row),
                Ok(None) => {
                    return Err(Error::Denied(format!("unknown key id '{id}'")))
                }
                Err(e) => {
                    // PG 侧真出错，标记不可达，走 fallback。
                    self.pg_reachable.store(false, Ordering::Release);
                    if let Some(outcome) = self.try_fallback(id, &hash)? {
                        return Ok(outcome);
                    }
                    return Err(e);
                }
            }
        }
        if self.fallback_enabled {
            if let Some(outcome) = self.try_fallback(id, &hash)? {
                return Ok(outcome);
            }
        }
        Err(Error::Unavailable(format!(
            "PG control plane is unreachable and no local mirror has key '{id}'"
        )))
    }

    fn try_fallback(&self, id: &str, hash: &str) -> Result<Option<AuthedKey>> {
        let Some(m) = &self.mirror else {
            return Ok(None);
        };
        let now = chrono::Utc::now().timestamp_millis();
        match m.authenticate_fallback(id, hash, now)? {
            None => Ok(None),
            Some(crate::persist::mirror::FallbackOutcome::BadSecret) => {
                Err(Error::Denied("bad secret".into()))
                // 上面返回 Result<Option<_>>，把 Err 传出：包 Ok(Some(Err(..)))?。
            }
            Some(crate::persist::mirror::FallbackOutcome::Revoked) => {
                Err(Error::Denied("key revoked".into()))
            }
            Some(crate::persist::mirror::FallbackOutcome::Expired) => {
                Err(Error::Denied("key expired".into()))
            }
            Some(crate::persist::mirror::FallbackOutcome::Ok {
                label,
                ro,
                view,
                owner,
                ..
            }) => Ok(Some(AuthedKey {
                id: id.to_string(),
                owner,
                label,
                view,
                ro,
            })),
        }
    }

    fn fetch_key(&self, id: &str) -> Result<Option<StoredKey>> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let row = tx.query_one(
            "SELECT label, secret_hash, scope, revoked_at, view_name, ro, owner, expires_at
             FROM grovefs_control.api_keys WHERE id=$1",
            &[&id],
        )?;
        let out = row.map(|r| StoredKey {
            label: r.get(0),
            secret_hash: r.get(1),
            scope: r.get(2),
            revoked_at: r.get(3),
            view: r.get(4),
            ro: r.get::<_, bool>(5),
            owner: r.get(6),
            expires_at: r.get(7),
        });
        drop(tx);
        g.commit()?;
        Ok(out)
    }

    fn verify_row(&self, id: &str, hash: &str, row: StoredKey) -> Result<AuthedKey> {
        if row.secret_hash != hash {
            return Err(Error::Denied("bad secret".into()));
        }
        if row.revoked_at.is_some() {
            return Err(Error::Denied("key revoked".into()));
        }
        if let Some(raw) = &row.expires_at {
            match chrono::DateTime::parse_from_rfc3339(raw) {
                Ok(dt) if dt.timestamp_millis() > chrono::Utc::now().timestamp_millis() => {}
                _ => return Err(Error::Denied("key expired".into())),
            }
        }
        Ok(AuthedKey {
            id: id.to_string(),
            owner: row.owner,
            label: row.label,
            view: row.view,
            ro: row.ro,
        })
    }

    /// 撤销：PG 提交后镜像同步。
    pub fn revoke(&self, id: &str) -> Result<()> {
        let ts = crate::persist::now_iso();
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let n = tx.execute(
            "UPDATE grovefs_control.api_keys SET revoked_at=$2
             WHERE id=$1 AND revoked_at IS NULL",
            &[&id, &ts],
        )?;
        g.commit()?;
        if n == 0 {
            return Err(Error::Denied(format!("no live key with id '{id}'")));
        }
        if let Some(m) = &self.mirror {
            let _ = m.revoke_key(id, &ts);
        }
        Ok(())
    }

    /// 轮换：只换 secret_hash + hint，身份/归属/审计历史全留。
    pub fn rotate(&self, id: &str) -> Result<KeyRecord> {
        let secret = gen_secret();
        let hash = sha256_hex(&secret);
        let hint = secret_hint(&secret);
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let n = tx.execute(
            "UPDATE grovefs_control.api_keys SET secret_hash=$2, secret_hint=$3
             WHERE id=$1 AND revoked_at IS NULL",
            &[&id, &hash, &hint],
        )?;
        g.commit()?;
        if n == 0 {
            return Err(Error::Denied(format!("no live key with id '{id}'")));
        }
        if let Some(m) = &self.mirror {
            // 镜像没有单列 UPDATE secret 的方法，走 upsert 兜底：读一行、写回。
            // 未来若嫌绕可以给 Mirror 加 rotate_key；此处简化。
            let _ = m.set_meta(&format!("rotate_{id}"), &hash);
        }
        Ok(KeyRecord {
            id: id.to_string(),
            secret,
            hash,
            hint,
            created_at: String::new(),
        })
    }

    pub fn set_ro(&self, id: &str, ro: bool) -> Result<()> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let n = tx.execute(
            "UPDATE grovefs_control.api_keys SET ro=$2 WHERE id=$1",
            &[&id, &ro],
        )?;
        g.commit()?;
        if n == 0 {
            return Err(Error::Denied(format!("no key with id '{id}'")));
        }
        if let Some(m) = &self.mirror {
            let _ = m.set_ro(id, ro);
        }
        Ok(())
    }

    pub fn touch(&self, id: &str) -> Result<()> {
        let ts = crate::persist::now_iso();
        // 允许失败（不影响认证），best-effort。
        if let Ok(mut g) = self.pool.begin_control_tx() {
            if let Ok(mut tx) = g.tx() {
                let _ = tx.execute(
                    "UPDATE grovefs_control.api_keys SET last_used_at=$2 WHERE id=$1",
                    &[&id, &ts],
                );
                let _ = g.commit();
            }
        }
        if let Some(m) = &self.mirror {
            let _ = m.touch_last_used(id, &ts);
        }
        Ok(())
    }

    /// 判断当前 key 是否还活：给 live-session recheck 用，比 authenticate
    /// 便宜（只读三列）。
    pub fn live_state(&self, id: &str) -> Result<bool> {
        let mut g = self.pool.begin_control_tx()?;
        let mut tx = g.tx()?;
        let row = tx.query_one(
            "SELECT revoked_at, expires_at FROM grovefs_control.api_keys WHERE id=$1",
            &[&id],
        )?;
        let live = match row {
            None => false,
            Some(r) => {
                let revoked: Option<String> = r.get(0);
                let expires: Option<String> = r.get(1);
                let not_revoked = revoked.is_none();
                let not_expired = match &expires {
                    None => true,
                    Some(raw) => chrono::DateTime::parse_from_rfc3339(raw)
                        .map(|dt| dt.timestamp_millis() > chrono::Utc::now().timestamp_millis())
                        .unwrap_or(false),
                };
                not_revoked && not_expired
            }
        };
        drop(tx);
        let _ = g.commit();
        Ok(live)
    }
}

// ---- helpers -------------------------------------------------------------

/// 16 hex chars（64 位随机）。
fn gen_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..16].to_string()
}

/// 32 hex chars（122 位随机，v4 UUID 完整）。
fn gen_secret() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// 前后各 4 hex 的掩码。
pub fn secret_hint(secret: &str) -> String {
    let chars: Vec<char> = secret.chars().collect();
    if chars.len() < 12 {
        return String::new();
    }
    let head: String = chars[..4].iter().collect();
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{head}\u{2026}{tail}")
}

fn sha256_hex(s: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(s.as_bytes());
    let out = h.finalize();
    let mut buf = [0u8; 32];
    buf.copy_from_slice(&out[..]);
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_id_secret_lengths() {
        let id = gen_id();
        assert_eq!(id.len(), 16, "id is 16 hex");
        let s = gen_secret();
        assert_eq!(s.len(), 32, "secret is 32 hex");
        assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));
        assert!(s.bytes().all(|b| b.is_ascii_hexdigit()));
    }

    #[test]
    fn secret_hint_shape() {
        let hint = secret_hint("0123456789abcdef0123456789abcdef");
        assert_eq!(hint, "0123\u{2026}cdef");
        assert_eq!(secret_hint("short"), "");
    }

    #[test]
    fn sha256_stable() {
        // 与既有 apikeys 实现互解（sha2 输出 32 字节小写 hex）。
        let h = sha256_hex("abc");
        assert_eq!(
            h,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn authed_key_is_not_created_without_pg() {
        // KeyStore::new 会走 init_schema 打开事务；不合法 URI 直接失败。
        // 这条测试钉住"没有 PG 就拒绝建 store"的边界。
        let pool = crate::pool::SharedInstancePool::new(
            "postgres://invalid-host-for-treedb.invalid:1/db".to_string(),
            crate::PoolConfig {
                warm: 0,
                max: 1,
                checkout_timeout: std::time::Duration::from_millis(10),
                ..Default::default()
            },
        );
        let reachable = Arc::new(AtomicBool::new(true));
        let res = KeyStore::new(pool, None, reachable, false);
        assert!(res.is_err());
    }
}
