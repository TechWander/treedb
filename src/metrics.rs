//! `--stats --format prometheus`：把 [`EngineStats`] 摊成 Prometheus 文本格式
//! （text exposition format 0.0.4）。
//!
//! 为什么不直接开一个 `/metrics` 端点：treedb 的接入面只有一条自定义二进制
//! 协议（§4），里面没有一个字节是 HTTP。抓取形状因此是**抓取方主动跑一次**
//! `treedb-server --stats --format prometheus`：输出可以原样喂给 node_exporter
//! 的 textfile collector，也可以被一个十行的 shell / sidecar 转成 `/metrics`。
//! 格式在这一层统一，引擎与事件循环都不用为重试、超时、鉴权之外的东西负责。
//!
//! 口径纪律：
//!
//! - **计数器一律 `_total`**，语义是「本进程启动至今」。进程一重启就归零 ——
//!   跨重启要连续的量只有 `treedb_seq` / `treedb_snapshot_seq` 这两个单调水位，
//!   它们才是可信的锚点。
//! - **字节数不换算**：`_bytes` 后面是基本单位，看板自己除 1024 才是 KB/MB。
//! - **副本专用的四个数在主上也输出**（恒 0）。面板必须用
//!   `treedb_replica == 1` 过滤，否则 `treedb_replica_lag 0` 会被读成「追平了」，
//!   而真相可能是「主根本不是副本」。这条与 `--stats` 文本档「只在副本上打印
//!   replica 行」是同一个判据，只是文本档能做条件打印、指标档不能（缺样本与
//!   值为 0 在 PromQL 里是两件事）。

use crate::engine::core::EngineStats;
use std::fmt::Write as _;

/// `--stats --format prometheus` 的正常输出。
///
/// 第一行是 `treedb_up 1`：抓取方拿到这一段就说明「连上了、握手过了、Stats
/// 拿到了」，不需要再拿退出码去区分。
pub fn prometheus(s: &EngineStats) -> String {
    let mut o = String::with_capacity(2048);
    head(&mut o, "treedb_up", "gauge", "1 if this scrape reached the server");
    o.push_str("treedb_up 1\n");
    gauge(&mut o, "treedb_views", "views (schemas) defined in the engine", s.views);
    gauge(&mut o, "treedb_nodes", "inodes alive in the current state", s.nodes);
    gauge(&mut o, "treedb_dentries", "directory entries alive in the current state", s.dentries);
    gauge(&mut o, "treedb_locks", "POSIX-style locks currently held", s.locks);
    gauge(&mut o, "treedb_refs", "content references (data_ref ledger) alive", s.refs);
    gauge(&mut o, "treedb_keys", "client access keys stored in the engine", s.keys);
    gauge(&mut o, "treedb_audit_rows", "rows in the audit table", s.audit);
    gauge(&mut o, "treedb_tables", "generic tables defined", s.tables);
    gauge(&mut o, "treedb_table_rows", "rows across all generic tables", s.rows);
    gauge(
        &mut o,
        "treedb_seq",
        "highest WAL seq handed out (the durable watermark)",
        s.seq,
    );
    gauge(
        &mut o,
        "treedb_snapshot_seq",
        "seq covered by the checkpoint snapshot (replay starts here)",
        s.snap_seq,
    );
    gauge(
        &mut o,
        "treedb_wal_logical_bytes",
        "bytes written to the WAL; the checkpoint threshold is measured on this",
        s.wal_bytes,
    );
    gauge(
        &mut o,
        "treedb_wal_disk_bytes",
        "WAL file size on disk, including preallocated holes",
        s.wal_disk_bytes,
    );
    gauge(
        &mut o,
        "treedb_snapshot_bytes",
        "checkpoint snapshot file size (0 before the first checkpoint)",
        s.snap_bytes,
    );
    counter(&mut o, "treedb_ops_total", "ops executed since process start", s.ops_total);
    counter(
        &mut o,
        "treedb_writes_total",
        "write ops committed since process start",
        s.writes_total,
    );
    counter(
        &mut o,
        "treedb_fsyncs_total",
        "fsync/fdatasync calls since process start; ops per fsync = writes_total / this",
        s.syncs_total,
    );
    // 微秒 → 秒：Prometheus 的时间约定是秒，`_total` 计数器又必须是基本单位。
    // 保留 6 位小数 ≈ 1 µs，正好是引擎自己记录的精度。
    counter_secs(
        &mut o,
        "treedb_fsync_seconds_total",
        "time spent inside fsync/fdatasync since process start",
        s.sync_us_total,
    );
    counter(
        &mut o,
        "treedb_checkpoints_total",
        "checkpoints written since process start",
        s.checkpoints,
    );
    gauge(
        &mut o,
        "treedb_protocol_version",
        "wire protocol version this server speaks (compare against the client's)",
        s.proto_version,
    );
    gauge(
        &mut o,
        "treedb_replica",
        "1 if this instance is a read-only replica, 0 if it is a primary",
        u8::from(s.replica),
    );
    gauge(
        &mut o,
        "treedb_replica_primary_seq",
        "durable seq last reported by the primary (replicas only)",
        s.replica_primary_seq,
    );
    gauge(
        &mut o,
        "treedb_replica_lag",
        "replica_primary_seq - local durable seq; meaningless unless treedb_replica == 1",
        s.replica_lag,
    );
    counter(
        &mut o,
        "treedb_replica_applied_total",
        "WAL records applied from the primary (replicas only)",
        s.replica_applied,
    );
    counter(
        &mut o,
        "treedb_replica_resyncs_total",
        "snapshot reinstalls after a gap (replicas only)",
        s.replica_resyncs,
    );
    counter(
        &mut o,
        "treedb_replica_errors_total",
        "failed fetch/apply rounds (replicas only)",
        s.replica_errors,
    );
    o
}

/// 抓取失败时的输出：**一条** `treedb_up 0`。
///
/// 失败时什么都不打，监控面板上看到的是 `no data`；打 0 才能让 `up` 型告警
/// （`treedb_up == 0`）在同一套规则里触发。退出码仍然是 1，供 shell 判断。
pub fn down() -> String {
    let mut o = String::new();
    head(&mut o, "treedb_up", "gauge", "1 if this scrape reached the server");
    o.push_str("treedb_up 0\n");
    o
}

fn head(o: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(o, "# HELP {name} {help}");
    let _ = writeln!(o, "# TYPE {name} {kind}");
}

fn gauge(o: &mut String, name: &str, help: &str, v: impl std::fmt::Display) {
    head(o, name, "gauge", help);
    let _ = writeln!(o, "{name} {v}");
}

fn counter(o: &mut String, name: &str, help: &str, v: u64) {
    head(o, name, "counter", help);
    let _ = writeln!(o, "{name} {v}");
}

/// 微秒累计 → 秒累计。整数微秒全在 `u64` 里，除以 1e6 之后仍然精确到 1 µs，
/// 所以这里不做「先转 f64 再除」之外的任何舍入补偿。
fn counter_secs(o: &mut String, name: &str, help: &str, us: u64) {
    head(o, name, "counter", help);
    let _ = writeln!(o, "{name} {:.6}", us as f64 / 1_000_000.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一份每个字段都不相同的样本：这样「值贴错字段」这种复制粘贴错误会被
    /// 断言抓出来，而全 0 的样本抓不出来。
    fn sample() -> EngineStats {
        EngineStats {
            views: 11,
            nodes: 22,
            dentries: 33,
            locks: 44,
            refs: 55,
            keys: 66,
            audit: 77,
            seq: 88,
            snap_seq: 99,
            wal_bytes: 1234,
            ops_total: 5678,
            writes_total: 91011,
            syncs_total: 12,
            sync_us_total: 34_500,
            checkpoints: 13,
            tables: 14,
            rows: 15,
            wal_disk_bytes: 16_777_216,
            snap_bytes: 4096,
            proto_version: 6,
            replica: true,
            replica_primary_seq: 101,
            replica_lag: 13,
            replica_applied: 102,
            replica_resyncs: 3,
            replica_errors: 4,
        }
    }

    /// 逐行拆成 (name, Option<value>, section) —— 校验只认文本，不认内部实现。
    fn scan(text: &str) -> Vec<(String, Option<String>, &'static str)> {
        let mut rows = Vec::new();
        for line in text.lines() {
            if line.is_empty() {
                continue;
            }
            if let Some(rest) = line.strip_prefix("# HELP ") {
                let (name, _help) = rest.split_once(' ').expect("HELP 必须带说明文本");
                rows.push((name.to_string(), None, "help"));
            } else if let Some(rest) = line.strip_prefix("# TYPE ") {
                let (name, kind) = rest.split_once(' ').expect("TYPE 必须带类型");
                assert!(
                    matches!(kind, "gauge" | "counter"),
                    "{name} 的类型 {kind:?} 不是本模块会产出的两种"
                );
                rows.push((name.to_string(), None, "type"));
            } else if line.starts_with('#') {
                panic!("不该出现其它注释行：{line}");
            } else {
                let (name, value) = line.split_once(' ').expect("样本行必须是 `<name> <value>`");
                assert!(
                    value.parse::<f64>().is_ok(),
                    "{name} 的值 {value:?} 不是数字"
                );
                rows.push((name.to_string(), Some(value.to_string()), "sample"));
            }
        }
        rows
    }

    /// 结构门：每个 HELP 立刻跟着同名 TYPE、同名样本；没有重复；名字都是
    /// `treedb_` 前缀。这三条一起挡住「漏写 HELP」「两个指标撞名」
    /// 「忘了换名字」这类手写格式最常见的错。
    #[test]
    fn exposition_has_one_help_type_and_sample_per_metric() {
        let text = prometheus(&sample());
        let rows = scan(&text);
        let mut seen: Vec<String> = Vec::new();
        let mut i = 0;
        while i < rows.len() {
            let (name, value, section) = &rows[i];
            assert_eq!(*section, "help", "{name}: 一段指标必须以 HELP 开头");
            assert!(name.starts_with("treedb_"), "{name}: 指标名必须有 treedb_ 前缀");
            assert_eq!(rows[i + 1].0, *name, "{name}: HELP 后面必须是同名 TYPE");
            assert_eq!(rows[i + 1].2, "type");
            assert_eq!(rows[i + 2].0, *name, "{name}: TYPE 后面必须是同名样本");
            assert!(value.is_none());
            assert!(rows[i + 2].1.is_some(), "{name}: TYPE 后面缺样本行");
            assert!(!seen.contains(name), "{name}: 同名指标出现了两次");
            seen.push(name.clone());
            i += 3;
        }
        assert!(!seen.is_empty());
        assert!(text.ends_with('\n'), "输出必须以换行结尾");
    }

    /// 值门：样本数与字段一一对应，且没贴错字段。逐条列出来而不是抽几个 ——
    /// 这份清单就是「字段 ↔ 指标」的对照表，改了任何一边都得在这里改。
    #[test]
    fn every_stat_lands_on_its_own_metric() {
        let text = prometheus(&sample());
        let get = |name: &str| -> String {
            let prefix = format!("{name} ");
            text.lines()
                .find_map(|l| l.strip_prefix(&prefix).map(|v| v.to_string()))
                .unwrap_or_else(|| panic!("缺样本 {name}"))
        };
        assert_eq!(get("treedb_up"), "1");
        assert_eq!(get("treedb_views"), "11");
        assert_eq!(get("treedb_nodes"), "22");
        assert_eq!(get("treedb_dentries"), "33");
        assert_eq!(get("treedb_locks"), "44");
        assert_eq!(get("treedb_refs"), "55");
        assert_eq!(get("treedb_keys"), "66");
        assert_eq!(get("treedb_audit_rows"), "77");
        assert_eq!(get("treedb_seq"), "88");
        assert_eq!(get("treedb_snapshot_seq"), "99");
        assert_eq!(get("treedb_wal_logical_bytes"), "1234");
        assert_eq!(get("treedb_ops_total"), "5678");
        assert_eq!(get("treedb_writes_total"), "91011");
        assert_eq!(get("treedb_fsyncs_total"), "12");
        // 34 500 µs = 0.0345 s，保留 6 位小数后不该出现浮点噪声。
        assert_eq!(get("treedb_fsync_seconds_total"), "0.034500");
        assert_eq!(get("treedb_checkpoints_total"), "13");
        assert_eq!(get("treedb_tables"), "14");
        assert_eq!(get("treedb_table_rows"), "15");
        assert_eq!(get("treedb_wal_disk_bytes"), "16777216");
        assert_eq!(get("treedb_snapshot_bytes"), "4096");
        assert_eq!(get("treedb_protocol_version"), "6");
        assert_eq!(get("treedb_replica"), "1");
        assert_eq!(get("treedb_replica_primary_seq"), "101");
        assert_eq!(get("treedb_replica_lag"), "13");
        assert_eq!(get("treedb_replica_applied_total"), "102");
        assert_eq!(get("treedb_replica_resyncs_total"), "3");
        assert_eq!(get("treedb_replica_errors_total"), "4");
        // 样本数 == 字段数 + treedb_up：加字段忘了加指标（或反之）会在这里断。
        assert_eq!(scan(&text).len(), (2 + 1) * 27, "指标条数变了却没改这条断言");
    }

    /// 计数器名必须以 `_total` 结尾、gauge 不能以它结尾 —— Prometheus 对
    /// counter 命名有硬约定，`increase()`/`rate()` 直接按后缀推导。
    #[test]
    fn counters_end_with_total_and_gauges_do_not() {
        let text = prometheus(&sample());
        let rows = scan(&text);
        let mut i = 0;
        while i < rows.len() {
            let name = &rows[i].0;
            let is_counter = text.lines().any(|l| l == format!("# TYPE {name} counter"));
            if is_counter {
                assert!(name.ends_with("_total"), "{name}: counter 必须以 _total 结尾");
            } else {
                assert!(!name.ends_with("_total"), "{name}: gauge 不该以 _total 结尾");
            }
            i += 3;
        }
    }

    /// 失败输出只有一条 `treedb_up 0`，且格式与正常输出同源。
    #[test]
    fn down_is_a_single_zero_sample() {
        let text = down();
        let rows = scan(&text);
        assert_eq!(rows.len(), 3, "{text}");
        assert_eq!(rows[2].0, "treedb_up");
        assert_eq!(rows[2].1.as_deref(), Some("0"));
    }
}
