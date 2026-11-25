#!/usr/bin/env python3
"""DESIGN §7.9 / §7.13 / §7.18 / §7.19 / §10 阶段 C 4 补段的表 vs 原始采集的逐格对账。

为什么单列一个检查器：这几组表的手抄数字都容错长得像真数字（3 和 4 拼成 34、
900 抄成 90）。判据只有一条：**每个数字都必须能从原始 TSV 重新算出来**。§7.13 的
final.tsv 还要由两份三路采集重算（`benches/three_way_table.py --check`）；§7.18 的
retained delta 由 rss_after − rss_before 派生，其余格子直接读 delete_storm.tsv 与
posix_delete_storm.tsv；§7.19 的表按 (auto_mb, views, conns) 三键索引
many_views_mix.tsv 与两份 mix_isolate_*.tsv，retained delta 同样派生；§7.9 的两张表按
**块时间戳**索引 sync_ratio.log —— 那份日志是追加式的，锚定块而不是取末块；
§10 阶段 C 4 末那两张（grovefs 各 ref 的现状、六条套件×两臂）索引
grovefs_pin_ab.log，套件格照 SUMMARY 的字段数写（两个计数器的套件不补零）。

用法：
  python3 benches/doc_table_check.py             # 对账（不一致 exit 1）
  python3 benches/doc_table_check.py --selftest  # 注入 23 处错，要求全部被拦下
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOC = ROOT + "/DESIGN.md"
TSV = ROOT + "/benches/results/final.tsv"
P2_TSV = ROOT + "/benches/results/delete_storm.tsv"
MV_TSV = ROOT + "/benches/results/many_views_mix.tsv"
COLS = ["op", "conns", "pipe", "t_ops", "t_p50", "t_p99", "pg_ops", "pg_p50",
        "pg_p99", "s_ops", "s_p50", "s_p99", "r_pg", "r_sqlite"]
P2_COLS = [
    "phase", "rows", "cycles", "ops", "time_ms", "records", "writes", "syncs",
    "checkpoints_start", "checkpoints_auto", "wal_start", "wal_peak", "wal_end",
    "wal_after_auto", "tail_records", "wal_before_crash", "wal_reopen",
    "snap_seq_start", "snap_seq_auto", "snap_bytes_auto", "rss_before_kb",
    "rss_after_kb", "rss_reopen_kb", "rss_peak_kb", "hwm_after_kb", "hwm_peak_kb",
    "rss_samples", "rows_reopen", "verify_checked", "errors",
]
P2_TABLES = [
    ("工作量与落盘", [
        "phase", "rows", "cycles", "ops", "time_ms", "records", "writes", "syncs",
        "checkpoints_start", "checkpoints_auto", "wal_start", "wal_peak", "wal_end",
        "wal_after_auto", "snap_seq_start", "snap_seq_auto", "snap_bytes_auto", "errors",
    ]),
    ("崩溃尾巴与终态", [
        "phase", "tail_records", "wal_before_crash", "wal_reopen", "rows_reopen",
        "verify_checked",
    ]),
    ("服务端 RSS", [
        "phase", "rss_before_kb", "rss_after_kb", "rss_reopen_kb", "retained_delta_kb",
        "rss_peak_kb", "hwm_after_kb", "hwm_peak_kb", "rss_samples",
    ]),
]
POSIX_COLS = [
    "auto_mb", "rows", "ops", "time_ms", "records", "writes", "syncs",
    "lat_p50_us", "lat_p90_us", "lat_p99_us", "lat_max_us", "lat_samples",
    "freed_refs", "verify_refs", "checkpoints_start", "checkpoints_auto",
    "wal_start", "wal_peak", "wal_end", "wal_after_auto", "snap_seq_start",
    "snap_seq_auto", "snap_bytes_auto", "snap_bytes_shutdown", "rss_before_kb",
    "rss_after_kb", "rss_reopen_kb", "rss_peak_kb", "hwm_after_kb", "hwm_peak_kb",
    "rss_samples", "worst_round_us", "rounds", "nodes_left", "dentries_left",
    "locks_left", "refs_left", "nodes_reopen", "dentries_reopen", "locks_reopen",
    "refs_reopen", "verify_checked_reopen", "errors",
]
POSIX_TABLES = [
    ("POSIX 固定工作量", [
        "auto_mb", "rows", "ops", "time_ms", "records", "writes", "syncs",
        "lat_p50_us", "lat_p90_us", "lat_p99_us", "lat_max_us", "lat_samples",
        "freed_refs", "verify_refs", "errors",
    ]),
    ("POSIX checkpoint 与账本", [
        "auto_mb", "checkpoints_start", "checkpoints_auto", "wal_start", "wal_peak",
        "wal_end", "wal_after_auto", "snap_seq_start", "snap_seq_auto",
        "snap_bytes_auto", "snap_bytes_shutdown", "nodes_left", "dentries_left",
        "locks_left", "refs_left", "errors",
    ]),
    ("POSIX 重开核对", [
        "auto_mb", "nodes_reopen", "dentries_reopen", "locks_reopen", "refs_reopen",
        "verify_checked_reopen", "errors",
    ]),
    ("POSIX RSS 与回合", [
        "auto_mb", "rss_before_kb", "rss_after_kb", "rss_reopen_kb", "retained_delta_kb",
        "rss_peak_kb", "hwm_after_kb", "hwm_peak_kb", "rss_samples",
        "worst_round_us", "rounds",
    ]),
]


# --- §7.19：万级混合视图并发曲线（benches/many_views_mix.sh）---
MV_COLS = [
    "views", "rows", "conns", "pipeline", "secs", "auto_mb", "seed_time_ms",
    "seed_records",
    "time_ms", "groups", "ops", "records", "writes", "syncs", "lookups",
    "readdirs", "creates", "unlinks", "lat_p50_us", "lat_p90_us", "lat_p99_us",
    "lat_max_us", "lat_samples", "views_touched", "gone_ok", "kept_ok",
    "checkpoints_start", "checkpoints_delta", "wal_start_bytes", "wal_end_bytes",
    "wal_peak_bytes", "snap_seq_start", "snap_seq_end", "rss_before_kb",
    "rss_after_kb", "rss_peak_kb", "hwm_after_kb", "rss_samples",
    "worst_round_us", "rounds", "nodes_left", "dentries_left", "locks_left",
    "refs_left", "ops_per_s", "errors",
]
MV_PROBE_TSVS = [
    ROOT + "/benches/results/mix_isolate_auto0.tsv",
    ROOT + "/benches/results/mix_isolate_auto1.tsv",
]
MV_PROBE_TABLES = [
    ("驻留隔离探针", [
        "auto_mb", "views", "conns", "groups", "time_ms", "ops_per_s",
        "checkpoints_delta", "wal_end_bytes", "wal_peak_bytes", "rss_before_kb",
        "rss_after_kb", "retained_delta_kb", "rss_peak_kb", "hwm_after_kb",
        "rss_samples", "errors",
    ]),
]
MV_TABLES = [
    ("混合负载工作量", [
        "views", "rows", "conns", "pipeline", "secs", "time_ms", "groups", "ops",
        "records", "writes", "syncs", "lookups", "readdirs", "creates",
        "unlinks", "ops_per_s", "errors",
    ]),
    ("延迟与覆盖", [
        "views", "conns", "groups", "lat_p50_us", "lat_p90_us", "lat_p99_us",
        "lat_max_us", "lat_samples", "views_touched", "gone_ok", "kept_ok",
        "errors",
    ]),
    ("种子、checkpoint 与账本", [
        "views", "conns", "auto_mb", "seed_time_ms", "seed_records",
        "checkpoints_start",
        "checkpoints_delta", "wal_start_bytes", "wal_end_bytes",
        "wal_peak_bytes", "snap_seq_start", "snap_seq_end", "nodes_left",
        "dentries_left", "locks_left", "refs_left", "errors",
    ]),
    ("RSS 与回合", [
        "views", "conns", "rss_before_kb", "rss_after_kb", "retained_delta_kb",
        "rss_peak_kb", "hwm_after_kb", "rss_samples", "worst_round_us", "rounds",
    ]),
]


# --- §7.9：一次 fsync 摊了几次写（benches/sync_ratio.sh）---
SR_LOG = ROOT + "/benches/results/sync_ratio.log"
# 日志是追加式的：跑一次多一块。文档的两张表各自锚定一块，键就是块头里的时间戳，
# 这样后来复跑追加的新块不会把旧表的对账带偏。
SR_BLOCKS = {"11-19": "2026-11-19T06:17:12", "11-25": "2026-11-25T13:05:31"}
SR_WAL2DISK = {
    "sr_group": "机械盘 XFS", "sr_always": "机械盘 XFS",
    "treedb-sr-group": "根盘 ext4", "treedb-sr-always": "根盘 ext4",
}
SR_HEAD_OLD = ["WAL 位置", "档位", "ops/s（p50 / p99 µs）", "writes", "syncs",
               "**写/fsync**"]
SR_HEAD_NEW = ["WAL 位置", "档位", "ops/s", "p50 µs", "p90 µs", "p99 µs", "max µs",
               "writes", "syncs", "写/fsync", "errors"]
SR_TABLES = [("11-19 采集", "11-19", SR_HEAD_OLD), ("11-25 复跑", "11-25", SR_HEAD_NEW)]
SR_COLMAP = {"ops/s": "ops_per_s", "p50 µs": "lat_p50_us", "p90 µs": "lat_p90_us",
             "p99 µs": "lat_p99_us", "max µs": "lat_max_us", "writes": "writes",
             "syncs": "syncs", "写/fsync": "per_sync", "**写/fsync**": "per_sync",
             "errors": "errors"}
# 采集行形如：group  wal=sr_group op=write … → 10501 ops/s over 3.06s | lat us: …
SR_BENCH = re.compile(
    r"^(group|always)\s+wal=(\S+)\s+op=write\b.*?\u2192\s*(\d[\d,]*) ops/s"
    r" over [\d.]+s\s*\|\s*lat us: p50=(\d+) p90=(\d+) p99=(\d+) max=(\d+)"
    r"\s*\|\s*errors=(\d+)")
# 计数器行形如：group  wal=sr_group   writes=32624 syncs=609 → 53.57 写/fsync
SR_COUNT = re.compile(
    r"^(group|always)\s+wal=(\S+)\s+writes=(\d+) syncs=(\d+) \u2192 (\S+) "
    r"\u5199/fsync")
SR_MERGED = re.compile(r"^([\d ]+)（([\d ]+) / ([\d ]+)）$")

# --- §10 阶段 C 4 末：grovefs 接入侧的 pin 跨代复核（benches/grovefs_pin_ab.sh）---
PIN_LOG = ROOT + "/benches/results/grovefs_pin_ab.log"
PIN_START = "4. **控制面搬家"
PIN_STOP = "## 11. 下一步"
PIN_REF_COLS = ["ref", "pin", "`control-migrate`", "`Op::Link`"]
PIN_SUITE_COLS = ["套件", "臂 A：pin v0.3.6", "臂 B：pin v0.8.0"]
PIN_SUITE_COLS_ORDER = ["test_treedb_control.sh", "test_treedb_matrix.sh",
                        "test_treedb.sh", "test_locks_treedb.sh",
                        "test_govcheck_treedb.sh", "test_govtime_treedb.sh"]
PIN_ARMS = ("A", "B")
# 臂头形如：# 臂 B：pin v0.8.0 / 引擎 /path/treedb-server（源码 613c1f9）@ 2026-…
PIN_ARM = re.compile(r"^#\s*臂 ([AB])：pin (\S+) / 引擎")
# ref 行形如：master  pin=v0.3.7  control_migrate=无  Op::Link=无
PIN_REF = re.compile(r"^(\S+)\s+pin=(\S+)\s+control_migrate=(\S+)\s+Op::Link=(.*)$")
# 套件行形如：==================== SUMMARY: pass=39 fail=0 skip=10 ==== [test_treedb_matrix.sh rc=0]
# 不锚行首、`fail=` 到括号之间也不按空白匹：摘要行两头都挂着 `====` 横幅
# （`====…==== SUMMARY: pass=… fail=… ====…====  [脚本名 rc=N]`），所以上下文只锚
# 行中的 `SUMMARY:`，计数与括号之间那串非数字交给 `\D*`。
PIN_SUM = re.compile(r"SUMMARY: pass=(\d+) fail=(\d+)(?: skip=(\d+))?\D*\[(\S+) rc=(\d+)\]$")
# 混代反证的点名行（§4.4 的非认证闸门）
PIN_GATE = "protocol version negotiation required"


def _cell(t):
    """文档格里的小装饰不算数据：反引号、全角括号、多余空白抹掉再比。"""
    return t.replace("`", "").replace("（", "(").replace("）", ")").strip()


def _si(t):
    return int(t.replace(" ", "").replace(",", ""))


def parse_sr_log(path):
    """-> {块标签: {(盘, 档): {字段: 值}}}；只收 SR_BLOCKS 锚定的块。"""
    out, tag, block = {}, None, {}
    for raw in open(path, encoding="utf-8"):
        line = raw.rstrip("\n")
        if line.startswith("=== sync_ratio @"):
            if tag is not None and block:
                out[tag] = block
            cells = line.split()
            # 块头带时区后缀（…T06:17:12+00:00），所以按前缀认，不整个查表。
            stamp = cells[3] if len(cells) > 3 else ""
            tag = next((t for t, ts in SR_BLOCKS.items() if stamp.startswith(ts)),
                       None)
            block = {}
            continue
        if tag is None:
            continue
        m = SR_BENCH.match(line) or SR_COUNT.match(line)
        if not m or m.group(2) not in SR_WAL2DISK:
            continue
        rec = block.setdefault((SR_WAL2DISK[m.group(2)], m.group(1)),
                               {"per_sync": None, "errors": None})
        if m.re is SR_BENCH:
            (rec["ops_per_s"], rec["lat_p50_us"], rec["lat_p90_us"],
             rec["lat_p99_us"], rec["lat_max_us"], rec["errors"]) = \
                [_si(g) for g in m.groups()[2:]]
        else:
            rec["writes"], rec["syncs"] = _si(m.group(3)), _si(m.group(4))
            rec["per_sync"] = float(m.group(5))
    if tag is not None:
        out[tag] = block
    for tag_ in SR_BLOCKS:
        if tag_ not in out:
            raise AssertionError("sync_ratio.log 里没有块 @%s（%s）"
                                 % (SR_BLOCKS[tag_], tag_))
    return out


def compare_sr_table(table, data, cols, label):
    bad = []
    if table[0] != cols:
        raise AssertionError("%s: 表头不符 %r" % (label, table[0]))
    if len(table) - 1 != len(data):
        raise AssertionError("%s: 行数 %d != %d" % (label, len(table) - 1, len(data)))
    for row in table[1:]:
        key = (row[0], row[1].strip("`"))
        if key not in data:
            bad.append("%s: 日志里没有格 %s" % (label, key))
            continue
        rec = data[key]
        for j, name in enumerate(cols):
            if j < 2:
                continue
            if name == "ops/s（p50 / p99 µs）":
                m = SR_MERGED.match(row[j])
                if not m:
                    bad.append("%s %s: 合并格解析不了 %r" % (label, key, row[j]))
                    continue
                for got, col in zip(m.groups(), ("ops/s", "p50 µs", "p99 µs")):
                    if _si(got) != rec[SR_COLMAP[col]]:
                        bad.append("%s %s %s: %s vs %s"
                                   % (label, key, col, got, rec[SR_COLMAP[col]]))
                continue
            want = rec.get(SR_COLMAP[name])
            if want is None:
                bad.append("%s %s: %s 没采到" % (label, key, name))
            elif abs(num(row[j]) - want) > (0.06 if name.endswith("写/fsync**")
                                           or name == "写/fsync" else 1e-9):
                bad.append("%s %s %s: %s vs %s" % (label, key, name, row[j], want))
    return bad


def sr_invariants(blocks):
    """§7.9 的两条主张：改前 always 每写一次 fsync、改后两个档同一提交点。"""
    bad = []
    for tag, data in blocks.items():
        for (disk, dur), r in sorted(data.items()):
            if not r["syncs"]:
                bad.append("%s %s/%s: syncs=0，比值无定义" % (tag, disk, dur))
                continue
            if abs(r["writes"] / r["syncs"] - r["per_sync"]) > 0.02:
                bad.append("%s %s/%s: 比值 %s != writes/syncs %.2f"
                           % (tag, disk, dur, r["per_sync"], r["writes"] / r["syncs"]))
            if r["errors"]:
                bad.append("%s %s/%s: errors=%s 非零" % (tag, disk, dur, r["errors"]))
        for disk in set(d for d, _ in data):
            g, a = data.get((disk, "group")), data.get((disk, "always"))
            if not g or not a:
                bad.append("%s %s: 两个档不齐" % (tag, disk))
                continue
            if tag == "11-19":       # 改前：always 必须逐写 fsync，且明显慢于 group
                if abs(a["per_sync"] - 1.0) > 0.01 or a["syncs"] != a["writes"]:
                    bad.append("11-19 %s/always: 不是 1.00 写/fsync（%s，%s/%s）"
                               % (disk, a["per_sync"], a["writes"], a["syncs"]))
                if a["ops_per_s"] >= g["ops_per_s"]:
                    bad.append("11-19 %s: 改前 always 不该快过 group" % disk)
            else:                    # 改后：两档同一提交点，合批比同量级
                if a["per_sync"] < 10 or a["syncs"] >= a["writes"]:
                    bad.append("11-25 %s/always: 没合批（%s 写/fsync，%s/%s）"
                               % (disk, a["per_sync"], a["writes"], a["syncs"]))
                if not 0.5 <= a["ops_per_s"] / g["ops_per_s"] <= 1.5:
                    bad.append("11-25 %s: always/group 吞吐 %.2f× 不该差出 50 %%"
                               % (disk, a["ops_per_s"] / g["ops_per_s"]))
    for disk in set(d for d, _ in blocks["11-25"]):
        old = blocks["11-19"][(disk, "always")]["ops_per_s"]
        new = blocks["11-25"][(disk, "always")]["ops_per_s"]
        if new < 3 * old:
            bad.append("%s/always: 复跑只有改前的 %.2f×，§7.9「已修」不成立"
                       % (disk, new / old))
    return bad


def parse_pin_log(path):
    """grovefs_pin_ab.log → (refs, suites, gate)。

    refs[ref] = [ref, pin, control_migrate, Op::Link]；
    suites[臂][套件] = "pass / fail[/ skip]"——照 SUMMARY 印出来的字段数，不补零：
    只带两个计数器的套件不应被伪造成「skip=0」。
    """
    refs, suites, gate = {}, {a: {} for a in PIN_ARMS}, False
    arm = None
    for raw in open(path):
        line = raw.strip()
        # 第 4 块是「应当红」的混代反证，两行 SUMMARY 不属于任何一臂；不关掉臂号的话，
        # 它会把臂 A 刚收好的六条读数覆盖成 33/81 与 0/1，两臂对称这条就假红。
        if line.startswith("----- 4)"):
            arm = None
            continue
        m = PIN_ARM.match(line)
        if m:
            arm = m.group(1)
            continue
        m = PIN_REF.match(line)
        if m:
            refs[m.group(1)] = list(m.groups())
            continue
        m = PIN_SUM.search(line)
        if m and arm:
            g = m.groups()
            suites[arm][g[3]] = " / ".join(x for x in g[:3] if x)
        if PIN_GATE in line:
            gate = True
    return refs, suites, gate


def parse_indented_tables(path, start, stop):
    """取一段里的 Markdown 表，容忍列表缩进（§10 的表挂在编号项里）。

    `parse_section_tables` 只认顶格的表，那条锚点是标题；这里的锚点是一句列表项
    开头，而表在三项缩进里，所以单独写一份只差在 strip()。
    """
    tables, current, inside = [], None, False
    for raw in open(path):
        line = raw.rstrip("\n")
        if not inside and line.startswith(start):
            inside = True
        if inside and line.startswith(stop):
            break
        if not inside:
            continue
        s = line.strip()
        if s.startswith("|"):
            cells = [c.strip() for c in s.strip("|").split("|")]
            if set("".join(cells)) <= set("-: "):
                continue
            current = [cells] if current is None else current + [cells]
        elif current is not None:
            tables.append(current)
            current = None
    if current is not None:
        tables.append(current)
    return tables


def compare_pin_ref_table(table, refs, cols, label):
    bad = []
    if table[0] != cols:
        bad.append("%s 表表头不符：%s" % (label, table[0]))
    seen = set()
    for row in table[1:]:
        ref = _cell(row[0])
        if ref not in refs:
            bad.append("%s 表里的 %s 在日志里找不到" % (label, ref))
            continue
        seen.add(ref)
        for i, (w, g) in enumerate(zip(refs[ref][1:], [_cell(c) for c in row[1:]])):
            if w != g:
                bad.append("%s 表 %s 第 %d 列：文档 %r ≠ 日志 %r"
                           % (label, ref, i + 2, g, w))
    for ref in refs:
        if ref not in seen:
            bad.append("%s 表缺日志里的 ref %s（日志 %d 行、表里 %d 行）"
                       % (label, ref, len(refs), len(seen)))
    return bad


def compare_pin_suite_table(table, suites, cols, label):
    bad = []
    if table[0] != cols:
        bad.append("%s 表表头不符：%s" % (label, table[0]))
    seen = set()
    for row in table[1:]:
        suite = _cell(row[0])
        seen.add(suite)
        for i, arm in enumerate(PIN_ARMS):
            want = suites[arm].get(suite)
            if want is None:
                bad.append("%s 表：日志臂 %s 里没有 %s 的 SUMMARY 行"
                           % (label, arm, suite))
                continue
            got = _cell(row[1 + i]) if 1 + i < len(row) else ""
            if got != want:
                bad.append("%s 表：臂 %s 的 %s 文档 %r ≠ 日志 %r"
                           % (label, arm, suite, got, want))
    for arm in PIN_ARMS:
        for suite in suites[arm]:
            if suite not in seen:
                bad.append("%s 表：日志臂 %s 的 %s 没进表" % (label, arm, suite))
    return bad


def pin_invariants(suites, gate):
    bad = []
    for suite in PIN_SUITE_COLS_ORDER:
        a, b = suites["A"].get(suite), suites["B"].get(suite)
        if a is None or b is None:
            bad.append("§10 补段：臂上缺 %s 的读数（A=%r B=%r）" % (suite, a, b))
            continue
        if a != b:
            bad.append("§10 补段：%s 两臂三元组不等（A=%s B=%s）—— "
                       "「只改 tag 一行、零回归」这句话就不成立了" % (suite, a, b))
        if a.split(" / ")[1] != "0":
            bad.append("§10 补段：%s 有 fail（%s）" % (suite, a))
    if not gate:
        bad.append("§10 补段：日志里没有混代被闸门拒的点名行（%r）"
                   "，文档里「混代是硬失败」那句失去证据" % PIN_GATE)
    return bad



def num(s):

    return float(s.replace("**", "").replace("×", "").replace(" ", "").replace(",", ""))


def parse_doc(path):
    rows, inside = [], False
    for line in open(path):
        if line.startswith("### 7.13"):
            inside = True
            continue
        if inside and line.startswith("**读得出来的五条**"):
            break
        if inside and line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if set("".join(cells)) <= set("-: "):
                continue  # 分隔行
            rows.append(cells)
    assert rows and rows[0][0] == "op", "§7.13 的表头没找到"
    return rows[1:]


def parse_tsv(path):
    return [l.rstrip("\n").split("\t") for l in open(path)][1:]


HEADING = re.compile(r"^#{2,4} ")


def parse_section_tables(path, heading, stop):
    """把一个章节里的 Markdown 表格切成 [表头, 数据行...] 列表。

    收口条件是「遇到下一个标题」而不仅是那个显式 stop：往章节之间插一节，不必回头
    改上一节的 stop，也不会把下一节的表误算进上一节。
    """
    tables, current, inside = [], None, False
    for raw in open(path):
        line = raw.rstrip("\n")
        if line.startswith(heading):
            inside = True
            continue
        if inside and (line.startswith(stop)
                       or (HEADING.match(line) and not line.startswith(heading))):
            break
        if not inside:
            continue
        if line.startswith("|"):
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if set("".join(cells)) <= set("-: "):
                continue
            if current is None:
                current = [cells]
            else:
                current.append(cells)
        elif current is not None:
            tables.append(current)
            current = None
    if current is not None:
        tables.append(current)
    return tables


def parse_p2_tsv(path):
    rows = [l.rstrip("\n").split("\t") for l in open(path) if l.strip()]
    assert rows and rows[0] == P2_COLS, "delete_storm.tsv 表头不符"
    out = {}
    for cells in rows[1:]:
        assert len(cells) == len(P2_COLS), "delete_storm.tsv 列数不符"
        rec = dict(zip(P2_COLS, cells))
        for name in P2_COLS[1:]:
            rec[name] = int(rec[name])
            assert rec[name] >= 0, "%s 出现负数" % name
        assert rec["phase"] not in out, "phase 重复：%s" % rec["phase"]
        out[rec["phase"]] = rec
    assert list(out) == ["rowdelete", "churn"], "delete_storm.tsv 必须恰有 rowdelete/churn 两行"
    return out


def parse_posix_tsv(path):
    rows = [l.rstrip("\n").split("\t") for l in open(path) if l.strip()]
    assert rows and rows[0] == POSIX_COLS, "posix_delete_storm.tsv 表头不符"
    out = {}
    for cells in rows[1:]:
        assert len(cells) == len(POSIX_COLS), "posix_delete_storm.tsv 列数不符"
        rec = dict(zip(POSIX_COLS, cells))
        for name in POSIX_COLS:
            rec[name] = int(rec[name])
            assert rec[name] >= 0, "%s 出现负数" % name
        key = str(rec["auto_mb"])
        assert key not in out, "auto_mb 重复：%s" % key
        out[key] = rec
    assert list(out) == ["0", "1"], "posix_delete_storm.tsv 必须恰有 auto_mb=0/1 两行"
    return out


def compare_p2_table(table, data, cols, label):
    if not table or table[0] != cols:
        raise AssertionError("§7.18 %s 表头不符" % label)
    doc = table[1:]
    if len(doc) != len(data):
        raise AssertionError("§7.18 %s 行数 %d != %d" % (label, len(doc), len(data)))
    bad = []
    for i, row in enumerate(doc):
        if len(row) != len(cols):
            raise AssertionError("§7.18 %s 第 %d 行列数不符" % (label, i + 1))
        phase = row[0]
        if phase not in data:
            raise AssertionError("§7.18 %s 未知 phase：%s" % (label, phase))
        for j, name in enumerate(cols[1:], 1):
            rec = data[phase]
            want = (rec["rss_after_kb"] - rec["rss_before_kb"]
                    if name == "retained_delta_kb" else rec[name])
            try:
                got = int(num(row[j]))
            except ValueError:
                got = None
            if got != want:
                bad.append("%s 行%d %s: %s vs %d" % (label, i + 1, name, row[j], want))
    return bad


def compare_posix_table(table, data, cols, label):
    if not table or table[0] != cols:
        raise AssertionError("§7.18 %s 表头不符" % label)
    doc = table[1:]
    if len(doc) != len(data):
        raise AssertionError("§7.18 %s 行数 %d != %d" % (label, len(doc), len(data)))
    bad = []
    for i, row in enumerate(doc):
        if len(row) != len(cols):
            raise AssertionError("§7.18 %s 第 %d 行列数不符" % (label, i + 1))
        auto = row[0]
        if auto not in data:
            raise AssertionError("§7.18 %s 未知 auto_mb：%s" % (label, auto))
        for j, name in enumerate(cols[1:], 1):
            rec = data[auto]
            want = (rec["rss_after_kb"] - rec["rss_before_kb"]
                    if name == "retained_delta_kb" else rec[name])
            try:
                got = int(num(row[j]))
            except ValueError:
                got = None
            if got != want:
                bad.append("%s 行%d %s: %s vs %d" % (label, i + 1, name, row[j], want))
    return bad


def p2_invariants(data):
    bad = []
    for phase, r in data.items():
        want_ops = r["rows"] if phase == "rowdelete" else 2 * r["rows"] * r["cycles"]
        checks = [
            ("ops 固定工作量", r["ops"] == want_ops),
            ("records/writes 等于 ops", r["records"] == want_ops and r["writes"] == want_ops),
            ("自动 checkpoint 前进", r["checkpoints_auto"] > r["checkpoints_start"]),
            ("快照水位前进", r["snap_seq_auto"] > r["snap_seq_start"]),
            ("快照非空", r["snap_bytes_auto"] > 0),
            ("WAL 从峰值回落", r["wal_peak"] > r["wal_after_auto"]),
            ("两条尾巴", r["tail_records"] == 2),
            ("崩溃前后 WAL 一致", r["wal_before_crash"] == r["wal_reopen"]),
            ("RSS 持续采样", r["rss_samples"] > 1 and r["rss_peak_kb"] > 0),
            ("HWM 单调", r["hwm_peak_kb"] >= r["hwm_after_kb"] >= r["rss_before_kb"]),
            ("零错误", r["errors"] == 0),
        ]
        want_rows = 0 if phase == "rowdelete" else r["rows"]
        checks.append(("重启终态行数", r["rows_reopen"] == want_rows))
        checks.append(("重启内容核对", r["verify_checked"] == (0 if phase == "rowdelete" else r["rows"])))
        bad.extend("%s: %s" % (phase, name) for name, ok in checks if not ok)
    return bad


def posix_invariants(data):
    bad = []
    for auto, r in data.items():
        n = r["rows"]
        checks = [
            ("固定 2N op", n > 0 and r["ops"] == 2 * n),
            ("固定 WAL 记录/写次数", r["records"] == 6 * n and r["writes"] == 2 * n),
            ("NodeRef 非空/ref 归零", r["freed_refs"] == n and r["verify_refs"] == n),
            ("延迟样本数", r["lat_samples"] == r["ops"]),
            ("延迟分位有序", 0 < r["lat_p50_us"] <= r["lat_p90_us"] <= r["lat_p99_us"] <= r["lat_max_us"]),
            ("删除起点 WAL 为零", r["wal_start"] == 0),
            ("WAL 峰值有效", r["wal_peak"] > 0 and r["wal_peak"] >= r["wal_end"]),
            ("删除后账本基线", (r["nodes_left"], r["dentries_left"], r["locks_left"], r["refs_left"]) == (1, 0, 0, 0)),
            ("重开账本基线", (r["nodes_reopen"], r["dentries_reopen"], r["locks_reopen"], r["refs_reopen"]) == (1, 0, 0, 0)),
            ("重开逐 ref 核对", r["verify_checked_reopen"] == n),
            ("停服快照非空", r["snap_bytes_shutdown"] > 0),
            ("RSS 持续采样", r["rss_samples"] > 1 and r["rss_peak_kb"] > 0),
            ("HWM 单调", r["hwm_peak_kb"] >= r["hwm_after_kb"] >= r["rss_before_kb"]),
            ("服务端回合报告", r["rounds"] > 0 and r["worst_round_us"] > 0),
            ("零错误", r["errors"] == 0),
        ]
        if auto == "1":
            checks.extend([
                ("自动 checkpoint 前进", r["checkpoints_auto"] > r["checkpoints_start"]),
                ("自动快照水位前进", r["snap_seq_auto"] > r["snap_seq_start"]),
                ("自动快照非空", r["snap_bytes_auto"] > 0),
                ("自动 checkpoint 后 WAL 回落", r["wal_after_auto"] < r["wal_peak"]),
            ])
        else:
            checks.extend([
                ("关闭档不新增 checkpoint", r["checkpoints_auto"] == r["checkpoints_start"]),
                ("关闭档不推进快照水位", r["snap_seq_auto"] == r["snap_seq_start"]),
                ("关闭档不改写 WAL", r["wal_after_auto"] == r["wal_end"]),
            ])
        bad.extend("auto=%s: %s" % (auto, name) for name, ok in checks if not ok)
    return bad


def parse_mv_tsv(path):
    rows = [l.rstrip("\n").split("\t") for l in open(path) if l.strip()]
    assert rows and rows[0] == MV_COLS, "many_views_mix.tsv 表头不符"
    out = {}
    for cells in rows[1:]:
        assert len(cells) == len(MV_COLS), "many_views_mix.tsv 列数不符"
        rec = dict(zip(MV_COLS, cells))
        for name in MV_COLS:
            rec[name] = int(rec[name])
            assert rec[name] >= 0, "%s 出现负数" % name
        key = (rec["auto_mb"], rec["views"], rec["conns"])
        assert key not in out, "(auto_mb, views, conns) 重复：%s" % (key,)
        out[key] = rec
    assert out, "many_views_mix.tsv 没有数据行"
    return out


def compare_mv_table(table, data, cols, label, auto_mb=1):
    """逐格对账，格子由 (views, conns) 定位而不是由行序定位。

    行序定位会让「把 8 连接那行抄到 64 连接下面」这种错蒙混过关；双键定位还会
    顺手揭出漏一格与抄两遍。
    """
    if not table or table[0] != cols:
        raise AssertionError("§7.19 %s 表头不符" % label)
    doc = table[1:]
    if len(doc) != len(data):
        raise AssertionError("§7.19 %s 行数 %d != %d" % (label, len(doc), len(data)))
    bad, seen = [], set()
    for i, row in enumerate(doc):
        if len(row) != len(cols):
            raise AssertionError("§7.19 %s 第 %d 行列数不符" % (label, i + 1))
        cell = dict(zip(cols, row))
        try:
            auto = int(num(cell["auto_mb"])) if "auto_mb" in cols else auto_mb
            key = (auto, int(num(cell["views"])), int(num(cell["conns"])))
        except ValueError:
            raise AssertionError("§7.19 %s 第 %d 行的 views/conns 不是数字" % (label, i + 1))
        if key not in data:
            raise AssertionError("§7.19 %s 未知格子：%s" % (label, key))
        if key in seen:
            raise AssertionError("§7.19 %s 格子重复：%s" % (label, key))
        seen.add(key)
        rec = data[key]
        for j, name in enumerate(cols):
            if name in ("views", "conns", "auto_mb"):
                continue
            if name == "retained_delta_kb":
                want = rec["rss_after_kb"] - rec["rss_before_kb"]
            else:
                want = rec[name]
            try:
                got = int(num(row[j]))
            except ValueError:
                got = None
            if got != want:
                bad.append("%s 行%d %s: %s vs %d" % (label, i + 1, name, row[j], want))
    return bad


def mv_invariants(data):
    bad = []
    for key, r in sorted(data.items()):
        auto, views, _conns = key
        rows, g = r["rows"], r["groups"]
        checks = [
            ("种子记录数 2N+3NW", r["seed_records"] == 2 * views + 3 * views * rows),
            ("pipeline 是 5 的倍数", r["pipeline"] > 0 and r["pipeline"] % 5 == 0),
            ("组数为正", g > 0),
            ("固定五个 op 一组", r["ops"] == 5 * g),
            ("记录数 5G", r["records"] == 5 * g),
            ("写次数 2G", r["writes"] == 2 * g),
            ("op 配比 2:1:1:1", r["lookups"] == 2 * g and r["readdirs"] == g
             and r["creates"] == g and r["unlinks"] == g),
            ("延迟样本等于 op", r["lat_samples"] == r["ops"]),
            ("延迟分位有序", 0 < r["lat_p50_us"] <= r["lat_p90_us"]
             <= r["lat_p99_us"] <= r["lat_max_us"]),
            ("视图覆盖数等于 min(N,G)", r["views_touched"] == min(views, g)),
            ("名字级核对跑满头尾", r["gone_ok"] == min(g, 400)
             and r["kept_ok"] == min(g, 400)),
            ("净零账本", r["nodes_left"] == views * (rows + 1)
             and r["dentries_left"] == views * rows),
            ("锁与引用归零", r["locks_left"] == 0 and r["refs_left"] == 0),
            ("窗口不短于设定秒数", r["time_ms"] >= r["secs"] * 1000),
            ("吞吐由 ops/time 重算", r["ops_per_s"] == r["ops"] * 1000 // max(r["time_ms"], 1)),
            ("确有合并 fsync", r["syncs"] > 0),
            ("WAL 峰值有效", r["wal_peak_bytes"] > 0 and r["wal_peak_bytes"] >= r["wal_end_bytes"]),
            ("checkpoint 差值非负", r["checkpoints_delta"] >= 0),
            ("自动档按阈值落快照", r["checkpoints_delta"] > 0 if auto else r["checkpoints_delta"] == 0),
            ("落过快照就推水位", r["checkpoints_delta"] == 0
             or r["snap_seq_end"] > r["snap_seq_start"]),
            ("RSS 持续采样", r["rss_samples"] > 1 and r["rss_peak_kb"] > 0),
            ("HWM 不低于采样峰值", r["hwm_after_kb"] >= max(r["rss_peak_kb"], r["rss_before_kb"])),
            ("服务端回合报告", r["rounds"] > 0 and r["worst_round_us"] > 0),
            ("零错误", r["errors"] == 0),
        ]
        for name, ok in checks:
            if not ok:
                bad.append("auto=%d views=%d conns=%d: %s" % (key + (name,)))
    return bad


def compare(doc, tsv):
    """-> 不一致清单（空 = 全对）。doc 行数/列数不对直接抛 AssertionError。"""
    if len(doc) != len(tsv):
        raise AssertionError("行数不符 doc=%d tsv=%d" % (len(doc), len(tsv)))
    bad = []
    for i, (d, t) in enumerate(zip(doc, tsv)):
        if len(d) != len(COLS):
            raise AssertionError("第 %d 行列数 %d != %d" % (i + 1, len(d), len(COLS)))
        for j, name in enumerate(COLS):
            if name == "op":
                if d[j] != t[j]:
                    bad.append("行%d op: %s vs %s" % (i + 1, d[j], t[j]))
            elif name in ("conns", "pipe"):
                if int(d[j]) != int(t[j]):
                    bad.append("行%d %s: %s vs %s" % (i + 1, name, d[j], t[j]))
            elif name.startswith("r_"):
                want = float(t[12] if name == "r_pg" else t[13])
                if abs(num(d[j]) - want) > 0.06:  # 文档保留 1 位小数
                    bad.append("行%d %s: %.2f vs %.2f" % (i + 1, name, num(d[j]), want))
            elif abs(num(d[j]) - float(t[j])) > 1e-9:
                bad.append("行%d %s: %s vs %s" % (i + 1, name, d[j], t[j]))
    return bad


if __name__ == "__main__":
    doc, tsv = parse_doc(DOC), parse_tsv(TSV)
    bad = compare(doc, tsv)
    if bad:
        print("\n".join(bad))
        sys.exit("MISMATCH: %d 格" % len(bad))
    print("OK: 24 格 × %d 列与 final.tsv 逐格一致" % len(COLS))

    p2_data = parse_p2_tsv(P2_TSV)
    posix_data = parse_posix_tsv(ROOT + "/benches/results/posix_delete_storm.tsv")
    p2_tables = parse_section_tables(DOC, "### 7.18", "## 8.")
    expected_tables = len(P2_TABLES) + len(POSIX_TABLES)
    if len(p2_tables) != expected_tables:
        sys.exit("§7.18 找到 %d 张表，应为 %d 张" % (len(p2_tables), expected_tables))
    p2_doc, posix_doc = [], []
    for label, cols in P2_TABLES:
        matches = [t for t in p2_tables if t and t[0] == cols]
        if len(matches) != 1:
            sys.exit("§7.18 %s 表找到 %d 张，应为 1 张" % (label, len(matches)))
        p2_doc.append(matches[0])
        bad.extend(compare_p2_table(matches[0], p2_data, cols, label))
    for label, cols in POSIX_TABLES:
        matches = [t for t in p2_tables if t and t[0] == cols]
        if len(matches) != 1:
            sys.exit("§7.18 %s 表找到 %d 张，应为 1 张" % (label, len(matches)))
        posix_doc.append(matches[0])
        bad.extend(compare_posix_table(matches[0], posix_data, cols, label))
    bad.extend(p2_invariants(p2_data))
    bad.extend(posix_invariants(posix_data))
    if bad:
        print("\n".join(bad))
        sys.exit("MISMATCH: §7.18 %d 项" % len(bad))
    print("OK: §7.18 三张通用表 + 四张 POSIX 表与两份 TSV 逐格一致，全部不变量通过")

    mv_data = parse_mv_tsv(MV_TSV)
    mv_tables = parse_section_tables(DOC, "### 7.19 ", "### 7.19.1")
    if len(mv_tables) != len(MV_TABLES):
        sys.exit("§7.19 找到 %d 张表，应为 %d 张" % (len(mv_tables), len(MV_TABLES)))
    mv_doc = []
    for label, cols in MV_TABLES:
        matches = [t for t in mv_tables if t and t[0] == cols]
        if len(matches) != 1:
            sys.exit("§7.19 %s 表找到 %d 张，应为 1 张" % (label, len(matches)))
        mv_doc.append(matches[0])
        bad.extend(compare_mv_table(matches[0], mv_data, cols, label))
    bad.extend(mv_invariants(mv_data))
    if bad:
        print("\n".join(bad))
        sys.exit("MISMATCH: §7.19 %d 项" % len(bad))
    print("OK: §7.19 四张矩阵表 × %d 格与 many_views_mix.tsv 逐格一致，全部不变量通过"
          % len(mv_data))

    probe_data = {}
    for path in MV_PROBE_TSVS:
        for key, rec in parse_mv_tsv(path).items():
            assert key not in probe_data, "探针两份 TSV 出现同一格：%s" % (key,)
            probe_data[key] = rec
    probe_tables = parse_section_tables(DOC, "#### 7.19.1", "## 8.")
    if len(probe_tables) != len(MV_PROBE_TABLES):
        sys.exit("§7.19.1 找到 %d 张表，应为 %d 张"
                 % (len(probe_tables), len(MV_PROBE_TABLES)))
    mv_probe_doc = []
    for label, cols in MV_PROBE_TABLES:
        matches = [t for t in probe_tables if t and t[0] == cols]
        if len(matches) != 1:
            sys.exit("§7.19.1 %s 表找到 %d 张，应为 1 张" % (label, len(matches)))
        mv_probe_doc.append(matches[0])
        bad.extend(compare_mv_table(matches[0], probe_data, cols, label))
    bad.extend(mv_invariants(probe_data))
    if bad:
        print("\n".join(bad))
        sys.exit("MISMATCH: §7.19.1 %d 项" % len(bad))
    print("OK: §7.19.1 探针 %d 格与两份 mix_isolate_*.tsv 逐格一致" % len(probe_data))

    sr = parse_sr_log(SR_LOG)
    sr_old_doc, sr_new_doc = [], []
    sr_tables = parse_section_tables(DOC, "### 7.9 ", "### 7.10")
    for label, tag, cols in SR_TABLES:
        matches = [t for t in sr_tables if t and t[0] == cols]
        if len(matches) != 1:
            sys.exit("§7.9 %s 表找到 %d 张，应为 1 张（非缩进表）"
                     % (label, len(matches)))
        bad.extend(compare_sr_table(matches[0], sr[tag], cols, label))
        (sr_old_doc if tag == "11-19" else sr_new_doc).append(matches[0])
    bad.extend(sr_invariants(sr))
    if bad:
        print("\n".join(bad))
        sys.exit("MISMATCH: §7.9 %d 项" % len(bad))
    print("OK: §7.9 两张表 × %d 格与 sync_ratio.log 两个锚定块逐格一致，全部不变量通过"
          % sum(len(v) for v in sr.values()))

    pin_refs, pin_suites, pin_gate = parse_pin_log(PIN_LOG)
    pin_tables = parse_indented_tables(DOC, PIN_START, PIN_STOP)
    m = [t for t in pin_tables if t and t[0] == PIN_REF_COLS]
    if len(m) != 1:
        sys.exit("§10 补段的 ref 表找到 %d 张，应为 1 张" % len(m))
    pin_ref_doc = m[0]
    bad.extend(compare_pin_ref_table(pin_ref_doc, pin_refs, PIN_REF_COLS, "ref"))
    m = [t for t in pin_tables if t and t[0] == PIN_SUITE_COLS]
    if len(m) != 1:
        sys.exit("§10 补段的套件表找到 %d 张，应为 1 张" % len(m))
    pin_suite_doc = m[0]
    bad.extend(compare_pin_suite_table(pin_suite_doc, pin_suites, PIN_SUITE_COLS, "套件"))
    bad.extend(pin_invariants(pin_suites, pin_gate))
    if bad:
        print("\n".join(bad))
        sys.exit("MISMATCH: §10 补段 %d 项" % len(bad))
    print("OK: §10 阶段 C 4 补段 %d+%d 格与 grovefs_pin_ab.log 逐格一致，全部不变量通过"
          % (len(pin_ref_doc) - 1, len(pin_suite_doc) - 1))

    if "--selftest" in sys.argv:

        # §7.13 三处注入：改一个 ops、把 p50/p99 拼成一列、删一行。
        m1 = [r[:] for r in doc]
        m1[0][3] = str(num(m1[0][3]) + 1)
        m2 = [r[:] for r in doc]
        m2[0] = m2[0][:10] + [m2[0][10] + m2[0][11]] + m2[0][12:]
        m3 = doc[:-1]
        for name, mutated in (("改一个 ops", m1), ("拼列", m2), ("缺一行", m3)):
            try:
                hit = compare(mutated, tsv)
            except AssertionError as e:
                hit = [str(e)]
            if not hit:
                sys.exit("selftest FAILED: 「%s」没被拦下" % name)
            print("selftest OK: 「%s」被拦下（%s）" % (name, hit[0]))

        # §7.18 三处注入：改工作量、改派生 RSS delta、删一行。
        q1 = [r[:] for r in p2_doc[0]]
        q1[1][3] = str(int(num(q1[1][3])) + 1)
        q2 = [r[:] for r in p2_doc[2]]
        q2[1][4] = str(int(num(q2[1][4])) + 1)
        q3 = p2_doc[0][:-1]
        for name, mutated, idx in (("P2 改 ops", q1, 0), ("P2 改 RSS delta", q2, 2),
                                   ("P2 缺一行", q3, 0)):
            label, cols = P2_TABLES[idx]
            try:
                hit = compare_p2_table(mutated, p2_data, cols, label)
            except AssertionError as e:
                hit = [str(e)]
            if not hit:
                sys.exit("selftest FAILED: 「%s」没被拦下" % name)
            print("selftest OK: 「%s」被拦下（%s）" % (name, hit[0]))
        # POSIX 四处注入：改 ops、破坏自动档、改账本终态、删一行。
        q4 = [r[:] for r in posix_doc[0]]
        q4[1][2] = str(int(num(q4[1][2])) + 1)
        bad_data = {k: dict(v) for k, v in posix_data.items()}
        bad_data["1"]["checkpoints_auto"] = bad_data["1"]["checkpoints_start"]
        bad_ledger = {k: dict(v) for k, v in posix_data.items()}
        bad_ledger["0"]["locks_left"] = 1
        q7 = posix_doc[0][:-1]
        for name, hit in (
            ("POSIX 改 ops", compare_posix_table(q4, posix_data, POSIX_TABLES[0][1], POSIX_TABLES[0][0])),
            ("POSIX 自动档不前进", posix_invariants(bad_data)),
            ("POSIX 账本未收敛", posix_invariants(bad_ledger)),
        ):
            if not hit:
                sys.exit("selftest FAILED: 「%s」没被拦下" % name)
            print("selftest OK: 「%s」被拦下（%s）" % (name, hit[0]))
        try:
            hit = compare_posix_table(q7, posix_data, POSIX_TABLES[0][1], POSIX_TABLES[0][0])
        except AssertionError as e:
            hit = [str(e)]
        if not hit:
            sys.exit("selftest FAILED: 「POSIX 缺一行」没被拦下")
        print("selftest OK: 「POSIX 缺一行」被拦下（%s）" % hit[0])
        ntest = 10
        # §7.19 五处注入：改工作量、改派生 RSS delta、两行数值对调（键不动）、
        # 缺一个格子、破坏不变量。mv_doc[*] 的第 0 行是表头，注入必须打数据行。
        v1 = [r[:] for r in mv_doc[0]]
        v1[1][6] = str(int(num(v1[1][6])) + 1)          # groups
        v2 = [r[:] for r in mv_doc[3]]
        v2[1][4] = str(int(num(v2[1][4])) + 1)          # retained_delta_kb
        v3 = [r[:] for r in mv_doc[1]]
        v3[1][2:], v3[2][2:] = v3[2][2:], v3[1][2:]     # 只换数值不换 views/conns
        v4 = [r[:] for r in mv_doc[2]]
        v4 = v4[:-1]
        bad_mv = {k: dict(v) for k, v in mv_data.items()}
        k0 = sorted(bad_mv)[0]
        bad_mv[k0]["records"] += 5
        for name, mutated, idx in (
            ("MV 改 groups", v1, 0),
            ("MV 改 RSS delta", v2, 3),
            ("MV 两行数值对调", v3, 1),
            ("MV 缺一个格子", v4, 2),
        ):
            label, cols = MV_TABLES[idx]
            try:
                hit = compare_mv_table(mutated, mv_data, cols, label)
            except AssertionError as e:
                hit = [str(e)]
            if not hit:
                sys.exit("selftest FAILED: 「%s」没被拦下" % name)
            ntest += 1
            print("selftest OK: 「%s」被拦下（%s）" % (name, hit[0]))
        v5 = [r[:] for r in mv_probe_doc[0]]
        v5[1][6] = str(int(num(v5[1][6])) + 1)          # checkpoints_delta
        label, cols = MV_PROBE_TABLES[0]
        hit = compare_mv_table(v5, probe_data, cols, label)
        if not hit:
            sys.exit("selftest FAILED: 「探针改 checkpoint」没被拦下")
        ntest += 1
        print("selftest OK: 「探针改 checkpoint」被拦下（%s）" % hit[0])
        for name, hit in (("MV 记录数背离 5G", mv_invariants(bad_mv)),):
            if not hit:
                sys.exit("selftest FAILED: 「%s」没被拦下" % name)
            ntest += 1
            print("selftest OK: 「%s」被拦下（%s）" % (name, hit[0]))
        # §7.9 三处注入：复跑表改 ops、改前表改比值（那格是 1.00）、把改后 always
        # 假装没合批。两张表都含表头行，注入一律打数据行。

        w1 = [r[:] for r in sr_new_doc[0]]
        w1[1][2] = str(_si(w1[1][2]) + 1)
        w2 = [r[:] for r in sr_old_doc[0]]
        w2[2][5] = "**%.2f**" % (num(w2[2][5]) + 0.5)   # 机械盘 always 那一格
        bad_sr = {t: {k: dict(v) for k, v in d.items()} for t, d in sr.items()}
        dk = sorted(k for k in bad_sr["11-25"] if k[1] == "always")[0]
        bad_sr["11-25"][dk]["per_sync"] = 1.0        # 比值自洽，但合批没了
        bad_sr["11-25"][dk]["syncs"] = bad_sr["11-25"][dk]["writes"]
        for name, hit in (
            ("§7.9 复跑改 ops",
             compare_sr_table(w1, sr["11-25"], SR_HEAD_NEW, "11-25 复跑")),
            ("§7.9 改前改比值",
             compare_sr_table(w2, sr["11-19"], SR_HEAD_OLD, "11-19 采集")),
            ("§7.9 改后没合批", sr_invariants(bad_sr)),
        ):
            if not hit:
                sys.exit("selftest FAILED: 「%s」没被拦下" % name)
            ntest += 1
            print("selftest OK: 「%s」被拦下（%s）" % (name, hit[0]))
        # §10 补段四处注入：改套件表一格、改 ref 表一个 pin、破坏两臂对称、
        # 抹掉混代闸门的点名行。前两条打文档表，后两条打不变量的入参。
        y1 = [r[:] for r in pin_suite_doc]
        y1[1][2] = "%d / 0" % (int(y1[1][2].split(" / ")[0]) + 1)
        y2 = [r[:] for r in pin_ref_doc]
        y2[1][1] = "v0.0.0"
        bad_pin = {k: dict(v) for k, v in pin_suites.items()}
        bad_pin["A"]["test_treedb.sh"] = "60 / 1 / 0"
        for name, hit in (
            ("§10 套件表改一格",
             compare_pin_suite_table(y1, pin_suites, PIN_SUITE_COLS, "套件")),
            ("§10 ref 表改 pin",
             compare_pin_ref_table(y2, pin_refs, PIN_REF_COLS, "ref")),
            ("§10 两臂不对称", pin_invariants(bad_pin, pin_gate)),
            ("§10 混代闸门证据被抹", pin_invariants(pin_suites, False)),
        ):
            if not hit:
                sys.exit("selftest FAILED: 「%s」没被拦下" % name)
            ntest += 1
            print("selftest OK: 「%s」被拦下（%s）" % (name, hit[0]))
        print("selftest %d/%d" % (ntest, ntest))
