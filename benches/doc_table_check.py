#!/usr/bin/env python3
"""DESIGN §7.13 / §7.18 的表 vs 原始采集的逐格对账。

为什么单列一个检查器：这两组表的手抄数字都容错长得像真数字（3 和 4 拼成 34、
900 抄成 90）。判据只有一条：**每个数字都必须能从原始 TSV 重新算出来**。§7.13 的
final.tsv 还要由两份三路采集重算（`benches/three_way_table.py --check`）；§7.18 的
retained delta 由 rss_after − rss_before 派生，其余格子直接读 delete_storm.tsv 与
posix_delete_storm.tsv。

用法：
  python3 benches/doc_table_check.py             # 对账（不一致 exit 1）
  python3 benches/doc_table_check.py --selftest  # 注入 10 处错，要求全部被拦下
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOC = ROOT + "/DESIGN.md"
TSV = ROOT + "/benches/results/final.tsv"
P2_TSV = ROOT + "/benches/results/delete_storm.tsv"
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


def parse_section_tables(path, heading, stop):
    """把一个章节里的 Markdown 表格切成 [表头, 数据行...] 列表。"""
    tables, current, inside = [], None, False
    for raw in open(path):
        line = raw.rstrip("\n")
        if line.startswith(heading):
            inside = True
            continue
        if inside and line.startswith(stop):
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
        print("selftest 10/10")
