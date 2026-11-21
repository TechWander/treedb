#!/usr/bin/env python3
"""DESIGN §7.13 的表 vs 原始采集的逐格对账。

为什么单列一个检查器：§7.13 那张表有 24 行 × 14 列数字，手抄一遍必然出错，而错法
（3 和 4 拼成 34、900 抄成 90）读起来都像真数字。判据只有一条：**每个数字都必须能
从 `benches/results/final.tsv` 重新算出来**，而 final.tsv 自己又必须能从两份原始采集
重算（`benches/three_way_table.py --check`）。

用法：
  python3 benches/doc_table_check.py             # 对账（不一致 exit 1）
  python3 benches/doc_table_check.py --selftest  # 注入三处错，要求检查器都变红
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOC = ROOT + "/DESIGN.md"
TSV = ROOT + "/benches/results/final.tsv"
COLS = ["op", "conns", "pipe", "t_ops", "t_p50", "t_p99", "pg_ops", "pg_p50",
        "pg_p99", "s_ops", "s_p50", "s_p99", "r_pg", "r_sqlite"]


def num(s):
    return float(s.replace("**", "").replace("×", "").replace(" ", ""))


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

    if "--selftest" in sys.argv:
        # 三处注入：改一个 ops、把 p50/p99 拼成一列、删一行
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
        print("selftest 3/3")
