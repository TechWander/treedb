#!/usr/bin/env python3
"""三臂对照表：把两份原始采集拼成一张可用于文档的 TSV。

输入（都不改）：
  benches/results/three_way.tsv   `three_way.sh` 一轮采完的 3 引擎 × 24 格
  benches/results/pg_rerun.tsv    PG 在 `ANALYZE` 之后单独复跑的 24 格

输出：benches/results/final.tsv，24 行 × 14 列，列含义见 HEADER。

为什么要有这个脚本：上一版的 final.tsv 是手搓的 one-liner 拼出来的，p50 与 p99
被拼进了同一列（`3` 和 `4` 变成 `34`），看上去像真数字 —— 这种表一旦被抄进
文档就再也查不出来。所以生成器自带三处断言：格数、字段数、倍率回算。

用法： python3 benches/three_way_table.py [--check|--selftest]
  --check    只校验这份表与原始采集是否自洽，不写文件。
  --selftest 拿三处人为注入的错去撞 check()，每处都必须报错（防止校验器变橡皮图章）。
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = ROOT + "/benches/results/three_way.tsv"
PG2 = ROOT + "/benches/results/pg_rerun.tsv"
OUT = ROOT + "/benches/results/final.tsv"

HEADER = ["op", "conns", "pipe",
          "treedb_w8", "t_p50", "t_p99",
          "pg", "pg_p50", "pg_p99",
          "sqlite", "s_p50", "s_p99",
          "ratio_pg", "ratio_sqlite"]
ENGINES = ("treedb-w8", "pg", "sqlite")
OPS = ["lookup", "readdir", "create", "write"]


def load(path, want):
    """-> {(op, conns, pipe): (ops, p50, p99)}，只收 want 这一臂的行。"""
    rows = {}
    for line in open(path):
        if line.startswith("#"):
            continue
        f = line.split()
        if len(f) != 7:
            continue
        eng, op, c, p, ops, p50, p99 = f
        if eng != want:
            continue
        key = (op, int(c), int(p))
        assert key not in rows, "%s 里 %s 重复" % (path, key)
        rows[key] = (int(ops), int(p50), int(p99))
    return rows


def build():
    cells = {e: load(RAW, e) for e in ENGINES}
    keys = sorted(cells["treedb-w8"], key=lambda k: (OPS.index(k[0]), k[1], k[2]))
    assert len(keys) == 24, "treedb 臂应 24 格，实得 %d" % len(keys)
    for e in ENGINES:
        assert set(cells[e]) == set(keys), "%s 的格集与 treedb 不一致" % e
    # PG 那一列用 ANALYZE 之后的复跑（未 ANALYZE 的 plan 把 readdir 低估了 1.5–2.5×）；
    # 复跑是同一台机同一块盘 24 分钟后的另一轮，treedb/sqlite 列仍是第一轮。
    rerun = load(PG2, "pg")
    assert set(rerun) == set(keys), "pg_rerun 的格集与主表不一致"
    cells["pg"] = rerun

    lines = ["\t".join(HEADER)]
    for k in keys:
        t, g, s = cells["treedb-w8"][k], cells["pg"][k], cells["sqlite"][k]
        lines.append("\t".join(str(x) for x in (
            k[0], k[1], k[2],
            t[0], t[1], t[2],
            g[0], g[1], g[2],
            s[0], s[1], s[2],
            round(t[0] / g[0], 2), round(t[0] / s[0], 2))))
    return "\n".join(lines) + "\n"


def check(text):
    lines = text.rstrip("\n").split("\n")
    assert lines[0].split("\t") == HEADER, "表头不对"
    assert len(lines) == 25, "应为 1 表头 + 24 行，实得 %d" % (len(lines) - 1)
    for ln in lines[1:]:
        f = ln.split("\t")
        assert len(f) == len(HEADER), "字段数 %d != %d: %s" % (len(f), len(HEADER), ln)
        for v in f[3:]:
            assert v.lstrip("-").replace(".", "", 1).isdigit(), "非数字格: %s" % ln
        t, g, s = int(f[3]), int(f[6]), int(f[9])
        # 倍率必须能由同行的 ops 回算出来，防止再出现"拼列"这类错位
        assert abs(float(f[12]) - round(t / g, 2)) < 0.011, "ratio_pg 回算不符: %s" % ln
        assert abs(float(f[13]) - round(t / s, 2)) < 0.011, "ratio_sqlite 回算不符: %s" % ln


if __name__ == "__main__":
    text = build()
    if "--selftest" in sys.argv:
        check(text)
        lines = text.rstrip("\n").split("\n")
        bad = [
            ("p50/p99 拼回一列", [lines[0]] + [ln.replace("\t3\t4\t", "\t34\t") for ln in lines[1:]]),
            ("倍率与 ops 不符", [lines[0], lines[1].replace("\t9.62\t", "\t19.62\t")] + lines[2:]),
            ("缺一格", lines[:-1]),
        ]
        for name, mutated in bad:
            try:
                check("\n".join(mutated) + "\n")
            except AssertionError:
                print("selftest OK: %s 被拦下" % name)
            else:
                raise SystemExit("selftest FAILED: %s 没被拦下" % name)
        print("selftest 3/3")
    elif "--check" in sys.argv:
        on_disk = open(OUT).read()
        check(on_disk)
        assert on_disk == text, "%s 落后于原始采集，请重跑生成器" % OUT
        print("final.tsv 与原始采集自洽（24 格 × %d 列）" % len(HEADER))
    else:
        check(text)
        open(OUT, "w").write(text)
        print("wrote %s (%d 行)" % (OUT, text.count("\n")))
