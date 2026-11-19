#!/usr/bin/env python3
"""把 sc_ops.sh 的 cell 行汇总成四臂对照表。

单独成文件而不是内嵌 heredoc，是因为它要被反复调用：优化前后各跑一次 t1 臂，
拿同一张表对比才有意义，而内嵌那份只在脚本末尾跑一次。

三列比值各自的含义必须分清，混起来就是「单核那一列输给 PG」这句话的来历：
  t1/p1  同一核、同并发 —— 两边都只给一个核的**绝对吞吐**，这才是
         「缺不缺索引、有没有压榨」的判据（p1 臂 PG 只用到 0.07 核，那是它
         在单核上抢时间片抢不动，不是它省下来的性能）
  t1/pf  一个核 对 PG 全核 —— 并行度差，treedb 侧靠 --workers 补，与单核无关
  t1/pf 每核  上式两边各自除以实测核数，把并行度彻底摘出去后的每核效率
"""
import re
import statistics
import sys

CELL = re.compile(
    r'cell arm=(\S+) op=(\S+) (?:w=\S+ )?rep=\d+ ops=(\d+) p50=(\d+) p99=(\d+) '
    r'cores=([0-9.]+|NA) cpu=\S+ load=([\d.]+)'
)
OPS_ORDER = ['ping', 'lookup', 'readdir', 'create', 'write']


def load(path):
    rows = {}
    for ln in open(path, encoding='utf-8', errors='replace'):
        m = CELL.match(ln.strip())
        if not m:
            continue
        arm, op, ops, p50, p99, cores, load1 = m.groups()
        if cores == 'NA':
            continue
        rows.setdefault((arm, op), []).append(
            (int(ops), int(p50), int(p99), float(cores), float(load1))
        )
    return rows


def med(rows, arm, op, i):
    v = rows.get((arm, op))
    if not v:
        return None
    return statistics.median(x[i] for x in v)


def spread(rows, arm, op):
    v = [x[0] for x in rows.get((arm, op), [])]
    if len(v) < 2:
        return ''
    return f'[{min(v)}~{max(v)}]'


def table(rows, arms=('t1', 'p1', 'pf', 't8'), title=''):
    if title:
        print(f'\n## {title}')
    hdr = f"{'op':9}" + ''.join(f'{a + " ops/s":>12}{a + "核":>7}' for a in arms)
    print(hdr)
    print('-' * len(hdr))
    for op in OPS_ORDER:
        # 只要有 t1 就出一行：SKIP_PG=1 的 A/B 复验里 PG 两臂本来就是空的，
        # 而这一档要看的恰恰就是 t1 自己前没动。
        if not rows.get(('t1', op)):
            continue
        line = f'{op:9}'
        for a in arms:
            v = med(rows, a, op, 0)
            c = med(rows, a, op, 3)
            line += (f'{v:>12.0f}{c:>7.2f}' if v is not None else f"{'-':>12}{'-':>7}")
        print(line)
        for a in arms:
            if a in ('t1', 'p1') and rows.get((a, op)):
                print(f'         {a} {spread(rows, a, op)} '
                      f'p50={med(rows, a, op, 1):.0f}us p99={med(rows, a, op, 2):.0f}us '
                      f'每核={med(rows, a, op, 0) / max(0.01, med(rows, a, op, 3)):.0f} ops/core')
    if rows.get(('t1', 'ping')) and rows.get(('p1', 'ping')):
        print(f"\n{'op':9}{'t1/p1 同核':>11}{'t1/pf 绝对':>12}{'t1/pf 每核':>12}{'t8/t1 扩展':>12}")
        for op in OPS_ORDER:
            t1, p1, pf, t8 = (med(rows, a, op, 0) for a in ('t1', 'p1', 'pf', 't8'))
            if not all([t1, p1, pf]):
                continue
            # t1/p1 比的是**绝对吞吐**，不是每核：两臂被钉在同一个核上，而 PG 那个
            # 核只用了 0.07（64 个 backend 在抢 CFS 时间片，全在等）。拿它自己的
            # 每核数去除会把「PG 在等」洗成「PG 很快」——这里要的就是单核表现本身。
            line = f'{op:9}{t1 / p1:>10.2f}x{t1 / pf:>11.2f}x'
            # t1/pf 必须除核数：pf 那臂 PG 摊在几十个核上，不除掉并行度就不可比。
            c1, cf = med(rows, 't1', op, 3), med(rows, 'pf', op, 3)
            line += (f'{(t1 / max(0.01, c1)) / (pf / max(0.01, cf)):>11.1f}x'
                     if c1 and cf else f'{"-":>12}')
            print(line + (f'{t8 / t1:>11.2f}x' if t8 else ' ' * 12))


if __name__ == '__main__':
    table(load(sys.argv[1]), title=sys.argv[2] if len(sys.argv) > 2 else '')
