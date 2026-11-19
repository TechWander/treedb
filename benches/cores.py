#!/usr/bin/env python3
"""测量窗口内服务端吃掉多少核：10 Hz 采样累计 CPU ticks，事后按窗口长度回切。

为什么不能"跑之前读一次、跑之后读一次、除以 --secs"：bench 进程的墙上时间 =
预热 + 测量窗口，而预热（lookup/readdir 要灌几万条 create）本身就让服务端满负荷
干活。分子含预热、分母只给窗口，算出来的核数直接虚高。故这里把采样与计算拆开：

    cores.py sample <pid> <threads|tree> <outfile>   # 后台跑，边采样边追加
    cores.py calc <outfile> <span>                   # span = bench 自报的窗口秒数

回切方式：以最后一个采样点当窗口结束（采样器紧跟在 bench 之后被杀，误差 <0.1 s），
往前找 span 秒的那个采样点，用两侧积分差 ÷ span。

为什么不用 bash 数 /proc/<pid>/stat 的字段号：第 2 个字段 comm 是带括号的可变串，
进程名一旦含空格（`postgres: walwriter` 这种改了标题的）整行字段号全错位，读回来
的数是垃圾。故一律先按 ')' 切一刀再 split。

两种求和口径：
  threads  /proc/<pid>/task/* 全算 —— treedb 的 worker/committer 是**线程**，
           主线程 stat 里没有它们的 CPU。
  tree     postmaster + 直系子进程 —— PG 的 backend 是 **fork 出来的进程**。
"""
import glob
import os
import sys
import time


def ticks(path):
    try:
        with open(path) as f:
            s = f.read()
    except OSError:
        return 0
    try:
        rest = s[s.rindex(')') + 2:].split()
    except ValueError:
        return 0
    # rest[0] 是 state（原字段 3），utime/stime 是原字段 14/15 → 下标 11/12
    return int(rest[11]) + int(rest[12])


def us_ticks(path):
    """同 ticks，但分开返回 (utime, stime) —— 归因要用它拆计算 vs 系统调用。"""
    try:
        with open(path) as f:
            s = f.read()
    except OSError:
        return 0, 0
    try:
        rest = s[s.rindex(')') + 2:].split()
    except ValueError:
        return 0, 0
    return int(rest[11]), int(rest[12])


def sum_us(pid, mode):
    """按口径汇总 (utime, stime) ticks。"""
    if mode == 'tree':
        pids = [pid] + [int(c) for c in kids(pid)]
    else:
        pids = [pid]
        u = 0
        s = 0
        for p in glob.glob(f'/proc/{pid}/task/*/stat'):
            a, b = us_ticks(p)
            u += a
            s += b
        return u, s
    u = s = 0
    for p in pids:
        a, b = us_ticks(f'/proc/{p}/stat')
        u += a
        s += b
    return u, s


def kids(pid):
    cfile = f'/proc/{pid}/task/{pid}/children'
    # 绝不能写 isdir —— /proc/<pid>/task/<pid>/children 是普通文件不是目录，
    # 上一版用 isdir 判，永远为假，于是 PG 64 个 backend 的 CPU 全部不算，
    # cores 永远报 0.00：看上去像「PG 一个核没花」。
    if not os.path.isfile(cfile):
        return []
    try:
        with open(cfile) as f:
            return f.read().split()
    except OSError:
        return []


def total(pid, mode):
    if mode == 'tree':
        t = ticks(f'/proc/{pid}/stat')
        for c in kids(pid):
            t += ticks(f'/proc/{c}/stat')
        return t
    return sum(ticks(p) for p in glob.glob(f'/proc/{pid}/task/*/stat'))


def main():
    cmd = sys.argv[1]
    if cmd == 'sample':
        pid, mode, out = int(sys.argv[2]), sys.argv[3], sys.argv[4]
        with open(out, 'w') as f:
            while True:
                try:
                    f.write(f'{time.monotonic():.3f} {total(pid, mode)}\n')
                    f.flush()
                except (ProcessLookupError, FileNotFoundError):
                    break
                time.sleep(0.1)
        return
    if cmd == 'split':
        # split <pid> <threads|tree> <秒数> —— 直接报这段时间里服务端吃了多少
        # 核的**用户时间**、多少核的**系统时间**。它是「瓶颈在计算还是在
        # syscall」的唯一凭据：micro 只量得到引擎里那几纳秒，量不到从网卡
        # 到引擎之间那段；没perf 的情况下，user/sys 拆分就是最便宜的归因。
        pid, mode, secs = int(sys.argv[2]), sys.argv[3], float(sys.argv[4])
        u0, s0 = sum_us(pid, mode)
        t0 = time.monotonic()
        time.sleep(secs)
        u1, s1 = sum_us(pid, mode)
        dur = time.monotonic() - t0
        cu, cs = (u1 - u0) / 100.0 / dur, (s1 - s0) / 100.0 / dur
        tot = cu + cs
        print(f'cores_user={cu:.2f} cores_sys={cs:.2f} cores_total={tot:.2f} '
              f'sys占比={100 * cs / tot if tot > 0 else 0:.0f}% 窗口={dur:.2f}s')
        return
    if cmd == 'idle':
        # 挑一个当下最闲的核。共享机器上 load 能到 50，写死 CPU=3 可刚好压到
        # 别人正在跑的那个核上 —— 单核口径的数就这样被偷走一半，且看不出来。
        # 排除核 0（中断/定时器默认落点）。
        def snap():
            out = {}
            for ln in open('/proc/stat'):
                f = ln.split()
                if f[0].startswith('cpu') and f[0] != 'cpu':
                    n = int(f[0][3:])
                    v = list(map(int, f[1:8]))
                    out[n] = (sum(v) - v[3] - v[4], sum(v))  # busy, total
            return out
        a, b = snap(), None
        time.sleep(0.3)
        b = snap()
        c = sorted(((bb[0] - a[k][0]) / max(1, bb[1] - a[k][1]), k)
                   for k, bb in b.items() if k != 0)
        print(c[0][1])
        return
    if cmd == 'calc':
        # 下标要从 2 起：argv[1] 是子命令名本身，把它当文件名会得到
        # FileNotFoundError: 'calc' 这种莫名其妙的报错。
        #
        # 逐行容错：上一版遇到一行垃圾就整个 traceback 退出，而垃圾是会出现的
        # （同名文件被两个采样器先后 O_TRUNC 打开，中间留出一段 NUL 洞）。宁可
        # 丢掉坏点也不要静默报 0.00 —— 那比报错更坏，它会让人以为 PG 没花 CPU。
        rows = []
        for ln in open(sys.argv[2], errors='replace'):
            a, _, b = ln.strip().partition(' ')
            try:
                rows.append((float(a), int(b)))
            except ValueError:
                continue
        span = float(sys.argv[3])
        if len(rows) < 2 or span <= 0:
            print('NA')
            return
        end_t, end_k = rows[-1]
        start = None
        for t, k in rows:
            if t >= end_t - span:
                start = (t, k)
                break
        if start is None:
            # 采样太短，不够回切一个完整窗口：报整段平均，别装作是窗口值
            start = rows[0]
        dur = end_t - start[0]
        print(f'{(end_k - start[1]) / 100.0 / dur:.2f}' if dur > 0 and end_k >= start[1] else 'NA')
        return
    print(__doc__)
    sys.exit(2)


main()
