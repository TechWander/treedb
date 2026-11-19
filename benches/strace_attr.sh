#!/bin/bash
# 归因：treedb --workers 1 每 op 到底付了几次 syscall、syscall 占多少时间。
# 为什么先做这步再谈优化：单核 ping 是 7.5 µs/op，而引擎侧 micro 量到 9 ns ——
# 差三个数量级，说明瓶颈不在数据结构；不先把 syscall 账算清楚就去改 BTreeMap 是白改。
#
# 两个手法上的坑：
#   1. 这台机 kernel.yama.ptrace_scope=1，非 root 不能 -p 附加到已有进程，
#      但**父进程 ptrace 自己 spawn 的子进程**是允许的 —— 所以 strace 当父起服务端。
#   2. strace 会把吞吐拖 5–10 倍，所以这里绝不报 ops/s，只报**每个 op 的 syscall 数**：
#      分子取 strace 的计数，分母取服务端自己关停时上报的累计 ops（同一进程、同一段
#      生命期，两边天然对齐，不需要猜窗口边界）。
#
# 用法：strace_attr.sh <op> [durability=never] [秒数=4]
cd /data/grovefs/treedb || exit 1
OP=${1:-ping}
DUR=${2:-never}
SECS=${3:-4}
CPU=${CPU:-1}
PORT=${PORT:-17871}
D=/tmp/strace-$OP
(exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && { echo "!! 端口 $PORT 被占"; exit 1; }
rm -rf "$D"; mkdir -p "$D"
strace -f -c -o "$D/sum" taskset -c "$CPU" ./target/release/treedb-server \
    --dir "$D" --listen 127.0.0.1:$PORT --durability "$DUR" --auto-checkpoint-mb 0 \
    --workers 1 >"$D/srv.log" 2>&1 &
ST=$!
i=0
until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
    i=$((i + 1))
    # 必须有上限：上一版 strace 不认 -Q 直接退出（服务端根本没起来），
    # 这个 until 就无限转圈，从外面看是「探活挂死」而不是「工具链报错」。
    [ $i -gt 100 ] && { echo "!! 服务端 5 s 没起来：$(cat "$D/srv.log") $(tail -2 "$D/sum" 2>/dev/null)"; exit 1; }
    kill -0 "$ST" 2>/dev/null || { echo "!! strace 自己退了：$(tail -3 "$D/srv.log")"; exit 1; }
    sleep 0.05
done
./target/release/treedb-bench --addr 127.0.0.1:$PORT --op "$OP" --conns 64 --threads 64 \
    --pipeline 1 --secs "$SECS" 2>&1 | tail -1 >"$D/bench.txt"
# 先收服务端（SIGTERM 走优雅关停，才会打印 ops），再收 strace。
# 早先按 $! 杀的是 strace 本身，服务端变孤儿接着跑 —— 既拿不到 ops 也占端口。
SRV=$(pgrep -nx treedb-server)
[ -n "$SRV" ] && { kill "$SRV" 2>/dev/null; for i in 1 2 3 4 5 6 7 8 9 10; do kill -0 "$SRV" 2>/dev/null || break; sleep 0.3; done; }
kill "$ST" 2>/dev/null; wait 2>/dev/null
echo "bench（strace 在场，只作参考不作性能数）: $(cat "$D/bench.txt")"
SRVOPS=$(grep -oE 'ops [0-9]+' "$D/srv.log" | tail -1 | grep -oE '[0-9]+')
echo "服务端累计 ops=$SRVOPS"
echo "--- 每 op syscall 数（strace -c 列序：%time seconds usecs/call calls errors syscall； calls 是第 4 列不是第 5 列）---"
awk -v n="$SRVOPS" 'NR>2 && NF>=6 && $1 ~ /^[0-9.]+$/ {
    c=$4+0; e=$5+0;
    printf "%-14s calls/op=%7.3f  err/op=%6.3f  %6.1f%% 系统时间  %8.2f us/op\n", $7, c/n, e/n, $1, $2*1e6/n
}' "$D/sum" | sort -t= -k2 -rn | head -12
grep -E 'rounds|worst' "$D/srv.log" | tail -1
