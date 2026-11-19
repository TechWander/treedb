#!/bin/bash
# 归因第二刀：单核那 6.6 µs/op 里，多少是系统时间（syscall），多少是用户时间
# （拆帧/编码/分配/表操作）。strace 只给了次数，没给「这些次数在总成本里占多少」，
# 而 strace 自己的开销又让它的秒数不能直接用 —— 所以这里在无 strace 的真实负载下
# 读 /proc/<pid>/task/*/stat 的 utime/stime 差分，这是没 perf 时最干净的拆法。
#
# 用法：split_attr.sh <op> [workers=1] [秒数=8] [durability=never]
#
# 想拿它做「改前 vs 改后」：把 tag 或基线 rev 用 `git worktree` 开出来、在里头
# `cargo build --release --bin treedb-server`，再把那个二进制临时放到
# ./target/release/treedb-server 上过一遍本脚本，跑完立即还原（`benches/ab_layout.sh`
# 就是这套手法；线协议未变时客户端不需要跟着换）。落盘的
# `benches/results/split_attr.log` 里 before 段就是这么出的，末尾记了还原后的 sha256。
cd /data/grovefs/treedb || exit 1
OP=${1:-ping}
PREROLL=${PREROLL:-2}
W=${2:-1}
SECS=${3:-8}
DUR=${4:-never}
CPU=${CPU:-1}
PORT=${PORT:-17872}
D=/tmp/split-$OP-$W
(exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && { echo "!! 端口 $PORT 被占"; exit 1; }
rm -rf "$D"; mkdir -p "$D"
taskset -c "$CPU" ./target/release/treedb-server --dir "$D" --listen 127.0.0.1:$PORT \
    --durability "$DUR" --auto-checkpoint-mb 0 --workers "$W" >"$D/srv.log" 2>&1 &
SRV=$!
i=0
until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
    i=$((i + 1)); [ $i -gt 100 ] && { echo "!! 服务端没起来：$(cat "$D/srv.log")"; exit 1; }
    sleep 0.05
done
MODE=threads
[ "$W" != 1 ] && MODE=threads
./target/release/treedb-bench --addr 127.0.0.1:$PORT --op "$OP" --conns 64 --threads 64 \
    --pipeline 1 --secs "$SECS" >"$D/bench.txt" 2>&1 &
BN=$!
sleep ${PREROLL:-2}
python3 benches/cores.py split "$SRV" "$MODE" $((SECS - PREROLL - 2))
wait "$BN"
grep -oE '→ [0-9]+ ops/s.*' "$D/bench.txt" | head -1
kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null
