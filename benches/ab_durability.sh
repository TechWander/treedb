#!/bin/bash
# 写轴持久化档的受控 A/B：同一份客户端，分别打「改动前 / 改动后」两个服务端
# 二进制，只换服务端，比 `--durability always|group` 的吞吐与尾延迟。
#
# 为什么必须同场对照：fsync 是机械盘的行为，两次不同日期的跑法差 30–60 %，
# 拿历史格子相减会得出错误结论（§9.1 第 1 条）。这条脚本的存在只为一件事：
# 证明「把 always 档的按连接串行 fsync 合成每轮一次」确实把队头等待消掉了，
# 而且没把 group 档改坏。
#
# 用法：
#   benches/ab_durability.sh [基线 rev=v0.3.1] [每格秒数=3] [每格次数=3]
# 前置：`cargo build --release` 已跑过（当前工作树的 treedb-server 当 new 档）。
# old 档自己建临时工作区构建，跑完删掉。结果追加到
# benches/results/ab_durability.log（原始行，中位数自己算，别手抄）。
set -u
cd "$(dirname "$0")/.." || exit 1
BASE=${1:-v0.3.1}
SECS=${2:-3}
REPS=${3:-3}
PORT=${PORT:-17846}
ROOT=$(pwd)
OLD_DIR=$(mktemp -d /tmp/treedb-abdur.XXXXXX)
DDIR=$ROOT/.matrix/abdur
OUT=benches/results/ab_durability.log
SRV=""
mkdir -p benches/results .matrix

say() { printf '%s\n' "$*"; }
# 一行同时进终端与结果文件。**不能用 `cell | tee`**：那会把 cell 丢进子 shell，
# 里面的 $SRV 传不出来，服务进程就成了没人管的孤儿，下一格抢不到端口。
emit() { say "$*"; printf '%s\n' "$*" >> "$OUT"; }
stop_srv() {
    [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
    wait "$SRV" 2>/dev/null
    SRV=""
}
trap 'stop_srv; rm -rf "$OLD_DIR"; rm -rf "$DDIR"-*' EXIT

say "构建基线 $BASE → $OLD_DIR"
git worktree add --detach "$OLD_DIR/repo" "$BASE" >/dev/null 2>&1 || exit 1
( cd "$OLD_DIR/repo" && cargo build --release --bin treedb-server ) 2>&1 | tail -1
NEW=$ROOT/target/release/treedb-server
OLD=$OLD_DIR/repo/target/release/treedb-server
BENCH=$ROOT/target/release/treedb-bench
[ -x "$NEW" ] && [ -x "$OLD" ] && [ -x "$BENCH" ] || { say "二进制不全"; exit 1; }

# cell 臂 档 第n次：每格全新数据目录（WAL 必须在被测量那块盘 = /data 机械 XFS）。
cell() {
    local arm=$1 dur=$2 n=$3
    local srv=$OLD
    [ "$arm" = new ] && srv=$NEW
    local d=$DDIR-$arm-$dur-$n
    stop_srv
    rm -rf "$d"; mkdir -p "$d"
    "$srv" --dir "$d" --listen 127.0.0.1:$PORT --durability "$dur" \
        --auto-checkpoint-mb 0 --workers 8 >"$d/srv.log" 2>&1 &
    SRV=$!
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
        i=$((i + 1)); [ $i -gt 100 ] && { say "$arm $dur #$n 起不来：$(cat "$d/srv.log")"; return 1; }
        sleep 0.05
    done
    local raw stats writes syncs per
    raw=$($BENCH --addr 127.0.0.1:$PORT --op write --conns 64 --threads 64 \
              --pipeline 1 --secs "$SECS" 2>/dev/null | head -1)
    # 计数器走服务端自己的 --stats 只读口：ops N (writes N, syncs N, avg fsync X us)
    stats=$("$srv" --dir "$d" --listen 127.0.0.1:$PORT --stats 2>/dev/null | grep '^ops ')
    writes=$(printf '%s' "$stats" | sed -n 's/.*writes \([0-9]*\).*/\1/p')
    syncs=$(printf '%s' "$stats"  | sed -n 's/.*syncs \([0-9]*\).*/\1/p')
    per=$(awk -v w="${writes:-0}" -v s="${syncs:-0}" 'BEGIN{ if (s>0) printf "%.2f", w/s; else print "?" }')
    emit "raw  $arm $dur #$n  $raw"
    emit "cell $arm $dur #$n  writes=${writes:-?} syncs=${syncs:-?} ratio=$per"
    stop_srv
    rm -rf "$d"
}

{
    say ""
    say "=== ab_durability @ $(date -Is) base=$BASE op=write conns=64 pipeline=1 ${SECS}s x${REPS} ==="
} >> "$OUT"

for n in $(seq 1 "$REPS"); do
    for dur in always group; do
        # old/new 相邻跑：同一分钟内两块盘的状态才可比
        cell old "$dur" "$n"
        cell new "$dur" "$n"
    done
done
say "结果在 $OUT（中位数：grep '^raw' $OUT | tail -24）"
