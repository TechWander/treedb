#!/bin/bash
# 端到端 A/B：同一个（口径已修正的）客户端分别打「改动前」与「改动后」两个服务端
# 二进制，把引擎改动的收益从量具改动里剥出来。
#
# 为什么必须这样量：矩阵里换一个口径（分母含不含预热、WAL 在哪块盘、几个视图、
# 目录多大）读档数字就动 30–60%，拿两次不同日期的矩阵格子对比会得出完全错误的
# 结论 —— 本轮就差点把「readdir 优化无效」写进设计文档，受控 A/B 出来是 1.4–1.8×。
#
# 用法：
#   benches/ab_layout.sh [基线 rev=HEAD~1]
# 前置：`cargo build --release` 已跑过（用当前工作树的 treedb-server 当 new 档）。
# old 档自己建一个临时工作区构建，跑完删掉。
set -u
cd "$(dirname "$0")/.." || exit 1
BASE=${1:-HEAD~1}
PORT=${PORT:-17840}
ROOT=$(pwd)
OLD_DIR=$(mktemp -d /tmp/treedb-ab.XXXXXX)
trap 'rm -rf "$OLD_DIR"; pkill -f "treedb-server --dir /tmp/ab-" 2>/dev/null' EXIT

say() { printf '%s\n' "$*"; }
say "构建基线 $BASE → $OLD_DIR"
git worktree add --detach "$OLD_DIR/repo" "$BASE" >/dev/null 2>&1 || exit 1
( cd "$OLD_DIR/repo" && cargo build --release --bin treedb-server ) 2>&1 | tail -1
NEW=$ROOT/target/release/treedb-server
OLD=$OLD_DIR/repo/target/release/treedb-server
BENCH=$ROOT/target/release/treedb-bench
[ -x "$NEW" ] && [ -x "$OLD" ] && [ -x "$BENCH" ] || { say "二进制不全"; exit 1; }

run_cell() { # $1=标签 $2=服务端 $3=warm $4=workers
    local tag=$1 srv=$2 warm=$3 w=$4
    local d=/tmp/ab-$tag-$w
    rm -rf "$d"; mkdir -p "$d"
    "$srv" --dir "$d" --listen 127.0.0.1:$PORT --durability never \
        --auto-checkpoint-mb 0 --workers "$w" >"$d/srv.log" 2>&1 &
    local pid=$!
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
        i=$((i+1)); [ $i -gt 100 ] && { say "$tag w=$w 起不来：$(cat "$d/srv.log")"; return 1; }
        sleep 0.05
    done
    # 一个服务端喂两个 op：预热（每线程 1 视图 × warm 条）只付一次。
    local op
    for op in readdir lookup; do
        printf '%-4s warm=%-6s w=%-3s ' "$tag" "$warm" "$w"
        TREEDB_BENCH_WARM="$warm" $BENCH --addr 127.0.0.1:$PORT --op "$op" --views 1 \
            --conns 64 --threads 64 --pipeline 1 --secs 3 2>&1 | tail -1 \
            | sed -E 's/^op=([a-z]+).* → ([0-9]+ ops\/s over [0-9.]+s).*p50=([0-9]+).*/\1 → \2 | p50=\3 us/'
    done
    kill $pid 2>/dev/null; wait $pid 2>/dev/null
    rm -rf "$d"
}

# 目录规模两档：500 = 与历史矩阵同形；16000 = 单目录万级，接近 grovefs 里
# 一个视图根目录的真实形状。
for WARM in 500 16000; do
    for W in 1 8; do
        run_cell old "$OLD" "$WARM" "$W"
        run_cell new "$NEW" "$WARM" "$W"
    done
done
