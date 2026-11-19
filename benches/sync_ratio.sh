#!/bin/bash
# 「写路径的领先来自跨连接合并 fsync」这句话的可复现证据。
#
# 为什么单独一个脚本：吞吐矩阵（throughput_matrix.sh）报的是 ops/s 与百分位，
# 它能说明 `group` 比 `always` 快几倍，但说不清**快在几次 fsync 上**。这里直接
# 读引擎自己的计数器（`--stats` 里的 writes / syncs），把「一次 fsync 摊了多少
# 次写」算出来 —— 那是机制本身，不是它的影子。
#
# 同一格（op=write、conns=64、pipeline=1）在两块盘 × 两个持久化档上各跑一次：
#   /data  = 161 TB 机械 XFS（真实部署位置，裸 fsync 毫秒级）
#   /tmp   = 根盘 ext4（裸 fsync ~236 µs）
# 顺带把 `always` 档在高并发下的尾延迟量出来（DESIGN.md §8 第 11 条）。
#
# 用法：benches/sync_ratio.sh [每格秒数=3]
# 结果追加到 benches/results/sync_ratio.log。
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
SECS=${1:-3}
BIN=./target/release
OUT=benches/results/sync_ratio.log
PORT=${PORT:-17795}
SRV=""
mkdir -p benches/results

stop_srv() {
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
  wait "$SRV" 2>/dev/null
  SRV=""
}
trap 'stop_srv' EXIT

# probe DUR DIR：起服务 → 打一格 → 读计数器 → 算每次 fsync 摊了多少写
probe() {
  local dur=$1 dir=$2
  stop_srv
  rm -rf "$dir"; mkdir -p "$dir"
  $BIN/treedb-server --dir "$dir" --listen 127.0.0.1:$PORT \
      --durability "$dur" --auto-checkpoint-mb 0 > "$dir/srv.log" 2>&1 &
  SRV=$!
  local i=0
  until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
    i=$((i + 1)); [ $i -gt 100 ] && { echo "服务端起不来：$(cat "$dir/srv.log")"; return 1; }
    sleep 0.05
  done
  local bench stats writes syncs per
  bench=$($BIN/treedb-bench --addr 127.0.0.1:$PORT --op write \
            --conns 64 --threads 64 --pipeline 1 --secs "$SECS" 2>/dev/null | head -1)
  # ops=212408 (writes 212406, syncs 3332, avg fsync 278 us)
  stats=$($BIN/treedb-server --dir "$dir" --listen 127.0.0.1:$PORT --stats 2>/dev/null \
            | grep '^ops ')
  writes=$(printf '%s' "$stats" | sed -n 's/.*writes \([0-9]*\).*/\1/p')
  syncs=$(printf '%s' "$stats"  | sed -n 's/.*syncs \([0-9]*\).*/\1/p')
  if [ -z "$writes" ] || [ -z "$syncs" ]; then echo "解析不到计数器：$stats"; return 1; fi
  per=$(awk -v w="$writes" -v s="$syncs" 'BEGIN{ if (s>0) printf "%.2f", w/s; else print "inf" }')
  printf '%-6s wal=%-8s %s\n' "$dur" "$(basename "$dir")" "$bench" | tee -a "$OUT"
  printf '%-6s wal=%-8s   writes=%s syncs=%s → %s 写/fsync\n' \
      "$dur" "$(basename "$dir")" "$writes" "$syncs" "$per" | tee -a "$OUT"
}

{
  echo "=== sync_ratio @ $(date -Is) (op=write conns=64 pipeline=1, ${SECS}s/格) ==="
} >> "$OUT"

probe group  /data/grovefs/treedb/.matrix/sr_group
probe always /data/grovefs/treedb/.matrix/sr_always
probe group  /tmp/treedb-sr-group
probe always /tmp/treedb-sr-always

stop_srv
echo "→ 结果在 $OUT"
