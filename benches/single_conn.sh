#!/bin/bash
# 单连接对标（DESIGN.md §7.11）：conns=1 下 treedb 默认档 vs 私有 PG 12 集群，
# 两边都必须真落盘（treedb `ack 晚于 fsync`；PG `fsync=on` + `synchronous_commit=on`）。
#
# 为什么单独要这么一支：throughput_matrix 的轴 2/轴 4 是全矩阵（3 个并发档 × 2 个
# pipeline × 6 个 op，两遍要十几分钟），而「一条连接、一问一答」这个形状是接入方
# 最先问的 —— group commit 无对象可合，两边都在付真实盘延迟。这一支只跑那几个
# 格子，pipeline 由 $PIPE 选、并发档由 $CONNS 选（默认只有 1），两遍取区间。
#
# 用法：bash benches/single_conn.sh [每格秒数=3]        # 单连接、一问一答
#       PIPE=32 bash benches/single_conn.sh 5           # 单连接批量
#       CONNS="1 8 64" bash benches/single_conn.sh 3    # 并发档扫描（§7.11 的对照行）
#       CONNS="8 64" WORKERS=8 bash benches/single_conn.sh 3   # 默认前端档（本机 8 worker）
# 输出追加到 benches/results/single_conn.log。
#
# WORKERS 默认 1（历史基线档，§7.11 那两张表就是它）：conns=1 时 workers 不起作用，
# 一条连接只会被派给一个 worker。但 conns=8/64 时 workers=1 就是「拿一个核去接
# 64 条 socket」，读档会被 PG 的多 backend 反超 —— 不标这一档就会把形状读成结论。
#
# 与轴 4 同样两条硬闸门：PG 的 fsync / synchronous_commit 只要有一个不是 on，
# PG 侧全部作废并 exit 1 —— 曾经踩过 `-F`（turn fsync off）量出 14.7 M ops/s 的鬼数字。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
BIN=./target/release
PIPE=${PIPE:-1}
CONNS=${CONNS:-1}
WORKERS=${WORKERS:-1}
DIR=.spot/db
PGBIN=/usr/lib/postgresql/12/bin
PGDATA="$ROOT/.pgref"
PGPORT=${PGPORT:-54391}
SRV=""
PG_STARTED=0

stop_srv() { [ -n "$SRV" ] && kill "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""; }
cleanup() { stop_srv; [ "$PG_STARTED" = 1 ] && "$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop >/dev/null 2>&1; }
trap cleanup EXIT

cell() { # OP CONNS PIPE DUR WORKERS
  local op=$1 conns=$2 pipe=$3 dur=$4 workers=$5
  stop_srv
  rm -rf "$DIR"; mkdir -p "$DIR"
  $BIN/treedb-server --dir "$DIR" --listen 127.0.0.1:$PORT \
      --durability "$dur" --auto-checkpoint-mb 0 --workers "$workers" > "$DIR/srv.log" 2>&1 &
  SRV=$!
  local i=0
  until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
    i=$((i + 1)); [ $i -gt 100 ] && { echo "treedb 服务端起不来：$(cat "$DIR/srv.log")" >> "$OUT"; return 1; }
    sleep 0.05
  done
  $BIN/treedb-bench --addr 127.0.0.1:$PORT --op "$op" \
      --conns "$conns" --threads "$conns" --pipeline "$pipe" --secs "$SECS" 2>"$DIR/bench.err" \
      | sed "s|^|treedb |" >> "$OUT"
  [ -s "$DIR/bench.err" ] && sed 's|^|  ! |' "$DIR/bench.err" | tail -2 >> "$OUT"
  # 把引擎自计的 fsync 计数器钉在同一格里：`syncs == writes` 就是「单连接没有
  # 可合并对象、这一格每 op 付一次真盘」的直接证据，不需要去推断。
  $BIN/treedb-server --listen 127.0.0.1:$PORT --stats 2>/dev/null \
      | grep -E '^ops ' | sed 's|^|treedb   [stats] |' >> "$OUT"
}

pgcell() { # OP CONNS
  local op=$1 conns=$2
  $BIN/treedb-pgbench --uri "postgresql://postgres@127.0.0.1:$PGPORT/postgres" \
      --op "$op" --conns "$conns" --pipeline "$PIPE" --secs "$SECS" 2>/tmp/pgc.err \
      | sed "s|^|pg     |" >> "$OUT"
  [ -s /tmp/pgc.err ] && sed 's|^|  ! |' /tmp/pgc.err | tail -2 >> "$OUT"
}

mkdir -p .spot benches/results
OUT=benches/results/single_conn.log
echo "=== 逐格对标 pipeline=$PIPE conns=\"$CONNS\" workers=$WORKERS ===" >> "$OUT"
PORT=${PORT:-17795}
if [ ! -f "$PGDATA/PG_VERSION" ]; then
  echo "initdb -> $PGDATA"
  mkdir -p "$PGDATA"; "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust -E UTF8 >/dev/null || exit 1
fi
PGO="-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$PGDATA -c max_connections=4050 -c fsync=on -c synchronous_commit=on"
"$PGBIN/pg_ctl" -D "$PGDATA" -o "$PGO" -w start >/dev/null 2>&1
pg_isready -q -h 127.0.0.1 -p $PGPORT || { echo "PG 起不来" | tee -a "$OUT"; exit 1; }
PG_STARTED=1
export PGHOST=127.0.0.1 PGPORT=$PGPORT PGUSER=postgres PGDATABASE=postgres
FSYNC=$(psql -Atc 'show fsync'); SYNCC=$(psql -Atc 'show synchronous_commit')
{ echo "pg $(psql -Atc 'select version()' | head -c 40)  fsync=$FSYNC synchronous_commit=$SYNCC"
  echo "host nproc=$(nproc)  WAL 盘：$(df -hT /data | tail -1 | awk '{print $2, $7}')"; } | tee -a "$OUT"
[ "$FSYNC" = on ] && [ "$SYNCC" = on ] || { echo "!! PG 持久化没开，PG 侧作废" | tee -a "$OUT"; exit 1; }

SECS=${1:-3}
for pass in 1 2; do
  echo "--- pass $pass (pipeline=$PIPE conns=\"$CONNS\" workers=$WORKERS) ---" >> "$OUT"
  for c in $CONNS; do
    for op in lookup readdir create write; do
      cell "$op" "$c" "$PIPE" group "$WORKERS"
      pgcell "$op" "$c"
    done
  done
done
echo "--- 以上： treedb 默认档（dur=group、workers=$WORKERS）vs PG（synchronous_commit=on），两边 ack 前都已落盘 ---" >> "$OUT"
tail -18 "$OUT"
