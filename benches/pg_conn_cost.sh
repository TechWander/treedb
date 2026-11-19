#!/bin/bash
# PG 连接成本对照：同样这些连接，走「1 连接 = 1 个 backend 进程」要多少内存。
#
# 这是给 treedb 的连接轴结论配的分母。刻意**不碰共享实例**（5432/5433 上
# 跑着别的服务），而是在 .pgref/ 里 initdb 一个私有集群，测完就停。
#
# 用法：
#   benches/pg_conn_cost.sh [连接数，默认 1000]
#
# 度量口径：postmaster 全部子进程的 VmRSS 之和，开连接前 / 后各取一次，
# 差值 ÷ 连接数 = 每条连接的真实进程内存代价。用差值而不是「按进程名挑
# backend」，是因为进程标题会随状态变（`postgres: xxx idle` 等），名字匹配
# 既脆又容易把后台 worker 算进去。
set -u

N=${1:-1000}
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PGDATA="$ROOT/.pgref"
PORT=54390
BIN=/usr/lib/postgresql/12/bin
SOCK="$ROOT/.pgref"
export PGHOST=127.0.0.1 PGPORT=$PORT PGUSER=postgres PGDATABASE=postgres

say() { printf '%s\n' "$*"; }
PGO="-p $PORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$SOCK -F"

start_cluster() {
  if [ ! -f "$PGDATA/PG_VERSION" ]; then
    say "initdb -> $PGDATA"
    mkdir -p "$PGDATA"
    "$BIN/initdb" -D "$PGDATA" -U postgres --auth=trust -E UTF8 >/dev/null || exit 1
  fi
  "$BIN/pg_ctl" -D "$PGDATA" -o "$PGO" -w start >/dev/null 2>&1 || \
  "$BIN/pg_ctl" -D "$PGDATA" -o "$PGO" -w start
}

children_rss_kb() {
  local pm=$1 sum=0 v
  for p in $(pgrep -P "$pm"); do
    v=$(awk '/^VmRSS:/{print $2}' "/proc/$p/status" 2>/dev/null)
    [ -n "$v" ] && sum=$((sum + v))
  done
  echo "$sum"
}

say "=== 1. 起私有 PG 12 集群（端口 $PORT，不动共享实例） ==="
start_cluster
# 把 max_connections 抬到 N+50：这是私有集群自己的配置文件，不改别人的。
want=$((N + 50))
cur=$(psql -Atc 'show max_connections')
if [ "${cur:-0}" -lt "$want" ]; then
  say "max_connections $cur -> $want（写私有集群 postgresql.conf 并重启）"
  sed -i "s/^#*max_connections = .*/max_connections = $want/" "$PGDATA/postgresql.conf"
  grep -q '^max_connections' "$PGDATA/postgresql.conf" || echo "max_connections = $want" >> "$PGDATA/postgresql.conf"
  "$BIN/pg_ctl" -D "$PGDATA" -o "$PGO" -w restart >/dev/null || {
    say "重启失败：多半是 shared memory 需求超了内核上限（ shmmax/shmall ）。降连接数重试。"
    exit 1
  }
  cur=$(psql -Atc 'show max_connections')
fi
pm=$(head -1 "$PGDATA/postmaster.pid")
say "postmaster pid=$pm  max_connections=$cur  shared_buffers=$(psql -Atc 'show shared_buffers')"

before=$(children_rss_kb "$pm")
bc=$(psql -Atc "select count(*) from pg_stat_activity")
say "基线：子进程 RSS 合计 $((before / 1024)) MiB（$bc 个活动后端）"

say
say "=== 2. 打开 $N 条常驻连接（每条一个 backend 进程） ==="
pids=()
i=0
while [ $i -lt $N ]; do
  psql -c 'select pg_sleep(600)' >/dev/null 2>&1 &
  pids+=($!)
  i=$((i + 1))
  # 每 200 条稍歇一下：别把 fork 风暴本身算进测量误差
  [ $((i % 200)) -eq 0 ] && sleep 0.2
done

# 等 backend 数到位（到点就停，别死等）
deadline=$((SECONDS + 180))
while :; do
  ac=$(psql -Atc "select count(*) from pg_stat_activity" 2>/dev/null || echo 0)
  [ "${ac:-0}" -ge "$((N + bc))" ] && break
  [ $SECONDS -gt $deadline ] && { say "只等到 $ac 个后端（目标 $((N + bc))）"; break; }
  sleep 1
done
after=$(children_rss_kb "$pm")
got=$((ac - bc))
say "实际建立的连接 = $got"

say
say "=== 3. 每条连接的进程内存代价 ==="
if [ "$got" -gt 0 ]; then
  delta=$((after - before))
  per=$((delta / got))
  say "子进程 RSS: $((before / 1024)) MiB -> $((after / 1024)) MiB，增量 $((delta / 1024)) MiB"
  say "RSS/conn  = ${per} KiB"
  say "外推 10 000 条连接 ≈ $((per * 10000 / 1024)) MiB（约 $((per * 10000 / 1024 / 1024)) GiB）进程内存"
fi
# 内核侧 socket 内存两边都要付，列在这里避免把 PG 说得比实际更差。
# 行格式：`TCP: inuse 25 orphan 0 tw 8 alloc 55 mem 4` → $10 是标签，$11 是页。
awk '/^TCP:/ {print "TCP sockmem（内核侧，两边都有）: " $10 " = " $11 " 页"}' /proc/net/sockstat

say
say "=== 4. 收尾：停私有集群 ==="
for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done
wait 2>/dev/null
"$BIN/pg_ctl" -D "$PGDATA" -m fast -w stop >/dev/null 2>&1
say "done"
