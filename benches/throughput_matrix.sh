#!/bin/bash
# 吞吐轴验收：同一个 op 在五条轴上各量一遍，把"引擎上限"和"磁盘上限"分开。
#
# 为什么必须分轴：本机 /data 是一块 160 TB **机械盘** XFS 阵列，裸 fsync 平均
# 3.8 ms、尖峰 53 ms；根盘 ext4 只要 236 µs。任何把 WAL 放 /data 又开 fsync 的
# 格子，数字都是被盘决定的，不是被引擎决定的。混在一张表里报就是骗人。
#
#   轴 1  dur=never  → 事件循环 + 内存状态的纯上限（不含任何持久化）
#   轴 2  dur=group  → 默认档：每轮一次 fsync（WAL 落 /data，真实部署位置）
#   轴 3  dur=always → 每 op 一次 fsync（POSIX O_DSYNC 语义的代价）
#   轴 4  PG         → 同样的 op 打到私有 PG 12 集群
#   轴 5  读并行     → 同一格扫 --workers，看前端 worker 能把读吞吐推到哪
#
# 用法：
#   benches/throughput_matrix.sh [每格秒数=3] [轴…：1 2 3 4 5，默认 1 2 3 4]
# 结果追加到 benches/results/matrix.log（带表头）。
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
SECS=${1:-3}
AXES="${*:2}"
AXES=${AXES:-1 2 3 4}
BIN=./target/release
OUT=benches/results/matrix.log
mkdir -p benches/results

# /data = 真实部署位置（慢机械盘）；轴 1 也放这里，这样"上限差"只由 durability
# 档解释，不由换盘解释。
SLOW_DIR=/data/grovefs/treedb/.matrix
FAST_DIR=/tmp/treedb-matrix
PORT=${PORT:-17791}
SRV=""

say() { printf '%s\n' "$*"; }

stop_srv() {
  [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
  wait "$SRV" 2>/dev/null
  SRV=""
}
trap 'stop_srv' EXIT

# cell DUR DIR OP CONNS PIPE [WORKERS=1]
cell() {
  local dur=$1 dir=$2 op=$3 conns=$4 pipe=$5 workers=${6:-1}
  stop_srv
  rm -rf "$dir"; mkdir -p "$dir"
  $BIN/treedb-server --dir "$dir" --listen 127.0.0.1:$PORT \
      --durability "$dur" --auto-checkpoint-mb 0 --workers "$workers" > "$dir/srv.log" 2>&1 &
  SRV=$!
  # 等监听就绪：用 /dev/tcp 探端口，不用真跑一轮 bench 当探针（那要白付一秒）。
  local i=0
  until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
    i=$((i + 1)); [ $i -gt 100 ] && { say "cell $dur/$op 服务端起不来：$(cat "$dir/srv.log")"; return 1; }
    sleep 0.05
  done
  $BIN/treedb-bench --addr 127.0.0.1:$PORT --op "$op" \
      --conns "$conns" --threads "$conns" --pipeline "$pipe" --secs "$SECS" \
      2>"$dir/bench.err" | sed "s|^|$dur wal=$(basename "$dir") workers=$workers |" | tee -a "$OUT"
  # bench 的 stderr 不能吞：服务端挂掉时它只会往 stderr 报 round failed，
  # 光看 stdout 那一行数字察觉不到这一格是废的。
  [ -s "$dir/bench.err" ] && sed 's|^|  ! |' "$dir/bench.err" | tail -3 | tee -a "$OUT"
}

matrix() { # $1=dur $2=dir  ops…
  local dur=$1 dir=$2; shift 2
  for op in "$@"; do
    for conns in 1 8 64; do
      for pipe in 1 32; do
        cell "$dur" "$dir" "$op" "$conns" "$pipe"
      done
    done
  done
}

say "=== 机器/存储基线 ===" | tee -a "$OUT"
grep -E '^(model name|mem total)' /proc/cpuinfo 2>/dev/null | head -1 | tee -a "$OUT"
awk '/^MemTotal:/{print "mem "$2/1048576" GiB"}' /proc/meminfo | tee -a "$OUT"
nproc | awk '{print "nproc "$1}' | tee -a "$OUT"
df -hT /data / 2>/dev/null | tail -2 | tee -a "$OUT"
lsblk -d -o NAME,ROTA,SIZE,MOUNTPOINT 2>/dev/null | grep -v loop | tee -a "$OUT"

for ax in $AXES; do
  case $ax in
  1)
    say; say "=== 轴 1  dur=never（无持久化，事件循环上限） dir=$(basename "$SLOW_DIR") ===" | tee -a "$OUT"
    matrix never "$SLOW_DIR" ping stat lookup readdir create
    ;;
  2)
    say; say "=== 轴 2  dur=group（默认档，WAL 在 /data 机械盘）===" | tee -a "$OUT"
    for op in create write lock key lookup readdir; do
      for conns in 1 8 64; do
        for pipe in 1 32; do
          cell group "$SLOW_DIR" "$op" "$conns" "$pipe"
        done
      done
    done
    ;;
  3)
    say; say "=== 轴 3  dur=always（每 op 一次 fsync）===" | tee -a "$OUT"
    for op in create write; do
      for conns in 1 8 64; do
        cell always "$SLOW_DIR" "$op" "$conns" 1
      done
    done
    say "--- 同档把 WAL 挑到根盘（fsync 236 us），只回答'一次 fsync 多贵' ---" | tee -a "$OUT"
    for op in create write; do
      for conns in 8 64; do
        cell always "$FAST_DIR" "$op" "$conns" 1
      done
    done
    ;;
  4)
    say; say "=== 轴 4  PG 12 私有集群（同 op 语义，synchronous_commit=on）===" | tee -a "$OUT"
    PGBIN=/usr/lib/postgresql/12/bin
    PGDATA="$ROOT/.pgref"
    PGPORT=54390
    # 绝对不要给 postgres 传 `-F`：那是“turn fsync off”，不是 initdb 的
    # --no-sync。本脚本早期就踩过这个坑，PG 写入档量出了 14.7 M ops/s
    # 的鬼数字（一块 fsync 要 4.6 ms 的机械盘不可能）。
    PGO="-p $PGPORT -c listen_addresses=127.0.0.1 -c unix_socket_directories=$PGDATA -c max_connections=4050 -c fsync=on -c synchronous_commit=on"
    if [ ! -f "$PGDATA/PG_VERSION" ]; then
      say "initdb -> $PGDATA"
      mkdir -p "$PGDATA"
      "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth=trust -E UTF8 >/dev/null || exit 1
    fi
    "$PGBIN/pg_ctl" -D "$PGDATA" -o "$PGO" -w start >/dev/null 2>&1 || \
      pg_isready -q -h 127.0.0.1 -p $PGPORT || { say "PG 起不来"; exit 1; }
    export PGHOST=127.0.0.1 PGPORT=$PGPORT PGUSER=postgres PGDATABASE=postgres
    say "pg $(psql -Atc 'select version()' 2>/dev/null | head -c 40)  max_connections=$(psql -Atc 'show max_connections' 2>/dev/null)"
    # 持久化闸门：这两项只要有一个不是 on，下面所有写入档数字全部作废，
    # 跟 treedb（真 fsync）也不是同一个可比条件。宁可 here退出也不报假数据。
    FSYNC=$(psql -Atc 'show fsync')
    SYNCC=$(psql -Atc 'show synchronous_commit')
    say "pg fsync=$FSYNC synchronous_commit=$SYNCC"
    if [ "$FSYNC" != on ] || [ "$SYNCC" != on ]; then
      say "!! PG 持久化没开（fsync=$FSYNC synchronous_commit=$SYNCC），轴 4 作废"
      exit 1
    fi
    say "fsync 基线（PG 也吃这块盘）：" | tee -a "$OUT"
    [ -x /tmp/fsync_probe ] && /tmp/fsync_probe "$PGDATA/probe.f" 200 | tee -a "$OUT"
    for op in ping lookup readdir create write lock key; do
      for conns in 1 8 64; do
        for pipe in 1 32; do
          $BIN/treedb-pgbench --uri "postgresql://postgres@127.0.0.1:$PGPORT/postgres" \
              --op "$op" --conns "$conns" --pipeline "$pipe" --secs "$SECS" \
              2>"/tmp/pgbench.err" | tee -a "$OUT"
          [ -s /tmp/pgbench.err ] && sed 's|^|  ! |' /tmp/pgbench.err | tail -3 | tee -a "$OUT"
        done
      done
    done
    "$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop >/dev/null 2>&1
    ;;
  5)
    # 读轴：与 §7.5 同口径（dur=group、WAL 在 /data、conns=64、bench 默认 8 视图
    # ×500 文件预热），不然 workers=1 那一列与历史数字对不上。
    say; say "=== 轴 5  读并行：扫 --workers（dur=group，WAL 在 /data）===" | tee -a "$OUT"
    for op in lookup readdir; do
      for pipe in 1 32; do
        for w in 1 2 4 8 16; do
          cell group "$SLOW_DIR" "$op" 64 "$pipe" "$w"
        done
      done
    done
    # ping 量的是前端循环本身（不碰 WAL），用 never 档免得周期 fsync 搅局。
    say "--- ping（never 档，纯事件循环）---" | tee -a "$OUT"
    for pipe in 1 32; do
      for w in 1 2 4 8 16; do
        cell never "$SLOW_DIR" ping 64 "$pipe" "$w"
      done
    done
    # 写档回退检查：读并行是拿“写要多走一次线程间递交”换来的，下面这些格子
    # 就是这笔代价的账单。workers>1 下写慢一点可以接受，但必须量出来写进 §7.5，
    # 不能只报读轴好看的那一半。
    say "--- 写档：同一格扫 workers，量 IPC 递交的代价 ---" | tee -a "$OUT"
    for op in create write; do
      for pipe in 1 32; do
        for w in 1 8 16; do
          cell group "$SLOW_DIR" "$op" 64 "$pipe" "$w"
        done
      done
    done
    ;;
  esac
done

say; say "结果已写入 $OUT"
