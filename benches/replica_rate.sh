#!/bin/bash
# treedb-standby 的传送吞吐与追平代价 —— DESIGN.md §6.6「边界」第 4 条那三格
# 的直接实测：冷启动下载速率、稳态追平速率（含 lag 分布）、追平期间副本的读延迟。
#
# 为什么单独一个脚本：§6.6 原来只有算术上界（20 ms 限频 × 1 MiB/轮 ≈ 50 MiB/s），
# 没有一次真跑；而「副本能不能跟上真实写入」这件事恰恰只能量，不能推。三格分别：
#
# [1] 冷启动：主已有数据 → 空目录副本从 0 追到 lag 0，量墙钟与下载字节。
#     两种 --fetch-bytes（1 MiB 默认 / 4 MiB 上限）各跑一次，看速率是否随块长走。
# [2] 稳态：主持续写、副本同时追；按采样算 applied 速率与 lag 的中位/最大。
# [3] 读侧代价：同机对副本打 ping，比较「静置」与「追平中」的 p50/p99。
#     这一格是**代理指标**：它量的是 tick 占住事件循环对外部读请求的可见代价，
#     不是 tick 本身的时长；同机 CPU 竞争也算在里面。
# [4] 副本退出摘要里自带 rounds / worst round / syncs / avg fsync —— 追平期间
#     最坏一轮循环占多久，那里有引擎自报的数。
#
# 用法：benches/replica_rate.sh [主上造数据秒数=3]
# 结果追加到 benches/results/replica_rate.log（原始输出同时留在 .spot/rrate/）。
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
SECS=${1:-3}
BIN=./target/release
OUT=benches/results/replica_rate.log
P1=${P1:-17831}
P2=${P2:-17832}
W=${W:-.spot/rrate}
REV=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
CHUNK=1048576
mkdir -p benches/results "$W"

SRV=""
REP=""
LOAD=""

log() { printf '%s\n' "$*" | tee -a "$OUT"; }

stop_all() {
    [ -n "$LOAD" ] && kill "$LOAD" 2>/dev/null
    if [ -n "$REP" ]; then
        kill -TERM "$REP" 2>/dev/null
        wait "$REP" 2>/dev/null
        REP=""
    fi
    [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
    wait 2>/dev/null
}
trap stop_all EXIT

wait_port() {
    local p=$1 i=0
    until (exec 3<>/dev/tcp/127.0.0.1/"$p") 2>/dev/null; do
        i=$((i + 1))
        if [ $i -gt 200 ]; then
            echo "port $p 起不来" >&2
            return 1
        fi
        sleep 0.05
    done
}

ms_now() { date +%s%N; }

# 读一行 stats（`--stats` 逐行 key = value）：s_line PORT KEYPREFIX
s_line() { $BIN/treedb-server --listen 127.0.0.1:"$1" --stats 2>/dev/null | awk -v k="$2" '$1==k'; }

# 取某行第 n 个字段。行形状见 src/bin/server.rs::print_stats：
#   seq      = 123 (snapshot 100)            → seq=$3, snapshot=$5
#   wal      = 1234 B logical, 5678 B on disk→ logical=$3, disk=$6
#   replica  = yes (primary seq N lag L applied A resyncs R errors E)
#                                            → primary=$6 lag=$8 applied=$10 resyncs=$12 errors=$14
s_num() { s_line "$1" "$2" | awk -v n="$3" '{print $n; exit}'; }

# 等主把「手头那次 checkpoint」做完：snapshot 水位连续两次采样不变才算稳。
settle() {
    local a b i=0
    a=$(s_num "$P1" seq 5)
    while [ $i -lt 40 ]; do
        sleep 0.15
        b=$(s_num "$P1" seq 5)
        [ "$a" = "$b" ] && return 0
        a=$b
        i=$((i + 1))
    done
    return 0
}

# 冷启动一次：空目录副本从 0 追到 lag 0。返回 R_MS / R_SECS / R_RATE。
cold_start() { # cold_start FETCH_BYTES LABEL
    local fb=$1 label=$2 t0 n l lag pseq
    rm -rf "$W/replica"
    mkdir -p "$W/replica"
    $BIN/treedb-standby --primary 127.0.0.1:"$P1" --dir "$W/replica" \
        --listen 127.0.0.1:"$P2" --workers 4 --interval-ms 20 --fetch-bytes "$fb" \
        > "$W/replica-$fb.log" 2>&1 &
    REP=$!
    wait_port "$P2" || return 1
    t0=$(ms_now)
    n=0
    while :; do
        l=$(s_line "$P2" replica)
        lag=$(printf '%s' "$l" | awk '{print $8}')
        pseq=$(printf '%s' "$l" | awk '{print $6}')
        if [ "$lag" = "0" ] && [ "$pseq" = "$SEQ" ]; then
            n=$((n + 1))
            [ $n -ge 3 ] && break
        else
            n=0
        fi
        sleep 0.05
        if [ $((($(ms_now) - t0) / 1000000)) -gt 60000 ]; then
            log "  副本   60 s 未追平（lag=${lag:-?}）"
            break
        fi
    done
    R_MS=$((($(ms_now) - t0) / 1000000))
    kill -TERM "$REP" 2>/dev/null
    wait "$REP" 2>/dev/null
    REP=""
    local worst
    worst=$(sed -n '$p' "$W/replica-$fb.log" | sed -n 's/.*worst \([0-9]*\) us.*/\1/p')
    R_WORST=${worst:-0}
    R_RATE=$(awk -v b="$BYTES" -v m="$R_MS" 'BEGIN{ if (m>0) printf "%.2f", b/1048576/(m/1000.0); else print "inf" }')
    # 「装快照」是一轮 apply + 写 state.snap，属于固定成本，与块长无关；把它从
    # 墙钟里减掉剩下的才是传输段的粗略读数（粗略：最坏一轮也可能含首批 WAL）。
    R_XFER=$(awk -v b="$BYTES" -v m="$R_MS" -v w="$R_WORST" 'BEGIN{ t=(m - w/1000.0)/1000.0; if (t>0) printf "%.2f", b/1048576/t; else print "n/a" }')
    log "  副本    $label（$fb B/轮）：端到端 $R_MS ms（其中装快照那一轮 ${R_WORST} µs）"
    log "          端到端 $R_RATE MiB/s；扣掉装快照 ≈ $R_XFER MiB/s"
}

# 副本的启停。每个阶段用独立进程：standby 只在退出时自报「最坏一轮」，
# 不分开就分不清那个最大值属于装快照、追积压还是稳态。
replica_start() {
    $BIN/treedb-standby --primary 127.0.0.1:"$P1" --dir "$W/replica" \
        --listen 127.0.0.1:"$P2" --workers 4 >> "$W/replica.log" 2>&1 &
    REP=$!
    wait_port "$P2" || return 1
    CKPT0=$(s_num "$P2" checkpoints= 2)
}

replica_stop() { # replica_stop 阶段名
    local w line c1
    c1=$(s_num "$P2" checkpoints= 2)
    kill -TERM "$REP" 2>/dev/null
    wait "$REP" 2>/dev/null
    REP=""
    line=$(sed -n '$p' "$W/replica.log")
    w=$(printf '%s' "$line" | sed -n 's/.*worst \([0-9]*\) us.*/\1/p')
    log "  最坏轮  $1：${w:-n/a} µs（引擎自报的完整一轮循环上限）"
    log "  落快照  checkpoints ${CKPT0:-?} → ${c1:-?}（相位内变没变；变了说明最坏轮里含快照）"
    log "  摘要    $(printf '%s' "$line" | sed -e 's/^shutdown: //')（摘要里的 checkpoints 1 是停服补的那一次，不在相位内）"
}

# 等副本追上指定的主水位（连续 3 个采样 lag 0）。
wait_lag0() { # wait_lag0 TARGET_SEQ LABEL
    local target=$1 label=$2 n=0 w=0 l
    while [ $w -lt 1200 ]; do
        l=$(s_line "$P2" replica)
        w=$((w + 1))
        if [ "$(printf '%s' "$l" | awk '{print $8}')" = "0" ] && [ "$(printf '%s' "$l" | awk '{print $6}')" = "$target" ]; then
            n=$((n + 1))
            [ $n -ge 3 ] && break
        else
            n=0
        fi
        sleep 0.05
    done
    log "  基线    $label：lag 0（seq $target）"
}

# ---------------------------------------------------------------- 开跑

log ""
log "=============== $(date '+%F %T')  treedb-standby 传送吞吐（rev $REV，§6.6 边界 4）"
log ""
log "[1] 冷启动：先在主上造数据，再让空目录副本从 0 追平"
rm -rf "$W"
mkdir -p "$W"
$BIN/treedb-server --dir "$W/primary" --listen 127.0.0.1:"$P1" --workers 4 \
    --auto-checkpoint-mb 1 > "$W/primary.log" 2>&1 &
SRV=$!
wait_port "$P1" || exit 1
SEED=$($BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
    --pipeline 32 --secs "$SECS" 2>&1 | head -1)
log "  主      $SEED"
settle
NODES=$(s_num "$P1" nodes 3)
SEQ=$(s_num "$P1" seq 3)
SNAP_SEQ=$(s_num "$P1" seq 5 | tr -d ')')
SNAPB=$(s_num "$P1" snapshot 3)
WALB=$(s_num "$P1" wal 3)
BYTES=$((SNAPB + WALB))
log "  主      nodes=$NODES seq=$SEQ snapshot=$SNAP_SEQ snap_bytes=$SNAPB wal_logical=$WALB"
log "  口径    下载字节 = 快照 + WAL 逻辑长度（不含逐块协议头）；快照块长 = --fetch-bytes"
cold_start $CHUNK "1 MiB/块（默认）"
MS1=$R_MS
cold_start 4194304 "4 MiB/块（WAL_FETCH_MAX）"
MS4=$R_MS
if [ "$MS1" -gt 0 ]; then
    log "  结论    端到端 4 MiB/块是 1 MiB/块的 $(awk -v a="$MS1" -v b="$MS4" 'BEGIN{printf "%.2f", a/b}')×（${MS1} → ${MS4} ms）；块长只影响传输段，端到端被装快照主导"
fi

# [2] 追赶：副本停着、主关掉自动 checkpoint 攒一段 WAL，再放副本追。
# 这一格不含装快照，量的就是「落后一段之后能不能靠传送追回来」。
log ""
log "[2] 追赶：副本停（目录停在 seq $SEQ），主攒 ${SECS}s 的 WAL（--auto-checkpoint-mb 0），再放副本追"
kill "$SRV" 2>/dev/null
wait "$SRV" 2>/dev/null
$BIN/treedb-server --dir "$W/primary" --listen 127.0.0.1:"$P1" --workers 4 \
    --auto-checkpoint-mb 0 > "$W/primary2.log" 2>&1 &
SRV=$!
wait_port "$P1" || exit 1
S0=$SEQ
W0=$(s_num "$P1" wal 3)
$BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
    --pipeline 32 --secs "$SECS" > "$W/load2.log" 2>&1
SEQ2=$(s_num "$P1" seq 3)
W2=$(s_num "$P1" wal 3)
BACKLOG=$((SEQ2 - S0))
BPR=$(awk -v w="$((W2 - W0))" -v r="$BACKLOG" 'BEGIN{ if (r>0) printf "%.1f", w/r; else print "0" }')
BBYTES=$(awk -v b="$BPR" -v r="$((SEQ2 - SEQ))" 'BEGIN{ printf "%.0f", b*r }')
log "  主      $(head -1 "$W/load2.log")"
log "  堆积    seq $SEQ → $SEQ2（+$BACKLOG 条，WAL 逻辑 +$((W2 - W0)) B，≈ $BPR B/条）"
replica_start || exit 1
T0=$(ms_now)
n=0
w=0
while [ $w -lt 1200 ]; do
    l=$(s_line "$P2" replica)
    w=$((w + 1))
    if [ "$(printf '%s' "$l" | awk '{print $8}')" = "0" ] && [ "$(printf '%s' "$l" | awk '{print $6}')" = "$SEQ2" ]; then
        n=$((n + 1))
        [ $n -ge 3 ] && break
    else
        n=0
    fi
    sleep 0.05
done
T1=$(ms_now)
DMS=$(((T1 - T0) / 1000000))
RECS=$((SEQ2 - SEQ))
DRATE=$(awk -v r="$RECS" -v m="$DMS" 'BEGIN{ if (m>0) printf "%.0f", r*1000.0/m; else print "inf" }')
MBRATE=$(awk -v b="$BBYTES" -v m="$DMS" 'BEGIN{ if (m>0) printf "%.2f", b/1048576/(m/1000.0); else print "inf" }')
log "  副本    追 $RECS 条 / $DMS ms → $DRATE 条/s，≈ $MBRATE MiB/s（估算字节 $BBYTES，按 $BPR B/条）"
log "  口径    这一段是传送 + 副本 apply + 副本 fsync，不含装快照"
replica_stop "追赶阶段"
replica_start || exit 1
SEQ3=$(s_num "$P1" seq 3)
wait_lag0 "$SEQ3" "稳态起跑"

# [3] 稳态：主持续写、副本同时追（副本仍在跑）。
log ""
log "[3] 稳态追平：主持续写 ${SECS}s（create，64 连接，pipeline 32），副本同步采样"
$BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
    --pipeline 32 --secs "$SECS" > "$W/load3.log" 2>&1 &
LOAD=$!
A0=$(s_num "$P2" replica 10)
SA=$(s_num "$P1" seq 3)
T2=$(ms_now)
LAGS="$W/lags3.txt"
: > "$LAGS"
while kill -0 "$LOAD" 2>/dev/null; do
    s_line "$P2" replica | awk '{print $8}' >> "$LAGS"
    sleep 0.2
done
wait "$LOAD" 2>/dev/null
LOAD=""
A1=$(s_num "$P2" replica 10)
SB=$(s_num "$P1" seq 3)
T3=$(ms_now)
RECS2=$((A1 - A0))
DMS2=$(((T3 - T2) / 1000000))
RRATE=$(awk -v r="$RECS2" -v m="$DMS2" 'BEGIN{ if (m>0) printf "%.0f", r*1000.0/m; else print "inf" }')
PRATE=$(awk -v r="$((SB - SA))" -v m="$DMS2" 'BEGIN{ if (m>0) printf "%.0f", r*1000.0/m; else print "inf" }')
SORTED=$(sort -n "$LAGS" | grep -E '^[0-9]+$' || true)
MED=$(printf '%s\n' "$SORTED" | awk '{a[NR]=$1} END{ if (NR) print a[int((NR+1)/2)]; else print "n/a" }')
MAX=$(printf '%s\n' "$SORTED" | tail -1)
SAMPLES=$(printf '%s\n' "$SORTED" | grep -c . || true)
log "  主      $(head -1 "$W/load3.log")"
log "  产出    主 seq +$((SB - SA)) 条 / ${DMS2} ms → $PRATE 条/s（副本同期 applied $RRATE 条/s）"
log "  副本    applied +$RECS2 条；lag 中位 ${MED:-n/a}、最大 ${MAX:-n/a}（$SAMPLES 个采样，200 ms/个）"
T4=$(ms_now)
n=0
w=0
while [ $w -lt 600 ]; do
    l=$(s_line "$P2" replica)
    w=$((w + 1))
    if [ "$(printf '%s' "$l" | awk '{print $8}')" = "0" ]; then
        n=$((n + 1))
        [ $n -ge 3 ] && break
    else
        n=0
    fi
    sleep 0.05
done
log "  收尾    负载停后 $(((($(ms_now) - T4) / 1000000))) ms 追平（lag 0）"
replica_stop "稳态阶段"
replica_start || exit 1
SEQ4=$(s_num "$P1" seq 3)
wait_lag0 "$SEQ4" "读侧压测起跑"

log ""
log "[4] 追平期间的读侧代价（代理指标：同机对副本打 ping，8 连接 pipeline 1）"
IDLE=$(timeout $((SECS + 15)) $BIN/treedb-bench --addr 127.0.0.1:"$P2" --op ping \
    --conns 8 --threads 8 --pipeline 1 --views 0 --secs "$SECS" 2>&1 | head -1)
log "  静置    $IDLE"
$BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
    --pipeline 32 --secs $((SECS + 2)) > "$W/load4.log" 2>&1 &
LOAD=$!
sleep 0.6
$BIN/treedb-bench --addr 127.0.0.1:"$P2" --op ping --conns 8 --threads 8 \
    --pipeline 1 --views 0 --secs "$SECS" > "$W/chase.log" 2>&1 &
CHASE=$!
LAGS3="$W/lags4.txt"
: > "$LAGS3"
while kill -0 "$CHASE" 2>/dev/null; do
    s_line "$P2" replica | awk '{print $8}' >> "$LAGS3"
    sleep 0.1
done
wait "$CHASE" 2>/dev/null
CHASE_LINE=$(head -1 "$W/chase.log")
wait "$LOAD" 2>/dev/null
LOAD=""
S3=$(sort -n "$LAGS3" | grep -E '^[0-9]+$' || true)
MED3=$(printf '%s\n' "$S3" | awk '{a[NR]=$1} END{ if (NR) print a[int((NR+1)/2)]; else print "n/a" }')
log "  追平中  $CHASE_LINE（同期 lag 中位 ${MED3:-n/a}）"
P50I=$(printf '%s' "$IDLE" | sed -n 's/.*p50=\([0-9]*\).*/\1/p')
P50C=$(printf '%s' "$CHASE_LINE" | sed -n 's/.*p50=\([0-9]*\).*/\1/p')
P99I=$(printf '%s' "$IDLE" | sed -n 's/.*p99=\([0-9]*\).*/\1/p')
P99C=$(printf '%s' "$CHASE_LINE" | sed -n 's/.*p99=\([0-9]*\).*/\1/p')
if [ -n "${P50I:-}" ] && [ -n "${P50C:-}" ] && [ "$P50I" -gt 0 ] 2>/dev/null; then
    log "  结论    p50 $(awk -v a="$P50I" -v b="$P50C" 'BEGIN{printf "%+d%%", (b-a)*100/a}')（$P50I → $P50C µs）、p99 $(awk -v a="$P99I" -v b="$P99C" 'BEGIN{printf "%+d%%", (b-a)*100/a}')（$P99I → $P99C µs）；同机 CPU 竞争计入"
fi
log "  注意    这是「tick 占住循环」的对外可见代价，不是 tick 时长本身；下一行有引擎自报的最坏一轮"
T5=$(ms_now)
n=0
w=0
while [ $w -lt 600 ]; do
    l=$(s_line "$P2" replica)
    w=$((w + 1))
    if [ "$(printf '%s' "$l" | awk '{print $8}')" = "0" ]; then
        n=$((n + 1))
        [ $n -ge 3 ] && break
    else
        n=0
    fi
    sleep 0.05
done
log "  收尾    压测停后 $(((($(ms_now) - T5) / 1000000))) ms 追平（lag 0）"
replica_stop "读侧压测阶段"

log ""
log "说明：所有格子都在同一台机器、同一块盘上跑（主与副本共用 CPU 与 IO），"
log "     数字回答的是「本机形状下能不能跟上」，不是跨机网络的基准。"

stop_all
