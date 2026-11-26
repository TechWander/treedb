#!/bin/bash
# 记录层（§4.5 的 SecureBegin/SecureOk 那个阶段）要付多少。分两笔量，因为它们
# 是两回事，且对照臂不同：
#
#   [1] **接入面**（对照 handshake_cost.sh 的规格，三臂）：off / auth / sec。
#       - off  ：无密钥，v0.7 之前的形状；
#       - auth ：有密钥但 --no-secure（认证握手照跑，线保持明文）；
#       - sec  ：有密钥（默认路径：握手 + 记录层）。
#       auth-off = 认证的代价（那次已入册），sec-auth = 记录层的增量。三臂按
#       轮次交替跑（off/auth/sec × 2），单轮读数不进结论 —— 本机同臂两轮就能
#       摆出 5 %，而 auth 臂稳态比 off 臂「快 4.5 %」正是只有单轮时才会写进
#       报告的那种噪声（DESIGN.md §4.4 的老教训照抄一遍）。
#
#   [2] **副本线冷启动**（两臂交替三轮）：同一条 SnapFetch 路径，
#       `uplink=plaintext:forced` 与 `uplink=encrypted`。第一版脚本各跑一次，
#       加密臂反而比明文臂「快一倍」—— 装快照那一轮（≈1.1 s 的固定成本）在
#       2.6–5.2 s 的总墙钟里占一半，一次读数量的根本不是传输。交替三轮取中位，
#       并且把「扣掉最坏一轮之后的传输段速率」单独列出来，固定成本就不混进去了。
#       两臂的 standby 日志都必须自报出预期的 uplink 形状 —— 否则那一臂按无效
#       处理，量出来的「加密很便宜」可能是两臂根本没差。
#
# 每字节的分辨率（封/封+解 vs 同尺寸拷贝）不在这里：那是
# `cargo test --release --test record -- --ignored --nocapture` 的活，端到端
# 口径混着 TCP 与 group commit，差值落进轮间抖动就什么都结论不了。
#
# 用法：benches/secure_cost.sh [连接数=2000]
# 前置：`cargo build --release` 已跑过。
# 结果追加到 benches/results/secure_cost.log。
set -u
cd "$(dirname "$0")/.." || exit 1
N=${1:-2000}
ROUNDS1=${ROUNDS1:-2}
ROUNDS2=${ROUNDS2:-3}
PORT=${PORT:-17871}
P1=${P1:-17873}   # [2] 主
P2=${P2:-17874}   # [2] 副本
SETTLE=${SETTLE:-2}
SEED_SECS=${SEED_SECS:-8}
ROOT=$(pwd)
SRV=$ROOT/target/release/treedb-server
STBY=$ROOT/target/release/treedb-standby
BENCH=$ROOT/target/release/treedb-bench
OUT=benches/results/secure_cost.log
for f in "$SRV" "$STBY" "$BENCH"; do
    [ -x "$f" ] || { echo "先跑 cargo build --release（缺 $f）"; exit 1; }
done
D=$(mktemp -d /tmp/treedb-sec.XXXXXX)
W="$D/rep"
mkdir -p benches/results "$W"
SPID=""
LOAD=""
REP=""
trap 'kill $SPID $LOAD $REP 2>/dev/null; rm -rf "$D"' EXIT
REV=$(git rev-parse --short HEAD)$(git diff --quiet 2>/dev/null || echo -dirty)
log() { printf '%s\n' "$*" | tee -a "$OUT"; }
rss_kib() { awk '/VmRSS/{print $2}' /proc/"$1"/status; }

# 32 原始字节：`Key::load` 认的第一种形状。
python3 -c "import os;open('$D/k.raw','wb').write(os.urandom(32))" || exit 1
K=$D/k.raw

wait_port() {
    local p=$1 i=0
    until (exec 3<>/dev/tcp/127.0.0.1/"$p") 2>/dev/null; do
        i=$((i+1)); [ $i -gt 200 ] && return 1
        sleep 0.05
    done
}

parse() {
    sed -E 's/^op=([a-z]+).* → ([0-9]+) ops\/s.*p50=([0-9]+).*p99=([0-9]+).*/\1 \2 ops\/s p50=\3us p99=\4us/'
}

# ---------------------------------------------------------------- [1] 接入面
arm() { # $1=标签 $2=服务器 extra $3=bench extra；读数追加进 $D/rows
    local tag=$1 srv_extra=$2 bench_extra=$3
    local d="$D/$tag-$ROUND"; mkdir -p "$d"
    # shellcheck disable=SC2086
    "$SRV" --dir "$d" --listen "127.0.0.1:$PORT" --durability group \
        --auto-checkpoint-mb 0 $srv_extra >"$d/srv.log" 2>&1 &
    SPID=$!
    wait_port "$PORT" || { echo "$tag 服务端起不来：$(cat "$d/srv.log")"; exit 1; }

    local r0 conn_ms opened r1 held_ms t0 t1
    r0=$(rss_kib $SPID)
    t0=$(date +%s%N)
    # shellcheck disable=SC2086
    "$BENCH" --addr "127.0.0.1:$PORT" --hold --conns "$N" --secs 30 \
        $bench_extra >"$d/hold.log" 2>&1 &
    local bp=$!
    until grep -q '^connected ' "$d/hold.log" 2>/dev/null; do
        kill -0 $bp 2>/dev/null || { echo "$tag 建连失败：$(cat "$d/hold.log")"; exit 1; }
        sleep 0.02
    done
    r1=$(rss_kib $SPID)
    opened=$(sed -nE 's/^connected ([0-9]+) conns in ([0-9]+) ms$/\1/p' "$d/hold.log")
    conn_ms=$(sed -nE 's/^connected ([0-9]+) conns in ([0-9]+) ms$/\2/p' "$d/hold.log")
    until grep -q '^held ' "$d/hold.log" 2>/dev/null; do
        kill -0 $bp 2>/dev/null || { echo "$tag 没打完存活确认：$(cat "$d/hold.log")"; exit 1; }
        sleep 0.02
    done
    t1=$(date +%s%N)
    held_ms=$(( (t1 - t0) / 1000000 ))
    [ "$opened" = "$N" ] || log "  ⚠ r$ROUND/$tag 只建上 $opened/$N 条，本轮不可用"
    kill $bp 2>/dev/null; wait $bp 2>/dev/null
    sleep 0.5

    # shellcheck disable=SC2086
    local ping write
    ping=$("$BENCH" --addr "127.0.0.1:$PORT" --op ping --conns 64 --threads 64 \
                --pipeline 1 --secs 4 $bench_extra | tail -1 | parse)
    write=$("$BENCH" --addr "127.0.0.1:$PORT" --op create --conns 32 --threads 32 \
                 --pipeline 1 --views 32 --secs 4 $bench_extra | tail -1 | parse)

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "r$ROUND" "$tag" "$r0" "$r1" "$conn_ms" "$opened" "$held_ms" "$ping" "$write" \
        >> "$D/rows"
    kill $SPID 2>/dev/null; wait $SPID 2>/dev/null; SPID=""
    sleep "$SETTLE"
}

log ""
log "=============== $(date '+%F %T')  记录层代价（rev $REV，§4.5） ==============="
if [ "${SKIP1:-0}" = "1" ]; then
    # 重跑只补 [2] 时用：[1] 的三臂两轮已经躺在上一段 log 里，再跑只是重复噪声。
    log "[1] 接入面：本轮跳过（SKIP1=1）"
else
    log "[1] 接入面：$N 条建连 + 稳态两问，三臂交替 $ROUNDS1 轮"
    ROUND=1
    while [ $ROUND -le $ROUNDS1 ]; do
        arm off  ""                   ""
        arm auth "--auth-key-file $K" "--auth-key-file $K --no-secure"
        arm sec  "--auth-key-file $K" "--auth-key-file $K"
        ROUND=$((ROUND + 1))
    done
fi

# 每臂一行：建连 ms 各轮原值、RSS、稳态两问各轮原值。不用一条 awk 打完：
# awk 的关联数组遍历顺序不稳定，报告里的臂序必须固定。
if [ -s "$D/rows" ]; then
for tag in off auth sec; do
    conn=$(awk -F'\t' -v t="$tag" '$2==t{printf "%s%s", sep, $5; sep=" / "} END{print ""}' "$D/rows")
    rss=$(awk -F'\t' -v t="$tag" -v n="$N" '$2==t{printf "%s%s", sep, ($4-$3)*1024/n" B/conn"; sep=" / "} END{print ""}' "$D/rows")
    ping=$(awk -F'\t' -v t="$tag" '$2==t{printf "%s%s", sep, $8; sep=" / "} END{print ""}' "$D/rows")
    write=$(awk -F'\t' -v t="$tag" '$2==t{printf "%s%s", sep, $9; sep=" / "} END{print ""}' "$D/rows")
    log "  [$tag]  建连 $conn ms    RSS $rss"
    log "         稳态 ping: $ping"
    log "         稳态 create: $write"
done
fi
log "  （每格是 $ROUNDS1 轮原值；sec−auth = 记录层增量，auth−off = 已入册的认证代价，"
log "    都要跨轮看区间 —— handshake_cost 那张表里 off 臂三轮自己就摆过 27 %）"

# ---------------------------------------------------------------- [2] 副本线
# ROUNDS2 = 每臂轮数；plain 先手交替，且每臂**第一轮只当热身**（副本进程首启 +
# 页缓存冷的那一轮能把传输段压到热身值的一半，第一版脚本就是被它骗了）。
log "[2] 副本线冷启动：明文认证臂 vs 加密臂，交替 $ROUNDS2 轮/臂（首轮热身不进中位；种子 ${SEED_SECS}s）"
# shellcheck disable=SC2086
"$SRV" --dir "$W/primary" --listen "127.0.0.1:$P1" --workers 4 --durability group \
    --auto-checkpoint-mb 1 --auth-key-file "$K" > "$W/primary.log" 2>&1 &
SPID=$!
wait_port "$P1" || { echo "主起不来：$(cat "$W/primary.log")"; exit 1; }
"$BENCH" --addr "127.0.0.1:$P1" --op create --conns 32 --threads 32 --pipeline 32 \
    --secs "$SEED_SECS" --auth-key-file "$K" > "$W/seed.log" 2>&1
s_line() { "$SRV" --listen 127.0.0.1:"$1" --stats --auth-key-file "$K" 2>/dev/null | awk -v k="$2" '$1==k'; }
s_num() { s_line "$1" "$2" | awk -v n="$3" '{print $n; exit}'; }
SEQ=$(s_num "$P1" seq 3)
SNAPB=$(s_num "$P1" snapshot 3)
WALB=$(s_num "$P1" wal 3)
BYTES=$((SNAPB + WALB))
log "  主      seq=$SEQ snap=$SNAPB B wal=$WALB B（下载量 $BYTES B）"

cold_arm() { # $1=标签 $2=standby extra $3=预期 uplink
    local tag=$1 stby_extra=$2 want=$3
    rm -rf "$W/replica"; mkdir -p "$W/replica"
    # shellcheck disable=SC2086
    "$STBY" --primary 127.0.0.1:"$P1" --dir "$W/replica" --listen "127.0.0.1:$P2" \
        --workers 4 --interval-ms 20 --fetch-bytes 4194304 \
        --auth-key-file "$K" $stby_extra > "$W/replica.log" 2>&1 &
    REP=$!
    wait_port "$P2" || { echo "$tag 副本起不来：$(cat "$W/replica.log")"; exit 1; }
    # 反空跑：这一臂必须自报出预期的 uplink 形状，否则量的是别的形状。
    if ! grep -q "uplink=$want" "$W/replica.log"; then
        log "  ⚠ $tag 臂无效：standby 日志里没有 uplink=$want（$(grep -o 'uplink=[a-z:]*' "$W/replica.log" | head -1)）"
        kill $REP 2>/dev/null; wait $REP 2>/dev/null; REP=""
        return 1
    fi
    local t0 n=0 l lag pseq
    t0=$(date +%s%N)
    while :; do
        l=$(s_line "$P2" replica)
        lag=$(printf '%s' "$l" | awk '{print $8}')
        pseq=$(printf '%s' "$l" | awk '{print $6}')
        if [ "$lag" = "0" ] && [ "$pseq" = "$SEQ" ]; then
            n=$((n+1)); [ $n -ge 3 ] && break
        else
            n=0
        fi
        if [ $(( ($(date +%s%N) - t0) / 1000000 )) -gt 120000 ]; then
            log "  ⚠ $tag 120 s 未追平（lag=${lag:-?}）"; break
        fi
        sleep 0.05
    done
    local ms=$(( ($(date +%s%N) - t0) / 1000000 ))
    kill -TERM "$REP" 2>/dev/null; wait "$REP" 2>/dev/null; REP=""
    # 停服摘要里自报的「最坏一轮」（装快照落在里面）：传输段 ≈ 墙钟 − 最坏轮。
    local sum worst xfer rate
    sum=$(sed -n '$p' "$W/replica.log")
    worst=$(printf '%s' "$sum" | sed -n 's/.*worst \([0-9]*\) us.*/\1/p')
    xfer=$(awk -v b="$BYTES" -v m="$ms" -v w="${worst:-0}" \
        'BEGIN{ t=(m - w/1000.0)/1000.0; if (t>0) printf "%.1f", b/1048576/t; else print "n/a" }')
    rate=$(awk -v b="$BYTES" -v m="$ms" 'BEGIN{ if (m>0) printf "%.1f", b/1048576/(m/1000.0); else print "n/a" }')
    log "  r$CROUND $tag  追平 $ms ms ≈ ${rate} MiB/s；扣最坏轮（${worst:-?} µs）≈ ${xfer} MiB/s"
    printf '%s\t%s\t%s\t%s\t%s\n' "$tag" "$ms" "${worst:-0}" "$rate" "$xfer" >> "$D/cold_rows"
    sleep 1
}

CROUND=1
rm -f "$D/cold_rows"
while [ $CROUND -le $(( ROUNDS2 * 2 )) ]; do
    if [ $((CROUND % 2)) = 1 ]; then
        cold_arm plain "--no-secure" "plaintext:forced"
    else
        cold_arm sec "" "encrypted"
    fi
    CROUND=$((CROUND + 1))
done

# 每臂取中位（丢掉首轮热身，取剩下的偶数个中位）。交接棒顺序造成的系统性
# 偏置只有交替能摊平；首启 outlier 不能靠它，只能丢。
median() { sort -n "$1" | awk '{a[NR]=$1} END{ if (NR==0) print 0; else if (NR%2) print a[(NR+1)/2]; else print int((a[NR/2]+a[NR/2+1])/2) }'; }
if [ -s "$D/cold_rows" ]; then
    for arm_tag in plain sec; do
        awk -F'\t' -v t="$arm_tag" 'NR>1 && $1==t{printf "%s%s", sep, $2; sep=" / "} END{print ""}' "$D/cold_rows" | sed 's/^/    /' | while IFS= read -r l; do log "  $arm_tag 墙钟各轮（去热身）：$l"; done
    done
    MP=$(awk -F'\t' 'NR>1 && $1=="plain"{print $2}' "$D/cold_rows")
    MS=$(awk -F'\t' 'NR>1 && $1=="sec"{print $2}' "$D/cold_rows")
    XP=$(awk -F'\t' 'NR>1 && $1=="plain"{print $5}' "$D/cold_rows" | grep -v n/a)
    XS=$(awk -F'\t' 'NR>1 && $1=="sec"{print $5}' "$D/cold_rows" | grep -v n/a)
    MPm=$(printf '%s\n' $MP | median /dev/stdin)
    MSm=$(printf '%s\n' $MS | median /dev/stdin)
    XPm=$(printf '%s\n' $XP | median /dev/stdin)
    XSm=$(printf '%s\n' $XS | median /dev/stdin)
    log "  中位    明文臂墙钟 ${MPm:-n/a} ms / 传输段 ${XPm:-n/a} MiB/s；加密臂墙钟 ${MSm:-n/a} ms / 传输段 ${XSm:-n/a} MiB/s"
    if [ "${MPm:-0}" -gt 0 ] && [ "${MSm:-0}" -gt 0 ]; then
        log "  结论    加密臂墙钟是明文臂的 $(awk -v a="$MSm" -v b="$MPm" 'BEGIN{printf "%.2f", a/b}')×；"
        log "          判「加密贵不贵」看传输段（装快照那轮的固定成本两边同付），别看墙钟"
    fi
fi

kill $SPID 2>/dev/null; wait $SPID 2>/dev/null; SPID=""
log "读法：[1] 里 sec−auth 是记录层的增量（一次往返 + 每帧 seal）；auth−off 是已入册的认证代价。"
log "     [2] 的两臂共享同一把密钥、同一条 SnapFetch 路径，唯一差别是记录层。"
log "     「可接受」的判据在 DESIGN.md §4.5 末表，不在这份 log。"
