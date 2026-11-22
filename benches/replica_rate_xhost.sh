#!/bin/bash
# 副本传送的跨机实测 —— DESIGN.md §6.6「边界」第 5 条（同机三格是同节第 4 条）。
#
# 同机的 benches/replica_rate.sh 回答「本机形状下能不能跟上」，但回答不了两件事：
#   ① 真实链路（不是 loopback）上的冷启动与追赶速率；
#   ② 副本在追时对**主** p99 的影响 —— 同机上 CPU 竞争与复制开销混在一起，分不开。
# 本脚本把副本放到第二台机（默认 gfs2，virtio 1 Gb）上量这两件事。
#
# 这一格历史上测不出来，因为多 worker 的 `src/net/multi.rs` 把「只有 EPOLLOUT、
# 没有可读字节」的事件在 `ops` 为空时直接 continue 掉了：大回包剩下的字节永远
# 留在用户态，电平触发的 EPOLLOUT 还会让线程空转。loopback 上整帧一次就能塞进
# 内核缓冲区，从没走到那条路径；跨机 + 大回包 + 多 worker 同时成立才暴露
# （1 MiB 块只到 ~110 KiB 就停住，一直读到 --timeout-ms 超时）。修复见同笔提交
# 的 flush_on_write，回归用例 tests/large_reply.rs 不依赖网络形状。
#
# 三格：
# [1] 跨机冷启动：主已有数据 → 远端空目录副本从 0 追平，两种 --fetch-bytes。
# [2] 跨机追赶：副本停着、主关掉自动 checkpoint 攒一段 WAL，再放副本追。
# [3] 副本对主 p99 的影响：同一份种子目录派生出的两个独立目录上跑**完全相同**
#     的写入，唯一差别是 B 有一个正在追积压的副本、A 没有。负载发生器始终在
#     宿主本地，两边与主的网络路径一致，「主 p99」量的就是主自己。
#
# 用法：benches/replica_rate_xhost.sh [轮数=3] [造数据秒数=8]
# 结果追加到 benches/results/replica_rate_xhost.log；原始日志留在 .spot/xrate/。
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
RUNS=${1:-3}
SECS=${2:-8}
BIN=./target/release
OUT=${OUT:-benches/results/replica_rate_xhost.log}
W=${W:-.spot/xrate}
P1=${P1:-17901}
P2=${P2:-17902}
HOST_IP=${HOST_IP:-192.168.122.1}
PEER=${PEER:-gfs2}
RWORK=${RWORK:-/data/gfs1/rrate/work}
PRIME_SECS=${PRIME_SECS:-15}
MEAS_SECS=${MEAS_SECS:-6}
CHUNK=1048576
mkdir -p benches/results
rm -rf "$W"
mkdir -p "$W"

SRV=""
RS_PID=""
PA=""
PB=""

log() { printf '%s\n' "$*" | tee -a "$OUT"; }

srv_stop() {
    if [ -n "$SRV" ]; then
        kill -TERM "$SRV" 2>/dev/null
        wait "$SRV" 2>/dev/null
        SRV=""
    fi
}

# 远端命令一律走这条：-n 免得 ssh 抢 stdin，timeout 兜底防挂住。
# 不能用 `ssh ... | grep -v setlocale` 直接当返回值 —— 远端安静成功时 grep 无
# 匹配会返回 1，调用方会把「跑了但没输出」误判成失败。
rs() {
    local out st
    out=$(timeout 240 ssh -n -o BatchMode=yes "$PEER" "$RWORK/rs.sh $*" 2>&1)
    st=$?
    printf '%s\n' "$out" | grep -v setlocale
    return $st
}

stop_all() {
    rs stop >/dev/null 2>&1
    srv_stop
}
trap stop_all EXIT

wait_port() {
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/"$1") 2>/dev/null; do
        i=$((i + 1))
        [ $i -gt 400 ] && { echo "端口 $1 起不来" >&2; return 1; }
        sleep 0.05
    done
}

ms_now() { date +%s%N; }

# 行形状见 src/bin/server.rs::print_stats：
#   nodes    = N
#   seq      = 123 (snapshot 100)     → seq=$3，快照水位=$5（带右括号）
#   snapshot = 12345 B                → 字节=$3
#   wal      = 1234 B logical, ...    → logical=$3
#   replica  = yes (primary seq N lag L applied A resyncs R errors E)
#                                     → 主 seq=$6 lag=$8 applied=$10
sl() { $BIN/treedb-server --listen 127.0.0.1:"$P1" --stats 2>/dev/null | awk -v k="$1" '$1==k'; }
sn() { sl "$1" | awk -v n="$2" '{print $n; exit}'; }

settle() {
    local a b i=0
    a=$(sn seq 5)
    while [ $i -lt 40 ]; do
        sleep 0.15
        b=$(sn seq 5)
        [ "$a" = "$b" ] && return 0
        a=$b
        i=$((i + 1))
    done
    return 0
}

srv_start() { # srv_start AUTO_CHECKPOINT_MB DIR LOG
    srv_stop
    $BIN/treedb-server --dir "$2" --listen 0.0.0.0:"$P1" --workers 4 \
        --auto-checkpoint-mb "$1" > "$3" 2>&1 &
    SRV=$!
    wait_port "$P1"
}

# 把远端控制脚本推上去。副本手工管理：每次都是一个会自然结束的远端进程，
# 不依赖「ssh + 后台符号」那种有时不回的行为。
push_remote() {
    sed -e "s|__P1__|$P1|g" -e "s|__P2__|$P2|g" -e "s|__PRIMARY__|$HOST_IP|g" \
        -e "s|__RWORK__|$RWORK|g" <<'REMOTE' |
#!/bin/bash
# 副本端控制（推到第二台机上跑，不入库）
set -u
BIN=/data/gfs1/rrate/bin
WORK=__RWORK__
D=$WORK/replica
LOG=$WORK/standby.log
P1=__P1__
P2=__P2__
PRIMARY=__PRIMARY__

st() { $BIN/treedb-server --listen 127.0.0.1:$P2 --stats 2>/dev/null; }
fld() { st | awk -v n="$1" '$1=="replica"{print $n; exit}'; }

case "${1:-}" in
ping) echo ok ;;
start)  # start FETCH_BYTES [fresh|resume]
    fb=${2:-1048576}; how=${3:-fresh}
    if [ "$how" = fresh ]; then rm -rf "$D"; fi
    mkdir -p "$D"
    : > "$LOG"
    $BIN/treedb-standby --primary "$PRIMARY:$P1" --dir "$D" --listen 0.0.0.0:$P2 \
        --workers 4 --interval-ms 20 --fetch-bytes "$fb" >> "$LOG" 2>&1 &
    echo $! > "$WORK/standby.pid"
    for i in $(seq 1 500); do
        (exec 3<>/dev/tcp/127.0.0.1/$P2) 2>/dev/null && exit 0
        sleep 0.02
    done
    echo "standby 端口 $P2 没起来" >&2
    exit 1
    ;;
wait)   # wait TARGET_SEQ MAX_MS：等 lag 0 且主水位到 target，打印耗时毫秒
    target=${2:-0}; max=${3:-60000}
    t0=$(date +%s%N); n=0; w=0; info=""
    while [ "$w" -lt "$max" ]; do
        info=$(st | awk '$1=="replica"{print $6, $8}')
        if [ "${info% *}" = "$target" ] && [ "${info#* }" = "0" ]; then
            n=$((n + 1))
        else
            n=0
        fi
        [ $n -ge 3 ] && break
        sleep 0.05
        w=$((w + 50))
    done
    echo $((($(date +%s%N) - t0) / 1000000))
    ;;
sample) # sample N INTERVAL：把 lag 逐行打到 stdout
    n=${2:-20}; iv=${3:-0.2}; i=0
    while [ $i -lt "$n" ]; do
        fld 8
        i=$((i + 1))
        sleep "$iv"
    done
    ;;
line) st | awk '$1=="replica"' ;;
stop)   # SIGTERM、等退出、打印最后两行（含自报的最坏一轮）
    pid=$(cat "$WORK/standby.pid" 2>/dev/null || true)
    if [ -n "${pid:-}" ]; then
        kill -TERM "$pid" 2>/dev/null
        for i in $(seq 1 300); do kill -0 "$pid" 2>/dev/null || break; sleep 0.02; done
    fi
    tail -2 "$LOG" 2>/dev/null
    ;;
worst) sed -n 's/.*worst \([0-9]*\) us.*/\1/p' "$LOG" 2>/dev/null | tail -1 ;;
esac
REMOTE
    ssh -o BatchMode=yes "$PEER" "cat > $RWORK/rs.sh && chmod +x $RWORK/rs.sh" 2>&1 | grep -v setlocale

    local ok
    ok=$(rs ping)
    if [ "$ok" != "ok" ]; then
        echo "远端脚本推不上去：$ok" >&2
        exit 1
    fi
}

# 冷启动一次：远端空目录副本从 0 追到 lag 0。结果写 R_MS / R_WORST。
cold_start() { # cold_start FETCH_BYTES LABEL
    local fb=$1 label=$2 t0 out worst rate xfer
    t0=$(ms_now)
    out=$(rs start "$fb" fresh) || { log "  副本    起不来：$out"; return 1; }
    R_MS=$(rs wait "$SEQ" 60000)
    R_MS=${R_MS:-0}
    out=$(rs stop)
    worst=$(rs worst)
    R_WORST=${worst:-0}
    rate=$(awk -v b="$BYTES" -v m="$R_MS" 'BEGIN{ if (m>0) printf "%.2f", b/1048576/(m/1000.0); else print "inf" }')
    xfer=$(awk -v b="$BYTES" -v m="$R_MS" -v w="$R_WORST" \
        'BEGIN{ t=(m - w/1000.0)/1000.0; if (t>0) printf "%.2f", b/1048576/t; else print "" }')
    log "  副本    $label（$fb B/轮）：端到端 $R_MS ms（装快照那一轮 ${R_WORST} µs）"
    if [ -n "$xfer" ]; then
        log "          端到端 $rate MiB/s；扣掉装快照 ≈ $xfer MiB/s"
    else
        # 最坏一轮 ≥ 端到端 = 这轮把另一轮盖住了（apply 大于传输），扣减模型失效；
        # 那就在端到端速率下面垫一个下界，不编造传输段的数。
        log "          端到端 $rate MiB/s（最坏一轮 ≥ 端到端，扣减模型失效，传输段只能取下界）"
    fi
    if [ "$R_MS" -ge 59000 ]; then
        log "          警告：60 s 没追平，这一格无效"
    fi
    log "  远端    $(printf '%s' "$out" | tail -1 | sed 's/^shutdown: //')"
}

lat_of() { sed -n "s/.*$2=\\([0-9]*\\).*/\\1/p" "$1" | head -1; }
qps_of() { sed -n 's/.*→ \([0-9]*\) ops\/s.*/\1/p' "$1" | head -1; }

# 裸 TCP 上限（不经 ssh 加密）：虚拟网桥上 ssh 吞吐往往被加密卡住，用它才能
# 判断「传输段读数是否被链路封顶」。两个 python3 临时进程，端口固定，用完即收。
link_probe() {
    local ip sink out
    ip=$(ssh -G "$PEER" 2>/dev/null | awk 'tolower($1)=="hostname"{print $2; exit}')
    [ -n "$ip" ] || { echo n/a; return; }
    cat > "$W/sink.py" <<'PY'
import socket, time
s = socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(("0.0.0.0", 17899)); s.listen(1)
c, _ = s.accept(); n = 0; t0 = time.time()
while True:
    b = c.recv(1 << 20)
    if not b: break
    n += len(b)
print(n / (time.time() - t0))
PY
    cat > "$W/src.py" <<'PY'
import socket, sys, time
ip, mb = sys.argv[1], int(sys.argv[2])
s = socket.create_connection((ip, 17899), 5)
s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 << 20)
buf = b"x" * (1 << 20); t0 = time.time()
for _ in range(mb): s.sendall(buf)
s.shutdown(socket.SHUT_WR)
print(mb / (time.time() - t0))
PY
    scp -q "$W/sink.py" "$PEER:/tmp/treedb_sink.py" 2>/dev/null
    ( timeout 120 ssh -n -o BatchMode=yes "$PEER" 'python3 /tmp/treedb_sink.py' 2>/dev/null > "$W/sink.out" ) &
    sink=$!
    sleep 1
    out=$(python3 "$W/src.py" "$ip" 512 2>/dev/null)
    wait "$sink" 2>/dev/null
    awk -v v="${out:-0}" 'BEGIN{ if (v+0>0) printf "%.0f", v; else print "n/a" }'
}

blank() { log ""; }

# ---------------------------------------------------------------- 开跑

push_remote

PEER_NPROC=$(timeout 20 ssh -n -o BatchMode=yes "$PEER" nproc 2>/dev/null | grep -v setlocale)
RAWTCP=$(link_probe)
DD_T=$(LC_ALL=C dd if=/dev/zero bs=1M count=512 2>/dev/null | \
    timeout 180 ssh -o BatchMode=yes "$PEER" 'LC_ALL=C dd of=/dev/null bs=1M 2>&1' 2>&1 | \
    sed -n 's/.*copied, \([0-9.]*\) s.*/\1/p' | tail -1)
NETRATE=$(awk -v s="${DD_T:-0}" 'BEGIN{ if (s+0>0) printf "%.0f", 512/s; else print "n/a" }')
# 副本端的同步写能力：apply 每轮一次 fsync，盘慢会直接压在「追赶速率」上。
DSYNC_T=$(timeout 120 ssh -o BatchMode=yes "$PEER" \
    'LC_ALL=C dd if=/dev/zero of=/data/gfs1/rrate/work/fsync.probe bs=4k count=1000 oflag=dsync 2>&1; rm -f /data/gfs1/rrate/work/fsync.probe' 2>&1 | \
    sed -n 's/.*copied, \([0-9.]*\) s.*/\1/p' | tail -1)
DSYNC_RATE=$(awk -v s="${DSYNC_T:-0}" 'BEGIN{ if (s+0>0) printf "%.0f", 1000/s; else print "n/a" }')
# 宿主同口径，用来区分「第二台机的盘慢」与「复制本身慢」。
# 本地可能是中文 locale（dd 会印「已复制，X s」），一律 LC_ALL=C 才能解析。
HDSYNC_T=$(LC_ALL=C dd if=/dev/zero of="$W/fs.probe" bs=4k count=1000 oflag=dsync 2>&1 | \
    sed -n 's/.*copied, \([0-9.]*\) s.*/\1/p' | tail -1; rm -f "$W/fs.probe")
HDSYNC_RATE=$(awk -v s="${HDSYNC_T:-0}" 'BEGIN{ if (s+0>0) printf "%.0f", 1000/s; else print "n/a" }')
# 探针只写几 MB（4 KiB×1000），但远端 dir 是宿主上的 qcow2：探测完 sync 一拍，
# 避免回写与第一轮测量撞车（实测会抬高追赶 3×、把主 p99 顶到 6 ms）。
sync 2>/dev/null; timeout 60 ssh -n -o BatchMode=yes "$PEER" sync 2>/dev/null; sleep 1

blank
log "=============== $(date '+%F %T')  副本跨机传送（rev $(git rev-parse --short HEAD 2>/dev/null || echo unknown)，§6.6 边界 5）"
log "链路    $PEER（${PEER_NPROC:-?} 核，virtio）：裸 TCP 512 MiB = $RAWTCP MiB/s；dd|ssh = $NETRATE MiB/s（受 ssh 加密限制）"
log "盘      $PEER：4 KiB fsync ${DSYNC_RATE:-n/a} 次/s、宿主同口径 fsync ${HDSYNC_RATE:-n/a} 次/s（副本每轮一次 fsync，这一行就是追赶速率的天花板）"
log "口径    下载字节 = 快照字节 + WAL 逻辑长度；快照块长 = --fetch-bytes；副本 4 worker、interval 20 ms"
log "轮数    $RUNS（每轮重新造数据；[3] 的 A/B 从同一份种子目录派生，写入量逐字节相同）"

for r in $(seq 1 "$RUNS"); do
    blank
    log "--------------- 第 $r/$RUNS 轮"

    # ---- 种子：主上造数据，落一次 checkpoint ----
    rm -rf "$W/primary"
    mkdir -p "$W/primary"
    srv_start 1 "$W/primary" "$W/primary-$r.log" || exit 1
    SEED=$($BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
        --pipeline 32 --secs "$SECS" 2>&1 | head -1)
    settle
    SEQ=$(sn seq 3)
    SNAPB=$(sn snapshot 3)
    WALB=$(sn wal 3)
    BYTES=$((SNAPB + WALB))
    blank
    log "[1] 跨机冷启动：主 nodes=$(sn nodes 3) seq=$SEQ，下载 $BYTES B（快照 $SNAPB + WAL $WALB）"
    log "  主      $SEED"
    cold_start $CHUNK "1 MiB/块（默认）"
    cold_start 4194304 "4 MiB/块（WAL_FETCH_MAX）"

    # ---- [2] 追赶：副本停着，主攒 WAL ----
    blank
    log "[2] 跨机追赶：副本停（远端目录停在 seq $SEQ），主攒 ${SECS}s 的 WAL（--auto-checkpoint-mb 0）"
    srv_start 0 "$W/primary" "$W/primary2-$r.log" || exit 1
    S0=$SEQ
    W0=$(sn wal 3)
    LOAD2=$($BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
        --pipeline 32 --secs "$SECS" 2>&1 | head -1)
    SEQ2=$(sn seq 3)
    W2=$(sn wal 3)
    BACKLOG=$((SEQ2 - S0))
    BPR=$(awk -v w="$((W2 - W0))" -v n="$BACKLOG" 'BEGIN{ if (n>0) printf "%.1f", w/n; else print "0" }')
    BBYTES=$(awk -v b="$BPR" -v n="$BACKLOG" 'BEGIN{ printf "%.0f", b*n }')
    log "  主      $LOAD2"
    log "  堆积    seq $S0 → $SEQ2（+$BACKLOG 条，WAL 逻辑 +$((W2 - W0)) B，≈ $BPR B/条）"
    T0=$(ms_now)
    out=$(rs start $CHUNK resume) || { log "  副本    起不来：$out"; exit 1; }
    rs wait "$SEQ2" 120000 > /dev/null
    DMS=$((($(ms_now) - T0) / 1000000))
    out=$(rs stop)
    DRATE=$(awk -v n="$BACKLOG" -v m="$DMS" 'BEGIN{ if (m>0) printf "%.0f", n*1000.0/m; else print "inf" }')
    MBRATE=$(awk -v b="$BBYTES" -v m="$DMS" 'BEGIN{ if (m>0) printf "%.2f", b/1048576/(m/1000.0); else print "inf" }')
    log "  副本    追 $BACKLOG 条 / $DMS ms → $DRATE 条/s，≈ $MBRATE MiB/s（字节按 $BPR B/条估算）"
    log "  远端    最坏一轮 $(rs worst) µs"
    if [ "$DMS" -ge 119000 ]; then
        log "          警告：120 s 没追平，这一格无效"
    fi

    # ---- [3] 副本对主 p99 的 A/B ----
    blank
    log "[3] 副本对主 p99：A=无副本、B=副本在追积压；两边同种子、同写入量"
    srv_stop
    rm -rf "$W/base"
    cp -a "$W/primary" "$W/base"

    ab_one() { # ab_one A|B 轮次
        local tag=$1 dir="$W/run$1" logf="$W/ab$1-$2.log"
        rm -rf "$dir"
        cp -a "$W/base" "$dir"
        srv_start 0 "$dir" "$logf" || return 1
        if [ "$tag" = B ]; then
            # 先把副本喂到与 base 同步，再停掉、攒积压、放它追 —— 这才是「落后一段」
            rs start $CHUNK fresh > /dev/null || return 1
            rs wait "$(sn seq 3)" 120000 > /dev/null
            rs stop > /dev/null
        fi
        $BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
            --pipeline 32 --secs "$PRIME_SECS" > "$W/prime$tag-$2.log" 2>&1
        if [ "$tag" = B ]; then
            rs start $CHUNK resume > /dev/null || return 1
            ( rs sample 25 0.2 > "$W/lag$2.txt" 2>/dev/null ) &
            RS_PID=$!
        fi
        $BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
            --pipeline 32 --secs "$MEAS_SECS" > "$W/meas$tag-$2.log" 2>&1
        if [ "$tag" = B ]; then
            wait "$RS_PID" 2>/dev/null
            RS_PID=""
            rs stop > /dev/null
        fi
        srv_stop
        return 0
    }

    A50=""; A90=""; A99=""; AQ=""
    B50=""; B90=""; B99=""; BQ=""
    ORDER="A B"
    [ $((r % 2)) -eq 0 ] && ORDER="B A"
    for tag in $ORDER; do
        if ! ab_one "$tag" "$r"; then
            log "  主延迟  $tag：这一格没跑起来"
            continue
        fi
        line=$(head -1 "$W/meas$tag-$r.log")
        a50=$(lat_of "$W/meas$tag-$r.log" p50)
        a90=$(lat_of "$W/meas$tag-$r.log" p90)
        a99=$(lat_of "$W/meas$tag-$r.log" p99)
        qps=$(qps_of "$W/meas$tag-$r.log")
        if [ "$tag" = A ]; then
            log "  主 A（无副本，第 $r 轮）：p50 ${a50:-?} / p90 ${a90:-?} / p99 ${a99:-?} µs、${qps:-?} ops/s"
            A50=$a50; A90=$a90; A99=$a99; AQ=$qps
        else
            log "  主 B（副本在追，第 $r 轮）：p50 ${a50:-?} / p90 ${a90:-?} / p99 ${a99:-?} µs、${qps:-?} ops/s"
            B50=$a50; B90=$a90; B99=$a99; BQ=$qps
        fi
        log "          $line"
        if [ "$tag" = B ]; then
            LAGS=$(sort -n "$W/lag$r.txt" 2>/dev/null | grep -E '^[0-9]+$' || true)
            NSAMP=$(printf '%s\n' "$LAGS" | grep -c . || true)
            NLAG=$(printf '%s\n' "$LAGS" | awk '$1>0' | grep -c . || true)
            MED=$(printf '%s\n' "$LAGS" | awk '{a[NR]=$1} END{ if (NR) print a[int((NR+1)/2)]; else print "n/a" }')
            MAXL=$(printf '%s\n' "$LAGS" | tail -1)
            log "          副本同期 lag 中位 ${MED:-n/a}、最大 ${MAXL:-n/a}（${NSAMP:-0} 个采样，200 ms/个）"
            if [ "${NLAG:-0}" -lt $(( ${NSAMP:-0} / 2 )) ]; then
                log "          警告：只有 ${NLAG:-0}/${NSAMP:-0} 个采样在 lag>0，副本大部分时间已追上 —— 这一格偏弱"
            fi
        fi
    done
    if [ -n "${AQ:-}" ] && [ -n "${BQ:-}" ]; then
        pct() { awk -v a="$1" -v b="$2" 'BEGIN{ if (a+0>0) printf "%+.0f%%", (b-a)*100/a; else print "n/a" }'; }
        # 先把百分比算进变量：`"$(pct "$x" "$y")"` 这种四层嵌套（字符串里套
        # 命令替换、替换里又套字符串）在本机 bash 5.0.17 上会让解析器把引号
        # 数错，整脚本报“寻找匹配的\"”。分开写无副作用。
        d50=$(pct "$A50" "$B50")
        d90=$(pct "$A90" "$B90")
        d99=$(pct "$A99" "$B99")
        dq=$(pct "$AQ" "$BQ")
        log "  本轮    p50 $d50（$A50 → $B50 µs）、p90 $d90（$A90 → $B90）、p99 $d99（$A99 → $B99）、吞吐 $dq（$AQ → $BQ ops/s）"
        echo "$A50 $A90 $A99 $AQ $B50 $B90 $B99 $BQ" >> "$W/pairs.txt"
    fi
done

blank
if [ -s "$W/pairs.txt" ]; then
    CNT=$(wc -l < "$W/pairs.txt")
    # 中位数（列 1-4 = A，5-8 = B；可移植，不依赖 gawk 的 asort）
    med() { awk -v c="$1" '{print $c}' "$W/pairs.txt" | sort -n | awk -v n="$CNT" 'NR==int((n+1)/2){print; exit}'; }
    rng() { awk -v c="$1" '{print $c}' "$W/pairs.txt" | sort -n | awk 'NR==1{a=$1} {b=$1} END{print a"–"b}'; }
    dlt() { awk -v a="$1" -v b="$2" 'BEGIN{ if (a+0>0) printf "%+.0f%%", (b-a)*100/a; else print "n/a" }'; }
    m1=$(med 1); m2=$(med 2); m3=$(med 3); m4=$(med 4)
    m5=$(med 5); m6=$(med 6); m7=$(med 7); m8=$(med 8)
    r1=$(rng 1); r2=$(rng 2); r3=$(rng 3); r4=$(rng 4)
    r5=$(rng 5); r6=$(rng 6); r7=$(rng 7); r8=$(rng 8)
    log "汇总    n=$CNT（每轮 A/B 同种子、同写入量）"
    log "        p50   A $m1 µs（$r1）→ B $m5 µs（$r5） = $(dlt "$m1" "$m5")"
    log "        p90   A $m2 µs（$r2）→ B $m6 µs（$r6） = $(dlt "$m2" "$m6")"
    log "        p99   A $m3 µs（$r3）→ B $m7 µs（$r7） = $(dlt "$m3" "$m7")"
    log "        吞吐  A $m4 ops/s（$r4）→ B $m8 ops/s（$r8） = $(dlt "$m4" "$m8")"
    log "        读数  p50/p90/吞吐逐轮稳定（分布整体平移）；p99 是单点分位，宿主后台回写会抬高它，"
    log "              所以结论以 p50/p90 + 吞吐为准"
fi
log "说明    负载发生器与主同机（loopback），副本在第二台机上走真实链路；"
log "        A/B 的唯一差别是主是否为副本提供 WAL/快照，故差值归因于复制开销本身。"

stop_all
