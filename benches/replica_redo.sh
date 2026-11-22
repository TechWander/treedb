#!/bin/bash
# 快照换代重下：前缀复用（`Reply::SnapPrefix` 探针）的 A/B —— DESIGN.md §6.6。
#
# 冷启动下载一份快照时，主又 checkpoint 一次（`snap_seq` 变了）是常态。老做法是
# `buf.clear(); off = 0` 整份重来；这一笔让副本先问主「我已下那 N 字节的前缀在新
# 的一代里是不是也这样」（`SnapFetch { offset: N, len: 0 }` → `SnapPrefix`），
# crc 对得上就接着下。这个脚本量的是**换代那一下白拉了多少**。
#
# 为什么要给副本限速（benches/throttle.py）：本机 128 核、localhost 之下 164 MB
# 快照 ~1.5 s 就下完了，而主侧一次 checkpoint 的编码要 4-5 s —— 换代永远落在
# 下载结束之后，那样两臂都只是「装完再 gap 一次」，测不到前缀复用。代理把下行
# 压到 20 MB/s，换代就必然落在下载窗口的中段。
#
# 每一轮这样对准：
#   1. 主用 `--auto-checkpoint-mb 1`，先慢慢写到 WAL 差一点满 1 MB 就停；
#   2. 空目录副本经限速代理从 0 开始下载（主没在写，快照不会自己换代）；
#   3. GAP_MS 之后只再写一点点，把 WAL 推过 1 MB → 自动 checkpoint 派发
#      → 一次换代正好落在下载中段；看到换代就停写（不再有第二次）；
#   4. 量「副本进程起 → lag 0」的墙钟。老做法要从 0 重下整份，新做法只补尾巴。
#
# 两个形状（换代负载不同）：
#   [row]    `rowput`：表的行在快照编码最尾部，换代只动了尾巴 —— 前缀整体可复用；
#   [create] `create`：`nodes` 条数变了，正文从很前面就分叉 —— crc 对不上，必须
#            整份重来。这一格证明判据是对内容的，也证明不留用时**没有回归**。
#
# 两份二进制：老的是 `c5ff0b7`（前一版，换代即整份重下），新的是当前工作树。
#
# 用法：benches/replica_redo.sh [造数据秒数=40] [每臂轮数=3]
# 结果追加到 benches/results/replica_redo.log；原始日志留在 .spot/redo/<arm>/。
set -u

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
SECS=${1:-40}
REPS=${2:-3}
BIN=./target/release
OUT=${OUT:-benches/results/replica_redo.log}
W=.spot/redo
P1=${P1:-17841}
P2=${P2:-17842}
P3=${P3:-17843}
OLDREV=${OLDREV:-c5ff0b7}
OLDSRC=${OLDSRC:-.spot/redo-oldsrc}
OLDTGT=${OLDTGT:-.spot/redo-oldtarget}
# 换代注入点：副本起跑后多久把 WAL 推过阈值。要早于「换代完成时刻 - 一小段」，
# 让副本在换代时已经下了一截（20 MB/s 下 500 ms ≈ 10 MB）。
GAP_MS=${GAP_MS:-500}
PRELOAD=${PRELOAD:-$((1048576 - 8192))}
THRESH=${THRESH:-1048576}
# 代理的下行限速（字节/秒）。必须明显低于主侧 checkpoint 的编码速率
# （164 MB 状态约 33 MB/s），否则下载先于换代结束。
RATE=${RATE:-20000000}
REV=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)
WT=$([ -n "$(git status --porcelain 2>/dev/null)" ] && echo "+wt" || echo "")
mkdir -p benches/results

SRV=""
REP=""
LOAD=""
THR=""

log() { printf '%s\n' "$*" | tee -a "$OUT"; }

stop_all() {
    [ -n "$LOAD" ] && kill "$LOAD" 2>/dev/null
    if [ -n "$REP" ]; then
        kill -TERM "$REP" 2>/dev/null
        wait "$REP" 2>/dev/null
        REP=""
    fi
    [ -n "$THR" ] && kill "$THR" 2>/dev/null
    [ -n "$SRV" ] && kill "$SRV" 2>/dev/null
    wait 2>/dev/null
}
trap stop_all EXIT

wait_port() {
    local p=$1 i=0
    until (exec 3<>/dev/tcp/127.0.0.1/"$p") 2>/dev/null; do
        i=$((i + 1))
        if [ $i -gt 400 ]; then
            log "端口 $p 起不来" >&2
            return 1
        fi
        sleep 0.05
    done
}

ms_now() { date +%s%N; }

s_line() { $BIN/treedb-server --listen 127.0.0.1:"$1" --stats 2>/dev/null | awk -v k="$2" '$1==k'; }
s_num() { s_line "$1" "$2" | awk -v n="$3" '{print $n; exit}'; }
snap_gen() { s_num "$1" seq 5 | tr -d ')'; }

# 等主的快照水位稳定：checkpoint 跑在后台，停写之后还要等它收口。
settle() {
    local a b i=0
    a=$(snap_gen "$P1")
    while [ $i -lt 200 ]; do
        sleep 0.1
        b=$(snap_gen "$P1")
        [ "$a" = "$b" ] && return 0
        a=$b
        i=$((i + 1))
    done
    return 0
}

# 上一版二进制：`git archive` 到 .spot 里独立编，不碰工作树、不借 target/。
build_old() {
    if [ -x "$OLDTGT/release/treedb-standby" ]; then
        return 0
    fi
    log "  编译旧二进制（$OLDREV → $OLDTGT/release）：开始"
    rm -rf "$OLDSRC"
    mkdir -p "$OLDSRC"
    git archive "$OLDREV" | tar -x -C "$OLDSRC" || return 1
    CARGO_TARGET_DIR="$OLDTGT" cargo build --release \
        --manifest-path "$OLDSRC/Cargo.toml" --bin treedb-server --bin treedb-standby >/dev/null 2>&1 || {
        log "  编译旧二进制失败"
        return 1
    }
    log "  编译旧二进制：完成"
}

# 造一份两臂**完全相同**的种子目录：先单独起主造数据、落快照、停掉，再拷给两臂。
# 不这么做的话两次造数据的节点数与快照字节都不一样，「追平耗时」就乘了一个尺寸
# 因子 —— 这条测的是换代，不是尺寸。
seed_state() {
    local d=$W/seed
    rm -rf "$d"
    mkdir -p "$d"
    $BIN/treedb-server --dir "$d/primary" --listen 127.0.0.1:"$P1" --workers 4 \
        --auto-checkpoint-mb 1 > "$d/primary.log" 2>&1 &
    SRV=$!
    wait_port "$P1" || return 1
    $BIN/treedb-bench --addr 127.0.0.1:"$P1" --op create --conns 64 --threads 64 \
        --pipeline 32 --secs "$SECS" > "$d/seed.log" 2>&1
    settle
    SEED_NODES=$(s_num "$P1" nodes 3)
    SEED_SEQ=$(s_num "$P1" seq 3)
    SEED_SNAP=$(s_num "$P1" snapshot 3)
    SEED_SSEQ=$(snap_gen "$P1")
    kill -TERM "$SRV" 2>/dev/null
    wait "$SRV" 2>/dev/null
    SRV=""
    log "  种子    nodes=$SEED_NODES seq=$SEED_SEQ snapshot=$SEED_SSEQ snap_bytes=$SEED_SNAP"
}

# 慢速写：逐行 fsync 的形状 ~200 KB/s，用来把 WAL 顶到「差一点满 1 MB」和随后的
# 「越过阈值」。换代注入不能更快：越过阈值只要 8 KB，写多了只会让 checkpoint 提前。
slow_write() { # slow_write OP MAX_SECS
    $BIN/treedb-bench --addr 127.0.0.1:"$P1" --op "$1" --conns 8 --threads 8 \
        --pipeline 1 --secs "$2" > "$W/last-slow.log" 2>&1 &
    LOAD=$!
}

# 一轮：$1 = 轮次目录，$2 = 二进制目录，$3 = 换代负载的 op。
# 结果写回 RUN_MS / RUN_GAP / RUN_KEEP / RUN_RESTART / RUN_RESYNCS / RUN_OK。
one_run() { # one_run DIR BIN OP
    local d=$1 bin=$2 op=$3
    rm -rf "$d"
    mkdir -p "$d"
    cp -a "$W/seed/primary" "$d/primary"

    "$bin/treedb-server" --dir "$d/primary" --listen 127.0.0.1:"$P1" --workers 4 \
        --auto-checkpoint-mb 1 > "$d/primary.log" 2>&1 &
    SRV=$!
    wait_port "$P1" || return 1

    # 下行限速代理：副本连 P3，代理转发到 P1。老臂也走它，两臂同形。
    python3 benches/throttle.py "$P3" "$P1" "$RATE" > "$d/throttle.log" 2>&1 &
    THR=$!
    sleep 0.3

    # 预装 WAL 到差一点满 1 MB。中途若已越过阈值（checkpoint 把 WAL 清空），重来。
    local g0 w g i=0
    g0=$(snap_gen "$P1")
    while [ $i -lt 40 ]; do
        i=$((i + 1))
        w=$(s_num "$P1" wal 3)
        if [ -n "$w" ] && [ "$w" -ge "$PRELOAD" ]; then
            break
        fi
        slow_write "$op" 20
        while kill -0 "$LOAD" 2>/dev/null; do
            w=$(s_num "$P1" wal 3)
            if [ -n "$w" ] && [ "$w" -ge "$PRELOAD" ]; then
                break
            fi
            if [ "$(snap_gen "$P1")" != "$g0" ]; then
                break
            fi
            sleep 0.01
        done
        kill "$LOAD" 2>/dev/null
        wait "$LOAD" 2>/dev/null
        LOAD=""
        if [ "$(snap_gen "$P1")" != "$g0" ]; then
            # 预装时就越过了阈值：等这次 checkpoint 收口，从零再预装。
            settle
            g0=$(snap_gen "$P1")
            continue
        fi
    done
    WAL0=$(s_num "$P1" wal 3)
    GEN0=$(snap_gen "$P1")

    # 副本从 0 起跑。时钟从这里算。
    mkdir -p "$d/replica"
    "$bin/treedb-standby" --primary 127.0.0.1:"$P3" --dir "$d/replica" \
        --listen 127.0.0.1:"$P2" --workers 4 --interval-ms 20 --fetch-bytes 1048576 \
        --timeout-ms 20000 > "$d/replica.log" 2>&1 &
    REP=$!
    wait_port "$P2" || return 1
    local t0
    t0=$(ms_now)

    # 下载一截之后把 WAL 推过 1 MB：换代正好落在下载窗口里。一越过阈值就停写 ——
    # 再写下去会在第一份 checkpoint 编码期间把 WAL 重新堆满，逼出第二次换代，
    # 两个形状就不同形了。
    sleep "0.$(printf '%03d' "$GAP_MS")"
    slow_write "$op" 20

    local g1 tck="" j=0 pushed=0
    while [ $j -lt 400 ]; do
        sleep 0.02
        if [ $pushed -eq 0 ]; then
            w=$(s_num "$P1" wal 3)
            if [ -n "$w" ] && [ "$w" -ge "$THRESH" ]; then
                kill "$LOAD" 2>/dev/null
                wait "$LOAD" 2>/dev/null
                LOAD=""
                pushed=1
            fi
        fi
        g1=$(snap_gen "$P1")
        if [ -n "$g1" ] && [ "$g1" != "$GEN0" ]; then
            tck=$((($(ms_now) - t0) / 1000000))
            break
        fi
        j=$((j + 1))
    done
    kill "$LOAD" 2>/dev/null
    wait "$LOAD" 2>/dev/null
    LOAD=""
    RUN_GAP=${tck:-none}
    settle
    local fin
    fin=$(s_num "$P1" seq 3)

    # 停写、快照收口之后，等副本到 lag 0（连续 3 次采样）。
    local n=0 k=0 l ms ok
    while [ $k -lt 2400 ]; do
        k=$((k + 1))
        l=$(s_line "$P2" replica)
        if [ "$(printf '%s' "$l" | awk '{print $8}')" = "0" ] &&
            [ "$(printf '%s' "$l" | awk '{print $6}')" = "$fin" ]; then
            n=$((n + 1))
            [ $n -ge 3 ] && break
        else
            n=0
        fi
        sleep 0.05
    done
    ms=$((($(ms_now) - t0) / 1000000))
    ok=$([ $k -lt 2400 ] && echo yes || echo NO)

    kill -TERM "$REP" 2>/dev/null
    wait "$REP" 2>/dev/null
    REP=""
    local keep restart resyncs
    keep=$(grep -c "keeping .* already downloaded" "$d/replica.log" 2>/dev/null || true)
    restart=$(grep -c "restarting from 0" "$d/replica.log" 2>/dev/null || true)
    resyncs=$(sed -n '$p' "$d/replica.log" | sed -n 's/.*resyncs \([0-9]*\).*/\1/p')
    kill -TERM "$SRV" 2>/dev/null
    wait "$SRV" 2>/dev/null
    SRV=""
    kill "$THR" 2>/dev/null
    wait "$THR" 2>/dev/null
    THR=""

    RUN_MS=$ms
    RUN_OK=$ok
    RUN_KEEP=${keep:-0}
    RUN_RESTART=${restart:-0}
    RUN_RESYNCS=${resyncs:-?}
    return 0
}

median3() { printf '%s\n' "$@" | sort -n | sed -n 2p; }

log ""
log "=============== $(date '+%F %T')  快照换代重下 A/B（§6.6，rev ${REV}${WT}，旧 $OLDREV）"
log "口径：两臂同一份种子；副本经限速代理（下行 $((RATE / 1000000)) MB/s）从 0 起跑，"
log "      起跑 ${GAP_MS} ms 后把主侧 WAL 推过 1 MB（--auto-checkpoint-mb 1）触发一次换代；"
log "      量「副本进程起 → lag 0」；每臂 ${REPS} 轮"
build_old || exit 1
NEW="$BIN"
OLD="$OLDTGT/release"

log ""
log "[种子] 两臂共用的起点（$SECS s 造数据）"
seed_state || exit 1

for shape in row create; do
    case $shape in
    row)
        op=rowput
        oplabel="rowput（改动落在快照尾部 → 前缀可留）"
        ;;
    create)
        op=create
        oplabel="create（nodes 条数变了 → 前缀留不下）"
        ;;
    esac
    log ""
    log "[$shape] $oplabel"
    OLD_MS=()
    NEW_MS=()
    for arm in old new; do
        bin=$NEW
        [ "$arm" = old ] && bin=$OLD
        if [ "$arm" = old ]; then
            log "  old（$OLDREV，换代即整份重下）"
        else
            log "  new（工作树，探针 + 前缀复用）"
        fi
        for r in $(seq 1 "$REPS"); do
            one_run "$W/$arm-$shape-$r" "$bin" "$op" || exit 1
            log "    第 $r 轮  端到端 $RUN_MS ms（$RUN_OK）｜换代落在 ${RUN_GAP} ms｜预装 WAL $WAL0 B｜留前缀 $RUN_KEEP、整份重下 $RUN_RESTART｜resyncs $RUN_RESYNCS"
            if [ "$arm" = old ]; then
                OLD_MS[$r]=$RUN_MS
            else
                NEW_MS[$r]=$RUN_MS
            fi
        done
        if [ "$arm" = old ]; then
            log "    中位  $([ "$REPS" -ge 3 ] && median3 "${OLD_MS[@]}" || echo "${OLD_MS[1]}") ms"
        else
            log "    中位  $([ "$REPS" -ge 3 ] && median3 "${NEW_MS[@]}" || echo "${NEW_MS[1]}") ms"
        fi
    done
    om=$([ "$REPS" -ge 3 ] && median3 "${OLD_MS[@]}" || echo "${OLD_MS[1]}")
    nm=$([ "$REPS" -ge 3 ] && median3 "${NEW_MS[@]}" || echo "${NEW_MS[1]}")
    log "  对比    $shape：端到端中位 ${om} → ${nm} ms（$(awk -v a="$om" -v b="$nm" 'BEGIN{ if (b>0) printf "%.2f×", a/b; else print "n/a" }')）"
done

log ""
log "读数提示："
log "  - [row] 的「留前缀」行数是撞上的换代次数；老二进制没有这行日志，它的对应"
log "    行为只能从「端到端 ms」看出来（它每次换代都从 0 重下）。"
log "  - [create] 改动落在 nodes 条数上，前缀从很前面就分叉：新臂也必须整份重下，"
log "    时间应当与老臂相当。这一格是「不静默拼错」的反面证据，不是性能数字。"
log "  - 端到端含 WAL 预装、限速下载、换代与装快照，两臂同形，只比同形状的两臂。"
