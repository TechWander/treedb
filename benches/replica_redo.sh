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
#   [mid]    `write --ino 中段节点`：正文前半段逐字节不动、后半段分叉 —— 整段 crc
#            对不上，只有二分才能把前半段留下；
#   [create] `create`：`nodes` 条数变了，正文从很前面就分叉 —— crc 对不上，二分
#            也只能留下一个头。这一格证明判据是对内容的，也证明不留用时**没有回归**。
#
# 造数据的库用 `--views 1 --view-tag 0` 把 64 个线程的节点全堆进同一棵树：节点在
# 快照里按 `(view, ino)` 排，只有一棵树时「中段 ino」才真落在正文中段（每个线程
# 一棵树的话，中段 ino 也在正文的 1/64 处）。
#
# 两份二进制：老的是 `43f55b3`（前一笔，探针只判「整段等不等」），新的是当前工作树。
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
OLDREV=${OLDREV:-43f55b3}
OLDSRC=${OLDSRC:-.spot/redo-oldsrc}
OLDTGT=${OLDTGT:-.spot/redo-oldtarget}
# 换代注入点：副本起跑后多久把 WAL 推过阈值。这个数决定「换代时已下多少」，
# 而 [mid] 要的就是「已下量 > 改动位置」—— 否则前半段一格没变，整段 crc 也对
# 得上，二分没机会上场。实测换代时刻 ≈ 起跑 + GAP + 1.5 s（编码）；20 MB/s 下
# 2000 ms 时已下约 70 MB（四成），比改动位置（三成）大一大截。
GAP_MS=${GAP_MS:-2000}
# 预装量：只到阈值的一半。它的作用是「让换代那份快照里有东西变」，不是去凑阈值
# —— 凑阈值留给 push 阶段（那时才知道副本已经下了多少）。
PRELOAD=${PRELOAD:-524288}
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
# 认一个 `.rev` 戳：重复跑时复用，但抄错 OLDREV（或换了基线）就重编 —— 否则旧
# 目标目录里坐着的可能是另一个 revision 的二进制，A/B 两臂就不成立。
build_old() {
    if [ -x "$OLDTGT/release/treedb-standby" ] &&
        [ "$(cat "$OLDTGT/.rev" 2>/dev/null)" = "$OLDREV" ]; then
        return 0
    fi
    log "  编译旧二进制（$OLDREV → $OLDTGT/release）：开始"
    rm -rf "$OLDSRC" "$OLDTGT"
    mkdir -p "$OLDSRC"
    git archive "$OLDREV" | tar -x -C "$OLDSRC" || return 1
    CARGO_TARGET_DIR="$OLDTGT" cargo build --release \
        --manifest-path "$OLDSRC/Cargo.toml" --bin treedb-server --bin treedb-standby >/dev/null 2>&1 || {
        log "  编译旧二进制失败"
        return 1
    }
    printf '%s\n' "$OLDREV" > "$OLDTGT/.rev"
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
        --views 1 --view-tag 0 --pipeline 32 --secs "$SECS" > "$d/seed.log" 2>&1
    settle
    SEED_NODES=$(s_num "$P1" nodes 3)
    SEED_SEQ=$(s_num "$P1" seq 3)
    SEED_SNAP=$(s_num "$P1" snapshot 3)
    SEED_SSEQ=$(snap_gen "$P1")
    # 中段节点：实测节点段占快照正文约 7 成、记录长得很均匀，所以 ino 取全量的
    # 42% 就落在正文三成处（ino=50% 实测 first_diff=35.5%）。取三成而不是正中，
    # 是为了让「换代时已下量」稳稳压过改动位置（四成 vs 三成）。
    MID_INO=$((2 + (${SEED_NODES:-0} - 2) * 42 / 100))
    kill -TERM "$SRV" 2>/dev/null
    wait "$SRV" 2>/dev/null
    SRV=""
    log "  种子    nodes=$SEED_NODES seq=$SEED_SEQ snapshot=$SEED_SSEQ snap_bytes=$SEED_SNAP"
}

# 慢速写：逐行 fsync 的形状 ~700 KB/s，用来把 WAL 预装到一半、以及随后的 push
# 把 WAL 推过 1 MB。换代注入不能更快：越过阈值只要一小段，写多了只会让
# checkpoint 提前，把「换代时刻」从我们手里抢走。
slow_write() { # slow_write OP MAX_SECS [EXTRA...]
    $BIN/treedb-bench --addr 127.0.0.1:"$P1" --op "$1" --conns 8 --threads 8 \
        --pipeline 1 --secs "$2" ${3:-} > "$W/last-slow.log" 2>&1 &
    LOAD=$!
}

# 预装一小段 WAL，并保证没有越过阈值。
#
# 预装写入就是「换代那份快照里有什么变」的来源；但它不能把 WAL 顶过 1 MB，
# 否则 checkpoint 在副本开跑之前就发起来了，脚本看到的换代其实是那一次，
# 「已下多少」就只剩两成多。以前预装到 1 MB − 8 KB，10 ms 的轮询间隔在
# 700 KB/s 下就能越过 7 KB —— 实测每一轮都越过了。所以留在阈值的一半。
#
# 返回 0 = 起点干净；1 = 还是越过了（调用方等 checkpoint 收口后重试）。
preload() { # preload OP EXTRA
    local w i=0
    while [ $i -lt 40 ]; do
        i=$((i + 1))
        w=$(s_num "$P1" wal 3)
        [ -n "$w" ] && [ "$w" -ge "$PRELOAD" ] && break
        slow_write "$1" 20 "$2"
        while kill -0 "$LOAD" 2>/dev/null; do
            w=$(s_num "$P1" wal 3)
            [ -n "$w" ] && [ "$w" -ge "$PRELOAD" ] && break
            sleep 0.01
        done
        kill "$LOAD" 2>/dev/null
        wait "$LOAD" 2>/dev/null
        LOAD=""
    done
    w=$(s_num "$P1" wal 3)
    [ -n "$w" ] && [ "$w" -lt "$THRESH" ] && return 0
    return 1
}

# 一轮：$1 = 轮次目录，$2 = 二进制目录，$3 = 换代负载的 op，$4 = 该负载的额外参数。
# 结果写回 RUN_MS / RUN_GAP / RUN_KEEP / RUN_KEEPB / RUN_RESTART / RUN_RESYNCS / RUN_OK。
one_run() { # one_run DIR BIN OP [EXTRA]
    local d=$1 bin=$2 op=$3 extra=${4:-}
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

    # 预装一小段 WAL（不过阈值），中途若还是越过了就等 checkpoint 收口重来。
    local tries=0
    until preload "$op" "$extra"; do
        tries=$((tries + 1))
        if [ $tries -ge 5 ]; then
            log "  WAL 预装反复越过阈值，放弃这一轮" >&2
            return 1
        fi
        settle
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

    # 下载一截之后把 WAL 推过 1 MB（从一半推到满，约 0.4 s）：换代正好落在
    # 下载窗口里。一越过阈值就停写 —— 再写下去会在第一份 checkpoint 编码期间
    # 把 WAL 重新堆满，逼出第二次换代，三个形状就不同形了。
    # （毫秒要按 1000 进制拆：以前写成 `sleep "0.$GAP_MS"`，
    # GAP_MS=2000 就变成 0.2 s —— 换代比 push 还早，量到的是预装那一次。）
    sleep "$(printf '%d.%03d' $((GAP_MS / 1000)) $((GAP_MS % 1000)))"
    slow_write "$op" 20 "$extra"

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
    local keep restart resyncs keepb
    keep=$(grep -c "keeping .* already downloaded" "$d/replica.log" 2>/dev/null || true)
    restart=$(grep -c "restarting from 0" "$d/replica.log" 2>/dev/null || true)
    # 探针判定留了多少字节：「keeping X of Y B already downloaded」。老臂没有这行。
    keepb=$(sed -n 's/.*keeping \([0-9]*\) of \([0-9]*\) B.*/\1 of \2/p' "$d/replica.log" 2>/dev/null | tail -1)
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
    RUN_KEEPB=${keepb:-none}
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
log "[种子] 两臂共用的起点（$SECS s 造数据，单视图）"
seed_state || exit 1
log "  中段节点 mid_ino=$MID_INO（[mid] 改它；单视图，ino 42% ≈ 正文三成处）"

for shape in row mid create; do
    case $shape in
    row)
        op=rowput
        extra=""
        oplabel="rowput（改动落在快照尾部 → 前缀整体可留）"
        ;;
    mid)
        op=write
        extra="--views 1 --view-tag 0 --ino $MID_INO"
        oplabel="write ino=$MID_INO（改动落在正文三成处 → 老臂整份重下，二分能留前段）"
        ;;
    create)
        op=create
        extra="--views 1 --view-tag 0"
        oplabel="create（nodes 条数变了 → 几乎留不下）"
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
            log "  old（$OLDREV，探针只判「整段等不等」，对不上就整份重下）"
        else
            log "  new（工作树，整段对不上再二分找最长公共前缀）"
        fi
        for r in $(seq 1 "$REPS"); do
            one_run "$W/$arm-$shape-$r" "$bin" "$op" "$extra" || exit 1
            log "    第 $r 轮  端到端 $RUN_MS ms（$RUN_OK）｜换代落在 ${RUN_GAP} ms｜预装 WAL $WAL0 B｜留前缀 $RUN_KEEP 次、留下 $RUN_KEEPB B、整份重下 $RUN_RESTART｜resyncs $RUN_RESYNCS"
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
log "  - 「留前缀 N 次」是撞上的换代次数，「留下 X of Y B」是探针最后判定的可复用"
log "    字节数（Y = 换代时已下量）。老臂没有前缀探针，这两列是 none。"
log "  - [row] 改的是快照最后一段：Y 内的字节逐字节相同，一次探针就能留全部。"
log "  - [mid] 改的是正文三成处的节点：整段 crc 必然对不上，老臂只能从 0 重下，"
log "    新臂二分后应留下「改动位置之前的全部」（三成正文），丢掉其余已下部分。"
log "  - [create] 改动落在 nodes 条数上，正文从很前面就分叉：新臂也几乎留不下，"
log "    时间应当与老臂相当。这一格是「不静默拼错」的反面证据。"
log "  - 端到端含 WAL 预装、限速下载、换代与装快照，两臂同形，只比同形状的两臂。"
