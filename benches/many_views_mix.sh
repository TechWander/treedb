#!/bin/bash
# 万级混合视图并发曲线：N 个视图 × W 个文件，混合负载「2 Lookup + 1 ReadDir +
# 1 Create + 1 Unlink」在两个连接档上各跑固定秒数。
#
# 与 many_views.sh 的分界：那个量具打 grovefs meta-server 侧的 treedb 连接池形状
# （要 `--features treedb` 的 grovefs 二进制与 fusermount），属跨仓；这个量具只用
# treedb 自己的 server/bench 两个二进制，是纯引擎侧的万级视图并发曲线。
#
# 用法：
#   bash benches/many_views_mix.sh
#   VIEWS="100 1000" CONNS="8 64" SECS=5 ROWS=20 bash benches/many_views_mix.sh
#   OUT=... TSV=... 把落盘改到别处，用于单格隔离探针（不覆盖正式矩阵）
#
# 输出：
#   benches/results/many_views_mix.log   人读摘要与本轮配置
#   benches/results/many_views_mix.tsv   机器可读逐格结果
#   .spot/many-views-mix-<随机>/         服务端日志、RSS 采样、bench 原行
#
# 硬断言：seed 的 records=2N+3NW、writes=N+NW、created=N*W、每视图一页 ReadDir 数
# 满 W 项；混合档 records=5G、writes=2G=lookups、ops=5G=lat_samples、
# readdirs=creates=unlinks=G、views_touched=min(N,G)、头尾名字级核对
# gone=kept=min(G,400)、nodes/dentries 停在语料形状、locks=refs=0、errors=0。
# 每档重开一次服务端：seed 与测量分属两个生命周期，停服日志里的 worst_round_us 才
# 只覆盖混合窗口；而 mvmix 开跑前的语料形状断言顺带成为「万级视图重启后账本还在」
# 的验收 —— 视图数、inode 数、dentry 数三项都必须精确相等，缺一格就退出。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
BIN=./target/release
VIEWS=${VIEWS:-"100 1000 5000 10000"}
CONNS=${CONNS:-"8 64"}
ROWS=${ROWS:-20}
SECS=${SECS:-5}
PIPE=${PIPE:-50}
WORKERS=${WORKERS:-8}
SAMPLE_MS=${SAMPLE_MS:-20}
RSS_INTERVAL=${RSS_INTERVAL:-0.02}
AUTO=${AUTO:-1}
OUT=${OUT:-benches/results/many_views_mix.log}
TSV=${TSV:-benches/results/many_views_mix.tsv}

for b in treedb-server treedb-bench; do
    [ -x "$BIN/$b" ] || { echo "缺 $BIN/$b，先跑 cargo build --release --bins"; exit 1; }
done
case "$ROWS:$SECS:$PIPE:$WORKERS:$SAMPLE_MS:$AUTO" in
    *[!0-9:]*|*:) echo "ROWS/SECS/PIPE/WORKERS/SAMPLE_MS/AUTO 必须是整数"; exit 2 ;;
esac
[ "$ROWS" -gt 0 ] && [ "$ROWS" -le 1000 ] || { echo "ROWS 必须在 1..1000"; exit 2; }
[ "$SECS" -gt 0 ] && [ "$WORKERS" -gt 0 ] || { echo "SECS/WORKERS 必须 > 0"; exit 2; }
[ "$SAMPLE_MS" -ge 0 ] || { echo "SAMPLE_MS 必须 >= 0"; exit 2; }
case "$AUTO" in 0|1) ;; *) echo "AUTO 只能 0/1"; exit 2 ;; esac
[ -n "$VIEWS" ] && [ -n "$CONNS" ] || { echo "VIEWS/CONNS 不能为空"; exit 2; }
for x in $VIEWS $CONNS; do
    case "$x" in *[!0-9]*) echo "视图/连接档只能是正整数：'$x'"; exit 2 ;; esac
    [ "$x" -gt 0 ] || { echo "视图/连接档必须 > 0：'$x'"; exit 2; }
done
[ $((PIPE % 5)) -eq 0 ] || { echo "PIPE 必须是 5 的倍数（一组五个 op）"; exit 2; }
awk -v x="$RSS_INTERVAL" 'BEGIN { exit !(x + 0 > 0) }' || {
    echo "RSS_INTERVAL 必须是 > 0 的秒数"; exit 2;
}

PORT=$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)
ADDR="127.0.0.1:$PORT"
mkdir -p benches/results .spot
R=$(mktemp -d "$ROOT/.spot/many-views-mix-XXXXXX")
SRV=""
MON=""
STOP_ROUNDS=0
STOP_WORST=0
: > "$OUT"
printf '%s\n' \
    'views	rows	conns	pipeline	secs	auto_mb	seed_time_ms	seed_records	time_ms	groups	ops	records	writes	syncs	lookups	readdirs	creates	unlinks	lat_p50_us	lat_p90_us	lat_p99_us	lat_max_us	lat_samples	views_touched	gone_ok	kept_ok	checkpoints_start	checkpoints_delta	wal_start_bytes	wal_end_bytes	wal_peak_bytes	snap_seq_start	snap_seq_end	rss_before_kb	rss_after_kb	rss_peak_kb	hwm_after_kb	rss_samples	worst_round_us	rounds	nodes_left	dentries_left	locks_left	refs_left	ops_per_s	errors' \
    > "$TSV"

say() { printf '%s\n' "$*" | tee -a "$OUT"; }
die() { say "!! $*"; exit 1; }
field() {
    printf '%s\n' "$1" | awk -v k="$2" '{
        for (i = 1; i <= NF; i++) if (index($i, k "=") == 1) {
            split($i, a, "="); print a[2]; exit
        }
    }'
}
uint() {
    local name=$1 value=$2
    case "$value" in
        ''|*[!0-9]*) die "$name 不是非负整数：'$value'" ;;
    esac
}
rss_now() {
    awk '/^VmRSS:/ {r=$2} /^VmHWM:/ {h=$2} END {print r+0, h+0}' "/proc/$SRV/status"
}
min2() { [ "$1" -le "$2" ] && printf '%s' "$1" || printf '%s' "$2"; }

start_server() {
    local dir=$1 tag=$2 i=0
    mkdir -p "$dir"
    "$BIN/treedb-server" --dir "$dir" --listen "$ADDR" --workers "$WORKERS" \
        --durability group --auto-checkpoint-mb "$AUTO" > "$R/$tag.srv.log" 2>&1 &
    SRV=$!
    until "$BIN/treedb-server" --listen "$ADDR" --stats > /dev/null 2>&1; do
        i=$((i + 1))
        [ "$i" -lt 600 ] || die "$tag 服务端 30s 内没就绪：$(tail -3 "$R/$tag.srv.log")"
        sleep 0.05
    done
}
stop_server() {
    local tag=$1 state i
    "$BIN/treedb-server" --listen "$ADDR" --shutdown > "$R/$tag.shutdown.log" 2>&1 \
        || die "$tag 停服没拿到 ack：$(cat "$R/$tag.shutdown.log")"
    for i in $(seq 600); do
        state=$(ps -o stat= -p "$SRV" 2>/dev/null || true)
        [ -z "$state" ] && break
        case "$state" in *Z*) break ;; esac
        sleep 0.05
    done
    if ! wait "$SRV"; then die "$tag 服务端非零退出：$(tail -5 "$R/$tag.srv.log")"; fi
    SRV=""
    if ! awk '
        /^shutdown: / {
            n++
            for (i = 1; i <= NF; i++) {
                if ($i == "rounds") r=$(i+1)
                if ($i == "worst") w=$(i+1)
            }
        }
        END { if (n != 1 || r == "" || w == "") exit 1; print r, w }
    ' "$R/$tag.srv.log" > "$R/$tag.report"; then
        die "$tag 停服日志没有唯一的 rounds/worst 汇总：$(tail -5 "$R/$tag.srv.log")"
    fi
    read -r STOP_ROUNDS STOP_WORST < "$R/$tag.report"
    uint "$tag rounds" "$STOP_ROUNDS"
    uint "$tag worst_round_us" "$STOP_WORST"
}
start_rss() {
    local tag=$1
    : > "$R/rss.tsv"
    printf 'run\n' > "$R/rss.state"
    (
        while [ "$(cat "$R/rss.state" 2>/dev/null)" = run ]; do
            if [ -r "/proc/$SRV/status" ]; then
                rss=$(awk '/^VmRSS:/ {print $2}' "/proc/$SRV/status")
                hwm=$(awk '/^VmHWM:/ {print $2}' "/proc/$SRV/status")
                printf '%s\t%s\t%s\n' "$(date +%s%N)" "${rss:-0}" "${hwm:-0}" >> "$R/rss.tsv"
            fi
            sleep "$RSS_INTERVAL"
        done
    ) > "$R/$tag.rss-monitor.log" 2>&1 &
    MON=$!
}
stop_rss() {
    printf 'stop\n' > "$R/rss.state"
    wait "$MON"
    MON=""
}
cleanup() {
    if [ -n "$MON" ]; then
        printf 'stop\n' > "$R/rss.state"
        wait "$MON" 2>/dev/null || true
        MON=""
    fi
    if [ -n "$SRV" ]; then
        kill -TERM "$SRV" 2>/dev/null || true
        wait "$SRV" 2>/dev/null || true
        SRV=""
    fi
}
trap cleanup EXIT

# 一格：在已灌好、且刚重开过的服务端上打 SECS 秒混合负载。
run_cell() {
    local nv=$1 conns=$2 dir=$3
    local tag="v${nv}c${conns}"
    local line groups ops records wrt sy look dirs crea unln
    local p50 p90 p99 pmax lat_samples touched gone kept samples
    local ck0 ck_delta wal0 wal_end wal_peak snap0 snap1
    local time_ms rss_before hwm_before rss_after hwm_after rss_peak rss_samples retained
    local nodes_left dentries_left locks_left refs_left errs worst rounds ops_per_s

    start_server "$dir" "$tag"
    read -r rss_before hwm_before < <(rss_now)
    start_rss "$tag"
    if ! "$BIN/treedb-bench" --addr "$ADDR" --bulk mvmix --views "$nv" --rows "$ROWS" \
        --conns "$conns" --pipeline "$PIPE" --secs "$SECS" --sample-ms "$SAMPLE_MS" \
        > "$R/$tag.bench.log" 2>&1; then
        stop_rss
        die "$tag mvmix 失败：$(tail -8 "$R/$tag.bench.log")"
    fi
    line=$(grep -m1 '^bulk mode=mvmix ' "$R/$tag.bench.log" || true)
    [ -n "$line" ] || die "$tag 没输出 mvmix 汇总"
    say "   $line"

    groups=$(field "$line" groups); ops=$(field "$line" ops)
    time_ms=$(field "$line" time_ms); records=$(field "$line" records)
    wrt=$(field "$line" writes); sy=$(field "$line" syncs)
    look=$(field "$line" lookups); dirs=$(field "$line" readdirs)
    crea=$(field "$line" creates); unln=$(field "$line" unlinks)
    p50=$(field "$line" lat_p50_us); p90=$(field "$line" lat_p90_us)
    p99=$(field "$line" lat_p99_us); pmax=$(field "$line" lat_max_us)
    lat_samples=$(field "$line" lat_samples); touched=$(field "$line" views_touched)
    gone=$(field "$line" gone_ok); kept=$(field "$line" kept_ok)
    ck0=$(field "$line" checkpoints_start); ck_delta=$(field "$line" checkpoints)
    wal0=$(field "$line" wal_start_bytes); wal_end=$(field "$line" wal_end_bytes)
    wal_peak=$(field "$line" wal_peak_bytes)
    snap0=$(field "$line" snap_seq_start); snap1=$(field "$line" snap_seq_end)
    samples=$(field "$line" samples)
    nodes_left=$(field "$line" nodes_left); dentries_left=$(field "$line" dentries_left)
    locks_left=$(field "$line" locks_left); refs_left=$(field "$line" refs_left)
    errs=$(field "$line" errors); ops_per_s=$(field "$line" ops_per_s)
    for pair in "groups:$groups" "ops:$ops" "time_ms:$time_ms" "records:$records" \
        "writes:$wrt" "syncs:$sy" "lookups:$look" "readdirs:$dirs" "creates:$crea" \
        "unlinks:$unln" "lat_p50:$p50" "lat_p90:$p90" "lat_p99:$p99" "lat_max:$pmax" \
        "lat_samples:$lat_samples" "views_touched:$touched" "gone_ok:$gone" "kept_ok:$kept" \
        "checkpoints_start:$ck0" "checkpoints_delta:$ck_delta" "wal_start:$wal0" \
        "wal_end:$wal_end" "wal_peak:$wal_peak" "snap_seq_start:$snap0" "snap_seq_end:$snap1" \
        "samples:$samples" "nodes_left:$nodes_left" "dentries_left:$dentries_left" \
        "locks_left:$locks_left" "refs_left:$refs_left" "errors:$errs" "ops_per_s:$ops_per_s"; do
        uint "$tag ${pair%%:*}" "${pair#*:}"
    done
    [ "$errs" = 0 ] || die "$tag errors=$errs"
    [ "$groups" -gt 0 ] || die "$tag 一个组都没跑完"
    [ "$ops" = $((5 * groups)) ] || die "$tag ops=$ops，应为 $((5 * groups))"
    [ "$records" = $((5 * groups)) ] || die "$tag records=$records，应为 $((5 * groups))"
    [ "$wrt" = $((2 * groups)) ] || die "$tag writes=$wrt，应为 $((2 * groups))"
    [ "$look" = $((2 * groups)) ] || die "$tag lookups=$look，应为 $((2 * groups))"
    [ "$dirs" = "$groups" ] && [ "$crea" = "$groups" ] && [ "$unln" = "$groups" ] ||
        die "$tag 读/写配比 $dirs/$crea/$unln，应为 $groups/$groups/$groups"
    [ "$lat_samples" = "$ops" ] || die "$tag 延迟样本 $lat_samples，应为 $ops"
    [ "$touched" = "$(min2 "$nv" "$groups")" ] ||
        die "$tag views_touched=$touched，应为 $(min2 "$nv" "$groups")"
    local want_ids=$(min2 "$groups" 400)
    [ "$gone" = "$want_ids" ] && [ "$kept" = "$want_ids" ] ||
        die "$tag 名字级核对 gone=$gone kept=$kept，应为 $want_ids"
    [ "$nodes_left" = $((nv * (ROWS + 1))) ] || die "$tag nodes_left=$nodes_left"
    [ "$dentries_left" = $((nv * ROWS)) ] || die "$tag dentries_left=$dentries_left"
    [ "$locks_left" = 0 ] && [ "$refs_left" = 0 ] ||
        die "$tag 终态 locks=$locks_left refs=$refs_left"
    [ "$sy" -gt 0 ] || die "$tag syncs=0"
    [ "$p50" -gt 0 ] && [ "$p99" -ge "$p50" ] || die "$tag 分位延迟不成序"
    if [ "$SAMPLE_MS" -gt 0 ]; then
        [ "$samples" -gt 1 ] || die "$tag Stats 采样点只有 $samples"
    fi
    [ "$wal_peak" -ge "$wal_end" ] || die "$tag WAL 峰值 $wal_peak 小于窗口末 $wal_end"
    [ "$snap1" -ge "$snap0" ] || die "$tag 快照水位倒退 $snap0→$snap1"
    [ "$ck_delta" -eq $(( $(field "$line" checkpoints_end) - ck0 )) ] ||
        die "$tag checkpoint 差值不自洽"
    if [ "$AUTO" = 1 ]; then
        [ "$snap1" -gt "$snap0" ] || die "$tag 自动 checkpoint 开着却没推进快照水位"
    fi

    stop_rss
    read -r rss_after hwm_after < <(rss_now)
    read -r rss_peak hwm_peak rss_samples < <(awk -F '\t' '
        NR == 1 {rmax=$2; hmax=$3}
        $2 > rmax {rmax=$2} $3 > hmax {hmax=$3}
        END {print rmax, hmax, NR}
    ' "$R/rss.tsv")
    for pair in "rss_before:$rss_before" "hwm_before:$hwm_before" "rss_after:$rss_after" \
        "hwm_after:$hwm_after" "rss_peak:$rss_peak" "rss_samples:$rss_samples"; do
        uint "$tag ${pair%%:*}" "${pair#*:}"
    done
    [ "$rss_samples" -gt 1 ] && [ "$rss_peak" -gt 0 ] && [ "$hwm_after" -ge "$hwm_before" ] ||
        die "$tag RSS 采样无效"
    retained=$((rss_after - rss_before))

    stop_server "$tag"
    worst=$STOP_WORST
    rounds=$STOP_ROUNDS
    say "   shutdown rounds=$rounds worst_round_us=$worst"
    [ "$rounds" -gt 0 ] || die "$tag rounds=0"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$nv" "$ROWS" "$conns" "$PIPE" "$SECS" "$AUTO" "$SEED_TIME_MS" "$SEED_RECORDS" \
        "$time_ms" "$groups" "$ops" "$records" "$wrt" "$sy" "$look" "$dirs" "$crea" "$unln" \
        "$p50" "$p90" "$p99" "$pmax" "$lat_samples" "$touched" "$gone" "$kept" "$ck0" \
        "$ck_delta" "$wal0" "$wal_end" "$wal_peak" "$snap0" "$snap1" "$rss_before" \
        "$rss_after" "$rss_peak" "$hwm_after" "$rss_samples" "$worst" "$rounds" \
        "$nodes_left" "$dentries_left" "$locks_left" "$refs_left" "$ops_per_s" "$errs" \
        >> "$TSV"
    say "   PASS $tag: groups=$groups ops=$ops records=$records writes=$wrt syncs=$sy"
    say "   lat_us p50/p90/p99/max=$p50/$p90/$p99/$pmax samples=$lat_samples ops_per_s=$ops_per_s worst_round_us=$worst"
    say "   RSS KiB before/after=$rss_before/$rss_after retained_delta=$retained peak=$rss_peak HWM=$hwm_after samples=$rss_samples"
    say "   checkpoint=$ck0→$((ck0 + ck_delta)) wal=$wal0→$wal_end(peak $wal_peak) snap_seq=$snap0→$snap1 touched=$touched/$nv"
}

say "== 万级混合视图并发曲线 $(date '+%F %T') =="
say "HEAD=$(git rev-parse HEAD) views=[$VIEWS] rows=$ROWS conns=[$CONNS] pipeline=$PIPE secs=$SECS workers=$WORKERS auto_checkpoint_mb=$AUTO sample_ms=$SAMPLE_MS"
CODE_SHA=$(sha256sum src/bin/bench.rs benches/many_views_mix.sh benches/many_views.sh | sha256sum | awk '{print $1}')
say "code_sha256=$CODE_SHA"
say "临时现场=$R"

for nv in $VIEWS; do
    dir="$R/v$nv-data"
    say ""
    say "-- views=$nv（seed 与测量分属两个服务端生命周期）"
    start_server "$dir" "v$nv-seed"
    if ! "$BIN/treedb-bench" --addr "$ADDR" --bulk mvseed --views "$nv" --rows "$ROWS" \
        > "$R/v$nv.seed.log" 2>&1; then
        die "views=$nv seed 失败：$(tail -5 "$R/v$nv.seed.log")"
    fi
    seed_line=$(grep -m1 '^mvseed ' "$R/v$nv.seed.log" || true)
    [ -n "$seed_line" ] || die "views=$nv 没输出 mvseed 汇总"
    say "   $seed_line"
    s_err=$(field "$seed_line" errors)
    s_created=$(field "$seed_line" created)
    s_dirs=$(field "$seed_line" dirs_ok)
    s_rec=$(field "$seed_line" records)
    s_wr=$(field "$seed_line" writes)
    s_ops=$(field "$seed_line" ops)
    s_nodes=$(field "$seed_line" nodes)
    s_dent=$(field "$seed_line" dentries)
    SEED_TIME_MS=$(field "$seed_line" time_ms)
    SEED_RECORDS=$s_rec
    for pair in "seed_errors:$s_err" "seed_created:$s_created" "seed_dirs:$s_dirs" \
        "seed_records:$s_rec" "seed_writes:$s_wr" "seed_ops:$s_ops" "seed_nodes:$s_nodes" \
        "seed_dentries:$s_dent" "seed_time_ms:$SEED_TIME_MS"; do
        uint "views=$nv ${pair%%:*}" "${pair#*:}"
    done
    [ "$s_err" = 0 ] || die "views=$nv seed errors=$s_err"
    [ "$s_created" = $((nv * ROWS)) ] || die "views=$nv 建了 $s_created 个文件，应为 $((nv * ROWS))"
    [ "$s_dirs" = "$nv" ] || die "views=$nv 只有 $s_dirs 个视图翻页数满"
    [ "$s_ops" = $((nv * (ROWS + 1))) ] || die "views=$nv seed ops=$s_ops"
    [ "$s_rec" = $((2 * nv + 3 * nv * ROWS)) ] ||
        die "views=$nv seed records=$s_rec，应为 $((2 * nv + 3 * nv * ROWS))"
    [ "$s_wr" = $((nv + nv * ROWS)) ] || die "views=$nv seed writes=$s_wr，应为 $((nv + nv * ROWS))"
    [ "$s_nodes" = $((nv * (ROWS + 1))) ] || die "views=$nv seed nodes=$s_nodes"
    [ "$s_dent" = $((nv * ROWS)) ] || die "views=$nv seed dentries=$s_dent"
    stop_server "v$nv-seed"
    say "   seed 停服 rounds=$STOP_ROUNDS worst_round_us=$STOP_WORST（不计入混合档）"
    for c in $CONNS; do
        run_cell "$nv" "$c" "$dir"
    done
done

say ""
say "== 并发曲线（同一视图档内 conns 递增）=="
awk -F '\t' '
    NR == 1 { for (i = 1; i <= NF; i++) col[$i] = i; next }
    {
        printf "   views=%-6s conns=%-3s ops/s=%-8s p50=%-6s p99=%-6s max=%-7s touched=%s RSS峰值=%s KiB\n",
            $(col["views"]), $(col["conns"]), $(col["ops_per_s"]),
            $(col["lat_p50_us"]), $(col["lat_p99_us"]), $(col["lat_max_us"]),
            $(col["views_touched"]), $(col["rss_peak_kb"])
    }' "$TSV" | tee -a "$OUT"
say ""
say "== PASS：万级视图灌种记录数、混合读写配比、账本净零、名字级核对、RSS 与 worst_round_us 全部通过 =="
say "逐格结果=$TSV"
