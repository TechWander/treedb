#!/bin/bash
# POSIX 固定删除风暴：N 个 Unlink + N 个 Rmdir，自动 checkpoint 开/关两档。
#
# 与 delete_storm.sh 的分界：那个量具打通用表 RowDelete/churn；这个量具打真实
# 目录树，每个文件带唯一 data_ref，验收 nodes/dentries/locks/refs 精确回到基线。
# 造数据与删除测量分开启服，确保停服日志里的 worst_round_us 不混入 seed 成本。
#
# 用法：
#   bash benches/posix_delete_storm.sh
#   ROWS=200000 CONNS=8 PIPE=64 WORKERS=8 bash benches/posix_delete_storm.sh
#   AUTO_MODES="0 1" bash benches/posix_delete_storm.sh
#
# 输出：
#   benches/results/posix_delete_storm.log  人读摘要与本轮配置
#   benches/results/posix_delete_storm.tsv  机器可读逐格结果
#   .spot/posix-delete-storm-<随机>/        服务端日志、RSS 采样、bench/指标原行
#
# 硬断言：2N 个 op、6N 条 WAL 记录、2N 次写、N 个非空 NodeRef、N 个 ref 归零；
# nodes/dentries/locks/refs 回到基线；p50/p99 样本数=2N；自动档必须新增快照，
# 关闭档必须既不新增 checkpoint 也不推进快照水位；优雅停服后重开逐 ref 核对。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
BIN=./target/release
ROWS=${ROWS:-100000}
CONNS=${CONNS:-8}
PIPE=${PIPE:-64}
WORKERS=${WORKERS:-8}
SAMPLE_MS=${SAMPLE_MS:-5}
RSS_INTERVAL=${RSS_INTERVAL:-0.02}
AUTO_MODES=${AUTO_MODES:-"0 1"}
OUT=benches/results/posix_delete_storm.log
TSV=benches/results/posix_delete_storm.tsv

for b in treedb-server treedb-bench; do
    [ -x "$BIN/$b" ] || { echo "缺 $BIN/$b，先跑 cargo build --release --bins"; exit 1; }
done
case "$ROWS:$CONNS:$PIPE:$WORKERS:$SAMPLE_MS" in
    *[!0-9:]*) echo "ROWS/CONNS/PIPE/WORKERS/SAMPLE_MS 必须是整数"; exit 2 ;;
esac
[ "$ROWS" -gt 0 ] && [ "$CONNS" -gt 0 ] && [ "$PIPE" -gt 0 ] \
    && [ "$WORKERS" -gt 0 ] && [ "$SAMPLE_MS" -gt 0 ] || {
    echo "ROWS/CONNS/PIPE/WORKERS/SAMPLE_MS 必须 > 0"; exit 2;
}
for auto in $AUTO_MODES; do
    case "$auto" in 0|1) ;; *) echo "AUTO_MODES 只能包含 0/1：'$AUTO_MODES'"; exit 2 ;; esac
done
[ -n "$AUTO_MODES" ] || { echo "AUTO_MODES 不能为空"; exit 2; }
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
R=$(mktemp -d "$ROOT/.spot/posix-delete-storm-XXXXXX")
SRV=""
MON=""
STOP_ROUNDS=0
STOP_WORST=0
: > "$OUT"
printf '%s\n' \
    'auto_mb	rows	ops	time_ms	records	writes	syncs	lat_p50_us	lat_p90_us	lat_p99_us	lat_max_us	lat_samples	freed_refs	verify_refs	checkpoints_start	checkpoints_auto	wal_start	wal_peak	wal_end	wal_after_auto	snap_seq_start	snap_seq_auto	snap_bytes_auto	snap_bytes_shutdown	rss_before_kb	rss_after_kb	rss_reopen_kb	rss_peak_kb	hwm_after_kb	hwm_peak_kb	rss_samples	worst_round_us	rounds	nodes_left	dentries_left	locks_left	refs_left	nodes_reopen	dentries_reopen	locks_reopen	refs_reopen	verify_checked_reopen	errors' \
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
metric() {
    awk -v k="$2" '$1 == k { print $2; exit }' "$1"
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
snapshot() {
    local tag=$1 up
    "$BIN/treedb-server" --listen "$ADDR" --stats --format prometheus > "$R/$tag.metrics"
    up=$(metric "$R/$tag.metrics" treedb_up)
    uint "$tag treedb_up" "$up"
    [ "$up" = 1 ] || die "$tag 指标抓取不是 up"
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

start_server() {
    local dir=$1 tag=$2 auto=$3 i=0
    mkdir -p "$dir"
    "$BIN/treedb-server" --dir "$dir" --listen "$ADDR" --workers "$WORKERS" \
        --durability group --auto-checkpoint-mb "$auto" > "$R/$tag.srv.log" 2>&1 &
    SRV=$!
    until "$BIN/treedb-server" --listen "$ADDR" --stats > /dev/null 2>&1; do
        i=$((i + 1))
        [ "$i" -lt 400 ] || die "$tag 服务端 20s 内没就绪：$(tail -2 "$R/$tag.srv.log")"
        sleep 0.05
    done
    snapshot "$tag"
}
stop_server() {
    local tag=$1 state i
    "$BIN/treedb-server" --listen "$ADDR" --shutdown > "$R/$tag.shutdown.log" 2>&1 \
        || die "$tag 停服没拿到 ack：$(cat "$R/$tag.shutdown.log")"
    for i in $(seq 400); do
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

run_phase() {
    local auto=$1
    local tag="auto$auto"
    local dir="$R/$tag-data"
    local seed_line line
    local ops done total time rec wrt sy errs left_nodes left_dentries left_locks left_refs
    local p50 p90 p99 max lat_samples freed verify samples
    local ck0 ck_auto snap0 snap_auto snap_bytes_auto wal0 wal_peak wal_end wal_auto
    local snap_bytes_start snap_bytes_shutdown seq_before wal_before
    local rss_before hwm_before rss_after hwm_after rss_reopen rss_peak hwm_peak rss_samples
    local nodes_reopen dentries_reopen locks_reopen refs_reopen snap_reopen wal_reopen
    local verify_line verify_checked retained worst_round rounds

    say ""
    say "-- $tag: POSIX Unlink/Rmdir rows=$ROWS conns=$CONNS pipeline=$PIPE workers=$WORKERS auto_checkpoint_mb=$auto"
    start_server "$dir" "$tag-seed" "$auto"
    if ! "$BIN/treedb-bench" --addr "$ADDR" --bulk posixseed --rows "$ROWS" \
        > "$R/$tag.seed.log" 2>&1; then
        die "$tag seed 失败：$(tail -5 "$R/$tag.seed.log")"
    fi
    seed_line=$(grep -m1 '^posixseed ' "$R/$tag.seed.log" || true)
    [ -n "$seed_line" ] || die "$tag 没输出 posixseed 汇总"
    say "   $seed_line"
    [ "$(field "$seed_line" errors)" = 0 ] || die "$tag seed errors 非零"
    [ "$(field "$seed_line" nodes)" = $((1 + 2 * ROWS)) ] || die "$tag seed nodes 不符"
    [ "$(field "$seed_line" dentries)" = $((2 * ROWS)) ] || die "$tag seed dentries 不符"
    [ "$(field "$seed_line" refs)" = "$ROWS" ] || die "$tag seed refs 不符"
    stop_server "$tag-seed"

    # 重启后 WAL 已被 seed 的优雅停服 checkpoint 收干净；删除窗口从这里开始。
    start_server "$dir" "$tag-delete" "$auto"
    read -r rss_before hwm_before < <(rss_now)
    start_rss "$tag"
    if ! "$BIN/treedb-bench" --addr "$ADDR" --bulk posixdelete --rows "$ROWS" \
        --conns "$CONNS" --pipeline "$PIPE" --sample-ms "$SAMPLE_MS" \
        > "$R/$tag.bench.log" 2>&1; then
        stop_rss
        die "$tag bench 失败：$(tail -8 "$R/$tag.bench.log")"
    fi
    line=$(grep -m1 '^bulk ' "$R/$tag.bench.log" || true)
    [ -n "$line" ] || die "$tag 没输出 bulk 汇总"
    say "   $line"

    ops=$(field "$line" ops); done=${ops%%/*}; total=${ops#*/}
    time=$(field "$line" time_ms); rec=$(field "$line" records); wrt=$(field "$line" writes)
    sy=$(field "$line" syncs); errs=$(field "$line" errors)
    p50=$(field "$line" lat_p50_us); p90=$(field "$line" lat_p90_us)
    p99=$(field "$line" lat_p99_us); max=$(field "$line" lat_max_us)
    lat_samples=$(field "$line" lat_samples); freed=$(field "$line" freed_refs)
    verify=$(field "$line" verify_refs); samples=$(field "$line" samples)
    left_nodes=$(field "$line" nodes_left); left_dentries=$(field "$line" dentries_left)
    left_locks=$(field "$line" locks_left); left_refs=$(field "$line" refs_left)
    ck0=$(field "$line" checkpoints_start); wal0=$(field "$line" wal_start_bytes)
    wal_peak=$(field "$line" wal_peak_bytes); wal_end=$(field "$line" wal_end_bytes)
    snap0=$(field "$line" snap_seq_start); snap_bytes_start=$(field "$line" snap_bytes_start)
    for pair in "ops:$done" "total:$total" "time_ms:$time" "records:$rec" "writes:$wrt" \
        "syncs:$sy" "errors:$errs" "lat_p50:$p50" "lat_p90:$p90" "lat_p99:$p99" \
        "lat_max:$max" "lat_samples:$lat_samples" "freed_refs:$freed" "verify_refs:$verify" \
        "samples:$samples" "nodes_left:$left_nodes" "dentries_left:$left_dentries" \
        "locks_left:$left_locks" "refs_left:$left_refs" "checkpoints_start:$ck0" \
        "wal_start:$wal0" "wal_peak:$wal_peak" "wal_end:$wal_end" "snap_seq_start:$snap0" \
        "snap_bytes_start:$snap_bytes_start"; do
        uint "$tag ${pair%%:*}" "${pair#*:}"
    done
    [ "$errs" = 0 ] || die "$tag errors=$errs"
    [ "$done" = $((2 * ROWS)) ] && [ "$total" = $((2 * ROWS)) ] || die "$tag op $done/$total 不符"
    [ "$rec" = $((6 * ROWS)) ] || die "$tag records=$rec，应为 $((6 * ROWS))"
    [ "$wrt" = $((2 * ROWS)) ] || die "$tag writes=$wrt，应为 $((2 * ROWS))"
    [ "$freed" = "$ROWS" ] && [ "$verify" = "$ROWS" ] || die "$tag NodeRef/ref 核对 $freed/$verify"
    [ "$lat_samples" = $((2 * ROWS)) ] || die "$tag 延迟样本 $lat_samples，应为 $((2 * ROWS))"
    [ "$left_nodes" = 1 ] && [ "$left_dentries" = 0 ] && [ "$left_locks" = 0 ] \
        && [ "$left_refs" = 0 ] || die "$tag 账本终态 $left_nodes/$left_dentries/$left_locks/$left_refs 不符"
    [ "$samples" -gt 1 ] || die "$tag Stats 采样点只有 $samples"
    [ "$wal0" = 0 ] || die "$tag 删除起点 WAL 不为 0：$wal0"
    [ "$wal_peak" -gt 0 ] && [ "$wal_peak" -ge "$wal_end" ] || die "$tag WAL 峰值无效"

    if [ "$auto" = 1 ]; then
        ck_auto=$ck0
        seq_auto=0
        snap_auto=0
        snap_bytes_auto=0
        wal_auto=0
        for _ in $(seq 600); do
            snapshot "$tag-auto"
            ck_auto=$(metric "$R/$tag-auto.metrics" treedb_checkpoints_total)
            uint "$tag checkpoints_auto" "$ck_auto"
            if [ "$ck_auto" -gt "$ck0" ]; then
                snap_auto=$(metric "$R/$tag-auto.metrics" treedb_snapshot_seq)
                snap_bytes_auto=$(metric "$R/$tag-auto.metrics" treedb_snapshot_bytes)
                wal_auto=$(metric "$R/$tag-auto.metrics" treedb_wal_logical_bytes)
                uint "$tag snap_seq_auto" "$snap_auto"
                uint "$tag snap_bytes_auto" "$snap_bytes_auto"
                uint "$tag wal_after_auto" "$wal_auto"
                break
            fi
            sleep 0.05
        done
        [ "$ck_auto" -gt "$ck0" ] || die "$tag 30s 内没有新增自动 checkpoint"
        [ "$snap_auto" -gt "$snap0" ] || die "$tag 自动快照水位没前进"
        [ "$snap_bytes_auto" -gt 0 ] || die "$tag 自动快照为空"
        [ "$wal_auto" -lt "$wal_peak" ] || die "$tag checkpoint 后 WAL $wal_auto 没低于峰值 $wal_peak"
    else
        # 关闭档观察 500ms：没有后台任务可等，必须保持 checkpoint/水位/WAL 不动。
        sleep 0.5
        snapshot "$tag-auto"
        ck_auto=$(metric "$R/$tag-auto.metrics" treedb_checkpoints_total)
        snap_auto=$(metric "$R/$tag-auto.metrics" treedb_snapshot_seq)
        snap_bytes_auto=$(metric "$R/$tag-auto.metrics" treedb_snapshot_bytes)
        wal_auto=$(metric "$R/$tag-auto.metrics" treedb_wal_logical_bytes)
        for pair in "checkpoints_auto:$ck_auto" "snap_seq_auto:$snap_auto" \
            "snap_bytes_auto:$snap_bytes_auto" "wal_after_auto:$wal_auto"; do
            uint "$tag ${pair%%:*}" "${pair#*:}"
        done
        [ "$ck_auto" = "$ck0" ] || die "$tag auto=0 仍新增 checkpoint：$ck0→$ck_auto"
        [ "$snap_auto" = "$snap0" ] || die "$tag auto=0 仍推进快照水位：$snap0→$snap_auto"
        [ "$wal_auto" = "$wal_end" ] || die "$tag auto=0 仍改写 WAL：$wal_end→$wal_auto"
    fi

    snapshot "$tag-before-shutdown"
    seq_before=$(metric "$R/$tag-before-shutdown.metrics" treedb_seq)
    wal_before=$(metric "$R/$tag-before-shutdown.metrics" treedb_wal_logical_bytes)
    uint "$tag seq_before_shutdown" "$seq_before"
    uint "$tag wal_before_shutdown" "$wal_before"
    stop_rss
    read -r rss_after hwm_after < <(rss_now)
    read -r rss_peak hwm_peak rss_samples < <(awk -F '\t' '
        NR == 1 {rmin=$2; rmax=$2; hmax=$3}
        $2 < rmin {rmin=$2} $2 > rmax {rmax=$2} $3 > hmax {hmax=$3}
        END {print rmax, hmax, NR}
    ' "$R/rss.tsv")
    for pair in "rss_before:$rss_before" "hwm_before:$hwm_before" "rss_after:$rss_after" \
        "hwm_after:$hwm_after" "rss_peak:$rss_peak" "hwm_peak:$hwm_peak" "rss_samples:$rss_samples"; do
        uint "$tag ${pair%%:*}" "${pair#*:}"
    done
    [ "$rss_samples" -gt 1 ] && [ "$rss_peak" -gt 0 ] && [ "$hwm_after" -ge "$hwm_before" ] \
        || die "$tag RSS 采样无效"
    retained=$((rss_after - rss_before))

    # 优雅停服会显式 checkpoint；它的 Report 给出本次删除生命周期的 worst round。
    stop_server "$tag-delete"
    worst_round=$STOP_WORST
    rounds=$STOP_ROUNDS
    say "   shutdown rounds=$rounds worst_round_us=$worst_round"
    [ "$rounds" -gt 0 ] || die "$tag rounds=0"

    start_server "$dir" "$tag-reopen" "$auto"
    snapshot "$tag-reopen"
    nodes_reopen=$(metric "$R/$tag-reopen.metrics" treedb_nodes)
    dentries_reopen=$(metric "$R/$tag-reopen.metrics" treedb_dentries)
    locks_reopen=$(metric "$R/$tag-reopen.metrics" treedb_locks)
    refs_reopen=$(metric "$R/$tag-reopen.metrics" treedb_refs)
    snap_reopen=$(metric "$R/$tag-reopen.metrics" treedb_snapshot_seq)
    wal_reopen=$(metric "$R/$tag-reopen.metrics" treedb_wal_logical_bytes)
    snap_bytes_shutdown=$(metric "$R/$tag-reopen.metrics" treedb_snapshot_bytes)
    for pair in "nodes_reopen:$nodes_reopen" "dentries_reopen:$dentries_reopen" \
        "locks_reopen:$locks_reopen" "refs_reopen:$refs_reopen" "snap_seq_reopen:$snap_reopen" \
        "wal_reopen:$wal_reopen" "snap_bytes_shutdown:$snap_bytes_shutdown"; do
        uint "$tag ${pair%%:*}" "${pair#*:}"
    done
    [ "$nodes_reopen" = 1 ] && [ "$dentries_reopen" = 0 ] && [ "$locks_reopen" = 0 ] \
        && [ "$refs_reopen" = 0 ] || die "$tag 重开账本未回到基线"
    [ "$snap_reopen" = "$seq_before" ] || die "$tag 停服快照水位 $snap_reopen != seq $seq_before"
    [ "$wal_reopen" = 0 ] || die "$tag 优雅停服后 WAL 不为 0：$wal_reopen"
    [ "$snap_bytes_shutdown" -gt 0 ] || die "$tag 停服快照为空"
    if ! "$BIN/treedb-bench" --addr "$ADDR" --bulk posixverify --rows "$ROWS" \
        > "$R/$tag.verify.log" 2>&1; then
        die "$tag 重开核对失败：$(tail -5 "$R/$tag.verify.log")"
    fi
    verify_line=$(grep -m1 '^posixverify ' "$R/$tag.verify.log" || true)
    [ -n "$verify_line" ] || die "$tag 没输出 posixverify 汇总"
    verify_checked=$(field "$verify_line" checked_refs)
    uint "$tag verify_checked_reopen" "$verify_checked"
    [ "$verify_checked" = "$ROWS" ] || die "$tag 重开只核对了 $verify_checked/$ROWS 个 ref"
    say "   $verify_line"
    read -r rss_reopen _ < <(rss_now)
    uint "$tag rss_reopen" "$rss_reopen"
    stop_server "$tag-reopen"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$auto" "$ROWS" "$done" "$time" "$rec" "$wrt" "$sy" "$p50" "$p90" "$p99" "$max" \
        "$lat_samples" "$freed" "$verify" "$ck0" "$ck_auto" "$wal0" "$wal_peak" "$wal_end" \
        "$wal_auto" "$snap0" "$snap_auto" "$snap_bytes_auto" "$snap_bytes_shutdown" \
        "$rss_before" "$rss_after" "$rss_reopen" "$rss_peak" "$hwm_after" "$hwm_peak" \
        "$rss_samples" "$worst_round" "$rounds" "$left_nodes" "$left_dentries" "$left_locks" \
        "$left_refs" "$nodes_reopen" "$dentries_reopen" "$locks_reopen" "$refs_reopen" \
        "$verify_checked" "$errs" >> "$TSV"
    say "   PASS $tag: ops=$done records=$rec writes=$wrt freed_refs=$freed verify_refs=$verify"
    say "   checkpoint=$ck0→$ck_auto snap_seq=$snap0→$snap_auto wal=$wal_peak→$wal_auto snap_bytes=$snap_bytes_auto/$snap_bytes_shutdown"
    say "   lat_us p50/p90/p99/max=$p50/$p90/$p99/$max samples=$lat_samples worst_round_us=$worst_round"
    say "   RSS KiB before/after/reopen=$rss_before/$rss_after/$rss_reopen retained_delta=$retained peak=$rss_peak HWM=$hwm_peak samples=$rss_samples"
}

say "== POSIX 删除风暴联合量具 $(date '+%F %T') =="
say "HEAD=$(git rev-parse HEAD) rows=$ROWS conns=$CONNS pipeline=$PIPE workers=$WORKERS auto_modes=$AUTO_MODES sample_ms=$SAMPLE_MS"
CODE_SHA=$(sha256sum src/bin/bench.rs src/engine/apply.rs benches/delete_storm.sh benches/posix_delete_storm.sh tests/delete_storm.rs | sha256sum | awk '{print $1}')
say "code_sha256=$CODE_SHA"
say "临时现场=$R"
for auto in $AUTO_MODES; do
    run_phase "$auto"
done
say ""
say "== PASS：POSIX 固定工作量、账本收敛、p50/p99、worst_round_us、自动 checkpoint 开关与重开核对全部通过 =="
say "逐格结果=$TSV"
