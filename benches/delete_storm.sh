#!/bin/bash
# 固定删除风暴 + 重复删后重插 + 真服务端 RSS + 自动 checkpoint + 重启恢复。
#
# 与 bench_bulk.sh 的分界：那一档关掉自动 checkpoint，只比较删除形状本身；这一档
# 故意打开 1 MiB 自动档，回答「删完之后系统是否真的收口、内存留下多少、重开是否
# 恢复」。两档都是固定工作量，不用跑满 N 秒的 ops/s 口径。
#
# 用法：
#   bash benches/delete_storm.sh
#   ROWS=200000 CYCLES=4 WORKERS=8 bash benches/delete_storm.sh
#
# 输出：
#   benches/results/delete_storm.log  人读摘要与本轮配置
#   benches/results/delete_storm.tsv  机器可读逐格结果
#   .spot/delete-storm-<随机>/         服务端日志、RSS 采样、每档 bench/指标原行
#
# 硬断言：实际 op 数、records/writes、终态行数、自动 checkpoint 次数与快照水位、
# WAL 从采样峰值回落、RSS 采样非空、SIGKILL 后重开行数/逐行内容。优雅停服会主动
# checkpoint 并把尾巴清零，所以重开这一段故意用 kill -9。任一失败非零退出。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
BIN=./target/release
ROWS=${ROWS:-100000}
CYCLES=${CYCLES:-4}
CONNS=${CONNS:-8}
PIPE=${PIPE:-64}
WORKERS=${WORKERS:-8}
AUTO_MB=${AUTO_MB:-1}
SAMPLE_MS=${SAMPLE_MS:-5}
RSS_INTERVAL=${RSS_INTERVAL:-0.02}
OUT=benches/results/delete_storm.log
TSV=benches/results/delete_storm.tsv

for b in treedb-server treedb-bench; do
    [ -x "$BIN/$b" ] || { echo "缺 $BIN/$b，先跑 cargo build --release --bins"; exit 1; }
done
case "$ROWS:$CYCLES:$CONNS:$PIPE:$WORKERS:$AUTO_MB:$SAMPLE_MS" in
    *[!0-9:]*) echo "ROWS/CYCLES/CONNS/PIPE/WORKERS/AUTO_MB/SAMPLE_MS 必须是整数"; exit 2 ;;
esac
[ "$ROWS" -gt 0 ] && [ "$CYCLES" -gt 0 ] && [ "$CONNS" -gt 0 ] && [ "$PIPE" -gt 0 ] \
    && [ "$WORKERS" -gt 0 ] && [ "$AUTO_MB" -gt 0 ] && [ "$SAMPLE_MS" -gt 0 ] || {
    echo "ROWS/CYCLES/CONNS/PIPE/WORKERS/AUTO_MB/SAMPLE_MS 必须 > 0"; exit 2;
}
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
R=$(mktemp -d "$ROOT/.spot/delete-storm-XXXXXX")
SRV=""
MON=""
: > "$OUT"
printf 'phase\trows\tcycles\tops\ttime_ms\trecords\twrites\tsyncs\tcheckpoints_start\tcheckpoints_auto\twal_start\twal_peak\twal_end\twal_after_auto\ttail_records\twal_before_crash\twal_reopen\tsnap_seq_start\tsnap_seq_auto\tsnap_bytes_auto\trss_before_kb\trss_after_kb\trss_reopen_kb\trss_peak_kb\thwm_after_kb\thwm_peak_kb\trss_samples\trows_reopen\tverify_checked\terrors\n' > "$TSV"

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
    local tag=$1
    local up
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
    local dir=$1 tag=$2
    mkdir -p "$dir"
    "$BIN/treedb-server" --dir "$dir" --listen "$ADDR" --workers "$WORKERS" \
        --durability group --auto-checkpoint-mb "$AUTO_MB" > "$R/$tag.srv.log" 2>&1 &
    SRV=$!
    local i=0
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
}

crash_server() {
    local tag=$1 rc
    kill -KILL "$SRV"
    if wait "$SRV"; then
        die "$tag SIGKILL 后异常零退出"
    else
        rc=$?
        [ "$rc" -eq 137 ] || die "$tag SIGKILL 退出码是 $rc，不是 137"
    fi
    SRV=""
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
    local phase=$1 mode=$2 expected_left=$3
    local dir="$R/$phase-data"
    local tag="$phase"
    local want_ops want_records line done total ops time rec wrt sy errs left shadow
    local ck0 ck_delta ck_end ck_auto seq_auto snap0 snap_auto snap_bytes wal0 wal_peak wal_end wal_auto
    local tail_line tail_records tail_writes tail_left tail_errs tail_wal0 tail_wal1
    local wal_before_crash seq_before_crash ck_before_crash
    local rss_before hwm_before rss_after hwm_after rss_reopen rss_peak hwm_peak samples
    local rows_reopen=0 verify_checked=0 seq_reopen snap_reopen wal_reopen retained
    if [ "$mode" = rowdelete ]; then
        want_ops=$ROWS
        want_records=$ROWS
    else
        want_ops=$((2 * ROWS * CYCLES))
        want_records=$want_ops
    fi

    say ""
    say "-- $phase: mode=$mode rows=$ROWS cycles=$CYCLES workers=$WORKERS auto_checkpoint_mb=$AUTO_MB"
    start_server "$dir" "$tag-start"
    read -r rss_before hwm_before < <(rss_now)
    start_rss "$tag"
    if ! "$BIN/treedb-bench" --addr "$ADDR" --bulk "$mode" --rows "$ROWS" \
        --cycles "$CYCLES" --conns "$CONNS" --pipeline "$PIPE" --sample-ms "$SAMPLE_MS" \
        > "$R/$tag.bench.log" 2>&1; then
        stop_rss
        die "$phase bench 失败：$(tail -5 "$R/$tag.bench.log")"
    fi
    line=$(grep -m1 '^bulk ' "$R/$tag.bench.log" || true)
    [ -n "$line" ] || die "$phase 没输出 bulk 汇总行"
    say "   $line"

    ops=$(field "$line" ops); done=${ops%%/*}; total=${ops#*/}
    time=$(field "$line" time_ms); rec=$(field "$line" records); wrt=$(field "$line" writes)
    sy=$(field "$line" syncs); errs=$(field "$line" errors); left=$(field "$line" rows_left)
    shadow=$(field "$line" shadow_left); samples=$(field "$line" samples)
    ck0=$(field "$line" checkpoints_start); ck_delta=$(field "$line" checkpoints)
    ck_end=$(field "$line" checkpoints_end); wal0=$(field "$line" wal_start_bytes)
    wal_peak=$(field "$line" wal_peak_bytes); wal_end=$(field "$line" wal_end_bytes)
    snap0=$(field "$line" snap_seq_start)
    for pair in "ops:$done" "total:$total" "time_ms:$time" "records:$rec" "writes:$wrt" \
        "syncs:$sy" "errors:$errs" "rows_left:$left" "shadow_left:$shadow" \
        "samples:$samples" "checkpoints_start:$ck0" "checkpoints_delta:$ck_delta" \
        "checkpoints_end:$ck_end" "wal_start:$wal0" "wal_peak:$wal_peak" \
        "wal_end:$wal_end" "snap_seq_start:$snap0"; do
        uint "$phase ${pair%%:*}" "${pair#*:}"
    done
    [ "$ck_end" -eq $((ck0 + ck_delta)) ] || die "$phase checkpoint 起止/增量不自洽"

    [ "$errs" = 0 ] || die "$phase errors=$errs"
    [ "$done" = "$want_ops" ] && [ "$total" = "$want_ops" ] || die "$phase 实际 op $done/$total，应为 $want_ops"
    [ "$rec" = "$want_records" ] || die "$phase records=$rec，应为 $want_records"
    [ "$wrt" = "$want_records" ] || die "$phase writes=$wrt，应为 $want_records"
    [ "$left" = "$expected_left" ] || die "$phase rows_left=$left，应为 $expected_left"
    [ "$shadow" = 0 ] || die "$phase 留着 $shadow 张影子表"
    [ "$samples" -gt 1 ] || die "$phase Stats 采样点只有 $samples"
    [ "$wal_peak" -gt 0 ] && [ "$wal_peak" -ge "$wal_end" ] || die "$phase WAL 峰值无效：$wal_peak/$wal_end"

    # 自动快照可能仍在后台。反复抓 Stats 既观察也驱动事件循环 poll；这里不拿
    # 显式 checkpoint 冒充自动档成功。
    ck_auto=$ck0
    seq_auto=0
    snap_auto=0
    snap_bytes=0
    wal_auto=0
    for _ in $(seq 600); do
        snapshot "$tag-auto"
        ck_auto=$(metric "$R/$tag-auto.metrics" treedb_checkpoints_total)
        uint "$phase checkpoints_auto" "$ck_auto"
        if [ "$ck_auto" -gt "$ck0" ]; then
            seq_auto=$(metric "$R/$tag-auto.metrics" treedb_seq)
            snap_auto=$(metric "$R/$tag-auto.metrics" treedb_snapshot_seq)
            snap_bytes=$(metric "$R/$tag-auto.metrics" treedb_snapshot_bytes)
            wal_auto=$(metric "$R/$tag-auto.metrics" treedb_wal_logical_bytes)
            uint "$phase seq_auto" "$seq_auto"
            uint "$phase snap_seq_auto" "$snap_auto"
            uint "$phase snap_bytes_auto" "$snap_bytes"
            uint "$phase wal_after_auto" "$wal_auto"
            break
        fi
        sleep 0.05
    done
    [ "$ck_auto" -gt "$ck0" ] || die "$phase 30s 内没有新增自动 checkpoint"
    [ "$snap_auto" -gt "$snap0" ] || die "$phase 快照水位没从 $snap0 前进"
    [ "$snap_bytes" -gt 0 ] || die "$phase 快照文件是空的"
    [ "$wal_auto" -lt "$wal_peak" ] || die "$phase checkpoint 后 WAL $wal_auto 没低于峰值 $wal_peak"

    # 自动快照收口后再写两条小记录，终态不变。下一步直接 SIGKILL，重开 WAL 必须
    # 与崩溃前逐字节长度相同，才能证明恢复的是「快照 + 尾巴」而非只有快照。
    if ! "$BIN/treedb-bench" --addr "$ADDR" --bulk tail > "$R/$tag.tail.log" 2>&1; then
        die "$phase 尾巴写入失败：$(tail -5 "$R/$tag.tail.log")"
    fi
    tail_line=$(grep -m1 '^tail ' "$R/$tag.tail.log" || true)
    [ -n "$tail_line" ] || die "$phase 没输出 tail 汇总行"
    say "   $tail_line"
    tail_records=$(field "$tail_line" records); tail_writes=$(field "$tail_line" writes)
    tail_left=$(field "$tail_line" rows_left); tail_errs=$(field "$tail_line" errors)
    tail_wal0=$(field "$tail_line" wal_start_bytes); tail_wal1=$(field "$tail_line" wal_end_bytes)
    for pair in "tail_records:$tail_records" "tail_writes:$tail_writes" "tail_rows:$tail_left" \
        "tail_errors:$tail_errs" "tail_wal_start:$tail_wal0" "tail_wal_end:$tail_wal1"; do
        uint "$phase ${pair%%:*}" "${pair#*:}"
    done
    [ "$tail_records" = 2 ] && [ "$tail_writes" = 2 ] && [ "$tail_errs" = 0 ] \
        && [ "$tail_left" = "$expected_left" ] || die "$phase 尾巴 op/终态不正确"
    [ "$tail_wal1" -gt "$tail_wal0" ] || die "$phase 尾巴没有增加 WAL"
    snapshot "$tag-tail"
    wal_before_crash=$(metric "$R/$tag-tail.metrics" treedb_wal_logical_bytes)
    seq_before_crash=$(metric "$R/$tag-tail.metrics" treedb_seq)
    ck_before_crash=$(metric "$R/$tag-tail.metrics" treedb_checkpoints_total)
    uint "$phase wal_before_crash" "$wal_before_crash"
    uint "$phase seq_before_crash" "$seq_before_crash"
    uint "$phase checkpoints_before_crash" "$ck_before_crash"
    [ "$wal_before_crash" -eq "$tail_wal1" ] || die "$phase 崩溃前 WAL 与 tail 汇总不一致"
    [ "$seq_before_crash" -eq $((seq_auto + 2)) ] || die "$phase 尾巴 seq 不是自动水位 + 2"
    [ "$ck_before_crash" -eq "$ck_auto" ] || die "$phase 小尾巴又触发了 checkpoint"

    stop_rss
    read -r rss_after hwm_after < <(rss_now)
    read -r rss_peak hwm_peak samples < <(awk -F '\t' '
        NR == 1 {rmin=$2; rmax=$2; hmax=$3}
        $2 < rmin {rmin=$2} $2 > rmax {rmax=$2} $3 > hmax {hmax=$3}
        END {print rmax, hmax, NR}
    ' "$R/rss.tsv")
    for pair in "rss_before:$rss_before" "hwm_before:$hwm_before" "rss_after:$rss_after" \
        "hwm_after:$hwm_after" "rss_peak:$rss_peak" "hwm_peak:$hwm_peak" "rss_samples:$samples"; do
        uint "$phase ${pair%%:*}" "${pair#*:}"
    done
    [ "$samples" -gt 1 ] && [ "$rss_peak" -gt 0 ] && [ "$hwm_after" -ge "$hwm_before" ] \
        || die "$phase RSS 采样无效：samples=$samples peak=$rss_peak hwm=$hwm_before/$hwm_after"
    retained=$((rss_after - rss_before))

    crash_server "$tag"
    start_server "$dir" "$tag-reopen"
    snapshot "$tag-reopen"
    rows_reopen=$(metric "$R/$tag-reopen.metrics" treedb_table_rows)
    seq_reopen=$(metric "$R/$tag-reopen.metrics" treedb_seq)
    snap_reopen=$(metric "$R/$tag-reopen.metrics" treedb_snapshot_seq)
    wal_reopen=$(metric "$R/$tag-reopen.metrics" treedb_wal_logical_bytes)
    for pair in "rows_reopen:$rows_reopen" "seq_reopen:$seq_reopen" \
        "snap_seq_reopen:$snap_reopen" "wal_reopen:$wal_reopen"; do
        uint "$phase ${pair%%:*}" "${pair#*:}"
    done
    [ "$rows_reopen" = "$expected_left" ] || die "$phase 重启后 rows=$rows_reopen，应为 $expected_left"
    [ "$seq_reopen" -eq "$seq_before_crash" ] || die "$phase 重启后 seq $seq_reopen != 崩溃前 $seq_before_crash"
    [ "$snap_reopen" -eq "$snap_auto" ] || die "$phase 重启后快照水位 $snap_reopen != 自动水位 $snap_auto"
    [ "$wal_reopen" -eq "$wal_before_crash" ] || die "$phase 重启后 WAL $wal_reopen != 崩溃前尾巴 $wal_before_crash"
    if [ "$mode" = churn ]; then
        if ! "$BIN/treedb-bench" --addr "$ADDR" --bulk verifychurn --rows "$ROWS" \
            --cycles "$CYCLES" > "$R/$tag.verify.log" 2>&1; then
            die "$phase 重启内容验收失败：$(tail -5 "$R/$tag.verify.log")"
        fi
        verify_checked=$(field "$(cat "$R/$tag.verify.log")" checked)
        uint "$phase verify_checked" "$verify_checked"
        [ "$verify_checked" = "$ROWS" ] || die "$phase 重启只核对 $verify_checked/$ROWS 行"
        say "   $(cat "$R/$tag.verify.log")"
    fi
    read -r rss_reopen _ < <(rss_now)
    uint "$phase rss_reopen" "$rss_reopen"
    stop_server "$tag-reopen"

    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$phase" "$ROWS" "$CYCLES" "$done" "$time" "$rec" "$wrt" "$sy" "$ck0" "$ck_auto" \
        "$wal0" "$wal_peak" "$wal_end" "$wal_auto" "$tail_records" "$wal_before_crash" \
        "$wal_reopen" "$snap0" "$snap_auto" "$snap_bytes" "$rss_before" "$rss_after" \
        "$rss_reopen" "$rss_peak" "$hwm_after" "$hwm_peak" "$samples" "$rows_reopen" \
        "$verify_checked" "$errs" >> "$TSV"
    say "   PASS $phase: checkpoints=$ck0→$ck_auto seq=$seq_auto→$seq_reopen snap_seq=$snap0→$snap_auto wal=$wal_peak→$wal_auto→$wal_before_crash→$wal_reopen"
    say "   RSS KiB before/after/reopen=$rss_before/$rss_after/$rss_reopen retained_delta=$retained peak=$rss_peak HWM=$hwm_peak samples=$samples"
}

say "== 删除风暴联合量具 $(date '+%F %T') =="
say "HEAD=$(git rev-parse HEAD) rows=$ROWS cycles=$CYCLES conns=$CONNS pipeline=$PIPE workers=$WORKERS auto_checkpoint_mb=$AUTO_MB sample_ms=$SAMPLE_MS"
CODE_SHA=$(sha256sum src/bin/bench.rs benches/delete_storm.sh tests/delete_storm.rs | sha256sum | awk '{print $1}')
say "code_sha256=$CODE_SHA"
say "临时现场=$R"
run_phase rowdelete rowdelete 0
run_phase churn churn "$ROWS"
say ""
say "== PASS：固定工作量、自动 checkpoint、WAL 回落、服务端 RSS、重启终态/内容全部通过 =="
say "逐格结果=$TSV"
