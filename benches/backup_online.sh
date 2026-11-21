#!/bin/bash
# 在线备份实测（DESIGN.md §6.10）：`treedb-backup` 从**运行中**的主拉一致快照。
# 回答四个具体问题：
#   1) 主还没 checkpoint 过（没有 state.snap）时，备份能不能从头重放出来？
#   2) 主有「快照 + 尾巴」时，备份出来的东西与主逐项相等吗？
#   3) 备份进行中主持续写，切面停在哪儿？自洽吗？
#   4) `--out -` 管道档（接 gzip）能不能走通并被 load 吃下？
#
# 用法：bash benches/backup_online.sh [每档写入秒数=2]
# 输出追加到 benches/results/backup_online.log。全程只用临时目录，不碰真数据。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
BIN=./target/release
SECS=${1:-2}
R=.spot/bkon
OUT=benches/results/backup_online.log
PORT=17821
SRVS=""

stop_all() { for p in $SRVS; do kill -TERM "$p" 2>/dev/null; done; wait 2>/dev/null; SRVS=""; }
trap stop_all EXIT

mkdir -p .spot benches/results
rm -rf "$R"; mkdir -p "$R"
say() { echo "$@" | tee -a "$OUT"; }

wait_up() {
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/$1) 2>/dev/null; do
        i=$((i + 1)); [ $i -gt 200 ] && return 1; sleep 0.05
    done
}

start() { # DIR PORT ARGS...
    local dir=$1 port=$2; shift 2
    $BIN/treedb-server --dir "$dir" --listen 127.0.0.1:$port "$@" > "$R/srv.$port.log" 2>&1 &
    SRVS="$SRVS $!"
    wait_up "$port" || { say "!! $port 起不来：$(tail -2 "$R/srv.$port.log")"; return 1; }
}

# 数据面四行 + seq 水位才是可比对的东西：`snapshot` 括号里的水位与 `checkpoints`
# 计数都跟进程生死有关，拿它们比会把正常行为读成不一致。
dataof() { grep -E '^(views|nodes|dentries|seq) *=' "$1"; }
# 用 paste 而不是 tr：tr 会在末尾留一个空格，与 `dataline_of_dump` 的串永远不等。
dataline() { grep -E '^(views|nodes|dentries) *=' "$1" | sed 's/ *= */=/' | paste -sd' ' -; }
seqof() { sed -n 's/^seq  *= \([0-9]*\).*/\1/p' "$1"; }
# dump 简介行里同样的三个数（`treedb-load --in` 打的 `dump = seq … logs … | views …`）。
dataline_of_dump() { grep '^dump *=' "$1" | sed -n 's/.*| views \([0-9]*\) nodes \([0-9]*\) dentries \([0-9]*\).*/views=\1 nodes=\2 dentries=\3/p'; }

say ""
say "=============== $(date '+%F %T')  treedb-backup（rev $(git rev-parse --short HEAD 2>/dev/null || echo '?')，每档写 ${SECS}s）"

# ---------------------------------------------------------------- 档 1：无快照
say ""
say "[1] 主从未 checkpoint：没有 state.snap，WalFetch 必须从头重放"
D=$R/nosnap
start "$D" $PORT --durability group --workers 4 --auto-checkpoint-mb 0 || exit 1
$BIN/treedb-bench --addr 127.0.0.1:$PORT --op create --conns 4 --threads 4 --secs "$SECS" \
    | sed 's/^/  bench   /' | tee -a "$OUT"
$BIN/treedb-server --listen 127.0.0.1:$PORT --stats > "$R/nosnap.stats"
say "  主      $(dataline "$R/nosnap.stats") seq=$(seqof "$R/nosnap.stats")"
$BIN/treedb-backup --primary 127.0.0.1:$PORT --out "$R/nosnap.dump" 2>&1 | sed 's/^/  backup  /' | tee -a "$OUT"
$BIN/treedb-load --in "$R/nosnap.dump" > "$R/nosnap.load"
say "  备件    $(dataline_of_dump "$R/nosnap.load")"
if [ "$(dataline "$R/nosnap.stats")" = "$(dataline_of_dump "$R/nosnap.load")" ]; then
    say "  判定：无快照档计数一致 ✓（切面 seq $(seqof "$R/nosnap.stats")）"
else
    say "  判定：无快照档计数不一致 ✗"; diff "$R/nosnap.stats" "$R/nosnap.load" | sed 's/^/    /' | tee -a "$OUT"
fi

# ---------------------------------------------------------------- 档 2-4 共用主
say ""
say "[2] 快照 + 尾巴：checkpoint 后再写一段，备份要两段拼接"
$BIN/treedb-server --listen 127.0.0.1:$PORT --shutdown 2>/dev/null; wait 2>/dev/null; SRVS=""
D=$R/live
start "$D" $PORT --durability group --workers 4 --auto-checkpoint-mb 1 || exit 1
$BIN/treedb-bench --addr 127.0.0.1:$PORT --op create --conns 4 --threads 4 --secs "$SECS" \
    | sed 's/^/  bench   /' | tee -a "$OUT"
$BIN/treedb-server --listen 127.0.0.1:$PORT --stats > "$R/live.stats"
say "  主      $(dataline "$R/live.stats") seq=$(seqof "$R/live.stats")（$(grep '^snapshot' "$R/live.stats" | sed 's/^snapshot *= *//')）"
$BIN/treedb-backup --primary 127.0.0.1:$PORT --out "$R/live.dump" 2>&1 | sed 's/^/  backup  /' | tee -a "$OUT"
$BIN/treedb-load --in "$R/live.dump" > "$R/live.load"
say "  备件    $(dataline_of_dump "$R/live.load")"
if [ "$(dataline "$R/live.stats")" = "$(dataline_of_dump "$R/live.load")" ]; then
    say "  判定：静置档计数一致 ✓"
else
    say "  判定：静置档计数不一致 ✗"
fi

# ---------------------------------------------------------------- 档 3：写入中
say ""
say "[3] 备份进行中主持续写：切面必须落在 [备份前, 备份后] 且自洽"
$BIN/treedb-server --listen 127.0.0.1:$PORT --stats > "$R/w0.stats"
S0=$(seqof "$R/w0.stats")
( $BIN/treedb-bench --addr 127.0.0.1:$PORT --op create --conns 4 --threads 4 --secs "$SECS" > "$R/w.bench" 2>&1 ) &
W=$!
sleep 0.05
$BIN/treedb-backup --primary 127.0.0.1:$PORT --out "$R/w.dump" --retries 8 > "$R/w.dump.log" 2>&1
sed 's/^/  backup  /' "$R/w.dump.log" | tee -a "$OUT"
$BIN/treedb-server --listen 127.0.0.1:$PORT --stats > "$R/w1.stats"
S1=$(seqof "$R/w1.stats")
wait $W
SD=$(sed -n 's/^treedb-backup:.*seq \([0-9]*\))$/\1/p' "$R/w.dump.log")
[ -z "${SD:-}" ] && SD=$(grep '^dump *=' <($BIN/treedb-load --in "$R/w.dump") | sed -n 's/^dump *= *seq \([0-9]*\).*/\1/p')
$BIN/treedb-load --in "$R/w.dump" --dir "$R/w.restore" | tail -2 | sed 's/^/  load    /' | tee -a "$OUT"
start "$R/w.restore" $((PORT + 1)) --durability group --workers 4 --auto-checkpoint-mb 0 || exit 1
$BIN/treedb-server --listen 127.0.0.1:$((PORT + 1)) --stats > "$R/w.r.stats"
say "  切面 seq=$SD，主 $S0 → $S1；恢复后 $(dataline "$R/w.r.stats") seq=$(seqof "$R/w.r.stats")"
if [ "$SD" -ge "$S0" ] && [ "$SD" -le "$S1" ] && [ "$(seqof "$R/w.r.stats")" = "$SD" ]; then
    say "  判定：切面在区间内且恢复水位等于切面 ✓"
else
    say "  判定：切面越界或恢复水位不符 ✗"
fi

# ---------------------------------------------------------------- 档 4：管道
say ""
say "[4] 管道档：--out - 接 gzip，摘要走 stderr，字节流不能被污染"
# 先落一份普通文件作基准：主这时已静置（档 3 的写线程已 join），两次备份应当
# 拿到同一个切面，于是「文件版本 == 解压后的管道版本」可以当字节级判据。
$BIN/treedb-backup --primary 127.0.0.1:$PORT --out "$R/p1.dump" > /dev/null 2>&1
$BIN/treedb-backup --primary 127.0.0.1:$PORT --out - 2> "$R/pipe.summary" | gzip -1 > "$R/pipe.dump.gz"
grep -E '^(treedb-backup|  )' "$R/pipe.summary" | sed 's/^/  backup  /' | tee -a "$OUT"
gunzip -c "$R/pipe.dump.gz" > "$R/pipe.dump"
SZ=$(stat -c %s "$R/pipe.dump.gz")
$BIN/treedb-load --in "$R/pipe.dump" > "$R/pipe.load"
say "  压缩后 $SZ B；$(dataline_of_dump "$R/pipe.load")"
if cmp -s "$R/p1.dump" "$R/pipe.dump"; then
    say "  判定：管道字节与同切面的文件版本逐字节相同 ✓"
else
    say "  判定：管道字节与文件版本不同 ✗（主在两次备份之间写过？）"
fi

stop_all
say "--- 完"
