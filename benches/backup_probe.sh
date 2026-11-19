#!/bin/bash
# 备份 / 还原往返实测（DESIGN.md §6.7）。回答三个具体问题，不是纸上推理：
#   1) 停服后 cp -a 整个数据目录，能不能原样还原？
#   2) 服务端还在跑就 cp -a（很多人真会这么"备份"），后果是什么？
#   3) `treedb-standby` 的目录，能不能直接当还原点起一个普通主？
#
# 用法：bash benches/backup_probe.sh [每档写入秒数=3]
# 输出追加到 benches/results/backup_probe.log。全程只用临时目录，不碰真数据。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
BIN=./target/release
SECS=${1:-3}
R=.spot/bk
OUT=benches/results/backup_probe.log
PORT=17811
SRVS=""

stop_all() { for p in $SRVS; do kill -TERM "$p" 2>/dev/null; done; wait 2>/dev/null; SRVS=""; }
trap stop_all EXIT

mkdir -p .spot benches/results
say() { echo "$@" | tee -a "$OUT"; }

stats() { $BIN/treedb-server --listen 127.0.0.1:$1 --stats 2>/dev/null | grep -E '^(nodes|dentries|refs|seq|checkpoints)'; }

# 数据面三行 + seq 水位才是可比对的东西。`snapshot` 括号里的水位与
# `checkpoints` 计数都跟进程生死有关，拿它们比会把正常行为读成不一致。
dataof() { grep -E '^(nodes|dentries|refs)' "$1"; }
seqof()  { sed -n 's/^seq  *= \([0-9]*\).*/\1/p' "$1"; }

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

rm -rf "$R"; mkdir -p "$R"
{ say "=== 备份/还原往返 $(date -u '+%F %T UTC') rev=$(git rev-parse --short HEAD) ==="
  say "host nproc=$(nproc)  数据盘：$(df -hT . | tail -1 | awk '{print $2, $7}')  每档写入 ${SECS}s"; }

say "--- 1) 冷备份：写 → 优雅停服（SIGTERM 自带最后一次 checkpoint）→ cp -a → 从拷贝起服"
start "$R/src" $PORT --durability group --auto-checkpoint-mb 1
$BIN/treedb-bench --addr 127.0.0.1:$PORT --op create --conns 4 --threads 4 --pipeline 8 --secs "$SECS" > "$R/bench.log" 2>&1
stats $PORT > "$R/before"; sed 's/^/  before /' "$R/before" | tee -a "$OUT"
stop_all
cp -a "$R/src" "$R/cold"
PORT=$((PORT + 1))
start "$R/cold" $PORT --durability group --auto-checkpoint-mb 1
stats $PORT > "$R/after"; sed 's/^/  after  /' "$R/after" | tee -a "$OUT"
# 数据面三行与 seq 水位必须逐项相等；`snapshot`/`checkpoints` 两行不参与比对：
# 停服那次 checkpoint 属于**上一个进程**，新进程的计数从 0 起，而快照水位反而
# 被推到了 seq（拷贝里已经含那份最终 checkpoint）。
if diff <(dataof "$R/before") <(dataof "$R/after") >/dev/null \
   && [ "$(seqof "$R/before")" = "$(seqof "$R/after")" ]; then
    say "  判定：冷备份一致 ✓（nodes/dentries/refs/seq 逐项相等，seq $(seqof "$R/before")）"
    say "        附带一条行为：停服那次 checkpoint 已进拷贝（快照水位 $(sed -n 's/.*(snapshot \([0-9]*\)).*/\1/p' "$R/after") == seq），"
    say "        所以还原不依赖尾日志重放；而 checkpoints 计数是新进程从 0 起。"
else
    say "  判定：冷备份不一致 ✗ —— 下面是差异"
    diff <(dataof "$R/before") <(dataof "$R/after") | sed 's/^/    /' | tee -a "$OUT"
    say "        seq: 源 $(seqof "$R/before") vs 还原 $(seqof "$R/after")"
fi
stop_all

say "--- 2) 热拷贝：写入正在进行时 cp -a，然后从拷贝起服"
PORT=$((PORT + 1))
start "$R/src" $PORT --durability group --auto-checkpoint-mb 1
# 关键点：拷贝必须在写入进行中发生，不然量的就不是热拷贝。拷贝**前一刻**先取
# 一份 mark，拿它跟还原后的结果比：相等 = 切点干净；落后 = 拷到了撕裂尾，
# §6.2 把它截掉了，丢的是已 ack 的写。
stats $PORT > "$R/hot_mark"; sed 's/^/  mark   /' "$R/hot_mark" | tee -a "$OUT"
$BIN/treedb-bench --addr 127.0.0.1:$PORT --op create --conns 4 --threads 4 --pipeline 8 --secs "$SECS" > "$R/bench2.log" 2>&1 &
BENCH=$!
sleep 0.5
cp -a "$R/src" "$R/hot" 2>/dev/null
wait $BENCH
stats $PORT > "$R/hot_final"; sed 's/^/  源终态 /' "$R/hot_final" | tee -a "$OUT"
# 拷完**不**停原服务，直接从拷贝起：这才叫热拷贝。原进程继续跑，两个进程写各自目录。
PORT=$((PORT + 1))
if start "$R/hot" $PORT --durability group --auto-checkpoint-mb 1; then
    stats $PORT > "$R/hot_after"; sed 's/^/  还原后 /' "$R/hot_after" | tee -a "$OUT"
    m=$(seqof "$R/hot_mark"); f=$(seqof "$R/hot_final"); r=$(seqof "$R/hot_after")
    say "  seq 三个读数：拷贝前一刻 $m、拷贝完成时源端 $f、还原后 $r"
    if [ "$r" = "$m" ]; then
        say "  判定：切点正落在拷贝前一刻 —— 不多不少，一次干净的快照切面"
    elif [ "$r" -gt "$m" ] && [ "$r" -le "$f" ]; then
        say "  判定：切点落在写入流中间（比 mark 多 $((r - m)) 条 seq）—— 这才是热拷贝的"
        say "        真实形状：**能起服、无撕裂、自洽，但切点不可控**。你不知道还原出来的是"
        say "        哪一刻的状态，也无法向调用方承诺 RPO —— 备份工具不该长这样。"
    else
        say "  判定：还原比拷贝前一刻还旧 $((m - r)) 条 seq ✗ —— 拷到了撕裂尾，恢复时"
        say "        被 §6.2 截掉，丢的是当时已 ack 的写"
    fi
fi
stop_all

say "--- 3) 副本目录当还原点：主写 → standby 追平 → 把副本目录当普通主起服"
PORT=$((PORT + 1))
PMAIN=$PORT
start "$R/p" $PORT --durability group --auto-checkpoint-mb 1
$BIN/treedb-bench --addr 127.0.0.1:$PORT --op create --conns 4 --threads 4 --pipeline 8 --secs "$SECS" > "$R/bench3.log" 2>&1
stats $PORT > "$R/primary"; sed 's/^/  主     /' "$R/primary" | tee -a "$OUT"
PORT=$((PORT + 1))
$BIN/treedb-standby --dir "$R/rep" --listen 127.0.0.1:$PORT --primary 127.0.0.1:$PMAIN \
    --durability group > "$R/stb.log" 2>&1 &
SRVS="$SRVS $!"
wait_up $PORT || { say "!! 副本起不来：$(tail -2 "$R/stb.log")"; }
sleep 2
$BIN/treedb-server --listen 127.0.0.1:$PORT --stats 2>/dev/null \
    | grep -E '^(nodes|dentries|seq|replica)' | sed 's/^/  副本   /' | tee -a "$OUT"
stop_all
PORT=$((PORT + 1))
start "$R/rep" $PORT --durability group --auto-checkpoint-mb 1
stats $PORT > "$R/restored"; sed 's/^/  还原后 /' "$R/restored" | tee -a "$OUT"
if diff <(dataof "$R/primary") <(dataof "$R/restored") >/dev/null \
   && [ "$(seqof "$R/primary")" = "$(seqof "$R/restored")" ]; then
    say "  判定：副本目录可作为普通主起服，计数与 seq 均一致 ✓（但仍不是「提升」—— 没有 fence，"
    say "        旧主必须确认已停，否则两边各自追加日志；§11 第 4 条③的 \`--promote\` 还欠着）"
else
    say "  判定：副本目录还原后与主不一致 ✗"
    diff "$R/primary" "$R/restored" | sed 's/^/    /' | tee -a "$OUT"
fi
stop_all
say "--- 完"
