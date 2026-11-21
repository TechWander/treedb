#!/bin/bash
# 通用表（DESIGN.md §5.1）的基准：`treedb-bench --op row*` 九条路径各一格。
#
# 为什么单开一个脚本：通用表是 G4（RowRange 范围扫描）与 G6（加列）的基准面，
# 而那两条改动都要回答「快了多少 / 慢了没有」。没有这张底表，后面只能拿目录树
# 的 ops/s 去类比 —— 两条路的常数开销（定位主键树、维护二级索引、帧里多出的两
# 列）根本不是一回事。
#
# 轴的选法：
#   A) durability=never（不 fsync）：把 WAL 落盘从数字里摘掉，剩下的就是「树 +
#      索引 + 帧」这一层；`rowcount` 那一格量的是分派地板（O(1) 计数，不碰树）。
#   B) durability=group（默认）：加回每轮一次 fsync，A→B 的落差就是落盘占比。
#   C) workers=1：读轴在单线程事件循环下的地板。与 A 的 workers=auto 对拉，
#      差的正是 §7.9 那套「只读 op 就地并行」的收益；写轴两边应当打平（写由
#      唯一提交线程定序），打平本身就是判据。
#
# 另外三条自检（不做断言就是假绿）：
#   - 每一格的 `errors` 必须是 0；
#   - 每格跑 `REPS` 次取最大值：落盘档的 fsync 抖动很大（同一格两次跑实测差过
#     2.4×），单次采样不配当基线；最大值才接近「这条路径本身多快」；
#   - `rowchurn` 跑前跑后的行数必须相同 —— 它是 put/del 成对发的，行数要是涨了，
#     说明配对没成（那量到的就不是「树节点缩回零」那条路）。
#
# 用法：bash benches/bench_rows.sh        → 追加到 benches/results/bench_rows.log
#       SECS=8 CONNS=64 REPS=3 bash benches/bench_rows.sh   # 换强度
#       TREEDB_BENCH_WARM=50000 bash benches/bench_rows.sh   # 大表形状（默认 500 行）
#
# 区间那一族为什么要两档窗宽：`rowrange` 的页宽固定 100，窗宽 64 时一页装完
# （`end=true`），窗宽 400 时每页都是满的且 `end=false` —— 后者与 `rowlist`
# 的 100 行回包同形，两格对拉才能把「扫描一段」与「回包大小」分开。
#
# 全程只用临时目录与 127.0.0.1:17846，不碰真数据。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
BIN=./target/release
PORT=${PORT:-17846}
SECS=${SECS:-3}
CONNS=${CONNS:-8}
PIPE=${PIPE:-8}
REPS=${REPS:-2}
OUT=benches/results/bench_rows.log
R=.spot/rows
SRV=""
fail=0

cleanup() {
    if [ -n "$SRV" ]; then
        kill -TERM "$SRV" 2>/dev/null
        for _ in $(seq 100); do kill -0 "$SRV" 2>/dev/null || break; sleep 0.05; done
        kill -KILL "$SRV" 2>/dev/null
        wait "$SRV" 2>/dev/null
    fi
}
trap cleanup EXIT

(exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null && { echo "!! 端口 $PORT 被占"; exit 1; }
mkdir -p benches/results .spot
rm -rf "$R"; mkdir -p "$R"
say() { echo "$@" | tee -a "$OUT"; }
: > "$R/raw.tsv"

wait_up() {
    local i=0
    until (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; do
        i=$((i + 1)); [ $i -gt 200 ] && return 1; sleep 0.05
    done
}

start() { # DIR WORKERS DUR
    $BIN/treedb-server --dir "$1" --listen 127.0.0.1:$PORT --workers "$2" \
        --durability "$3" --auto-checkpoint-mb 0 > "$R/srv.log" 2>&1 &
    SRV=$!
    wait_up || { say "!! 服务端起不来：$(tail -2 "$R/srv.log")"; exit 1; }
    # auto 到底取到几路要写进日志：否则“workers=auto”这一栏在产物里无法复查。
    say "   服务端 $(grep -oE '(durability|workers)=[a-z0-9]+' "$R/srv.log" | paste -sd' ')"
}

stop() { # 换 durability / workers 都要重启：两个 flag 都只在启动时定
    kill -TERM "$SRV" 2>/dev/null
    for _ in $(seq 100); do kill -0 "$SRV" 2>/dev/null || break; sleep 0.05; done
    wait "$SRV" 2>/dev/null
    SRV=""
}

rows_now() { $BIN/treedb-server --listen 127.0.0.1:$PORT --stats | grep -oE 'rows [0-9]+' | head -1 | cut -d' ' -f2; }

run() { # TAG OP[:NAME] [extra...]
    # `OP:NAME` 让同一个 bin op 能在表里占两行（`rowrange` 的两档窗宽），
    # 汇总以 NAME 为键；不写 `:NAME` 时两者相同。
    local tag=$1 spec=$2; shift 2
    local op=${spec%%:*} name=${spec#*:}
    [ "$name" = "$spec" ] && name=$op
    local r l q best=0 lo=0 line=""
    for r in $(seq "$REPS"); do
        l=$($BIN/treedb-bench --addr 127.0.0.1:$PORT --op "$op" --conns "$CONNS" \
            --threads "$CONNS" --pipeline "$PIPE" --secs "$SECS" "$@" 2>&1 | tail -1)
        case "$l" in
            *"errors=0"*) ;;
            *) say "   !! $tag/$op 第 $r 次有错误：$l"; fail=$((fail + 1)); continue ;;
        esac
        q=$(echo "$l" | grep -oE '→ [0-9]+ ops/s' | grep -oE '[0-9]+')
        [ -z "$q" ] && { say "   !! $tag/$op 第 $r 次没出数字：$l"; fail=$((fail + 1)); continue; }
        [ "$best" -eq 0 ] && lo=$q
        [ "$q" -lt "$lo" ] && lo=$q
        if [ "$q" -gt "$best" ]; then best=$q; line=$l; fi
    done
    [ "$best" -eq 0 ] && return 1
    # 抖动用倍数写在行尾：同一个盘的 fsync 在两次跑里可能差一倍，看到 1.5× 以上
    # 就该知道这一格的绝对值不能拿去跟另一台机器对拉。
    say "$(printf '%-16s %s  [best of %d, lo/hi=%.2f]' "$name" "$line" "$REPS" \
        "$(echo "$lo $best" | awk '{printf "%.2f", $1 / $2}')")"
    printf '%s\t%s\t%s\n' "$tag" "$name" "$best" >> "$R/raw.tsv"
}

say "== 通用表基准 $(date '+%F %T')  secs=$SECS conns=$CONNS pipeline=$PIPE =="

# ---- A：不落盘，看树 + 索引 + 帧这一层的天花板 ----
# 每种形状都从**空目录**起，且把读格排在写格前面：`rowinsert` 每跑一遍就往表里
# 添十几万行，若先跑写，后面的读就压在逐渐变大的表上 —— 那样 A/B/C 三栏的读
# 数就混进了「表多大」这个变量。现在每种形状的读都发生在刚预热完的 500 行上
# （`TREEDB_BENCH_WARM` 可改，见文件头），三栏读直接对拉。
D=$R/never
say "-- A) durability=never workers=auto：树 + 索引 + 帧"
start "$D" 0 never
for op in rowget rowlist rowidx rowidxrange rowcount; do run never "$op"; done
run never rowrange:rowrange_w64 --range 64
run never rowrange:rowrange_w400 --range 400
# churn 单独跑：前后各数一次行数，成对才算真的在「长-缩」。
R0=$(rows_now)
run never rowchurn
R1=$(rows_now)
if [ "$R0" = "$R1" ]; then
    say "   churn 前后行数 $R0 = $R1（put/del 成对）"
else
    say "   !! churn 前后行数 $R0 → $R1：配对没成"; fail=$((fail + 1))
fi
for op in rowinsert rowput; do run never "$op"; done

# ---- B：加回落盘（默认形状） ----
say "-- B) durability=group workers=auto：同一批 op 加回每轮 fsync"
stop
start "$R/group" 0 group
for op in rowget rowlist rowidx rowidxrange rowcount rowinsert rowput rowchurn; do run group "$op"; done
run group rowrange:rowrange_w64 --range 64

# ---- C：单线程事件循环的地板（读轴对照组） ----
say "-- C) durability=never workers=1：单线程事件循环，读轴对照组"
stop
start "$R/w1" 1 never
for op in rowget rowlist rowidx rowidxrange rowcount rowput; do run w1 "$op"; done
run w1 rowrange:rowrange_w64 --range 64

# ---- 汇总 ----
say ""
say "== 汇总（ops/s）=="
awk -F'\t' '
{ v[$2, $1] = $3 }
END {
  printf "%-14s %12s %12s %12s %9s\n", "op", "never/auto", "group/auto", "never/w1", "g/n"
  n = split("rowinsert rowput rowget rowlist rowidx rowidxrange rowrange_w64 rowrange_w400 rowcount rowchurn", order, " ")
  for (i = 1; i <= n; i++) {
    k = order[i]
    a = v[k, "never"] + 0; b = v[k, "group"] + 0; c = v[k, "w1"] + 0
    if (a == 0) continue
    r = (b > 0) ? sprintf("%.2f", b / a) : "-"
    printf "%-14s %12d %12s %12s %9s\n", k, a, (b > 0 ? b : "-"), (c > 0 ? c : "-"), r
  }
}' "$R/raw.tsv" | tee -a "$OUT"

say ""
if [ "$fail" -eq 0 ]; then
    say "== 全部通过：每格 errors=0，churn 行数不变 =="
    exit 0
fi
say "== $fail 处失败 =="
exit 1
