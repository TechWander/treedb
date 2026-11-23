#!/bin/bash
# 批量形状（DESIGN.md §5.1 的 `TableClear` / `RowRangeDelete` / `TableRename`）
# 与 `RowPut` 同值消除的实测：同一张表、同样 N 行，三种删法各花多少。
#
# 为什么单开一个脚本、而不是往 `bench_rows.sh` 里加几格：那一套是**时长口径**
# （跑满 SECS 秒数 ops/s），而破坏性负载不能按时长量 —— 表在第 0.3 秒就被删空了，
# 剩下 2.7 秒全在数空区间，报出来的 ops/s 反而是全场最好看的那个。这里改成
# **固定工作量口径**：删掉正好 N 行，报时间、WAL 记录数、fsync 次数、字节数。
#
# 六档：
#   rowdelete    逐行 `RowDelete`，N 条 op、N 条记录（这就是要被打下去的基线）
#   range        `RowRangeDelete`，每批 `--batch` 行 → ceil(N/batch) 条记录
#   clear        一条 `TableClear` 删完 → 1 条记录
#   rename       影子表顶替（一条 `TableRename`）：切换本身的代价
#   putsame      N 次逐字节相同的 `RowPut` → 同值消除应当 0 条记录
#   putchanged   N 次真的改值 → 对照组，必须 N 条记录
#
# 两档 durability：`never` 看「扫描 + 索引维护」这一层，`group` 加回每轮一次
# fsync —— 后者的差距才是在机械盘上真正省下来的东西。
#
# 每一档都带**格式自己的断言**（不是时间断言，所以不会 flaky）：`clear` 必须
# 只推 1 个 seq、`range` 必须正好 ceil(N/batch) 个、`putsame` 必须 0 条记录而
# `putchanged` 必须 N 条、四种删法之后 `rows_left` 必须为 0、`rename` 之后不许
# 有孤儿影子表。数字再好看，断言不过就算失败。
#
# 用法：bash benches/bench_bulk.sh
#       ROWS=1000000 BATCH=5000 bash benches/bench_bulk.sh
#
# 全程只用临时目录与 127.0.0.1:17847，不碰真数据。
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT" || exit 1
BIN=./target/release
PORT=${PORT:-17847}
ROWS=${ROWS:-200000}
BATCH=${BATCH:-1000}
CONNS=${CONNS:-8}
PIPE=${PIPE:-64}
OUT=benches/results/bench_bulk.log
R=.spot/bulk
SRV=""
fail=0

cleanup() {
    if [ -n "$SRV" ]; then
        kill -TERM "$SRV" 2>/dev/null
        for _ in $(seq 200); do kill -0 "$SRV" 2>/dev/null || break; sleep 0.05; done
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
        i=$((i + 1)); [ $i -gt 400 ] && return 1; sleep 0.05
    done
}

start() { # DIR DUR
    $BIN/treedb-server --dir "$1" --listen 127.0.0.1:$PORT --workers 0 \
        --durability "$2" --auto-checkpoint-mb 0 > "$R/srv.log" 2>&1 &
    SRV=$!
    wait_up || { say "!! 服务端起不来：$(tail -2 "$R/srv.log")"; exit 1; }
    say "   服务端 $(grep -oE '(durability|workers)=[a-z0-9]+' "$R/srv.log" | paste -sd' ')"
}

stop() {
    kill -TERM "$SRV" 2>/dev/null
    for _ in $(seq 200); do kill -0 "$SRV" 2>/dev/null || break; sleep 0.05; done
    wait "$SRV" 2>/dev/null
    SRV=""
}

# 从 `bulk mode=... k=v ...` 那一行里抠一个字段。
f() { echo "$1" | tr ' ' '\n' | grep -m1 -oE "^$2=[0-9]+" | cut -d= -f2; }

run() { # TAG MODE [batch]
    local tag=$1 mode=$2 batch=${3:-$BATCH}
    local l rec wrt sy time left err shadow nops want
    l=$($BIN/treedb-bench --addr 127.0.0.1:$PORT --bulk "$mode" --rows "$ROWS" \
        --batch "$batch" --conns "$CONNS" --pipeline "$PIPE" 2>&1 | grep -m1 '^bulk ')
    [ -z "$l" ] && { say "   !! $tag/$mode 没出数字"; fail=$((fail + 1)); return 1; }
    rec=$(f "$l" records); wrt=$(f "$l" writes); sy=$(f "$l" syncs)
    time=$(f "$l" time_ms); left=$(f "$l" rows_left); err=$(f "$l" errors)
    shadow=$(f "$l" shadow_left); nops=$(f "$l" ops)
    [ "${err:-1}" != "0" ] && { say "   !! $tag/$mode errors=$err：$l"; fail=$((fail + 1)); return 1; }
    case "$tag/$mode" in */*) ;; esac

    # 期望的记录数：这一档的格式契约，算不出来就是断言漏了。
    case "$mode" in
        rowdelete)   want=$ROWS ;;
        clear)       want=1 ;;
        rename)      want=1 ;;
        range)       want=$(( (ROWS + batch - 1) / batch )) ;;
        putsame)     want=0 ;;
        putchanged)  want=$ROWS ;;
        *)           want=-1 ;;
    esac
    if [ "$want" != "-1" ] && [ "$rec" != "$want" ]; then
        say "   !! $tag/$mode records=$rec，应为 $want：$l"
        fail=$((fail + 1))
    fi
    case "$mode" in
        rowdelete|range|clear)
            [ "$left" != "0" ] && { say "   !! $tag/$mode 删完还剩 rows_left=$left"; fail=$((fail + 1)); } ;;
        # rename 不是删法，是换内容：顶替之后表里必须还是 N 行，而且 bench 已经
        # 逐行比对过首行来自影子表（`q` 前缀）。这里只守住行数这一头。
        rename)
            [ "$left" != "$ROWS" ] && { say "   !! $tag/rename rows_left=$left，应为 $ROWS"; fail=$((fail + 1)); } ;;
    esac
    [ "$mode" = "rename" ] && [ "$shadow" != "0" ] && {
        say "   !! $tag/rename 留着 $shadow 张孤儿影子表"; fail=$((fail + 1)); }
    [ "$mode" = "putsame" ] && [ "$wrt" != "0" ] && {
        say "   !! $tag/putsame writes=$wrt，被消除的写不该计入"; fail=$((fail + 1)); }

    # 汇总里的键：只有 `range` 会跑两档批大小，靠 batch 区分显示名，其余一档一个键。
    local name=$mode
    [ "$mode" = "range" ] && name="range/b$batch"
    say "$(printf '   %-11s %8s ms  records=%-8s writes=%-8s syncs=%-7s wal=%-9s rows_left=%s ops=%s' \
        "$name" "$time" "$rec" "$wrt" "$sy" \
        "$(f "$l" wal_bytes)" "$left" "$nops")"
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$tag" "$name" "$time" "$rec" "$sy" \
        "$(f "$l" wal_bytes)" "$(f "$l" rows_per_s)" >> "$R/raw.tsv"
}

say "== 批量形状基准 $(date '+%F %T')  rows=$ROWS batch=$BATCH conns=$CONNS pipeline=$PIPE =="

say "-- A) durability=never：扫描 + 索引维护这一层"
start "$R/never" never
for m in rowdelete range clear rename putsame putchanged; do run never "$m"; done
run never range 10000

say "-- B) durability=group：加回每轮 fsync"
stop
start "$R/group" group
for m in rowdelete range clear rename putsame putchanged; do run group "$m"; done
run group range 10000

# ---- 汇总 ----
say ""
say "== 汇总（time_ms / records / syncs / wal 字节）=="
# 两件容易做错的事：
#   1. 列名不能写死 `range/b1000` —— `BATCH` 是可改的（这一档就是拿
#      `ROWS=1000000 BATCH=2000` 跑的），写死了就在第二个格子上除零；
#   2. `records=0` 是 putsame 那一格的**正确结果**，不能拿「字段为 0」当「这一格
#      缺失」来看，否则汇总里最该被看见的那个数会被当成空白略过。两者靠
#      「键在不在表里」区分，不靠值的大小。
awk -F'\t' -v bat="$BATCH" '
{
  k = $1 "/" $2
  t[k] = $3; r[k] = $4; s[k] = $5; w[k] = $6; have[k] = 1
}
END {
  printf "%-14s %10s %10s %10s %10s %10s %11s %11s\n", \
    "mode", "never ms", "group ms", "never rec", "group rec", "never syn", "group syn", "group wal"
  n = split("rowdelete range/b" bat " range/b10000 clear rename putsame putchanged", order, " ")
  miss = 0
  for (i = 1; i <= n; i++) {
    k = order[i]
    if (!("never/" k in have)) { miss = 1; printf "   !! 缺格 never/%s\n", k; continue }
    printf "%-14s %10s %10s %10s %10s %10s %11s %11s\n", k, \
      t["never/" k], ("group/" k in have ? t["group/" k] : "-"), \
      r["never/" k], ("group/" k in have ? r["group/" k] : "-"), \
      s["never/" k], ("group/" k in have ? s["group/" k] : "-"), \
      ("group/" k in have ? w["group/" k] : "-")
  }
  exit miss
}' "$R/raw.tsv" | tee -a "$OUT"
[ "${PIPESTATUS[0]}" != 0 ] && { say "!! 汇总有缺格"; fail=$((fail + 1)); }

# 省下多少：逐行 → 区间 → 清空，三档的记录数之比就是这个 op 存在的全部理由。
say ""
calc=$(awk -F'\t' -v bat="$BATCH" '
$1 == "group" { t[$2] = $3; r[$2] = $4; s[$2] = $5; w[$2] = $6; have[$2] = 1 }
END {
  rg = "range/b" bat
  if (!("rowdelete" in have) || !(rg in have) || !("clear" in have) || !("putsame" in have)) {
    print "!! 结论行缺格，算不出比值"; exit 1
  }
  printf "group 档记录数：rowdelete=%s  %s=%s  clear=%s  → 区间删省 %.0f×，清空省 %.0f×\n", \
    r["rowdelete"], rg, r[rg], r["clear"], \
    r["rowdelete"] / r[rg], r["rowdelete"] / r["clear"]
  printf "group 档时间：  rowdelete=%sms  %s=%sms  clear=%sms  → 快 %.1f× / %.1f×\n", \
    t["rowdelete"], rg, t[rg], t["clear"], \
    t["rowdelete"] / t[rg], t["rowdelete"] / t["clear"]
  printf "group 档 fsync：rowdelete=%s  %s=%s  clear=%s 次\n", \
    s["rowdelete"], rg, s[rg], s["clear"]
  printf "group 档 WAL：   rowdelete=%s  %s=%s  clear=%s 字节\n", \
    w["rowdelete"], rg, w[rg], w["clear"]
  printf "同值消除：putsame records=%s syncs=%s（应为 0），putchanged records=%s syncs=%s；时间 %sms vs %sms\n", \
    r["putsame"], s["putsame"], r["putchanged"], s["putchanged"], t["putsame"], t["putchanged"]
}' "$R/raw.tsv")
if [ $? != 0 ]; then
    say "!! 结论行算不出来"; fail=$((fail + 1))
fi
say "$calc"

say ""
if [ "$fail" -eq 0 ]; then
    say "== 全部通过：每档 errors=0、终态与格式契约一致 =="
    exit 0
fi
say "== $fail 处失败 =="
exit 1
